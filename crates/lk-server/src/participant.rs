//! Participant state and signaling outbound channel.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};

use lk_proto::livekit as lk;
use tokio::sync::mpsc;

use crate::core::{unix_millis, unix_seconds, ParticipantKind, ParticipantSid, VersionCounter};
use crate::media::ParticipantMedia;
use crate::track::PublishedTrack;
use crate::{auth, room::Room};

/// Capacity of the outbound signal channel. Matches the reference server's
/// generous buffering; broadcasts drop rather than block when full.
pub const SIGNAL_CHANNEL_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantState {
    Joining,
    Joined,
    Active,
    Disconnected,
}

impl ParticipantState {
    pub fn to_proto(self) -> i32 {
        (match self {
            ParticipantState::Joining => lk::participant_info::State::Joining,
            ParticipantState::Joined => lk::participant_info::State::Joined,
            ParticipantState::Active => lk::participant_info::State::Active,
            ParticipantState::Disconnected => lk::participant_info::State::Disconnected,
        }) as i32
    }
}

pub struct Participant {
    pub sid: ParticipantSid,
    pub identity: String,
    pub name: Mutex<String>,
    pub metadata: Mutex<String>,
    pub attributes: Mutex<BTreeMap<String, String>>,
    pub kind: ParticipantKind,
    pub joined_at_ms: i64,
    pub client_protocol: AtomicI32,
    pub is_publisher: AtomicBool,
    pub permission: Mutex<lk::ParticipantPermission>,
    pub auto_subscribe: AtomicBool,
    state: AtomicU8,
    pub version: VersionCounter,
    pub disconnected_reason: AtomicI32,
    /// Outbound signal channel to the websocket writer.
    tx: Mutex<Option<mpsc::Sender<lk::SignalResponse>>>,
    /// Rooms this participant belongs to (single room for now).
    room: Mutex<Weak<Room>>,
    /// Published tracks, keyed by sid and by client cid.
    pub tracks: Mutex<BTreeMap<String, Arc<PublishedTrack>>>,
    pub tracks_by_cid: Mutex<BTreeMap<String, Arc<PublishedTrack>>>,
    /// Media plane state (peer connections, forwarders, down-tracks).
    pub media: Mutex<ParticipantMedia>,
}

impl Participant {
    pub fn new(
        identity: String,
        name: String,
        metadata: String,
        kind: ParticipantKind,
    ) -> Arc<Self> {
        Arc::new(Participant {
            sid: crate::core::new_participant_sid(),
            identity,
            name: Mutex::new(name),
            metadata: Mutex::new(metadata),
            attributes: Mutex::new(BTreeMap::new()),
            kind,
            joined_at_ms: unix_millis(),
            client_protocol: AtomicI32::new(0),
            is_publisher: AtomicBool::new(false),
            permission: Mutex::new(lk::ParticipantPermission::default()),
            auto_subscribe: AtomicBool::new(true),
            state: AtomicU8::new(ParticipantState::Joining as u8),
            version: VersionCounter::default(),
            disconnected_reason: AtomicI32::new(0),
            tx: Mutex::new(None),
            room: Mutex::new(Weak::new()),
            tracks: Mutex::new(BTreeMap::new()),
            tracks_by_cid: Mutex::new(BTreeMap::new()),
            media: Mutex::new(ParticipantMedia::default()),
        })
    }

    pub fn attach_room(&self, room: Weak<Room>) {
        *self.room.lock().unwrap() = room;
    }

    pub fn room(&self) -> Option<Arc<Room>> {
        self.room.lock().unwrap().upgrade()
    }

    pub fn state(&self) -> ParticipantState {
        match self.state.load(Ordering::Relaxed) {
            1 => ParticipantState::Joined,
            2 => ParticipantState::Active,
            3 => ParticipantState::Disconnected,
            _ => ParticipantState::Joining,
        }
    }

    /// Monotonic state transition (out-of-order transitions are ignored).
    pub fn set_state(&self, state: ParticipantState) -> bool {
        let new = state as u8;
        let mut prev = self.state.load(Ordering::Relaxed);
        loop {
            if new <= prev {
                return false;
            }
            match self
                .state
                .compare_exchange(prev, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(current) => prev = current,
            }
        }
    }

    /// Sets the websocket outbound channel, returning any replaced sender.
    pub fn set_signal_sink(
        &self,
        tx: mpsc::Sender<lk::SignalResponse>,
    ) -> Option<mpsc::Sender<lk::SignalResponse>> {
        self.tx.lock().unwrap().replace(tx)
    }

    /// Removes the signal sink (websocket disconnected).
    pub fn clear_signal_sink(&self) {
        *self.tx.lock().unwrap() = None;
    }

    /// Sends a response to this participant's websocket, awaiting capacity.
    /// Returns false if the participant is disconnected.
    pub async fn send(&self, resp: lk::SignalResponse) -> bool {
        let tx = { self.tx.lock().unwrap().clone() };
        match tx {
            Some(tx) => tx.send(resp).await.is_ok(),
            None => false,
        }
    }

    /// Best-effort broadcast send; drops when the channel is full.
    pub fn send_update(&self, resp: lk::SignalResponse) -> bool {
        let tx = { self.tx.lock().unwrap().clone() };
        match tx {
            Some(tx) => tx.try_send(resp).is_ok(),
            None => false,
        }
    }

    pub fn is_disconnected(&self) -> bool {
        self.state() == ParticipantState::Disconnected
    }

    /// Milliseconds since the participant session started.
    pub fn session_age_ms(&self) -> i64 {
        unix_millis() - self.joined_at_ms
    }

    pub fn can_subscribe(&self) -> bool {
        self.permission.lock().unwrap().can_subscribe
    }

    pub fn can_publish(&self) -> bool {
        self.permission.lock().unwrap().can_publish
    }

    pub fn get_attribute(&self, key: &str) -> Option<String> {
        self.attributes.lock().unwrap().get(key).cloned()
    }

    pub fn set_attributes(&self, attrs: BTreeMap<String, String>) -> bool {
        let mut map = self.attributes.lock().unwrap();
        let mut changed = false;
        for (k, v) in attrs {
            if v.is_empty() {
                changed |= map.remove(&k).is_some();
            } else {
                changed |= map.get(&k) != Some(&v);
                map.insert(k, v);
            }
        }
        changed
    }

    pub fn attributes(&self) -> BTreeMap<String, String> {
        self.attributes.lock().unwrap().clone()
    }

    pub fn update_metadata(&self, metadata: String, name: Option<String>) -> bool {
        let mut changed = false;
        if !metadata.is_empty() {
            let mut m = self.metadata.lock().unwrap();
            if *m != metadata {
                *m = metadata;
                changed = true;
            }
        }
        if let Some(name) = name {
            if !name.is_empty() {
                let mut n = self.name.lock().unwrap();
                if *n != name {
                    *n = name;
                    changed = true;
                }
            }
        }
        changed
    }

    pub fn update_permission(&self, permission: lk::ParticipantPermission) -> bool {
        let mut p = self.permission.lock().unwrap();
        if *p == permission {
            return false;
        }
        *p = permission;
        true
    }

    pub fn add_track(&self, track: Arc<PublishedTrack>) {
        self.tracks
            .lock()
            .unwrap()
            .insert(track.sid.clone(), track.clone());
        self.tracks_by_cid
            .lock()
            .unwrap()
            .insert(track.cid.clone(), track);
    }

    pub fn remove_track(&self, sid: &str) -> Option<Arc<PublishedTrack>> {
        let track = self.tracks.lock().unwrap().remove(sid);
        if let Some(t) = &track {
            self.tracks_by_cid.lock().unwrap().remove(&t.cid);
        }
        track
    }

    pub fn get_track(&self, sid: &str) -> Option<Arc<PublishedTrack>> {
        self.tracks.lock().unwrap().get(sid).cloned()
    }

    pub fn get_track_by_cid(&self, cid: &str) -> Option<Arc<PublishedTrack>> {
        self.tracks_by_cid.lock().unwrap().get(cid).cloned()
    }

    pub fn tracks(&self) -> Vec<Arc<PublishedTrack>> {
        self.tracks.lock().unwrap().values().cloned().collect()
    }

    pub fn track_infos(&self) -> Vec<lk::TrackInfo> {
        self.tracks().iter().map(|t| t.to_proto()).collect()
    }

    pub fn to_proto(&self) -> lk::ParticipantInfo {
        let attributes = self.attributes();
        let permission = self.permission.lock().unwrap().clone();
        lk::ParticipantInfo {
            sid: self.sid.clone(),
            identity: self.identity.clone(),
            state: self.state().to_proto(),
            tracks: self.track_infos(),
            metadata: self.metadata.lock().unwrap().clone(),
            joined_at: unix_seconds(),
            joined_at_ms: self.joined_at_ms,
            name: self.name.lock().unwrap().clone(),
            version: self.version.get(),
            permission: Some(permission),
            is_publisher: self.is_publisher.load(Ordering::Relaxed),
            kind: self.kind.to_proto(),
            attributes,
            disconnect_reason: self.disconnected_reason.load(Ordering::Relaxed),
            client_protocol: self.client_protocol.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    /// Builds the permission from a verified token's video grant.
    #[allow(deprecated)]
    pub fn permission_from_grant(grant: &auth::VideoGrant) -> lk::ParticipantPermission {
        lk::ParticipantPermission {
            can_publish: grant.can_publish.unwrap_or(true),
            can_subscribe: grant.can_subscribe.unwrap_or(true),
            can_publish_data: grant
                .can_publish_data
                .unwrap_or(grant.can_publish.unwrap_or(true)),
            can_publish_sources: grant
                .can_publish_sources
                .iter()
                .map(|s| {
                    crate::track::TrackSource::from_proto(crate::track::source_from_str(s))
                        .to_proto()
                })
                .collect(),
            hidden: grant.hidden,
            recorder: grant.recorder,
            can_update_metadata: grant.can_update_own_metadata.unwrap_or(false),
            agent: grant.agent,
            can_subscribe_metrics: grant.can_subscribe_metrics.unwrap_or(false),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_transitions_are_monotonic() {
        let p = Participant::new(
            "a".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        assert!(p.set_state(ParticipantState::Joined));
        assert!(!p.set_state(ParticipantState::Joining)); // cannot go backwards
        assert!(p.set_state(ParticipantState::Active));
        assert_eq!(p.state(), ParticipantState::Active);
    }

    #[test]
    fn tracks_keyed_by_sid_and_cid() {
        let p = Participant::new(
            "a".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        let t = Arc::new(PublishedTrack::new(
            "mic".to_string(),
            "cid1".to_string(),
            crate::track::TrackSource::Microphone,
            String::new(),
        ));
        p.add_track(t);
        assert!(p.get_track(&p.tracks().first().unwrap().sid).is_some());
        assert!(p.get_track_by_cid("cid1").is_some());
        assert_eq!(p.track_infos().len(), 1);
    }

    #[test]
    fn metadata_update() {
        let p = Participant::new(
            "a".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        assert!(p.update_metadata("{\"k\":1}".to_string(), None));
        assert_eq!(*p.metadata.lock().unwrap(), "{\"k\":1}");
        assert!(!p.update_metadata("{\"k\":1}".to_string(), None)); // unchanged
    }
}
