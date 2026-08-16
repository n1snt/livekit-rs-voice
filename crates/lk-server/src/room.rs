//! Room lifecycle and participant broadcast logic.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use lk_proto::livekit as lk;

use crate::core::{new_room_sid, unix_millis, TimedVersion};
use crate::media::RtcEngine;
use crate::metrics::Metrics;
use crate::participant::{Participant, ParticipantState};
use crate::webhook::WebhookNotifier;

/// Shared services a room can reach (avoids a server<->room cycle).
pub struct RoomContext {
    pub config: Arc<crate::config::Config>,
    pub rtc: Arc<RtcEngine>,
    pub webhook: WebhookNotifier,
    pub metrics: Arc<Metrics>,
    pub agent: Arc<crate::agent::AgentManager>,
    /// Last broadcast speaker-set per room (used to avoid redundant updates).
    pub speaker_states: std::sync::Mutex<HashMap<String, String>>,
}

impl RoomContext {
    pub fn new(
        config: Arc<crate::config::Config>,
        rtc: Arc<RtcEngine>,
        webhook: WebhookNotifier,
        metrics: Arc<Metrics>,
        agent: Arc<crate::agent::AgentManager>,
    ) -> Self {
        RoomContext {
            config,
            rtc,
            webhook,
            metrics,
            agent,
            speaker_states: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Test helper: a context with default stubs.
    pub fn test_context() -> Arc<RoomContext> {
        Arc::new(RoomContext::new(
            Arc::new(crate::config::Config::default()),
            Arc::new(RtcEngine::new()),
            WebhookNotifier::disabled(),
            Arc::new(Metrics::default()),
            Arc::new(crate::agent::AgentManager::new()),
        ))
    }
}

/// Test helper (free function) for constructing a default context.
pub fn test_context() -> Arc<RoomContext> {
    RoomContext::test_context()
}

pub struct Room {
    pub sid: String,
    pub name: String,
    pub metadata: Mutex<String>,
    pub empty_timeout: std::sync::atomic::AtomicU32,
    pub departure_timeout: std::sync::atomic::AtomicU32,
    pub max_participants: std::sync::atomic::AtomicU32,
    pub creation_time_ms: i64,
    pub version: Mutex<TimedVersion>,
    pub active_recording: AtomicBool,
    /// Room-level agent dispatches (auto-dispatched on first join).
    pub agents: Mutex<Vec<crate::auth::RoomAgentDispatch>>,
    /// Dispatch keys already launched, to avoid re-launching per session.
    launched_agents: Mutex<std::collections::HashSet<String>>,
    pub closed: AtomicBool,
    pub ctx: Weak<RoomContext>,
    participants: Mutex<HashMap<String, Arc<Participant>>>,
    /// Called exactly once when the room closes, to remove it from the manager.
    on_close: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// Last time the room became empty (unix millis), for departure timeout.
    empty_since: Mutex<Option<i64>>,
    /// Whether any non-dependent participant has joined (drives empty_timeout).
    pub ever_joined: AtomicBool,
}

impl Room {
    pub fn new(name: String, ctx: Weak<RoomContext>) -> Arc<Self> {
        let config = ctx.upgrade().map(|c| c.config.clone());
        let (empty_timeout, departure_timeout, max_participants) = config
            .map(|c| {
                (
                    c.room.empty_timeout,
                    c.room.departure_timeout,
                    c.room.max_participants,
                )
            })
            .unwrap_or((300, 20, 0));
        Arc::new(Room {
            sid: new_room_sid(),
            name,
            metadata: Mutex::new(String::new()),
            empty_timeout: std::sync::atomic::AtomicU32::new(empty_timeout),
            departure_timeout: std::sync::atomic::AtomicU32::new(departure_timeout),
            max_participants: std::sync::atomic::AtomicU32::new(max_participants),
            creation_time_ms: unix_millis(),
            version: Mutex::new(TimedVersion::new()),
            active_recording: AtomicBool::new(false),
            agents: Mutex::new(Vec::new()),
            launched_agents: Mutex::new(std::collections::HashSet::new()),
            closed: AtomicBool::new(false),
            ctx,
            participants: Mutex::new(HashMap::new()),
            on_close: Mutex::new(None),
            empty_since: Mutex::new(None),
            ever_joined: AtomicBool::new(false),
        })
    }

    pub fn context(&self) -> Option<Arc<RoomContext>> {
        self.ctx.upgrade()
    }

    pub fn set_on_close(&self, f: impl FnOnce() + Send + 'static) {
        *self.on_close.lock().unwrap() = Some(Box::new(f));
    }

    pub fn bump_version(&self) {
        let mut v = self.version.lock().unwrap();
        v.bump();
    }

    pub fn update_metadata(&self, metadata: String) -> bool {
        let mut m = self.metadata.lock().unwrap();
        if *m == metadata {
            return false;
        }
        *m = metadata;
        self.bump_version();
        true
    }

    pub fn is_full(&self) -> bool {
        let max = self
            .max_participants
            .load(std::sync::atomic::Ordering::Relaxed);
        if max == 0 {
            return false;
        }
        let count = self
            .participants
            .lock()
            .unwrap()
            .values()
            .filter(|p| !p.kind.is_dependent())
            .count();
        count >= max as usize
    }

    pub fn participants(&self) -> Vec<Arc<Participant>> {
        self.participants
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn get_participant(&self, sid: &str) -> Option<Arc<Participant>> {
        self.participants.lock().unwrap().get(sid).cloned()
    }

    pub fn get_participant_by_identity(&self, identity: &str) -> Option<Arc<Participant>> {
        self.participants
            .lock()
            .unwrap()
            .values()
            .find(|p| p.identity == identity)
            .cloned()
    }

    /// Adds a participant to the room. Returns false if the room is closed/full.
    pub fn join(self: &Arc<Self>, participant: Arc<Participant>) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        if !participant.kind.is_dependent() && self.is_full() {
            return false;
        }
        let mut map = self.participants.lock().unwrap();
        // Replace any existing participant with the same identity (duplicate join).
        if map.values().any(|p| p.identity == participant.identity) {
            drop(map);
            return false; // handled by caller: terminate the existing session
        }
        map.insert(participant.sid.clone(), participant.clone());
        drop(map);
        self.ever_joined.store(true, Ordering::Relaxed);
        *self.empty_since.lock().unwrap() = None;
        self.bump_version();
        participant.attach_room(Arc::downgrade(self));
        true
    }

    /// Removes a participant, returns true if they were present.
    pub fn remove_participant(&self, sid: &str) -> Option<Arc<Participant>> {
        let removed = self.participants.lock().unwrap().remove(sid);
        if removed.is_some() {
            self.bump_version();
            if self.participants.lock().unwrap().is_empty() {
                *self.empty_since.lock().unwrap() = Some(unix_millis());
            }
        }
        removed
    }

    pub fn num_participants(&self) -> usize {
        self.participants.lock().unwrap().len()
    }

    pub fn num_publishers(&self) -> usize {
        self.participants
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_publisher.load(Ordering::Relaxed))
            .count()
    }

    pub fn to_proto(&self) -> lk::Room {
        let num_participants = self.num_participants() as u32;
        lk::Room {
            sid: self.sid.clone(),
            name: self.name.clone(),
            empty_timeout: self
                .empty_timeout
                .load(std::sync::atomic::Ordering::Relaxed),
            departure_timeout: self
                .departure_timeout
                .load(std::sync::atomic::Ordering::Relaxed),
            max_participants: self
                .max_participants
                .load(std::sync::atomic::Ordering::Relaxed),
            creation_time: self.creation_time_ms / 1000,
            creation_time_ms: self.creation_time_ms,
            metadata: self.metadata.lock().unwrap().clone(),
            num_participants,
            num_publishers: self.num_publishers() as u32,
            active_recording: self.active_recording.load(Ordering::Relaxed),
            version: self.version.lock().unwrap().to_proto(),
            ..Default::default()
        }
    }

    /// Broadcasts a `SignalResponse::Update` with the given participant infos.
    /// `except` excludes a participant sid (e.g. the publisher themselves).
    pub fn broadcast_participant_update(
        &self,
        infos: Vec<lk::ParticipantInfo>,
        except: Option<&str>,
    ) {
        let resp = lk::SignalResponse {
            message: Some(lk::signal_response::Message::Update(
                lk::ParticipantUpdate {
                    participants: infos,
                },
            )),
        };
        let targets: Vec<Arc<Participant>> = self
            .participants()
            .into_iter()
            .filter(|p| except.map(|sid| sid != p.sid).unwrap_or(true))
            .collect();
        for p in targets {
            p.send_update(resp.clone());
        }
    }

    pub fn broadcast_room_update(&self) {
        let room = self.to_proto();
        let resp = lk::SignalResponse {
            message: Some(lk::signal_response::Message::RoomUpdate(lk::RoomUpdate {
                room: Some(room),
            })),
        };
        for p in self.participants() {
            p.send_update(resp.clone());
        }
    }

    /// Broadcasts a data packet to all participants matching destination identities.
    pub fn broadcast_data(&self, packet: lk::DataPacket, destinations: &[String]) {
        let targets = self.participants();
        for p in targets {
            if destinations.is_empty() || destinations.iter().any(|d| *d == p.identity) {
                crate::media::send_data_to_participant(&p, &packet);
            }
        }
    }

    /// Called when a participant leaves; tracks empty-since for timeout handling.
    pub fn on_participant_left(&self) {
        if self.participants.lock().unwrap().is_empty() {
            let mut es = self.empty_since.lock().unwrap();
            if es.is_none() {
                *es = Some(unix_millis());
            }
        }
    }

    /// Returns the number of ms the room has been empty, if any.
    pub fn empty_since_ms(&self) -> Option<i64> {
        self.empty_since.lock().unwrap().map(|t| unix_millis() - t)
    }

    pub fn clear_empty_since(&self) {
        *self.empty_since.lock().unwrap() = None;
    }

    /// Whether the room should close: empty past departure timeout, or never
    /// joined past empty timeout. Mirrors `CloseIfEmpty` semantics.
    pub fn should_close(&self) -> bool {
        if self.closed.load(Ordering::Relaxed) || !self.participants.lock().unwrap().is_empty() {
            return false;
        }
        let timeout = if self.ever_joined.load(Ordering::Relaxed) {
            self.departure_timeout
                .load(std::sync::atomic::Ordering::Relaxed)
        } else {
            self.empty_timeout
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        if timeout == 0 {
            return false;
        }
        let elapsed = if self.ever_joined.load(Ordering::Relaxed) {
            match self.empty_since_ms() {
                Some(e) => e,
                None => return false,
            }
        } else {
            // Never joined: measure from room creation.
            unix_millis() - self.creation_time_ms
        };
        elapsed >= i64::from(timeout) * 1000
    }

    /// Closes the room, disconnecting all participants. Fires `room_finished`
    /// and terminates room-level agent jobs once, regardless of caller.
    pub async fn close(&self, reason: lk::DisconnectReason) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }
        let room_proto = self.to_proto();
        let ctx = self.context();
        if let Some(ctx) = &ctx {
            ctx.agent.terminate_room_jobs(&self.name).await;
        }

        let participants = self.participants();
        for p in participants {
            p.set_state(ParticipantState::Disconnected);
            p.disconnected_reason
                .store(reason as i32, Ordering::Relaxed);
            p.clear_signal_sink();
            crate::media::close_participant_media(&p).await;
            // Notify the participant it is being disconnected.
            let leave = lk::SignalResponse {
                message: Some(lk::signal_response::Message::Leave(lk::LeaveRequest {
                    reason: reason as i32,
                    ..Default::default()
                })),
            };
            p.send(leave).await;
            if let Some(ctx) = &ctx {
                ctx.metrics
                    .participants_total
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                ctx.webhook
                    .participant_left(&room_proto, &p.to_proto())
                    .await;
            }
        }
        self.participants.lock().unwrap().clear();
        if let Some(ctx) = &ctx {
            ctx.webhook.room_finished(&room_proto).await;
        }
        if let Some(f) = self.on_close.lock().unwrap().take() {
            f(); // removes the room from the manager and decrements room_total
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn set_empty_timeout(&self, v: u32) {
        self.empty_timeout
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_departure_timeout(&self, v: u32) {
        self.departure_timeout
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_max_participants(&self, v: u32) {
        self.max_participants
            .store(v, std::sync::atomic::Ordering::Relaxed);
    }

    /// Adds a room-level agent dispatch (from token roomConfig.agents).
    /// Returns true if the dispatch is new (and should be launched).
    pub fn add_agent_dispatch(&self, d: crate::auth::RoomAgentDispatch) -> bool {
        let key = format!("{}\0{}", d.agent_name, d.metadata);
        let mut agents = self.agents.lock().unwrap();
        if agents
            .iter()
            .any(|a| format!("{}\0{}", a.agent_name, a.metadata) == key)
        {
            return false;
        }
        agents.push(d);
        true
    }

    /// Marks a dispatch as launched.
    pub fn mark_dispatch_launched(&self, key: &str) {
        self.launched_agents.lock().unwrap().insert(key.to_string());
    }

    /// Returns true if this dispatch key was already launched.
    pub fn was_dispatch_launched(&self, key: &str) -> bool {
        self.launched_agents.lock().unwrap().contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ParticipantKind;

    #[test]
    fn join_and_remove() {
        let room = Room::new(
            "room1".to_string(),
            Arc::downgrade(&crate::room::test_context()),
        );
        let p = Participant::new(
            "a".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        assert!(room.join(p.clone()));
        assert_eq!(room.num_participants(), 1);
        assert!(room.get_participant_by_identity("a").is_some());
        room.remove_participant(&p.sid);
        assert_eq!(room.num_participants(), 0);
        assert!(room.empty_since_ms().is_some());
    }

    #[test]
    fn max_participants_excludes_dependents() {
        let room = Room::new(
            "r".to_string(),
            Arc::downgrade(&crate::room::test_context()),
        );
        room.max_participants
            .store(1, std::sync::atomic::Ordering::Relaxed);
        let p1 = Participant::new(
            "a".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        let agent = Participant::new(
            "agent-1".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Agent,
        );
        assert!(room.join(p1.clone()));
        assert!(room.join(agent)); // agent is dependent and joins despite the limit
        assert!(room.is_full()); // the standard participant still fills the room
        let p2 = Participant::new(
            "b".to_string(),
            String::new(),
            String::new(),
            ParticipantKind::Standard,
        );
        assert!(!room.join(p2)); // room is full for standard participants
    }

    #[test]
    fn room_proto_shape() {
        let room = Room::new(
            "r".to_string(),
            Arc::downgrade(&crate::room::test_context()),
        );
        let proto = room.to_proto();
        assert_eq!(proto.name, "r");
        assert!(proto.sid.starts_with("RM_"));
        assert!(proto.creation_time_ms > 0);
    }

    #[test]
    fn should_close_after_departure_timeout() {
        let room = Room::new(
            "r".to_string(),
            Arc::downgrade(&crate::room::test_context()),
        );
        room.empty_timeout
            .store(1, std::sync::atomic::Ordering::Relaxed);
        room.departure_timeout
            .store(1, std::sync::atomic::Ordering::Relaxed);
        room.ever_joined.store(true, Ordering::Relaxed);
        *room.empty_since.lock().unwrap() = Some(unix_millis() - 2000);
        assert!(room.should_close());
    }
}
