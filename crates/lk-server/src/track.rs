//! Published-track model (the signaling side of a media track).
//!
//! The media forwarding state (RTP receiver, per-subscriber down-tracks) lives
//! in the `media` module and is attached to the participant, keyed by track sid.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use lk_proto::livekit as lk;

use crate::core::{new_track_sid, TimedVersion, TrackSid};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackKind {
    Audio,
}

impl TrackKind {
    pub fn to_proto(self) -> i32 {
        lk::TrackType::Audio as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackSource {
    Unknown,
    Camera,
    Microphone,
    ScreenShare,
    ScreenShareAudio,
}

impl TrackSource {
    pub fn to_proto(self) -> i32 {
        (match self {
            TrackSource::Unknown => lk::TrackSource::Unknown,
            TrackSource::Camera => lk::TrackSource::Camera,
            TrackSource::Microphone => lk::TrackSource::Microphone,
            TrackSource::ScreenShare => lk::TrackSource::ScreenShare,
            TrackSource::ScreenShareAudio => lk::TrackSource::ScreenShareAudio,
        }) as i32
    }

    pub fn from_proto(v: i32) -> Self {
        match v {
            1 => TrackSource::Camera,
            2 => TrackSource::Microphone,
            3 => TrackSource::ScreenShare,
            4 => TrackSource::ScreenShareAudio,
            _ => TrackSource::Unknown,
        }
    }

    /// Reference `TrackSource.String()` value (used for metric labels).
    pub fn source_str(self) -> &'static str {
        match self {
            TrackSource::Unknown => "unknown",
            TrackSource::Camera => "camera",
            TrackSource::Microphone => "microphone",
            TrackSource::ScreenShare => "screen_share",
            TrackSource::ScreenShareAudio => "screen_share_audio",
        }
    }
}

/// Maps a grant's `canPublishSources` string to the wire `TrackSource` value.
pub fn source_from_str(s: &str) -> i32 {
    match s.to_uppercase().as_str() {
        "CAMERA" => 1,
        "MICROPHONE" => 2,
        "SCREEN_SHARE" => 3,
        "SCREEN_SHARE_AUDIO" => 4,
        _ => 0,
    }
}

/// A track published by a participant. Only audio is supported (voice-only).
pub struct PublishedTrack {
    pub sid: TrackSid,
    /// Client-assigned track id, used to correlate the SDP mid with the track.
    pub cid: String,
    pub name: String,
    pub kind: TrackKind,
    pub source: TrackSource,
    pub stream: String,
    muted: AtomicBool,
    pub mime: Mutex<String>,
    /// SDP mid once the track is negotiated.
    pub mid: Mutex<Option<String>>,
    pub version: Mutex<TimedVersion>,
    /// Redundant encoding (audio/red) enabled by the publisher.
    pub red_enabled: AtomicBool,
}

impl PublishedTrack {
    pub fn new(name: String, cid: String, source: TrackSource, stream: String) -> Self {
        PublishedTrack {
            sid: new_track_sid(),
            cid,
            name,
            kind: TrackKind::Audio,
            source,
            stream,
            muted: AtomicBool::new(false),
            mime: Mutex::new("audio/opus".to_string()),
            mid: Mutex::new(None),
            version: Mutex::new(TimedVersion::new()),
            red_enabled: AtomicBool::new(false),
        }
    }

    pub fn is_muted(&self) -> bool {
        self.muted.load(Ordering::Relaxed)
    }

    pub fn set_muted(&self, muted: bool) -> bool {
        let prev = self.muted.swap(muted, Ordering::Relaxed);
        if prev != muted {
            self.bump_version();
        }
        prev != muted
    }

    pub fn set_mime(&self, mime: String) {
        *self.mime.lock().unwrap() = mime;
    }

    pub fn mime(&self) -> String {
        self.mime.lock().unwrap().clone()
    }

    pub fn set_mid(&self, mid: Option<String>) {
        *self.mid.lock().unwrap() = mid;
    }

    pub fn get_mid(&self) -> Option<String> {
        self.mid.lock().unwrap().clone()
    }

    fn bump_version(&self) {
        let mut v = self.version.lock().unwrap();
        v.bump();
    }

    pub fn to_proto(&self) -> lk::TrackInfo {
        let mime = self.mime();
        lk::TrackInfo {
            sid: self.sid.clone(),
            r#type: self.kind.to_proto(),
            name: self.name.clone(),
            muted: self.is_muted(),
            source: self.source.to_proto(),
            mime_type: mime.clone(),
            mid: self.get_mid().unwrap_or_default(),
            disable_red: !self.red_enabled.load(Ordering::Relaxed),
            version: self.version.lock().unwrap().to_proto(),
            stream: self.stream.clone(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_info_shape() {
        let t = PublishedTrack::new(
            "mic".to_string(),
            "TR_client_id".to_string(),
            TrackSource::Microphone,
            "s".to_string(),
        );
        assert!(t.sid.starts_with("TR_"));
        assert_eq!(t.mime(), "audio/opus");
        let info = t.to_proto();
        assert_eq!(info.r#type, lk::TrackType::Audio as i32);
        assert_eq!(info.source, lk::TrackSource::Microphone as i32);
        assert!(!info.muted);
    }

    #[test]
    fn mute_changes_version() {
        let t = PublishedTrack::new(
            "mic".to_string(),
            "c".to_string(),
            TrackSource::Microphone,
            String::new(),
        );
        assert!(!t.is_muted());
        assert!(t.set_muted(true));
        assert!(!t.set_muted(true)); // no change -> false
        assert!(t.is_muted());
    }
}
