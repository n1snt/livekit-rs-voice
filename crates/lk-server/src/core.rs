//! Core identifiers and value types shared across the server.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lk_proto::livekit as lk;

pub type RoomName = String;
pub type ParticipantSid = String;
pub type TrackSid = String;
pub type Identity = String;

/// Clock used for all TimedVersion / timestamp fields (unix micros).
pub fn unix_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

pub fn unix_millis() -> i64 {
    unix_micros() / 1_000
}

pub fn unix_seconds() -> i64 {
    unix_micros() / 1_000_000
}

/// Random unique ids in the shape LiveKit uses (`RM_`, `PA_`, `TR_`, `LX_`...).
pub fn generate_id(prefix: &str) -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let suffix: String = (0..12)
        .map(|_| {
            let idx = rng.gen_range(0..ALPHABET.len());
            ALPHABET[idx] as char
        })
        .collect();
    format!("{prefix}{suffix}")
}

pub fn new_room_sid() -> String {
    generate_id("RM_")
}

/// Node id with the reference server's prefix (e.g. `LX` + random suffix).
pub fn node_id(prefix: Option<&str>) -> String {
    let prefix = prefix
        .unwrap_or(crate::config::DEFAULT_NODE_ID_PREFIX)
        .to_uppercase();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}{suffix}")
}

pub fn new_participant_sid() -> String {
    generate_id("PA_")
}

pub fn new_track_sid() -> String {
    generate_id("TR_")
}

pub fn new_dispatch_id() -> String {
    generate_id("DA_")
}

pub fn new_worker_id() -> String {
    generate_id("LW_")
}

pub fn new_job_id() -> String {
    generate_id("LJ_")
}

/// Conflict-free monotonically increasing version used by rooms and
/// participants so clients can order updates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct TimedVersion {
    pub unix_micro: i64,
    pub ticks: i32,
}

impl TimedVersion {
    pub fn new() -> Self {
        TimedVersion {
            unix_micro: unix_micros(),
            ticks: 0,
        }
    }

    pub fn bump(&mut self) -> TimedVersion {
        let now = unix_micros();
        if now == self.unix_micro {
            self.ticks += 1;
        } else {
            self.unix_micro = now;
            self.ticks = 0;
        }
        *self
    }

    pub fn to_proto(&self) -> Option<lk::TimedVersion> {
        Some(lk::TimedVersion {
            unix_micro: self.unix_micro,
            ticks: self.ticks,
        })
    }
}

/// Monotonic counter that never goes backwards, used for participant versions.
#[derive(Debug, Default)]
pub struct VersionCounter(AtomicI64);

impl VersionCounter {
    pub fn next(&self) -> u32 {
        let prev = self.0.fetch_add(1, Ordering::Relaxed);
        prev.max(1) as u32
    }

    pub fn get(&self) -> u32 {
        self.0.load(Ordering::Relaxed) as u32
    }
}

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

/// Participant kind, mapped to the wire `ParticipantInfo.Kind` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParticipantKind {
    #[default]
    Standard = 0,
    Ingress = 1,
    Egress = 2,
    Sip = 3,
    Agent = 4,
    Connector = 7,
    Bridge = 8,
}

impl ParticipantKind {
    pub fn to_proto(self) -> i32 {
        self as i32
    }

    pub fn from_proto(v: i32) -> Self {
        match v {
            1 => ParticipantKind::Ingress,
            2 => ParticipantKind::Egress,
            3 => ParticipantKind::Sip,
            4 => ParticipantKind::Agent,
            7 => ParticipantKind::Connector,
            8 => ParticipantKind::Bridge,
            _ => ParticipantKind::Standard,
        }
    }

    /// Dependent participants (agents/egress) don't count toward room capacity
    /// or keep the room open.
    pub fn is_dependent(self) -> bool {
        matches!(self, ParticipantKind::Agent | ParticipantKind::Egress)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_expected_prefixes() {
        assert!(new_room_sid().starts_with("RM_"));
        assert!(new_participant_sid().starts_with("PA_"));
        assert!(new_track_sid().starts_with("TR_"));
        assert!(new_room_sid().len() > 3);
    }

    #[test]
    fn timed_version_bumps_forward() {
        let mut v = TimedVersion::new();
        let v1 = v.bump();
        let v2 = v.bump();
        assert!(v2.unix_micro > v1.unix_micro || v2.ticks > v1.ticks);
    }

    #[test]
    fn participant_kinds_match_wire() {
        assert_eq!(ParticipantKind::Standard.to_proto(), 0);
        assert_eq!(ParticipantKind::Sip.to_proto(), 3);
        assert_eq!(ParticipantKind::Agent.to_proto(), 4);
        assert!(ParticipantKind::Agent.is_dependent());
        assert!(!ParticipantKind::Standard.is_dependent());
    }
}
