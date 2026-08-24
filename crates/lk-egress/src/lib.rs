//! A voice-only `livekit-egress` drop-in in Rust.

pub mod audio;
pub mod client;
pub mod config;
pub mod io;
pub mod mp3;
pub mod redact;
pub mod recorder;
pub mod server;
pub mod sts;
pub mod upload;
pub mod wav;

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Unix time in nanoseconds — the wire unit for `EgressInfo.started_at` /
/// `ended_at` / `updated_at` and the per-result timestamps (the Go egress
/// reports `time.Now().UnixNano()`).
pub fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}
