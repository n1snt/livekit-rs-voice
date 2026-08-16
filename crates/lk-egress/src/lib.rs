//! A voice-only `livekit-egress` drop-in in Rust.

pub mod audio;
pub mod client;
pub mod config;
pub mod io;
pub mod mp3;
pub mod recorder;
pub mod server;
pub mod wav;

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
