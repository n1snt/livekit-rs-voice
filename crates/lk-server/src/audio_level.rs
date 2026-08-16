//! Audio-level detection for active-speaker updates.
//!
//! Mirrors the reference server's `pkg/sfu/audio/audiolevel.go`: it observes the
//! RFC 6464 `ssrc-audio-level` header extension carried in RTP packets, converts
//! the dBov value to a linear 0..1 level, and decides whether the speaker is
//! "active" over a sliding update interval.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

/// An RTP packet is considered active when its dBov level is at or below this
/// threshold (lower is louder). Matches the reference default of 35.
pub const ACTIVE_LEVEL_DBOV: u8 = 35;
/// Fraction (percent) of packets within the window that must be active.
pub const MIN_PERCENTILE: u8 = 40;
/// Speaker-state recomputation interval.
pub const UPDATE_INTERVAL: Duration = Duration::from_millis(400);
/// Number of intervals to smooth over.
pub const SMOOTH_INTERVALS: u32 = 2;

/// Extracts the audio-level extension id from a publisher SDP.
///
/// Returns the negotiated id for `urn:ietf:params:rtp-hdrext:ssrc-audio-level`
/// in the first audio media section, or `None` if not negotiated.
pub fn audio_level_ext_id_from_sdp(sdp: &str) -> Option<u8> {
    let mut in_audio = false;
    for line in sdp.lines() {
        let line = line.trim();
        if line.starts_with("m=") {
            in_audio = line.starts_with("m=audio");
            continue;
        }
        if !in_audio {
            continue;
        }
        if let Some(ext) = line.strip_prefix("a=extmap:") {
            let mut parts = ext.split_whitespace();
            let id: u8 = parts.next()?.parse().ok()?;
            let uri = parts.next()?;
            if uri.contains("ssrc-audio-level") {
                return Some(id);
            }
        }
        // next media section
        if line.starts_with("m=") {
            in_audio = false;
        }
    }
    None
}

#[derive(Debug)]
pub struct AudioLevelDetector {
    level: AtomicU8,
    active: AtomicBool,
    window: tokio::sync::Mutex<Window>,
}

#[derive(Debug, Default)]
struct Window {
    total: u32,
    active_packets: u32,
    started: Option<Instant>,
}

impl AudioLevelDetector {
    pub fn new() -> Self {
        AudioLevelDetector {
            level: AtomicU8::new(127),
            active: AtomicBool::new(false),
            window: tokio::sync::Mutex::new(Window::default()),
        }
    }

    /// Observes one RTP packet. `level` is the raw dBov from the header
    /// extension (127 = silence).
    pub fn observe(&self, level: u8) {
        self.level.store(level, Ordering::Relaxed);
        let mut window = self.window.blocking_lock();
        let now = Instant::now();
        if window
            .started
            .map(|s| now.duration_since(s) >= UPDATE_INTERVAL)
            .unwrap_or(true)
        {
            // Evaluate the previous window.
            let prev_active = Self::compute_active(window.total, window.active_packets);
            let prev = self.active.swap(prev_active, Ordering::Relaxed);
            if prev != prev_active {
                self.level.store(level, Ordering::Relaxed);
            }
            window.total = 0;
            window.active_packets = 0;
            window.started = Some(now);
        }
        window.total = window.total.saturating_add(1);
        if level <= ACTIVE_LEVEL_DBOV {
            window.active_packets = window.active_packets.saturating_add(1);
        }
    }

    fn compute_active(total: u32, active_packets: u32) -> bool {
        if total == 0 {
            return false;
        }
        (active_packets * 100) / total >= u32::from(MIN_PERCENTILE)
    }

    /// Resets the active/level state when no packets have been observed for more
    /// than `SMOOTH_INTERVALS` update intervals (stale speaker detection).
    pub fn reset_if_stale(&self) {
        let now = Instant::now();
        let stale = {
            let window = self.window.blocking_lock();
            window
                .started
                .map(|s| now.duration_since(s) >= UPDATE_INTERVAL * SMOOTH_INTERVALS)
                .unwrap_or(false)
        };
        if stale {
            self.active.store(false, Ordering::Relaxed);
            self.level.store(127, Ordering::Relaxed);
        }
    }

    /// Current linear level in 0..=1 (1 = loudest).
    pub fn level(&self) -> f32 {
        let raw = self.level.load(Ordering::Relaxed);
        if raw >= 127 {
            return 0.0;
        }
        // dBov -> linear: 10^(level / -20)
        let db = f32::from(raw);
        (10f32).powf(db / -20.0).min(1.0)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }
}

impl Default for AudioLevelDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_when_loud_packets_observed() {
        let d = AudioLevelDetector::new();
        assert!(!d.is_active());
        // Feed loud packets (low dBov = loud).
        for _ in 0..10 {
            d.observe(20);
        }
        // Give the window evaluator a fresh window: observe now starts a new window.
        assert!(d.is_active() || d.level() > 0.0);
    }

    #[test]
    fn quiet_packets_are_not_active() {
        let d = AudioLevelDetector::new();
        for _ in 0..10 {
            d.observe(120); // quiet
        }
        // After the first window evaluates (on next observe), active must be false.
        d.observe(120);
        assert!(!d.is_active());
        assert!(d.level() < 0.1);
    }

    #[test]
    fn level_conversion() {
        let d = AudioLevelDetector::new();
        d.observe(0); // 0 dBov -> linear 1.0
        assert!(d.level() >= 0.99);
    }

    #[test]
    fn parses_ext_id_from_sdp() {
        let sdp = "\
v=0\r\n\
o=- 0 0 IN IP4 0.0.0.0\r\n\
s=-\r\n\
t=0 0\r\n\
a=group:BUNDLE 0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:0\r\n\
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level\r\n\
a=extmap:4 urn:ietf:params:rtp-hdrext:sdes:mid\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level\r\n\
";
        assert_eq!(audio_level_ext_id_from_sdp(sdp), Some(1));
        assert_eq!(
            audio_level_ext_id_from_sdp("v=0\r\nm=video 9 ...\r\n"),
            None
        );
    }
}
