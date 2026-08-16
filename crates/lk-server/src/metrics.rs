//! Minimal Prometheus metrics. The deploy health check requires at least a
//! `livekit_room_total` gauge on the metrics endpoint.

use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub room_total: AtomicI64,
    pub participants_total: AtomicI64,
    pub tracks_published_total: AtomicI64,
    pub participants_joined_total: AtomicI64,
}

impl Metrics {
    /// Renders the text/plain Prometheus exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP livekit_room_total Number of active rooms.\n");
        out.push_str("# TYPE livekit_room_total gauge\n");
        out.push_str(&format!(
            "livekit_room_total {}\n",
            self.room_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP livekit_participants_total Number of active participants.\n");
        out.push_str("# TYPE livekit_participants_total gauge\n");
        out.push_str(&format!(
            "livekit_participants_total {}\n",
            self.participants_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP livekit_tracks_published_total Total number of published tracks.\n");
        out.push_str("# TYPE livekit_tracks_published_total counter\n");
        out.push_str(&format!(
            "livekit_tracks_published_total {}\n",
            self.tracks_published_total.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP livekit_participants_joined_total Total participants joined.\n");
        out.push_str("# TYPE livekit_participants_joined_total counter\n");
        out.push_str(&format!(
            "livekit_participants_joined_total {}\n",
            self.participants_joined_total.load(Ordering::Relaxed)
        ));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_required_metric() {
        let m = Metrics::default();
        m.room_total.store(3, Ordering::Relaxed);
        let out = m.render();
        assert!(out.contains("livekit_room_total 3"));
        assert!(out.contains("# TYPE livekit_room_total gauge"));
    }
}
