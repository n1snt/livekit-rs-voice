//! Prometheus metrics, drop-in compatible with the reference `livekit-server`.
//!
//! Every metric name, label, and histogram bucket below matches the Go server
//! (`pkg/telemetry/prometheus`), so existing LiveKit Grafana dashboards work
//! unchanged. All collectors carry `node_id` / `node_type` const labels like
//! the reference.
//!
//! Emitted set: room / participant / connection gauges, track published +
//! subscribed gauges and publish/subscribe counters, session join/start/
//! duration histograms, connection-quality rating + score histograms, and
//! RTP packet + byte counters. The voice-only SFU does not intercept RTCP
//! feedback or measure forwarding delay, so `nack`/`pli`/`fir` counters and
//! `forward_latency_ms`/`forward_jitter_ms` are not emitted.

use std::collections::HashMap;

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

/// Const labels matching the reference server.
fn node_labels(node_id: &str, node_type: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    labels.insert("node_id".to_string(), node_id.to_string());
    labels.insert("node_type".to_string(), node_type.to_string());
    labels
}

/// A `HistogramVec` with the node const labels baked in.
fn hist_vec(
    name: &str,
    help: &str,
    buckets: &[f64],
    labels: &[&str],
    nl: HashMap<String, String>,
) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help)
            .const_labels(nl)
            .buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid histogram vec")
}

/// A `CounterVec` with the node const labels baked in.
fn counter_vec(
    name: &str,
    help: &str,
    labels: &[&str],
    nl: HashMap<String, String>,
) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help).const_labels(nl), labels).expect("valid counter vec")
}

/// A `GaugeVec` with the node const labels baked in.
fn gauge_vec(name: &str, help: &str, labels: &[&str], nl: HashMap<String, String>) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help).const_labels(nl), labels).expect("valid gauge vec")
}

fn gauge(name: &str, help: &str, nl: HashMap<String, String>) -> IntGauge {
    IntGauge::with_opts(Opts::new(name, help).const_labels(nl)).expect("valid gauge")
}

pub struct Metrics {
    registry: Registry,

    // rooms
    pub room_total: IntGauge,
    pub room_duration: Histogram,
    // participants / sessions
    pub participant_total: IntGauge,
    pub participant_join: IntCounterVec, // status
    pub connection_total: IntGaugeVec,
    pub session_join_latency: HistogramVec, // protocol_version
    pub session_start_time: HistogramVec,   // protocol_version, warp
    pub session_duration: HistogramVec,     // protocol_version
    // tracks
    pub track_published: IntGaugeVec,           // kind
    pub track_subscribed: IntGaugeVec,          // kind
    pub track_publish_counter: IntCounterVec,   // kind, state
    pub track_subscribe_counter: IntCounterVec, // state, error
    // quality
    pub quality_rating: Histogram,
    pub quality_score: Histogram,
    // media
    pub packet_total: IntCounterVec,       // direction, transmission
    pub packet_bytes: IntCounterVec,       // direction, transmission
    pub nack_total: IntCounterVec,         // direction, country
    pub pli_total: IntCounterVec,          // direction, country
    pub fir_total: IntCounterVec,          // direction, country
    pub packet_loss_percent: HistogramVec, // direction, source, type, country
    pub packet_loss_total: IntCounterVec,  // direction, source, type, country
    pub packet_out_of_order_percent: HistogramVec, // direction, source, type, country
    pub packet_out_of_order_total: IntCounterVec, // direction, source, type, country
    pub jitter_us: HistogramVec,           // direction, source, type, country
    pub rtt_ms: HistogramVec,              // direction, source, type, country
    pub forward_latency: prometheus::Gauge,
    pub forward_jitter: prometheus::Gauge,
    pub forward_latency_ns: Histogram,
}

impl Metrics {
    pub fn new(node_id: &str, node_type: &str) -> Self {
        let nl = node_labels(node_id, node_type);
        let registry = Registry::new();

        let room_total = gauge("livekit_room_total", "Number of active rooms.", nl.clone());
        let room_duration = Histogram::with_opts(
            HistogramOpts::new("livekit_room_duration_seconds", "Room lifetime in seconds.")
                .const_labels(nl.clone())
                .buckets(vec![
                    5.0, 10.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 18000.0, 36000.0,
                ]),
        )
        .expect("room duration histogram");

        let participant_total = gauge(
            "livekit_participant_total",
            "Number of active participants.",
            nl.clone(),
        );
        let participant_join = counter_vec(
            "livekit_participant_join_total",
            "Total participants that connected to the signal channel.",
            &["state", "warp"],
            nl.clone(),
        );
        let connection_total = gauge_vec(
            "livekit_connection_total",
            "Current number of signal connections by direction.",
            &["kind"],
            nl.clone(),
        );
        let session_join_latency = hist_vec(
            "livekit_session_join_latency_ms",
            "Time (ms) from signal connect to session established.",
            &prometheus::exponential_buckets(10.0, 2.0, 11).unwrap(),
            &["protocol_version"],
            nl.clone(),
        );
        let session_start_time = hist_vec(
            "livekit_session_start_time_ms",
            "Time (ms) from signal connect to first track published.",
            &prometheus::exponential_buckets(100.0, 2.0, 7).unwrap(),
            &["protocol_version", "warp"],
            nl.clone(),
        );
        let session_duration = hist_vec(
            "livekit_session_duration_ms",
            "Participant session duration (ms).",
            &prometheus::exponential_buckets(100.0, 2.0, 15).unwrap(),
            &["protocol_version"],
            nl.clone(),
        );

        let track_published = gauge_vec(
            "livekit_track_published_total",
            "Number of currently published tracks.",
            &["kind"],
            nl.clone(),
        );
        let track_subscribed = gauge_vec(
            "livekit_track_subscribed_total",
            "Number of currently subscribed tracks.",
            &["kind"],
            nl.clone(),
        );
        let track_publish_counter = counter_vec(
            "livekit_track_publish_counter_total",
            "Number of track publish attempts by kind and state.",
            &["kind", "state"],
            nl.clone(),
        );
        let track_subscribe_counter = counter_vec(
            "livekit_track_subscribe_counter_total",
            "Number of track subscribe attempts by state.",
            &["state", "error"],
            nl.clone(),
        );

        let quality_rating = Histogram::with_opts(
            HistogramOpts::new("livekit_quality_rating", "Connection quality rating.")
                .const_labels(nl.clone())
                .buckets(vec![0.0, 1.0, 2.0]),
        )
        .expect("quality rating histogram");
        let quality_score = Histogram::with_opts(
            HistogramOpts::new("livekit_quality_score", "Connection quality score (0-5).")
                .const_labels(nl.clone())
                .buckets(vec![1.0, 2.0, 2.5, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5]),
        )
        .expect("quality score histogram");

        let packet_total = counter_vec(
            "livekit_packet_total",
            "Total RTP packets forwarded by direction and transmission type.",
            &["direction", "transmission"],
            nl.clone(),
        );
        let packet_bytes = counter_vec(
            "livekit_packet_bytes",
            "Total RTP bytes forwarded by direction and transmission type.",
            &["direction", "transmission"],
            nl.clone(),
        );
        let nack_total = counter_vec(
            "livekit_nack_total",
            "Total RTCP NACKs received by direction.",
            &["direction", "country"],
            nl.clone(),
        );
        let pli_total = counter_vec(
            "livekit_pli_total",
            "Total RTCP PLIs received by direction.",
            &["direction", "country"],
            nl.clone(),
        );
        let fir_total = counter_vec(
            "livekit_fir_total",
            "Total RTCP FIRs received by direction.",
            &["direction", "country"],
            nl.clone(),
        );
        let stream_labels = &["direction", "source", "type", "country"];
        let packet_loss_percent = hist_vec(
            "livekit_packet_loss_percent",
            "Packet loss percentage by stream.",
            &[0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 5.0, 10.0, 40.0, 100.0],
            stream_labels,
            nl.clone(),
        );
        let packet_loss_total = counter_vec(
            "livekit_packet_loss_total",
            "Total packets lost by stream.",
            stream_labels,
            nl.clone(),
        );
        let packet_out_of_order_percent = hist_vec(
            "livekit_packet_out_of_order_percent",
            "Out-of-order packet percentage by stream.",
            &[0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 5.0, 10.0, 40.0, 100.0],
            stream_labels,
            nl.clone(),
        );
        let packet_out_of_order_total = counter_vec(
            "livekit_packet_out_of_order_total",
            "Total out-of-order packets by stream.",
            stream_labels,
            nl.clone(),
        );
        let jitter_us = hist_vec(
            "livekit_jitter_us",
            "Interarrival jitter in microseconds by stream.",
            &[
                1000.0, 10000.0, 30000.0, 50000.0, 70000.0, 100000.0, 300000.0, 600000.0, 1000000.0,
            ],
            stream_labels,
            nl.clone(),
        );
        let rtt_ms = hist_vec(
            "livekit_rtt_ms",
            "Round-trip time in milliseconds by stream.",
            &[
                50.0, 100.0, 150.0, 200.0, 250.0, 500.0, 750.0, 1000.0, 5000.0, 10000.0,
            ],
            stream_labels,
            nl.clone(),
        );
        let forward_latency = prometheus::Gauge::with_opts(
            Opts::new(
                "livekit_forward_latency",
                "Long-term average forwarding latency (ns).",
            )
            .const_labels(nl.clone()),
        )
        .expect("forward latency gauge");
        let forward_jitter = prometheus::Gauge::with_opts(
            Opts::new(
                "livekit_forward_jitter",
                "Long-term forwarding jitter, stddev (ns).",
            )
            .const_labels(nl.clone()),
        )
        .expect("forward jitter gauge");
        let forward_latency_ns = Histogram::with_opts(
            HistogramOpts::new(
                "livekit_forward_latency_ns",
                "Per-packet forwarding latency (ns).",
            )
            .const_labels(nl.clone())
            .buckets(vec![
                50_000.0,
                100_000.0,
                250_000.0,
                500_000.0,
                1_000_000.0,
                2_000_000.0,
                3_000_000.0,
                5_000_000.0,
                10_000_000.0,
                20_000_000.0,
            ]),
        )
        .expect("forward latency histogram");

        let m = Metrics {
            registry,
            room_total,
            room_duration,
            participant_total,
            participant_join,
            connection_total,
            session_join_latency,
            session_start_time,
            session_duration,
            track_published,
            track_subscribed,
            track_publish_counter,
            track_subscribe_counter,
            quality_rating,
            quality_score,
            packet_total,
            packet_bytes,
            nack_total,
            pli_total,
            fir_total,
            packet_loss_percent,
            packet_loss_total,
            packet_out_of_order_percent,
            packet_out_of_order_total,
            jitter_us,
            rtt_ms,
            forward_latency,
            forward_jitter,
            forward_latency_ns,
        };
        m.register_all();
        m.prime();
        m
    }

    /// Creates the expected label sets so every metric series is always
    /// exposed (even at zero), keeping dashboards populated from first boot.
    fn prime(&self) {
        self.participant_join
            .with_label_values(&["signal_connected", ""]);
        self.participant_join
            .with_label_values(&["signal_failed", ""]);
        self.participant_join
            .with_label_values(&["signal_validation_failed", ""]);
        self.participant_join
            .with_label_values(&["signal_upgrade_failed", ""]);
        self.connection_total.with_label_values(&["incoming"]);
        self.connection_total.with_label_values(&["outgoing"]);
        self.track_published.with_label_values(&["audio"]);
        self.track_subscribed.with_label_values(&["audio"]);
        self.track_publish_counter
            .with_label_values(&["audio", "started"]);
        self.track_publish_counter
            .with_label_values(&["audio", "ended"]);
        self.track_subscribe_counter
            .with_label_values(&["started", ""]);
        self.track_subscribe_counter
            .with_label_values(&["ended", ""]);
        self.session_join_latency.with_label_values(&["0"]);
        self.session_start_time.with_label_values(&["0", "false"]);
        self.session_duration.with_label_values(&["0"]);
        self.packet_total
            .with_label_values(&["incoming", "initial"]);
        self.packet_total
            .with_label_values(&["outgoing", "initial"]);
        self.packet_bytes
            .with_label_values(&["incoming", "initial"]);
        self.packet_bytes
            .with_label_values(&["outgoing", "initial"]);
        for d in ["incoming", "outgoing"] {
            self.nack_total.with_label_values(&[d, ""]);
            self.pli_total.with_label_values(&[d, ""]);
            self.fir_total.with_label_values(&[d, ""]);
            for labels in [
                &["incoming", "audio", "audio", ""],
                &["outgoing", "audio", "audio", ""],
            ] {
                self.packet_loss_percent.with_label_values(labels);
                self.packet_loss_total.with_label_values(labels);
                self.packet_out_of_order_percent.with_label_values(labels);
                self.packet_out_of_order_total.with_label_values(labels);
                self.jitter_us.with_label_values(labels);
                self.rtt_ms.with_label_values(labels);
            }
        }
    }

    fn register_all(&self) {
        macro_rules! reg {
            ($m:expr) => {
                self.registry
                    .register(Box::new($m.clone()))
                    .expect("register metric");
            };
        }
        reg!(self.room_total);
        reg!(self.room_duration);
        reg!(self.participant_total);
        reg!(self.participant_join);
        reg!(self.connection_total);
        reg!(self.session_join_latency);
        reg!(self.session_start_time);
        reg!(self.session_duration);
        reg!(self.track_published);
        reg!(self.track_subscribed);
        reg!(self.track_publish_counter);
        reg!(self.track_subscribe_counter);
        reg!(self.quality_rating);
        reg!(self.quality_score);
        reg!(self.packet_total);
        reg!(self.packet_bytes);
        reg!(self.nack_total);
        reg!(self.pli_total);
        reg!(self.fir_total);
        reg!(self.packet_loss_percent);
        reg!(self.packet_loss_total);
        reg!(self.packet_out_of_order_percent);
        reg!(self.packet_out_of_order_total);
        reg!(self.jitter_us);
        reg!(self.rtt_ms);
        reg!(self.forward_latency);
        reg!(self.forward_jitter);
        reg!(self.forward_latency_ns);
    }

    /// Renders the text/plain Prometheus exposition format.
    pub fn render(&self) -> String {
        let mut buffer = String::new();
        let encoder = TextEncoder::new();
        encoder
            .encode_utf8(&self.registry.gather(), &mut buffer)
            .expect("encode metrics");
        buffer
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new("local", "SERVER")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_drop_in_metric_names() {
        let m = Metrics::default();
        let out = m.render();
        for name in [
            "livekit_room_total",
            "livekit_room_duration_seconds",
            "livekit_participant_total",
            "livekit_participant_join_total",
            "livekit_connection_total",
            "livekit_track_published_total",
            "livekit_track_subscribed_total",
            "livekit_track_publish_counter_total",
            "livekit_track_subscribe_counter_total",
            "livekit_session_join_latency_ms",
            "livekit_session_start_time_ms",
            "livekit_session_duration_ms",
            "livekit_quality_rating",
            "livekit_quality_score",
            "livekit_packet_total",
            "livekit_packet_bytes",
            "livekit_nack_total",
            "livekit_pli_total",
            "livekit_fir_total",
            "livekit_packet_loss_percent",
            "livekit_packet_loss_total",
            "livekit_packet_out_of_order_percent",
            "livekit_packet_out_of_order_total",
            "livekit_jitter_us",
            "livekit_rtt_ms",
            "livekit_forward_latency",
            "livekit_forward_jitter",
            "livekit_forward_latency_ns",
        ] {
            assert!(
                out.contains(&format!("# TYPE {name} ")),
                "missing {name}:\n{out}"
            );
        }
    }

    #[test]
    fn quality_score_histogram_aggregates() {
        let m = Metrics::default();
        m.quality_score.observe(4.5);
        m.quality_score.observe(3.0);
        let out = m.render();
        assert!(out.contains("livekit_quality_score_count{"));
        assert!(out.contains(" 2"));
        assert!(out.contains("livekit_quality_score_sum{"));
        assert!(out.contains(" 7.5"));
    }

    #[test]
    fn room_and_participant_gauges_update() {
        let m = Metrics::default();
        m.room_total.inc();
        m.room_total.inc();
        m.participant_total.inc();
        m.room_total.dec();
        let out = m.render();
        assert!(out.contains("livekit_room_total{node_id=\"local\",node_type=\"SERVER\"} 1"));
        assert!(out.contains("livekit_participant_total{node_id=\"local\",node_type=\"SERVER\"} 1"));
    }

    #[test]
    fn node_labels_present() {
        let m = Metrics::new("node-abc", "SERVER");
        let out = m.render();
        assert!(out.contains("node_id=\"node-abc\""));
        assert!(out.contains("node_type=\"SERVER\""));
    }
}
