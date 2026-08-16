//! The media plane (SFU): WebRTC peer connections per participant, per-track
//! RTP forwarding, data channels, and negotiation.
//!
//! Voice-only: only audio (opus) is forwarded, per-subscriber down-tracks with
//! a per-subscriber SSRC, exactly like the reference server. NACK and RTCP
//! sender/receiver reports are handled by the default interceptor set.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio::sync::Mutex as AsyncMutex;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_OPUS};
use webrtc::api::{APIBuilder, API};
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::{
    RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
};
use webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection;
use webrtc::rtp_transceiver::{RTCPFeedback, RTCRtpTransceiver};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocalWriter;
use webrtc::track::track_remote::TrackRemote;

use crate::audio_level::{audio_level_ext_id_from_sdp, AudioLevelDetector};
use crate::participant::Participant;
use crate::track::{PublishedTrack, TrackSource};

pub const RELIABLE_DATA_CHANNEL: &str = "_reliable";
pub const LOSSY_DATA_CHANNEL: &str = "_lossy";
pub const DATA_TRACK_DATA_CHANNEL: &str = "_data_track";

fn opus_capability() -> RTCRtpCodecCapability {
    RTCRtpCodecCapability {
        mime_type: MIME_TYPE_OPUS.to_owned(),
        clock_rate: 48_000,
        channels: 2,
        sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
        rtcp_feedback: vec![
            RTCPFeedback {
                typ: "nack".to_owned(),
                parameter: String::new(),
            },
            RTCPFeedback {
                typ: "transport-cc".to_owned(),
                parameter: String::new(),
            },
        ],
    }
}

/// Shared WebRTC API (media engine + interceptors) used by every peer
/// connection.
pub struct RtcEngine {
    api: API,
}

impl RtcEngine {
    pub fn new() -> Self {
        let mut media_engine = MediaEngine::default();
        media_engine
            .register_codec(
                RTCRtpCodecParameters {
                    capability: opus_capability(),
                    payload_type: 111,
                    ..Default::default()
                },
                RTPCodecType::Audio,
            )
            .expect("register opus codec");
        // Negotiate the RFC 6464 audio-level extension so active-speaker
        // detection can read it from publisher RTP.
        media_engine
            .register_header_extension(
                webrtc::rtp_transceiver::rtp_codec::RTCRtpHeaderExtensionCapability {
                    uri: "urn:ietf:params:rtp-hdrext:ssrc-audio-level".to_owned(),
                },
                RTPCodecType::Audio,
                Some(RTCRtpTransceiverDirection::Sendrecv),
            )
            .expect("register audio-level extension");
        media_engine
            .register_header_extension(
                webrtc::rtp_transceiver::rtp_codec::RTCRtpHeaderExtensionCapability {
                    uri: "urn:ietf:params:rtp-hdrext:sdes:mid".to_owned(),
                },
                RTPCodecType::Audio,
                None,
            )
            .expect("register mid extension");

        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media_engine)
            .expect("register default interceptors");

        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build();
        RtcEngine { api }
    }

    async fn create_pc(
        &self,
        ice_servers: Vec<RTCIceServer>,
    ) -> Result<Arc<RTCPeerConnection>, String> {
        let config = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };
        self.api
            .new_peer_connection(config)
            .await
            .map(Arc::new)
            .map_err(|e| format!("create peer connection: {e}"))
    }
}

impl Default for RtcEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-participant media state.
#[derive(Default)]
pub struct ParticipantMedia {
    pub publisher: Option<Arc<RTCPeerConnection>>,
    pub subscriber: Option<Arc<RTCPeerConnection>>,
    /// sid -> receiver forwarder for tracks THIS participant published.
    pub forwarders: HashMap<String, Arc<Forwarder>>,
    /// sid -> local down-track for tracks THIS participant subscribes to.
    pub subscriber_tracks: HashMap<String, Arc<TrackLocalStaticRTP>>,
    /// sid -> the RTPSender attached to the subscriber PC (for removal).
    pub subscriber_senders: HashMap<String, Arc<webrtc::rtp_transceiver::rtp_sender::RTCRtpSender>>,
    /// mids from the last publisher offer, mapping SDP mid -> client cid.
    pub publisher_mids: std::collections::BTreeMap<String, String>,
    /// Audio-level extension id from the last publisher offer.
    pub publisher_audio_level_ext: Option<u8>,
    pub reliable: Option<Arc<RTCDataChannel>>,
    pub lossy: Option<Arc<RTCDataChannel>>,
    /// ICE candidates received before their peer connection was created.
    pub pending_candidates:
        HashMap<i32, Vec<webrtc::ice_transport::ice_candidate::RTCIceCandidateInit>>,
    pub negotiating: Arc<AsyncMutex<()>>,
    pub answer_pending: AtomicBool,
    pub needs_negotiation: AtomicBool,
}

/// Receiver side of a published audio track. Forwards every RTP packet to all
/// subscribers (selective forwarding) and tracks the audio level.
pub struct Forwarder {
    pub track_sid: String,
    pub publisher_sid: String,
    pub audio: AudioLevelDetector,
    pub ext_id: Option<u8>,
    pub subscribers: Mutex<HashMap<String, Arc<TrackLocalStaticRTP>>>,
    pub closed: AtomicBool,
    /// Per-track RTP statistics used to derive connection quality.
    pub stats: Mutex<RtpStats>,
    pub metrics: Arc<crate::metrics::Metrics>,
    /// Track source string for the `source` metric label.
    pub track_source: String,
    /// Latest publisher SenderReport (rtp_time, NTP wall-clock in unix ns),
    /// used to derive per-packet forwarding latency.
    pub sender_report: Mutex<Option<(u32, i64)>>,
    /// Running forwarding-jitter estimate (RFC 3550) over the latency series.
    pub forward_jitter: std::sync::Mutex<f64>,
    pub last_forward_latency: std::sync::Mutex<Option<f64>>,
}

/// Running RTP loss + jitter estimates for a published track. Loss is derived
/// from sequence gaps; jitter uses the RFC 3550 interarrival estimator.
#[derive(Default)]
pub struct RtpStats {
    pub packets: u64,
    pub expected: u64,
    pub lost: u64,
    pub out_of_order: u64,
    pub jitter_ts: f64,
    max_seq: Option<u16>,
    last_rtp_ts: Option<u32>,
    last_arrival_ms: Option<i64>,
}

impl RtpStats {
    /// Feeds one RTP packet into the estimator (RFC 3550 interarrival jitter,
    /// loss + out-of-order from sequence tracking against the high-water
    /// mark).
    fn observe(&mut self, seq: u16, rtp_ts: u32, arrival_ms: i64) {
        self.packets += 1;
        match self.max_seq {
            None => {
                self.max_seq = Some(seq);
                self.expected = 1;
            }
            Some(max) => {
                let behind = max.wrapping_sub(seq);
                if behind > 0 && behind < 32768 {
                    // Behind the high-water mark: reordered. Fills one
                    // previously-missing slot, so it is not counted as loss.
                    self.out_of_order += 1;
                    self.expected += 1;
                    if self.lost > 0 {
                        self.lost -= 1;
                    }
                } else {
                    let gap = seq.wrapping_sub(max);
                    if gap > 0 && gap < 32768 {
                        self.expected += gap as u64;
                        self.lost += (gap - 1) as u64;
                        self.max_seq = Some(seq);
                    } else {
                        self.expected += 1; // duplicate
                    }
                }
            }
        }
        if let (Some(prev_ts), Some(prev_arr)) = (self.last_rtp_ts, self.last_arrival_ms) {
            // Opus runs at 48 kHz; expected inter-arrival delta = elapsed ms * 48.
            let d = (rtp_ts.wrapping_sub(prev_ts) as i64) - (arrival_ms - prev_arr) * 48;
            self.jitter_ts += (d.abs() as f64 - self.jitter_ts) / 16.0;
        }
        self.last_rtp_ts = Some(rtp_ts);
        self.last_arrival_ms = Some(arrival_ms);
    }

    /// Loss percentage since tracking began.
    fn loss_percent(&self) -> f64 {
        if self.expected > 0 {
            self.lost as f64 / self.expected as f64 * 100.0
        } else {
            0.0
        }
    }

    /// Out-of-order percentage since tracking began.
    fn out_of_order_percent(&self) -> f64 {
        if self.expected > 0 {
            self.out_of_order as f64 / self.expected as f64 * 100.0
        } else {
            0.0
        }
    }

    /// Interarrival jitter in microseconds (48 kHz clock).
    fn jitter_us(&self) -> f64 {
        self.jitter_ts / 48.0 * 1_000_000.0
    }
}

impl Forwarder {
    pub fn add_subscriber(&self, sid: &str, track: Arc<TrackLocalStaticRTP>) {
        self.subscribers
            .lock()
            .unwrap()
            .insert(sid.to_string(), track);
    }

    pub fn remove_subscriber(&self, sid: &str) {
        self.subscribers.lock().unwrap().remove(sid);
    }

    pub fn num_subscribers(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }

    /// Feeds one forwarded RTP packet into the loss/jitter estimator.
    fn observe_rtp(&self, seq: u16, rtp_ts: u32, arrival_ms: i64) {
        self.stats.lock().unwrap().observe(seq, rtp_ts, arrival_ms);
    }

    /// Records the current loss/jitter into the quality and per-stream
    /// histograms. Called periodically from the forwarding loop.
    fn record_quality(&self) {
        let (loss_pct, ooo_pct, jitter_us) = {
            let st = self.stats.lock().unwrap();
            (st.loss_percent(), st.out_of_order_percent(), st.jitter_us())
        };
        let score = quality_score(loss_pct, jitter_us / 1000.0);
        self.metrics.quality_score.observe(score);
        self.metrics
            .quality_rating
            .observe(quality_rating(score) as f64);

        let labels = &["incoming", self.track_source.as_str(), "audio", ""];
        if loss_pct > 0.0 {
            self.metrics
                .packet_loss_percent
                .with_label_values(labels)
                .observe(loss_pct);
        }
        if ooo_pct > 0.0 {
            self.metrics
                .packet_out_of_order_percent
                .with_label_values(labels)
                .observe(ooo_pct);
        }
        if jitter_us > 0.0 {
            self.metrics
                .jitter_us
                .with_label_values(labels)
                .observe(jitter_us);
        }
    }

    /// Updates the publisher SenderReport mapping used for forward latency.
    fn set_sender_report(&self, rtp_time: u32, ntp_unix_ns: i64) {
        *self.sender_report.lock().unwrap() = Some((rtp_time, ntp_unix_ns));
    }

    /// Records the forwarding latency (ns) for one packet: updates the
    /// per-sample histogram, the long-term gauges, and the jitter estimate.
    fn observe_forward_latency(&self, latency_ns: f64) {
        self.metrics.forward_latency_ns.observe(latency_ns.max(0.0));
        let mut jitter = self.forward_jitter.lock().unwrap();
        let mut last = self.last_forward_latency.lock().unwrap();
        if let Some(prev) = *last {
            let d = (latency_ns - prev).abs();
            *jitter += (d - *jitter) / 16.0;
        }
        *last = Some(latency_ns);
        let j = *jitter;
        self.metrics.forward_latency.set(latency_ns.max(0.0));
        self.metrics.forward_jitter.set(j);
    }
}

/// Maps packet loss % and jitter (ms) to a 0-5 connection quality score,
/// mirroring the reference server's MOS-like model.
fn quality_score(loss_pct: f64, jitter_ms: f64) -> f64 {
    (5.0 - 8.0 * (loss_pct / 100.0) - jitter_ms / 50.0).clamp(0.0, 5.0)
}

/// Maps a score to the `ConnectionQuality` enum int (EXCELLENT=0, GOOD=1,
/// POOR=2, LOST=3).
fn quality_rating(score: f64) -> i32 {
    if score >= 4.0 {
        0
    } else if score >= 3.0 {
        1
    } else if score >= 1.5 {
        2
    } else {
        3
    }
}

/// Converts an RTCP NTP timestamp (1900 epoch, 64-bit fixed point) to unix ns.
fn ntp_to_unix_ns(ntp: u64) -> i64 {
    let seconds = (ntp >> 32) as i64;
    let fraction = (ntp & 0xffff_ffff) as i64;
    let unix_secs = seconds - 2_208_988_800; // NTP 1900 -> unix 1970
    unix_secs * 1_000_000_000 + fraction * 1_000_000_000 / (1 << 32)
}

/// Round-trip time (ms) from an RTCP receiver report's LSR + DLSR fields.
fn rtt_ms_from_report(lsr: u32, dlsr: u32) -> f64 {
    let now_sec32 = (crate::core::unix_seconds() + 2_208_988_800) & 0xffff_ffff;
    let secs = (now_sec32 - lsr as i64) as f64 - dlsr as f64 / 65536.0;
    (secs * 1000.0).max(0.0)
}

/// Reads RTCP from the publisher's receiver: SenderReports feed the forward
/// latency mapping.
fn spawn_publisher_rtcp_reader(
    receiver: Arc<webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver>,
    forwarder: Arc<Forwarder>,
) {
    tokio::spawn(async move {
        while let Ok((packets, _)) = receiver.read_rtcp().await {
            for p in packets {
                let Some(sr) = p
                    .as_any()
                    .downcast_ref::<webrtc::rtcp::sender_report::SenderReport>()
                else {
                    continue;
                };
                forwarder.set_sender_report(sr.rtp_time, ntp_to_unix_ns(sr.ntp_time));
            }
        }
    });
}

/// Reads RTCP feedback (NACK/PLI/FIR) and receiver reports from a subscriber:
/// feeds the outgoing-direction RTCP and stream metrics.
fn spawn_subscriber_rtcp_reader(
    sender: Arc<webrtc::rtp_transceiver::rtp_sender::RTCRtpSender>,
    metrics: Arc<crate::metrics::Metrics>,
    source: &str,
) {
    let source = source.to_string();
    tokio::spawn(async move {
        while let Ok((packets, _)) = sender.read_rtcp().await {
            for p in packets {
                let any = p.as_any();
                if let Some(nack) = any.downcast_ref::<
                    webrtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack,
                >() {
                    metrics
                        .nack_total
                        .with_label_values(&["outgoing", ""])
                        .inc_by(nack.nacks.len() as u64);
                } else if any
                    .downcast_ref::<
                        webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
                    >()
                    .is_some()
                {
                    metrics
                        .pli_total
                        .with_label_values(&["outgoing", ""])
                        .inc();
                } else if any
                    .downcast_ref::<
                        webrtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest,
                    >()
                    .is_some()
                {
                    metrics
                        .fir_total
                        .with_label_values(&["outgoing", ""])
                        .inc();
                } else if let Some(rr) = any
                    .downcast_ref::<webrtc::rtcp::receiver_report::ReceiverReport>()
                {
                    for rep in &rr.reports {
                        let labels = &["outgoing", source.as_str(), "audio", ""];
                        metrics
                            .packet_loss_percent
                            .with_label_values(labels)
                            .observe(rep.fraction_lost as f64 / 256.0 * 100.0);
                        metrics
                            .jitter_us
                            .with_label_values(labels)
                            .observe(rep.jitter as f64 * 1_000_000.0 / 48000.0);
                        if rep.last_sender_report != 0 {
                            metrics
                                .rtt_ms
                                .with_label_values(labels)
                                .observe(rtt_ms_from_report(rep.last_sender_report, rep.delay));
                        }
                    }
                }
            }
        }
    });
}

/// Returns the RTCIceServers advertised to clients (currently none; TURN is
/// not embedded, host candidates are used).
pub fn ice_servers(_config: &crate::config::Config) -> Vec<RTCIceServer> {
    vec![]
}

pub async fn setup_subscriber(participant: &Arc<Participant>) -> Result<(), String> {
    let ctx = participant
        .room()
        .and_then(|r| r.context())
        .ok_or("participant has no room context")?;

    {
        let media = participant.media.lock().unwrap();
        if media.subscriber.is_some() {
            return Ok(());
        }
    }
    let pending = participant
        .media
        .lock()
        .unwrap()
        .pending_candidates
        .remove(&(lk::SignalTarget::Subscriber as i32))
        .unwrap_or_default();
    let pc = ctx
        .rtc
        .create_pc(ice_servers(&ctx.config))
        .await
        .map_err(|e| format!("subscriber pc: {e}"))?;

    // ICE candidates -> subscriber trickle.
    let p = Arc::downgrade(participant);
    let pc_ref = pc.clone();
    pc.on_ice_candidate(Box::new(
        move |candidate: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let p = p.clone();
            let pc = pc_ref.clone();
            Box::pin(async move {
                if let Some(c) = candidate {
                    let Some(p) = p.upgrade() else { return };
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
                            let _ = p
                                .send(lk::SignalResponse {
                                    message: Some(lk::signal_response::Message::Trickle(
                                        lk::TrickleRequest {
                                            candidate_init: json,
                                            target: lk::SignalTarget::Subscriber as i32,
                                            r#final: false,
                                        },
                                    )),
                                })
                                .await;
                        }
                    }
                }
                let _ = pc;
            })
        },
    ));

    // Data channels created on the subscriber PC (server-initiated).
    let ordered = true;
    let reliable = pc
        .create_data_channel(
            RELIABLE_DATA_CHANNEL,
            Some(RTCDataChannelInit {
                ordered: Some(ordered),
                ..Default::default()
            }),
        )
        .await
        .map_err(|e| format!("create reliable data channel: {e}"))?;
    let lossy = pc
        .create_data_channel(
            LOSSY_DATA_CHANNEL,
            Some(RTCDataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                ..Default::default()
            }),
        )
        .await
        .map_err(|e| format!("create lossy data channel: {e}"))?;

    let data_track = pc
        .create_data_channel(
            DATA_TRACK_DATA_CHANNEL,
            Some(RTCDataChannelInit {
                ordered: Some(false),
                max_retransmits: Some(0),
                ..Default::default()
            }),
        )
        .await
        .map_err(|e| format!("create data-track channel: {e}"))?;

    let p2 = Arc::downgrade(participant);
    reliable.on_message(Box::new(move |data| {
        let p = p2.clone();
        Box::pin(async move {
            if let Some(p) = p.upgrade() {
                crate::signal::handle_incoming_data(&p, &data.data).await;
            }
        })
    }));
    let p3 = Arc::downgrade(participant);
    lossy.on_message(Box::new(move |data| {
        let p = p3.clone();
        Box::pin(async move {
            if let Some(p) = p.upgrade() {
                crate::signal::handle_incoming_data(&p, &data.data).await;
            }
        })
    }));
    let p6 = Arc::downgrade(participant);
    data_track.on_message(Box::new(move |data| {
        let p = p6.clone();
        Box::pin(async move {
            if let Some(p) = p.upgrade() {
                crate::signal::handle_incoming_data(&p, &data.data).await;
            }
        })
    }));

    // Track publisher connection state so we can tear down cleanly.
    let p4 = Arc::downgrade(participant);
    pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        let p = p4.clone();
        Box::pin(async move {
            if matches!(
                s,
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
            ) {
                if let Some(p) = p.upgrade() {
                    crate::signal::on_media_disconnected(&p).await;
                }
            }
        })
    }));

    {
        let mut media = participant.media.lock().unwrap();
        media.subscriber = Some(pc.clone());
        media.reliable = Some(reliable);
        media.lossy = Some(lossy);
        let _ = data_track;
    }
    // Flush any candidates that arrived before the PC was created.
    for init in pending {
        let _ = pc.add_ice_candidate(init).await;
    }

    Ok(())
}

/// Creates the publisher peer connection and wires its handlers.
pub async fn ensure_publisher(
    participant: &Arc<Participant>,
) -> Result<Arc<RTCPeerConnection>, String> {
    let ctx = participant
        .room()
        .and_then(|r| r.context())
        .ok_or("participant has no room context")?;
    {
        let media = participant.media.lock().unwrap();
        if let Some(pc) = &media.publisher {
            return Ok(pc.clone());
        }
    }
    let pending = participant
        .media
        .lock()
        .unwrap()
        .pending_candidates
        .remove(&(lk::SignalTarget::Publisher as i32))
        .unwrap_or_default();
    let pc = ctx
        .rtc
        .create_pc(ice_servers(&ctx.config))
        .await
        .map_err(|e| format!("publisher pc: {e}"))?;
    for init in pending {
        let _ = pc.add_ice_candidate(init).await;
    }
    // Pre-create an audio receiver transceiver so incoming RTP is routed to it
    // (matches the reference SFU; on_track only fires once a receiver exists).
    pc.add_transceiver_from_kind(RTPCodecType::Audio, None)
        .await
        .map_err(|e| format!("add audio transceiver: {e}"))?;

    let p = Arc::downgrade(participant);
    let pc_ref = pc.clone();
    pc.on_ice_candidate(Box::new(
        move |candidate: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let p = p.clone();
            let _ = pc_ref.clone();
            Box::pin(async move {
                if let Some(c) = candidate {
                    let Some(p) = p.upgrade() else { return };
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
                            let _ = p
                                .send(lk::SignalResponse {
                                    message: Some(lk::signal_response::Message::Trickle(
                                        lk::TrickleRequest {
                                            candidate_init: json,
                                            target: lk::SignalTarget::Publisher as i32,
                                            r#final: false,
                                        },
                                    )),
                                })
                                .await;
                        }
                    }
                }
            })
        },
    ));

    // Published tracks arrive here. LiveKit clients set the RTP stream id to the
    // client track id (cid), so we match on that first; fall back to the offer's
    // mid->cid map for clients that only provide midToTrackId.
    let p2 = Arc::downgrade(participant);
    let p_data = Arc::downgrade(participant);
    pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
        let p = p_data.clone();
        Box::pin(async move {
            let p2 = p.clone();
            dc.on_message(Box::new(move |data| {
                let p = p2.clone();
                Box::pin(async move {
                    if let Some(p) = p.upgrade() {
                        crate::signal::handle_incoming_data(&p, &data.data).await;
                    }
                })
            }));
        })
    }));
    pc.on_track(Box::new(move |track_remote: Arc<TrackRemote>, receiver: Arc<webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver>, transceiver: Arc<RTCRtpTransceiver>| {
        let p = p2.clone();
        Box::pin(async move {
            let Some(p) = p.upgrade() else { return };
            if track_remote.kind() != RTPCodecType::Audio {
                tracing::warn!(sid = %p.sid, "received non-audio track; ignoring (voice-only)");
                return;
            }
            let mid = transceiver.mid().map(|m| m.to_string());
            tracing::debug!(sid = %p.sid, "on_track fired, stream_id={}", track_remote.stream_id());
            let stream_cid = track_remote.stream_id();
            let cid = if !stream_cid.is_empty() {
                Some(stream_cid)
            } else {
                mid.as_ref()
                    .and_then(|m| p.media.lock().unwrap().publisher_mids.get(m).cloned())
            };
            let track = match cid {
                Some(cid) => {
                    if let Some(t) = p.get_track_by_cid(&cid) {
                        t
                    } else {
                        tracing::warn!(sid = %p.sid, cid = %cid, "incoming track has unknown cid; creating");
                        let t = Arc::new(PublishedTrack::new(
                            "audio".to_string(),
                            cid,
                            TrackSource::Unknown,
                            String::new(),
                        ));
                        p.add_track(t.clone());
                        t
                    }
                }
                None => {
                    tracing::warn!(sid = %p.sid, "incoming track has no mid mapping; creating");
                    let t = Arc::new(PublishedTrack::new(
                        "audio".to_string(),
                        String::new(),
                        TrackSource::Unknown,
                        String::new(),
                    ));
                    p.add_track(t.clone());
                    t
                }
            };
            if let Some(m) = mid {
                track.set_mid(Some(m));
            }
            track.set_mime("audio/opus".to_string());
            p.is_publisher.store(true, Ordering::Relaxed);

            let ext_id = { p.media.lock().unwrap().publisher_audio_level_ext };
            let metrics = p
                .room()
                .and_then(|r| r.context())
                .map(|c| c.metrics.clone())
                .unwrap_or_default();
            let forwarder = Arc::new(Forwarder {
                track_sid: track.sid.clone(),
                publisher_sid: p.sid.clone(),
                audio: AudioLevelDetector::new(),
                ext_id,
                subscribers: Mutex::new(HashMap::new()),
                closed: AtomicBool::new(false),
                stats: Mutex::new(RtpStats::default()),
                metrics,
                track_source: track.source.source_str().to_string(),
                sender_report: Mutex::new(None),
                forward_jitter: std::sync::Mutex::new(0.0),
                last_forward_latency: std::sync::Mutex::new(None),
            });
            p.media.lock().unwrap().forwarders.insert(track.sid.clone(), forwarder.clone());
            spawn_publisher_rtcp_reader(receiver, forwarder.clone());

            crate::signal::on_track_published(&p, &track).await;

            // Spawn the RTP forwarding loop.
            let fwd = forwarder.clone();
            let p_loop = Arc::downgrade(&p);
            let remote_track = track_remote.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                while let Ok((mut pkt, _attrs)) = remote_track.read(&mut buf).await {
                    let Some(p_loop) = p_loop.upgrade() else { break };
                    if p_loop.state() == crate::participant::ParticipantState::Disconnected {
                        break;
                    }
                    // Extract audio level before stripping extensions.
                    if let Some(ext_id) = fwd.ext_id {
                        for ext in &pkt.header.extensions {
                            if ext.id == ext_id && !ext.payload.is_empty() {
                                fwd.audio.observe(ext.payload[0] & 0x7f);
                                break;
                            }
                        }
                    }
                    // Strip header extensions for subscribers (each subscriber
                    // demuxes by its own SSRC).
                    pkt.header.extension = false;
                    pkt.header.extensions.clear();
                    pkt.header.extensions_padding = 0;

                    let size = pkt.payload.len() as u64 + 12;
                    fwd.observe_rtp(
                        pkt.header.sequence_number,
                        pkt.header.timestamp,
                        crate::core::unix_millis(),
                    );
                    // Forwarding latency: how long ago the publisher sent this
                    // packet, from the latest SenderReport RTP<->NTP mapping.
                    if let Some((sr_rtp, sr_ntp_ns)) = *fwd.sender_report.lock().unwrap() {
                        let ts_delta_ns =
                            (pkt.header.timestamp.wrapping_sub(sr_rtp) as f64) / 48000.0 * 1e9;
                        let latency_ns = crate::core::unix_nanos() as f64 - (sr_ntp_ns as f64 + ts_delta_ns);
                        fwd.observe_forward_latency(latency_ns);
                    }
                    if fwd.stats.lock().unwrap().packets % 100 == 0 {
                        fwd.record_quality();
                    }
                    fwd.metrics
                        .packet_total
                        .with_label_values(&["incoming", "initial"])
                        .inc();
                    fwd.metrics
                        .packet_bytes
                        .with_label_values(&["incoming", "initial"])
                        .inc_by(size);

                    let subs: Vec<Arc<TrackLocalStaticRTP>> = {
                        fwd.subscribers.lock().unwrap().values().cloned().collect()
                    };
                    if !subs.is_empty() {
                        fwd.metrics
                            .packet_total
                            .with_label_values(&["outgoing", "initial"])
                            .inc_by(subs.len() as u64);
                        fwd.metrics
                            .packet_bytes
                            .with_label_values(&["outgoing", "initial"])
                            .inc_by(size * subs.len() as u64);
                    }
                    for sub in subs {
                        if let Err(e) = sub.write_rtp(&pkt).await {
                            tracing::debug!(track = %fwd.track_sid, "write rtp to subscriber: {e}");
                        }
                    }
                }
                fwd.closed.store(true, Ordering::Relaxed);
                if let Some(p_loop) = p_loop.upgrade() {
                    p_loop
                        .media
                        .lock()
                        .unwrap()
                        .forwarders
                        .remove(&fwd.track_sid);
                    crate::signal::on_track_unpublished(&p_loop, &fwd.track_sid).await;
                }
            });
        })
    }));

    let p5 = participant.clone();
    pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        let p = p5.clone();
        tracing::debug!(sid = %p.sid, "SERVER publisher pc state: {s:?}");
        Box::pin(async move {
            if matches!(
                s,
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
            ) {
                crate::signal::on_media_disconnected(&p).await;
            }
        })
    }));
    let p6 = participant.clone();
    pc.on_ice_connection_state_change(Box::new(
        move |s: webrtc::ice_transport::ice_connection_state::RTCIceConnectionState| {
            tracing::debug!(sid = %p6.sid, "SERVER publisher ice: {s:?}");
            if s == webrtc::ice_transport::ice_connection_state::RTCIceConnectionState::Connected
                && p6.set_state(crate::participant::ParticipantState::Active)
            {
                // First time the media plane is up: record session join latency.
                if let Some(ctx) = p6.room().and_then(|r| r.context()) {
                    ctx.metrics
                        .session_join_latency
                        .with_label_values(&["0"])
                        .observe(p6.session_age_ms() as f64);
                }
            }
            Box::pin(async {})
        },
    ));

    participant.media.lock().unwrap().publisher = Some(pc.clone());
    Ok(pc)
}

/// Handles a publisher offer: sets the remote description, creates an answer.
pub async fn handle_publisher_offer(
    participant: &Arc<Participant>,
    offer: &lk::SessionDescription,
) -> Result<lk::SessionDescription, String> {
    let pc = ensure_publisher(participant).await?;
    {
        let mut media = participant.media.lock().unwrap();
        media.publisher_mids = offer.mid_to_track_id.clone();
        media.publisher_audio_level_ext = audio_level_ext_id_from_sdp(&offer.sdp);
    }
    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Offer;
    sd.sdp = offer.sdp.clone();
    pc.set_remote_description(sd)
        .await
        .map_err(|e| format!("set publisher remote description: {e}"))?;
    let answer = pc
        .create_answer(None)
        .await
        .map_err(|e| format!("create publisher answer: {e}"))?;
    pc.set_local_description(answer.clone())
        .await
        .map_err(|e| format!("set publisher local description: {e}"))?;
    Ok(lk::SessionDescription {
        r#type: "answer".to_string(),
        sdp: answer.sdp.clone(),
        id: offer.id,
        mid_to_track_id: Default::default(),
    })
}

/// Handles a client answer for the subscriber PC.
pub async fn handle_subscriber_answer(
    participant: &Arc<Participant>,
    answer: &lk::SessionDescription,
) -> Result<(), String> {
    let pc = {
        let media = participant.media.lock().unwrap();
        media.subscriber.clone().ok_or("no subscriber pc")?
    };
    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Answer;
    sd.sdp = answer.sdp.clone();
    if let Err(e) = pc.set_remote_description(sd).await {
        tracing::warn!(sid = %participant.sid, "set subscriber remote description failed: {e}");
        return Err(format!("set subscriber remote description: {e}"));
    }
    {
        let m = participant.media.lock().unwrap();
        m.answer_pending.store(false, Ordering::Relaxed);
        if m.needs_negotiation.swap(false, Ordering::Relaxed) {
            // Further subscriptions arrived while negotiating; send another offer.
            tokio::spawn(subscribe_negotiation(participant.clone(), true));
        }
    }
    Ok(())
}

/// Adds a subscription: creates a per-subscriber down-track and requests a
/// subscriber renegotiation.
pub async fn add_subscription(
    subscriber: &Arc<Participant>,
    track: &Arc<PublishedTrack>,
    publisher: &Arc<Participant>,
) -> Result<(), String> {
    let ctx = subscriber
        .room()
        .and_then(|r| r.context())
        .ok_or("no room context")?;
    setup_subscriber(subscriber).await?;

    tracing::debug!(subscriber = %subscriber.sid, track = %track.sid, "add_subscription");
    let forwarder = publisher
        .media
        .lock()
        .unwrap()
        .forwarders
        .get(&track.sid)
        .cloned();
    let Some(forwarder) = forwarder else {
        tracing::debug!(subscriber = %subscriber.sid, track = %track.sid, "track has no forwarder");
        return Err(format!("track {} has no forwarder yet", track.sid));
    };

    let track_local = Arc::new(TrackLocalStaticRTP::new(
        opus_capability(),
        track.sid.clone(),
        format!("TR{}_{}", publisher.sid, track.sid),
    ));

    let pc = {
        let media = subscriber.media.lock().unwrap();
        media.subscriber.clone().ok_or("no subscriber pc")?
    };
    let sender = pc
        .add_track(track_local.clone())
        .await
        .map_err(|e| format!("add subscriber track: {e}"))?;

    {
        let mut media = subscriber.media.lock().unwrap();
        media
            .subscriber_tracks
            .insert(track.sid.clone(), track_local.clone());
        media
            .subscriber_senders
            .insert(track.sid.clone(), sender.clone());
    }
    forwarder.add_subscriber(&subscriber.sid, track_local);
    if let Some(room) = subscriber.room() {
        if let Some(ctx) = room.context() {
            ctx.metrics
                .track_subscribed
                .with_label_values(&["audio"])
                .inc();
            ctx.metrics
                .track_subscribe_counter
                .with_label_values(&["started", ""])
                .inc();
        }
    }
    // Read RTCP feedback from this subscriber (NACK/PLI/FIR + RR) for metrics.
    spawn_subscriber_rtcp_reader(
        sender.clone(),
        ctx.metrics.clone(),
        track.source.source_str(),
    );
    let _ = ctx;

    request_subscriber_negotiation(subscriber);
    Ok(())
}

/// Removes a subscription and requests renegotiation.
pub async fn remove_subscription(
    subscriber: &Arc<Participant>,
    track_sid: &str,
) -> Result<(), String> {
    let (sender, pc) = {
        let mut media = subscriber.media.lock().unwrap();
        media.subscriber_tracks.remove(track_sid);
        let sender = media.subscriber_senders.remove(track_sid);
        (sender, media.subscriber.clone())
    };
    if let Some(room) = subscriber.room() {
        for p in room.participants() {
            let f = p.media.lock().unwrap().forwarders.get(track_sid).cloned();
            if let Some(f) = f {
                f.remove_subscriber(&subscriber.sid);
            }
        }
    }
    if let (Some(sender), Some(pc)) = (sender, pc) {
        let _ = pc.remove_track(&sender).await;
    }
    if let Some(room) = subscriber.room() {
        if let Some(ctx) = room.context() {
            ctx.metrics
                .track_subscribed
                .with_label_values(&["audio"])
                .dec();
            ctx.metrics
                .track_subscribe_counter
                .with_label_values(&["ended", ""])
                .inc();
        }
    }
    request_subscriber_negotiation(subscriber);
    Ok(())
}

pub fn request_subscriber_negotiation(participant: &Arc<Participant>) {
    participant
        .media
        .lock()
        .unwrap()
        .needs_negotiation
        .store(true, Ordering::Relaxed);
    let p = participant.clone();
    tokio::spawn(subscribe_negotiation(p, false));
}

async fn subscribe_negotiation(participant: Arc<Participant>, force: bool) {
    // Debounce rapid changes before offering.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let lock = participant.media.lock().unwrap().negotiating.clone();
    let _guard = lock.lock().await;
    if participant.state() == crate::participant::ParticipantState::Disconnected {
        tracing::debug!(sid = %participant.sid, "negotiation: participant disconnected");
        return;
    }
    {
        let media = participant.media.lock().unwrap();
        if media.answer_pending.load(Ordering::Relaxed) {
            tracing::debug!(sid = %participant.sid, "negotiation: answer pending");
            media.needs_negotiation.store(true, Ordering::Relaxed);
            return;
        }
        if !force && !media.needs_negotiation.swap(false, Ordering::Relaxed) {
            tracing::debug!(sid = %participant.sid, "negotiation: nothing to do");
            return;
        }
        if force {
            media.needs_negotiation.store(false, Ordering::Relaxed);
        }
    }
    tracing::debug!(sid = %participant.sid, "subscriber negotiation proceeding");
    let pc = {
        let media = participant.media.lock().unwrap();
        media.subscriber.clone()
    };
    let Some(pc) = pc else { return };
    match pc.create_offer(None).await {
        Ok(offer) => {
            if let Err(e) = pc.set_local_description(offer.clone()).await {
                tracing::warn!(sid = %participant.sid, "set subscriber local description: {e}");
                return;
            }
            participant
                .media
                .lock()
                .unwrap()
                .answer_pending
                .store(true, Ordering::Relaxed);
            let resp = lk::SignalResponse {
                message: Some(lk::signal_response::Message::Offer(
                    lk::SessionDescription {
                        r#type: "offer".to_string(),
                        sdp: offer.sdp,
                        id: 0,
                        mid_to_track_id: Default::default(),
                    },
                )),
            };
            let _ = participant.send(resp).await;
        }
        Err(e) => {
            tracing::warn!(sid = %participant.sid, "create subscriber offer: {e}");
        }
    }
}

/// Adds an ICE candidate to the target peer connection.
pub async fn add_ice_candidate(
    participant: &Arc<Participant>,
    candidate: &str,
    target: i32,
) -> Result<(), String> {
    let pc = {
        let media = participant.media.lock().unwrap();
        match target {
            0 => media.publisher.clone(),
            _ => media.subscriber.clone(),
        }
    };
    // Clients send the candidate as a JSON-encoded RTCIceCandidateInit.
    let init: webrtc::ice_transport::ice_candidate::RTCIceCandidateInit =
        serde_json::from_str(candidate)
            .map_err(|_| format!("invalid ice candidate json: {candidate}"))?;
    let Some(pc) = pc else {
        // Buffer candidates that arrive before the peer connection exists.
        participant
            .media
            .lock()
            .unwrap()
            .pending_candidates
            .entry(target)
            .or_default()
            .push(init);
        return Ok(());
    };
    pc.add_ice_candidate(init)
        .await
        .map_err(|e| format!("add ice candidate: {e}"))
}

/// Sends a data packet to a participant over the reliable/lossy channels.
#[allow(deprecated)]
pub fn send_data_to_participant(participant: &Arc<Participant>, packet: &lk::DataPacket) {
    let bytes = packet.encode_to_vec();
    let kind =
        lk::data_packet::Kind::try_from(packet.kind).unwrap_or(lk::data_packet::Kind::Reliable);
    let channel = {
        let media = participant.media.lock().unwrap();
        match kind {
            lk::data_packet::Kind::Reliable => media.reliable.clone(),
            lk::data_packet::Kind::Lossy => media.lossy.clone(),
        }
    };
    if let Some(channel) = channel {
        let data = bytes::Bytes::from(bytes);
        tokio::spawn(async move {
            let _ = channel.send(&data).await;
        });
    }
}

/// Tears down all peer connections for a participant.
pub async fn close_participant_media(participant: &Arc<Participant>) {
    let (publisher, subscriber) = {
        let mut media = participant.media.lock().unwrap();
        let p = media.publisher.take();
        let s = media.subscriber.take();
        media.forwarders.clear();
        media.subscriber_tracks.clear();
        media.subscriber_senders.clear();
        (p, s)
    };
    // Release this participant from any forwarders it subscribed to, so the
    // per-subscriber down-track Arcs drop (avoiding retention cycles).
    if let Some(room) = participant.room() {
        for other in room.participants() {
            let fwd = other.media.lock().unwrap().forwarders.clone();
            for f in fwd.values() {
                f.remove_subscriber(&participant.sid);
            }
        }
    }

    if let Some(pc) = publisher {
        let _ = pc.close().await;
    }
    if let Some(pc) = subscriber {
        let _ = pc.close().await;
    }
}

/// Returns a list of current speakers (sid, level 0..1, active) for a room,
/// computed from the audio-level detectors of published tracks.
pub fn active_speakers(participants: &[Arc<Participant>]) -> Vec<lk::SpeakerInfo> {
    let mut speakers = Vec::new();
    for p in participants {
        let forwarders = {
            p.media
                .lock()
                .unwrap()
                .forwarders
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };
        for f in forwarders {
            if f.audio.is_active() {
                speakers.push(lk::SpeakerInfo {
                    sid: p.sid.clone(),
                    level: f.audio.level(),
                    active: true,
                });
                break; // one entry per participant
            }
        }
    }
    speakers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_score_maps_loss_and_jitter() {
        assert_eq!(quality_score(0.0, 0.0), 5.0);
        assert_eq!(quality_score(0.0, 50.0), 4.0);
        assert_eq!(quality_score(10.0, 0.0), 4.2);
        // Never below 0.
        assert_eq!(quality_score(100.0, 1000.0), 0.0);
    }

    #[test]
    fn quality_rating_buckets() {
        assert_eq!(quality_rating(4.5), 0); // EXCELLENT
        assert_eq!(quality_rating(3.5), 1); // GOOD
        assert_eq!(quality_rating(2.0), 2); // POOR
        assert_eq!(quality_rating(1.0), 3); // LOST
    }

    #[test]
    fn rtp_stats_counts_sequence_gaps() {
        let mut st = RtpStats::default();
        for seq in 0u16..=99 {
            st.observe(seq, seq as u32, seq as i64);
        }
        assert_eq!(st.packets, 100);
        assert_eq!(st.lost, 0);
        assert_eq!(st.expected, 100);

        // A hole (packet 42 missing) counts as one lost packet.
        let mut st = RtpStats::default();
        for seq in 0u16..=100 {
            if seq != 42 {
                st.observe(seq, seq as u32, seq as i64);
            }
        }
        assert_eq!(st.lost, 1);
    }

    #[test]
    fn rtp_stats_estimates_jitter() {
        let mut st = RtpStats::default();
        // Constant rate: 48 timestamp units per ms arrival; no jitter.
        for i in 0..64 {
            st.observe(i as u16, (i * 48) as u32, i as i64);
        }
        assert!(st.jitter_ts.abs() < 1.0);

        // One packet arrives 2 ms late -> jitter grows.
        let mut st = RtpStats::default();
        for i in 0..64 {
            let late = if i == 30 { i + 2 } else { i };
            st.observe(i as u16, (i * 48) as u32, late as i64);
        }
        assert!(st.jitter_ts > 1.0);
    }

    #[test]
    fn rtp_stats_counts_out_of_order() {
        let mut st = RtpStats::default();
        for seq in [1u16, 2, 4, 5, 3] {
            st.observe(seq, seq as u32, seq as i64);
        }
        // Packet 3 arrives after 4 and 5 -> one out-of-order.
        assert_eq!(st.out_of_order, 1);
        assert_eq!(st.lost, 0);
        assert!(st.out_of_order_percent() > 0.0);
    }

    #[test]
    fn ntp_conversion_lands_in_unix_epoch() {
        // NTP 0 = 1900-01-01; unix 1970-01-01 = 2_208_988_800 NTP seconds.
        let ntp = (2_208_988_800u64) << 32;
        assert_eq!(ntp_to_unix_ns(ntp), 0);
        let ntp = (2_208_988_800u64 + 1) << 32;
        assert_eq!(ntp_to_unix_ns(ntp), 1_000_000_000);
    }

    #[test]
    fn rtt_is_non_negative() {
        // A report just received with a tiny DLSR.
        let now_sec32 = (crate::core::unix_seconds() + 2_208_988_800) & 0xffff_ffff;
        let rtt = rtt_ms_from_report((now_sec32 - 1) as u32, 655);
        assert!(rtt > 0.0 && rtt < 2000.0);
    }
}
