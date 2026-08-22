//! Publishes N seconds of Opus audio into a room, for egress testing and
//! benchmarking.
//!
//! Usage:
//! ```text
//! cargo run -p lk-egress --release --example send_audio -- \
//!   --ws ws://127.0.0.1:7880 --key devkey --secret secret \
//!   --room room-a --seconds 10
//! ```

use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_OPUS};
use webrtc::api::APIBuilder;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtp_transceiver::rtp_codec::{
    RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocalWriter;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

static VERBOSE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn log(args: std::fmt::Arguments<'_>) {
    if VERBOSE.load(std::sync::atomic::Ordering::Relaxed) {
        eprintln!("{args}");
    }
}

fn message_name(m: &lk::signal_response::Message) -> &'static str {
    match m {
        lk::signal_response::Message::Join(_) => "Join",
        lk::signal_response::Message::Answer(_) => "Answer",
        lk::signal_response::Message::Offer(_) => "Offer",
        lk::signal_response::Message::Trickle(_) => "Trickle",
        lk::signal_response::Message::Update(_) => "Update",
        lk::signal_response::Message::TrackPublished(_) => "TrackPublished",
        lk::signal_response::Message::Leave(_) => "Leave",
        lk::signal_response::Message::Mute(_) => "Mute",
        lk::signal_response::Message::SpeakersChanged(_) => "SpeakersChanged",
        lk::signal_response::Message::RoomUpdate(_) => "RoomUpdate",
        lk::signal_response::Message::ConnectionQuality(_) => "ConnectionQuality",
        lk::signal_response::Message::StreamStateUpdate(_) => "StreamStateUpdate",
        lk::signal_response::Message::SubscribedQualityUpdate(_) => "SubscribedQualityUpdate",
        lk::signal_response::Message::SubscriptionPermissionUpdate(_) => {
            "SubscriptionPermissionUpdate"
        }
        lk::signal_response::Message::RefreshToken(_) => "RefreshToken",
        lk::signal_response::Message::TrackUnpublished(_) => "TrackUnpublished",
        lk::signal_response::Message::Pong(_) => "Pong",
        lk::signal_response::Message::Reconnect(_) => "Reconnect",
        lk::signal_response::Message::PongResp(_) => "PongResp",
        lk::signal_response::Message::SubscriptionResponse(_) => "SubscriptionResponse",
        lk::signal_response::Message::RequestResponse(_) => "RequestResponse",
        lk::signal_response::Message::TrackSubscribed(_) => "TrackSubscribed",
        lk::signal_response::Message::RoomMoved(_) => "RoomMoved",
        lk::signal_response::Message::MediaSectionsRequirement(_) => "MediaSectionsRequirement",
        lk::signal_response::Message::SubscribedAudioCodecUpdate(_) => "SubscribedAudioCodecUpdate",
        lk::signal_response::Message::PublishDataTrackResponse(_) => "PublishDataTrackResponse",
        lk::signal_response::Message::UnpublishDataTrackResponse(_) => "UnpublishDataTrackResponse",
        lk::signal_response::Message::DataTrackSubscriberHandles(_) => "DataTrackSubscriberHandles",
        lk::signal_response::Message::StoreDataBlobResponse(_) => "StoreDataBlobResponse",
        lk::signal_response::Message::GetDataBlobResponse(_) => "GetDataBlobResponse",
    }
}

fn main() {
    let mut ws_url = "ws://127.0.0.1:7880".to_string();
    let mut key = "devkey".to_string();
    let mut secret = "secret".to_string();
    let mut room = "bench".to_string();
    let mut seconds = 10u64;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--ws" => ws_url = iter.next().unwrap(),
            "--key" => key = iter.next().unwrap(),
            "--secret" => secret = iter.next().unwrap(),
            "--room" => room = iter.next().unwrap(),
            "--seconds" => seconds = iter.next().unwrap().parse().unwrap(),
            "--verbose" => VERBOSE.store(true, std::sync::atomic::Ordering::Relaxed),
            "--help" => {
                println!(
                    "send_audio --ws <url> --key <k> --secret <s> --room <room> --seconds <n> [--verbose]"
                );
                return;
            }
            other => panic!("unknown arg {other}"),
        }
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(publish(&ws_url, &key, &secret, &room, seconds))
        .unwrap();
}

fn join_token(key: &str, secret: &str, identity: &str, room: &str) -> String {
    let now = lk_egress::now_secs();
    let payload = serde_json::json!({
        "iss": key, "sub": identity, "iat": now, "nbf": now - 5, "exp": now + 3600,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

async fn read_response(ws: &mut Ws) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                let resp = lk::SignalResponse::decode(bytes.as_ref()).unwrap();
                if let Some(m) = &resp.message {
                    log(format_args!("recv {}", message_name(m)));
                }
                // The Go server refreshes access tokens periodically; skip.
                if matches!(
                    resp.message,
                    Some(lk::signal_response::Message::RefreshToken(_))
                ) {
                    continue;
                }
                return resp;
            }
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => continue,
            _ => panic!("ws closed"),
        }
    }
}

async fn send_request(ws: &mut Ws, req: &lk::SignalRequest) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

async fn client_pc(
    ws: Arc<tokio::sync::Mutex<Ws>>,
    target: i32,
) -> Arc<webrtc::peer_connection::RTCPeerConnection> {
    let mut m = MediaEngine::default();
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48_000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m).unwrap();
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();
    let pc = api
        .new_peer_connection(RTCConfiguration::default())
        .await
        .unwrap();
    let ws2 = ws.clone();
    pc.on_ice_candidate(Box::new(
        move |c: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let ws = ws2.clone();
            Box::pin(async move {
                if let Some(c) = c {
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
                            log(format_args!("trickle target={target} candidate={json}"));
                            let mut ws = ws.lock().await;
                            let _ = send_request(
                                &mut ws,
                                &lk::SignalRequest {
                                    message: Some(lk::signal_request::Message::Trickle(
                                        lk::TrickleRequest {
                                            candidate_init: json,
                                            target,
                                            r#final: false,
                                        },
                                    )),
                                },
                            )
                            .await;
                        }
                    }
                }
            })
        },
    ));
    let name = if target == 0 {
        "publisher"
    } else {
        "subscriber"
    };
    pc.on_peer_connection_state_change(Box::new(
        move |s: webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState| {
            log(format_args!("{name} PC state: {s}"));
            Box::pin(async {})
        },
    ));
    Arc::new(pc)
}

/// Waits for the subscriber offer, skipping refresh tokens, participant
/// updates and other unrelated messages (the Go server interleaves them).
async fn await_offer(ws: &mut Ws) -> String {
    loop {
        let resp = read_response(ws).await;
        match resp.message {
            Some(lk::signal_response::Message::Offer(o)) => return o.sdp,
            _ => continue,
        }
    }
}

/// Answers a subscriber offer sent by the server and returns the subscriber PC.
async fn setup_subscriber(
    ws: &Arc<tokio::sync::Mutex<Ws>>,
    sdp: &str,
) -> Result<Arc<webrtc::peer_connection::RTCPeerConnection>, String> {
    let pc = client_pc(ws.clone(), 1).await;
    let mut offer = RTCSessionDescription::default();
    offer.sdp_type = RTCSdpType::Offer;
    offer.sdp = sdp.to_string();
    pc.set_remote_description(offer)
        .await
        .map_err(|e| e.to_string())?;
    log(format_args!("subscriber offer set"));
    let answer = pc.create_answer(None).await.map_err(|e| e.to_string())?;
    pc.set_local_description(answer.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut guard = ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Answer(
                lk::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: answer.sdp.clone(),
                    ..Default::default()
                },
            )),
        },
    )
    .await;
    drop(guard);
    log(format_args!("subscriber answer sent"));
    Ok(pc)
}

/// Feeds a trickle candidate to the PC it targets (0 = publisher, 1 = subscriber).
async fn feed_trickle(
    pub_pc: &Arc<webrtc::peer_connection::RTCPeerConnection>,
    sub_pc: Option<&Arc<webrtc::peer_connection::RTCPeerConnection>>,
    t: &lk::TrickleRequest,
) {
    let Ok(init) = serde_json::from_str::<webrtc::ice_transport::ice_candidate::RTCIceCandidateInit>(
        &t.candidate_init,
    ) else {
        return;
    };
    match t.target {
        0 => {
            let _ = pub_pc.add_ice_candidate(init).await;
        }
        1 => {
            if let Some(pc) = sub_pc {
                let _ = pc.add_ice_candidate(init).await;
            }
        }
        _ => {}
    }
}

async fn publish(
    ws_url: &str,
    key: &str,
    secret: &str,
    room: &str,
    seconds: u64,
) -> Result<(), String> {
    let token = join_token(key, secret, "sender", room);
    let url = format!("{}/rtc?access_token={token}", ws_url);
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| e.to_string())?;
    log(format_args!("connected to {url}"));
    let ws = Arc::new(tokio::sync::Mutex::new(ws));

    // Keep the signal connection alive (the server closes sessions that send
    // no frames within its ~15s ping timeout).
    let ws_keepalive = ws.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut ws = ws_keepalive.lock().await;
            let _ = send_request(
                &mut ws,
                &lk::SignalRequest {
                    message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                        timestamp: lk_egress::now_secs() * 1000,
                        rtt: 0,
                    })),
                },
            )
            .await;
        }
    });

    let mut guard = ws.lock().await;
    let _ = read_response(&mut guard).await; // join
    drop(guard);

    // The legacy flow answers the server's subscriber offer before publishing.
    // In fastPublish mode (the Go server) no offer is sent, so fall back to
    // publishing directly after a short wait.
    let mut sub_pc: Option<Arc<webrtc::peer_connection::RTCPeerConnection>> = None;
    match tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut guard = ws.lock().await;
        let sdp = await_offer(&mut guard).await;
        drop(guard);
        sdp
    })
    .await
    {
        Ok(sdp) => {
            sub_pc = Some(setup_subscriber(&ws, &sdp).await?);
        }
        Err(_) => log(format_args!(
            "no subscriber offer within 3s (fastPublish); publishing directly"
        )),
    }

    let pub_pc = client_pc(ws.clone(), 0).await;
    let out_track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            ..Default::default()
        },
        "bench-audio".to_string(),
        "bench-mic".to_string(),
    ));
    let _sender = pub_pc.add_transceiver_from_track(
        out_track.clone(),
        Some(webrtc::rtp_transceiver::RTCRtpTransceiverInit {
            direction: webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection::Sendonly,
            send_encodings: vec![],
        }),
    ).await.map_err(|e| e.to_string())?;
    let offer = pub_pc.create_offer(None).await.map_err(|e| e.to_string())?;
    pub_pc
        .set_local_description(offer.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut mid_to_track_id = std::collections::BTreeMap::new();
    mid_to_track_id.insert("0".to_string(), "bench-mic".to_string());
    let mut guard = ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Offer(lk::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer.sdp.clone(),
                id: 0,
                mid_to_track_id,
            })),
        },
    )
    .await;
    log(format_args!("publisher offer sent"));

    // Wait for the publisher answer, handling a late subscriber offer and
    // trickle candidates along the way.
    let answer_sdp = loop {
        let resp = read_response(&mut guard).await;
        match resp.message {
            Some(lk::signal_response::Message::Answer(a)) => break a.sdp,
            Some(lk::signal_response::Message::Offer(o)) => {
                if sub_pc.is_none() {
                    drop(guard);
                    sub_pc = Some(setup_subscriber(&ws, &o.sdp).await?);
                    guard = ws.lock().await;
                }
            }
            Some(lk::signal_response::Message::Trickle(t)) => {
                feed_trickle(&pub_pc, sub_pc.as_ref(), &t).await;
            }
            _ => {}
        }
    };
    drop(guard);
    log(format_args!("publisher answer received"));
    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Answer;
    sd.sdp = answer_sdp;
    pub_pc
        .set_remote_description(sd)
        .await
        .map_err(|e| e.to_string())?;
    log(format_args!("publisher remote description set"));
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    let enc = audiopus::coder::Encoder::new(
        audiopus::SampleRate::Hz48000,
        audiopus::Channels::Mono,
        audiopus::Application::Voip,
    )
    .unwrap();
    let silence = vec![0i16; 960];
    let mut opus = vec![0u8; 4000];
    let n = enc.encode(&silence, &mut opus).unwrap();
    let payload = opus[..n].to_vec();
    let mut seq = 1u16;
    let frames = seconds * 50;
    for _ in 0..frames {
        let pkt = webrtc::rtp::packet::Packet {
            header: webrtc::rtp::header::Header {
                version: 2,
                payload_type: 111,
                sequence_number: seq,
                timestamp: seq as u32 * 960,
                ssrc: 0xdeadbeef,
                ..Default::default()
            },
            payload: payload.clone().into(),
        };
        let _ = out_track.write_rtp(&pkt).await;
        seq = seq.wrapping_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    println!("sent {frames} frames to room {room}");
    // Stay joined a moment so the recorder drains, then leave.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    log(format_args!("done"));
    Ok(())
}
