//! End-to-end media test: a real WebRTC publisher and subscriber exchange
//! audio through the server's SFU (validates ICE/DTLS, publisher offer/answer,
//! RTP forwarding, auto-subscription and subscriber renegotiation).

use std::sync::atomic::{AtomicBool, Ordering};
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
use webrtc::track::track_remote::TrackRemote;

use lk_server::config::Config;
use lk_server::http;
use lk_server::server::Server;

const API_KEY: &str = "devkey";
const SECRET: &str = "secret";

fn test_config() -> Config {
    Config {
        port: Some(0),
        keys: std::collections::BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        ..Default::default()
    }
}

fn join_token(identity: &str, room: &str) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": identity,
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

async fn start_server() -> (Arc<Server>, String) {
    let config = test_config();
    let server = Server::new(config);
    let app = http::router(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (server, format!("http://{addr}"))
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(base: &str, token: &str) -> Ws {
    let url = format!("{}/rtc?access_token={}", base.replace("http", "ws"), token);
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

async fn read_response(ws: &mut Ws) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error: {e}"),
            None => panic!("ws closed"),
        }
    }
}

async fn send_request(ws: &mut Ws, req: &lk::SignalRequest) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

/// Reads responses until one of the given kinds arrives, applying trickles to
/// the peer connection.
async fn apply_trickle(
    pc: &Arc<webrtc::peer_connection::RTCPeerConnection>,
    init: &webrtc::ice_transport::ice_candidate::RTCIceCandidateInit,
) {
    let _ = pc.add_ice_candidate(init.clone()).await;
}

async fn await_message<F: Fn(&lk::SignalResponse) -> bool>(
    ws: &mut Ws,
    pc_pub: Option<&Arc<webrtc::peer_connection::RTCPeerConnection>>,
    pc_sub: Option<&Arc<webrtc::peer_connection::RTCPeerConnection>>,
    matches: F,
) -> lk::SignalResponse {
    for _ in 0..400 {
        let resp = read_response(ws).await;
        if let Some(lk::signal_response::Message::Trickle(t)) = &resp.message {
            if let Ok(init) = serde_json::from_str::<
                webrtc::ice_transport::ice_candidate::RTCIceCandidateInit,
            >(&t.candidate_init)
            {
                match t.target {
                    0 => {
                        if let Some(pc) = pc_pub {
                            apply_trickle(pc, &init).await;
                        }
                    }
                    _ => {
                        if let Some(pc) = pc_sub {
                            apply_trickle(pc, &init).await;
                        }
                    }
                }
            }
            continue;
        }
        if matches(&resp) {
            return resp;
        }
    }
    panic!("timed out waiting for expected signal response");
}

/// Builds a client peer connection that registers its own ICE candidates back
/// to the signaling websocket with the given target.
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
    let pc = Arc::new(
        api.new_peer_connection(RTCConfiguration::default())
            .await
            .unwrap(),
    );

    let ws2 = ws.clone();

    pc.on_ice_candidate(Box::new(
        move |c: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let ws = ws2.clone();
            Box::pin(async move {
                if let Some(c) = c {
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
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
    pc
}

#[tokio::test]
async fn audio_flows_from_publisher_to_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let (_server, base) = start_server().await;

    // ---------- publisher ----------
    let pub_token = join_token("pub", "media-room");
    let pub_ws = Arc::new(tokio::sync::Mutex::new(ws_connect(&base, &pub_token).await));

    // Join response.
    let mut guard = pub_ws.lock().await;
    let resp = read_response(&mut guard).await;
    assert!(matches!(
        resp.message,
        Some(lk::signal_response::Message::Join(_))
    ));
    // The server also sends a subscriber offer (data channels) for the
    // publisher; the publisher answers it with a second (subscriber) PC.
    let resp = read_response(&mut guard).await;
    let sub_offer_sdp = match resp.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected subscriber offer, got {other:?}"),
    };
    drop(guard);

    // Publisher's own peer connections: a publisher PC (audio) and a
    // subscriber PC (data channels).
    let pub_pc = client_pc(pub_ws.clone(), 0).await;
    let pub_sub_pc = client_pc(pub_ws.clone(), 1).await;
    // Answer the server's subscriber (data channel) offer.
    let mut sub_offer = RTCSessionDescription::default();
    sub_offer.sdp_type = RTCSdpType::Offer;
    sub_offer.sdp = sub_offer_sdp;
    pub_sub_pc.set_remote_description(sub_offer).await.unwrap();
    let sub_answer = pub_sub_pc.create_answer(None).await.unwrap();
    pub_sub_pc
        .set_local_description(sub_answer.clone())
        .await
        .unwrap();
    let mut guard = pub_ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Answer(
                lk::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: sub_answer.sdp.clone(),
                    id: 0,
                    mid_to_track_id: Default::default(),
                },
            )),
        },
    )
    .await;
    drop(guard);
    let out_track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            ..Default::default()
        },
        "pub-audio".to_string(),
        "mic1".to_string(), // stream id == client cid
    ));
    let _sender = pub_pc
        .add_transceiver_from_track(
            out_track.clone(),
            Some(webrtc::rtp_transceiver::RTCRtpTransceiverInit {
                direction: webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection::Sendonly,
                send_encodings: vec![],
            }),
        )
        .await
        .unwrap();

    // Create + send publisher offer with the audio track.
    let offer = pub_pc.create_offer(None).await.unwrap();
    pub_pc.set_local_description(offer.clone()).await.unwrap();
    let mut mid_to_track_id = std::collections::BTreeMap::new();
    mid_to_track_id.insert("0".to_string(), "mic1".to_string());
    let mut guard = pub_ws.lock().await;
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
    // Wait for the publisher answer.
    let resp = await_message(&mut guard, Some(&pub_pc), Some(&pub_sub_pc), |r| {
        matches!(r.message, Some(lk::signal_response::Message::Answer(_)))
    })
    .await;
    let answer_sdp = match resp.message {
        Some(lk::signal_response::Message::Answer(a)) => a.sdp,
        other => panic!("expected answer, got {other:?}"),
    };
    drop(guard);

    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Answer;
    sd.sdp = answer_sdp.clone();
    if let Err(e) = pub_pc.set_remote_description(sd).await {
        panic!("set_remote_description failed: {e}");
    }

    // Let the publisher's publisher-PC connection establish; ping to keep the
    // signal connection alive (the server enforces a 15s read timeout).
    let keepalive_ws = pub_ws.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut ws = keepalive_ws.lock().await;
            let _ = send_request(
                &mut ws,
                &lk::SignalRequest {
                    message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                        timestamp: lk_server::core::unix_micros() / 1000,
                        rtt: 0,
                    })),
                },
            )
            .await;
        }
    });
    // Let the publisher's publisher-PC establish, then start sending audio RTP.
    // This triggers on_track on the server and creates the forwarder.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let mut seq = 1u16;
    for _ in 0..400 {
        let pkt = webrtc::rtp::packet::Packet {
            header: webrtc::rtp::header::Header {
                version: 2,
                payload_type: 111,
                sequence_number: seq,
                timestamp: seq as u32 * 960,
                ssrc: 0xdeadbeef,
                ..Default::default()
            },
            payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe, 0x00, 0x01, 0x02, 0x03]),
        };
        if let Err(e) = out_track.write_rtp(&pkt).await {
            eprintln!("write_rtp error: {e}");
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // ---------- subscriber ----------
    let sub_token = join_token("sub", "media-room");
    let sub_ws = Arc::new(tokio::sync::Mutex::new(ws_connect(&base, &sub_token).await));
    let keepalive_sub = sub_ws.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut ws = keepalive_sub.lock().await;
            let _ = send_request(
                &mut ws,
                &lk::SignalRequest {
                    message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                        timestamp: lk_server::core::unix_micros() / 1000,
                        rtt: 0,
                    })),
                },
            )
            .await;
        }
    });
    let mut guard = sub_ws.lock().await;
    let resp = read_response(&mut guard).await;
    assert!(matches!(
        resp.message,
        Some(lk::signal_response::Message::Join(_))
    ));
    // First subscriber offer (data channels).
    let resp = read_response(&mut guard).await;
    let first_offer_sdp = match resp.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected subscriber offer, got {other:?}"),
    };
    drop(guard);

    // Answer the data-channel offer.
    let sub_pc = client_pc(sub_ws.clone(), 1).await;
    let mut sub_offer = RTCSessionDescription::default();
    sub_offer.sdp_type = RTCSdpType::Offer;
    sub_offer.sdp = first_offer_sdp;
    sub_pc.set_remote_description(sub_offer).await.unwrap();
    let answer = sub_pc.create_answer(None).await.unwrap();
    sub_pc.set_local_description(answer.clone()).await.unwrap();

    // Receive forwarded RTP on the subscriber's incoming track.
    let received = Arc::new(AtomicBool::new(false));
    let rcvd = received.clone();
    let sub_pc2 = sub_pc.clone();
    sub_pc.on_track(Box::new(move |track: Arc<TrackRemote>, _r, _t| {
        let rcvd = rcvd.clone();
        Box::pin(async move {
            let mut buf = vec![0u8; 2048];
            if track.read(&mut buf).await.is_ok() {
                rcvd.store(true, Ordering::Relaxed);
            }
        })
    }));
    let _ = sub_pc2;

    let mut guard = sub_ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Answer(
                lk::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: answer.sdp.clone(),
                    id: 0,
                    mid_to_track_id: Default::default(),
                },
            )),
        },
    )
    .await;
    drop(guard);

    // Wait for the auto-subscription renegotiation offer (contains the audio track).
    let mut guard = sub_ws.lock().await;
    let resp = await_message(&mut guard, None, Some(&sub_pc), |r| {
        matches!(r.message, Some(lk::signal_response::Message::Offer(_)))
    })
    .await;
    let sub_offer2_sdp = match resp.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected second subscriber offer, got {other:?}"),
    };
    drop(guard);

    let mut sub_offer2 = RTCSessionDescription::default();
    sub_offer2.sdp_type = RTCSdpType::Offer;
    sub_offer2.sdp = sub_offer2_sdp;
    sub_pc.set_remote_description(sub_offer2).await.unwrap();
    let answer2 = sub_pc.create_answer(None).await.unwrap();
    sub_pc.set_local_description(answer2.clone()).await.unwrap();
    let mut guard = sub_ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Answer(
                lk::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: answer2.sdp.clone(),
                    id: 0,
                    mid_to_track_id: Default::default(),
                },
            )),
        },
    )
    .await;
    drop(guard);

    // Let the subscriber's media connection establish, then send another burst
    // of RTP that the forwarder must relay to the subscribed down-track.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    for _ in 0..300 {
        let pkt = webrtc::rtp::packet::Packet {
            header: webrtc::rtp::header::Header {
                version: 2,
                payload_type: 111,
                sequence_number: seq,
                timestamp: seq as u32 * 960,
                ssrc: 0xdeadbeef,
                ..Default::default()
            },
            payload: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe, 0x00, 0x01, 0x02, 0x03]),
        };
        if let Err(e) = out_track.write_rtp(&pkt).await {
            eprintln!("write_rtp error: {e}");
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // The subscriber must have received forwarded RTP.
    for _ in 0..100 {
        if received.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        received.load(Ordering::Relaxed),
        "subscriber did not receive forwarded audio RTP"
    );
}
