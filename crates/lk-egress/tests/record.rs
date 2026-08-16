//! End-to-end egress test: a real WebRTC publisher sends audio into a room on
//! livekit-voice; a livekit-egress instance receives a `StartEgress` job over
//! the in-memory psrpc bus, joins the room, records, and writes a valid WAV.

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

use lk_egress::config::EgressConfig;
use lk_egress::io::IoClient;
use lk_egress::server::EgressServer;
use lk_psrpc::MemoryBus;
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
    let video = serde_json::json!({
        "roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true
    });
    let payload = serde_json::json!({
        "iss": API_KEY, "sub": identity, "iat": now, "nbf": now - 5, "exp": now + 3600, "video": video
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

fn record_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY, "sub": "admin", "iat": now, "nbf": now - 5, "exp": now + 3600,
        "video": {"roomRecord": true}
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

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(base: &str, token: &str) -> Ws {
    let url = format!("{}/rtc?access_token={token}", base.replace("http", "ws"));
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

async fn read_response(ws: &mut Ws) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap()
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
    Arc::new(pc)
}

async fn await_message<F: Fn(&lk::SignalResponse) -> bool>(
    ws: &mut Ws,
    pc_pub: &Arc<webrtc::peer_connection::RTCPeerConnection>,
    pc_sub: &Arc<webrtc::peer_connection::RTCPeerConnection>,
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
                        let _ = pc_pub.add_ice_candidate(init.clone()).await;
                    }
                    _ => {
                        let _ = pc_sub.add_ice_candidate(init).await;
                    }
                }
            }
            continue;
        }
        if matches(&resp) {
            return resp;
        }
    }
    panic!("timed out waiting for signal response");
}

#[tokio::test]
async fn records_room_audio_to_wav() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let server = Server::new(test_config());
    let base = {
        let app = http::router(server.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    };
    let bus = MemoryBus::new();
    server.start_sip_io_with(bus.clone()).await.unwrap();
    let _eg = server.egress_client_with(bus.clone()).await.unwrap();

    // Start the egress recorder.
    let out_dir = std::env::temp_dir().join(format!("lk_egress_test_{}", std::process::id()));
    std::fs::create_dir_all(&out_dir).unwrap();
    let conf = EgressConfig {
        api_key: API_KEY.to_string(),
        api_secret: SECRET.to_string(),
        ws_url: base.replace("http", "ws"),
        output_dir: out_dir.to_str().unwrap().to_string(),
        redis: Default::default(),
        ..Default::default()
    };
    let io = IoClient::new(bus.clone()).await.unwrap();
    let _egress = EgressServer::new(bus.clone(), conf, io).await.unwrap();

    // ---------- publisher ----------
    let pub_token = join_token("pub", "egress-room");
    let pub_ws = Arc::new(tokio::sync::Mutex::new(ws_connect(&base, &pub_token).await));
    let mut guard = pub_ws.lock().await;
    let resp = read_response(&mut guard).await;
    assert!(matches!(
        resp.message,
        Some(lk::signal_response::Message::Join(_))
    ));
    let sub_offer_sdp = match read_response(&mut guard).await.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected subscriber offer, got {other:?}"),
    };
    drop(guard);

    let pub_pc = client_pc(pub_ws.clone(), 0).await;
    let pub_sub_pc = client_pc(pub_ws.clone(), 1).await;
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
                    ..Default::default()
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
        "mic1".to_string(),
    ));
    let _sender = pub_pc.add_transceiver_from_track(
        out_track.clone(),
        Some(webrtc::rtp_transceiver::RTCRtpTransceiverInit {
            direction: webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection::Sendonly,
            send_encodings: vec![],
        }),
    ).await.unwrap();
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
    let resp = await_message(&mut guard, &pub_pc, &pub_sub_pc, |r| {
        matches!(r.message, Some(lk::signal_response::Message::Answer(_)))
    })
    .await;
    let answer_sdp = match resp.message {
        Some(lk::signal_response::Message::Answer(a)) => a.sdp,
        _ => panic!("no answer"),
    };
    drop(guard);
    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Answer;
    sd.sdp = answer_sdp;
    pub_pc.set_remote_description(sd).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    // Start the recording.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/twirp/livekit.Egress/StartRoomCompositeEgress"
        ))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(
            r#"{"roomName":"egress-room","fileOutputs":[{"fileType":0,"filepath":"/record.wav"}]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    let start: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let egress_id = start["egressId"].as_str().unwrap().to_string();

    // Encode valid Opus silence and send it for a few seconds.
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
    for i in 0..150 {
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
        if i % 20 == 0 {
            let _ = pub_ws.lock().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // Stop the recording through the Twirp API -> EgressHandler.StopEgress.
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/StopEgress"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(format!(r#"{{"egressId":"{egress_id}"}}"#))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());

    // The egress must finalize the recording (write the WAV header).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let wav = loop {
        let mut finalized = None;
        for f in std::fs::read_dir(&out_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
        {
            if f.extension().map(|e| e == "wav").unwrap_or(false) {
                if let Ok(b) = std::fs::read(&f) {
                    if b.len() >= 12 && &b[0..4] == b"RIFF" {
                        finalized = Some(f);
                    }
                }
            }
        }
        if let Some(f) = finalized {
            break f;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "recording not finalized"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    let bytes = std::fs::read(&wav).unwrap();
    assert_eq!(
        &bytes[0..4],
        b"RIFF",
        "not a WAV file: {} ({} bytes)",
        wav.display(),
        bytes.len()
    );
    assert_eq!(&bytes[8..12], b"WAVE");
    let data_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
    assert!(data_len > 0, "WAV has no audio data");
    drop(pub_ws);
    let _ = std::fs::remove_dir_all(&out_dir);
}
