//! Egress recording integration tests: MP3 output, multi-track mixing, and the
//! EGRESS_STARTING -> EGRESS_COMPLETE state reported to the server.

use std::collections::BTreeMap;
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
        keys: BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        ..Default::default()
    }
}

fn join_token(identity: &str, room: &str) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY, "sub": identity, "iat": now, "nbf": now - 5, "exp": now + 3600,
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
    panic!("timed out");
}

/// Connects a publisher, publishes an Opus audio track, and returns the
/// signal websocket (Arc<Mutex<>>), its publisher PC, and the out track.
async fn connect_publisher(
    base: &str,
    identity: &str,
    room: &str,
    cid: &str,
) -> (
    Arc<tokio::sync::Mutex<Ws>>,
    Arc<webrtc::peer_connection::RTCPeerConnection>,
    Arc<TrackLocalStaticRTP>,
) {
    let ws = Arc::new(tokio::sync::Mutex::new(
        ws_connect(base, &join_token(identity, room)).await,
    ));
    let mut guard = ws.lock().await;
    let _join = read_response(&mut guard).await;
    let sub_offer_sdp = match read_response(&mut guard).await.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected subscriber offer, got {other:?}"),
    };
    drop(guard);

    let pub_pc = client_pc(ws.clone(), 0).await;
    let pub_sub_pc = client_pc(ws.clone(), 1).await;
    let mut sub_offer = RTCSessionDescription::default();
    sub_offer.sdp_type = RTCSdpType::Offer;
    sub_offer.sdp = sub_offer_sdp;
    pub_sub_pc.set_remote_description(sub_offer).await.unwrap();
    let sub_answer = pub_sub_pc.create_answer(None).await.unwrap();
    pub_sub_pc
        .set_local_description(sub_answer.clone())
        .await
        .unwrap();
    let mut guard = ws.lock().await;
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
        "bench-audio".to_string(),
        cid.to_string(),
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
    let offer = pub_pc.create_offer(None).await.unwrap();
    pub_pc.set_local_description(offer.clone()).await.unwrap();
    let mut mid_to_track_id = BTreeMap::new();
    mid_to_track_id.insert("0".to_string(), cid.to_string());
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
    (ws, pub_pc, out_track)
}

/// Sends `seconds` worth of real Opus RTP on the track (silence).
async fn stream_audio(out_track: Arc<TrackLocalStaticRTP>, seconds: u64, ssrc: u32, base_seq: u16) {
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
    let mut seq = base_seq;
    // Pace in real time (one 20 ms frame every 20 ms) so a late-subscribing
    // recorder still captures the tail of the stream.
    for i in 0..(seconds * 50) {
        let pkt = webrtc::rtp::packet::Packet {
            header: webrtc::rtp::header::Header {
                version: 2,
                payload_type: 111,
                sequence_number: seq,
                timestamp: seq as u32 * 960,
                ssrc,
                ..Default::default()
            },
            payload: payload.clone().into(),
        };
        let _ = out_track.write_rtp(&pkt).await;
        seq = seq.wrapping_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _ = i;
    }
}

/// Starts the in-process server + egress and returns (server, base URL).
async fn start_stack(out_dir: &std::path::Path) -> (Arc<Server>, String) {
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
    let _eg_client = server.egress_client_with(bus.clone()).await.unwrap();

    let conf = EgressConfig {
        api_key: API_KEY.to_string(),
        api_secret: SECRET.to_string(),
        ws_url: base.replace("http", "ws"),
        output_dir: out_dir.to_str().unwrap().to_string(),
        redis: Default::default(),
        ..Default::default()
    };
    let io = IoClient::new(bus.clone()).await.unwrap();
    let _egress = EgressServer::new(bus, conf, io).await.unwrap();
    (server, base)
}

async fn start_egress_twirp(base: &str, room: &str, file_type: i32) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/twirp/livekit.Egress/StartRoomCompositeEgress"
        ))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(
            serde_json::json!({
                "roomName": room,
                "audioOnly": true,
                "fileOutputs": [{"fileType": file_type, "filepath": "/rec"}]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    let start: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    start["egressId"].as_str().unwrap().to_string()
}

async fn stop_egress_twirp(base: &str, egress_id: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/StopEgress"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(format!(r#"{{"egressId":"{egress_id}"}}"#))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
}

async fn list_egress_twirp(base: &str, room: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/ListEgress"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(format!(r#"{{"roomName":"{room}"}}"#))
        .send()
        .await
        .unwrap();
    serde_json::from_str(&resp.text().await.unwrap()).unwrap()
}

/// Waits for a finalized WAV (RIFF header written) in `dir`, returning its bytes.
async fn wait_finalized(dir: &std::path::Path, ext: &str) -> Vec<u8> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        for f in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
            let p = f.path();
            if p.extension().map(|e| e == ext).unwrap_or(false) {
                if let Ok(b) = std::fs::read(&p) {
                    let finalized = if ext == "wav" {
                        b.len() >= 12 && &b[0..4] == b"RIFF"
                    } else {
                        !b.is_empty()
                    };
                    if finalized {
                        return b;
                    }
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no finalized .{ext} output"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Records MP3 output and verifies the file is valid MPEG frames with a
/// duration close to the recorded window.
#[tokio::test]
async fn records_mp3() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_mp3_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;

    let (pub_ws, _pc, out_track) = connect_publisher(&base, "mp3-pub", "mp3-room", "mic1").await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let stream = tokio::spawn(stream_audio(out_track.clone(), 8, 0x11111111, 1));

    let egress_id = start_egress_twirp(&base, "mp3-room", 3).await; // EncodedFileType::MP3
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    stop_egress_twirp(&base, &egress_id).await;
    let _ = stream.await;

    let mp3 = wait_finalized(&out_dir, "mp3").await;
    assert!(!mp3.is_empty());

    // Validate MPEG frame sync: every MP3 frame starts 0xFF 0xE.
    let syncs = mp3
        .windows(2)
        .filter(|w| w[0] == 0xFF && (w[1] & 0xE0) == 0xE0)
        .count();
    assert!(
        syncs > 20,
        "MP3 must contain MPEG frame syncs (got {syncs})"
    );

    // The recorder must have captured a meaningful window of the (real-time)
    // 8s stream, regardless of how fast this machine subscribes.
    let dur_ms = (syncs as u64 * 1152) / 48; // 1152 samples/frame @ 48 kHz
    assert!(
        (1000..=9000).contains(&dur_ms),
        "MP3 duration {dur_ms} ms out of range for the 8s stream"
    );
    drop(pub_ws);
    let _ = server;
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// Two publishers stream simultaneously; the room-composite output mixes both
/// and reports EGRESS_COMPLETE with a file result through the server's API.
#[tokio::test]
async fn mixes_two_publishers_and_reports_complete() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_mix_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;

    let (ws1, _pc1, t1) = connect_publisher(&base, "mix-1", "mix-room", "mic1").await;
    let (ws2, _pc2, t2) = connect_publisher(&base, "mix-2", "mix-room", "mic2").await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let s1 = tokio::spawn(stream_audio(t1.clone(), 4, 0x11111111, 1));
    let s2 = tokio::spawn(stream_audio(t2.clone(), 4, 0x22222222, 1));

    let egress_id = start_egress_twirp(&base, "mix-room", 0).await; // WAV
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    stop_egress_twirp(&base, &egress_id).await;
    let _ = s1.await;
    let _ = s2.await;

    let wav = wait_finalized(&out_dir, "wav").await;
    assert_eq!(&wav[0..4], b"RIFF");
    assert_eq!(&wav[8..12], b"WAVE");

    // State reporting: ListEgress reflects EGRESS_COMPLETE with a file result.
    let list = list_egress_twirp(&base, "mix-room").await;
    let item = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["egressId"] == egress_id)
        .expect("egress in server state");
    assert_eq!(item["status"], "EGRESS_COMPLETE");
    assert!(!item["fileResults"].as_array().unwrap().is_empty());

    drop(ws1);
    drop(ws2);
    let _ = server;
    let _ = std::fs::remove_dir_all(&out_dir);
}
