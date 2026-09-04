//! Egress API end-to-end tests: the `livekit.Egress` Twirp surface wired to a
//! real recorder. Asserts the wire-compatible `EgressInfo` shape clients depend
//! on (nanosecond timestamps, room id, source type, status flow), the
//! `ListEgress`/`StopEgress` lifecycle, permissions, and webhooks.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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

/// Connects a publisher that publishes one Opus track, returning the ws, pub
/// PC, and out track.
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
        "api-audio".to_string(),
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

async fn start_room_composite(base: &str, room: &str) -> (reqwest::StatusCode, serde_json::Value) {
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
                "fileOutputs": [{"fileType": 0, "filepath": "/rec"}]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// Starts a room-composite egress over the protobuf Twirp content type (what
/// the SDKs actually send) and decodes the `EgressInfo` response.
async fn start_room_composite_pb(base: &str, room: &str) -> lk::EgressInfo {
    let req = lk::RoomCompositeEgressRequest {
        room_name: room.to_string(),
        audio_only: true,
        file_outputs: vec![lk::EncodedFileOutput {
            file_type: 0,
            filepath: "/rec".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{base}/twirp/livekit.Egress/StartRoomCompositeEgress"
        ))
        .header("Content-Type", "application/protobuf")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(req.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    lk::EgressInfo::decode(resp.bytes().await.unwrap().as_ref()).unwrap()
}

async fn list_egress(base: &str, body: serde_json::Value) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/ListEgress"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    serde_json::from_str(&resp.text().await.unwrap()).unwrap()
}

async fn list_egress_pb(base: &str, body: lk::ListEgressRequest) -> lk::ListEgressResponse {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/ListEgress"))
        .header("Content-Type", "application/protobuf")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(body.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    lk::ListEgressResponse::decode(resp.bytes().await.unwrap().as_ref()).unwrap()
}

async fn stop_egress(base: &str, egress_id: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.Egress/StopEgress"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", record_token()))
        .body(format!(r#"{{"egressId":"{egress_id}"}}"#))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// A recorder identity is `egress_{egress_id}` on this implementation; the
/// Go egress uses the bare egress id. The tests below deliberately avoid
/// asserting on the recorder identity so the identity scheme can change.

/// The `EgressInfo` returned by `StartRoomCompositeEgress` must match the Go
/// wire shape: egress id prefix `EG_`, the resolved room id, nanosecond
/// timestamps, `EGRESS_SOURCE_TYPE_SDK` for the SDK room-composite path, and
/// the original request echoed in `request.roomComposite`.
#[tokio::test]
async fn start_returns_wire_compatible_egress_info() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_api_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;

    // Create the room through the API so it has a real `RM_...` sid.
    let now = lk_server::core::unix_seconds();
    let create_payload = serde_json::json!({
        "iss": API_KEY, "sub": "admin", "iat": now, "nbf": now - 5, "exp": now + 3600,
        "video": {"roomCreate": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    let create_token = jsonwebtoken::encode(
        &header,
        &create_payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.RoomService/CreateRoom"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {create_token}"))
        .body(r#"{"name":"eg-api-room"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let room: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let room_sid = room["sid"].as_str().unwrap().to_string();
    assert!(room_sid.starts_with("RM_"));

    // Decode over the protobuf content type (what the SDKs send), so the wire
    // contract — including the EGRESS_STARTING (0) enum — is observable.
    let info = start_room_composite_pb(&base, "eg-api-room").await;
    assert!(info.egress_id.starts_with("EG_"), "egress id must use the EG_ prefix");
    assert_eq!(info.room_name, "eg-api-room");
    assert_eq!(info.room_id, room_sid, "room id must be resolved from the room");
    assert_eq!(info.status, lk::EgressStatus::EgressStarting as i32);
    assert_eq!(info.source_type, lk::EgressSourceType::Sdk as i32);

    // Timestamps are UnixNano (>= 1e15), matching the Go egress.
    assert!(
        info.started_at >= 1_000_000_000_000_000,
        "started_at must be nanoseconds, got {}",
        info.started_at
    );
    assert_eq!(info.started_at, info.updated_at);

    // The request is echoed back in the `request.roomComposite` oneof.
    match &info.request {
        Some(lk::egress_info::Request::RoomComposite(r)) => {
            assert_eq!(r.room_name, "eg-api-room");
            assert!(r.audio_only);
            assert_eq!(r.file_outputs.len(), 1);
        }
        other => panic!("expected request.roomComposite, got {other:?}"),
    }
    assert_eq!(info.ended_at, 0, "endedAt is unset while starting");

    drop(server);
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// `ListEgress` filters by room name and egress id, and returns a paginated
/// `nextPageToken` (empty when there are no more pages).
#[tokio::test]
async fn list_egress_filters_and_roundtrips() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_list_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;
    let _ = server;

    let (status, a) = start_room_composite(&base, "list-room-a").await;
    assert_eq!(status, 200, "{a}");
    let (status, b) = start_room_composite(&base, "list-room-b").await;
    assert_eq!(status, 200, "{b}");

    // By room name.
    let resp = list_egress(&base, serde_json::json!({"roomName": "list-room-a"})).await;
    let items = resp["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["egressId"], a["egressId"]);

    // By egress id.
    let resp = list_egress(
        &base,
        serde_json::json!({"egressId": b["egressId"].as_str().unwrap()}),
    )
    .await;
    let items = resp["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["roomName"], "list-room-b");

    // No filter returns both; the page token is empty (single page).
    let resp = list_egress(&base, serde_json::json!({})).await;
    assert_eq!(resp["items"].as_array().unwrap().len(), 2);
    assert!(resp["nextPageToken"].is_null());
}

/// Starting a recording requires the `roomRecord` grant; other tokens get the
/// reference `unauthenticated` (401) Twirp error.
#[tokio::test]
async fn start_requires_record_permission() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_perm_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;
    let _ = server;

    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY, "sub": "admin", "iat": now, "nbf": now - 5, "exp": now + 3600
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    let token = jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();

    let resp = reqwest::Client::new()
        .post(format!(
            "{base}/twirp/livekit.Egress/StartRoomCompositeEgress"
        ))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"roomName":"perm-room","fileOutputs":[{"fileType":0,"filepath":"/r"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "unauthenticated");
}

/// Full lifecycle: start, record real audio, stop, and observe the store end
/// in EGRESS_COMPLETE with a file result.
#[tokio::test]
async fn full_lifecycle_records_and_completes() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_full_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;

    let (pub_ws, _pc, out_track) = connect_publisher(&base, "api-pub", "full-room", "mic1").await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let stream = tokio::spawn(stream_audio(out_track.clone(), 4, 0x33333333, 1));

    let (status, start) = start_room_composite(&base, "full-room").await;
    assert_eq!(status, 200, "{start}");
    let egress_id = start["egressId"].as_str().unwrap().to_string();

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let (status, stopped) = stop_egress(&base, &egress_id).await;
    assert_eq!(status, 200, "stop failed: {stopped}");
    let _ = stream.await;

    // The store ends in EGRESS_COMPLETE with a file result.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = list_egress_pb(
            &base,
            lk::ListEgressRequest {
                egress_id: egress_id.clone(),
                ..Default::default()
            },
        )
        .await;
        if let Some(item) = resp.items.first() {
            if item.status == lk::EgressStatus::EgressComplete as i32 {
                assert_eq!(item.egress_id, egress_id);
                assert_eq!(item.room_name, "full-room");
                assert_eq!(item.source_type, lk::EgressSourceType::Sdk as i32);
                assert_eq!(item.file_results.len(), 1);
                assert!(!item.file_results[0].filename.is_empty());
                assert!(!item.file_results[0].location.is_empty());
                assert!(
                    item.ended_at >= 1_000_000_000_000_000,
                    "ended_at must be nanoseconds, got {}",
                    item.ended_at
                );
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "egress did not reach EGRESS_COMPLETE"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    drop(pub_ws);
    drop(server);
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// Sends `seconds` worth of real Opus RTP on the track (silence), paced in
/// real time so a late-subscribing recorder captures the tail.
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
    for _ in 0..(seconds * 50) {
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
    }
}

/// A webhook receiver that records (body, signature) pairs.
type Sink = Arc<Mutex<Vec<(Vec<u8>, String)>>>;

async fn start_receiver() -> (String, Sink) {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let sink2 = sink.clone();
    let app = axum::Router::new().route(
        "/webhook",
        axum::routing::post(move |req: axum::extract::Request| {
            let sink = sink2.clone();
            async move {
                let signature = req
                    .headers()
                    .get("x-livekit-signature")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let body = axum::body::to_bytes(req.into_body(), 1 << 20)
                    .await
                    .unwrap()
                    .to_vec();
                sink.lock().unwrap().push((body, signature));
                axum::http::StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/webhook"), sink)
}

fn config_with_webhook(url: String) -> Config {
    Config {
        port: Some(0),
        keys: BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        webhook: lk_server::config::WebhookConfig {
            api_key: API_KEY.to_string(),
            urls: vec![url],
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn wait_for_event(sink: &Sink, event: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        {
            let events = sink.lock().unwrap();
            for (body, _sig) in events.iter() {
                let v: serde_json::Value = serde_json::from_slice(body).unwrap();
                if v["event"] == event {
                    return v;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {event}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The egress reports `egress_started` (IOInfo.CreateEgress) and
/// `egress_ended` (IOInfo.UpdateEgress) webhooks with the room and info.
#[tokio::test]
async fn egress_reports_started_and_ended_webhooks() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_wh_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (webhook_url, sink) = start_receiver().await;

    let server = Server::new(config_with_webhook(webhook_url));
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

    let (pub_ws, _pc, out_track) =
        connect_publisher(&base, "wh-pub", "wh-room", "mic1").await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let stream = tokio::spawn(stream_audio(out_track.clone(), 4, 0x44444444, 1));

    let (status, start) = start_room_composite(&base, "wh-room").await;
    assert_eq!(status, 200, "{start}");
    let egress_id = start["egressId"].as_str().unwrap().to_string();

    let started = wait_for_event(&sink, "egress_started").await;
    assert_eq!(started["egressInfo"]["egressId"], egress_id);
    assert_eq!(started["room"]["name"], "wh-room");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let (status, _) = stop_egress(&base, &egress_id).await;
    assert_eq!(status, 200);
    let _ = stream.await;

    let ended = wait_for_event(&sink, "egress_ended").await;
    assert_eq!(ended["egressInfo"]["egressId"], egress_id);
    assert_eq!(ended["egressInfo"]["status"], "EGRESS_COMPLETE");
    assert_eq!(ended["room"]["name"], "wh-room");

    drop(pub_ws);
    drop(server);
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// `StopEgress` on an egress id that was never started reports a Twirp
/// `not_found` (the reference maps the store miss to `egress does not exist`).
#[tokio::test]
async fn stop_unknown_egress_errors() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let out_dir = std::env::temp_dir().join(format!("lk_egress_stop_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();
    let (server, base) = start_stack(&out_dir).await;
    let _ = server;

    let (status, body) = stop_egress(&base, "EG_nonexistent").await;
    assert_eq!(status, 404, "status: {status}");
    assert_eq!(body["code"], "not_found");
    assert_eq!(body["msg"], "egress does not exist");
}