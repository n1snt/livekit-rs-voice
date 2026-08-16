//! End-to-end integration tests for the HTTP + WebSocket signaling surface.

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;

use lk_server::auth::{KeyProvider, VerifiedToken};
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

/// Builds a join token for the given room/identity/grants.
fn raw_token(video: serde_json::Value) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "admin",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": video
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

fn join_token(identity: &str, room: &str, extra: serde_json::Value) -> String {
    let now = lk_server::core::unix_seconds();
    let mut video = serde_json::json!({
        "roomJoin": true,
        "room": room,
        "canPublish": true,
        "canSubscribe": true,
        "canPublishData": true
    });
    if let Some(obj) = extra.as_object() {
        for (k, v) in obj {
            video[k] = v.clone();
        }
    }
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": identity,
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": video
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

/// Starts the server on an ephemeral port and returns its base URL.
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

async fn ws_connect(
    base: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    // JWT characters (alphanumerics, `.`, `-`, `_`) are URL-safe.
    let url = format!("{}/rtc?access_token={}", base.replace("http", "ws"), token);
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

async fn read_response(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).unwrap();
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error: {e}"),
            None => panic!("ws closed unexpectedly"),
        }
    }
}

async fn send_request(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    req: &lk::SignalRequest,
) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

const OFFER_SDP: &str = "\
v=0\r\n\
o=- 0 0 IN IP4 127.0.0.1\r\n\
s=-\r\n\
t=0 0\r\n\
a=group:BUNDLE 0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
c=IN IP4 0.0.0.0\r\n\
a=mid:0\r\n\
a=setup:actpass\r\n\
a=ice-ufrag:aaaa\r\n\
a=ice-pwd:bbbb\r\n\
a=sendrecv\r\n\
a=rtpmap:111 opus/48000/2\r\n\
a=fmtp:111 minptime=10;useinbandfec=1\r\n\
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level\r\n\
a=fingerprint:sha-256 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00\r\n";

fn publish_offer() -> lk::SignalRequest {
    let mut mid_to_track_id = BTreeMap::new();
    mid_to_track_id.insert("0".to_string(), "mic1".to_string());
    lk::SignalRequest {
        message: Some(lk::signal_request::Message::Offer(lk::SessionDescription {
            r#type: "offer".to_string(),
            sdp: OFFER_SDP.to_string(),
            id: 0,
            mid_to_track_id,
        })),
    }
}

#[tokio::test]
async fn health_and_metrics() {
    let (_server, base) = start_server().await;
    let body = reqwest::get(format!("{base}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "OK");
    // Metrics are served on the dedicated prometheus port, not the main port.
    let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
    assert_ne!(resp.status(), 200);
}

#[tokio::test]
async fn validate_requires_join_grant() {
    let (_server, base) = start_server().await;
    let client = reqwest::Client::new();
    // Valid join token -> success
    let token = join_token("alice", "room-a", serde_json::json!({}));
    let resp = client
        .get(format!("{base}/rtc/validate"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "success");
    // No token -> 401
    let resp = client
        .get(format!("{base}/rtc/validate"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    // Join grant missing -> 403
    let admin_token = raw_token(serde_json::json!({"roomCreate": true, "room": "room-a"}));
    let resp = client
        .get(format!("{base}/rtc/validate"))
        .header("Authorization", format!("Bearer {admin_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn twirp_room_service_round_trip() {
    let (_server, base) = start_server().await;
    let client = reqwest::Client::new();
    let token = join_token(
        "admin",
        "room-a",
        serde_json::json!({
            "roomCreate": true, "roomList": true, "roomAdmin": true
        }),
    );

    let create = client
        .post(format!("{base}/twirp/livekit.RoomService/CreateRoom"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"name":"int-test-room","metadata":"{\"m\":1}"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), 200);
    let room: serde_json::Value = serde_json::from_str(&create.text().await.unwrap()).unwrap();
    assert_eq!(room["name"], "int-test-room");
    assert!(room["sid"].as_str().unwrap().starts_with("RM_"));

    let list = client
        .post(format!("{base}/twirp/livekit.RoomService/ListRooms"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    let rooms: serde_json::Value = serde_json::from_str(&list.text().await.unwrap()).unwrap();
    assert_eq!(rooms["rooms"].as_array().unwrap().len(), 1);

    // Delete requires roomCreate
    let del = client
        .post(format!("{base}/twirp/livekit.RoomService/DeleteRoom"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"room":"int-test-room"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 200);
}

#[tokio::test]
async fn twirp_requires_auth() {
    let (_server, base) = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.RoomService/ListRooms"))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn twirp_permissions_enforced() {
    let (_server, base) = start_server().await;
    let client = reqwest::Client::new();
    // Join-only token has no roomCreate -> CreateRoom must be forbidden
    let token = join_token("bob", "room-a", serde_json::json!({}));
    let resp = client
        .post(format!("{base}/twirp/livekit.RoomService/CreateRoom"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"name":"nope"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "permission_denied");
}

#[tokio::test]
async fn signaling_join_receives_join_and_offer() {
    let (_server, base) = start_server().await;
    let token = join_token("alice", "room-a", serde_json::json!({}));
    let mut ws = ws_connect(&base, &token).await;

    // 1. JoinResponse
    let resp = read_response(&mut ws).await;
    match resp.message {
        Some(lk::signal_response::Message::Join(join)) => {
            assert_eq!(join.room.as_ref().unwrap().name, "room-a");
            assert_eq!(join.participant.as_ref().unwrap().identity, "alice");
            assert!(join.subscriber_primary);
            assert_eq!(join.server_info.as_ref().unwrap().protocol, 17);
            assert_eq!(join.ping_interval, 5);
        }
        other => panic!("expected join, got {other:?}"),
    }

    // 2. Subscriber offer (server-initiated, data channels)
    let resp = read_response(&mut ws).await;
    match resp.message {
        Some(lk::signal_response::Message::Offer(offer)) => {
            assert_eq!(offer.r#type, "offer");
            // Data channels surface as an SCTP application section.
            assert!(
                offer.sdp.contains("application"),
                "offer lacks SCTP: {}",
                offer.sdp
            );
        }
        other => panic!("expected subscriber offer, got {other:?}"),
    }

    // 3. Ping -> Pong
    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                timestamp: 12345,
                rtt: 0,
            })),
        },
    )
    .await;
    let resp = loop {
        match read_response(&mut ws).await.message {
            Some(lk::signal_response::Message::Trickle(_)) => continue,
            other => break other,
        }
    };
    assert!(matches!(
        resp,
        Some(lk::signal_response::Message::PongResp(p))
        if p.last_ping_timestamp == 12345
    ));

    // 4. Publisher offer -> answer
    send_request(&mut ws, &publish_offer()).await;
    let resp = read_response(&mut ws).await;
    match resp.message {
        Some(lk::signal_response::Message::Answer(answer)) => {
            assert_eq!(answer.r#type, "answer");
            assert!(answer.sdp.contains("opus"));
        }
        other => panic!("expected answer, got {other:?}"),
    }

    // 5. Leave -> participant removed from room
    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Leave(lk::LeaveRequest {
                reason: lk::DisconnectReason::ClientInitiated as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(_server.get_room("room-a").is_some());
    let room = _server.get_room("room-a").unwrap();
    assert_eq!(room.num_participants(), 0);
}

#[tokio::test]
async fn add_track_returns_track_published() {
    let (_server, base) = start_server().await;
    let token = join_token("carol", "room-b", serde_json::json!({}));
    let mut ws = ws_connect(&base, &token).await;
    let _join = read_response(&mut ws).await;
    let _offer = read_response(&mut ws).await;

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::AddTrack(lk::AddTrackRequest {
                cid: "mic1".to_string(),
                name: "microphone".to_string(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                ..Default::default()
            })),
        },
    )
    .await;

    let resp = loop {
        match read_response(&mut ws).await.message {
            Some(lk::signal_response::Message::Trickle(_)) => continue,
            other => break other,
        }
    };
    match resp {
        Some(lk::signal_response::Message::TrackPublished(tp)) => {
            assert_eq!(tp.cid, "mic1");
            let track = tp.track.unwrap();
            assert!(track.sid.starts_with("TR_"));
            assert_eq!(track.mime_type, "audio/opus");
        }
        other => panic!("expected trackPublished, got {other:?}"),
    }
}

#[tokio::test]
async fn auth_module_integration() {
    let keys = BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]);
    let provider = KeyProvider::from_map(keys);
    let token = join_token("dave", "room-c", serde_json::json!({}));
    let verified: VerifiedToken = provider.verify(&token).unwrap();
    assert_eq!(verified.identity, "dave");
    assert!(verified.video.room_join);
    assert_eq!(verified.video.room, "room-c");
}

#[tokio::test]
async fn agent_worker_registration_and_job_dispatch() {
    let (_server, base) = start_server().await;
    // Agent worker token.
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "worker-1",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"agent": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    let worker_token = jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();

    let url = format!(
        "{}/agent?access_token={}",
        base.replace("http", "ws"),
        worker_token
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Register the worker.
    use futures_util::SinkExt;
    ws.send(Message::Binary(
        lk::WorkerMessage {
            message: Some(lk::worker_message::Message::Register(
                lk::RegisterWorkerRequest {
                    r#type: lk::JobType::JtRoom as i32,
                    agent_name: "voice-agent".to_string(),
                    version: "1.0".to_string(),
                    ping_interval: 5,
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec()
        .into(),
    ))
    .await
    .unwrap();

    // Expect RegisterWorkerResponse.
    let resp = loop {
        match futures_util::StreamExt::next(&mut ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                break lk::ServerMessage::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(_)) => continue,
            other => panic!("unexpected: {other:?}"),
        }
    };
    let worker_id = match resp.message {
        Some(lk::server_message::Message::Register(r)) => {
            assert_eq!(r.server_info.as_ref().unwrap().agent_protocol, 1);
            r.worker_id
        }
        other => panic!("expected register, got {other:?}"),
    };

    // Dispatch a job by joining a room whose token requests an agent.
    let join = join_token("caller", "agent-room", serde_json::json!({}));
    let join_payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "caller",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"roomJoin": true, "room": "agent-room", "canPublish": true, "canSubscribe": true, "canPublishData": true},
        "roomConfig": {"agents": [{"agentName": "voice-agent", "metadata": "{}"}]}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    let join_token = jsonwebtoken::encode(
        &header,
        &join_payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    let _ = join;

    let join_url = format!(
        "{}/rtc?access_token={}",
        base.replace("http", "ws"),
        join_token
    );
    let (mut join_ws, _) = tokio_tungstenite::connect_async(&join_url).await.unwrap();
    // Drain join response + subscriber offer.
    for _ in 0..2 {
        let _ = read_response(&mut join_ws).await;
    }

    // The worker should receive an AvailabilityRequest for the room job.
    let avail = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match futures_util::StreamExt::next(&mut ws).await {
                Some(Ok(Message::Binary(bytes))) => {
                    let msg = lk::ServerMessage::decode(bytes.as_ref()).unwrap();
                    match msg.message {
                        Some(lk::server_message::Message::Availability(a)) => return a,
                        Some(lk::server_message::Message::Pong(_)) => continue,
                        other => panic!("expected availability, got {other:?}"),
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("unexpected: {other:?}"),
            }
        }
    })
    .await
    .expect("worker never received availability request");

    let job = avail.job.unwrap();
    assert_eq!(job.agent_name, "voice-agent");
    assert_eq!(job.room.as_ref().unwrap().name, "agent-room");

    // Worker accepts the job.
    ws.send(Message::Binary(
        lk::WorkerMessage {
            message: Some(lk::worker_message::Message::Availability(
                lk::AvailabilityResponse {
                    job_id: job.id.clone(),
                    available: true,
                    supports_resume: true,
                    participant_identity: format!("agent-{}", job.id),
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec()
        .into(),
    ))
    .await
    .unwrap();

    // Worker receives a JobAssignment with a token.
    let assignment = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match futures_util::StreamExt::next(&mut ws).await {
                Some(Ok(Message::Binary(bytes))) => {
                    let msg = lk::ServerMessage::decode(bytes.as_ref()).unwrap();
                    match msg.message {
                        Some(lk::server_message::Message::Assignment(a)) => return a,
                        Some(lk::server_message::Message::Pong(_)) => continue,
                        other => panic!("expected assignment, got {other:?}"),
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("unexpected: {other:?}"),
            }
        }
    })
    .await
    .expect("worker never received job assignment");

    // The assignment token must let the agent join the room as kind AGENT.
    let provider =
        KeyProvider::from_map(BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]));
    let verified = provider.verify(&assignment.token).unwrap();
    assert_eq!(verified.kind.to_uppercase(), "AGENT");
    assert_eq!(verified.video.room, "agent-room");
    assert!(verified.can_publish());
    assert!(!worker_id.is_empty());
}
