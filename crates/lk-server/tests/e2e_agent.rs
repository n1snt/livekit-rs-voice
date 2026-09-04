//! End-to-end tests for the agent worker protocol (`/agent` WebSocket),
//! emulating what the `livekit-agents` SDK does: register a worker, receive a
//! job availability request, accept it, get a job assignment with a join
//! token, join the room as an agent, and report job status.

mod common;

use common::*;
use lk_proto::livekit as lk;
use prost::Message as _;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A token with the `video.agent` grant (worker registration).
fn agent_worker_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY, "sub": "worker-1", "iat": now, "nbf": now - 5, "exp": now + 3600,
        "video": {"agent": true}
    });
    encode(payload)
}

fn encode(payload: serde_json::Value) -> String {
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// Opens the `/agent` websocket with a bearer token in the Authorization
/// header (exactly what the agents SDK does).
async fn ws_connect_agent(base: &str, token: &str) -> Ws {
    let url = format!("{}/agent", base.replace("http", "ws"));
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    ws
}

async fn read_server_msg(ws: &mut Ws) -> lk::ServerMessage {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::ServerMessage::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error: {e}"),
            None => panic!("ws closed unexpectedly"),
        }
    }
}

async fn send_worker_msg(ws: &mut Ws, msg: &lk::WorkerMessage) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(msg.encode_to_vec().into()))
        .await
        .unwrap();
}

async fn read_until<F: Fn(&lk::ServerMessage) -> bool>(
    ws: &mut Ws,
    matches: F,
) -> lk::ServerMessage {
    for _ in 0..400 {
        let msg = read_server_msg(ws).await;
        if matches(&msg) {
            return msg;
        }
    }
    panic!("timed out waiting for server message");
}

fn register_worker_request(agent_name: &str) -> lk::WorkerMessage {
    lk::WorkerMessage {
        message: Some(lk::worker_message::Message::Register(
            lk::RegisterWorkerRequest {
                r#type: lk::JobType::JtRoom as i32,
                agent_name: agent_name.to_string(),
                version: "1.1.14".to_string(),
                ping_interval: 10,
                allowed_permissions: Some(lk::ParticipantPermission {
                    can_publish: true,
                    can_subscribe: true,
                    can_publish_data: true,
                    agent: true,
                    ..Default::default()
                }),
                deployment: "test".to_string(),
                ..Default::default()
            },
        )),
    }
}

/// Registers a worker on `/agent` and returns (worker id, server info).
async fn register_worker(base: &str) -> (Ws, String, lk::ServerInfo) {
    let mut ws = ws_connect_agent(base, &agent_worker_token()).await;
    send_worker_msg(&mut ws, &register_worker_request("voice-agent")).await;
    let resp = read_until(&mut ws, |m| {
        matches!(m.message, Some(lk::server_message::Message::Register(_)))
    })
    .await;
    let register = match resp.message {
        Some(lk::server_message::Message::Register(r)) => r,
        other => panic!("expected register response, got {other:?}"),
    };
    assert!(!register.worker_id.is_empty());
    assert_eq!(register.server_info.as_ref().unwrap().edition, 0);
    (ws, register.worker_id, register.server_info.unwrap())
}

/// The worker pings and the server pongs with echo timestamps.
#[tokio::test]
async fn worker_registers_and_pings() {
    let (_server, base) = start_server().await;
    let (mut ws, worker_id, info) = register_worker(&base).await;
    assert!(info.node_id.len() >= 12);
    assert!(info.protocol > 0);

    let ts = lk_server::core::unix_micros() / 1000;
    send_worker_msg(
        &mut ws,
        &lk::WorkerMessage {
            message: Some(lk::worker_message::Message::Ping(lk::WorkerPing {
                timestamp: ts,
            })),
        },
    )
    .await;
    let pong = read_until(&mut ws, |m| {
        matches!(m.message, Some(lk::server_message::Message::Pong(_)))
    })
    .await;
    match pong.message {
        Some(lk::server_message::Message::Pong(p)) => {
            assert_eq!(p.last_timestamp, ts);
            assert!(p.timestamp > 0);
        }
        other => panic!("expected pong, got {other:?}"),
    }
    drop(ws);
    let _ = worker_id;
}

/// Full dispatch flow: a room with an agent dispatch, a registered worker, the
/// availability handshake, and a job assignment whose token lets the agent
/// join the room.
#[tokio::test]
async fn dispatch_assigns_job_and_agent_joins() {
    let (_server, base) = start_server().await;
    let (mut ws, _worker_id, _info) = register_worker(&base).await;

    // Create the room and dispatch through the Twirp API (what an app does).
    let admin = admin_token("", json!({"roomCreate": true, "roomList": true}));
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "CreateRoom",
        &admin,
        json!({"name": "agent-job-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);

    let room_admin = admin_token("agent-job-room", json!({}));
    let (status, dispatch) = twirp(
        &base,
        "livekit.AgentDispatchService",
        "CreateDispatch",
        &room_admin,
        json!({"agentName": "voice-agent", "room": "agent-job-room", "metadata": "{}"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let dispatch_id = dispatch["id"].as_str().unwrap().to_string();
    assert!(dispatch_id.starts_with("DA_"));

    // The server asks the worker for availability.
    let availability = read_until(&mut ws, |m| {
        matches!(m.message, Some(lk::server_message::Message::Availability(_)))
    })
    .await;
    let job = match availability.message {
        Some(lk::server_message::Message::Availability(a)) => a.job.unwrap(),
        other => panic!("expected availability request, got {other:?}"),
    };
    assert_eq!(job.agent_name, "voice-agent");
    assert_eq!(job.room.as_ref().unwrap().name, "agent-job-room");
    assert_eq!(job.dispatch_id, dispatch_id);
    assert_eq!(job.state.as_ref().unwrap().status, lk::JobStatus::JsPending as i32);

    // Accept the job, asking for a specific agent identity.
    send_worker_msg(
        &mut ws,
        &lk::WorkerMessage {
            message: Some(lk::worker_message::Message::Availability(
                lk::AvailabilityResponse {
                    job_id: job.id.clone(),
                    available: true,
                    participant_identity: "my-agent".to_string(),
                    participant_name: "My Agent".to_string(),
                    participant_metadata: "{}".to_string(),
                    participant_attributes: std::collections::BTreeMap::from([(
                        "lk.agent.name".to_string(),
                        "voice-agent".to_string(),
                    )]),
                    ..Default::default()
                },
            )),
        },
    )
    .await;

    // The server assigns the job with a token.
    let assignment = read_until(&mut ws, |m| {
        matches!(m.message, Some(lk::server_message::Message::Assignment(_)))
    })
    .await;
    let assigned = match assignment.message {
        Some(lk::server_message::Message::Assignment(a)) => a,
        other => panic!("expected assignment, got {other:?}"),
    };
    assert_eq!(assigned.job.as_ref().unwrap().id, job.id);
    assert!(!assigned.token.is_empty());
    assert!(assigned.url.is_none());

    // The token grants room join as the requested agent identity.
    let provider = _server.keys.clone();
    let verified = provider.verify(&assigned.token).unwrap();
    assert_eq!(verified.identity, "my-agent");
    assert_eq!(verified.video.room, "agent-job-room");
    assert!(verified.video.room_join);
    assert!(verified.video.agent);

    // The agent joins the room with the assigned token.
    let mut agent_ws = ws_connect(&base, &assigned.token).await;
    let join = expect_join(&mut agent_ws).await;
    let participant = join.participant.as_ref().expect("join has participant");
    assert_eq!(participant.identity, "my-agent");
    assert_eq!(participant.name, "My Agent");
    assert_eq!(participant.metadata, "{}");
    assert_eq!(
        participant.attributes.get("lk.agent.name"),
        Some(&"voice-agent".to_string())
    );

    // The worker reports job status (JS_RUNNING) without error.
    send_worker_msg(
        &mut ws,
        &lk::WorkerMessage {
            message: Some(lk::worker_message::Message::UpdateJob(
                lk::UpdateJobStatus {
                    job_id: job.id,
                    status: lk::JobStatus::JsRunning as i32,
                    ..Default::default()
                },
            )),
        },
    )
    .await;

    // ListDispatch surfaces the running job.
    let (status, list) = twirp(
        &base,
        "livekit.AgentDispatchService",
        "ListDispatch",
        &room_admin,
        json!({"room": "agent-job-room", "dispatchId": dispatch_id}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let dispatches = list["agentDispatches"].as_array().unwrap();
    assert_eq!(dispatches.len(), 1);
    let jobs = dispatches[0]["state"]["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["agentName"], "voice-agent");

    // Cleanup: remove the agent so the room can close.
    drop(agent_ws);
    drop(ws);
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "DeleteRoom",
        &admin,
        json!({"room": "agent-job-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
}

/// A worker that declines the job leaves it unassigned; the job stays pending
/// and is not dispatched to another worker.
#[tokio::test]
async fn declined_availability_is_not_assigned() {
    let (_server, base) = start_server().await;
    let (mut ws, _worker_id, _info) = register_worker(&base).await;

    let admin = admin_token("", json!({"roomCreate": true}));
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "CreateRoom",
        &admin,
        json!({"name": "decline-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let room_admin = admin_token("decline-room", json!({}));
    let (status, _) = twirp(
        &base,
        "livekit.AgentDispatchService",
        "CreateDispatch",
        &room_admin,
        json!({"agentName": "voice-agent", "room": "decline-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);

    let availability = read_until(&mut ws, |m| {
        matches!(m.message, Some(lk::server_message::Message::Availability(_)))
    })
    .await;
    let job_id = match availability.message {
        Some(lk::server_message::Message::Availability(a)) => a.job.unwrap().id,
        other => panic!("expected availability, got {other:?}"),
    };
    send_worker_msg(
        &mut ws,
        &lk::WorkerMessage {
            message: Some(lk::worker_message::Message::Availability(
                lk::AvailabilityResponse {
                    job_id: job_id.clone(),
                    available: false,
                    ..Default::default()
                },
            )),
        },
    )
    .await;

    // No assignment is sent.
    let timeout = std::time::Duration::from_secs(2);
    let extra = tokio::time::timeout(timeout, read_until(&mut ws, |_| true)).await;
    assert!(
        extra.is_err(),
        "worker must not receive an assignment after declining"
    );
    drop(ws);
}

/// Registering with a token that lacks the `video.agent` grant is rejected.
#[tokio::test]
async fn worker_requires_agent_grant() {
    let (_server, base) = start_server().await;
    let token = raw_token(serde_json::json!({"roomJoin": true, "room": "r"}));
    let url = format!("{}/agent", base.replace("http", "ws"));
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {token}").parse().unwrap(),
    );
    let resp = tokio_tungstenite::connect_async(request).await;
    // The server rejects the upgrade with 401.
    let err = resp.err().expect("connection must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("401") || msg.contains("permission"),
        "expected auth rejection, got: {msg}"
    );
}