//! Multi-node integration test: two in-process `Server` nodes sharing an
//! in-memory cluster bus. A client connected to node A joins a room hosted on
//! node B, and signaling (join, ping/pong, leave) flows over the Redis-style
//! relay.

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;

use lk_server::cluster::{Cluster, MemoryBus};
use lk_server::config::Config;
use lk_server::http;
use lk_server::server::Server;

const API_KEY: &str = "devkey";
const SECRET: &str = "secret";

fn test_config() -> Config {
    Config {
        port: Some(0),
        keys: BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        room: lk_server::config::RoomConfig {
            empty_timeout: 1,
            departure_timeout: 1,
            ..Default::default()
        },
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

/// Starts a cluster node with the given id and returns its base URL.
async fn start_node(bus: Arc<MemoryBus>, node_id: &str) -> (Arc<Server>, String) {
    let cluster = Cluster::new_with_bus(bus, node_id, true);
    let server = Server::with_cluster(test_config(), cluster);
    server.start_background_tasks();
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

#[tokio::test]
async fn client_joins_room_on_remote_node_via_relay() {
    let bus = Arc::new(MemoryBus::default());
    let (node_a, base_a) = start_node(bus.clone(), "node-a").await;
    let (node_b, base_b) = start_node(bus.clone(), "node-b").await;
    let _ = node_a;

    // Client 1 connects to node B and creates the room there.
    let tok_b = join_token("caller-1", "cluster-room");
    let mut ws_b = ws_connect(&base_b, &tok_b).await;
    let join_b = read_response(&mut ws_b).await;
    assert!(matches!(
        join_b.message,
        Some(lk::signal_response::Message::Join(ref j))
        if j.room.as_ref().map(|r| r.name.as_str()) == Some("cluster-room")
    ));
    let _ = read_response(&mut ws_b).await; // subscriber offer

    // Client 2 connects to node A for the same room. Node A should route to
    // node B and the join must be relayed.
    let tok_a = join_token("caller-2", "cluster-room");
    let mut ws_a = ws_connect(&base_a, &tok_a).await;
    let join_a = read_response(&mut ws_a).await;
    match join_a.message {
        Some(lk::signal_response::Message::Join(ref j)) => {
            assert_eq!(j.room.as_ref().unwrap().name, "cluster-room");
            assert_eq!(j.participant.as_ref().unwrap().identity, "caller-2");
        }
        other => panic!("expected relayed join, got {other:?}"),
    }
    let _ = read_response(&mut ws_a).await; // subscriber offer (relayed)

    // Ping/pong through the relay.
    send_request(
        &mut ws_a,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                timestamp: 12345,
                rtt: 0,
            })),
        },
    )
    .await;
    let resp = loop {
        match read_response(&mut ws_a).await.message {
            Some(lk::signal_response::Message::Trickle(_)) => continue,
            other => break other,
        }
    };
    assert!(matches!(
        resp,
        Some(lk::signal_response::Message::PongResp(p)) if p.last_ping_timestamp == 12345
    ));

    // Both participants are hosted on node B.
    let room_b = node_b.get_room("cluster-room").expect("room on node B");
    assert_eq!(room_b.num_participants(), 2);
    // Node A must NOT host the room (it was relayed away).
    assert!(node_a.get_room("cluster-room").is_none());

    // Client 2 leaves through the relay; participant count drops.
    send_request(
        &mut ws_a,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Leave(lk::LeaveRequest {
                reason: lk::DisconnectReason::ClientInitiated as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(room_b.num_participants(), 1);

    // Client 1 also leaves; the room closes and releases its registry entry.
    send_request(
        &mut ws_b,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Leave(lk::LeaveRequest {
                reason: lk::DisconnectReason::ClientInitiated as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(room_b.num_participants(), 0);
    // Let the departure timeout elapse so the room closes and releases its
    // registry entry.
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
    let owner = node_b
        .cluster
        .bus
        .hget("lk:rooms", "cluster-room")
        .await
        .unwrap();
    assert!(
        owner.is_none(),
        "room registry entry should be released: {owner:?}"
    );
}
