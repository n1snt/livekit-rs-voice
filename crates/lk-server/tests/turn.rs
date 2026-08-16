//! Integration test for TURN: with `turn.enabled`, the JoinResponse must carry
//! a valid ICE server (URL + long-term credentials matching the reference
//! scheme), and the embedded TURN server must start.

use std::collections::BTreeMap;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;

use lk_server::config::Config;
use lk_server::http;
use lk_server::server::Server;

const API_KEY: &str = "devkey";
const SECRET: &str = "secret";

fn turn_config() -> Config {
    Config {
        port: Some(0),
        keys: BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        turn: lk_server::config::TurnConfig {
            enabled: true,
            udp_port: 3479,
            ttl: 300,
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

#[tokio::test]
async fn join_response_carries_turn_ice_server() {
    let server = Server::new(turn_config());
    server.start_background_tasks();
    let app = http::router(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let base = format!("http://{addr}");

    let token = join_token("turn-test", "room-a");
    let url = format!("{}/rtc?access_token={}", base.replace("http", "ws"), token);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // First frame: JoinResponse.
    let resp = loop {
        match futures_util::StreamExt::next(&mut ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                break lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(_)) => continue,
            other => panic!("unexpected: {other:?}"),
        }
    };
    let join = match resp.message {
        Some(lk::signal_response::Message::Join(j)) => j,
        other => panic!("expected join, got {other:?}"),
    };

    assert_eq!(
        join.ice_servers.len(),
        1,
        "ice_servers should be advertised"
    );
    let ice = &join.ice_servers[0];
    assert_eq!(ice.urls.len(), 1);
    assert!(
        ice.urls[0].starts_with("turn:") && ice.urls[0].contains(":3479"),
        "unexpected turn url: {}",
        ice.urls[0]
    );

    // The username must decode (base62) to apiKey|participantSid|expiry and the
    // credential must match base62(sha256(secret|sid|expiry)).
    let decoded = lk_server::turn::base62_decode(&ice.username).expect("username must be base62");
    let decoded = String::from_utf8(decoded).unwrap();
    let parts: Vec<&str> = decoded.split('|').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], API_KEY);
    assert!(parts[1].starts_with("PA_"));
    let expiry: i64 = parts[2].parse().unwrap();
    assert!(expiry > lk_server::core::unix_seconds());

    let expected = lk_server::turn::turn_password(SECRET, parts[1], expiry);
    assert_eq!(ice.credential, expected, "credential must match the scheme");
}
