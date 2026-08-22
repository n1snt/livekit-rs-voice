//! Shared helpers for the lk-server integration tests. Each test file includes
//! this module with `mod common;`; helpers that a given file does not use are
//! fine because this is a test-support module (never shipped).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;

use lk_server::config::Config;
use lk_server::http;
use lk_server::server::Server;

pub const API_KEY: &str = "devkey";
pub const SECRET: &str = "secret";

pub fn test_config() -> Config {
    Config {
        port: Some(0),
        keys: BTreeMap::from([(API_KEY.to_string(), SECRET.to_string())]),
        ..Default::default()
    }
}

/// Starts the server on an ephemeral port and returns it plus its base URL.
pub async fn start_server() -> (Arc<Server>, String) {
    start_server_with(test_config()).await
}

pub async fn start_server_with(config: Config) -> (Arc<Server>, String) {
    let server = Server::new(config);
    let app = http::router(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (server, format!("http://{addr}"))
}

/// A minimal `Config` with a webhook pointing at `url`.
pub fn config_with_webhook(url: String) -> Config {
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

/// Mints an HS256 JWT with the given `video` grant (identity "admin").
pub fn raw_token(video: serde_json::Value) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "admin",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": video
    });
    encode_token(&payload)
}

/// A join token for `identity`/`room` with all publish/subscribe grants.
pub fn join_token(identity: &str, room: &str) -> String {
    join_token_with(identity, room, serde_json::json!({}), serde_json::json!({}))
}

/// A join token with extra `video` grant keys and extra top-level claims
/// (e.g. `attributes`, `metadata`, `name`).
pub fn join_token_with(
    identity: &str,
    room: &str,
    extra_grants: serde_json::Value,
    extra_claims: serde_json::Value,
) -> String {
    let now = lk_server::core::unix_seconds();
    let mut video = serde_json::json!({
        "roomJoin": true,
        "room": room,
        "canPublish": true,
        "canSubscribe": true,
        "canPublishData": true
    });
    if let Some(obj) = extra_grants.as_object() {
        for (k, v) in obj {
            video[k] = v.clone();
        }
    }
    let mut payload = serde_json::json!({
        "iss": API_KEY,
        "sub": identity,
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": video
    });
    if let Some(obj) = extra_claims.as_object() {
        for (k, v) in obj {
            payload[k] = v.clone();
        }
    }
    encode_token(&payload)
}

fn encode_token(payload: &serde_json::Value) -> String {
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// A token signed with the wrong secret (to exercise 401 handling).
pub fn wrong_secret_token(video: serde_json::Value) -> String {
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
        &jsonwebtoken::EncodingKey::from_secret(b"wrong-secret"),
    )
    .unwrap()
}

/// An expired join token.
pub fn expired_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "admin",
        "iat": now - 7200,
        "nbf": now - 7205,
        "exp": now - 3600,
        "video": {"roomJoin": true, "room": "r"}
    });
    encode_token(&payload)
}

/// A token whose `iss` (API key) is unknown to the server.
pub fn unknown_key_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": "nope",
        "sub": "admin",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"roomJoin": true, "room": "r"}
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

// ---------------------------------------------------------------------------
// HTTP / Twirp helpers
// ---------------------------------------------------------------------------

/// POSTs a JSON Twirp request with a bearer token, returning (status, body).
pub async fn twirp(
    base: &str,
    service: &str,
    method: &str,
    token: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/{service}/{method}"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, value)
}

pub async fn twirp_unauthed(
    base: &str,
    service: &str,
    method: &str,
    body: serde_json::Value,
) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("{base}/twirp/{service}/{method}"))
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .status()
}

/// A token carrying `roomAdmin` (and friends) for `room`.
pub fn admin_token(room: &str, grants: serde_json::Value) -> String {
    let mut g = serde_json::json!({"roomAdmin": true});
    if let Some(obj) = grants.as_object() {
        for (k, v) in obj {
            g[k] = v.clone();
        }
    }
    if !room.is_empty() {
        g["room"] = serde_json::json!(room);
    }
    raw_token(g)
}

pub fn record_token(room: &str) -> String {
    raw_token(serde_json::json!({"roomRecord": true, "room": room}))
}

// ---------------------------------------------------------------------------
// Signaling helpers
// ---------------------------------------------------------------------------

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn ws_connect(base: &str, token: &str) -> Ws {
    let url = format!("{}/rtc?access_token={}", base.replace("http", "ws"), token);
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

pub async fn read_response(ws: &mut Ws) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error: {e}"),
            None => panic!("ws closed unexpectedly"),
        }
    }
}

pub async fn send_request(ws: &mut Ws, req: &lk::SignalRequest) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

/// Reads the first message on `ws` (the join response) and asserts it is a Join.
pub async fn expect_join(ws: &mut Ws) -> lk::JoinResponse {
    match read_response(ws).await.message {
        Some(lk::signal_response::Message::Join(j)) => j,
        other => panic!("expected Join, got {other:?}"),
    }
}

/// Reads until the given predicate matches, returning the matching response.
/// Non-matching responses are returned for the caller to inspect (unlike
/// `await_message`, this does not auto-consume trickles).
pub async fn await_message<F: Fn(&lk::SignalResponse) -> bool>(
    ws: &mut Ws,
    matches: F,
) -> lk::SignalResponse {
    for _ in 0..400 {
        let resp = read_response(ws).await;
        if matches(&resp) {
            return resp;
        }
    }
    panic!("timed out waiting for expected signal response");
}

/// Spawns a background task that sends a ping every 2s so the server does not
/// close the connection on its 15s ping timeout. Returns a handle; the task
/// stops when the sender is dropped (i.e. the websocket closes).
pub fn spawn_keepalive(ws: Arc<tokio::sync::Mutex<Ws>>) -> tokio::task::JoinHandle<()> {
    use futures_util::SinkExt;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let req = lk::SignalRequest {
                message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                    timestamp: lk_server::core::unix_micros() / 1000,
                    rtt: 0,
                })),
            };
            let mut ws = ws.lock().await;
            let encoded = req.encode_to_vec().into();
            if ws.send(Message::Binary(encoded)).await.is_err() {
                return;
            }
        }
    })
}
