//! Wire-compatibility tests for webhooks: the reference lifecycle events are
//! emitted and each POST carries a valid `X-Livekit-Signature` (hex
//! HMAC-SHA256 of the body with the API secret).

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use hmac::{Hmac, Mac};
use lk_proto::livekit as lk;
use serde_json::json;
use sha2::Sha256;

type Sink = Arc<Mutex<Vec<(Vec<u8>, String)>>>;

/// Spins up a webhook receiver that records `(body, signature)` pairs.
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

/// Verifies a signature is `hex(HMAC-SHA256(apiSecret, body))`.
fn check_signature(secret: &str, body: &[u8], sig: &str) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    let expected: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    expected == sig
}

async fn wait_for_event(sink: &Sink, event: &str) -> (serde_json::Value, bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        {
            let events = sink.lock().unwrap();
            for (body, sig) in events.iter() {
                let v: serde_json::Value = serde_json::from_slice(body).unwrap();
                if v["event"] == event {
                    let ok = check_signature(SECRET, body, sig);
                    return (v, ok);
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

#[tokio::test]
async fn webhook_lifecycle_and_signature() {
    let (webhook_url, sink) = start_receiver().await;
    let (_server, base) = start_server_with(config_with_webhook(webhook_url)).await;
    let admin = admin_token("", json!({"roomCreate": true}));

    let mut ws = ws_connect(&base, &join_token("alice", "wh-room")).await;
    let _join = expect_join(&mut ws).await;

    // room_started + participant_joined fire on the first join.
    let (room_started, ok) = wait_for_event(&sink, "room_started").await;
    assert!(ok, "room_started must be HMAC-signed with the API secret");
    assert_eq!(room_started["room"]["name"], "wh-room");
    assert!(!room_started["room"]["sid"].as_str().unwrap().is_empty());

    let (joined, ok) = wait_for_event(&sink, "participant_joined").await;
    assert!(ok);
    assert_eq!(joined["participant"]["identity"], "alice");

    // Note: track_published fires on the media plane (when a publisher's RTP
    // arrives), exercised by the media loopback tests — not on AddTrack alone.

    // Leave -> participant_left.
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
    let (left, ok) = wait_for_event(&sink, "participant_left").await;
    assert!(ok);
    assert_eq!(left["participant"]["identity"], "alice");

    // DeleteRoom -> room_finished.
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "DeleteRoom",
        &admin,
        json!({"room": "wh-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let (finished, ok) = wait_for_event(&sink, "room_finished").await;
    assert!(ok);
    assert_eq!(finished["room"]["name"], "wh-room");
}
