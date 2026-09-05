use futures_util::StreamExt;
use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() {
    let key = "lk_OtUnBBBpXjRF4zDCq4ObtEs0mfMxUcd0";
    let secret = "lks_uVl7Prz8EwYxeAFKWr7lJLE6ea8V7jva1DibCERMAwUtu9BS";
    let room = format!(
        "diag-stun-{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(6)
            .collect::<String>()
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = serde_json::json!({
        "iss": key, "sub": format!("diag-{}", uuid::Uuid::new_v4().simple()), "name": "diag",
        "iat": now - 5, "nbf": now - 5, "exp": now + 300,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    });
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();
    turn_creds();
    let url = format!(
        "ws://216.48.182.132:7880/rtc?access_token={token}&reconnect=false&auto_subscribe=true"
    );
    let (mut ws, _) = connect_async(&url).await.expect("ws connect");
    let mut el = 0;
    for _ in 0..40 {
        let msg = match tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => {
                el += 5;
                if el > 60 {
                    break;
                }
                continue;
            }
        };
        if let tokio_tungstenite::tungstenite::Message::Binary(bytes) = msg {
            let resp = lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            if let Some(lk::signal_response::Message::Trickle(t)) = resp.message {
                println!("{}", t.candidate_init);
            }
        }
    }
}
#[allow(dead_code)]
fn turn_creds() {
    let key = "lk_OtUnBBBpXjRF4zDCq4ObtEs0mfMxUcd0";
    let secret = "lks_uVl7Prz8EwYxeAFKWr7lJLE6ea8V7jva1DibCERMAwUtu9BS";
    let sid = "PA_diag_turn_test";
    let expiry = crate_turn_expiry();
    println!(
        "TURN_USERNAME={}",
        lk_server::turn::turn_username(key, sid, expiry)
    );
    println!(
        "TURN_PASSWORD={}",
        lk_server::turn::turn_password(secret, sid, expiry)
    );
    println!("TURN_EXPIRY={}", expiry);
}

fn crate_turn_expiry() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 300
}
