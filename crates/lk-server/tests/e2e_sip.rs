//! End-to-end SIP tests: `TransferSIPParticipant` bridges to the
//! `livekit/sip` container over psrpc (topic = sip call id), with the
//! reference validation and permission semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::internal;
use lk_proto::livekit as lk;
use prost::Message as _;

use futures_util::StreamExt;
use lk_server::config::Config;
use lk_server::http;
use lk_server::psrpc::{self, MemoryBus, PsrpcBus};
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

fn encode(payload: serde_json::Value) -> String {
    let now = lk_server::core::unix_seconds();
    let mut payload = payload;
    payload["iss"] = serde_json::json!(API_KEY);
    payload["iat"] = serde_json::json!(now);
    payload["nbf"] = serde_json::json!(now - 5);
    payload["exp"] = serde_json::json!(now + 3600);
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// A token carrying both the sip.call grant and roomAdmin on the target room
/// (what the agents SDK uses for `TransferSIPParticipant`).
fn transfer_token(room: &str) -> String {
    encode(serde_json::json!({
        "sub": "admin",
        "sip": {"call": true},
        "video": {"roomAdmin": true, "room": room}
    }))
}

/// A join token for a participant that carries SIP attributes.
fn sip_join_token(identity: &str, room: &str, attributes: serde_json::Value) -> String {
    encode(serde_json::json!({
        "sub": identity,
        "attributes": attributes,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    }))
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
            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap();
            }
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                return serde_json::from_str(&text).unwrap();
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("ws error: {e}"),
            None => panic!("ws closed"),
        }
    }
}

/// A psrpc server acting as the `livekit/sip` bridge: it registers a handler
/// for `TransferSIPParticipant` on a given call-id topic and records the
/// request payloads it receives.
fn spawn_transfer_bridge(
    bus: Arc<MemoryBus>,
    sip_call_id: &str,
) -> Arc<tokio::sync::Mutex<Vec<Vec<u8>>>> {
    let received: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let received2 = received.clone();
    let bus2 = bus.clone();
    let topic = sip_call_id.to_string();
    tokio::spawn(async move {
        let rpc_ch = psrpc::rpc_channel("SIPInternal", "TransferSIPParticipant", &topic);
        let rclaim_ch =
            psrpc::claim_response_channel("SIPInternal", "TransferSIPParticipant", &topic);
        let mut stream = bus2
            .subscribe(vec![rpc_ch.clone(), rclaim_ch.clone()])
            .await
            .unwrap();
        let mut pending: BTreeMap<String, String> = BTreeMap::new();
        while let Some((channel, payload)) = stream.next().await {
            let Ok(env) = internal::Msg::decode(payload.as_slice()) else {
                continue;
            };
            if channel == rpc_ch {
                let Ok(req) = internal::Request::decode(env.value.as_slice()) else {
                    continue;
                };
                received2.lock().await.push(req.raw_request.clone());
                pending.insert(req.request_id.clone(), req.client_id.clone());
                let claim = internal::ClaimRequest {
                    request_id: req.request_id.clone(),
                    server_id: "SRV_sip_mock".to_string(),
                    affinity: 1.0,
                    handling: false,
                };
                let _ = bus2
                    .publish(
                        &psrpc::claim_request_channel("SIPInternal", &req.client_id),
                        psrpc::envelope("internal.ClaimRequest", &claim),
                    )
                    .await;
            } else if channel == rclaim_ch {
                let Ok(grant) = internal::ClaimResponse::decode(env.value.as_slice()) else {
                    continue;
                };
                let Some(client_id) = pending.remove(&grant.request_id) else {
                    continue;
                };
                let resp = internal::Response {
                    request_id: grant.request_id,
                    server_id: "SRV_sip_mock".to_string(),
                    sent_at: lk_server::psrpc::unix_nanos(),
                    raw_response: lk_proto::well_known::Empty {}.encode_to_vec(),
                    ..Default::default()
                };
                let _ = bus2
                    .publish(
                        &psrpc::response_channel("SIPInternal", &client_id),
                        psrpc::envelope("internal.Response", &resp),
                    )
                    .await;
            }
        }
    });
    received
}

async fn start_server_with_bus() -> (Arc<Server>, String, Arc<MemoryBus>) {
    let config = test_config();
    let server = Server::new(config);
    let bus = MemoryBus::new();
    let _client = server.sip_client_with(bus.clone()).await.unwrap();
    let app = http::router(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (server, format!("http://{addr}"), bus)
}

/// The full transfer flow: a SIP participant (with `sip.callID`) is in the
/// room; the transfer request reaches the `livekit/sip` bridge on the per-call
/// topic with the expected fields.
#[tokio::test]
async fn transfer_bridges_to_sip_container() {
    let (server, base, bus) = start_server_with_bus().await;
    let _ = server;

    // A SIP participant joins the room with the attributes the SIP bridge
    // would have set (sip.callID from the outbound flow).
    let mut ws = ws_connect(
        &base,
        &sip_join_token(
            "sip_+1777",
            "transfer-room",
            serde_json::json!({"sip.callID": "SC_transfer1", "sip.trunkID": "ST_1"}),
        ),
    )
    .await;
    let _join = read_response(&mut ws).await;

    // The SIP bridge listens for the transfer on the call-id topic.
    let received = spawn_transfer_bridge(bus, "SC_transfer1");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", transfer_token("transfer-room")))
        .body(
            r#"{"room_name":"transfer-room","participant_identity":"sip_+1777","transfer_to":"sip:+1999@example.com"}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());

    // The bridge received exactly one well-formed request on the per-call topic.
    let got = received.lock().await.clone();
    assert_eq!(got.len(), 1, "transfer request must reach the bridge once");
    let ireq =
        lk_proto::rpc::InternalTransferSipParticipantRequest::decode(got[0].as_slice()).unwrap();
    assert_eq!(ireq.sip_call_id, "SC_transfer1");
    assert_eq!(ireq.transfer_to, "sip:+1999@example.com");
    assert!(!ireq.play_dialtone);
    drop(ws);
}

/// Transferring without the roomAdmin grant is rejected (reference
/// `transferSIPParticipantRequest` checks sip.call AND roomAdmin).
#[tokio::test]
async fn transfer_requires_room_admin() {
    let (server, base, _bus) = start_server_with_bus().await;
    let _ = server;

    // sip.call without roomAdmin on the target room.
    let token = encode(serde_json::json!({"sub": "admin", "sip": {"call": true}}));
    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(
            r#"{"room_name":"transfer-room","participant_identity":"sip_+1777","transfer_to":"sip:+1999@example.com"}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "unauthenticated");
}

/// A participant without the `sip.callID` attribute cannot be transferred.
#[tokio::test]
async fn transfer_requires_sip_session() {
    let (server, base, _bus) = start_server_with_bus().await;
    let _ = server;

    // Join a regular participant (no SIP attributes).
    let mut ws = ws_connect(
        &base,
        &sip_join_token("plain-user", "plain-room", serde_json::json!({})),
    )
    .await;
    let _join = read_response(&mut ws).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", transfer_token("plain-room")))
        .body(
            r#"{"room_name":"plain-room","participant_identity":"plain-user","transfer_to":"sip:+1999@example.com"}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "invalid_argument");
    assert_eq!(body["msg"], "no SIP session associated with participant");
    drop(ws);
}

/// Transferring a participant that does not exist is not_found.
#[tokio::test]
async fn transfer_missing_participant_is_not_found() {
    let (server, base, _bus) = start_server_with_bus().await;
    let _ = server;

    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", transfer_token("empty-room")))
        .body(
            r#"{"room_name":"empty-room","participant_identity":"nobody","transfer_to":"sip:+1999@example.com"}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "not_found");
}

/// Missing required fields are invalid_argument (reference validation).
#[tokio::test]
async fn transfer_validates_required_fields() {
    let (server, base, _bus) = start_server_with_bus().await;
    let _ = server;

    let client = reqwest::Client::new();
    // Missing room name.
    let resp = client
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", transfer_token("any")))
        .body(r#"{"participant_identity":"x","transfer_to":"sip:+1@x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "invalid_argument");
    assert_eq!(body["msg"], "Missing room name");

    // Missing identity.
    let resp = client
        .post(format!("{base}/twirp/livekit.SIP/TransferSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", transfer_token("any")))
        .body(r#"{"room_name":"r","transfer_to":"sip:+1@x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "invalid_argument");
    assert_eq!(body["msg"], "Missing participant identity");
}
