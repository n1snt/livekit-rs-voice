//! End-to-end test for outbound SIP: `CreateSIPParticipant` bridges to a
//! `livekit/sip`-shaped psrpc server over an in-memory bus.

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

fn sip_call_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "admin",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "sip": {"call": true}
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

/// A minimal `livekit/sip` bridge speaking the psrpc v0.7 claim flow. It
/// answers `CreateSIPParticipant` (topic `""`) with a canned participant.
fn spawn_mock_bridge(bus: Arc<MemoryBus>) -> Arc<tokio::sync::Mutex<Vec<Vec<u8>>>> {
    let rpc_ch = psrpc::rpc_channel("SIPInternal", "CreateSIPParticipant", "");
    let rclaim_ch = psrpc::claim_response_channel("SIPInternal", "CreateSIPParticipant", "");
    let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let received2 = received.clone();
    let bus2 = bus.clone();
    tokio::spawn(async move {
        let mut stream = bus2
            .subscribe(vec![rpc_ch.clone(), rclaim_ch.clone()])
            .await
            .unwrap();
        let mut pending: BTreeMap<String, (String, Vec<u8>)> = BTreeMap::new();
        while let Some((channel, payload)) = stream.next().await {
            let Ok(env) = internal::Msg::decode(payload.as_slice()) else {
                continue;
            };
            if channel == rpc_ch {
                let Ok(req) = internal::Request::decode(env.value.as_slice()) else {
                    continue;
                };
                let raw = req.raw_request.clone();
                received2.lock().await.push(raw.clone());
                pending.insert(req.request_id.clone(), (req.client_id.clone(), raw));
                let claim = internal::ClaimRequest {
                    request_id: req.request_id.clone(),
                    server_id: "SRV_mock".to_string(),
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
                let Some((client_id, raw)) = pending.remove(&grant.request_id) else {
                    continue;
                };
                let ireq =
                    lk_proto::rpc::InternalCreateSipParticipantRequest::decode(raw.as_slice())
                        .unwrap();
                let resp = internal::Response {
                    request_id: grant.request_id,
                    server_id: "SRV_mock".to_string(),
                    sent_at: lk_server::psrpc::unix_nanos(),
                    raw_response: lk_proto::rpc::InternalCreateSipParticipantResponse {
                        participant_id: "PA_mock1".to_string(),
                        participant_identity: ireq.participant_identity.clone(),
                        sip_call_id: ireq.sip_call_id.clone(),
                    }
                    .encode_to_vec(),
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

async fn start_server_with_bus() -> (
    Arc<Server>,
    String,
    Arc<MemoryBus>,
    Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
) {
    let config = test_config();
    let server = Server::new(config);
    let bus = MemoryBus::new();
    let received = spawn_mock_bridge(bus.clone());
    let _client = server.sip_client_with(bus.clone()).await.unwrap();
    let app = http::router(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (server, format!("http://{addr}"), bus, received)
}

async fn create_outbound_trunk(server: &Arc<Server>) {
    let trunk = lk::SipOutboundTrunkInfo {
        sip_trunk_id: "ST_test".to_string(),
        name: "test".to_string(),
        address: "203.0.113.10".to_string(),
        numbers: vec!["+1555".to_string()],
        auth_username: "u".to_string(),
        auth_password: "p".to_string(),
        ..Default::default()
    };
    server.store.store_sip_outbound_trunk(&trunk).await.unwrap();
}

#[tokio::test]
async fn create_participant_bridges_to_sip_container() {
    let (server, base, _bus, received) = start_server_with_bus().await;
    create_outbound_trunk(&server).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.SIP/CreateSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", sip_call_token()))
        .body(
            r#"{"sip_trunk_id":"ST_test","sip_call_to":"+1777","room_name":"room-a","participant_name":"caller"}"#,
        )
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "body: {text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["participantId"], "PA_mock1");
    assert_eq!(body["participantIdentity"], "sip_+1777");
    assert_eq!(body["roomName"], "room-a");
    assert!(body["sipCallId"].as_str().unwrap().starts_with("SC_"));

    // The bridge must have received exactly one well-formed request.
    let got = received.lock().await.clone();
    assert_eq!(got.len(), 1);
    let ireq =
        lk_proto::rpc::InternalCreateSipParticipantRequest::decode(got[0].as_slice()).unwrap();
    assert_eq!(ireq.room_name, "room-a");
    assert_eq!(ireq.call_to, "+1777");
    assert_eq!(ireq.number, "+1555");
    assert_eq!(ireq.address, "203.0.113.10");
    assert_eq!(ireq.participant_identity, "sip_+1777");
    assert_eq!(ireq.attributes_to_headers, Default::default());
    // Attributes stamped by the server.
    assert_eq!(
        ireq.participant_attributes.get("sip.callID"),
        Some(&ireq.sip_call_id)
    );
    assert_eq!(
        ireq.participant_attributes.get("sip.trunkID"),
        Some(&"ST_test".to_string())
    );
    assert_eq!(
        ireq.participant_attributes.get("sip.phoneNumber"),
        Some(&"+1777".to_string())
    );
}

#[tokio::test]
async fn missing_trunk_is_not_found() {
    let (server, base, _bus, received) = start_server_with_bus().await;
    let _ = server;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/twirp/livekit.SIP/CreateSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", sip_call_token()))
        .body(r#"{"sip_trunk_id":"ST_nope","sip_call_to":"+1777","room_name":"room-a"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    assert!(received.lock().await.is_empty());
}

#[tokio::test]
async fn requires_sip_call_permission() {
    let (server, base, _bus, _received) = start_server_with_bus().await;
    create_outbound_trunk(&server).await;

    // No sip grant at all -> permission denied.
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
        .post(format!("{base}/twirp/livekit.SIP/CreateSIPParticipant"))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(r#"{"sip_trunk_id":"ST_test","sip_call_to":"+1777","room_name":"room-a"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}
