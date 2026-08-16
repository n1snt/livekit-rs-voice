//! Real-Redis psrpc round trip. Requires a running Redis; set `REDIS_ADDR`
//! (e.g. `127.0.0.1:6379`) to enable. The test drives the full claim flow
//! over the actual Redis PubSub bus, including a queue subscriber acting as
//! the `livekit/sip` bridge.

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::internal;
use lk_proto::livekit as lk;
use lk_proto::rpc;
use prost::Message as _;

use futures_util::StreamExt;
use lk_server::config::{Config, RedisConfig};
use lk_server::psrpc::{self, PsrpcBus, RedisBus};

fn redis_addr() -> Option<String> {
    std::env::var("REDIS_ADDR").ok()
}

fn config_with_redis(addr: &str) -> Config {
    Config {
        redis: RedisConfig {
            address: addr.to_string(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Bridges `CreateSIPParticipant` on the real Redis bus, mirroring the
/// livekit/sip queue subscriber + claim/response flow.
async fn spawn_real_bridge(bus: Arc<RedisBus>) -> Arc<tokio::sync::Mutex<Vec<Vec<u8>>>> {
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
                    server_id: "SRV_real".to_string(),
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
                    rpc::InternalCreateSipParticipantRequest::decode(raw.as_slice()).unwrap();
                let resp = internal::Response {
                    request_id: grant.request_id,
                    server_id: "SRV_real".to_string(),
                    sent_at: lk_server::psrpc::unix_nanos(),
                    raw_response: rpc::InternalCreateSipParticipantResponse {
                        participant_id: "PA_real".to_string(),
                        participant_identity: ireq.participant_identity,
                        sip_call_id: ireq.sip_call_id,
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

#[tokio::test]
async fn redis_bus_round_trips_full_claim_flow() {
    let Some(addr) = redis_addr() else {
        eprintln!("skipping: REDIS_ADDR not set");
        return;
    };
    let config = config_with_redis(&addr);
    let bus = Arc::new(RedisBus::new(&config));
    let received = spawn_real_bridge(bus.clone()).await;
    let client = lk_server::psrpc::SipInternalClient::new(bus.clone())
        .await
        .expect("client connects to redis");

    let req = rpc::InternalCreateSipParticipantRequest {
        sip_call_id: "SC_real".to_string(),
        number: "+1555".to_string(),
        call_to: "+1777".to_string(),
        room_name: "room-real".to_string(),
        participant_identity: "sip_+1777".to_string(),
        ..Default::default()
    };
    let resp = client
        .create_sip_participant(&req)
        .await
        .expect("rpc over real redis");
    assert_eq!(resp.participant_id, "PA_real");
    assert_eq!(resp.sip_call_id, "SC_real");

    let got = received.lock().await.clone();
    assert_eq!(got.len(), 1);
    let ireq = rpc::InternalCreateSipParticipantRequest::decode(got[0].as_slice()).unwrap();
    assert_eq!(ireq.room_name, "room-real");
    assert_eq!(ireq.call_to, "+1777");
}

#[tokio::test]
async fn io_server_round_trips_over_real_redis() {
    let Some(addr) = redis_addr() else {
        eprintln!("skipping: REDIS_ADDR not set");
        return;
    };
    let config = config_with_redis(&addr);
    let bus: Arc<RedisBus> = Arc::new(RedisBus::new(&config));
    let server = lk_server::server::Server::new(config);
    server
        .store
        .store_sip_inbound_trunk(&lk::SipInboundTrunkInfo {
            sip_trunk_id: "ST_real".to_string(),
            numbers: vec!["+1555".to_string()],
            auth_username: "u".to_string(),
            auth_password: "p".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    server
        .start_sip_io_with(bus.clone())
        .await
        .expect("io server starts");

    let client = lk_server::psrpc::SipInternalClient::new_with_service(
        bus,
        "IOInfoSIP",
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap();
    let auth_req = rpc::GetSipTrunkAuthenticationRequest {
        call: Some(rpc::SipCall {
            from: Some(lk::SipUri {
                user: "+1999".to_string(),
                ..Default::default()
            }),
            to: Some(lk::SipUri {
                user: "+1555".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = client
        .request("GetSIPTrunkAuthentication", &auth_req)
        .await
        .expect("auth over real redis");
    let auth = rpc::GetSipTrunkAuthenticationResponse::decode(raw.as_slice()).unwrap();
    assert_eq!(auth.sip_trunk_id, "ST_real");
    assert_eq!(auth.username, "u");
    assert_eq!(auth.password, "p");
}
