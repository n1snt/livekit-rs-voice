#![allow(deprecated)]
//! End-to-end test for inbound SIP: the server hosts the `IOInfoSIP` psrpc
//! service and a `livekit/sip`-shaped psrpc client (same wire protocol) gets
//! trunk authentication and dispatch-rule evaluation over an in-memory bus.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use lk_proto::livekit as lk;
use lk_proto::livekit::sip_dispatch_rule::Rule as DispatchRule;
use lk_proto::rpc;
use prost::Message as _;

use lk_server::config::Config;
use lk_server::psrpc::{MemoryBus, SipInternalClient};
use lk_server::server::Server;

fn test_config() -> Config {
    Config {
        port: Some(0),
        keys: BTreeMap::from([("devkey".to_string(), "secret".to_string())]),
        ..Default::default()
    }
}

fn uri(user: &str) -> lk::SipUri {
    lk::SipUri {
        user: user.to_string(),
        ..Default::default()
    }
}

async fn start_server_with_io() -> (Arc<Server>, Arc<SipInternalClient>) {
    let server = Server::new(test_config());
    let bus = MemoryBus::new();
    server.start_sip_io_with(bus.clone()).await.unwrap();
    let client = SipInternalClient::new_with_service(bus, "IOInfoSIP", Duration::from_secs(2))
        .await
        .unwrap();
    (server, client)
}

async fn seed_trunk_and_rule(server: &Arc<Server>) {
    let trunk = lk::SipInboundTrunkInfo {
        sip_trunk_id: "ST_in".to_string(),
        name: "main".to_string(),
        numbers: vec!["+1555".to_string()],
        auth_username: "u".to_string(),
        auth_password: "p".to_string(),
        auth_realm: "realm".to_string(),
        ..Default::default()
    };
    server.store.store_sip_inbound_trunk(&trunk).await.unwrap();

    let rule = lk::SipDispatchRuleInfo {
        sip_dispatch_rule_id: "SR_in".to_string(),
        trunk_ids: vec!["ST_in".to_string()],
        rule: Some(lk::SipDispatchRule {
            rule: Some(DispatchRule::DispatchRuleDirect(
                lk::SipDispatchRuleDirect {
                    room_name: "support".to_string(),
                    ..Default::default()
                },
            )),
        }),
        ..Default::default()
    };
    server.store.store_sip_dispatch_rule(&rule).await.unwrap();
}

#[tokio::test]
async fn inbound_trunk_auth_and_dispatch_round_trip() {
    let (server, client) = start_server_with_io().await;
    seed_trunk_and_rule(&server).await;

    // The sip container calls GetSIPTrunkAuthentication first.
    let auth_req = rpc::GetSipTrunkAuthenticationRequest {
        call: Some(rpc::SipCall {
            lk_call_id: "SC_1".to_string(),
            source_ip: "1.2.3.4".to_string(),
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = client
        .request("GetSIPTrunkAuthentication", &auth_req)
        .await
        .unwrap();
    let auth = rpc::GetSipTrunkAuthenticationResponse::decode(raw.as_slice()).unwrap();
    assert_eq!(auth.sip_trunk_id, "ST_in");
    assert_eq!(auth.username, "u");
    assert_eq!(auth.password, "p");
    assert_eq!(auth.realm, "realm");

    // Then it evaluates dispatch rules (with the matched trunk id).
    let dispatch_req = rpc::EvaluateSipDispatchRulesRequest {
        sip_trunk_id: "ST_in".to_string(),
        calling_number: "+1999".to_string(),
        called_number: "+1555".to_string(),
        call: Some(rpc::SipCall {
            lk_call_id: "SC_1".to_string(),
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = client
        .request("EvaluateSIPDispatchRules", &dispatch_req)
        .await
        .unwrap();
    let dispatch = rpc::EvaluateSipDispatchRulesResponse::decode(raw.as_slice()).unwrap();
    assert_eq!(dispatch.result, rpc::SipDispatchResult::Accept as i32);
    assert_eq!(dispatch.room_name, "support");
    assert_eq!(dispatch.sip_dispatch_rule_id, "SR_in");
    assert_eq!(dispatch.participant_identity, "sip_+1999");
    assert_eq!(
        dispatch.participant_attributes.get("sip.callID"),
        Some(&"SC_1".to_string())
    );
    assert_eq!(
        dispatch.participant_attributes.get("sip.trunkID"),
        Some(&"ST_in".to_string())
    );
    assert_eq!(
        dispatch.participant_attributes.get("sip.phoneNumber"),
        Some(&"+1999".to_string())
    );
}

#[tokio::test]
async fn unknown_trunk_is_empty_auth() {
    let (server, client) = start_server_with_io().await;
    let _ = server;

    let auth_req = rpc::GetSipTrunkAuthenticationRequest {
        call: Some(rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1999")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = client
        .request("GetSIPTrunkAuthentication", &auth_req)
        .await
        .unwrap();
    let auth = rpc::GetSipTrunkAuthenticationResponse::decode(raw.as_slice()).unwrap();
    assert!(auth.sip_trunk_id.is_empty());
    assert!(auth.username.is_empty());
}

#[tokio::test]
async fn no_dispatch_rule_drops() {
    let (server, client) = start_server_with_io().await;
    // Trunk exists but no dispatch rule.
    server
        .store
        .store_sip_inbound_trunk(&lk::SipInboundTrunkInfo {
            sip_trunk_id: "ST_in".to_string(),
            numbers: vec!["+1555".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();

    let dispatch_req = rpc::EvaluateSipDispatchRulesRequest {
        sip_trunk_id: "ST_in".to_string(),
        calling_number: "+1999".to_string(),
        called_number: "+1555".to_string(),
        call: Some(rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = client
        .request("EvaluateSIPDispatchRules", &dispatch_req)
        .await
        .unwrap();
    let dispatch = rpc::EvaluateSipDispatchRulesResponse::decode(raw.as_slice()).unwrap();
    assert_eq!(dispatch.result, rpc::SipDispatchResult::Drop as i32);
}

#[tokio::test]
async fn call_state_and_context_are_accepted() {
    let (server, client) = start_server_with_io().await;
    let _ = server;

    let state = rpc::UpdateSipCallStateRequest {
        call_info: Some(lk::SipCallInfo::default()),
        ..Default::default()
    };
    let raw = client.request("UpdateSIPCallState", &state).await.unwrap();
    assert_eq!(raw, lk_proto::well_known::Empty {}.encode_to_vec());

    let ctx = rpc::RecordCallContextRequest {
        call_info: Some(lk::SipCallInfo::default()),
    };
    client.request("RecordCallContext", &ctx).await.unwrap();
}
