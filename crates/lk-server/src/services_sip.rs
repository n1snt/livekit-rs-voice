//! Twirp service implementations: `livekit.SIP` and `livekit.Egress`.
//!
//! SIP trunks and dispatch rules are stored (Redis-backed when configured) so
//! the external `livekit/sip` container can service inbound calls. Outbound
//! calls (`CreateSIPParticipant`, `TransferSIPParticipant`) are bridged to a
//! `livekit/sip` container over the psrpc message bus (`psrpc.rs`). Egress
//! requests are persisted so the `livekit/egress` container can pick them up.

use std::collections::BTreeMap;
use std::sync::Arc;

use lk_proto::livekit as lk;

use crate::core::unix_seconds;
use crate::http::{parse_body, write_body, Req, TwirpError, WireFormat};
use crate::server::Server;

fn ensure_sip_admin(req: &Req) -> Result<(), TwirpError> {
    if req.token.sip.admin {
        Ok(())
    } else {
        Err(TwirpError::permission_denied("sip admin permission denied"))
    }
}

fn ensure_sip_call(req: &Req) -> Result<(), TwirpError> {
    if req.token.sip.call {
        Ok(())
    } else {
        Err(TwirpError::permission_denied("sip call permission denied"))
    }
}

fn ensure_record(req: &Req) -> Result<(), TwirpError> {
    if req.token.video.room_record {
        Ok(())
    } else {
        Err(TwirpError::permission_denied(
            "roomRecord permission denied",
        ))
    }
}

fn apply_list_update(current: &mut Vec<String>, update: &lk::ListUpdate) {
    if update.clear {
        current.clear();
    }
    if !update.set.is_empty() {
        *current = update.set.clone();
    }
    for add in &update.add {
        if !current.contains(add) {
            current.push(add.clone());
        }
    }
    for rem in &update.remove {
        current.retain(|v| v != rem);
    }
}

fn trunk_not_found(id: &str) -> TwirpError {
    TwirpError::not_found(format!("SIP trunk {id} not found"))
}

fn out<T: serde::Serialize + prost::Message>(
    msg: &T,
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    write_body(msg, format)
}

pub async fn sip_service(
    server: &Arc<Server>,
    method: &str,
    req: &Req,
    body: &[u8],
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    let store = server.store.clone();
    match method {
        "CreateSIPInboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::CreateSipInboundTrunkRequest = parse_body(body, format)?;
            let mut trunk = r.trunk.unwrap_or_default();
            if !trunk.sip_trunk_id.is_empty() {
                return Err(TwirpError::invalid_argument("trunk ID must be empty"));
            }
            trunk.sip_trunk_id = crate::core::generate_id("ST_");
            let now = unix_seconds();
            trunk.created_at = Some(lk_proto::well_known::Timestamp { seconds: now, nanos: 0 });
            trunk.updated_at = trunk.created_at;
            store.store_sip_inbound_trunk(&trunk).await.map_err(twirp_internal)?;
            out(&trunk, format)
        }
        "CreateSIPOutboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::CreateSipOutboundTrunkRequest = parse_body(body, format)?;
            let mut trunk = r.trunk.unwrap_or_default();
            if !trunk.sip_trunk_id.is_empty() {
                return Err(TwirpError::invalid_argument("trunk ID must be empty"));
            }
            if trunk.address.is_empty() {
                return Err(TwirpError::invalid_argument("trunk address is required"));
            }
            trunk.sip_trunk_id = crate::core::generate_id("ST_");
            let now = unix_seconds();
            trunk.created_at = Some(lk_proto::well_known::Timestamp { seconds: now, nanos: 0 });
            trunk.updated_at = trunk.created_at;
            store.store_sip_outbound_trunk(&trunk).await.map_err(twirp_internal)?;
            out(&trunk, format)
        }
        "GetSIPInboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::GetSipInboundTrunkRequest = parse_body(body, format)?;
            let trunk = store
                .load_sip_inbound_trunk(&r.sip_trunk_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| trunk_not_found(&r.sip_trunk_id))?;
            out(&lk::GetSipInboundTrunkResponse { trunk: Some(trunk) }, format)
        }
        "GetSIPOutboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::GetSipOutboundTrunkRequest = parse_body(body, format)?;
            let trunk = store
                .load_sip_outbound_trunk(&r.sip_trunk_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| trunk_not_found(&r.sip_trunk_id))?;
            out(&lk::GetSipOutboundTrunkResponse { trunk: Some(trunk) }, format)
        }
        "UpdateSIPInboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::UpdateSipInboundTrunkRequest = parse_body(body, format)?;
            let mut trunk = store
                .load_sip_inbound_trunk(&r.sip_trunk_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| trunk_not_found(&r.sip_trunk_id))?;
            match r.action {
                Some(lk::update_sip_inbound_trunk_request::Action::Replace(replace)) => {
                    trunk = replace;
                    trunk.sip_trunk_id = r.sip_trunk_id.clone();
                }
                Some(lk::update_sip_inbound_trunk_request::Action::Update(update)) => {
                    if let Some(numbers) = &update.numbers {
                        apply_list_update(&mut trunk.numbers, numbers);
                    }
                    if let Some(addrs) = &update.allowed_addresses {
                        apply_list_update(&mut trunk.allowed_addresses, addrs);
                    }
                    if let Some(nums) = &update.allowed_numbers {
                        apply_list_update(&mut trunk.allowed_numbers, nums);
                    }
                    if let Some(v) = update.auth_username {
                        trunk.auth_username = v;
                    }
                    if let Some(v) = update.auth_password {
                        trunk.auth_password = v;
                    }
                    if let Some(v) = update.auth_realm {
                        trunk.auth_realm = v;
                    }
                    if let Some(v) = update.name {
                        trunk.name = v;
                    }
                    if let Some(v) = update.metadata {
                        trunk.metadata = v;
                    }
                    if update.media.is_some() {
                        trunk.media = update.media;
                    }
                }
                None => return Err(TwirpError::invalid_argument("missing action")),
            }
            trunk.updated_at = Some(lk_proto::well_known::Timestamp {
                seconds: unix_seconds(),
                nanos: 0,
            });
            store.store_sip_inbound_trunk(&trunk).await.map_err(twirp_internal)?;
            out(&trunk, format)
        }
        "UpdateSIPOutboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::UpdateSipOutboundTrunkRequest = parse_body(body, format)?;
            let mut trunk = store
                .load_sip_outbound_trunk(&r.sip_trunk_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| trunk_not_found(&r.sip_trunk_id))?;
            match r.action {
                Some(lk::update_sip_outbound_trunk_request::Action::Replace(replace)) => {
                    trunk = replace;
                    trunk.sip_trunk_id = r.sip_trunk_id.clone();
                }
                Some(lk::update_sip_outbound_trunk_request::Action::Update(update)) => {
                    if let Some(v) = update.address {
                        trunk.address = v;
                    }
                    if let Some(v) = update.transport {
                        trunk.transport = v;
                    }
                    if let Some(v) = update.destination_country {
                        trunk.destination_country = v;
                    }
                    if let Some(v) = update.from_host {
                        trunk.from_host = v;
                    }
                    if let Some(numbers) = &update.numbers {
                        apply_list_update(&mut trunk.numbers, numbers);
                    }
                    if let Some(v) = update.auth_username {
                        trunk.auth_username = v;
                    }
                    if let Some(v) = update.auth_password {
                        trunk.auth_password = v;
                    }
                    if let Some(v) = update.name {
                        trunk.name = v;
                    }
                    if let Some(v) = update.metadata {
                        trunk.metadata = v;
                    }
                    if update.media.is_some() {
                        trunk.media = update.media;
                    }
                }
                None => return Err(TwirpError::invalid_argument("missing action")),
            }
            trunk.updated_at = Some(lk_proto::well_known::Timestamp {
                seconds: unix_seconds(),
                nanos: 0,
            });
            store.store_sip_outbound_trunk(&trunk).await.map_err(twirp_internal)?;
            out(&trunk, format)
        }
        "ListSIPInboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::ListSipInboundTrunkRequest = parse_body(body, format)?;
            let all = store.list_sip_inbound_trunks().await.map_err(twirp_internal)?;
            let items: Vec<lk::SipInboundTrunkInfo> = all
                .into_iter()
                .filter(|t| {
                    if !r.trunk_ids.is_empty() {
                        return r.trunk_ids.contains(&t.sip_trunk_id);
                    }
                    if !r.numbers.is_empty() && !t.numbers.is_empty() {
                        return t.numbers.iter().any(|n| r.numbers.contains(n));
                    }
                    true
                })
                .collect();
            out(&lk::ListSipInboundTrunkResponse { items }, format)
        }
        "ListSIPOutboundTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::ListSipOutboundTrunkRequest = parse_body(body, format)?;
            let all = store.list_sip_outbound_trunks().await.map_err(twirp_internal)?;
            let items: Vec<lk::SipOutboundTrunkInfo> = all
                .into_iter()
                .filter(|t| {
                    if !r.trunk_ids.is_empty() {
                        return r.trunk_ids.contains(&t.sip_trunk_id);
                    }
                    if !r.numbers.is_empty() && !t.numbers.is_empty() {
                        return t.numbers.iter().any(|n| r.numbers.contains(n));
                    }
                    true
                })
                .collect();
            out(&lk::ListSipOutboundTrunkResponse { items }, format)
        }
        "DeleteSIPTrunk" => {
            ensure_sip_admin(req)?;
            let r: lk::DeleteSipTrunkRequest = parse_body(body, format)?;
            if r.sip_trunk_id.is_empty() {
                return Err(TwirpError::invalid_argument("trunk ID is required"));
            }
            store.delete_sip_trunk(&r.sip_trunk_id).await.map_err(twirp_internal)?;
            out(
                &lk::SipTrunkInfo {
                    sip_trunk_id: r.sip_trunk_id,
                    ..Default::default()
                },
                format,
            )
        }
        "CreateSIPDispatchRule" => {
            ensure_sip_admin(req)?;
            let r: lk::CreateSipDispatchRuleRequest = parse_body(body, format)?;
            let mut info = dispatch_rule_info(&r);
            if !info.sip_dispatch_rule_id.is_empty() {
                return Err(TwirpError::invalid_argument("dispatch rule ID must be empty"));
            }
            if info.rule.is_none() {
                return Err(TwirpError::invalid_argument("dispatch rule is required"));
            }
            info.sip_dispatch_rule_id = crate::core::generate_id("SR_");
            let now = unix_seconds();
            info.created_at = Some(lk_proto::well_known::Timestamp { seconds: now, nanos: 0 });
            info.updated_at = info.created_at;
            store.store_sip_dispatch_rule(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "UpdateSIPDispatchRule" => {
            ensure_sip_admin(req)?;
            let r: lk::UpdateSipDispatchRuleRequest = parse_body(body, format)?;
            let mut info = store
                .load_sip_dispatch_rule(&r.sip_dispatch_rule_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| TwirpError::not_found("dispatch rule not found"))?;
            match r.action {
                Some(lk::update_sip_dispatch_rule_request::Action::Replace(replace)) => {
                    info = replace;
                    info.sip_dispatch_rule_id = r.sip_dispatch_rule_id.clone();
                }
                Some(lk::update_sip_dispatch_rule_request::Action::Update(update)) => {
                    if let Some(trunk_ids) = &update.trunk_ids {
                        apply_list_update(&mut info.trunk_ids, trunk_ids);
                    }
                    if update.rule.is_some() {
                        info.rule = update.rule;
                    }
                    if let Some(v) = update.name {
                        info.name = v;
                    }
                    if let Some(v) = update.metadata {
                        info.metadata = v;
                    }
                    if !update.attributes.is_empty() {
                        info.attributes = update.attributes;
                    }
                    if update.media.is_some() {
                        info.media = update.media;
                    }
                }
                None => return Err(TwirpError::invalid_argument("missing action")),
            }
            info.updated_at = Some(lk_proto::well_known::Timestamp {
                seconds: unix_seconds(),
                nanos: 0,
            });
            store.store_sip_dispatch_rule(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "ListSIPDispatchRule" => {
            ensure_sip_admin(req)?;
            let r: lk::ListSipDispatchRuleRequest = parse_body(body, format)?;
            let all = store.list_sip_dispatch_rules().await.map_err(twirp_internal)?;
            let items: Vec<lk::SipDispatchRuleInfo> = all
                .into_iter()
                .filter(|rule| {
                    if !r.dispatch_rule_ids.is_empty() {
                        return r.dispatch_rule_ids.contains(&rule.sip_dispatch_rule_id);
                    }
                    if !r.trunk_ids.is_empty() && !rule.trunk_ids.is_empty() {
                        return rule.trunk_ids.iter().any(|t| r.trunk_ids.contains(t));
                    }
                    true
                })
                .collect();
            out(&lk::ListSipDispatchRuleResponse { items }, format)
        }
        "DeleteSIPDispatchRule" => {
            ensure_sip_admin(req)?;
            let r: lk::DeleteSipDispatchRuleRequest = parse_body(body, format)?;
            if r.sip_dispatch_rule_id.is_empty() {
                return Err(TwirpError::invalid_argument("dispatch rule ID is required"));
            }
            let info = store
                .load_sip_dispatch_rule(&r.sip_dispatch_rule_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| TwirpError::not_found("dispatch rule not found"))?;
            store
                .delete_sip_dispatch_rule(&r.sip_dispatch_rule_id)
                .await
                .map_err(twirp_internal)?;
            out(&info, format)
        }
        "CreateSIPParticipant" => {
            ensure_sip_call(req)?;
            let r: lk::CreateSipParticipantRequest = parse_body(body, format)?;
            if r.sip_call_to.is_empty() {
                return Err(TwirpError::invalid_argument("sip_call_to is required"));
            }
            if r.room_name.is_empty() {
                return Err(TwirpError::invalid_argument("room_name is required"));
            }
            // Ensure the room exists so inbound/agent flows have a target.
            server.get_or_create_room(&r.room_name);
            let ireq = build_internal_create_participant(server, &r).await?;
            let client = server
                .sip_client()
                .await
                .map_err(TwirpError::failed_precondition)?;
            let resp = client
                .create_sip_participant(&ireq)
                .await
                .map_err(psrpc_to_twirp)?;
            out(
                &lk::SipParticipantInfo {
                    participant_id: resp.participant_id,
                    participant_identity: resp.participant_identity,
                    room_name: r.room_name,
                    sip_call_id: resp.sip_call_id,
                },
                format,
            )
        }
        "TransferSIPParticipant" => {
            ensure_sip_call(req)?;
            let r: lk::TransferSipParticipantRequest = parse_body(body, format)?;
            if r.transfer_to.is_empty() {
                return Err(TwirpError::invalid_argument("transferTo is required"));
            }
            if r.room_name.is_empty() {
                return Err(TwirpError::invalid_argument("room_name is required"));
            }
            let room = server
                .get_room(&r.room_name)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.participant_identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            let sip_call_id = participant
                .attributes
                .lock()
                .unwrap()
                .get(ATTR_SIP_CALL_ID)
                .cloned()
                .ok_or_else(|| TwirpError::failed_precondition("participant is not a SIP participant"))?;
            let ireq = lk_proto::rpc::InternalTransferSipParticipantRequest {
                sip_call_id: sip_call_id.clone(),
                transfer_to: r.transfer_to,
                play_dialtone: r.play_dialtone,
                headers: r.headers,
                ringing_timeout: r.ringing_timeout,
                ..Default::default()
            };
            let client = server
                .sip_client()
                .await
                .map_err(TwirpError::failed_precondition)?;
            client
                .transfer_sip_participant(&sip_call_id, &ireq)
                .await
                .map_err(psrpc_to_twirp)?;
            out(&lk_proto::well_known::Empty {}, format)
        }
        "ListSIPTrunk" => Err(TwirpError::failed_precondition(
            "deprecated ListSIPTrunk is not supported; use ListSIPInboundTrunk/ListSIPOutboundTrunk",
        )),
        _ => Err(TwirpError::not_found(format!("method not found: {method}"))),
    }
}

#[allow(deprecated)]
fn dispatch_rule_info(r: &lk::CreateSipDispatchRuleRequest) -> lk::SipDispatchRuleInfo {
    if let Some(info) = r.dispatch_rule.clone() {
        return info;
    }
    lk::SipDispatchRuleInfo {
        rule: r.rule.clone(),
        trunk_ids: r.trunk_ids.clone(),
        hide_phone_number: r.hide_phone_number,
        inbound_numbers: r.inbound_numbers.clone(),
        name: r.name.clone(),
        metadata: r.metadata.clone(),
        attributes: r.attributes.clone(),
        room_preset: r.room_preset.clone(),
        room_config: r.room_config.clone(),
        ..Default::default()
    }
}

fn twirp_internal(e: String) -> TwirpError {
    TwirpError::internal(e)
}

// ---------------------------------------------------------------------------
// Outbound SIP (psrpc bridge)
// ---------------------------------------------------------------------------

const ATTR_SIP_CALL_ID: &str = "sip.callID";

/// Maps a psrpc client error onto the matching Twirp error so API clients see
/// the same semantics as the reference `livekit-server`.
fn psrpc_to_twirp(e: crate::psrpc::PsrpcError) -> TwirpError {
    match e {
        crate::psrpc::PsrpcError::Timeout => {
            TwirpError::deadline_exceeded("sip bridge did not respond in time")
        }
        crate::psrpc::PsrpcError::Rpc { message, .. } => TwirpError::failed_precondition(message),
        other => TwirpError::internal(other.to_string()),
    }
}

/// Carries the deprecated `media_encryption` value into `media.encryption`
/// when the latter is unset (mirrors `SIPMediaConfig.UpgradeWith`).
#[allow(deprecated)]
fn upgrade_media(
    media: &Option<lk::SipMediaConfig>,
    encryption: i32,
) -> Option<lk::SipMediaConfig> {
    let mut m = media.clone()?;
    if m.encryption.is_none() && encryption != 0 {
        m.encryption = Some(encryption);
    }
    Some(m)
}

/// Merges a base media config (e.g. from a trunk) with an overlay, filling
/// zero fields from the overlay (mirrors the reference `Merge` behavior).
fn merge_media(
    base: Option<lk::SipMediaConfig>,
    other: Option<lk::SipMediaConfig>,
) -> Option<lk::SipMediaConfig> {
    match (base, other) {
        (None, o) => o,
        (Some(b), None) => Some(b),
        (Some(mut b), Some(o)) => {
            if !b.only_listed_codecs {
                b.only_listed_codecs = o.only_listed_codecs;
            }
            if b.codecs.is_empty() {
                b.codecs = o.codecs;
            }
            if b.encryption.is_none() {
                b.encryption = o.encryption;
            }
            if b.media_timeout.is_none() {
                b.media_timeout = o.media_timeout;
            }
            Some(b)
        }
    }
}

/// Builds the psrpc `InternalCreateSIPParticipantRequest`, mirroring
/// `rpc.NewCreateSIPParticipantRequestResult` in `livekit-server`: resolves
/// the outbound trunk, selects the caller number, and stamps the SIP
/// participant attributes (`sip.callID`, `sip.trunkID`, ...).
#[allow(deprecated)]
async fn build_internal_create_participant(
    server: &Arc<Server>,
    r: &lk::CreateSipParticipantRequest,
) -> Result<lk_proto::rpc::InternalCreateSipParticipantRequest, TwirpError> {
    let store = server.store.clone();
    let trunk = if r.sip_trunk_id.is_empty() {
        None
    } else {
        Some(
            store
                .load_sip_outbound_trunk(&r.sip_trunk_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| trunk_not_found(&r.sip_trunk_id))?,
        )
    };

    let mut hostname = String::new();
    let mut from_host = String::new();
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    let mut include_headers = 0i32;
    let mut transport = 0i32;
    let mut destination_country = String::new();
    let mut auth_user = String::new();
    let mut auth_pass = String::new();
    let mut hdr_to_attr: BTreeMap<String, String> = BTreeMap::new();
    let mut attr_to_hdr: BTreeMap<String, String> = BTreeMap::new();
    let mut trunk_media: Option<lk::SipMediaConfig> = None;

    if let Some(trunk) = &trunk {
        hostname = trunk.address.clone();
        from_host = trunk.from_host.clone();
        headers = trunk.headers.clone();
        include_headers = trunk.include_headers;
        transport = trunk.transport;
        destination_country = trunk.destination_country.clone();
        auth_user = trunk.auth_username.clone();
        auth_pass = trunk.auth_password.clone();
        hdr_to_attr = trunk.headers_to_attributes.clone();
        attr_to_hdr = trunk.attributes_to_headers.clone();
        trunk_media = upgrade_media(&trunk.media, trunk.media_encryption);
    } else if let Some(cfg) = &r.trunk {
        hostname = cfg.hostname.clone();
        from_host = cfg.from_host.clone();
        transport = cfg.transport;
        destination_country = cfg.destination_country.clone();
        auth_user = cfg.auth_username.clone();
        auth_pass = cfg.auth_password.clone();
        hdr_to_attr = cfg.headers_to_attributes.clone();
        attr_to_hdr = cfg.attributes_to_headers.clone();
    }

    let mut outbound_number = r.sip_number.clone();
    if outbound_number.is_empty() {
        let numbers = trunk
            .as_ref()
            .map(|t| t.numbers.clone())
            .unwrap_or_default();
        if numbers.is_empty() {
            return Err(TwirpError::failed_precondition(
                "no numbers on outbound trunk",
            ));
        }
        let idx = rand::random::<usize>() % numbers.len();
        outbound_number = numbers[idx].clone();
    }
    if hostname.ends_with("twilio.com") && !outbound_number.starts_with('+') {
        outbound_number = format!("+{outbound_number}");
    }

    let call_id = crate::core::generate_id("SC_");
    let trunk_id = if !r.sip_trunk_id.is_empty() {
        r.sip_trunk_id.clone()
    } else {
        trunk
            .as_ref()
            .map(|t| t.sip_trunk_id.clone())
            .unwrap_or_default()
    };
    let mut attrs = r.participant_attributes.clone();
    attrs.insert(ATTR_SIP_CALL_ID.to_string(), call_id.clone());
    attrs.insert("sip.trunkID".to_string(), trunk_id.clone());
    if !r.hide_phone_number {
        attrs.insert("sip.phoneNumber".to_string(), r.sip_call_to.clone());
        attrs.insert("sip.hostname".to_string(), hostname.clone());
        attrs.insert("sip.trunkPhoneNumber".to_string(), outbound_number.clone());
    }

    let mut features = Vec::new();
    if r.krisp_enabled {
        features.push(lk::SipFeature::KrispEnabled as i32);
    }
    if !r.headers.is_empty() {
        headers.extend(r.headers.clone());
    }
    if r.include_headers != 0 {
        include_headers = r.include_headers;
    }

    let participant_identity = if r.participant_identity.is_empty() {
        format!("sip_{}", r.sip_call_to)
    } else {
        r.participant_identity.clone()
    };

    let media = merge_media(trunk_media, upgrade_media(&r.media, r.media_encryption));

    Ok(lk_proto::rpc::InternalCreateSipParticipantRequest {
        project_id: String::new(),
        sip_call_id: call_id,
        sip_trunk_id: trunk_id,
        sip_request_uri: r.sip_request_uri.clone(),
        sip_from_header: r.sip_from_header.clone(),
        sip_to_header: r.sip_to_header.clone(),
        address: hostname,
        hostname: from_host,
        destination_country,
        transport,
        number: outbound_number,
        call_to: r.sip_call_to.clone(),
        username: auth_user,
        password: auth_pass,
        room_name: r.room_name.clone(),
        participant_identity,
        participant_name: r.participant_name.clone(),
        participant_metadata: r.participant_metadata.clone(),
        participant_attributes: attrs,
        token: String::new(),
        ws_url: String::new(),
        dtmf: r.dtmf.clone(),
        play_dialtone: r.play_ringtone || r.play_dialtone,
        headers,
        headers_to_attributes: hdr_to_attr,
        attributes_to_headers: attr_to_hdr,
        include_headers,
        enabled_features: features,
        ringing_timeout: r.ringing_timeout,
        max_call_duration: r.max_call_duration,
        media_encryption: media.as_ref().and_then(|m| m.encryption).unwrap_or(0),
        media,
        wait_until_answered: r.wait_until_answered,
        display_name: r.display_name.clone(),
        destination: r.destination.clone(),
        feature_flags: Default::default(),
        observability: None,
    })
}

// ---------------------------------------------------------------------------
// Egress service
// ---------------------------------------------------------------------------

pub async fn egress_service(
    server: &Arc<Server>,
    method: &str,
    req: &Req,
    body: &[u8],
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    let store = server.store.clone();
    match method {
        "StartRoomCompositeEgress" => {
            ensure_record(req)?;
            let r: lk::RoomCompositeEgressRequest = parse_body(body, format)?;
            if r.room_name.is_empty() {
                return Err(TwirpError::invalid_argument("room_name is required"));
            }
            let info = new_egress_info(
                &r.room_name,
                lk::egress_info::Request::RoomComposite(r.clone()),
            );
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "StartEgress" => {
            ensure_record(req)?;
            let r: lk::StartEgressRequest = parse_body(body, format)?;
            let info = new_egress_info(&r.room_name, lk::egress_info::Request::Egress(r.clone()));
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "StartTrackEgress" => {
            ensure_record(req)?;
            let r: lk::TrackEgressRequest = parse_body(body, format)?;
            let info = new_egress_info("", lk::egress_info::Request::Track(r.clone()));
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "StartTrackCompositeEgress" => {
            ensure_record(req)?;
            let r: lk::TrackCompositeEgressRequest = parse_body(body, format)?;
            let info = new_egress_info(
                &r.room_name,
                lk::egress_info::Request::TrackComposite(r.clone()),
            );
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "StartParticipantEgress" => {
            ensure_record(req)?;
            let r: lk::ParticipantEgressRequest = parse_body(body, format)?;
            let info = new_egress_info(
                &r.room_name,
                lk::egress_info::Request::Participant(r.clone()),
            );
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "ListEgress" => {
            let r: lk::ListEgressRequest = parse_body(body, format)?;
            let all = store.list_egress().await.map_err(twirp_internal)?;
            let items: Vec<lk::EgressInfo> = all
                .into_iter()
                .filter(|e| {
                    if !r.egress_id.is_empty() {
                        return e.egress_id == r.egress_id;
                    }
                    if !r.room_name.is_empty() {
                        return e.room_name == r.room_name;
                    }
                    true
                })
                .collect();
            out(
                &lk::ListEgressResponse {
                    items,
                    ..Default::default()
                },
                format,
            )
        }
        "StopEgress" => {
            let r: lk::StopEgressRequest = parse_body(body, format)?;
            let mut info = store
                .load_egress(&r.egress_id)
                .await
                .map_err(twirp_internal)?
                .ok_or_else(|| TwirpError::not_found("egress not found"))?;
            info.status = lk::EgressStatus::EgressEnding as i32;
            store.store_egress(&info).await.map_err(twirp_internal)?;
            out(&info, format)
        }
        "UpdateLayout" | "UpdateStream" | "StartWebEgress" => Err(TwirpError::failed_precondition(
            "not supported on voice-only server",
        )),
        _ => Err(TwirpError::not_found(format!("method not found: {method}"))),
    }
}

fn new_egress_info(room_name: &str, request: lk::egress_info::Request) -> lk::EgressInfo {
    lk::EgressInfo {
        egress_id: crate::core::generate_id("EG_"),
        room_name: room_name.to_string(),
        status: lk::EgressStatus::EgressStarting as i32,
        started_at: unix_seconds(),
        updated_at: unix_seconds(),
        request: Some(request),
        ..Default::default()
    }
}
