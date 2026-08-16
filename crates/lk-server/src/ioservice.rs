//! `IOInfoSIP` psrpc service handlers.
//!
//! The `livekit/sip` container is a psrpc *client* of this service. These
//! handlers serve inbound calls: authenticate a trunk, evaluate dispatch
//! rules, and (best-effort) record call state. Matching logic mirrors
//! `livekit/protocol` (`sip.MatchTrunk` / `MatchDispatchRuleIter` /
//! `EvaluateDispatchRule`) and `livekit-server`'s `IOInfoService`.

// The `livekit/sip` container still populates the deprecated legacy fields
// (`from`/`to`/`calling_number`/...), so they are part of the wire contract.
#![allow(deprecated)]

use std::net::IpAddr;
use std::sync::Arc;

use lk_proto::livekit as lk;
use lk_proto::livekit::sip_dispatch_rule::Rule as DispatchRule;
use lk_proto::rpc;
use prost::Message as _;
use sha2::Digest;

use crate::psrpc::IoHandler;
use crate::redis_store::Store;

const ATTR_SIP_CALL_ID: &str = "sip.callID";
const ATTR_SIP_TRUNK_ID: &str = "sip.trunkID";
const ATTR_SIP_RULE_ID: &str = "sip.ruleID";
const ATTR_SIP_PHONE_NUMBER: &str = "sip.phoneNumber";
const ATTR_SIP_HOST_NAME: &str = "sip.hostname";
const ATTR_SIP_TRUNK_NUMBER: &str = "sip.trunkPhoneNumber";

const ERR_NO_DISPATCH_MATCHED: &str = "no dispatch rules matched the call";

/// Handlers for the four `IOInfoSIP` methods.
pub struct SipIoHandlers {
    pub store: Arc<Store>,
}

/// Handlers for the `IOInfo` egress methods (`CreateEgress`, `UpdateEgress`),
/// which the `livekit-egress` recorder calls to report state back.
pub struct EgressIoHandlers {
    pub store: Arc<Store>,
}

#[async_trait::async_trait]
impl IoHandler for EgressIoHandlers {
    async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String> {
        match method {
            "CreateEgress" | "UpdateEgress" => {
                let info = lk::EgressInfo::decode(raw.as_slice()).map_err(|e| e.to_string())?;
                self.store.store_egress(&info).await?;
                Ok(lk_proto::well_known::Empty {}.encode_to_vec())
            }
            _ => Err(format!("unknown IOInfo method: {method}")),
        }
    }
}

#[async_trait::async_trait]
impl IoHandler for SipIoHandlers {
    async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String> {
        match method {
            "GetSIPTrunkAuthentication" => {
                let req = rpc::GetSipTrunkAuthenticationRequest::decode(raw.as_slice())
                    .map_err(|e| e.to_string())?;
                let resp = self.get_sip_trunk_authentication(&req).await?;
                Ok(resp.encode_to_vec())
            }
            "EvaluateSIPDispatchRules" => {
                let req = rpc::EvaluateSipDispatchRulesRequest::decode(raw.as_slice())
                    .map_err(|e| e.to_string())?;
                let resp = self.evaluate_sip_dispatch_rules(&req).await?;
                Ok(resp.encode_to_vec())
            }
            // Best-effort call-state recording; the reference server treats
            // these as placeholders too.
            "UpdateSIPCallState" | "RecordCallContext" => {
                Ok(lk_proto::well_known::Empty {}.encode_to_vec())
            }
            _ => Err(format!("unknown IOInfoSIP method: {method}")),
        }
    }
}

impl SipIoHandlers {
    async fn get_sip_trunk_authentication(
        &self,
        req: &rpc::GetSipTrunkAuthenticationRequest,
    ) -> Result<rpc::GetSipTrunkAuthenticationResponse, String> {
        let call = sip_call_from_auth(req);
        let trunk = self.match_trunk_for("", &call).await?;
        let Some(trunk) = trunk else {
            return Ok(rpc::GetSipTrunkAuthenticationResponse::default());
        };
        Ok(rpc::GetSipTrunkAuthenticationResponse {
            username: trunk.auth_username.clone(),
            password: trunk.auth_password.clone(),
            realm: trunk.auth_realm.clone(),
            sip_trunk_id: trunk.sip_trunk_id.clone(),
            provider_info: Some(lk::ProviderInfo {
                id: trunk.sip_trunk_id.clone(),
                name: trunk.name.clone(),
                r#type: lk::ProviderType::External as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn evaluate_sip_dispatch_rules(
        &self,
        req: &rpc::EvaluateSipDispatchRulesRequest,
    ) -> Result<rpc::EvaluateSipDispatchRulesResponse, String> {
        let call = sip_call_from_dispatch(req);
        let trunk = self.match_trunk_for(&req.sip_trunk_id, &call).await?;
        let trunk_id = trunk
            .as_ref()
            .map(|t| t.sip_trunk_id.clone())
            .unwrap_or_default();
        let rules = self.store.list_sip_dispatch_rules().await?;
        match match_dispatch_rule(&rules, trunk.as_ref(), req) {
            Ok(Some(rule)) => evaluate_dispatch_rule(&trunk, &rule, req, &call, &trunk_id),
            Ok(None) => Ok(rpc::EvaluateSipDispatchRulesResponse {
                sip_trunk_id: trunk_id,
                result: rpc::SipDispatchResult::Drop as i32,
                ..Default::default()
            }),
            Err(e) if e == ERR_NO_DISPATCH_MATCHED => Ok(rpc::EvaluateSipDispatchRulesResponse {
                sip_trunk_id: trunk_id,
                result: rpc::SipDispatchResult::Drop as i32,
                ..Default::default()
            }),
            Err(e) => Err(e),
        }
    }

    /// Loads the trunk referenced by `trunk_id` and checks it against the
    /// call; falls back to matching all inbound trunks. Mirrors
    /// `IOInfoService.matchSIPTrunk`.
    async fn match_trunk_for(
        &self,
        trunk_id: &str,
        call: &rpc::SipCall,
    ) -> Result<Option<lk::SipInboundTrunkInfo>, String> {
        if !trunk_id.is_empty() {
            if let Some(trunk) = self.store.load_sip_inbound_trunk(trunk_id).await? {
                if match_trunk(std::slice::from_ref(&trunk), call)?.is_some() {
                    return Ok(Some(trunk));
                }
            }
        }
        let all = self.store.list_sip_inbound_trunks().await?;
        match_trunk(&all, call)
    }
}

// ---------------------------------------------------------------------------
// SIPCall construction (mirrors `rpc.(GetSIPTrunkAuthenticationRequest|
// EvaluateSIPDispatchRulesRequest).SIPCall`)
// ---------------------------------------------------------------------------

fn sip_uri(user: &str, host: &str) -> lk::SipUri {
    lk::SipUri {
        user: user.to_string(),
        host: host.to_string(),
        ..Default::default()
    }
}

fn strip_port(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host.to_string(),
        _ => addr.to_string(),
    }
}

fn sip_call_from_auth(req: &rpc::GetSipTrunkAuthenticationRequest) -> rpc::SipCall {
    if let Some(call) = &req.call {
        return call.clone();
    }
    let to = sip_uri(&req.to, &req.to_host);
    rpc::SipCall {
        lk_call_id: req.sip_call_id.clone(),
        source_ip: strip_port(&req.src_address),
        address: Some(to.clone()),
        from: Some(sip_uri(&req.from, &req.from_host)),
        to: Some(to),
        ..Default::default()
    }
}

fn sip_call_from_dispatch(req: &rpc::EvaluateSipDispatchRulesRequest) -> rpc::SipCall {
    if let Some(call) = &req.call {
        return call.clone();
    }
    let to = sip_uri(&req.called_number, &req.called_host);
    rpc::SipCall {
        lk_call_id: req.sip_call_id.clone(),
        source_ip: strip_port(&req.src_address),
        address: Some(to.clone()),
        from: Some(sip_uri(&req.calling_number, &req.calling_host)),
        to: Some(to),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Trunk matching (mirrors `sip.MatchTrunkDetailed`)
// ---------------------------------------------------------------------------

/// Normalizes a phone number by stripping formatting and ensuring a leading
/// `+` (mirrors `livekit.NormalizeNumber`).
fn normalize_number(num: &str) -> String {
    if num.is_empty() {
        return String::new();
    }
    if !num
        .chars()
        .all(|c| c.is_ascii_digit() || "+- ()".contains(c))
    {
        return num.to_string();
    }
    let cleaned: String = num.chars().filter(|c| !" -()".contains(*c)).collect();
    if cleaned.starts_with('+') {
        cleaned
    } else {
        format!("+{cleaned}")
    }
}

fn match_numbers(num: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let norm = normalize_number(num);
    allowed
        .iter()
        .any(|a| num == a || norm == normalize_number(a))
}

fn is_valid_mask(mask: &str) -> bool {
    if mask.contains(['(', ')', '+', '*', ';', ',', ' ', '\t', '\n', '\r']) {
        return false;
    }
    !mask.contains("://")
}

fn match_addr_mask(ip: &IpAddr, mask: &str) -> bool {
    if !mask.contains('/') {
        return mask.parse::<IpAddr>().map(|e| *ip == e).unwrap_or(false);
    }
    mask.parse::<ipnet::IpNet>()
        .map(|net| net.contains(ip))
        .unwrap_or(false)
}

fn match_addr_masks(addr: &str, host: &str, masks: &[String]) -> bool {
    let Ok(ip) = addr.parse::<IpAddr>() else {
        return true;
    };
    let valid: Vec<&String> = masks.iter().filter(|m| is_valid_mask(m)).collect();
    if valid.is_empty() {
        return true;
    }
    valid
        .iter()
        .any(|m| m.as_str() == host || match_addr_mask(&ip, m))
}

/// Finds the trunk matching `call`; `None` when nothing matched, an error on
/// conflicting definitions.
fn match_trunk(
    trunks: &[lk::SipInboundTrunkInfo],
    call: &rpc::SipCall,
) -> Result<Option<lk::SipInboundTrunkInfo>, String> {
    let from = call
        .from
        .as_ref()
        .map(|f| f.user.clone())
        .unwrap_or_default();
    let from_host = call
        .from
        .as_ref()
        .map(|f| f.host.clone())
        .unwrap_or_default();
    let to = call.to.as_ref().map(|t| t.user.clone()).unwrap_or_default();
    let called_norm = normalize_number(&to);

    let mut selected: Option<lk::SipInboundTrunkInfo> = None;
    let mut default_trunk: Option<lk::SipInboundTrunkInfo> = None;
    let mut default_cnt = 0usize;

    for tr in trunks {
        if !match_numbers(&from, &tr.allowed_numbers) {
            continue;
        }
        if !match_addr_masks(&call.source_ip, &from_host, &tr.allowed_addresses) {
            continue;
        }
        if tr.numbers.is_empty() {
            // Default/wildcard trunk.
            default_cnt += 1;
            default_trunk = Some(tr.clone());
        } else {
            for num in &tr.numbers {
                if num == &to || normalize_number(num) == called_norm {
                    if selected.is_some() {
                        return Err(format!("Multiple SIP Trunks matched for {to:?}"));
                    }
                    selected = Some(tr.clone());
                    break;
                }
            }
        }
    }
    if let Some(s) = selected {
        return Ok(Some(s));
    }
    if default_cnt > 1 {
        return Err(format!("Multiple default SIP Trunks matched for {to:?}"));
    }
    Ok(default_trunk)
}

// ---------------------------------------------------------------------------
// Dispatch rule matching (mirrors `sip.MatchDispatchRuleIter`)
// ---------------------------------------------------------------------------

fn get_pin_and_room(info: &lk::SipDispatchRuleInfo) -> Result<(String, String), String> {
    let rule = info.rule.as_ref().ok_or("dispatch rule has no rule")?;
    match &rule.rule {
        Some(DispatchRule::DispatchRuleDirect(d)) => Ok((d.room_name.clone(), d.pin.clone())),
        Some(DispatchRule::DispatchRuleIndividual(i)) => Ok((i.room_prefix.clone(), i.pin.clone())),
        Some(DispatchRule::DispatchRuleCallee(c)) => Ok((c.room_prefix.clone(), c.pin.clone())),
        None => Err("unsupported dispatch rule".to_string()),
    }
}

fn dispatch_rule_priority(info: &lk::SipDispatchRuleInfo) -> i32 {
    let priority = match info.rule.as_ref().and_then(|r| r.rule.as_ref()) {
        Some(DispatchRule::DispatchRuleDirect(d)) => {
            if d.pin.is_empty() {
                100
            } else {
                0
            }
        }
        Some(DispatchRule::DispatchRuleIndividual(i)) => {
            if i.pin.is_empty() {
                101
            } else {
                1
            }
        }
        Some(DispatchRule::DispatchRuleCallee(c)) => {
            if c.pin.is_empty() {
                102
            } else {
                2
            }
        }
        _ => return i32::MAX,
    };
    let mut priority = priority;
    if info.inbound_numbers.is_empty() {
        priority += 1000;
    }
    if info.numbers.is_empty() {
        priority += 1000;
    }
    priority
}

fn has_higher_priority(r1: &lk::SipDispatchRuleInfo, r2: &lk::SipDispatchRuleInfo) -> bool {
    let p1 = dispatch_rule_priority(r1);
    let p2 = dispatch_rule_priority(r2);
    if p1 != p2 {
        return p1 < p2;
    }
    let room1 = get_pin_and_room(r1).map(|r| r.0).unwrap_or_default();
    let room2 = get_pin_and_room(r2).map(|r| r.0).unwrap_or_default();
    room1 < room2
}

/// Finds the best dispatch rule for `req`; `None` when nothing matched.
fn match_dispatch_rule(
    rules: &[lk::SipDispatchRuleInfo],
    trunk: Option<&lk::SipInboundTrunkInfo>,
    req: &rpc::EvaluateSipDispatchRulesRequest,
) -> Result<Option<lk::SipDispatchRuleInfo>, String> {
    let no_pin = req.no_pin;
    let sent_pin = req.pin.clone();
    let mut specific: Option<lk::SipDispatchRuleInfo> = None;
    let mut specific_cnt = 0usize;
    let mut default_rule: Option<lk::SipDispatchRuleInfo> = None;
    let mut default_cnt = 0usize;

    for info in rules {
        if !info.inbound_numbers.is_empty() && !info.inbound_numbers.contains(&req.calling_number) {
            continue;
        }
        if !info.numbers.is_empty() && !info.numbers.contains(&req.called_number) {
            continue;
        }
        let Ok((_, rule_pin)) = get_pin_and_room(info) else {
            continue;
        };
        if no_pin {
            if !rule_pin.is_empty() {
                continue;
            }
        } else if !sent_pin.is_empty() && (rule_pin.is_empty() || sent_pin != rule_pin) {
            continue;
        }
        if info.trunk_ids.is_empty() {
            default_cnt += 1;
            if default_rule.is_none() || has_higher_priority(info, default_rule.as_ref().unwrap()) {
                default_rule = Some(info.clone());
            }
            continue;
        }
        let Some(trunk) = trunk else {
            continue;
        };
        if !info.trunk_ids.contains(&trunk.sip_trunk_id) {
            continue;
        }
        specific_cnt += 1;
        if specific.is_none() || has_higher_priority(info, specific.as_ref().unwrap()) {
            specific = Some(info.clone());
        }
    }

    if specific_cnt == 0 && default_cnt == 0 {
        return Err(ERR_NO_DISPATCH_MATCHED.to_string());
    }
    Ok(specific.or(default_rule))
}

// ---------------------------------------------------------------------------
// Dispatch rule evaluation (mirrors `sip.EvaluateDispatchRule`)
// ---------------------------------------------------------------------------

/// Carries the deprecated `media_encryption` into `media.encryption` when
/// unset.
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

/// Fills zero fields of `base` from `other`.
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

fn evaluate_dispatch_rule(
    trunk: &Option<lk::SipInboundTrunkInfo>,
    rule: &lk::SipDispatchRuleInfo,
    req: &rpc::EvaluateSipDispatchRulesRequest,
    call: &rpc::SipCall,
    trunk_id: &str,
) -> Result<rpc::EvaluateSipDispatchRulesResponse, String> {
    let sent_pin = req.pin.clone();
    let from_full = call
        .from
        .as_ref()
        .map(|f| f.user.clone())
        .unwrap_or_default();
    let from_host = call
        .from
        .as_ref()
        .map(|f| f.host.clone())
        .unwrap_or_default();
    let to_full = call.to.as_ref().map(|t| t.user.clone()).unwrap_or_default();

    let mut attrs = rule.attributes.clone();
    for (k, v) in &req.extra_attributes {
        attrs.insert(k.clone(), v.clone());
    }
    attrs.insert(ATTR_SIP_CALL_ID.to_string(), call.lk_call_id.clone());
    attrs.insert(ATTR_SIP_TRUNK_ID.to_string(), trunk_id.to_string());

    let mut from = from_full.clone();
    let mut from_name = format!("Phone {from}");
    let mut from_id = format!("sip_{from}");
    if rule.hide_phone_number {
        // Mask the number, hash the identity, omit number attributes.
        let digest = format!("{:x}", sha2::Sha256::digest(from_full.as_bytes()));
        from_id = format!("sip_{}", &digest[..8]);
        let n = if from.len() <= 4 { 1 } else { 4 };
        from = from[from.len() - n..].to_string();
        from_name = format!("Phone {from}");
    } else {
        attrs.insert(ATTR_SIP_PHONE_NUMBER.to_string(), from_full.clone());
        attrs.insert(ATTR_SIP_HOST_NAME.to_string(), from_host);
        attrs.insert(ATTR_SIP_TRUNK_NUMBER.to_string(), to_full.clone());
    }

    let media = merge_media(
        trunk
            .as_ref()
            .and_then(|t| upgrade_media(&t.media, t.media_encryption)),
        upgrade_media(&rule.media, rule.media_encryption),
    );
    let enc = media.as_ref().and_then(|m| m.encryption).unwrap_or(0);

    let (room, rule_pin) = get_pin_and_room(rule)?;
    if !rule_pin.is_empty() {
        if sent_pin.is_empty() {
            return Ok(rpc::EvaluateSipDispatchRulesResponse {
                sip_trunk_id: trunk_id.to_string(),
                sip_dispatch_rule_id: rule.sip_dispatch_rule_id.clone(),
                request_pin: true,
                result: rpc::SipDispatchResult::RequestPin as i32,
                media_encryption: enc,
                media,
                ..Default::default()
            });
        }
        if rule_pin != sent_pin {
            return Err("Incorrect PIN for SIP room".to_string());
        }
    }

    let mut room = room;
    match rule.rule.as_ref().and_then(|r| r.rule.as_ref()) {
        Some(DispatchRule::DispatchRuleIndividual(i)) => {
            room = from.clone();
            if !i.room_prefix.is_empty() {
                room = format!("{}_{}", i.room_prefix, from);
            }
            if !i.no_randomness {
                room = format!("{}_{}", room, crate::core::generate_id(""));
            }
        }
        Some(DispatchRule::DispatchRuleCallee(c)) => {
            room = to_full.clone();
            if !c.room_prefix.is_empty() {
                room = format!("{}_{}", c.room_prefix, to_full);
            }
            if c.randomize {
                room = format!("{}_{}", room, crate::core::generate_id(""));
            }
        }
        _ => {}
    }
    attrs.insert(
        ATTR_SIP_RULE_ID.to_string(),
        rule.sip_dispatch_rule_id.clone(),
    );

    let mut resp = rpc::EvaluateSipDispatchRulesResponse {
        sip_trunk_id: trunk_id.to_string(),
        sip_dispatch_rule_id: rule.sip_dispatch_rule_id.clone(),
        result: rpc::SipDispatchResult::Accept as i32,
        room_name: room,
        participant_identity: from_id,
        participant_name: from_name,
        participant_metadata: rule.metadata.clone(),
        participant_attributes: attrs,
        room_preset: rule.room_preset.clone(),
        room_config: rule.room_config.clone(),
        media_encryption: enc,
        media,
        ..Default::default()
    };
    let mut krisp = false;
    if let Some(trunk) = trunk {
        resp.headers = trunk.headers.clone();
        resp.headers_to_attributes = trunk.headers_to_attributes.clone();
        resp.attributes_to_headers = trunk.attributes_to_headers.clone();
        resp.include_headers = trunk.include_headers;
        resp.ringing_timeout = trunk.ringing_timeout;
        resp.max_call_duration = trunk.max_call_duration;
        krisp = trunk.krisp_enabled;
    }
    if rule.krisp_enabled {
        krisp = true;
    }
    if krisp {
        resp.enabled_features
            .push(lk::SipFeature::KrispEnabled as i32);
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri(user: &str) -> lk::SipUri {
        lk::SipUri {
            user: user.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn normalizes_phone_numbers() {
        assert_eq!(normalize_number("+1 (555) 123-4567"), "+15551234567");
        assert_eq!(normalize_number("15551234567"), "+15551234567");
        assert_eq!(normalize_number("not-a-number"), "not-a-number");
        assert_eq!(normalize_number(""), "");
    }

    #[test]
    fn trunk_matches_specific_then_default() {
        let specific = lk::SipInboundTrunkInfo {
            sip_trunk_id: "ST_a".to_string(),
            numbers: vec!["+1555".to_string()],
            ..Default::default()
        };
        let wildcard = lk::SipInboundTrunkInfo {
            sip_trunk_id: "ST_w".to_string(),
            ..Default::default()
        };
        let call = rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            source_ip: "1.2.3.4".to_string(),
            ..Default::default()
        };
        let matched = match_trunk(&[specific.clone(), wildcard.clone()], &call).unwrap();
        assert_eq!(matched.unwrap().sip_trunk_id, "ST_a");

        let call2 = rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1444")),
            source_ip: "1.2.3.4".to_string(),
            ..Default::default()
        };
        let matched = match_trunk(&[specific, wildcard], &call2).unwrap();
        assert_eq!(matched.unwrap().sip_trunk_id, "ST_w");
    }

    #[test]
    fn trunk_allowed_numbers_and_addresses_filter() {
        let trunk = lk::SipInboundTrunkInfo {
            sip_trunk_id: "ST_a".to_string(),
            numbers: vec!["+1555".to_string()],
            allowed_numbers: vec!["+1999".to_string()],
            allowed_addresses: vec!["10.0.0.0/8".to_string()],
            ..Default::default()
        };
        let ok = rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            source_ip: "10.1.2.3".to_string(),
            ..Default::default()
        };
        assert!(match_trunk(std::slice::from_ref(&trunk), &ok)
            .unwrap()
            .is_some());

        // Disallowed calling number.
        let bad_from = rpc::SipCall {
            from: Some(uri("+1111")),
            to: Some(uri("+1555")),
            source_ip: "10.1.2.3".to_string(),
            ..Default::default()
        };
        assert!(match_trunk(std::slice::from_ref(&trunk), &bad_from)
            .unwrap()
            .is_none());

        // Disallowed source address.
        let bad_ip = rpc::SipCall {
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            source_ip: "203.0.113.5".to_string(),
            ..Default::default()
        };
        assert!(match_trunk(&[trunk], &bad_ip).unwrap().is_none());
    }

    fn direct_rule(id: &str, room: &str) -> lk::SipDispatchRuleInfo {
        lk::SipDispatchRuleInfo {
            sip_dispatch_rule_id: id.to_string(),
            rule: Some(lk::SipDispatchRule {
                rule: Some(DispatchRule::DispatchRuleDirect(
                    lk::SipDispatchRuleDirect {
                        room_name: room.to_string(),
                        ..Default::default()
                    },
                )),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn dispatch_rule_matches_and_prefers_specific() {
        let trunk_id = "ST_a";
        let trunk = lk::SipInboundTrunkInfo {
            sip_trunk_id: trunk_id.to_string(),
            ..Default::default()
        };
        let mut specific = direct_rule("SR_s", "room-s");
        specific.trunk_ids = vec![trunk_id.to_string()];
        let wildcard = direct_rule("SR_w", "room-w");
        let req = rpc::EvaluateSipDispatchRulesRequest::default();

        let matched =
            match_dispatch_rule(&[wildcard.clone(), specific.clone()], Some(&trunk), &req).unwrap();
        assert_eq!(matched.unwrap().sip_dispatch_rule_id, "SR_s");

        // Without a trunk, only the wildcard rule applies.
        let matched = match_dispatch_rule(&[wildcard.clone(), specific], None, &req).unwrap();
        assert_eq!(matched.unwrap().sip_dispatch_rule_id, "SR_w");
    }

    #[test]
    fn dispatch_rule_pin_flow() {
        let mut rule = direct_rule("SR_p", "room-p");
        rule.rule = Some(lk::SipDispatchRule {
            rule: Some(DispatchRule::DispatchRuleDirect(
                lk::SipDispatchRuleDirect {
                    room_name: "room-p".to_string(),
                    pin: "1234".to_string(),
                },
            )),
        });
        let call = rpc::SipCall {
            lk_call_id: "SC_1".to_string(),
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            ..Default::default()
        };
        // No pin sent -> request pin.
        let resp = evaluate_dispatch_rule(
            &None,
            &rule,
            &rpc::EvaluateSipDispatchRulesRequest::default(),
            &call,
            "",
        )
        .unwrap();
        assert_eq!(resp.result, rpc::SipDispatchResult::RequestPin as i32);
        assert!(resp.request_pin);

        // Wrong pin -> error.
        let req = rpc::EvaluateSipDispatchRulesRequest {
            pin: "9999".to_string(),
            ..Default::default()
        };
        assert!(evaluate_dispatch_rule(&None, &rule, &req, &call, "").is_err());

        // Correct pin -> accept into the room.
        let req = rpc::EvaluateSipDispatchRulesRequest {
            pin: "1234".to_string(),
            ..Default::default()
        };
        let resp = evaluate_dispatch_rule(&None, &rule, &req, &call, "").unwrap();
        assert_eq!(resp.result, rpc::SipDispatchResult::Accept as i32);
        assert_eq!(resp.room_name, "room-p");
        assert_eq!(resp.participant_identity, "sip_+1999");
        assert_eq!(
            resp.participant_attributes.get("sip.callID"),
            Some(&"SC_1".to_string())
        );
    }

    #[test]
    fn individual_rule_builds_per_caller_room() {
        let mut rule = lk::SipDispatchRuleInfo {
            sip_dispatch_rule_id: "SR_i".to_string(),
            rule: Some(lk::SipDispatchRule {
                rule: Some(DispatchRule::DispatchRuleIndividual(
                    lk::SipDispatchRuleIndividual {
                        room_prefix: "sales".to_string(),
                        no_randomness: true,
                        ..Default::default()
                    },
                )),
            }),
            ..Default::default()
        };
        let call = rpc::SipCall {
            lk_call_id: "SC_2".to_string(),
            from: Some(uri("+1999")),
            to: Some(uri("+1555")),
            ..Default::default()
        };
        let resp = evaluate_dispatch_rule(
            &None,
            &rule,
            &rpc::EvaluateSipDispatchRulesRequest::default(),
            &call,
            "",
        )
        .unwrap();
        assert_eq!(resp.room_name, "sales_+1999");
        assert_eq!(resp.participant_identity, "sip_+1999");

        // Hide the number: identity is a hash and number attributes are omitted.
        rule.hide_phone_number = true;
        let resp = evaluate_dispatch_rule(
            &None,
            &rule,
            &rpc::EvaluateSipDispatchRulesRequest::default(),
            &call,
            "",
        )
        .unwrap();
        assert_eq!(resp.participant_identity.len(), 12); // "sip_" + 8 hex
        assert!(!resp.participant_identity.contains('+'));
        assert!(!resp.participant_attributes.contains_key("sip.phoneNumber"));
    }
}
