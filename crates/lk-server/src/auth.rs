//! Access-token verification, compatible with LiveKit's HS256 JWT scheme.
//!
//! LiveKit tokens are standard HS256 JWTs where:
//!   - `iss` is the API key
//!   - `sub` is the participant identity
//!   - the `video` claim holds a `VideoGrant`
//!   - `roomConfig` may embed `RoomConfiguration` (e.g. auto-dispatched agents)
//!
//! The secret for the API key is looked up in `Config::keys`.

use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use crate::config::Config;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingAuthorization,
    #[error("invalid authorization header")]
    InvalidAuthorization,
    #[error("invalid api key")]
    InvalidApiKey,
    #[error("invalid token: {0}")]
    InvalidToken(String),
}

/// Parsed and verified claims from a LiveKit access token.
#[derive(Debug, Clone, Default)]
pub struct VerifiedToken {
    pub api_key: String,
    pub identity: String,
    pub name: String,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub kind: String,
    pub video: VideoGrant,
    pub sip: SipGrant,
    pub room_config: Option<RoomConfiguration>,
    pub room_preset: String,
    pub sha256: String,
    pub nbf: Option<i64>,
}

impl VerifiedToken {
    pub fn can_publish(&self) -> bool {
        self.video.can_publish.unwrap_or(true)
    }
    pub fn can_subscribe(&self) -> bool {
        self.video.can_subscribe.unwrap_or(true)
    }
    pub fn can_publish_data(&self) -> bool {
        self.video.can_publish_data.unwrap_or(self.can_publish())
    }
}

#[derive(Debug, Clone, Default)]
pub struct VideoGrant {
    pub room_create: bool,
    pub room_list: bool,
    pub room_record: bool,
    pub room_admin: bool,
    pub room_join: bool,
    pub room: String,
    pub can_publish: Option<bool>,
    pub can_subscribe: Option<bool>,
    pub can_publish_data: Option<bool>,
    pub can_publish_sources: Vec<String>,
    pub can_update_own_metadata: Option<bool>,
    pub hidden: bool,
    pub recorder: bool,
    pub agent: bool,
    pub can_subscribe_metrics: Option<bool>,
    pub destination_room: String,
}

#[derive(Debug, Clone, Default)]
pub struct SipGrant {
    pub admin: bool,
    pub call: bool,
}

#[derive(Debug, Clone, Default)]
pub struct RoomConfiguration {
    pub name: String,
    pub empty_timeout: u32,
    pub departure_timeout: u32,
    pub max_participants: u32,
    pub metadata: String,
    pub agents: Vec<RoomAgentDispatch>,
}

#[derive(Debug, Clone, Default)]
pub struct RoomAgentDispatch {
    pub agent_name: String,
    pub metadata: String,
    pub deployment: String,
    pub attributes: BTreeMap<String, String>,
}

/// Looks up secrets for API keys.
#[derive(Debug, Clone)]
pub struct KeyProvider {
    keys: BTreeMap<String, String>,
}

impl KeyProvider {
    pub fn new(config: &Config) -> Self {
        KeyProvider {
            keys: config.keys.clone(),
        }
    }

    pub fn from_map(keys: BTreeMap<String, String>) -> Self {
        KeyProvider { keys }
    }

    pub fn get_secret(&self, api_key: &str) -> Option<&str> {
        self.keys.get(api_key).map(String::as_str)
    }

    pub fn has_key(&self, api_key: &str) -> bool {
        self.keys.contains_key(api_key)
    }

    /// The full api key -> secret map (used by the TURN auth handler).
    pub fn as_map(&self) -> std::collections::BTreeMap<String, String> {
        self.keys.clone()
    }

    /// Parse and fully verify a bearer token against the configured keys.
    pub fn verify(&self, token: &str) -> Result<VerifiedToken, AuthError> {
        // 1. Read claims without verifying the signature to discover the API key.
        let unverified = decode_payload_unverified(token)?;

        let api_key = unverified
            .get("iss")
            .and_then(Value::as_str)
            .ok_or(AuthError::InvalidApiKey)?
            .to_string();
        let secret = self.get_secret(&api_key).ok_or(AuthError::InvalidApiKey)?;

        // 2. Verify signature, expiry, issuer and nbf.
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_issuer(std::slice::from_ref(&api_key));
        validation.validate_exp = true;
        validation.required_spec_claims = std::collections::HashSet::from(["exp".to_string()]);
        validation.validate_nbf = true;
        validation.leeway = 60;

        let data = jsonwebtoken::decode::<Value>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .map_err(|e| AuthError::InvalidToken(e.to_string()))?;

        Ok(parse_claims(api_key, data.claims))
    }
}

fn get_str(map: &Value, names: &[&str]) -> String {
    for name in names {
        if let Some(s) = map.get(*name).and_then(Value::as_str) {
            return s.to_string();
        }
    }
    String::new()
}

fn get_bool(map: &Value, names: &[&str]) -> bool {
    for name in names {
        if let Some(b) = map.get(*name).and_then(Value::as_bool) {
            return b;
        }
    }
    false
}

fn get_optional_bool(map: &Value, names: &[&str]) -> Option<bool> {
    for name in names {
        if let Some(v) = map.get(*name) {
            return v.as_bool();
        }
    }
    None
}

fn parse_claims(api_key: String, claims: Value) -> VerifiedToken {
    let identity = get_str(&claims, &["sub", "identity", "jti"]);
    let video = claims
        .get("video")
        .or_else(|| claims.get("vid"))
        .and_then(|v| v.as_object())
        .map(|v| {
            let v = serde_json::Value::Object(v.clone());
            VideoGrant {
                room_create: get_bool(&v, &["roomCreate", "room_create"]),
                room_list: get_bool(&v, &["roomList", "room_list"]),
                room_record: get_bool(&v, &["roomRecord", "room_record"]),
                room_admin: get_bool(&v, &["roomAdmin", "room_admin"]),
                room_join: get_bool(&v, &["roomJoin", "room_join"]),
                room: get_str(&v, &["room"]),
                can_publish: get_optional_bool(&v, &["canPublish", "can_publish"]),
                can_subscribe: get_optional_bool(&v, &["canSubscribe", "can_subscribe"]),
                can_publish_data: get_optional_bool(&v, &["canPublishData", "can_publish_data"]),
                can_publish_sources: v
                    .get("canPublishSources")
                    .or_else(|| v.get("can_publish_sources"))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
                can_update_own_metadata: get_optional_bool(
                    &v,
                    &["canUpdateOwnMetadata", "can_update_own_metadata"],
                ),
                hidden: get_bool(&v, &["hidden"]),
                recorder: get_bool(&v, &["recorder"]),
                agent: get_bool(&v, &["agent"]),
                can_subscribe_metrics: get_optional_bool(
                    &v,
                    &["canSubscribeMetrics", "can_subscribe_metrics"],
                ),
                destination_room: get_str(&v, &["destinationRoom", "destination_room"]),
            }
        })
        .unwrap_or_default();

    let room_config = claims
        .get("roomConfig")
        .or_else(|| claims.get("room_config"))
        .and_then(|v| v.as_object())
        .map(|rc| {
            let rc = serde_json::Value::Object(rc.clone());
            RoomConfiguration {
                name: get_str(&rc, &["name"]),
                empty_timeout: rc
                    .get("emptyTimeout")
                    .or_else(|| rc.get("empty_timeout"))
                    .and_then(Value::as_u64)
                    .map(|v| v as u32)
                    .unwrap_or(0),
                departure_timeout: rc
                    .get("departureTimeout")
                    .or_else(|| rc.get("departure_timeout"))
                    .and_then(Value::as_u64)
                    .map(|v| v as u32)
                    .unwrap_or(0),
                max_participants: rc
                    .get("maxParticipants")
                    .or_else(|| rc.get("max_participants"))
                    .and_then(Value::as_u64)
                    .map(|v| v as u32)
                    .unwrap_or(0),
                metadata: get_str(&rc, &["metadata"]),
                agents: rc
                    .get("agents")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_object())
                            .map(|d| {
                                let d = serde_json::Value::Object(d.clone());
                                RoomAgentDispatch {
                                    agent_name: get_str(&d, &["agentName", "agent_name"]),
                                    metadata: get_str(&d, &["metadata"]),
                                    deployment: get_str(&d, &["deployment"]),
                                    attributes: get_string_map(&d),
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        });

    VerifiedToken {
        api_key,
        identity,
        name: get_str(&claims, &["name"]),
        metadata: get_str(&claims, &["metadata"]),
        attributes: get_string_map(&claims),
        kind: get_str(&claims, &["kind"]),
        video,
        sip: claims
            .get("sip")
            .and_then(|v| v.as_object())
            .map(|v| {
                let v = serde_json::Value::Object(v.clone());
                SipGrant {
                    admin: get_bool(&v, &["admin"]),
                    call: get_bool(&v, &["call"]),
                }
            })
            .unwrap_or_default(),
        room_config,
        room_preset: get_str(&claims, &["roomPreset", "room_preset"]),
        sha256: get_str(&claims, &["sha256"]),
        nbf: claims.get("nbf").and_then(Value::as_i64),
    }
}

fn get_string_map(obj: &Value) -> BTreeMap<String, String> {
    obj.get("attributes")
        .or_else(|| obj.get("participantAttributes"))
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Decodes the JWT payload without verifying the signature (used to discover
/// the issuer/API key before selecting the verification secret).
fn decode_payload_unverified(token: &str) -> Result<Value, AuthError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::InvalidToken("malformed token".to_string()));
    }
    let payload_b64 = parts[1];
    let decoded = base64::Engine::decode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        payload_b64,
    )
    .map_err(|e| AuthError::InvalidToken(format!("invalid token payload: {e}")))?;
    serde_json::from_slice(&decoded)
        .map_err(|e| AuthError::InvalidToken(format!("invalid token claims: {e}")))
}

/// Convenience: parse a `Bearer <token>` Authorization header.
pub fn bearer_token(header: Option<&str>) -> Result<&str, AuthError> {
    match header {
        Some(h) => match h.strip_prefix("Bearer ") {
            Some(token) => Ok(token.trim()),
            None => Err(AuthError::InvalidAuthorization),
        },
        None => Err(AuthError::MissingAuthorization),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    const API_KEY: &str = "devkey";
    const SECRET: &str = "secret";

    fn provider() -> KeyProvider {
        let mut keys = BTreeMap::new();
        keys.insert(API_KEY.to_string(), SECRET.to_string());
        KeyProvider::from_map(keys)
    }

    fn make_token(payload: Value) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some("JWT".to_string());
        encode(
            &header,
            &payload,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    fn base_payload() -> Value {
        let now = crate::core::unix_seconds();
        serde_json::json!({
            "iss": API_KEY,
            "sub": "alice",
            "name": "Alice",
            "iat": now,
            "nbf": now - 5,
            "exp": now + 3600,
            "video": {
                "roomJoin": true,
                "room": "test-room",
                "canPublish": true,
                "canSubscribe": true,
                "canPublishData": true
            },
            "metadata": "{}",
            "roomConfig": {
                "agents": [{"agentName": "voice-agent", "metadata": "{\"a\":1}"}]
            }
        })
    }

    #[test]
    fn verifies_valid_token() {
        let token = make_token(base_payload());
        let verified = provider().verify(&token).unwrap();
        assert_eq!(verified.api_key, API_KEY);
        assert_eq!(verified.identity, "alice");
        assert_eq!(verified.name, "Alice");
        assert!(verified.can_publish());
        assert!(verified.can_subscribe());
        assert_eq!(verified.video.room, "test-room");
        let rc = verified.room_config.unwrap();
        assert_eq!(rc.agents.len(), 1);
        assert_eq!(rc.agents[0].agent_name, "voice-agent");
    }

    #[test]
    fn rejects_wrong_secret() {
        let token = make_token(base_payload());
        // Provider with a different secret
        let mut keys = BTreeMap::new();
        keys.insert(API_KEY.to_string(), "wrong-secret".to_string());
        let bad = KeyProvider::from_map(keys);
        assert!(bad.verify(&token).is_err());
    }

    #[test]
    fn rejects_unknown_api_key() {
        let mut payload = base_payload();
        payload["iss"] = Value::String("unknown-key".to_string());
        let token = make_token(payload);
        assert!(matches!(
            provider().verify(&token),
            Err(AuthError::InvalidApiKey)
        ));
    }

    #[test]
    fn rejects_expired_token() {
        let mut payload = base_payload();
        payload["exp"] = Value::from(1_690_000_000);
        let token = make_token(payload);
        assert!(provider().verify(&token).is_err());
    }

    #[test]
    fn rejects_missing_exp() {
        let mut payload = base_payload();
        payload.as_object_mut().unwrap().remove("exp");
        let token = make_token(payload);
        assert!(provider().verify(&token).is_err());
    }

    #[test]
    fn defaults_publish_and_subscribe_when_unset() {
        let mut payload = base_payload();
        payload["video"] = serde_json::json!({"roomJoin": true, "room": "r"});
        let token = make_token(payload);
        let verified = provider().verify(&token).unwrap();
        assert!(verified.can_publish());
        assert!(verified.can_subscribe());
        assert!(verified.can_publish_data());
    }

    #[test]
    fn accepts_snake_case_claims() {
        let payload = serde_json::json!({
            "iss": API_KEY,
            "sub": "bob",
            "exp": crate::core::unix_seconds() + 3600,
            "video": {
                "room_join": true,
                "room": "r2"
            },
            "room_config": {"empty_timeout": 120}
        });
        let token = make_token(payload);
        let verified = provider().verify(&token).unwrap();
        assert!(verified.video.room_join);
        assert_eq!(verified.video.room, "r2");
        assert_eq!(verified.room_config.unwrap().empty_timeout, 120);
    }

    #[test]
    fn parses_sip_grant() {
        let payload = serde_json::json!({
            "iss": API_KEY,
            "sub": "sip-client",
            "exp": crate::core::unix_seconds() + 3600,
            "sip": {"admin": true, "call": true}
        });
        let token = make_token(payload);
        let verified = provider().verify(&token).unwrap();
        assert!(verified.sip.admin);
        assert!(verified.sip.call);
    }

    #[test]
    fn bearer_token_extraction() {
        assert_eq!(
            bearer_token(Some("Bearer abc.def.ghi")).unwrap(),
            "abc.def.ghi"
        );
        assert!(matches!(
            bearer_token(Some("Basic abc")),
            Err(AuthError::InvalidAuthorization)
        ));
        assert!(matches!(
            bearer_token(None),
            Err(AuthError::MissingAuthorization)
        ));
    }
}
