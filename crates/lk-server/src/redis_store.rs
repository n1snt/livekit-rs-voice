//! Optional Redis store for SIP trunks/dispatch rules and egress state.
//!
//! Uses the same Redis hash keys and binary-protobuf encoding as the reference
//! server (`sip_trunk`, `sip_inbound_trunk`, `sip_outbound_trunk`,
//! `sip_dispatch_rule`, `egress`, `ended_egress`) so the external
//! `livekit/sip` and `livekit/egress` containers interoperate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lk_proto::livekit as lk;
use prost::Message as _;

use crate::config::Config;

const KEY_SIP_TRUNK: &str = "sip_trunk";
const KEY_SIP_INBOUND: &str = "sip_inbound_trunk";
const KEY_SIP_OUTBOUND: &str = "sip_outbound_trunk";
const KEY_SIP_DISPATCH: &str = "sip_dispatch_rule";
const KEY_EGRESS: &str = "egress";

#[allow(clippy::large_enum_variant)]
/// SIP trunk/dispatch + egress storage. Falls back to in-memory when Redis is
/// not configured (so the server runs standalone), but persists to Redis when
/// configured for container interoperability.
pub enum Store {
    Memory(MemoryStore),
    Redis(RedisStore),
}

impl Store {
    pub fn from_config(config: &Config) -> Arc<Self> {
        if config.redis.is_configured() {
            Arc::new(Store::Redis(RedisStore::new(config)))
        } else {
            Arc::new(Store::Memory(MemoryStore::default()))
        }
    }
}

// ---------------------------------------------------------------------------
// Memory store
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct MemoryStore {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemoryStore {
    fn set(&self, key: &str, id: &str, bytes: Vec<u8>) {
        self.data
            .lock()
            .unwrap()
            .insert(format!("{key}:{id}"), bytes);
    }

    fn get(&self, key: &str, id: &str) -> Option<Vec<u8>> {
        self.data
            .lock()
            .unwrap()
            .get(&format!("{key}:{id}"))
            .cloned()
    }

    fn del(&self, key: &str, id: &str) {
        self.data.lock().unwrap().remove(&format!("{key}:{id}"));
    }

    fn sadd(&self, _key: &str, _member: &str) {
        // The in-memory store serves the room set from the egress hash itself.
    }

    fn list(&self, key: &str) -> Vec<Vec<u8>> {
        let prefix = format!("{key}:");
        self.data
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Redis store
// ---------------------------------------------------------------------------

pub struct RedisStore {
    address: String,
    username: String,
    password: String,
    db: i64,
    use_tls: bool,
    conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
}

impl RedisStore {
    pub fn new(config: &Config) -> Self {
        RedisStore {
            address: config.redis.address.clone(),
            username: config.redis.username.clone(),
            password: config.redis.password.clone(),
            db: config.redis.db,
            use_tls: config.redis.use_tls,
            conn: tokio::sync::OnceCell::new(),
        }
    }

    async fn connect(&self) -> Result<redis::aio::ConnectionManager, String> {
        let url = format!(
            "{}://{}:{}@{}/{}",
            if self.use_tls { "rediss" } else { "redis" },
            self.username,
            self.password,
            self.address,
            self.db
        );
        let client = redis::Client::open(url).map_err(|e| format!("redis connect: {e}"))?;
        let manager = client
            .get_connection_manager()
            .await
            .map_err(|e| format!("redis manager: {e}"))?;
        Ok(manager)
    }

    async fn conn(&self) -> Result<redis::aio::ConnectionManager, String> {
        if let Some(c) = self.conn.get() {
            return Ok(c.clone());
        }
        let manager = self.connect().await?;
        let _ = self.conn.set(manager.clone());
        Ok(manager)
    }

    async fn hset(&self, key: &str, id: &str, bytes: &[u8]) -> Result<(), String> {
        let mut conn = self.conn().await?;
        redis::AsyncCommands::hset(&mut conn, key, id, bytes)
            .await
            .map_err(|e| format!("redis hset: {e}"))
    }

    async fn hget(&self, key: &str, id: &str) -> Result<Option<Vec<u8>>, String> {
        let mut conn = self.conn().await?;
        let out: Option<Vec<u8>> = redis::AsyncCommands::hget(&mut conn, key, id)
            .await
            .map_err(|e| format!("redis hget: {e}"))?;
        Ok(out)
    }

    async fn hdel(&self, key: &str, id: &str) -> Result<(), String> {
        let mut conn = self.conn().await?;
        let _: () = redis::AsyncCommands::hdel(&mut conn, key, id)
            .await
            .map_err(|e| format!("redis hdel: {e}"))?;
        Ok(())
    }

    async fn sadd(&self, key: &str, member: &str) -> Result<(), String> {
        let mut conn = self.conn().await?;
        let _: () = redis::AsyncCommands::sadd(&mut conn, key, member)
            .await
            .map_err(|e| format!("redis sadd: {e}"))?;
        Ok(())
    }

    async fn hgetall(&self, key: &str) -> Result<Vec<Vec<u8>>, String> {
        let mut conn = self.conn().await?;
        let map: HashMap<String, Vec<u8>> = redis::AsyncCommands::hgetall(&mut conn, key)
            .await
            .map_err(|e| format!("redis hgetall: {e}"))?;
        Ok(map.into_values().collect())
    }
}

// ---------------------------------------------------------------------------
// Store API
// ---------------------------------------------------------------------------

impl Store {
    pub async fn store_sip_inbound_trunk(&self, t: &lk::SipInboundTrunkInfo) -> Result<(), String> {
        let bytes = t.encode_to_vec();
        match self {
            Store::Memory(m) => m.set(KEY_SIP_INBOUND, &t.sip_trunk_id, bytes),
            Store::Redis(r) => r.hset(KEY_SIP_INBOUND, &t.sip_trunk_id, &bytes).await?,
        }
        Ok(())
    }

    pub async fn load_sip_inbound_trunk(
        &self,
        id: &str,
    ) -> Result<Option<lk::SipInboundTrunkInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.get(KEY_SIP_INBOUND, id),
            Store::Redis(r) => r.hget(KEY_SIP_INBOUND, id).await?,
        };
        Ok(bytes.map(|b| lk::SipInboundTrunkInfo::decode(b.as_slice()).unwrap_or_default()))
    }

    pub async fn list_sip_inbound_trunks(&self) -> Result<Vec<lk::SipInboundTrunkInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.list(KEY_SIP_INBOUND),
            Store::Redis(r) => r.hgetall(KEY_SIP_INBOUND).await?,
        };
        Ok(bytes
            .iter()
            .filter_map(|b| lk::SipInboundTrunkInfo::decode(b.as_slice()).ok())
            .collect())
    }

    pub async fn store_sip_outbound_trunk(
        &self,
        t: &lk::SipOutboundTrunkInfo,
    ) -> Result<(), String> {
        let bytes = t.encode_to_vec();
        match self {
            Store::Memory(m) => m.set(KEY_SIP_OUTBOUND, &t.sip_trunk_id, bytes),
            Store::Redis(r) => r.hset(KEY_SIP_OUTBOUND, &t.sip_trunk_id, &bytes).await?,
        }
        Ok(())
    }

    pub async fn load_sip_outbound_trunk(
        &self,
        id: &str,
    ) -> Result<Option<lk::SipOutboundTrunkInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.get(KEY_SIP_OUTBOUND, id),
            Store::Redis(r) => r.hget(KEY_SIP_OUTBOUND, id).await?,
        };
        Ok(bytes.map(|b| lk::SipOutboundTrunkInfo::decode(b.as_slice()).unwrap_or_default()))
    }

    pub async fn list_sip_outbound_trunks(&self) -> Result<Vec<lk::SipOutboundTrunkInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.list(KEY_SIP_OUTBOUND),
            Store::Redis(r) => r.hgetall(KEY_SIP_OUTBOUND).await?,
        };
        Ok(bytes
            .iter()
            .filter_map(|b| lk::SipOutboundTrunkInfo::decode(b.as_slice()).ok())
            .collect())
    }

    pub async fn delete_sip_trunk(&self, id: &str) -> Result<(), String> {
        match self {
            Store::Memory(m) => {
                m.del(KEY_SIP_TRUNK, id);
                m.del(KEY_SIP_INBOUND, id);
                m.del(KEY_SIP_OUTBOUND, id);
            }
            Store::Redis(r) => {
                r.hdel(KEY_SIP_TRUNK, id).await?;
                r.hdel(KEY_SIP_INBOUND, id).await?;
                r.hdel(KEY_SIP_OUTBOUND, id).await?;
            }
        }
        Ok(())
    }

    pub async fn store_sip_dispatch_rule(
        &self,
        rule: &lk::SipDispatchRuleInfo,
    ) -> Result<(), String> {
        let bytes = rule.encode_to_vec();
        match self {
            Store::Memory(m) => m.set(KEY_SIP_DISPATCH, &rule.sip_dispatch_rule_id, bytes),
            Store::Redis(r) => {
                r.hset(KEY_SIP_DISPATCH, &rule.sip_dispatch_rule_id, &bytes)
                    .await?
            }
        }
        Ok(())
    }

    pub async fn load_sip_dispatch_rule(
        &self,
        id: &str,
    ) -> Result<Option<lk::SipDispatchRuleInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.get(KEY_SIP_DISPATCH, id),
            Store::Redis(r) => r.hget(KEY_SIP_DISPATCH, id).await?,
        };
        Ok(bytes.map(|b| lk::SipDispatchRuleInfo::decode(b.as_slice()).unwrap_or_default()))
    }

    pub async fn list_sip_dispatch_rules(&self) -> Result<Vec<lk::SipDispatchRuleInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.list(KEY_SIP_DISPATCH),
            Store::Redis(r) => r.hgetall(KEY_SIP_DISPATCH).await?,
        };
        Ok(bytes
            .iter()
            .filter_map(|b| lk::SipDispatchRuleInfo::decode(b.as_slice()).ok())
            .collect())
    }

    pub async fn delete_sip_dispatch_rule(&self, id: &str) -> Result<(), String> {
        match self {
            Store::Memory(m) => m.del(KEY_SIP_DISPATCH, id),
            Store::Redis(r) => r.hdel(KEY_SIP_DISPATCH, id).await?,
        }
        Ok(())
    }

    pub async fn store_egress(&self, info: &lk::EgressInfo) -> Result<(), String> {
        let bytes = info.encode_to_vec();
        match self {
            Store::Memory(m) => {
                m.set(KEY_EGRESS, &info.egress_id, bytes);
                if !info.room_name.is_empty() {
                    m.sadd(
                        &format!("{KEY_EGRESS}:room:{}", info.room_name),
                        &info.egress_id,
                    );
                }
            }
            Store::Redis(r) => {
                r.hset(KEY_EGRESS, &info.egress_id, &bytes).await?;
                if !info.room_name.is_empty() {
                    r.sadd(
                        &format!("{KEY_EGRESS}:room:{}", info.room_name),
                        &info.egress_id,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn load_egress(&self, id: &str) -> Result<Option<lk::EgressInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.get(KEY_EGRESS, id),
            Store::Redis(r) => r.hget(KEY_EGRESS, id).await?,
        };
        Ok(bytes.map(|b| lk::EgressInfo::decode(b.as_slice()).unwrap_or_default()))
    }

    pub async fn list_egress(&self) -> Result<Vec<lk::EgressInfo>, String> {
        let bytes = match self {
            Store::Memory(m) => m.list(KEY_EGRESS),
            Store::Redis(r) => r.hgetall(KEY_EGRESS).await?,
        };
        Ok(bytes
            .iter()
            .filter_map(|b| lk::EgressInfo::decode(b.as_slice()).ok())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_round_trips() {
        let store = Store::Memory(MemoryStore::default());
        let trunk = lk::SipOutboundTrunkInfo {
            sip_trunk_id: "ST_1".to_string(),
            name: "plat".to_string(),
            address: "203.0.113.10".to_string(),
            numbers: vec!["+91".to_string()],
            ..Default::default()
        };
        store.store_sip_outbound_trunk(&trunk).await.unwrap();
        let loaded = store
            .load_sip_outbound_trunk("ST_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.name, "plat");
        assert_eq!(loaded.address, "203.0.113.10");
        assert_eq!(store.list_sip_outbound_trunks().await.unwrap().len(), 1);

        store.delete_sip_trunk("ST_1").await.unwrap();
        assert!(store
            .load_sip_outbound_trunk("ST_1")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_store_dispatch_rules() {
        let store = Store::Memory(MemoryStore::default());
        let rule = lk::SipDispatchRuleInfo {
            sip_dispatch_rule_id: "SR_1".to_string(),
            name: "inbound".to_string(),
            ..Default::default()
        };
        store.store_sip_dispatch_rule(&rule).await.unwrap();
        let loaded = store.load_sip_dispatch_rule("SR_1").await.unwrap().unwrap();
        assert_eq!(loaded.name, "inbound");
        assert_eq!(store.list_sip_dispatch_rules().await.unwrap().len(), 1);
        store.delete_sip_dispatch_rule("SR_1").await.unwrap();
        assert!(store
            .load_sip_dispatch_rule("SR_1")
            .await
            .unwrap()
            .is_none());
    }
}
