//! Rust-native multi-node clustering over Redis.
//!
//! A small cluster bus (Redis streams + hashes) provides:
//! - a **node registry** (heartbeats) so nodes discover each other,
//! - a **room registry** so each room is hosted on exactly one node,
//! - a **signaling relay** so a client connected to any node can join a room
//!   hosted on another node, transparently.
//!
//! This is internal to `livekit-rs-voice` nodes; the client-facing LiveKit
//! protocol is unchanged. The relay carries protobuf-encoded
//! `SignalRequest`/`SignalResponse` messages between nodes over Redis streams.
//!
//! When Redis is not configured (or `redis.cluster` is false) the cluster is
//! disabled and every room is local, preserving the single-node fast path.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lk_proto::livekit as lk;
use prost::Message as _;

use crate::config::Config;
use crate::server::Server;
use crate::signal::{self, SessionParams};

const NODE_HEARTBEAT_SECS: u64 = 5;
const NODE_TTL_SECS: u64 = 15;
const BUS_READ_TIMEOUT_MS: u64 = 5000;

// Redis key layout (all under the `lk` prefix).
fn node_key(node_id: &str) -> String {
    format!("lk:node:{node_id}")
}
fn rooms_key() -> &'static str {
    "lk:rooms"
}
fn relay_ctrl(node_id: &str) -> String {
    format!("lk:relay:{node_id}:ctrl")
}
fn relay_in(session_id: &str) -> String {
    format!("lk:relay:{session_id}:in")
}
fn relay_out(session_id: &str) -> String {
    format!("lk:relay:{session_id}:out")
}

/// Minimal key-value / hash / stream operations needed by the cluster.
/// Implemented on real Redis and on an in-memory bus (for tests and for the
/// disabled single-node mode).
#[async_trait::async_trait]
pub trait ClusterBus: Send + Sync {
    async fn set_with_ttl(&self, key: &str, value: &str, ttl_secs: u64) -> Result<(), String>;
    async fn get(&self, key: &str) -> Result<Option<String>, String>;
    async fn hset(&self, hash: &str, field: &str, value: &str) -> Result<(), String>;
    async fn hsetnx(&self, hash: &str, field: &str, value: &str) -> Result<bool, String>;
    async fn hget(&self, hash: &str, field: &str) -> Result<Option<String>, String>;
    async fn hdel(&self, hash: &str, field: &str) -> Result<(), String>;
    async fn del(&self, key: &str) -> Result<(), String>;
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, String>;
    async fn xadd(&self, stream: &str, fields: Vec<(String, String)>) -> Result<(), String>;
    /// Blocks up to `timeout_ms` for entries newer than `last_id` on `stream`.
    /// Returns `(id, fields)` entries in order; `None` on timeout.
    async fn xread_block(
        &self,
        stream: &str,
        last_id: &str,
        timeout_ms: u64,
    ) -> Result<Option<Vec<(String, Vec<(String, String)>)>>, String>;
}

// ---------------------------------------------------------------------------
// RedisBus
// ---------------------------------------------------------------------------

pub struct RedisBus {
    address: String,
    username: String,
    password: String,
    db: i64,
    use_tls: bool,
    conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
}

impl RedisBus {
    pub fn new(config: &Config) -> Arc<Self> {
        Arc::new(RedisBus {
            address: config.redis.address.clone(),
            username: config.redis.username.clone(),
            password: config.redis.password.clone(),
            db: config.redis.db,
            use_tls: config.redis.use_tls,
            conn: tokio::sync::OnceCell::new(),
        })
    }

    async fn conn(&self) -> Result<redis::aio::ConnectionManager, String> {
        if let Some(c) = self.conn.get() {
            return Ok(c.clone());
        }
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
        let _ = self.conn.set(manager.clone());
        Ok(manager)
    }
}

#[async_trait::async_trait]
impl ClusterBus for RedisBus {
    async fn set_with_ttl(&self, key: &str, value: &str, ttl_secs: u64) -> Result<(), String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::set_ex(&mut c, key, value, ttl_secs)
            .await
            .map_err(|e| format!("redis set: {e}"))
    }
    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::get(&mut c, key)
            .await
            .map_err(|e| format!("redis get: {e}"))
    }
    async fn hset(&self, hash: &str, field: &str, value: &str) -> Result<(), String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::hset(&mut c, hash, field, value)
            .await
            .map_err(|e| format!("redis hset: {e}"))
    }
    async fn hsetnx(&self, hash: &str, field: &str, value: &str) -> Result<bool, String> {
        let mut c = self.conn().await?;
        let n: i32 = redis::cmd("HSETNX")
            .arg(hash)
            .arg(field)
            .arg(value)
            .query_async(&mut c)
            .await
            .map_err(|e| format!("redis hsetnx: {e}"))?;
        Ok(n == 1)
    }
    async fn hget(&self, hash: &str, field: &str) -> Result<Option<String>, String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::hget(&mut c, hash, field)
            .await
            .map_err(|e| format!("redis hget: {e}"))
    }
    async fn hdel(&self, hash: &str, field: &str) -> Result<(), String> {
        let mut c = self.conn().await?;
        let _: () = redis::AsyncCommands::hdel(&mut c, hash, field)
            .await
            .map_err(|e| format!("redis hdel: {e}"))?;
        Ok(())
    }
    async fn del(&self, key: &str) -> Result<(), String> {
        let mut c = self.conn().await?;
        let _: () = redis::AsyncCommands::del(&mut c, key)
            .await
            .map_err(|e| format!("redis del: {e}"))?;
        Ok(())
    }
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::keys(&mut c, pattern)
            .await
            .map_err(|e| format!("redis keys: {e}"))
    }
    async fn xadd(&self, stream: &str, fields: Vec<(String, String)>) -> Result<(), String> {
        let mut c = self.conn().await?;
        redis::AsyncCommands::xadd(&mut c, stream, "*", &fields)
            .await
            .map_err(|e| format!("redis xadd: {e}"))
    }
    async fn xread_block(
        &self,
        stream: &str,
        last_id: &str,
        timeout_ms: u64,
    ) -> Result<Option<Vec<(String, Vec<(String, String)>)>>, String> {
        let mut c = self.conn().await?;
        let opts = redis::streams::StreamReadOptions::default().block(timeout_ms as usize);
        let reply: redis::streams::StreamReadReply =
            redis::AsyncCommands::xread_options(&mut c, &[stream], &[last_id], &opts)
                .await
                .map_err(|e| format!("redis xread: {e}"))?;
        let mut out = Vec::new();
        for key in reply.keys {
            for id in key.ids {
                let mut fields = Vec::new();
                for (k, v) in id.map {
                    let s: String = redis::FromRedisValue::from_redis_value(&v).unwrap_or_default();
                    fields.push((k, s));
                }
                out.push((id.id, fields));
            }
        }
        Ok(Some(out))
    }
}

// MemoryBus (tests + disabled mode)
// ---------------------------------------------------------------------------

type StreamEntry = (u64, Vec<(String, String)>);

#[derive(Default)]
struct MemInner {
    kv: HashMap<String, String>,
    hashes: HashMap<String, HashMap<String, String>>,
    streams: HashMap<String, VecDeque<StreamEntry>>,
    next_id: HashMap<String, u64>,
}

#[derive(Default)]
pub struct MemoryBus {
    inner: std::sync::Mutex<MemInner>,
}

impl MemoryBus {
    fn next(&self, stream: &str) -> u64 {
        let mut i = self.inner.lock().unwrap();
        let n = i.next_id.entry(stream.to_string()).or_insert(0);
        *n += 1;
        *n
    }
}

#[async_trait::async_trait]
impl ClusterBus for MemoryBus {
    async fn set_with_ttl(&self, key: &str, value: &str, _ttl: u64) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap()
            .kv
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.inner.lock().unwrap().kv.get(key).cloned())
    }
    async fn hset(&self, hash: &str, field: &str, value: &str) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap()
            .hashes
            .entry(hash.to_string())
            .or_default()
            .insert(field.to_string(), value.to_string());
        Ok(())
    }
    async fn hsetnx(&self, hash: &str, field: &str, value: &str) -> Result<bool, String> {
        let mut i = self.inner.lock().unwrap();
        let h = i.hashes.entry(hash.to_string()).or_default();
        if h.contains_key(field) {
            Ok(false)
        } else {
            h.insert(field.to_string(), value.to_string());
            Ok(true)
        }
    }
    async fn hget(&self, hash: &str, field: &str) -> Result<Option<String>, String> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .hashes
            .get(hash)
            .and_then(|h| h.get(field))
            .cloned())
    }
    async fn hdel(&self, hash: &str, field: &str) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap()
            .hashes
            .get_mut(hash)
            .map(|h| h.remove(field));
        Ok(())
    }
    async fn del(&self, key: &str) -> Result<(), String> {
        self.inner.lock().unwrap().kv.remove(key);
        Ok(())
    }
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, String> {
        let prefix = pattern.trim_end_matches('*');
        Ok(self
            .inner
            .lock()
            .unwrap()
            .kv
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
    async fn xadd(&self, stream: &str, fields: Vec<(String, String)>) -> Result<(), String> {
        let id = self.next(stream);
        let mut i = self.inner.lock().unwrap();
        i.streams
            .entry(stream.to_string())
            .or_default()
            .push_back((id, fields));
        Ok(())
    }
    async fn xread_block(
        &self,
        stream: &str,
        last_id: &str,
        timeout_ms: u64,
    ) -> Result<Option<Vec<(String, Vec<(String, String)>)>>, String> {
        let last: u64 = if last_id == "$" {
            0
        } else {
            last_id.parse().unwrap_or(0)
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        // Poll the in-memory stream (this bus is only used in tests / disabled
        // mode; the real Redis bus blocks natively).
        loop {
            let mut out = Vec::new();
            {
                let mut i = self.inner.lock().unwrap();
                if let Some(q) = i.streams.get_mut(stream) {
                    while let Some(front) = q.front() {
                        if front.0 > last {
                            let (id, fields) = q.pop_front().unwrap();
                            out.push((id.to_string(), fields));
                        } else {
                            q.pop_front();
                        }
                    }
                }
            }
            if !out.is_empty() {
                return Ok(Some(out));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

pub enum Routing {
    Local,
    Remote(String),
}

pub struct Cluster {
    pub bus: Arc<dyn ClusterBus>,
    pub node_id: String,
    pub enabled: AtomicBool,
}

impl Cluster {
    pub fn new(config: &Config, node_id: &str) -> Arc<Self> {
        if config.redis.is_configured() && config.redis.cluster {
            Arc::new(Cluster {
                bus: RedisBus::new(config),
                node_id: node_id.to_string(),
                enabled: AtomicBool::new(true),
            })
        } else {
            Arc::new(Cluster {
                bus: Arc::new(MemoryBus::default()),
                node_id: node_id.to_string(),
                enabled: AtomicBool::new(false),
            })
        }
    }

    pub fn new_with_bus(bus: Arc<dyn ClusterBus>, node_id: &str, enabled: bool) -> Arc<Self> {
        Arc::new(Cluster {
            bus,
            node_id: node_id.to_string(),
            enabled: AtomicBool::new(enabled),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Heartbeat loop registering this node in the registry.
    pub fn start_heartbeat(&self) {
        if !self.is_enabled() {
            return;
        }
        let bus = self.bus.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            loop {
                let _ = bus
                    .set_with_ttl(&node_key(&node_id), "alive", NODE_TTL_SECS)
                    .await;
                tokio::time::sleep(Duration::from_secs(NODE_HEARTBEAT_SECS)).await;
            }
        });
    }

    /// Removes this node from the registry (call on shutdown).
    pub async fn deregister(&self) {
        if self.is_enabled() {
            let _ = self.bus.del(&node_key(&self.node_id)).await;
        }
    }

    pub async fn node_alive(&self, node_id: &str) -> bool {
        self.bus
            .get(&node_key(node_id))
            .await
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    /// Determines which node hosts `room`, claiming it for this node when
    /// unowned (or when its previous owner is dead). Atomic via `HSETNX`.
    pub async fn route_room(&self, room: &str) -> Routing {
        if !self.is_enabled() {
            return Routing::Local;
        }
        for _ in 0..8 {
            let owner = self.bus.hget(rooms_key(), room).await.unwrap_or_default();
            match owner {
                Some(owner) if owner == self.node_id => return Routing::Local,
                Some(owner) if self.node_alive(&owner).await => return Routing::Remote(owner),
                _ => {
                    // Unowned or dead owner: try to claim.
                    if self
                        .bus
                        .hsetnx(rooms_key(), room, &self.node_id)
                        .await
                        .unwrap_or(false)
                    {
                        return Routing::Local;
                    }
                }
            }
        }
        // Contention exhausted: resolve to the observed owner rather than
        // silently splitting the room across nodes.
        let owner = self.bus.hget(rooms_key(), room).await.unwrap_or_default();
        match owner {
            Some(owner) if owner == self.node_id => Routing::Local,
            Some(owner) if self.node_alive(&owner).await => Routing::Remote(owner),
            _ => Routing::Local,
        }
    }

    /// Releases the registry entry for a room, only if this node owns it.
    pub async fn release_room(&self, room: &str) {
        if !self.is_enabled() {
            return;
        }
        let owner = self.bus.hget(rooms_key(), room).await.unwrap_or_default();
        if owner.as_deref() == Some(self.node_id.as_str()) {
            let _ = self.bus.hdel(rooms_key(), room).await;
        }
    }

    // ---- signaling relay ----

    /// Node B: consumes relay control messages for this node and runs the
    /// remote sessions they start. Spawned once at startup.
    pub async fn run_relay_consumer(self: &Arc<Self>, server: &Arc<Server>) {
        let bus = self.bus.clone();
        let node_id = self.node_id.clone();
        let mut last_id = "0".to_string();
        loop {
            if let Ok(Some(entries)) = bus
                .xread_block(&relay_ctrl(&node_id), &last_id, BUS_READ_TIMEOUT_MS)
                .await
            {
                for (id, fields) in entries {
                    last_id = id;
                    let fields: HashMap<String, String> = fields.into_iter().collect();
                    if fields.get("kind").map(String::as_str) == Some("start") {
                        let session_id = fields.get("session_id").cloned().unwrap_or_default();
                        let token = fields.get("token").cloned().unwrap_or_default();
                        let params = fields
                            .get("params")
                            .and_then(|p| serde_json::from_str::<SessionParams>(p).ok())
                            .unwrap_or_default();
                        let server = server.clone();
                        let cluster = self.clone();
                        tokio::spawn(async move {
                            run_remote_session(&cluster, &server, &session_id, &token, params)
                                .await;
                        });
                    }
                }
            }
        }
    }
}

/// Node B: runs a full participant session whose signal transport is the
/// Redis relay (mirrors the local WS session path).
pub async fn run_remote_session(
    cluster: &Arc<Cluster>,
    server: &Arc<Server>,
    session_id: &str,
    token_str: &str,
    params: SessionParams,
) {
    let io = relay_io(cluster.bus.clone(), session_id.to_string());
    let token = match server.keys.verify(token_str) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(session = %session_id, "relay: invalid token: {e}");
            let _ = cluster
                .bus
                .xadd(
                    &relay_out(session_id),
                    vec![("kind".to_string(), "close".to_string())],
                )
                .await;
            return;
        }
    };
    let kind = signal::participant_kind_from_token(&token);
    if let Err(e) = signal::run_session_with_io(io, server, token, params, kind).await {
        tracing::warn!(session = %session_id, "relay session ended: {e}");
    }
}

/// Read half of the relay transport: reads `SignalRequest`s from the
/// node-A→node-B stream. Ends (returns `None`) on a `kind=close` sentinel.
pub struct RelayIoReader {
    bus: Arc<dyn ClusterBus>,
    session_id: String,
    last_id: String,
    pending: VecDeque<lk::SignalRequest>,
}

/// Write half of the relay transport: writes `SignalResponse`s to the
/// node-B→node-A stream.
pub struct RelayIoWriter {
    bus: Arc<dyn ClusterBus>,
    session_id: String,
}

/// Builds a relay signal transport.
pub fn relay_io(bus: Arc<dyn ClusterBus>, session_id: String) -> signal::SignalIo {
    signal::SignalIo::new(
        Box::new(RelayIoReader {
            bus: bus.clone(),
            session_id: session_id.clone(),
            last_id: "$".to_string(),
            pending: VecDeque::new(),
        }),
        Box::new(RelayIoWriter { bus, session_id }),
    )
}

#[async_trait::async_trait]
impl signal::SignalIoReader for RelayIoReader {
    async fn next_request(&mut self) -> Option<lk::SignalRequest> {
        if let Some(req) = self.pending.pop_front() {
            return Some(req);
        }
        loop {
            match self
                .bus
                .xread_block(
                    &relay_in(&self.session_id),
                    &self.last_id,
                    BUS_READ_TIMEOUT_MS,
                )
                .await
            {
                Ok(Some(entries)) => {
                    let mut got_close = false;
                    for (id, fields) in entries {
                        self.last_id = id;
                        let fields: HashMap<String, String> = fields.into_iter().collect();
                        match fields.get("kind").map(String::as_str) {
                            Some("close") => got_close = true,
                            Some("request") => {
                                if let (Some(data), Ok(bytes)) = (
                                    fields.get("data"),
                                    base64::Engine::decode(
                                        &base64::engine::general_purpose::STANDARD,
                                        fields.get("data").map(String::as_str).unwrap_or_default(),
                                    ),
                                ) {
                                    let _ = data;
                                    if let Ok(req) = lk::SignalRequest::decode(bytes.as_slice()) {
                                        self.pending.push_back(req);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if got_close {
                        return None;
                    }
                    if let Some(req) = self.pending.pop_front() {
                        return Some(req);
                    }
                }
                _ => return None,
            }
        }
    }
}

#[async_trait::async_trait]
impl signal::SignalIoWriter for RelayIoWriter {
    async fn send(&mut self, resp: &lk::SignalResponse) -> bool {
        let data = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            resp.encode_to_vec(),
        );
        self.bus
            .xadd(
                &relay_out(&self.session_id),
                vec![
                    ("kind".to_string(), "response".to_string()),
                    ("data".to_string(), data),
                ],
            )
            .await
            .is_ok()
    }

    async fn close(&mut self) {
        let _ = self
            .bus
            .xadd(
                &relay_out(&self.session_id),
                vec![("kind".to_string(), "close".to_string())],
            )
            .await;
    }
}

/// Node A: pipes a client's websocket to/from a relayed session on node B.
pub async fn run_relay_client(
    socket: axum::extract::ws::WebSocket,
    server: &Arc<Server>,
    token: &str,
    params: SessionParams,
    target_node: &str,
) {
    use axum::extract::ws::Message;
    use futures_util::{SinkExt, StreamExt};

    let bus = server.cluster.bus.clone();
    let session_id = format!("c{}", uuid::Uuid::new_v4().simple());
    let (mut sink, mut stream) = socket.split();

    // Tell node B to start the session.
    let start_fields = vec![
        ("kind".to_string(), "start".to_string()),
        ("session_id".to_string(), session_id.clone()),
        ("token".to_string(), token.to_string()),
        (
            "params".to_string(),
            serde_json::to_string(&params).unwrap_or_default(),
        ),
    ];
    if let Err(e) = bus.xadd(&relay_ctrl(target_node), start_fields).await {
        tracing::warn!(target = %target_node, "relay start failed: {e}");
        return;
    }

    // Out consumer: relay responses back to the client. Wire mode (binary vs
    // JSON) is shared with the in-pump so responses match the client.
    let out_bus = bus.clone();
    let out_sid = session_id.clone();
    let mode = Arc::new(tokio::sync::Mutex::new(false)); // false = binary, true = json
    let out_mode = mode.clone();
    let out_sink = tokio::spawn(async move {
        let mut last_id = "0".to_string();
        loop {
            if let Ok(Some(entries)) = out_bus
                .xread_block(&relay_out(&out_sid), &last_id, BUS_READ_TIMEOUT_MS)
                .await
            {
                let mut closed = false;
                for (id, fields) in entries {
                    last_id = id;
                    let fields: HashMap<String, String> = fields.into_iter().collect();
                    match fields.get("kind").map(String::as_str) {
                        Some("close") => {
                            closed = true;
                            let _ = sink
                                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                    code: 1000,
                                    reason: "".into(),
                                })))
                                .await;
                            break;
                        }
                        Some("response") => {
                            let bytes = base64::Engine::decode(
                                &base64::engine::general_purpose::STANDARD,
                                fields.get("data").map(String::as_str).unwrap_or_default(),
                            );
                            if let Ok(bytes) = bytes {
                                if let Ok(resp) = lk::SignalResponse::decode(bytes.as_slice()) {
                                    let msg = signal::ws_encode(&resp, *out_mode.lock().await);
                                    if sink.send(msg).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if closed {
                    return;
                }
            }
        }
    });

    // In pump: relay client requests to node B.
    let read_timeout = std::time::Duration::from_secs(signal::PING_TIMEOUT_SECS as u64);
    loop {
        let frame = match tokio::time::timeout(read_timeout, stream.next()).await {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        };
        let mut json = *mode.lock().await;
        match frame {
            Message::Ping(_) | Message::Pong(_) => continue, // tungstenite auto-answers
            Message::Close(_) => break,
            _ => match signal::ws_decode(frame, &mut json) {
                Some(req) => {
                    *mode.lock().await = json;
                    let data = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        req.encode_to_vec(),
                    );
                    if bus
                        .xadd(
                            &relay_in(&session_id),
                            vec![
                                ("kind".to_string(), "request".to_string()),
                                ("data".to_string(), data),
                            ],
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                None => break,
            },
        }
    }

    // Signal close to node B, then stop the out-consumer (the client is gone;
    // don't wait for a close that may never arrive if node B died).
    let _ = bus
        .xadd(
            &relay_in(&session_id),
            vec![("kind".to_string(), "close".to_string())],
        )
        .await;
    out_sink.abort();
    // Give node B a moment to read the close before we delete the streams.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = bus.del(&relay_in(&session_id)).await;
    let _ = bus.del(&relay_out(&session_id)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bus() -> Arc<MemoryBus> {
        Arc::new(MemoryBus::default())
    }

    #[tokio::test]
    async fn room_routing_claims_to_one_node() {
        let shared = bus();
        let a = Cluster::new_with_bus(shared.clone(), "A", true);
        let b = Cluster::new_with_bus(shared, "B", true);
        // A is registered (alive), so B routes to A.
        a.bus
            .set_with_ttl(&node_key("A"), "alive", 30)
            .await
            .unwrap();
        assert!(matches!(a.route_room("r1").await, Routing::Local));
        assert!(matches!(b.route_room("r1").await, Routing::Remote(n) if n == "A"));
    }

    #[tokio::test]
    async fn dead_owner_is_reclaimed() {
        let shared = bus();
        let a = Cluster::new_with_bus(shared.clone(), "A", true);
        let b = Cluster::new_with_bus(shared, "B", true);
        assert!(matches!(a.route_room("r1").await, Routing::Local));
        // A has no heartbeat registered -> dead -> B reclaims.
        assert!(matches!(b.route_room("r1").await, Routing::Local));
    }

    #[tokio::test]
    async fn disabled_cluster_is_always_local() {
        let a = Cluster::new_with_bus(bus(), "A", false);
        assert!(matches!(a.route_room("r1").await, Routing::Local));
        assert!(matches!(a.route_room("r2").await, Routing::Local));
    }

    #[tokio::test]
    async fn relay_io_round_trips_over_memory_bus() {
        let shared = bus();
        let session = "s1".to_string();
        let in_stream = relay_in(&session);
        let out_stream = relay_out(&session);

        let server_io = relay_io(shared.clone(), session.clone());
        // Node A writes a request.
        shared
            .xadd(
                &in_stream,
                vec![
                    ("kind".to_string(), "request".to_string()),
                    (
                        "data".to_string(),
                        base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            lk::SignalRequest {
                                message: Some(lk::signal_request::Message::Ping(42)),
                            }
                            .encode_to_vec(),
                        ),
                    ),
                ],
            )
            .await
            .unwrap();
        let req = server_io.next_request().await.expect("request");
        assert!(matches!(
            req.message,
            Some(lk::signal_request::Message::Ping(_))
        ));

        // Node B responds.
        let resp = lk::SignalResponse {
            message: Some(lk::signal_response::Message::Pong(1)),
        };
        server_io.send(&resp).await;
        let entries = shared
            .xread_block(&out_stream, "$", 1000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entries.len(), 1);

        // Close sentinel ends the IO.
        shared
            .xadd(&in_stream, vec![("kind".to_string(), "close".to_string())])
            .await
            .unwrap();
        assert!(server_io.next_request().await.is_none());
    }
}
