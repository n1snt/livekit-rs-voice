//! psrpc-compatible RPC client (livekit psrpc v0.7 wire protocol).
//!
//! Reference `livekit-server` and the `livekit/sip` container exchange
//! outbound SIP requests over the psrpc Redis message bus: a client publishes
//! an `internal.Request` envelope to `<service>|<method>|<topic>|REQ`, a
//! bridge (the `livekit/sip` container) bids with an `internal.ClaimRequest`
//! on `<service>|<client_id>|CLAIM`, the client grants it with an
//! `internal.ClaimResponse` on `<service>|<method>|<topic>|RCLAIM`, and the
//! bridge answers with an `internal.Response` on `<service>|<client_id>|RES`.
//!
//! This module implements the client half of that protocol in Rust so that
//! `CreateSIPParticipant` / `TransferSIPParticipant` reach a real
//! `livekit/sip` container over a shared Redis instance. Channel names,
//! envelope type URLs, and message field numbers are wire-compatible with
//! psrpc v0.7.x (see `protos/internal.proto` and `protos/rpc/sip.proto`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use lk_proto::internal;
use lk_proto::rpc;
use prost::Message as _;

use crate::config::Config;

/// Default per-request timeout, matching `livekit-server`'s 30s default.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SIP_SERVICE: &str = "SIPInternal";

#[derive(Debug, thiserror::Error)]
pub enum PsrpcError {
    #[error("psrpc bus: {0}")]
    Bus(String),
    #[error("request timed out")]
    Timeout,
    #[error("response channel closed")]
    ChannelClosed,
    #[error("malformed response: {0}")]
    Malformed(#[from] prost::DecodeError),
    #[error("rpc error ({code}): {message}")]
    Rpc { code: String, message: String },
}

// ---------------------------------------------------------------------------
// Message bus
// ---------------------------------------------------------------------------

/// Publish/subscribe primitives used by the RPC client. Implemented on real
/// Redis and on an in-memory bus (for tests).
#[async_trait::async_trait]
pub trait PsrpcBus: Send + Sync {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<(), String>;
    async fn subscribe(
        &self,
        channels: Vec<String>,
    ) -> Result<BoxStream<'static, (String, Vec<u8>)>, String>;
}

/// Redis Pub/Sub bus. Channel names and payloads match the psrpc Redis
/// message bus exactly (raw protobuf `Msg` envelopes on PubSub channels).
pub struct RedisBus {
    address: String,
    username: String,
    password: String,
    db: i64,
    use_tls: bool,
    conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
}

impl RedisBus {
    pub fn new(config: &Config) -> Self {
        RedisBus {
            address: config.redis.address.clone(),
            username: config.redis.username.clone(),
            password: config.redis.password.clone(),
            db: config.redis.db,
            use_tls: config.redis.use_tls,
            conn: tokio::sync::OnceCell::new(),
        }
    }

    fn client(&self) -> Result<redis::Client, String> {
        let url = format!(
            "{}://{}:{}@{}/{}",
            if self.use_tls { "rediss" } else { "redis" },
            self.username,
            self.password,
            self.address,
            self.db
        );
        redis::Client::open(url).map_err(|e| format!("redis connect: {e}"))
    }

    async fn conn(&self) -> Result<redis::aio::ConnectionManager, String> {
        if let Some(c) = self.conn.get() {
            return Ok(c.clone());
        }
        let manager = self
            .client()?
            .get_connection_manager()
            .await
            .map_err(|e| format!("redis manager: {e}"))?;
        let _ = self.conn.set(manager.clone());
        Ok(manager)
    }
}

#[async_trait::async_trait]
impl PsrpcBus for RedisBus {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<(), String> {
        let mut conn = self.conn().await?;
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(payload)
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("redis publish: {e}"))
    }

    async fn subscribe(
        &self,
        channels: Vec<String>,
    ) -> Result<BoxStream<'static, (String, Vec<u8>)>, String> {
        let client = self.client()?;
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| format!("redis pubsub: {e}"))?;
        for c in &channels {
            pubsub
                .subscribe(c)
                .await
                .map_err(|e| format!("redis subscribe {c}: {e}"))?;
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut stream = pubsub.on_message();
            while let Some(msg) = stream.next().await {
                let channel = msg.get_channel_name().to_string();
                let payload: Vec<u8> = msg.get_payload().unwrap_or_default();
                if tx.send((channel, payload)).is_err() {
                    break;
                }
            }
        });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(stream.boxed())
    }
}

/// In-memory bus for tests and the standalone (no-Redis) dev path.
type Subscriber = tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>;

#[derive(Default)]
struct MemoryInner {
    subs: HashMap<String, Vec<Subscriber>>,
}

#[derive(Default)]
pub struct MemoryBus {
    inner: Arc<Mutex<MemoryInner>>,
}

impl MemoryBus {
    pub fn new() -> Arc<Self> {
        Arc::new(MemoryBus::default())
    }
}

#[async_trait::async_trait]
impl PsrpcBus for MemoryBus {
    async fn publish(&self, channel: &str, payload: Vec<u8>) -> Result<(), String> {
        let subscribers: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .subs
            .get(channel)
            .cloned()
            .unwrap_or_default();
        for sub in subscribers {
            let _ = sub.send((channel.to_string(), payload.clone()));
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        channels: Vec<String>,
    ) -> Result<BoxStream<'static, (String, Vec<u8>)>, String> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut inner = self.inner.lock().unwrap();
        for c in channels {
            inner.subs.entry(c).or_default().push(tx.clone());
        }
        drop(inner);
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(stream.boxed())
    }
}

// ---------------------------------------------------------------------------
// Envelope + channel helpers
// ---------------------------------------------------------------------------

/// Channel-part sanitization: psrpc keeps `[0-9A-Za-z_]` and hex-escapes the
/// rest (`u+XXXX` / `U+XXXXXXXX`), matching `pkg/info/channels.go`.
fn sanitize(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else if (c as u32) < 0x10000 {
            out.push_str(&format!("u+{:04x}", c as u32));
        } else {
            out.push_str(&format!("U+{:08x}", c as u32));
        }
    }
    out
}

fn channel(parts: &[&str]) -> String {
    let sanitized: Vec<String> = parts
        .iter()
        .filter(|p| !p.is_empty())
        .map(|p| sanitize(p))
        .collect();
    sanitized.join("|")
}

pub fn rpc_channel(service: &str, method: &str, topic: &str) -> String {
    channel(&[service, method, topic, "REQ"])
}

pub fn claim_response_channel(service: &str, method: &str, topic: &str) -> String {
    channel(&[service, method, topic, "RCLAIM"])
}

pub fn response_channel(service: &str, client_id: &str) -> String {
    channel(&[service, client_id, "RES"])
}

pub fn claim_request_channel(service: &str, client_id: &str) -> String {
    channel(&[service, client_id, "CLAIM"])
}

/// Serializes a message into the `Msg` envelope that psrpc puts on the wire.
/// The type URL carries the full protobuf name so the Go side can resolve it.
pub fn envelope(type_name: &str, msg: &impl prost::Message) -> Vec<u8> {
    internal::Msg {
        type_url: format!("type.googleapis.com/{type_name}"),
        value: msg.encode_to_vec(),
        channel: String::new(),
    }
    .encode_to_vec()
}

pub fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn new_id(prefix: &str) -> String {
    crate::core::generate_id(prefix)
}

// ---------------------------------------------------------------------------
// SipInternalClient
// ---------------------------------------------------------------------------

type ClaimSender = tokio::sync::mpsc::UnboundedSender<internal::ClaimRequest>;
type ResponseSender = tokio::sync::mpsc::UnboundedSender<internal::Response>;

struct Pending {
    claim: ClaimSender,
    resp: ResponseSender,
}

/// psrpc client for the `livekit.sip` `SIPInternal` service
/// (`CreateSIPParticipant`, `TransferSIPParticipant`). It subscribes once to
/// its response + claim channels and routes incoming messages to in-flight
/// requests by `request_id`.
pub struct SipInternalClient {
    bus: Arc<dyn PsrpcBus>,
    service: String,
    client_id: String,
    pending: Arc<Mutex<HashMap<String, Pending>>>,
    timeout: Duration,
}

impl SipInternalClient {
    pub async fn new(bus: Arc<dyn PsrpcBus>) -> Result<Arc<Self>, String> {
        Self::new_with_timeout(bus, DEFAULT_TIMEOUT).await
    }

    pub async fn new_with_timeout(
        bus: Arc<dyn PsrpcBus>,
        timeout: Duration,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_service(bus, SIP_SERVICE, timeout).await
    }

    /// Builds a client for an arbitrary psrpc service (used for the
    /// `IOInfoSIP` service in tests and tooling).
    pub async fn new_with_service(
        bus: Arc<dyn PsrpcBus>,
        service: &str,
        timeout: Duration,
    ) -> Result<Arc<Self>, String> {
        let client = Arc::new(SipInternalClient {
            bus,
            service: service.to_string(),
            client_id: new_id("CLI_"),
            pending: Arc::new(Mutex::new(HashMap::new())),
            timeout,
        });
        client.spawn_listener().await?;
        Ok(client)
    }

    /// The response channel this client listens on (per-request responses).
    pub fn response_channel(&self) -> String {
        response_channel(&self.service, &self.client_id)
    }

    async fn spawn_listener(self: &Arc<Self>) -> Result<(), String> {
        let channels = vec![
            response_channel(&self.service, &self.client_id),
            claim_request_channel(&self.service, &self.client_id),
        ];
        let mut stream = self.bus.subscribe(channels).await?;
        let client = self.clone();
        tokio::spawn(async move {
            while let Some((_channel, payload)) = stream.next().await {
                client.dispatch(payload);
            }
        });
        Ok(())
    }
    fn dispatch(&self, payload: Vec<u8>) {
        let Ok(envelope) = internal::Msg::decode(payload.as_slice()) else {
            return;
        };
        let pending = self.pending.lock().unwrap();
        if envelope.type_url.ends_with("internal.ClaimRequest") {
            let Ok(claim) = internal::ClaimRequest::decode(envelope.value.as_slice()) else {
                return;
            };
            if let Some(p) = pending.get(&claim.request_id) {
                let _ = p.claim.send(claim);
            }
        } else if envelope.type_url.ends_with("internal.Response") {
            let Ok(resp) = internal::Response::decode(envelope.value.as_slice()) else {
                return;
            };
            if let Some(p) = pending.get(&resp.request_id) {
                let _ = p.resp.send(resp);
            }
        }
    }

    pub async fn create_sip_participant(
        &self,
        req: &rpc::InternalCreateSipParticipantRequest,
    ) -> Result<rpc::InternalCreateSipParticipantResponse, PsrpcError> {
        let payload = req.encode_to_vec();
        let resp = self
            .request_single("CreateSIPParticipant", "", payload, self.timeout)
            .await?;
        rpc::InternalCreateSipParticipantResponse::decode(resp.as_slice())
            .map_err(PsrpcError::Malformed)
    }

    pub async fn transfer_sip_participant(
        &self,
        sip_call_id: &str,
        req: &rpc::InternalTransferSipParticipantRequest,
    ) -> Result<(), PsrpcError> {
        let payload = req.encode_to_vec();
        self.request_single("TransferSIPParticipant", sip_call_id, payload, self.timeout)
            .await?;
        Ok(())
    }

    /// Generic single request against this service: publish `req` for
    /// `method` and return the raw response payload.
    pub async fn request(
        &self,
        method: &str,
        req: &impl prost::Message,
    ) -> Result<Vec<u8>, PsrpcError> {
        let payload = req.encode_to_vec();
        self.request_single(method, "", payload, self.timeout).await
    }

    /// Runs a single queue RPC: publish the request, negotiate the claim, and
    /// wait for the bridge's response until `timeout`.
    async fn request_single(
        &self,
        method: &str,
        topic: &str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>, PsrpcError> {
        let request_id = new_id("REQ_");
        let now = unix_nanos();
        let request = internal::Request {
            request_id: request_id.clone(),
            client_id: self.client_id.clone(),
            sent_at: now,
            expiry: now + timeout.as_nanos() as i64,
            raw_request: payload,
            ..Default::default()
        };

        let (claim_tx, mut claim_rx) = tokio::sync::mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
        self.pending.lock().unwrap().insert(
            request_id.clone(),
            Pending {
                claim: claim_tx,
                resp: resp_tx,
            },
        );

        let result = tokio::time::timeout(
            timeout,
            self.request_single_inner(&request, method, topic, &mut claim_rx, &mut resp_rx),
        )
        .await
        .map_err(|_| PsrpcError::Timeout)?;

        self.pending.lock().unwrap().remove(&request_id);
        result
    }

    async fn request_single_inner(
        &self,
        request: &internal::Request,
        method: &str,
        topic: &str,
        claim_rx: &mut tokio::sync::mpsc::UnboundedReceiver<internal::ClaimRequest>,
        resp_rx: &mut tokio::sync::mpsc::UnboundedReceiver<internal::Response>,
    ) -> Result<Vec<u8>, PsrpcError> {
        let req_channel = rpc_channel(&self.service, method, topic);
        self.bus
            .publish(&req_channel, envelope("internal.Request", request))
            .await
            .map_err(PsrpcError::Bus)?;

        let rclaim_channel = claim_response_channel(&self.service, method, topic);
        loop {
            tokio::select! {
                biased;
                resp = resp_rx.recv() => {
                    match resp {
                        Some(resp) => return self.response_payload(resp),
                        None => return Err(PsrpcError::ChannelClosed),
                    }
                }
                claim = claim_rx.recv() => {
                    match claim {
                        Some(claim) => {
                            let grant = internal::ClaimResponse {
                                request_id: claim.request_id,
                                server_id: claim.server_id,
                            };
                            self.bus
                                .publish(&rclaim_channel, envelope("internal.ClaimResponse", &grant))
                                .await
                                .map_err(PsrpcError::Bus)?;
                        }
                        None => return Err(PsrpcError::ChannelClosed),
                    }
                }
            }
        }
    }

    fn response_payload(&self, resp: internal::Response) -> Result<Vec<u8>, PsrpcError> {
        if !resp.error.is_empty() {
            return Err(PsrpcError::Rpc {
                code: resp.code,
                message: resp.error,
            });
        }
        Ok(resp.raw_response)
    }
}

// ---------------------------------------------------------------------------
// SipIoServer (psrpc server hosting the `IOInfoSIP` service)
// ---------------------------------------------------------------------------

/// Handler for one `IOInfoSIP` method: takes the raw request payload and
/// returns the raw response payload (or an error string that becomes the
/// RPC error).
#[async_trait::async_trait]
pub trait IoHandler: Send + Sync {
    async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String>;
}

/// psrpc server that hosts the `livekit.sip` `IOInfoSIP` service. The
/// `livekit/sip` container is a psrpc *client* of this service for inbound
/// trunk authentication, dispatch-rule evaluation, and call-state recording.
///
/// For each registered method it subscribes to the method's RPC channel,
/// bids on the caller's CLAIM channel, waits for the grant on RCLAIM, and
/// answers on the caller's RES channel — mirroring the psrpc v0.7 server
/// half.
pub struct SipIoServer {
    bus: Arc<dyn PsrpcBus>,
    service: String,
    server_id: String,
}

impl SipIoServer {
    pub async fn new(bus: Arc<dyn PsrpcBus>) -> Result<Arc<Self>, String> {
        Ok(Arc::new(SipIoServer {
            bus,
            service: "IOInfoSIP".to_string(),
            server_id: new_id("SRV_"),
        }))
    }

    /// Starts listening for `method` and dispatches requests to `handler`.
    pub async fn register(
        self: &Arc<Self>,
        method: &str,
        handler: Arc<dyn IoHandler>,
    ) -> Result<(), String> {
        let rpc_ch = rpc_channel(&self.service, method, "");
        let rclaim_ch = claim_response_channel(&self.service, method, "");
        let mut stream = self
            .bus
            .subscribe(vec![rpc_ch.clone(), rclaim_ch.clone()])
            .await?;
        let bus = self.bus.clone();
        let service = self.service.clone();
        let server_id = self.server_id.clone();
        let method = method.to_string();
        let handler = handler.clone();
        tokio::spawn(async move {
            // request_id -> (client_id, raw_request, expiry)
            let mut pending: HashMap<String, (String, Vec<u8>, i64)> = HashMap::new();
            while let Some((channel, payload)) = stream.next().await {
                let Ok(env) = internal::Msg::decode(payload.as_slice()) else {
                    continue;
                };
                if channel == rpc_ch {
                    let Ok(req) = internal::Request::decode(env.value.as_slice()) else {
                        continue;
                    };
                    let now = unix_nanos();
                    if req.expiry <= now {
                        continue;
                    }
                    // Drop requests that were never granted past their expiry.
                    pending.retain(|_, (_, _, exp)| *exp > now);
                    let client_id = req.client_id.clone();
                    let raw = req.raw_request.clone();
                    pending.insert(req.request_id.clone(), (client_id.clone(), raw, req.expiry));
                    let claim = internal::ClaimRequest {
                        request_id: req.request_id.clone(),
                        server_id: server_id.clone(),
                        affinity: 1.0,
                        handling: false,
                    };
                    let _ = bus
                        .publish(
                            &claim_request_channel(&service, &client_id),
                            envelope("internal.ClaimRequest", &claim),
                        )
                        .await;
                } else if channel == rclaim_ch {
                    let Ok(grant) = internal::ClaimResponse::decode(env.value.as_slice()) else {
                        continue;
                    };
                    if grant.server_id != server_id {
                        continue;
                    }
                    let Some((client_id, raw, _)) = pending.remove(&grant.request_id) else {
                        continue;
                    };
                    let bus = bus.clone();
                    let service = service.clone();
                    let server_id = server_id.clone();
                    let handler = handler.clone();
                    let method = method.clone();
                    tokio::spawn(async move {
                        let mut resp = internal::Response {
                            request_id: grant.request_id,
                            server_id,
                            sent_at: unix_nanos(),
                            ..Default::default()
                        };
                        match handler.handle(&method, raw).await {
                            Ok(bytes) => resp.raw_response = bytes,
                            Err(e) => {
                                resp.error = e;
                                resp.code = "internal".to_string();
                            }
                        }
                        let _ = bus
                            .publish(
                                &response_channel(&service, &client_id),
                                envelope("internal.Response", &resp),
                            )
                            .await;
                    });
                }
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lk_proto::rpc;

    type Received = Arc<Mutex<Vec<Vec<u8>>>>;

    /// A minimal `livekit/sip` bridge that speaks the psrpc v0.7 claim flow:
    /// receives a request on the RPC channel, bids on the client's CLAIM
    /// channel, waits for the grant on RCLAIM, then answers on the client's
    /// RES channel. Records every request payload it received.
    async fn spawn_mock_bridge(
        bus: Arc<MemoryBus>,
        service: &str,
        method: &str,
        topic: &str,
        error: Option<&'static str>,
    ) -> Received {
        let rpc_ch = rpc_channel(service, method, topic);
        let rclaim_ch = claim_response_channel(service, method, topic);
        let mut stream = bus
            .subscribe(vec![rpc_ch.clone(), rclaim_ch.clone()])
            .await
            .unwrap();
        let received: Received = Arc::new(Mutex::new(Vec::new()));
        let recv2 = received.clone();
        let bus2 = bus.clone();
        let service = service.to_string();
        tokio::spawn(async move {
            // request_id -> (client_id, raw_request)
            let mut pending: HashMap<String, (String, Vec<u8>)> = HashMap::new();
            while let Some((channel, payload)) = stream.next().await {
                let Ok(env) = internal::Msg::decode(payload.as_slice()) else {
                    continue;
                };
                if channel == rpc_ch {
                    let Ok(req) = internal::Request::decode(env.value.as_slice()) else {
                        continue;
                    };
                    let raw = req.raw_request.clone();
                    recv2.lock().unwrap().push(raw.clone());
                    pending.insert(req.request_id.clone(), (req.client_id.clone(), raw));
                    let claim = internal::ClaimRequest {
                        request_id: req.request_id.clone(),
                        server_id: "SRV_mock".to_string(),
                        affinity: 1.0,
                        handling: false,
                    };
                    let _ = bus2
                        .publish(
                            &claim_request_channel(&service, &req.client_id),
                            envelope("internal.ClaimRequest", &claim),
                        )
                        .await;
                } else if channel == rclaim_ch {
                    let Ok(grant) = internal::ClaimResponse::decode(env.value.as_slice()) else {
                        continue;
                    };
                    let Some((client_id, raw)) = pending.remove(&grant.request_id) else {
                        continue;
                    };
                    let mut resp = internal::Response {
                        request_id: grant.request_id.clone(),
                        server_id: "SRV_mock".to_string(),
                        sent_at: unix_nanos(),
                        ..Default::default()
                    };
                    if let Some(err) = error {
                        resp.error = err.to_string();
                        resp.code = "failed_precondition".to_string();
                    } else if let Ok(ireq) =
                        rpc::InternalCreateSipParticipantRequest::decode(raw.as_slice())
                    {
                        resp.raw_response = rpc::InternalCreateSipParticipantResponse {
                            participant_id: format!("PA_mock_{}", grant.request_id),
                            participant_identity: ireq.participant_identity,
                            sip_call_id: ireq.sip_call_id,
                        }
                        .encode_to_vec();
                    }
                    let _ = bus2
                        .publish(
                            &response_channel(&service, &client_id),
                            envelope("internal.Response", &resp),
                        )
                        .await;
                }
            }
        });
        received
    }

    fn create_request() -> rpc::InternalCreateSipParticipantRequest {
        rpc::InternalCreateSipParticipantRequest {
            sip_call_id: "SC_1".to_string(),
            number: "+1555".to_string(),
            call_to: "+1777".to_string(),
            room_name: "room-a".to_string(),
            participant_identity: "sip_+1777".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn claim_flow_round_trips() {
        let bus = MemoryBus::new();
        let received =
            spawn_mock_bridge(bus.clone(), SIP_SERVICE, "CreateSIPParticipant", "", None).await;
        let client = SipInternalClient::new(bus).await.unwrap();

        let resp = client
            .create_sip_participant(&create_request())
            .await
            .unwrap();
        assert!(resp.participant_id.starts_with("PA_mock_"));
        assert_eq!(resp.sip_call_id, "SC_1");

        let got = received.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        let ireq = rpc::InternalCreateSipParticipantRequest::decode(got[0].as_slice()).unwrap();
        assert_eq!(ireq.room_name, "room-a");
        assert_eq!(ireq.call_to, "+1777");
        assert_eq!(ireq.number, "+1555");
    }

    #[tokio::test]
    async fn concurrent_requests_all_answered() {
        let bus = MemoryBus::new();
        let _received =
            spawn_mock_bridge(bus.clone(), SIP_SERVICE, "CreateSIPParticipant", "", None).await;
        let client = SipInternalClient::new(bus).await.unwrap();

        let mut handles = Vec::new();
        for i in 0..5 {
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let mut req = create_request();
                req.sip_call_id = format!("SC_{i}");
                client.create_sip_participant(&req).await.unwrap()
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            let resp = h.await.unwrap();
            assert_eq!(resp.sip_call_id, format!("SC_{i}"));
        }
    }

    #[tokio::test]
    async fn transfer_uses_call_id_topic() {
        let bus = MemoryBus::new();
        let received = spawn_mock_bridge(
            bus.clone(),
            SIP_SERVICE,
            "TransferSIPParticipant",
            "SC_abc",
            None,
        )
        .await;
        let client = SipInternalClient::new(bus).await.unwrap();

        let req = rpc::InternalTransferSipParticipantRequest {
            sip_call_id: "SC_abc".to_string(),
            transfer_to: "+1999".to_string(),
            ..Default::default()
        };
        client
            .transfer_sip_participant("SC_abc", &req)
            .await
            .unwrap();

        let got = received.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        let ireq = rpc::InternalTransferSipParticipantRequest::decode(got[0].as_slice()).unwrap();
        assert_eq!(ireq.transfer_to, "+1999");
    }

    #[tokio::test]
    async fn bridge_error_propagates() {
        let bus = MemoryBus::new();
        let _received = spawn_mock_bridge(
            bus.clone(),
            SIP_SERVICE,
            "CreateSIPParticipant",
            "",
            Some("boom"),
        )
        .await;
        let client = SipInternalClient::new(bus).await.unwrap();

        let err = client
            .create_sip_participant(&create_request())
            .await
            .unwrap_err();
        match err {
            PsrpcError::Rpc { code, message } => {
                assert_eq!(code, "failed_precondition");
                assert_eq!(message, "boom");
            }
            other => panic!("expected Rpc error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_bridge_times_out() {
        let bus = MemoryBus::new();
        let client = SipInternalClient::new_with_timeout(bus, Duration::from_millis(200))
            .await
            .unwrap();
        let err = client
            .create_sip_participant(&create_request())
            .await
            .unwrap_err();
        assert!(matches!(err, PsrpcError::Timeout));
    }

    struct EchoHandler;

    #[async_trait::async_trait]
    impl IoHandler for EchoHandler {
        async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String> {
            let req = internal::Request::decode(raw.as_slice()).map_err(|e| e.to_string())?;
            let mut resp = internal::Request {
                request_id: req.request_id,
                ..Default::default()
            };
            resp.metadata.insert(method.to_string(), "echo".to_string());
            Ok(resp.encode_to_vec())
        }
    }

    #[tokio::test]
    async fn io_server_serves_claim_flow() {
        let bus = MemoryBus::new();
        let io = SipIoServer::new(bus.clone()).await.unwrap();
        io.register("GetSIPTrunkAuthentication", Arc::new(EchoHandler))
            .await
            .unwrap();

        let client = SipInternalClient::new_with_service(bus, "IOInfoSIP", Duration::from_secs(2))
            .await
            .unwrap();
        let req = internal::Request {
            request_id: "REQ_test".to_string(),
            ..Default::default()
        };
        let resp = client
            .request("GetSIPTrunkAuthentication", &req)
            .await
            .unwrap();
        let decoded = internal::Request::decode(resp.as_slice()).unwrap();
        assert_eq!(
            decoded
                .metadata
                .get("GetSIPTrunkAuthentication")
                .map(String::as_str),
            Some("echo")
        );
    }
}
