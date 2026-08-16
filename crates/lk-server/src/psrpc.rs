//! psrpc wire protocol glue for the livekit-voice server.
//!
//! The generic bus/envelope/client/server live in the shared `lk-psrpc`
//! crate; this module re-exports them and adds the SIP-specific typed client
//! and the `IOInfoSIP` server wrapper.

pub use lk_psrpc::{
    claim_request_channel, claim_response_channel, envelope, response_channel, rpc_channel,
    unix_nanos, IoHandler, MemoryBus, PsrpcBus, PsrpcClient, PsrpcError, PsrpcServer, RedisBus,
    RedisConfig,
};

use std::sync::Arc;
use std::time::Duration;

use lk_proto::rpc;
use prost::Message as _;

use crate::config::Config;

const SIP_SERVICE: &str = "SIPInternal";
const IOINFO_SIP_SERVICE: &str = "IOInfoSIP";

/// Converts the server config's Redis settings into the self-contained
/// `lk-psrpc` connection config.
pub fn redis_config(config: &Config) -> RedisConfig {
    RedisConfig {
        address: config.redis.address.clone(),
        username: config.redis.username.clone(),
        password: config.redis.password.clone(),
        db: config.redis.db,
        use_tls: config.redis.use_tls,
    }
}

// ---------------------------------------------------------------------------
// SipInternalClient (outbound SIP bridge)
// ---------------------------------------------------------------------------

/// psrpc client for the `livekit.sip` `SIPInternal` service
/// (`CreateSIPParticipant`, `TransferSIPParticipant`), which reaches a real
/// `livekit/sip` container.
pub struct SipInternalClient {
    inner: Arc<PsrpcClient>,
}

impl SipInternalClient {
    pub async fn new(bus: Arc<dyn PsrpcBus>) -> Result<Arc<Self>, String> {
        Ok(Arc::new(SipInternalClient {
            inner: PsrpcClient::new(bus, SIP_SERVICE).await?,
        }))
    }

    pub async fn new_with_timeout(
        bus: Arc<dyn PsrpcBus>,
        timeout: Duration,
    ) -> Result<Arc<Self>, String> {
        Ok(Arc::new(SipInternalClient {
            inner: PsrpcClient::new_with_timeout(bus, SIP_SERVICE, timeout).await?,
        }))
    }

    /// Builds a client for an arbitrary service over the given bus (tests and
    /// tooling; e.g. an `IOInfoSIP` client acting as the `livekit/sip`
    /// container).
    pub async fn new_with_service(
        bus: Arc<dyn PsrpcBus>,
        service: &str,
        timeout: Duration,
    ) -> Result<Arc<Self>, String> {
        Ok(Arc::new(SipInternalClient {
            inner: PsrpcClient::new_with_timeout(bus, service, timeout).await?,
        }))
    }

    /// The response channel this client listens on (per-request responses).
    pub fn response_channel(&self) -> String {
        self.inner.response_channel()
    }

    pub async fn create_sip_participant(
        &self,
        req: &rpc::InternalCreateSipParticipantRequest,
    ) -> Result<rpc::InternalCreateSipParticipantResponse, PsrpcError> {
        let raw = self.inner.request("CreateSIPParticipant", "", req).await?;
        rpc::InternalCreateSipParticipantResponse::decode(raw.as_slice())
            .map_err(PsrpcError::Malformed)
    }

    pub async fn transfer_sip_participant(
        &self,
        sip_call_id: &str,
        req: &rpc::InternalTransferSipParticipantRequest,
    ) -> Result<(), PsrpcError> {
        self.inner
            .request("TransferSIPParticipant", sip_call_id, req)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SipIoServer (IOInfoSIP service host)
// ---------------------------------------------------------------------------

/// psrpc server hosting the `IOInfoSIP` service (inbound trunk auth +
/// dispatch + call state), used by the `livekit/sip` container.
pub struct SipIoServer {
    inner: Arc<PsrpcServer>,
}

impl SipIoServer {
    pub async fn new(bus: Arc<dyn PsrpcBus>) -> Result<Arc<Self>, String> {
        Ok(Arc::new(SipIoServer {
            inner: PsrpcServer::new(bus, IOINFO_SIP_SERVICE).await?,
        }))
    }

    pub async fn register(&self, method: &str, handler: Arc<dyn IoHandler>) -> Result<(), String> {
        self.inner.register(method, handler).await
    }
}
