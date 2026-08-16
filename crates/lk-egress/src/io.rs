//! psrpc client for the server's `IOInfo` service: reports egress state back
//! (`CreateEgress`, `UpdateEgress`), mirroring the Go egress's
//! `SessionReporter`.

use std::sync::Arc;

use lk_proto::livekit as lk;
use lk_psrpc::{PsrpcBus, PsrpcClient, PsrpcError};

pub struct IoClient {
    client: Arc<PsrpcClient>,
}

impl IoClient {
    pub async fn new(bus: Arc<dyn PsrpcBus>) -> Result<Arc<Self>, String> {
        Ok(Arc::new(IoClient {
            client: PsrpcClient::new(bus, "IOInfo").await?,
        }))
    }

    pub async fn create_egress(&self, info: &lk::EgressInfo) -> Result<(), PsrpcError> {
        self.client.request("CreateEgress", "", info).await?;
        Ok(())
    }

    pub async fn update_egress(&self, info: &lk::EgressInfo) -> Result<(), PsrpcError> {
        self.client.request("UpdateEgress", "", info).await?;
        Ok(())
    }
}
