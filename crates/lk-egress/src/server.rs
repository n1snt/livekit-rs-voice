//! psrpc `EgressInternal` server: receives `StartEgress` / `ListActiveEgress`
//! from the livekit-voice server and runs voice recordings.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use lk_proto::livekit as lk;
use lk_proto::rpc;
use lk_psrpc::{IoHandler, PsrpcBus, PsrpcServer};
use prost::Message as _;

use crate::client;
use crate::config::EgressConfig;
use crate::io::IoClient;
use crate::recorder::{self, OutputFormat};

/// The recorder instance: hosts the `EgressInternal` service and tracks active
/// recordings.
pub struct EgressServer {
    server: Arc<PsrpcServer>,
}

impl EgressServer {
    pub async fn new(
        bus: Arc<dyn PsrpcBus>,
        conf: EgressConfig,
        io: Arc<IoClient>,
    ) -> Result<Arc<Self>, String> {
        let server = PsrpcServer::new(bus, "EgressInternal").await?;
        let handlers = Arc::new(Handlers {
            conf,
            io,
            active: Arc::new(Mutex::new(HashSet::new())),
        });
        let svc = Arc::new(EgressServer { server });
        svc.server.register("StartEgress", handlers.clone()).await?;
        svc.server.register("ListActiveEgress", handlers).await?;
        Ok(svc)
    }
}

struct Handlers {
    conf: EgressConfig,
    io: Arc<IoClient>,
    active: Arc<Mutex<HashSet<String>>>,
}

/// The room + output format for a recording request.
fn room_and_format(req: &rpc::StartEgressRequest) -> Result<(String, OutputFormat), String> {
    let some = |m: &[lk::EncodedFileOutput]| -> OutputFormat {
        m.first()
            .map(|f| encoded_format(f.file_type))
            .unwrap_or(OutputFormat::Wav)
    };
    match &req.request {
        Some(rpc::start_egress_request::Request::RoomComposite(r)) => Ok((
            r.room_name.clone(),
            match &r.output {
                Some(lk::room_composite_egress_request::Output::File(f)) => {
                    encoded_format(f.file_type)
                }
                _ => some(&r.file_outputs),
            },
        )),
        Some(rpc::start_egress_request::Request::Track(r)) => {
            Ok((r.room_name.clone(), OutputFormat::Wav))
        }
        Some(rpc::start_egress_request::Request::Participant(r)) => Ok((
            r.room_name.clone(),
            r.file_outputs
                .first()
                .map(|f| encoded_format(f.file_type))
                .unwrap_or(OutputFormat::Wav),
        )),
        Some(rpc::start_egress_request::Request::TrackComposite(r)) => Ok((
            r.room_name.clone(),
            match &r.output {
                Some(lk::track_composite_egress_request::Output::File(f)) => {
                    encoded_format(f.file_type)
                }
                _ => OutputFormat::Wav,
            },
        )),
        Some(rpc::start_egress_request::Request::Egress(r)) => {
            Ok((r.room_name.clone(), OutputFormat::Wav))
        }
        _ => Err("web/replay egress is not supported on the voice-only recorder".to_string()),
    }
}

/// Maps an `EncodedFileType` to an output format. Voice-only: MP3 stays MP3,
/// everything else (default/MP4/OGG) records raw PCM as WAV.
fn encoded_format(file_type: i32) -> OutputFormat {
    if file_type == lk::EncodedFileType::Mp3 as i32 {
        OutputFormat::Mp3
    } else {
        OutputFormat::Wav
    }
}

fn request_info(req: &rpc::StartEgressRequest) -> Option<lk::egress_info::Request> {
    use lk::egress_info::Request;
    use rpc::start_egress_request::Request as SR;
    match &req.request {
        Some(SR::RoomComposite(r)) => Some(Request::RoomComposite(r.clone())),
        Some(SR::Track(r)) => Some(Request::Track(r.clone())),
        Some(SR::Participant(r)) => Some(Request::Participant(r.clone())),
        Some(SR::TrackComposite(r)) => Some(Request::TrackComposite(r.clone())),
        Some(SR::Egress(r)) => Some(Request::Egress(r.clone())),
        _ => None,
    }
}

#[async_trait::async_trait]
impl IoHandler for Handlers {
    async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String> {
        match method {
            "StartEgress" => {
                let req =
                    rpc::StartEgressRequest::decode(raw.as_slice()).map_err(|e| e.to_string())?;
                let egress_id = req.egress_id.clone();
                if egress_id.is_empty() {
                    return Err("egress_id is required".to_string());
                }
                let (room, format) = room_and_format(&req)?;
                if room.is_empty() {
                    return Err("room_name is required".to_string());
                }
                let request = request_info(&req).ok_or("unsupported egress request")?;

                let starting = lk::EgressInfo {
                    egress_id: egress_id.clone(),
                    room_name: room.clone(),
                    status: lk::EgressStatus::EgressStarting as i32,
                    started_at: crate::now_secs(),
                    updated_at: crate::now_secs(),
                    request: Some(request),
                    ..Default::default()
                };
                let _ = self.io.create_egress(&starting).await;

                let conf = self.conf.clone();
                let io = self.io.clone();
                let active = self.active.clone();
                self.active.lock().unwrap().insert(egress_id.clone());
                tokio::spawn(async move {
                    let _ = run_one(&conf, &io, &active, &egress_id, &room, format).await;
                });

                Ok(starting.encode_to_vec())
            }
            "ListActiveEgress" => {
                let ids: Vec<String> = self.active.lock().unwrap().iter().cloned().collect();
                Ok(rpc::ListActiveEgressResponse { egress_ids: ids }.encode_to_vec())
            }
            _ => Err(format!("unknown EgressInternal method: {method}")),
        }
    }
}

/// Records one room's audio to `output_dir/{egress_id}.{ext}` and reports the
/// final state back to the server.
async fn run_one(
    conf: &EgressConfig,
    io: &Arc<IoClient>,
    active: &Arc<Mutex<HashSet<String>>>,
    egress_id: &str,
    room: &str,
    format: OutputFormat,
) -> Result<(), String> {
    let ext = if format == OutputFormat::Mp3 {
        "mp3"
    } else {
        "wav"
    };
    let path = format!("{}/{egress_id}.{ext}", conf.output_dir);
    let audio = client::connect(
        &conf.api_key,
        &conf.api_secret,
        &conf.ws_url,
        room,
        &format!("egress_{egress_id}"),
    )
    .await?;
    let _ = recorder::run_recording(audio, &path, format, conf.mp3_bitrate).await?;
    let request = lk::egress_info::Request::RoomComposite(lk::RoomCompositeEgressRequest {
        room_name: room.to_string(),
        ..Default::default()
    });
    let info = recorder::finished_info(egress_id, room, &path, request);
    let _ = io.update_egress(&info).await;
    active.lock().unwrap().remove(egress_id);
    tracing::info!(egress_id, room, path, "recording finished");
    Ok(())
}
