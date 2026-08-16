//! psrpc egress services: `EgressInternal` (StartEgress / ListActiveEgress)
//! and `EgressHandler` (StopEgress, per-egress topic), receiving jobs from the
//! livekit-voice server and running voice recordings.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use lk_proto::livekit as lk;
use lk_proto::rpc;
use lk_psrpc::{IoHandler, PsrpcBus, PsrpcServer};
use prost::Message as _;
use tokio::sync::watch;

use crate::client;
use crate::config::EgressConfig;
use crate::io::IoClient;
use crate::recorder::{self, OutputFormat};

type Stops = Arc<Mutex<HashMap<String, watch::Sender<bool>>>>;
type Infos = Arc<Mutex<HashMap<String, lk::EgressInfo>>>;

/// The recorder instance: hosts the `EgressInternal` + `EgressHandler`
/// services and tracks active recordings.
pub struct EgressServer {
    _internal: Arc<PsrpcServer>,
    _handler: Arc<PsrpcServer>,
}

impl EgressServer {
    pub async fn new(
        bus: Arc<dyn PsrpcBus>,
        conf: EgressConfig,
        io: Arc<IoClient>,
    ) -> Result<Arc<Self>, String> {
        let stops: Stops = Arc::new(Mutex::new(HashMap::new()));
        let infos: Infos = Arc::new(Mutex::new(HashMap::new()));
        let handler = PsrpcServer::new(bus.clone(), "EgressHandler").await?;
        let handlers = Arc::new(Handlers {
            conf,
            io,
            active: Arc::new(Mutex::new(HashSet::new())),
            stops: stops.clone(),
            infos: infos.clone(),
            handler: handler.clone(),
            stop_tasks: Arc::new(Mutex::new(HashMap::new())),
        });
        let internal = PsrpcServer::new(bus, "EgressInternal").await?;
        internal.register("StartEgress", handlers.clone()).await?;
        internal.register("ListActiveEgress", handlers).await?;
        let _ = stops;
        let _ = infos;
        Ok(Arc::new(EgressServer {
            _internal: internal,
            _handler: handler,
        }))
    }
}

/// Shared context passed to a recording job.
struct JobCtx {
    io: Arc<IoClient>,
    active: Arc<Mutex<HashSet<String>>>,
    infos: Infos,
}

struct Handlers {
    conf: EgressConfig,
    io: Arc<IoClient>,
    active: Arc<Mutex<HashSet<String>>>,
    stops: Stops,
    infos: Infos,
    handler: Arc<PsrpcServer>,
    stop_tasks: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
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

/// Per-egress `StopEgress` handler (topic = egress id): signals the recording
/// to stop and returns the current info.
struct StopHandler {
    stops: Stops,
    infos: Infos,
}

#[async_trait::async_trait]
impl IoHandler for StopHandler {
    async fn handle(&self, _method: &str, raw: Vec<u8>) -> Result<Vec<u8>, String> {
        let req = lk::StopEgressRequest::decode(raw.as_slice()).map_err(|e| e.to_string())?;
        let egress_id = req.egress_id.clone();
        if let Some(tx) = self.stops.lock().unwrap().get(&egress_id) {
            let _ = tx.send(true);
        }
        self.infos
            .lock()
            .unwrap()
            .get(&egress_id)
            .cloned()
            .map(|i| i.encode_to_vec())
            .ok_or_else(|| format!("egress {egress_id} not found"))
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
                self.infos
                    .lock()
                    .unwrap()
                    .insert(egress_id.clone(), starting.clone());

                let (stop_tx, stop_rx) = watch::channel(false);
                self.stops
                    .lock()
                    .unwrap()
                    .insert(egress_id.clone(), stop_tx);
                self.active.lock().unwrap().insert(egress_id.clone());

                // Register the per-egress StopEgress topic.
                let stop_handler = Arc::new(StopHandler {
                    stops: self.stops.clone(),
                    infos: self.infos.clone(),
                });
                let stop_task = self
                    .handler
                    .register_topic("StopEgress", &egress_id, stop_handler)
                    .await?;
                self.stop_tasks
                    .lock()
                    .unwrap()
                    .insert(egress_id.clone(), stop_task);

                let conf = self.conf.clone();
                let ctx = JobCtx {
                    io: self.io.clone(),
                    active: self.active.clone(),
                    infos: self.infos.clone(),
                };
                let stop_tasks = self.stop_tasks.clone();
                tokio::spawn(async move {
                    if let Err(e) = run_one(&conf, &ctx, &egress_id, &room, format, stop_rx).await {
                        tracing::warn!(egress_id, "recording failed: {e}");
                    }
                    if let Some(task) = stop_tasks.lock().unwrap().remove(&egress_id) {
                        task.abort();
                    }
                });
                Ok(starting.encode_to_vec())
            }
            "ListActiveEgress" => {
                let ids: Vec<String> = self.active.lock().unwrap().iter().cloned().collect();
                Ok(rpc::ListActiveEgressResponse { egress_ids: ids }.encode_to_vec())
            }
            _ => Err(format!("unknown egress method: {method}")),
        }
    }
}

/// Records one room's audio to `output_dir/{egress_id}.{ext}`, stopping on
/// `StopEgress` or when the room's audio stream ends.
async fn run_one(
    conf: &EgressConfig,
    ctx: &JobCtx,
    egress_id: &str,
    room: &str,
    format: OutputFormat,
    stop_rx: watch::Receiver<bool>,
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
    let frames =
        recorder::run_recording(audio, &path, format, conf.mp3_bitrate, stop_rx.clone()).await?;
    let request = lk::egress_info::Request::RoomComposite(lk::RoomCompositeEgressRequest {
        room_name: room.to_string(),
        ..Default::default()
    });
    let info = recorder::finished_info(egress_id, room, &path, request, frames);
    let _ = ctx.io.update_egress(&info).await;
    ctx.infos
        .lock()
        .unwrap()
        .insert(egress_id.to_string(), info.clone());
    ctx.active.lock().unwrap().remove(egress_id);
    tracing::info!(egress_id, room, path, frames, "recording finished");
    Ok(())
}
