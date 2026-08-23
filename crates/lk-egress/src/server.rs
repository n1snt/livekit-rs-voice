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
use crate::upload::{self, Destination, S3Target};

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

/// The room, output format, and upload destination for a recording request.
struct RecSpec {
    room: String,
    format: OutputFormat,
    destination: Destination,
    filepath: String,
}

fn rec_spec(req: &rpc::StartEgressRequest, conf: &EgressConfig) -> Result<RecSpec, String> {
    let default = |conf: &EgressConfig| -> Result<(OutputFormat, Destination, String), String> {
        Ok((OutputFormat::Wav, conf_default(conf)?, String::new()))
    };
    match &req.request {
        Some(rpc::start_egress_request::Request::RoomComposite(r)) => {
            let file = match &r.output {
                Some(lk::room_composite_egress_request::Output::File(f)) => Some(f),
                _ => r.file_outputs.first(),
            };
            let (format, destination, filepath) = match file {
                Some(f) => encoded_spec(f, conf)?,
                None => default(conf)?,
            };
            Ok(RecSpec {
                room: r.room_name.clone(),
                format,
                destination,
                filepath,
            })
        }
        Some(rpc::start_egress_request::Request::Track(r)) => match &r.output {
            Some(lk::track_egress_request::Output::File(f)) => Ok(RecSpec {
                room: r.room_name.clone(),
                format: OutputFormat::Wav,
                destination: direct_destination(f, conf)?,
                filepath: f.filepath.clone(),
            }),
            _ => {
                let (format, destination, filepath) = default(conf)?;
                Ok(RecSpec {
                    room: r.room_name.clone(),
                    format,
                    destination,
                    filepath,
                })
            }
        },
        Some(rpc::start_egress_request::Request::Participant(r)) => {
            let (format, destination, filepath) = match r.file_outputs.first() {
                Some(f) => encoded_spec(f, conf)?,
                None => default(conf)?,
            };
            Ok(RecSpec {
                room: r.room_name.clone(),
                format,
                destination,
                filepath,
            })
        }
        Some(rpc::start_egress_request::Request::TrackComposite(r)) => {
            let file = match &r.output {
                Some(lk::track_composite_egress_request::Output::File(f)) => Some(f),
                _ => r.file_outputs.first(),
            };
            let (format, destination, filepath) = match file {
                Some(f) => encoded_spec(f, conf)?,
                None => default(conf)?,
            };
            Ok(RecSpec {
                room: r.room_name.clone(),
                format,
                destination,
                filepath,
            })
        }
        Some(rpc::start_egress_request::Request::Egress(r)) => {
            let (format, destination, filepath) = match r.outputs.first() {
                Some(o) => {
                    // Request-level storage config wins; else the container
                    // `s3:` default; else local.
                    let destination = match &o.storage {
                        Some(s) => storage_destination(s, conf)?,
                        None => conf_default(conf)?,
                    };
                    match &o.config {
                        Some(lk::output::Config::File(f)) => {
                            (encoded_format(f.file_type), destination, f.filepath.clone())
                        }
                        _ => (OutputFormat::Wav, destination, String::new()),
                    }
                }
                None => default(conf)?,
            };
            Ok(RecSpec {
                room: r.room_name.clone(),
                format,
                destination,
                filepath,
            })
        }
        _ => Err("web/replay egress is not supported on the voice-only recorder".to_string()),
    }
}

/// Maps an `EncodedFileOutput` to (format, upload destination, storage key).
/// Request-level S3 upload config wins; otherwise the container `s3:` default;
/// otherwise local.
fn encoded_spec(
    f: &lk::EncodedFileOutput,
    conf: &EgressConfig,
) -> Result<(OutputFormat, Destination, String), String> {
    let format = encoded_format(f.file_type);
    let destination = match &f.output {
        Some(lk::encoded_file_output::Output::S3(s3)) => Ok(s3_destination(s3)),
        Some(_) => Err(unsupported_upload()),
        None => conf_default(conf),
    }?;
    Ok((format, destination, f.filepath.clone()))
}

/// Upload destination from a track-egress `DirectFileOutput`.
fn direct_destination(
    f: &lk::DirectFileOutput,
    conf: &EgressConfig,
) -> Result<Destination, String> {
    match &f.output {
        Some(lk::direct_file_output::Output::S3(s3)) => Ok(s3_destination(s3)),
        Some(_) => Err(unsupported_upload()),
        None => conf_default(conf),
    }
}

/// Upload destination from a `StartEgressRequest` request-level `StorageConfig`.
fn storage_destination(s: &lk::StorageConfig, conf: &EgressConfig) -> Result<Destination, String> {
    match &s.provider {
        Some(lk::storage_config::Provider::S3(s3)) => Ok(s3_destination(s3)),
        Some(_) => Err(unsupported_upload()),
        None => conf_default(conf),
    }
}

fn unsupported_upload() -> String {
    "gcp/azure/aliOSS upload is not supported on the voice-only recorder".to_string()
}

fn s3_destination(s3: &lk::S3Upload) -> Destination {
    Destination::S3(Box::new(S3Target {
        access_key: s3.access_key.clone(),
        secret: s3.secret.clone(),
        session_token: s3.session_token.clone(),
        region: s3.region.clone(),
        endpoint: s3.endpoint.clone(),
        bucket: s3.bucket.clone(),
        force_path_style: s3.force_path_style,
        metadata: s3
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        tagging: s3.tagging.clone(),
    }))
}

/// The container-config upload default (`s3:` block), or local storage.
fn conf_default(conf: &EgressConfig) -> Result<Destination, String> {
    Ok(match &conf.s3 {
        Some(c) => Destination::S3(Box::new(S3Target {
            access_key: c.access_key.clone(),
            secret: c.secret.clone(),
            session_token: c.session_token.clone(),
            region: c.region.clone(),
            endpoint: c.endpoint.clone(),
            bucket: c.bucket.clone(),
            force_path_style: c.force_path_style,
            metadata: c.metadata.clone(),
            tagging: c.tagging.clone(),
        })),
        None => Destination::Local,
    })
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
        tracing::debug!(method, len = raw.len(), "psrpc request");
        match method {
            "StartEgress" => {
                let req =
                    rpc::StartEgressRequest::decode(raw.as_slice()).map_err(|e| e.to_string())?;
                let egress_id = req.egress_id.clone();
                if egress_id.is_empty() {
                    return Err("egress_id is required".to_string());
                }
                let (spec, room) = {
                    let spec = rec_spec(&req, &self.conf)?;
                    let room = spec.room.clone();
                    (spec, room)
                };
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
                    if let Err(e) = run_one(&conf, &ctx, &egress_id, &spec, stop_rx).await {
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

/// Records one room's audio, then uploads the finished file (S3-compatible or
/// local). Stops on `StopEgress` or when the room's audio stream ends.
async fn run_one(
    conf: &EgressConfig,
    ctx: &JobCtx,
    egress_id: &str,
    spec: &RecSpec,
    stop_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    let ext = if spec.format == OutputFormat::Mp3 {
        "mp3"
    } else {
        "wav"
    };
    let local = format!("{}/{egress_id}.{ext}", conf.output_dir);
    tracing::info!(egress_id, room = %spec.room, "starting recording");
    let audio = client::connect(
        &conf.api_key,
        &conf.api_secret,
        &conf.ws_url,
        &spec.room,
        &format!("egress_{egress_id}"),
    )
    .await?;
    tracing::info!(egress_id, room = %spec.room, "connected; recording");
    let frames = recorder::run_recording(
        audio,
        &local,
        spec.format,
        conf.mp3_bitrate,
        stop_rx.clone(),
    )
    .await?;
    let size = std::fs::metadata(&local).map(|m| m.len()).unwrap_or(0);
    // Upload the finished file; `location` becomes FileInfo.location.
    let key = storage_key(&spec.filepath, egress_id, ext);
    let (location, _) = upload::upload(&local, &key, &spec.destination).await?;
    let request = lk::egress_info::Request::RoomComposite(lk::RoomCompositeEgressRequest {
        room_name: spec.room.clone(),
        ..Default::default()
    });
    let info = recorder::finished_info(
        egress_id, &spec.room, &key, &location, request, frames, size,
    );
    let _ = ctx.io.update_egress(&info).await;
    ctx.infos
        .lock()
        .unwrap()
        .insert(egress_id.to_string(), info.clone());
    ctx.active.lock().unwrap().remove(egress_id);
    tracing::info!(egress_id, room = %spec.room, key, location, frames, "recording finished");
    Ok(())
}

/// The storage key for a recording: the request `filepath` (with extension
/// appended when missing) or `{egress_id}.{ext}`.
fn storage_key(filepath: &str, egress_id: &str, ext: &str) -> String {
    let suffix = format!(".{ext}");
    if filepath.is_empty() {
        return format!("{egress_id}.{ext}");
    }
    if filepath.ends_with(&suffix) {
        filepath.to_string()
    } else {
        format!("{filepath}.{ext}")
    }
}
