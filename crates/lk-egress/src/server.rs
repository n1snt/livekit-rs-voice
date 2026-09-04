//! psrpc egress services: `EgressInternal` (StartEgress / ListActiveEgress)
//! and `EgressHandler` (StopEgress, per-egress topic), receiving jobs from the
//! livekit-voice server and running voice recordings.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use lk_proto::livekit as lk;
use lk_proto::rpc;
use lk_psrpc::{IoHandler, PsrpcBus, PsrpcServer, RpcError};
use prost::Message as _;
use tokio::sync::watch;

use crate::client;
use crate::config::EgressConfig;
use crate::io::IoClient;
use crate::recorder::{self, OutputFormat};
use crate::upload::{self, AzureTarget, Destination, GcpTarget, S3Target};

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

/// `cpu_cost` job-admission: reserves `room_composite_cpu_cost` per active
/// recording against the node's CPU count (Go egress parity — the reference
/// config sets 0.25 for audio-only so a 4-vCPU node admits ~16 recordings).
fn admitted(conf: &EgressConfig, active_count: usize) -> bool {
    let cost = conf
        .cpu_cost
        .as_ref()
        .map(|c| c.room_composite_cpu_cost)
        .unwrap_or(3.0);
    let capacity = std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0);
    ((active_count as f64) + 1.0) * cost <= capacity
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
    /// Request-level audio bitrate (kbps); 0 = use the config default.
    bitrate: i32,
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
                bitrate: match &r.options {
                    Some(lk::room_composite_egress_request::Options::Advanced(o)) => {
                        o.audio_bitrate
                    }
                    _ => 0,
                },
            })
        }
        Some(rpc::start_egress_request::Request::Track(r)) => match &r.output {
            Some(lk::track_egress_request::Output::File(f)) => Ok(RecSpec {
                room: r.room_name.clone(),
                format: OutputFormat::Wav,
                destination: direct_destination(f, conf)?,
                filepath: f.filepath.clone(),
                bitrate: 0,
            }),
            _ => {
                let (format, destination, filepath) = default(conf)?;
                Ok(RecSpec {
                    room: r.room_name.clone(),
                    format,
                    destination,
                    filepath,
                    bitrate: 0,
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
                bitrate: 0,
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
                bitrate: 0,
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
                bitrate: 0,
            })
        }
        _ => Err("web/replay egress is not supported on the voice-only recorder".to_string()),
    }
}

/// Maps an `EncodedFileOutput` to (format, upload destination, storage key).
/// Request-level upload config wins; otherwise the container default
/// (`s3:`/`gcp:`/`azure:`); otherwise local.
fn encoded_spec(
    f: &lk::EncodedFileOutput,
    conf: &EgressConfig,
) -> Result<(OutputFormat, Destination, String), String> {
    let format = encoded_format(f.file_type);
    let destination = match &f.output {
        Some(lk::encoded_file_output::Output::S3(s3)) => s3_destination(s3, conf),
        Some(lk::encoded_file_output::Output::Gcp(g)) => gcp_destination(g),
        Some(lk::encoded_file_output::Output::Azure(a)) => Ok(azure_destination(a)),
        Some(lk::encoded_file_output::Output::AliOss(_)) => Err(unsupported_alioss()),
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
        Some(lk::direct_file_output::Output::S3(s3)) => s3_destination(s3, conf),
        Some(lk::direct_file_output::Output::Gcp(g)) => gcp_destination(g),
        Some(lk::direct_file_output::Output::Azure(a)) => Ok(azure_destination(a)),
        Some(lk::direct_file_output::Output::AliOss(_)) => Err(unsupported_alioss()),
        None => conf_default(conf),
    }
}

/// Upload destination from a `StartEgressRequest` request-level `StorageConfig`.
fn storage_destination(s: &lk::StorageConfig, conf: &EgressConfig) -> Result<Destination, String> {
    match &s.provider {
        Some(lk::storage_config::Provider::S3(s3)) => s3_destination(s3, conf),
        Some(lk::storage_config::Provider::Gcp(g)) => gcp_destination(g),
        Some(lk::storage_config::Provider::Azure(a)) => Ok(azure_destination(a)),
        Some(lk::storage_config::Provider::AliOss(_)) => Err(unsupported_alioss()),
        None => conf_default(conf),
    }
}

fn unsupported_alioss() -> String {
    "aliOSS upload is not supported on the voice-only recorder".to_string()
}

fn s3_destination(s3: &lk::S3Upload, conf: &EgressConfig) -> Result<Destination, String> {
    if s3.proxy.is_some() {
        return Err("s3 proxy is not supported on the voice-only recorder".to_string());
    }
    let assume_role_arn = if s3.assume_role_arn.is_empty() {
        conf.s3_assume_role_arn.clone()
    } else {
        s3.assume_role_arn.clone()
    };
    let assume_role_external_id = if s3.assume_role_external_id.is_empty() {
        conf.s3_assume_role_external_id.clone()
    } else {
        s3.assume_role_external_id.clone()
    };
    let (access_key, secret) = if !assume_role_arn.is_empty() && s3.access_key.is_empty() {
        (
            conf.s3_assume_role_key.clone(),
            conf.s3_assume_role_secret.clone(),
        )
    } else {
        (s3.access_key.clone(), s3.secret.clone())
    };
    Ok(Destination::S3(Box::new(S3Target {
        access_key,
        secret,
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
        content_disposition: s3.content_disposition.clone(),
        assume_role_arn,
        assume_role_external_id,
    })))
}

fn gcp_destination(g: &lk::GcpUpload) -> Result<Destination, String> {
    if g.proxy.is_some() {
        return Err("gcp proxy is not supported on the voice-only recorder".to_string());
    }
    Ok(Destination::Gcp(Box::new(GcpTarget {
        credentials: g.credentials.clone(),
        bucket: g.bucket.clone(),
        metadata: HashMap::new(),
        tagging: String::new(),
        content_disposition: String::new(),
    })))
}

fn azure_destination(a: &lk::AzureBlobUpload) -> Destination {
    Destination::Azure(Box::new(AzureTarget {
        account_name: a.account_name.clone(),
        account_key: a.account_key.clone(),
        container_name: a.container_name.clone(),
        metadata: HashMap::new(),
        tagging: String::new(),
        content_disposition: String::new(),
    }))
}

/// The container-config upload default (`s3:`/`gcp:`/`azure:`), or local
/// storage. `alioss:` is parsed but rejected.
fn conf_default(conf: &EgressConfig) -> Result<Destination, String> {
    if let Some(c) = &conf.s3 {
        if c.proxy.is_some() {
            return Err("s3 proxy is not supported on the voice-only recorder".to_string());
        }
        let assume_role_arn = if c.assume_role_arn.is_empty() {
            conf.s3_assume_role_arn.clone()
        } else {
            c.assume_role_arn.clone()
        };
        let assume_role_external_id = if c.assume_role_external_id.is_empty() {
            conf.s3_assume_role_external_id.clone()
        } else {
            c.assume_role_external_id.clone()
        };
        let (access_key, secret) = if !assume_role_arn.is_empty() && c.access_key.is_empty() {
            (
                conf.s3_assume_role_key.clone(),
                conf.s3_assume_role_secret.clone(),
            )
        } else {
            (c.access_key.clone(), c.secret.clone())
        };
        return Ok(Destination::S3(Box::new(S3Target {
            access_key,
            secret,
            session_token: c.session_token.clone(),
            region: c.region.clone(),
            endpoint: c.endpoint.clone(),
            bucket: c.bucket.clone(),
            force_path_style: c.force_path_style,
            metadata: c.metadata.clone(),
            tagging: c.tagging.clone(),
            content_disposition: c.content_disposition.clone(),
            assume_role_arn,
            assume_role_external_id,
        })));
    }
    if let Some(g) = &conf.gcp {
        return Ok(Destination::Gcp(Box::new(GcpTarget {
            credentials: g.credentials.clone(),
            bucket: g.bucket.clone(),
            metadata: g.metadata.clone(),
            tagging: g.tagging.clone(),
            content_disposition: g.content_disposition.clone(),
        })));
    }
    if let Some(a) = &conf.azure {
        return Ok(Destination::Azure(Box::new(AzureTarget {
            account_name: a.account_name.clone(),
            account_key: a.account_key.clone(),
            container_name: a.container_name.clone(),
            metadata: a.metadata.clone(),
            tagging: a.tagging.clone(),
            content_disposition: a.content_disposition.clone(),
        })));
    }
    if conf.alioss.is_some() {
        return Err(unsupported_alioss());
    }
    Ok(Destination::Local)
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
        Some(SR::RoomComposite(r)) => {
            let mut r = r.clone();
            crate::redact::redact_room_composite(&mut r);
            Some(Request::RoomComposite(r))
        }
        Some(SR::Track(r)) => {
            let mut r = r.clone();
            crate::redact::redact_direct(&mut r);
            Some(Request::Track(r))
        }
        Some(SR::Participant(r)) => {
            let mut r = r.clone();
            crate::redact::redact_encoded(&mut r);
            Some(Request::Participant(r))
        }
        Some(SR::TrackComposite(r)) => {
            let mut r = r.clone();
            crate::redact::redact_encoded(&mut r);
            Some(Request::TrackComposite(r))
        }
        Some(SR::Egress(r)) => {
            let mut r = r.clone();
            crate::redact::redact_start(&mut r);
            Some(Request::Egress(r))
        }
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
    async fn handle(&self, _method: &str, raw: Vec<u8>) -> Result<Vec<u8>, RpcError> {
        let req = lk::StopEgressRequest::decode(raw.as_slice())?;
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
            .ok_or_else(|| RpcError::not_found(format!("egress {egress_id} not found")))
    }
}

#[async_trait::async_trait]
impl IoHandler for Handlers {
    async fn handle(&self, method: &str, raw: Vec<u8>) -> Result<Vec<u8>, RpcError> {
        tracing::debug!(method, len = raw.len(), "psrpc request");
        match method {
            "StartEgress" => {
                let req = rpc::StartEgressRequest::decode(raw.as_slice())
                    ?;
                let egress_id = req.egress_id.clone();
                if egress_id.is_empty() {
                    return Err(RpcError::invalid_argument("egress_id is required"));
                }
                let (spec, room) = {
                    let spec = rec_spec(&req, &self.conf).map_err(RpcError::invalid_argument)?;
                    let room = spec.room.clone();
                    (spec, room)
                };
                if room.is_empty() {
                    return Err(RpcError::invalid_argument("room_name is required"));
                }
                let request = request_info(&req).ok_or_else(|| {
                    RpcError::invalid_argument("unsupported egress request")
                })?;
                if !admitted(&self.conf, self.active.lock().unwrap().len()) {
                    return Err(RpcError::unavailable(format!(
                        "egress node at capacity ({} active, cpu_cost admission)",
                        self.active.lock().unwrap().len()
                    )));
                }

                let now = crate::now_nanos();
                let starting = lk::EgressInfo {
                    egress_id: egress_id.clone(),
                    room_id: req.room_id.clone(),
                    room_name: room.clone(),
                    status: lk::EgressStatus::EgressStarting as i32,
                    started_at: now,
                    updated_at: now,
                    request: Some(request.clone()),
                    // The Go egress reports EGRESS_SOURCE_TYPE_SDK for the SDK
                    // (room-composite-audio/track/participant/media) sources.
                    source_type: lk::EgressSourceType::Sdk as i32,
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
                let request = request.clone();
                let room_id = req.room_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = run_one(
                        &conf,
                        &ctx,
                        &egress_id,
                        &room_id,
                        &spec,
                        request.clone(),
                        stop_rx,
                    )
                    .await
                    {
                        tracing::warn!(egress_id, "recording failed: {e}");
                        // Report EGRESS_FAILED so the server fires the terminal
                        // `egress_ended` webhook (Go egress parity) and the
                        // stored info is not left in a stale STARTING/ACTIVE
                        // state that a sweeper could adopt.
                        let now = crate::now_nanos();
                        let failed = lk::EgressInfo {
                            egress_id: egress_id.clone(),
                            room_id: room_id.clone(),
                            room_name: spec.room.clone(),
                            status: lk::EgressStatus::EgressFailed as i32,
                            started_at: now,
                            ended_at: now,
                            updated_at: now,
                            error: e,
                            request: Some(request.clone()),
                            source_type: lk::EgressSourceType::Sdk as i32,
                            ..Default::default()
                        };
                        let _ = ctx.io.update_egress(&failed).await;
                        ctx.infos.lock().unwrap().insert(egress_id.clone(), failed);
                        ctx.active.lock().unwrap().remove(&egress_id);
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
            _ => Err(RpcError::internal(format!("unknown egress method: {method}"))),
        }
    }
}

/// Records one room's audio, then uploads the finished file (S3-compatible or
/// local). Stops on `StopEgress` or when the room's audio stream ends.
async fn run_one(
    conf: &EgressConfig,
    ctx: &JobCtx,
    egress_id: &str,
    room_id: &str,
    spec: &RecSpec,
    request: lk::egress_info::Request,
    stop_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    let ext = if spec.format == OutputFormat::Mp3 {
        "mp3"
    } else {
        "wav"
    };
    let local = format!("{}/{egress_id}.{ext}", conf.output_dir);
    let bitrate = if spec.bitrate > 0 {
        spec.bitrate
    } else {
        conf.mp3_bitrate
    };
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

    // Report ACTIVE so the server fires the reference `egress_updated` webhook.
    let now = crate::now_nanos();
    let active = lk::EgressInfo {
        egress_id: egress_id.to_string(),
        room_id: room_id.to_string(),
        room_name: spec.room.clone(),
        status: lk::EgressStatus::EgressActive as i32,
        started_at: now,
        updated_at: now,
        request: Some(request.clone()),
        source_type: lk::EgressSourceType::Sdk as i32,
        ..Default::default()
    };
    let _ = ctx.io.update_egress(&active).await;
    ctx.infos
        .lock()
        .unwrap()
        .insert(egress_id.to_string(), active);

    let frames =
        recorder::run_recording(audio, &local, spec.format, bitrate, stop_rx.clone()).await?;
    let size = std::fs::metadata(&local).map(|m| m.len()).unwrap_or(0);
    // Upload the finished file; `location` becomes FileInfo.location.
    let key = storage_key(&spec.filepath, egress_id, ext);
    let (location, _) = upload::upload(&local, &key, &spec.destination).await?;
    // Uploaded recordings don't need to stay on local disk.
    if !matches!(spec.destination, Destination::Local) {
        let _ = std::fs::remove_file(&local);
    }
    let info = recorder::finished_info(
        egress_id, room_id, &spec.room, &key, &location, request, frames, size,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load_config_from_yaml;

    #[test]
    fn cpu_cost_admission_bounds_concurrency() {
        let capacity = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Configured 0.25 cost (the audio-only prod value): a 4-vCPU node
        // admits ~16 concurrent recordings.
        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
cpu_cost:
  room_composite_cpu_cost: 0.25
"#,
        )
        .unwrap();
        assert!(admitted(&conf, 0));
        assert!(admitted(&conf, capacity * 4 - 1));
        assert!(!admitted(&conf, capacity * 4));

        // Unconfigured cost falls back to the Go room-composite default (3.0),
        // which caps small nodes well below any large active count.
        let default = EgressConfig::default();
        assert!(admitted(&default, 0));
        assert!(!admitted(&default, capacity * 4));
    }

    #[test]
    fn storage_key_defaults_and_appends_extension() {
        assert_eq!(storage_key("", "EG_1", "wav"), "EG_1.wav");
        assert_eq!(storage_key("sub/rec", "EG_1", "mp3"), "sub/rec.mp3");
        assert_eq!(storage_key("sub/rec.mp3", "EG_1", "mp3"), "sub/rec.mp3");
    }

    #[test]
    fn config_default_resolves_gcp_and_azure() {
        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
gcp:
  credentials: '{"type":"service_account"}'
  bucket: g-bucket
"#,
        )
        .unwrap();
        assert!(matches!(conf_default(&conf).unwrap(), Destination::Gcp(_)));

        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
azure:
  account_name: acct
  account_key: key
  container_name: cont
"#,
        )
        .unwrap();
        assert!(matches!(
            conf_default(&conf).unwrap(),
            Destination::Azure(_)
        ));

        let conf = EgressConfig::default();
        assert!(matches!(conf_default(&conf).unwrap(), Destination::Local));
    }

    #[test]
    fn config_default_rejects_alioss_and_proxy() {
        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
alioss:
  access_key: a
  secret: b
  bucket: c
"#,
        )
        .unwrap();
        assert!(conf_default(&conf).unwrap_err().contains("aliOSS"));

        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
s3:
  access_key: a
  secret: b
  bucket: c
  proxy:
    url: http://proxy:8080
"#,
        )
        .unwrap();
        assert!(conf_default(&conf).unwrap_err().contains("proxy"));
    }

    #[test]
    fn request_s3_destination_applies_assume_role_defaults() {
        let conf = load_config_from_yaml(
            r#"
api_key: k
api_secret: s
ws_url: ws://x
s3_assume_role_key: base-key
s3_assume_role_secret: base-secret
s3_assume_role_arn: arn:aws:iam::123:role/default
"#,
        )
        .unwrap();
        let mut req = lk::S3Upload {
            bucket: "b".into(),
            ..Default::default()
        };
        // Request sets no arn and no keys → config arn + config base keys.
        let dest = s3_destination(&req, &conf).unwrap();
        let Destination::S3(t) = dest else {
            panic!("expected s3");
        };
        assert_eq!(t.assume_role_arn, "arn:aws:iam::123:role/default");
        assert_eq!(t.access_key, "base-key");
        assert_eq!(t.secret, "base-secret");

        // Request-level arn wins over the config default.
        req.assume_role_arn = "arn:aws:iam::123:role/requested".into();
        req.access_key = "req-key".into();
        let dest = s3_destination(&req, &conf).unwrap();
        let Destination::S3(t) = dest else {
            panic!("expected s3");
        };
        assert_eq!(t.assume_role_arn, "arn:aws:iam::123:role/requested");
        assert_eq!(t.access_key, "req-key");
    }

    #[test]
    fn request_s3_proxy_is_rejected() {
        let conf = EgressConfig::default();
        let req = lk::S3Upload {
            bucket: "b".into(),
            proxy: Some(lk::ProxyConfig {
                url: "http://proxy:8080".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(s3_destination(&req, &conf).unwrap_err().contains("proxy"));
    }

    #[test]
    fn request_audio_bitrate_flows_into_spec() {
        let mut req = rpc::StartEgressRequest {
            egress_id: "EG_1".into(),
            ..Default::default()
        };
        req.request = Some(rpc::start_egress_request::Request::RoomComposite(
            lk::RoomCompositeEgressRequest {
                room_name: "room".into(),
                options: Some(lk::room_composite_egress_request::Options::Advanced(
                    lk::EncodingOptions {
                        audio_bitrate: 128,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        ));
        let conf = EgressConfig::default();
        let spec = rec_spec(&req, &conf).unwrap();
        assert_eq!(spec.bitrate, 128);
    }
}
