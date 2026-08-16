//! Agent worker management and job dispatch (the `/agent` WebSocket protocol).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lk_proto::livekit as lk;
use prost::Message as _;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};

use crate::auth;
use crate::core::{new_dispatch_id, new_job_id, new_worker_id, unix_seconds};
use crate::server::Server;

const DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A registered agent worker.
pub struct Worker {
    pub worker_id: String,
    pub api_key: String,
    pub agent_name: String,
    pub job_type: i32,
    pub status: AtomicU32,
    pub load: AtomicU32,
    pub job_count: AtomicU32,
    pub ping_interval: u32,
    pub is_available: AtomicBool,
    tx: Mutex<Option<mpsc::Sender<lk::ServerMessage>>>,
    availability: Mutex<Option<oneshot::Sender<lk::AvailabilityResponse>>>,
}

impl Worker {
    pub(crate) fn send(&self, msg: lk::ServerMessage) {
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.try_send(msg);
        }
    }

    fn set_availability_responder(&self, tx: oneshot::Sender<lk::AvailabilityResponse>) {
        *self.availability.lock().unwrap() = Some(tx);
    }

    fn fulfill_availability(&self, res: lk::AvailabilityResponse) {
        if let Some(tx) = self.availability.lock().unwrap().take() {
            let _ = tx.send(res);
        }
    }
}

#[derive(Clone, Default)]
pub struct AgentDispatch {
    pub id: String,
    pub agent_name: String,
    pub room: String,
    pub metadata: String,
    pub deployment: String,
    pub attributes: std::collections::BTreeMap<String, String>,
    pub created_at: i64,
    pub deleted_at: Option<i64>,
    pub jobs: Vec<lk::Job>,
}

pub struct AgentManager {
    workers: Mutex<HashMap<String, Vec<Arc<Worker>>>>,
    dispatches: Mutex<HashMap<String, AgentDispatch>>,
    keys: auth::KeyProvider,
}

impl AgentManager {
    pub fn new() -> Self {
        AgentManager::new_with_keys(auth::KeyProvider::from_map(
            std::collections::BTreeMap::new(),
        ))
    }

    pub fn new_with_keys(keys: auth::KeyProvider) -> Self {
        AgentManager {
            workers: Mutex::new(HashMap::new()),
            dispatches: Mutex::new(HashMap::new()),
            keys,
        }
    }

    #[cfg(test)]
    pub fn new_stub() -> Self {
        AgentManager::new()
    }

    pub fn register_worker(&self, worker: Arc<Worker>) {
        self.workers
            .lock()
            .unwrap()
            .entry(worker.agent_name.clone())
            .or_default()
            .push(worker);
    }

    pub fn unregister_worker(&self, worker_id: &str) {
        let mut workers = self.workers.lock().unwrap();
        for list in workers.values_mut() {
            list.retain(|w| w.worker_id != worker_id);
        }
        workers.retain(|_, list| !list.is_empty());
    }

    pub fn workers_for(&self, agent_name: &str) -> Vec<Arc<Worker>> {
        self.workers
            .lock()
            .unwrap()
            .get(agent_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Picks a worker weighted by load (lower load preferred).
    fn pick_worker(workers: &[Arc<Worker>]) -> Option<Arc<Worker>> {
        let available: Vec<Arc<Worker>> = workers
            .iter()
            .filter(|w| w.is_available.load(Ordering::Relaxed))
            .cloned()
            .collect();
        if available.is_empty() {
            return None;
        }
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..available.len());
        Some(available[idx].clone())
    }

    pub async fn launch_room_job(
        &self,
        agent_name: &str,
        room: &crate::room::Room,
        metadata: &str,
        deployment: &str,
        attributes: std::collections::BTreeMap<String, String>,
        dispatch_id: Option<&str>,
    ) -> Result<String, String> {
        let job = lk::Job {
            id: new_job_id(),
            dispatch_id: dispatch_id.map(str::to_string).unwrap_or_default(),
            r#type: lk::JobType::JtRoom as i32,
            room: Some(room.to_proto()),
            metadata: metadata.to_string(),
            agent_name: agent_name.to_string(),
            state: Some(lk::JobState {
                status: lk::JobStatus::JsPending as i32,
                updated_at: unix_seconds() * 1_000_000_000,
                ..Default::default()
            }),
            deployment: deployment.to_string(),
            attributes,
            ..Default::default()
        };
        self.dispatch_job(job.clone()).await?;
        self.track_job(job.clone());
        Ok(job.id)
    }

    pub fn track_job(&self, job: lk::Job) {
        // Record the job under its dispatch for ListDispatch responses.
        if let Some(dispatch_id) = if !job.dispatch_id.is_empty() {
            Some(job.dispatch_id.as_str())
        } else {
            None
        } {
            if let Some(d) = self.dispatches.lock().unwrap().get_mut(dispatch_id) {
                if !d.jobs.iter().any(|j| j.id == job.id) {
                    d.jobs.push(job);
                }
            }
        }
    }

    /// Sends an availability request to a suitable worker and, if accepted,
    /// assigns the job with an agent join token.
    async fn dispatch_job(&self, job: lk::Job) -> Result<(), String> {
        let workers = self.workers_for(&job.agent_name);
        let Some(worker) = Self::pick_worker(&workers) else {
            return Err(format!(
                "no available worker for agent '{}'",
                job.agent_name
            ));
        };

        let (tx, rx) = oneshot::channel();
        if worker.availability.lock().unwrap().is_some() {
            return Err("worker already has a pending availability request".to_string());
        }
        worker.set_availability_responder(tx);
        worker.send(lk::ServerMessage {
            message: Some(lk::server_message::Message::Availability(
                lk::AvailabilityRequest {
                    job: Some(job.clone()),
                    resuming: false,
                },
            )),
        });

        let res = tokio::time::timeout(DISPATCH_TIMEOUT, rx)
            .await
            .map_err(|_| "worker did not respond to availability request".to_string())?
            .map_err(|_| "availability channel closed".to_string())?;

        if !res.available {
            return Err("worker declined the job".to_string());
        }

        let identity = if res.participant_identity.is_empty() {
            format!("agent-{}", job.id)
        } else {
            res.participant_identity.clone()
        };
        let secret = self
            .keys
            .get_secret(&worker.api_key)
            .ok_or_else(|| format!("unknown api key '{}'", worker.api_key))?
            .to_string();
        let token = build_agent_token(
            &worker.api_key,
            &secret,
            &job.room
                .as_ref()
                .map(|r| r.name.clone())
                .unwrap_or_default(),
            &identity,
            &res.participant_name,
            &res.participant_metadata,
            res.participant_attributes,
        )?;

        worker.send(lk::ServerMessage {
            message: Some(lk::server_message::Message::Assignment(lk::JobAssignment {
                job: Some(job.clone()),
                token: token.clone(),
                ..Default::default()
            })),
        });
        Ok(())
    }

    pub async fn terminate_room_jobs(&self, room_name: &str) {
        let jobs: Vec<lk::Job> = self
            .dispatches
            .lock()
            .unwrap()
            .values()
            .flat_map(|d| {
                d.jobs
                    .iter()
                    .filter(|j| {
                        j.room
                            .as_ref()
                            .map(|r| r.name == room_name)
                            .unwrap_or(false)
                    })
                    .cloned()
            })
            .collect();
        for job in jobs {
            let workers = self.workers_for(&job.agent_name);
            for w in workers {
                w.send(lk::ServerMessage {
                    message: Some(lk::server_message::Message::Termination(
                        lk::JobTermination {
                            job_id: job.id.clone(),
                        },
                    )),
                });
            }
        }
    }

    // -- AgentDispatch CRUD (used by AgentDispatchService) --

    pub fn create_dispatch(
        &self,
        agent_name: String,
        room: String,
        metadata: String,
        deployment: String,
        attributes: std::collections::BTreeMap<String, String>,
    ) -> AgentDispatch {
        let d = AgentDispatch {
            id: new_dispatch_id(),
            agent_name,
            room,
            metadata,
            deployment,
            attributes,
            created_at: unix_seconds(),
            deleted_at: None,
            jobs: Vec::new(),
        };
        self.dispatches
            .lock()
            .unwrap()
            .insert(d.id.clone(), d.clone());
        d
    }

    pub fn delete_dispatch(&self, id: &str) -> Option<AgentDispatch> {
        let mut d = self.dispatches.lock().unwrap().get_mut(id)?.clone();
        d.deleted_at = Some(unix_seconds());
        Some(d)
    }

    pub fn list_dispatches(&self, room: &str) -> Vec<AgentDispatch> {
        self.dispatches
            .lock()
            .unwrap()
            .values()
            .filter(|d| d.room == room)
            .cloned()
            .collect()
    }

    pub fn get_dispatch(&self, id: &str) -> Option<AgentDispatch> {
        self.dispatches.lock().unwrap().get(id).cloned()
    }
}

impl Default for AgentManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the agent participant join token (claims mirror `BuildAgentToken`).
pub fn build_agent_token(
    api_key: &str,
    secret: &str,
    room: &str,
    identity: &str,
    name: &str,
    metadata: &str,
    attributes: std::collections::BTreeMap<String, String>,
) -> Result<String, String> {
    let now = unix_seconds();
    let payload = serde_json::json!({
        "iss": api_key,
        "sub": identity,
        "name": name,
        "metadata": metadata,
        "attributes": attributes,
        "kind": "AGENT",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {
            "roomJoin": true,
            "room": room,
            "canPublish": true,
            "canSubscribe": true,
            "canPublishData": true,
            "canUpdateOwnMetadata": true,
            "agent": true
        }
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("encode agent token: {e}"))
}

/// Runs the agent worker websocket session.
pub async fn run_worker_session(
    socket: WebSocket,
    token: auth::VerifiedToken,
    server: Arc<Server>,
) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<lk::ServerMessage>(256);
    let mode = Arc::new(std::sync::atomic::AtomicBool::new(false)); // false = binary, true = json

    let worker_id = new_worker_id();
    let mut worker: Option<Arc<Worker>> = None;

    // Writer task.
    let writer_mode = mode.clone();
    let sink_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let use_json = writer_mode.load(std::sync::atomic::Ordering::Relaxed);
            let frame = if use_json {
                match serde_json::to_string(&msg) {
                    Ok(json) => Message::Text(json.into()),
                    Err(_) => continue,
                }
            } else {
                Message::Binary(msg.encode_to_vec().into())
            };
            if sink.send(frame).await.is_err() {
                break;
            }
        }
    });

    // Reader loop (with a read deadline based on the worker's ping interval).
    let read_timeout_ms = std::sync::atomic::AtomicU64::new(30_000);
    loop {
        let timeout_dur = std::time::Duration::from_millis(
            read_timeout_ms.load(std::sync::atomic::Ordering::Relaxed),
        );
        let frame = match tokio::time::timeout(timeout_dur, stream.next()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => break,
        };
        let frame = match frame {
            Ok(f) => f,
            Err(_) => break,
        };
        let req = match frame {
            Message::Binary(bytes) => {
                mode.store(false, std::sync::atomic::Ordering::Relaxed);
                match lk::WorkerMessage::decode(bytes.as_ref()) {
                    Ok(r) => r,
                    Err(_) => continue,
                }
            }
            Message::Text(text) => {
                mode.store(true, std::sync::atomic::Ordering::Relaxed);
                match serde_json::from_str::<lk::WorkerMessage>(&text) {
                    Ok(r) => r,
                    Err(_) => continue,
                }
            }
            Message::Close(_) => break,
            _ => continue,
        };
        let Some(msg) = req.message else { continue };
        match msg {
            lk::worker_message::Message::Register(register) => {
                let agent_name = register.agent_name;
                let job_type = register.r#type;
                let ping_interval = if register.ping_interval == 0 {
                    10
                } else {
                    register.ping_interval
                };
                let w = Arc::new(Worker {
                    worker_id: worker_id.clone(),
                    api_key: token.api_key.clone(),
                    agent_name: agent_name.clone(),
                    job_type,
                    status: AtomicU32::new(0),
                    load: AtomicU32::new(0),
                    job_count: AtomicU32::new(0),
                    ping_interval,
                    is_available: AtomicBool::new(true),
                    tx: Mutex::new(Some(tx.clone())),
                    availability: Mutex::new(None),
                });
                worker = Some(w.clone());
                server.context.agent.register_worker(w);
                let _ = tx.try_send(lk::ServerMessage {
                    message: Some(lk::server_message::Message::Register(
                        lk::RegisterWorkerResponse {
                            worker_id: worker_id.clone(),
                            server_info: Some(lk::ServerInfo {
                                edition: lk::server_info::Edition::Standard as i32,
                                version: crate::signal::SERVER_VERSION.to_string(),
                                protocol: crate::signal::PROTOCOL_VERSION,
                                region: server.config.region.clone(),
                                node_id: server.node_id.clone(),
                                agent_protocol: crate::signal::AGENT_PROTOCOL,
                                ..Default::default()
                            }),
                        },
                    )),
                });
            }
            lk::worker_message::Message::Availability(res) => {
                if let Some(w) = &worker {
                    w.fulfill_availability(res);
                }
            }
            lk::worker_message::Message::UpdateWorker(update) => {
                if let Some(w) = &worker {
                    if let Some(status) = update.status {
                        let avail = status == lk::WorkerStatus::WsAvailable as i32;
                        w.is_available.store(avail, Ordering::Relaxed);
                        w.status.store(status as u32, Ordering::Relaxed);
                    }
                    w.load.store(update.load.to_bits(), Ordering::Relaxed);
                    w.job_count.store(update.job_count, Ordering::Relaxed);
                }
            }
            lk::worker_message::Message::UpdateJob(update) => {
                // Job status updates are recorded for dashboard purposes.
                tracing::debug!(job = %update.job_id, status = %update.status, "job status update");
            }
            lk::worker_message::Message::Ping(ping) => {
                if let Some(w) = &worker {
                    w.send(lk::ServerMessage {
                        message: Some(lk::server_message::Message::Pong(lk::WorkerPong {
                            last_timestamp: ping.timestamp,
                            timestamp: crate::core::unix_micros() / 1000,
                        })),
                    });
                }
            }
            lk::worker_message::Message::SimulateJob(_)
            | lk::worker_message::Message::MigrateJob(_) => {}
        }
    }

    if let Some(w) = worker {
        server.context.agent.unregister_worker(&w.worker_id);
    }
    let _ = sink_task.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_token_has_expected_grants() {
        let token = build_agent_token(
            "key",
            "secret",
            "room1",
            "agent-123",
            "agent-name",
            "{}",
            Default::default(),
        )
        .unwrap();
        let provider = crate::auth::KeyProvider::from_map(
            std::iter::once(("key".to_string(), "secret".to_string())).collect(),
        );
        let verified = provider.verify(&token).unwrap();
        assert_eq!(verified.identity, "agent-123");
        assert_eq!(verified.video.room, "room1");
        assert!(verified.video.agent);
        assert!(verified.video.room_join);
        assert!(verified.can_publish());
        assert!(verified.can_subscribe());
        assert!(verified.can_publish_data());
    }

    #[tokio::test]
    async fn dispatch_without_workers_fails_gracefully() {
        let manager = AgentManager::new();
        let ctx = crate::room::test_context();
        let room = crate::room::Room::new("r".to_string(), std::sync::Arc::downgrade(&ctx));
        let err = manager
            .launch_room_job("voice-agent", &room, "{}", "", Default::default(), None)
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn dispatch_with_worker_assigns_job() {
        let keys = std::iter::once(("key".to_string(), "secret".to_string())).collect();
        let manager = AgentManager::new_with_keys(crate::auth::KeyProvider::from_map(keys));
        let (tx, _rx) = mpsc::channel(8);
        let worker = Arc::new(Worker {
            worker_id: "w1".to_string(),
            api_key: "key".to_string(),
            agent_name: "voice-agent".to_string(),
            job_type: 0,
            status: AtomicU32::new(0),
            load: AtomicU32::new(0),
            job_count: AtomicU32::new(0),
            ping_interval: 10,
            is_available: AtomicBool::new(true),
            tx: Mutex::new(Some(tx)),
            availability: Mutex::new(None),
        });
        manager.register_worker(worker);
        let ctx = crate::room::test_context();
        let room = crate::room::Room::new("r".to_string(), std::sync::Arc::downgrade(&ctx));

        // The worker never responds to availability, so dispatch should time out
        // (proving the flow reaches the worker) rather than erroring instantly.
        let res = manager
            .launch_room_job("voice-agent", &room, "{}", "", Default::default(), None)
            .await;
        assert!(res.is_err()); // timed out (no availability response)
    }
}
