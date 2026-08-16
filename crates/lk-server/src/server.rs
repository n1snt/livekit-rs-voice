//! Server-level room management and background workers.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lk_proto::livekit as lk;

use crate::agent::AgentManager;
use crate::auth::KeyProvider;
use crate::config::Config;
use crate::media::RtcEngine;
use crate::metrics::Metrics;
use crate::room::{Room, RoomContext};
use crate::webhook::WebhookNotifier;

pub struct Server {
    pub config: Arc<Config>,
    pub keys: KeyProvider,
    pub context: Arc<RoomContext>,
    pub node_id: String,
    pub start_time: Instant,
    pub store: Arc<crate::redis_store::Store>,
    pub cluster: Arc<crate::cluster::Cluster>,
    rooms: Mutex<HashMap<String, Arc<Room>>>,
}

impl Server {
    pub fn new(config: Config) -> Arc<Self> {
        let node_id = if config.node_id.is_empty() {
            crate::core::node_id(None)
        } else {
            config.node_id.clone()
        };
        let cluster = crate::cluster::Cluster::new(&config, &node_id);
        Server::with_cluster(config, cluster)
    }

    /// Builds a server around an explicitly provided cluster (used by the
    /// multi-node integration tests to share a cluster bus between nodes).
    pub fn with_cluster(config: Config, cluster: Arc<crate::cluster::Cluster>) -> Arc<Self> {
        let config = Arc::new(config);
        let keys = KeyProvider::new(&config);
        let rtc = Arc::new(RtcEngine::new());
        let webhook = WebhookNotifier::from_config(&config);
        let metrics = Arc::new(Metrics::default());
        let agent = Arc::new(AgentManager::new_with_keys(keys.clone()));
        let node_id = cluster.node_id.clone();
        let context = Arc::new(RoomContext::new(
            config.clone(),
            rtc,
            webhook,
            metrics,
            agent,
            cluster.clone(),
        ));
        let store = crate::redis_store::Store::from_config(&config);
        Arc::new(Server {
            config,
            keys,
            context,
            node_id,
            start_time: Instant::now(),
            store,
            cluster,
            rooms: Mutex::new(HashMap::new()),
        })
    }

    pub fn get_or_create_room(self: &Arc<Self>, name: &str) -> Arc<Room> {
        {
            let rooms = self.rooms.lock().unwrap();
            if let Some(room) = rooms.get(name) {
                return room.clone();
            }
        }
        let room = Room::new(name.to_string(), Arc::downgrade(&self.context));
        let server = Arc::downgrade(self);
        let weak_room = Arc::downgrade(&room);
        room.set_on_close(move || {
            if let Some(s) = server.upgrade() {
                s.on_room_closed(&weak_room);
            }
        });
        self.context
            .metrics
            .room_total
            .fetch_add(1, Ordering::Relaxed);
        let mut rooms = self.rooms.lock().unwrap();
        rooms
            .entry(name.to_string())
            .or_insert_with(|| room.clone());
        room
    }

    pub fn get_room(&self, name: &str) -> Option<Arc<Room>> {
        self.rooms.lock().unwrap().get(name).cloned()
    }

    pub fn list_rooms(&self) -> Vec<Arc<Room>> {
        self.rooms.lock().unwrap().values().cloned().collect()
    }

    pub fn num_rooms(&self) -> usize {
        self.rooms.lock().unwrap().len()
    }

    pub fn remove_room(&self, name: &str) {
        self.rooms.lock().unwrap().remove(name);
        self.context
            .metrics
            .room_total
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn on_room_closed(&self, room: &std::sync::Weak<Room>) {
        if let Some(room) = room.upgrade() {
            self.context
                .speaker_states
                .lock()
                .unwrap()
                .remove(&room.name);
            self.remove_room(&room.name);
        }
    }

    /// Closes a room by name, returning true if it existed.
    pub async fn close_room(&self, name: &str, reason: lk::DisconnectReason) -> bool {
        let room = self.get_room(name);
        let Some(room) = room else {
            return false;
        };
        room.close(reason).await;
        true
    }

    /// Background worker: room empty-timeout enforcement and active-speaker
    /// broadcasts.
    pub fn start_background_tasks(self: &Arc<Self>) {
        // Embedded TURN server (if enabled).
        {
            let config = self.config.clone();
            let keys = self.keys.as_map();
            tokio::spawn(async move {
                if let Err(e) = crate::turn::start_turn_server(&config, keys).await {
                    tracing::error!("failed to start TURN server: {e}");
                }
            });
        }
        self.cluster.start_heartbeat();
        if self.cluster.is_enabled() {
            let server = self.clone();
            let cluster = self.cluster.clone();
            tokio::spawn(async move {
                cluster.run_relay_consumer(&server).await;
            });
        }
        let server = self.clone();
        tokio::spawn(async move {
            let mut room_ticker = tokio::time::interval(Duration::from_secs(1));
            let mut speaker_ticker = tokio::time::interval(Duration::from_millis(400));
            loop {
                tokio::select! {
                    _ = room_ticker.tick() => {
                        server.check_room_timeouts().await;
                    }
                    _ = speaker_ticker.tick() => {
                        server.broadcast_speakers();
                    }
                }
            }
        });
    }

    async fn check_room_timeouts(&self) {
        let rooms: Vec<Arc<Room>> = self.list_rooms();
        for room in rooms {
            if room.should_close() {
                room.close(lk::DisconnectReason::RoomClosed).await;
            }
        }
    }

    fn broadcast_speakers(&self) {
        for room in self.list_rooms() {
            if room.is_closed() {
                continue;
            }
            let participants = room.participants();
            let speakers = crate::media::active_speakers(&participants);
            // Only broadcast when the speaker set changed to avoid churn.
            let mut current = speakers.iter().map(|s| s.sid.clone()).collect::<Vec<_>>();
            current.sort();
            let key = current.join(",");
            let last = self.speaker_state(room.name.as_str());
            if last == key {
                continue;
            }
            self.set_speaker_state(room.name.as_str(), key);
            let resp = lk::SignalResponse {
                message: Some(lk::signal_response::Message::SpeakersChanged(
                    lk::SpeakersChanged { speakers },
                )),
            };
            for p in &participants {
                p.send_update(resp.clone());
            }
        }
    }

    fn speaker_state(&self, room: &str) -> String {
        self.context
            .speaker_states
            .lock()
            .unwrap()
            .get(room)
            .cloned()
            .unwrap_or_default()
    }

    fn set_speaker_state(&self, room: &str, state: String) {
        self.context
            .speaker_states
            .lock()
            .unwrap()
            .insert(room.to_string(), state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn room_lifecycle() {
        let server = Server::new(Config::default());
        let room = server.get_or_create_room("test-room");
        assert_eq!(server.num_rooms(), 1);
        assert!(server.get_room("test-room").is_some());

        room.close(lk::DisconnectReason::RoomClosed).await;
        // on_close removes from manager
        tokio::task::yield_now().await;
        assert_eq!(server.num_rooms(), 0);
    }

    #[tokio::test]
    async fn get_or_create_is_idempotent() {
        let server = Server::new(Config::default());
        let a = server.get_or_create_room("r");
        let b = server.get_or_create_room("r");
        assert!(Arc::ptr_eq(&a, &b));
    }
}
