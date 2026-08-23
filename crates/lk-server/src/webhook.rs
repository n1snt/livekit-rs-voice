//! Webhook notifier. Posts `WebhookEvent` payloads to configured URLs,
//! signed with `X-Livekit-Signature: hex(HMAC-SHA256(webhook.api_key, body))`
//! exactly like the reference server.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use lk_proto::livekit as lk;
use sha2::Sha256;
use tokio::sync::Mutex;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct WebhookNotifier {
    inner: Option<Arc<Mutex<Inner>>>,
}

#[derive(Debug)]
struct Inner {
    urls: Vec<String>,
    secret: String,
    client: reqwest::Client,
}

impl WebhookNotifier {
    /// Builds the notifier from config, resolving the API secret for the
    /// configured webhook `api_key` (the reference signs with the API secret,
    /// not the key string). Returns a disabled notifier when no webhook URLs
    /// are configured.
    pub fn from_config(config: &Config, keys: &crate::auth::KeyProvider) -> Self {
        if config.webhook.urls.is_empty() {
            return Self { inner: None };
        }
        let secret = keys
            .get_secret(&config.webhook.api_key)
            .map(str::to_string)
            .unwrap_or_else(|| config.webhook.api_key.clone());
        WebhookNotifier {
            inner: Some(Arc::new(Mutex::new(Inner {
                urls: config.webhook.urls.clone(),
                secret,
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            }))),
        }
    }

    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
        mac.update(body);
        let bytes = mac.finalize().into_bytes();
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push_str(&format!("{b:02x}"));
        }
        out
    }

    /// Sends a webhook event asynchronously with bounded retries and
    /// exponential backoff. Lifecycle events (room_finished, egress_ended)
    /// drive billing/DID release, so a single lost delivery is costly; the
    /// reference keeps a durable queue, but in-process retries cover transient
    /// backend/network failures.
    pub async fn send_event(&self, event: lk::WebhookEvent) {
        let Some(inner) = self.inner.clone() else {
            return;
        };
        let body = serde_json::to_vec(&event).unwrap_or_default();
        let signature = {
            let inner = inner.lock().await;
            (inner.urls.clone(), Self::sign(&inner.secret, &body))
        };
        for url in &signature.0 {
            let url = url.clone();
            let sig = signature.1.clone();
            let client = inner.lock().await.client.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let headers = [("X-Livekit-Signature", sig.as_str())];
                // 1 initial attempt + 5 retries with backoff (250ms..4s).
                let delays = [250u64, 500, 1000, 2000, 4000];
                for (i, delay_ms) in delays.iter().enumerate() {
                    if Self::post_once(&client, &url, &headers, &body)
                        .await
                        .is_ok()
                    {
                        return;
                    }
                    if i + 1 < delays.len() {
                        tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
                    }
                }
            });
        }
    }

    async fn post_once(
        client: &reqwest::Client,
        url: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(), reqwest::Error> {
        let mut req = client.post(url).body(body.to_vec());
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        req.send().await.map(|_| ())
    }

    // Convenience event constructors used by the room lifecycle.

    pub async fn room_started(&self, room: &lk::Room) {
        self.send_event(lk::WebhookEvent {
            event: "room_started".to_string(),
            room: Some(room.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn room_finished(&self, room: &lk::Room) {
        self.send_event(lk::WebhookEvent {
            event: "room_finished".to_string(),
            room: Some(room.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn participant_joined(&self, room: &lk::Room, participant: &lk::ParticipantInfo) {
        self.send_event(lk::WebhookEvent {
            event: "participant_joined".to_string(),
            room: Some(room.clone()),
            participant: Some(participant.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn participant_left(&self, room: &lk::Room, participant: &lk::ParticipantInfo) {
        self.send_event(lk::WebhookEvent {
            event: "participant_left".to_string(),
            room: Some(room.clone()),
            participant: Some(participant.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn track_published(
        &self,
        room: &lk::Room,
        participant: &lk::ParticipantInfo,
        track: &lk::TrackInfo,
    ) {
        self.send_event(lk::WebhookEvent {
            event: "track_published".to_string(),
            room: Some(room.clone()),
            participant: Some(participant.clone()),
            track: Some(track.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn track_unpublished(
        &self,
        room: &lk::Room,
        participant: &lk::ParticipantInfo,
        track: &lk::TrackInfo,
    ) {
        self.send_event(lk::WebhookEvent {
            event: "track_unpublished".to_string(),
            room: Some(room.clone()),
            participant: Some(participant.clone()),
            track: Some(track.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn agent_job_started(&self, job: &lk::Job) {
        self.send_event(lk::WebhookEvent {
            event: "agent_job_started".to_string(),
            job: Some(job.clone()),
            room: job.room.clone(),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn agent_job_ended(&self, job: &lk::Job) {
        self.send_event(lk::WebhookEvent {
            event: "agent_job_ended".to_string(),
            job: Some(job.clone()),
            room: job.room.clone(),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    /// A minimal `Room` carrying the egress's room id/name, mirroring the
    /// reference server's `egress_started`/`egress_updated`/`egress_ended`
    /// payloads (room id + name + full `EgressInfo`).
    fn egress_room(info: &lk::EgressInfo) -> lk::Room {
        lk::Room {
            sid: info.room_id.clone(),
            name: info.room_name.clone(),
            ..Default::default()
        }
    }

    pub async fn egress_started(&self, info: &lk::EgressInfo) {
        self.send_event(lk::WebhookEvent {
            event: "egress_started".to_string(),
            room: Some(Self::egress_room(info)),
            egress_info: Some(info.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn egress_updated(&self, info: &lk::EgressInfo) {
        self.send_event(lk::WebhookEvent {
            event: "egress_updated".to_string(),
            room: Some(Self::egress_room(info)),
            egress_info: Some(info.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }

    pub async fn egress_ended(&self, info: &lk::EgressInfo) {
        self.send_event(lk::WebhookEvent {
            event: "egress_ended".to_string(),
            room: Some(Self::egress_room(info)),
            egress_info: Some(info.clone()),
            created_at: crate::core::unix_seconds(),
            id: crate::core::generate_id("EV_"),
            ..Default::default()
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_expected() {
        // Test vector: HMAC-SHA256("secret", "hello") hex
        let sig = WebhookNotifier::sign("secret", b"hello");
        assert_eq!(
            sig,
            "88aab3ede8d3adf94d26ab90d3bafd4a2083070c3bcce9c014ee04a443847c0b"
        );
        assert_eq!(sig.len(), 64);
    }

    #[test]
    fn disabled_when_no_urls() {
        let n = WebhookNotifier::disabled();
        assert!(!n.is_enabled());
    }

    #[test]
    fn signs_deterministically() {
        assert_eq!(
            WebhookNotifier::sign("k", b"body"),
            WebhookNotifier::sign("k", b"body")
        );
    }
}
