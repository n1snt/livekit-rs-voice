//! `livekit-egress` configuration (YAML).
//!
//! Accepts the reference `livekit/egress` container's config keys so the same
//! `egress.yaml` works unchanged: `s3` (default upload destination), `insecure`
//! and `cpu_cost` (accepted; unused on the voice-only recorder, matching the Go
//! egress where they only affect web egress / job admission), and `log_level`.

use std::collections::HashMap;

use lk_psrpc::RedisConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EgressConfig {
    /// API key/secret used to mint room-join tokens.
    pub api_key: String,
    pub api_secret: String,
    /// Base WebSocket URL of the livekit-voice server, e.g. `ws://127.0.0.1:7880`.
    pub ws_url: String,
    /// Where recordings are written before any upload.
    pub output_dir: String,
    /// Shared Redis (the psrpc bus the livekit-voice server uses).
    pub redis: RedisConfig,
    /// MP3 bitrate in kbps (default 64).
    pub mp3_bitrate: i32,
    /// Prometheus metrics port (0 disables).
    pub prometheus_port: u16,
    /// Logging level.
    pub logging: LoggingConfig,
    /// Go-style top-level `log_level` alias for `logging.level`.
    pub log_level: String,
    /// Default S3 (S3-compatible) upload destination. Request-level upload
    /// config overrides this.
    pub s3: Option<S3Config>,
    /// Go egress's `insecure` (web-egress Chrome flags). The voice-only
    /// recorder never runs Chrome, so this is accepted and unused.
    pub insecure: bool,
    /// Go egress's `cpu_cost` job-admission costs. The voice-only recorder has
    /// no admission gating, so this is accepted and unused.
    pub cpu_cost: Option<CpuCostConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

/// Default S3 upload target (`s3:` block). Mirrors the fields the Go egress
/// accepts for its default storage config.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct S3Config {
    pub access_key: String,
    pub secret: String,
    pub session_token: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub force_path_style: bool,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
}

/// Go egress job-admission CPU costs (`cpu_cost:` block). Parsed so the key is
/// not silently dropped; nothing consumes it on the voice-only recorder.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct CpuCostConfig {
    pub room_composite_cpu_cost: f64,
}

impl Default for EgressConfig {
    fn default() -> Self {
        EgressConfig {
            api_key: String::new(),
            api_secret: String::new(),
            ws_url: "ws://127.0.0.1:7880".to_string(),
            output_dir: "/out".to_string(),
            redis: RedisConfig::default(),
            mp3_bitrate: 64,
            prometheus_port: 0,
            logging: LoggingConfig {
                level: "info".to_string(),
            },
            log_level: String::new(),
            s3: None,
            insecure: false,
            cpu_cost: None,
        }
    }
}

impl EgressConfig {
    /// Effective log level: the Go-style `log_level` key wins when set,
    /// otherwise `logging.level`.
    pub fn effective_log_level(&self) -> &str {
        if !self.log_level.is_empty() {
            return &self.log_level;
        }
        &self.logging.level
    }
}

pub fn load_config_from_yaml(yaml: &str) -> Result<EgressConfig, String> {
    serde_yaml::from_str(yaml).map_err(|e| format!("config: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let cfg = load_config_from_yaml(
            r#"
api_key: devkey
api_secret: secret
ws_url: ws://127.0.0.1:7880
output_dir: /out
redis:
  address: 127.0.0.1:6379
"#,
        )
        .unwrap();
        assert_eq!(cfg.api_key, "devkey");
        assert_eq!(cfg.redis.address, "127.0.0.1:6379");
        assert_eq!(cfg.mp3_bitrate, 64);
        assert!(cfg.s3.is_none());
    }

    #[test]
    fn parses_go_egress_keys() {
        let cfg = load_config_from_yaml(
            r#"
log_level: debug
api_key: devkey
api_secret: secret
ws_url: ws://127.0.0.1:7880
insecure: true
cpu_cost:
  room_composite_cpu_cost: 0.25
s3:
  access_key: ak
  secret: sk
  region: auto
  bucket: voice-ai-recordings
  endpoint: https://example.r2.cloudflarestorage.com
redis:
  address: 127.0.0.1:6379
"#,
        )
        .unwrap();
        assert_eq!(cfg.effective_log_level(), "debug");
        assert!(cfg.insecure);
        assert_eq!(cfg.cpu_cost.as_ref().unwrap().room_composite_cpu_cost, 0.25);
        let s3 = cfg.s3.unwrap();
        assert_eq!(s3.bucket, "voice-ai-recordings");
        assert_eq!(s3.endpoint, "https://example.r2.cloudflarestorage.com");
        assert_eq!(s3.region, "auto");
    }

    #[test]
    fn log_level_falls_back_to_logging() {
        let cfg = load_config_from_yaml(
            r#"
api_key: devkey
api_secret: secret
ws_url: ws://127.0.0.1:7880
logging:
  level: warn
"#,
        )
        .unwrap();
        assert_eq!(cfg.effective_log_level(), "warn");
    }
}
