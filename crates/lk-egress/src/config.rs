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
    /// Default GCP upload destination.
    pub gcp: Option<GcpConfig>,
    /// Default Azure upload destination.
    pub azure: Option<AzureConfig>,
    /// Default AliOSS upload destination (parsed; AliOSS uploads are not
    /// supported on the voice-only recorder).
    pub alioss: Option<AliOssConfig>,
    /// Go egress's `insecure` (web-egress Chrome flags). The voice-only
    /// recorder never runs Chrome, so this is accepted and unused.
    pub insecure: bool,
    /// Go egress's `cpu_cost` job-admission costs. The voice-only recorder has
    /// no admission gating, so this is accepted and unused.
    pub cpu_cost: Option<CpuCostConfig>,
    /// Base credentials used to assume `s3.assume_role_arn` when the request
    /// or config provides no access key (Go egress parity).
    pub s3_assume_role_key: String,
    pub s3_assume_role_secret: String,
    /// Default role to assume for S3 uploads when the request does not set one.
    pub s3_assume_role_arn: String,
    pub s3_assume_role_external_id: String,
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
    pub assume_role_arn: String,
    pub assume_role_external_id: String,
    /// Parsed and rejected (not silently ignored): object_store has no HTTP
    /// proxy support.
    pub proxy: Option<ProxyConfig>,
}

/// Default GCP upload target (`gcp:` block): a serialized service-account
/// credentials JSON plus the bucket.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct GcpConfig {
    pub credentials: String,
    pub bucket: String,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
}

/// Default Azure upload target (`azure:` block).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AzureConfig {
    pub account_name: String,
    pub account_key: String,
    pub container_name: String,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
}

/// AliOSS default (`alioss:` block). Parsed; uploads are rejected with a clear
/// error on the voice-only recorder.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AliOssConfig {
    pub access_key: String,
    pub secret: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
}

/// HTTP proxy for S3 uploads (Go `ProxyConfig`). Parsed and rejected: the
/// uploader has no proxy support.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ProxyConfig {
    pub url: String,
    pub username: String,
    pub password: String,
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
            gcp: None,
            azure: None,
            alioss: None,
            insecure: false,
            cpu_cost: None,
            s3_assume_role_key: String::new(),
            s3_assume_role_secret: String::new(),
            s3_assume_role_arn: String::new(),
            s3_assume_role_external_id: String::new(),
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
  content_disposition: attachment
  assume_role_arn: arn:aws:iam::123:role/uploader
s3_assume_role_key: base-key
s3_assume_role_secret: base-secret
gcp:
  credentials: '{"type":"service_account"}'
  bucket: gcp-recordings
azure:
  account_name: acct
  account_key: key
  container_name: recordings
alioss:
  access_key: a
  secret: s
  region: cn-hangzhou
  endpoint: https://oss-cn-hangzhou.aliyuncs.com
  bucket: alios-bucket
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
        assert_eq!(s3.content_disposition, "attachment");
        assert_eq!(s3.assume_role_arn, "arn:aws:iam::123:role/uploader");
        assert_eq!(cfg.s3_assume_role_key, "base-key");
        let gcp = cfg.gcp.unwrap();
        assert_eq!(gcp.bucket, "gcp-recordings");
        let azure = cfg.azure.unwrap();
        assert_eq!(azure.container_name, "recordings");
        let alioss = cfg.alioss.unwrap();
        assert_eq!(alioss.bucket, "alios-bucket");
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
