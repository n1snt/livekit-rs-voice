//! Configuration, compatible with LiveKit's `livekit-server` YAML config.
//!
//! A drop-in server must accept standard LiveKit YAML config files, so the
//! layout here mirrors the reference `pkg/config/config.go`
//! from the reference implementation, and unknown keys are ignored so newer
//! configs keep loading.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const DEFAULT_PORT: u16 = 7880;
pub const DEFAULT_RTC_TCP_PORT: u16 = 7881;
pub const DEFAULT_NODE_ID_PREFIX: &str = "LX";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub port: Option<u16>,
    pub bind_addresses: Vec<String>,
    pub rtc: RTCConfig,
    pub redis: RedisConfig,
    pub keys: BTreeMap<String, String>,
    pub logging: LoggingConfig,
    pub room: RoomConfig,
    pub webhook: WebhookConfig,
    pub prometheus_port: Option<u16>,
    #[serde(alias = "prometheus")]
    pub prometheus: PrometheusConfig,
    pub turn: TurnConfig,
    pub agents: AgentConfig,
    pub limit: LimitConfig,
    pub region: String,
    pub node_id: String,
    pub dev: bool,
    pub signal_relay: SignalRelayConfig,
    pub psrpc: PsrpcConfig,
    /// Legacy top-level `log_level` key (maps onto `logging.level`).
    pub log_level: Option<String>,
    /// Legacy `debug_handler` block (accepted and ignored).
    pub debug_handler: serde_yaml::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RTCConfig {
    /// UDP port for media. Defaults to `tcp_port` when unset (single port).
    pub udp_port: u16,
    /// TCP port for ICE/TCP fallback.
    pub tcp_port: u16,
    /// Inclusive start of the UDP media port range.
    pub port_range_start: u16,
    /// Inclusive end of the UDP media port range.
    pub port_range_end: u16,
    pub use_external_ip: bool,
    pub ips: RTCIPConfig,
    /// When set, only this IP is advertised (replaces host candidates).
    pub node_ip: String,
    pub packet_buffer_size_audio: usize,
    pub packet_buffer_size_video: usize,
    pub enable_datachannel_data_tracks: bool,
    pub congestion_control: CongestionControlConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RTCIPConfig {
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CongestionControlConfig {
    pub enabled: bool,
    pub allow_pause: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    pub address: String,
    pub username: String,
    pub password: String,
    pub db: i64,
    pub use_tls: bool,
    pub max_retries: i32,
    pub cluster: bool,
}

impl RedisConfig {
    pub fn is_configured(&self) -> bool {
        !self.address.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    pub pion_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoomConfig {
    pub auto_create: bool,
    pub empty_timeout: u32,
    pub departure_timeout: u32,
    pub max_participants: u32,
    pub create_room_timeout: u64,
    pub enabled_codecs: Vec<CodecConfig>,
    pub update_batch_target_size: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CodecConfig {
    pub mime: String,
    pub fmtp_line: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
    pub api_key: String,
    pub urls: Vec<String>,
    pub notify_agent_events: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrometheusConfig {
    pub port: u16,
    pub ignore_tags: Vec<String>,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TurnConfig {
    pub enabled: bool,
    pub domain: String,
    pub udp_port: u16,
    pub tls_port: u16,
    pub cert_file: String,
    pub key_file: String,
    pub ttl: u64,
    pub use_external_ip: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    pub target_load: f32,
    pub default_name: String,
    pub room_auto_join: bool,
    pub namespace: String,
    pub max_agents: u32,
    pub agent_signal_message_size_limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitConfig {
    #[serde(alias = "max_metadata_size")]
    pub max_metadata: usize,
    #[serde(alias = "max_attributes_size")]
    pub max_attributes: usize,
    pub max_room_name_length: usize,
    pub max_participant_name_length: usize,
    pub max_identity_length: usize,
    pub max_agent_name_length: usize,
    #[serde(alias = "max_data_blobs_size")]
    pub max_data_blob_size: usize,
    pub signal_message_size_limit: usize,
    pub agent_signal_message_size_limit: usize,
    pub max_api_request_body_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SignalRelayConfig {
    pub retry_timeout: u64,
    #[serde(alias = "min_retry_interval")]
    pub min_retry: u64,
    #[serde(alias = "max_retry_interval")]
    pub max_retry: u64,
    pub stream_buffer_size: usize,
    pub connect_attempts: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PsrpcConfig {
    pub enabled: bool,
}

impl Default for RTCConfig {
    fn default() -> Self {
        RTCConfig {
            udp_port: 0,
            tcp_port: DEFAULT_RTC_TCP_PORT,
            port_range_start: 0,
            port_range_end: 0,
            use_external_ip: false,
            ips: RTCIPConfig::default(),
            node_ip: String::new(),
            packet_buffer_size_audio: 200,
            packet_buffer_size_video: 500,
            enable_datachannel_data_tracks: true,
            congestion_control: CongestionControlConfig {
                enabled: true,
                allow_pause: false,
            },
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            level: "info".to_string(),
            pion_level: "error".to_string(),
        }
    }
}

impl Default for RoomConfig {
    fn default() -> Self {
        RoomConfig {
            auto_create: true,
            empty_timeout: 300,
            departure_timeout: 20,
            max_participants: 0,
            create_room_timeout: 10,
            enabled_codecs: default_codecs(),
            update_batch_target_size: 128 * 1024,
        }
    }
}

impl Default for TurnConfig {
    fn default() -> Self {
        TurnConfig {
            enabled: false,
            domain: String::new(),
            udp_port: 5349,
            tls_port: 0,
            cert_file: String::new(),
            key_file: String::new(),
            ttl: 300,
            use_external_ip: false,
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            target_load: 0.7,
            default_name: String::new(),
            room_auto_join: false,
            namespace: String::new(),
            max_agents: 0,
            agent_signal_message_size_limit: 2 * 1024 * 1024,
        }
    }
}

impl Default for LimitConfig {
    fn default() -> Self {
        LimitConfig {
            max_metadata: 512 * 1024,
            max_attributes: 64 * 1024,
            max_room_name_length: 256,
            max_participant_name_length: 256,
            max_identity_length: 256,
            max_agent_name_length: 256,
            max_data_blob_size: 64_000,
            signal_message_size_limit: 2 * 1024 * 1024,
            agent_signal_message_size_limit: 2 * 1024 * 1024,
            max_api_request_body_size: 10 * 1024 * 1024,
        }
    }
}

impl Default for SignalRelayConfig {
    fn default() -> Self {
        SignalRelayConfig {
            retry_timeout: 7500,
            min_retry: 500,
            max_retry: 4000,
            stream_buffer_size: 1000,
            connect_attempts: 3,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: Some(DEFAULT_PORT),
            bind_addresses: vec![],
            rtc: RTCConfig::default(),
            redis: RedisConfig::default(),
            keys: BTreeMap::new(),
            logging: LoggingConfig::default(),
            room: RoomConfig::default(),
            webhook: WebhookConfig::default(),
            prometheus_port: None,
            prometheus: PrometheusConfig::default(),
            turn: TurnConfig::default(),
            agents: AgentConfig::default(),
            limit: LimitConfig::default(),
            region: String::new(),
            node_id: String::new(),
            dev: false,
            signal_relay: SignalRelayConfig::default(),
            psrpc: PsrpcConfig::default(),
            log_level: None,
            debug_handler: serde_yaml::Value::Null,
        }
    }
}

fn default_codecs() -> Vec<CodecConfig> {
    ["audio/opus", "audio/red", "audio/PCMU", "audio/PCMA"]
        .iter()
        .map(|mime| CodecConfig {
            mime: mime.to_string(),
            fmtp_line: String::new(),
        })
        .collect()
}

impl Config {
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }

    /// Resolve the UDP media port. When only `rtc.tcp_port` is set (as in the
    /// project's dev configs), media reuses that single port.
    pub fn effective_udp_port(&self) -> u16 {
        if self.rtc.udp_port != 0 {
            self.rtc.udp_port
        } else {
            self.rtc.tcp_port
        }
    }

    pub fn enabled_codec_mimes(&self) -> Vec<&str> {
        self.room
            .enabled_codecs
            .iter()
            .map(|c| c.mime.as_str())
            .collect()
    }

    pub fn is_codec_enabled(&self, mime: &str) -> bool {
        self.enabled_codec_mimes()
            .iter()
            .any(|m| m.eq_ignore_ascii_case(mime))
    }
}

/// Loads the config from a YAML file and applies environment-variable
/// overrides (same scheme as the reference server).
pub fn load_config_from_yaml(yaml: &str) -> Result<Config, String> {
    let mut config =
        serde_yaml::from_str::<Config>(yaml).map_err(|e| format!("failed to parse config: {e}"))?;
    // Legacy top-level `log_level` overrides the nested logging config.
    if let Some(level) = config.log_level.take() {
        config.logging.level = level;
    }
    Ok(apply_env_overrides(config))
}

fn apply_env_overrides(mut config: Config) -> Config {
    if let Ok(value) = std::env::var("LIVEKIT_REDIS_ADDRESS") {
        config.redis.address = value;
    }
    if let Ok(value) = std::env::var("LIVEKIT_RTC_TCP_PORT") {
        if let Ok(v) = value.parse() {
            config.rtc.tcp_port = v;
        }
    }
    if let Ok(value) = std::env::var("LIVEKIT_LOG_LEVEL") {
        config.logging.level = value;
    }
    if let Ok(value) = std::env::var("LIVEKIT_KEYS") {
        match serde_yaml::from_str::<BTreeMap<String, String>>(&value) {
            Ok(keys) => config.keys = keys,
            Err(e) => eprintln!("warning: failed to parse LIVEKIT_KEYS: {e}"),
        }
    }
    if let Ok(port) = std::env::var("LIVEKIT_PORT") {
        config.port = port.parse().ok();
    }
    if let Ok(region) = std::env::var("LIVEKIT_REGION") {
        config.region = region;
    }
    if let Ok(node_ip) = std::env::var("NODE_IP") {
        config.rtc.node_ip = node_ip;
    }
    if let Ok(udp_port) = std::env::var("UDP_PORT") {
        config.rtc.udp_port = udp_port.parse().unwrap_or(0);
    }
    config
}

/// Derives the node id, matching the reference server's prefix + random suffix.
pub fn node_id(prefix: Option<&str>) -> String {
    let prefix = prefix.unwrap_or(DEFAULT_NODE_ID_PREFIX).to_uppercase();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_project_dev_config() {
        let yaml = r#"
port: 7880
log_level: debug
rtc:
  udp_port: 7881
  tcp_port: 7882
  use_external_ip: false
redis:
  address: 127.0.0.1:6379
keys:
  devkey: secret
webhook:
  api_key: devkey
  urls:
    - http://127.0.0.1:8000/livekit/webhook
"#;
        let config = load_config_from_yaml(yaml).unwrap();
        assert_eq!(config.effective_port(), 7880);
        assert_eq!(config.rtc.udp_port, 7881);
        assert_eq!(config.rtc.tcp_port, 7882);
        assert_eq!(config.redis.address, "127.0.0.1:6379");
        assert_eq!(config.keys.len(), 1);
        assert_eq!(config.webhook.urls.len(), 1);
        // log_level is a top-level legacy key, must be tolerated (ignored).
        assert_eq!(config.room.empty_timeout, 300);
    }

    #[test]
    fn parses_prod_config_with_ignored_unknowns() {
        let yaml = r#"
port: 7880
log_level: info
prometheus_port: 6789
rtc:
  tcp_port: 7881
  port_range_start: 50000
  port_range_end: 60000
  use_external_ip: false
  ips:
    includes:
      - 203.0.113.10/32
      - 192.0.2.10/32
redis:
  address: 127.0.0.1:6379
keys:
  lk_key: lk_secret
webhook:
  api_key: lk_key
  urls:
    - https://example.com/livekit/webhook
turn:
  enabled: true
  udp_port: 443
"#;
        let config = load_config_from_yaml(yaml).unwrap();
        assert_eq!(config.prometheus_port, Some(6789));
        assert_eq!(config.rtc.port_range_start, 50000);
        assert_eq!(config.rtc.port_range_end, 60000);
        assert_eq!(config.rtc.ips.includes.len(), 2);
        assert_eq!(config.effective_udp_port(), 7881); // udp_port unset -> falls back to tcp_port
        assert!(config.turn.enabled);
    }

    #[test]
    fn default_config_matches_reference_defaults() {
        let config = Config::default();
        assert_eq!(config.effective_port(), 7880);
        assert_eq!(config.rtc.tcp_port, 7881);
        assert_eq!(config.room.empty_timeout, 300);
        assert_eq!(config.room.departure_timeout, 20);
        assert!(config.room.auto_create);
        assert_eq!(config.limit.signal_message_size_limit, 2 * 1024 * 1024);
    }

    #[test]
    fn partial_nested_blocks_merge_with_defaults() {
        // A partial `rtc:` block must not reset sibling fields to zero.
        let yaml = r#"
port: 7880
rtc:
  tcp_port: 7000
keys:
  k: s
"#;
        let config = load_config_from_yaml(yaml).unwrap();
        assert_eq!(config.rtc.tcp_port, 7000);
        // Defaults preserved:
        assert_eq!(config.rtc.packet_buffer_size_audio, 200);
        assert!(config.rtc.enable_datachannel_data_tracks);
        assert_eq!(config.turn.ttl, 300);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.room.empty_timeout, 300);
        assert_eq!(config.limit.signal_message_size_limit, 2 * 1024 * 1024);
    }

    #[test]
    fn log_level_legacy_overrides_logging() {
        let yaml = "log_level: debug\nkeys:\n  k: s\n";
        let config = load_config_from_yaml(yaml).unwrap();
        assert_eq!(config.logging.level, "debug");
    }

    #[test]
    fn go_compatible_limit_aliases_accepted() {
        let yaml = r#"
keys:
  k: s
limit:
  max_metadata_size: 123
  max_data_blobs_size: 456
signal_relay:
  min_retry_interval: 1
  max_retry_interval: 2
"#;
        let config = load_config_from_yaml(yaml).unwrap();
        assert_eq!(config.limit.max_metadata, 123);
        assert_eq!(config.limit.max_data_blob_size, 456);
        assert_eq!(config.signal_relay.min_retry, 1);
        assert_eq!(config.signal_relay.max_retry, 2);
    }

    #[test]
    fn env_overrides_apply() {
        std::env::set_var("LIVEKIT_PORT", "9999");
        let config = Config::default();
        let config = apply_env_overrides(config);
        assert_eq!(config.effective_port(), 9999);
        std::env::remove_var("LIVEKIT_PORT");
    }
}
