//! `livekit-egress` configuration (YAML).

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
    /// Where recordings are written.
    pub output_dir: String,
    /// Shared Redis (the psrpc bus the livekit-voice server uses).
    pub redis: RedisConfig,
    /// MP3 bitrate in kbps (default 64).
    pub mp3_bitrate: i32,
    /// Prometheus metrics port (0 disables).
    pub prometheus_port: u16,
    /// Logging level.
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
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
        }
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
    }
}
