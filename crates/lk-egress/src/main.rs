//! `livekit-egress`: a voice-only, drop-in replacement for the
//! `livekit/egress` container. Hosts the psrpc `EgressInternal` service and
//! records room audio to WAV/MP3 files.

use std::sync::Arc;

use lk_psrpc::{RedisBus, RedisConfig};
use tracing_subscriber::EnvFilter;

use lk_egress::config::{load_config_from_yaml, EgressConfig};
use lk_egress::io::IoClient;
use lk_egress::server::EgressServer;

fn parse_args() -> (Option<String>, Option<String>, bool) {
    let mut args = (None, None, false);
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => args.0 = iter.next(),
            "--config-body" => args.1 = iter.next(),
            "--dev" => args.2 = true,
            "--help" | "-h" => {
                println!("livekit-egress: voice-only recorder");
                println!("Usage: livekit-egress [--config <path>] [--config-body <yaml>] [--dev]");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

fn load_config(
    path: Option<String>,
    body: Option<String>,
    dev: bool,
) -> Result<EgressConfig, String> {
    if let Some(body) = body {
        return load_config_from_yaml(&body);
    }
    if let Some(path) = path {
        let yaml = std::fs::read_to_string(&path)
            .map_err(|e| format!("failed to read config {path}: {e}"))?;
        return load_config_from_yaml(&yaml);
    }
    let mut config = EgressConfig::default();
    if dev {
        config.api_key = "devkey".to_string();
        config.api_secret = "secret".to_string();
        config.output_dir = "/tmp".to_string();
        config.redis = RedisConfig {
            address: "127.0.0.1:6379".to_string(),
            ..Default::default()
        };
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config_path, config_body, dev) = parse_args();
    let config = load_config(config_path, config_body, dev)?;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    if config.redis.address.is_empty() {
        return Err("redis is required (psrpc bus to the livekit-voice server)".into());
    }
    if config.api_secret.is_empty() || config.ws_url.is_empty() {
        return Err("api_key/api_secret and ws_url are required".into());
    }
    std::fs::create_dir_all(&config.output_dir)
        .map_err(|e| format!("output_dir {}: {e}", config.output_dir))?;

    let bus: Arc<dyn lk_psrpc::PsrpcBus> = Arc::new(RedisBus::new(&config.redis));
    let io = IoClient::new(bus.clone()).await?;
    let server = EgressServer::new(bus, config.clone(), io).await?;
    let _ = server;
    tracing::info!(output_dir = %config.output_dir, "livekit-egress started");

    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    Ok(())
}
