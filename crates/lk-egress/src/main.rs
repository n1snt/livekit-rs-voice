//! `livekit-egress`: a voice-only, drop-in replacement for the
//! `livekit/egress` container. Hosts the psrpc `EgressInternal` service and
//! records room audio to WAV/MP3 files.

use std::sync::Arc;

use lk_psrpc::{RedisBus, RedisConfig};
use serde_json::{json, Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
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

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.effective_log_level()));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(JsonEventFormatter)
        .init();

    // Go egress keys with no voice-only-recorder equivalent: accepted, unused.
    if config.insecure {
        tracing::warn!(
            "insecure=true accepted but unused (it only affects the Go egress's web/Chrome path)"
        );
    }
    if let Some(cpu) = &config.cpu_cost {
        tracing::info!(
            room_composite_cpu_cost = cpu.room_composite_cpu_cost,
            "cpu_cost accepted but unused (voice-only recorder has no job-admission gating)"
        );
    }

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

/// Renders events as JSON with a lowercase `level`, matching the reference zap
/// logs so the promtail json stage extracts a `level` label (identical to the
/// `livekit-voice` server's formatter).
#[derive(Default)]
struct JsonEventFormatter;

/// Collects an event's fields into a JSON object.
struct FieldCollector<'a>(&'a mut Map<String, Value>);

impl Visit for FieldCollector<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().into(), Value::String(value.into()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), Value::String(format!("{value:?}")));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), Value::Bool(value));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0
            .insert(field.name().into(), Value::String(value.to_string()));
    }
}

impl<S, N> FormatEvent<S, N> for JsonEventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut fields = Map::new();
        event.record(&mut FieldCollector(&mut fields));
        let message = fields.remove("message").unwrap_or(Value::Null);
        let level = match *event.metadata().level() {
            tracing::Level::TRACE => "trace",
            tracing::Level::DEBUG => "debug",
            tracing::Level::INFO => "info",
            tracing::Level::WARN => "warn",
            tracing::Level::ERROR => "error",
        };
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut obj = Map::new();
        obj.insert("level".into(), Value::String(level.into()));
        obj.insert("ts".into(), json!(ts));
        obj.insert(
            "target".into(),
            Value::String(event.metadata().target().into()),
        );
        obj.insert("msg".into(), message);
        obj.extend(fields);
        writeln!(writer, "{}", Value::Object(obj))
    }
}
