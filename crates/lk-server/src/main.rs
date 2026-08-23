//! `livekit-voice`: a drop-in, voice-only LiveKit-compatible server.

use std::sync::Arc;

use lk_server::config::{load_config_from_yaml, Config};
use lk_server::http;
use lk_server::server::Server;
use serde_json::{json, Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    let env_config = std::env::var("LIVEKIT_CONFIG").ok();
    let config = match (&args.config_body, &args.config, env_config) {
        (Some(body), _, _) => load_config_from_yaml(body)?,
        (None, _, Some(body)) => load_config_from_yaml(&body)?,
        (None, Some(path), _) => {
            let yaml = std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read config {path}: {e}"))?;
            load_config_from_yaml(&yaml)?
        }
        (None, None, _) => {
            // Dev-mode defaults, mirroring the reference `--dev` behaviour.
            let mut config = Config::default();
            if args.dev {
                config.dev = true;
                config
                    .keys
                    .insert("devkey".to_string(), "secret".to_string());
            }
            config
        }
    };

    init_logging(&config);

    let server = Server::new(config);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_server(server))?;
    Ok(())
}

struct Args {
    config: Option<String>,
    config_body: Option<String>,
    dev: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        config: None,
        config_body: None,
        dev: false,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => args.config = iter.next(),
            "--config-body" => args.config_body = iter.next(),
            "--dev" => args.dev = true,
            "--help" | "-h" => {
                println!("livekit-voice: drop-in voice-only LiveKit server");
                println!("Usage: livekit-voice [--config <path>] [--config-body <yaml>] [--dev]");
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

fn init_logging(config: &Config) {
    let level = if config.logging.level.is_empty() {
        "info"
    } else {
        config.logging.level.as_str()
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(JsonEventFormatter)
        .init();
}

/// Renders events as JSON with a lowercase `level`, matching the Go
/// `livekit-server`'s zap logs: the promtail json stage can extract a `level`
/// label, and alerts that match lowercase `error` behave like they did against
/// Go (uppercase `ERROR` never appears).
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

async fn run_server(server: Arc<Server>) -> Result<(), Box<dyn std::error::Error>> {
    server.start_background_tasks();
    let app = http::router(server.clone());
    let port = server.config.effective_port();
    let addrs: Vec<String> = if server.config.dev {
        vec!["127.0.0.1".to_string(), "::1".to_string()]
    } else if !server.config.bind_addresses.is_empty() {
        server.config.bind_addresses.clone()
    } else {
        vec!["0.0.0.0".to_string()]
    };
    let bind_addr = addrs
        .first()
        .cloned()
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let listener = tokio::net::TcpListener::bind(format!("{bind_addr}:{port}")).await?;
    tracing::info!(node_id = %server.node_id, port, "livekit-voice server started");

    let prometheus = server.config.prometheus_port.or_else(|| {
        if server.config.prometheus.port != 0 {
            Some(server.config.prometheus.port)
        } else {
            None
        }
    });

    if let Some(prom_port) = prometheus {
        let prom_app = Router::new().route(
            "/metrics",
            axum::routing::get(metrics_handler).with_state(server.clone()),
        );
        let prom_listener =
            tokio::net::TcpListener::bind(format!("{bind_addr}:{prom_port}")).await?;
        tracing::info!(port = prom_port, "prometheus metrics listening");
        tokio::select! {
            _ = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()) => {},
            _ = axum::serve(prom_listener, prom_app).with_graceful_shutdown(shutdown_signal()) => {},
        }
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }
    // Remove this node from the cluster registry so its rooms can be reclaimed
    // immediately by other nodes (rather than after the heartbeat TTL).
    server.cluster.deregister().await;
    Ok(())
}

async fn metrics_handler(State(server): State<Arc<Server>>) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        server.context.metrics.render(),
    )
        .into_response()
}

use axum::extract::State;
use axum::Router;

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
