//! `livekit-voice`: a drop-in, voice-only LiveKit-compatible server.

use std::sync::Arc;

use lk_server::config::{load_config_from_yaml, Config};
use lk_server::http;
use lk_server::server::Server;
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
        .with_target(false)
        .init();
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
