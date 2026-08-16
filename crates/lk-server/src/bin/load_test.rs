//! Signaling load-test harness.
//!
//! Connects a number of WebSocket clients to a LiveKit-compatible server
//! (either this Rust server or the reference Go server) and reports join
//! latency, signal round-trip time, and join throughput. Both servers speak
//! the same wire protocol, so the results are directly comparable.
//!
//! Usage:
//! ```text
//! cargo run -p lk-server --bin load_test --release -- \
//!     --target ws://127.0.0.1:7880 \
//!     --key devkey \
//!     --secret secret \
//!     --clients 100 --rooms 20 --duration 5
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;

/// Smallest / largest reported latencies.
#[derive(Default, Clone)]
struct Stats {
    count: u64,
    total_us: u64,
    samples: Vec<u64>, // in microseconds, for percentiles
}

impl Stats {
    fn record(&mut self, us: u64) {
        self.count += 1;
        self.total_us += us;
        self.samples.push(us);
    }

    fn summarize(&self, label: &str) {
        if self.samples.is_empty() {
            println!("  {label:<24} n=0");
            return;
        }
        let mut s = self.samples.clone();
        s.sort_unstable();
        let p = |q: f64| {
            let idx = (((s.len() as f64) * q).floor() as usize).min(s.len() - 1);
            s[idx]
        };
        let avg = self.total_us as f64 / self.count as f64;
        println!(
            "  {label:<24} n={:<6} avg={:>8.1}us p50={:>7}us p95={:>7}us p99={:>7}us max={:>7}us",
            self.count,
            avg,
            p(0.50),
            p(0.95),
            p(0.99),
            p(1.0),
        );
    }
}

/// HS256 join token for a room.
fn join_token(key: &str, secret: &str, identity: &str, room: &str) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": key,
        "sub": identity,
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

struct Args {
    target: String,
    key: String,
    secret: String,
    clients: usize,
    rooms: usize,
    duration: u64,
    scenario: String,
}

fn parse_args() -> Args {
    let mut args = Args {
        target: "ws://127.0.0.1:7880".to_string(),
        key: "devkey".to_string(),
        secret: "secret".to_string(),
        clients: 50,
        rooms: 10,
        duration: 5,
        scenario: "all".to_string(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut val = || it.next().expect("missing value");
        match a.as_str() {
            "--target" => args.target = val(),
            "--key" => args.key = val(),
            "--secret" => args.secret = val(),
            "--clients" => args.clients = val().parse().unwrap(),
            "--rooms" => args.rooms = val().parse().unwrap(),
            "--duration" => args.duration = val().parse().unwrap(),
            "--scenario" => args.scenario = val(),
            "--help" | "-h" => {
                println!("usage: load_test --target <ws-url> --key <k> --secret <s> --clients <n> --rooms <n> --duration <secs> --scenario <all|join|rtt|throughput>");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

/// Connects a client and waits for the JoinResponse, returning the join
/// latency and a live websocket.
async fn join_once(
    target: &str,
    token: &str,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        u128,
    ),
    String,
> {
    let url = format!("{target}/rtc?access_token={token}");
    let t0 = Instant::now();
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| e.to_string())?;
    let mut ws = ws;
    // First server frame must be the JoinResponse.
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Binary(bytes)))) => {
                let resp = lk::SignalResponse::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
                if matches!(resp.message, Some(lk::signal_response::Message::Join(_))) {
                    return Ok((ws, t0.elapsed().as_micros()));
                }
            }
            Ok(Some(Ok(Message::Text(text)))) => {
                let resp: lk::SignalResponse =
                    serde_json::from_str(&text).map_err(|e| e.to_string())?;
                if matches!(resp.message, Some(lk::signal_response::Message::Join(_))) {
                    return Ok((ws, t0.elapsed().as_micros()));
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(e.to_string()),
            Ok(None) => return Err("connection closed".to_string()),
            Err(_) => return Err("join timeout".to_string()),
        }
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    println!("== Load test ==");
    println!(
        "target={} clients={} rooms={} duration={}s",
        args.target, args.clients, args.rooms, args.duration
    );

    // --- Scenario 1: join latency ---
    let mut failures = 0u64;
    if args.scenario != "all" && args.scenario != "join" {
        println!("\n[1] join latency skipped");
    } else {
        let mut join_stats = Stats::default();
        let mut tasks = Vec::new();
        for i in 0..args.clients {
            let target = args.target.clone();
            let token = join_token(
                &args.key,
                &args.secret,
                &format!("bench-{i}"),
                &format!("room-{}", i % args.rooms),
            );
            tasks.push(tokio::spawn(
                async move { join_once(&target, &token).await },
            ));
        }
        for t in tasks {
            match t.await.unwrap() {
                Ok((_ws, us)) => join_stats.record(us as u64),
                Err(_) => failures += 1,
            }
        }
        println!("\n[1] Concurrent join latency (all clients in parallel)");
        join_stats.summarize("time-to-JoinResponse");
        println!("  failures={failures}");
    }

    // --- Scenario 2: signal RTT (ping/pong) ---
    if args.scenario != "all" && args.scenario != "rtt" {
        println!("\n[2] RTT skipped");
    } else {
        let mut rtt_stats = Stats::default();
        let mut rtt_throughput = 0u64;
        let mut tasks = Vec::new();
        for i in 0..args.clients {
            let target = args.target.clone();
            let token = join_token(
                &args.key,
                &args.secret,
                &format!("rtt-{i}"),
                &format!("room-{}", i % args.rooms),
            );
            tasks.push(tokio::spawn(async move {
                let (mut ws, _) = join_once(&target, &token).await?;
                let mut stats = Stats::default();
                let mut count = 0u64;
                let deadline = Instant::now() + Duration::from_secs(args.duration);
                while Instant::now() < deadline {
                    // Send one ping and wait for its pong (local microsecond RTT).
                    let sent_at = Instant::now();
                    ws.send(Message::Binary(
                        lk::SignalRequest {
                            message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                                timestamp: lk_server::core::unix_micros() / 1000,
                                rtt: 0,
                            })),
                        }
                        .encode_to_vec()
                        .into(),
                    ))
                    .await
                    .map_err(|e| e.to_string())?;
                    let got = tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            match ws.next().await {
                                Some(Ok(Message::Binary(bytes))) => {
                                    let resp = lk::SignalResponse::decode(bytes.as_ref())
                                        .map_err(|e| e.to_string())?;
                                    match resp.message {
                                        Some(lk::signal_response::Message::PongResp(_)) => {
                                            return Ok::<(), String>(())
                                        }
                                        Some(lk::signal_response::Message::Trickle(_)) => continue,
                                        _ => continue,
                                    }
                                }
                                Some(Ok(Message::Text(text))) => {
                                    let resp: lk::SignalResponse =
                                        serde_json::from_str(&text).map_err(|e| e.to_string())?;
                                    if matches!(
                                        resp.message,
                                        Some(lk::signal_response::Message::PongResp(_))
                                    ) {
                                        return Ok(());
                                    }
                                }
                                _ => return Err("no pong".to_string()),
                            }
                        }
                    })
                    .await;
                    if let Ok(Ok(_)) = got {
                        stats.record(sent_at.elapsed().as_micros() as u64);
                        count += 1;
                    }
                }
                Ok::<(Stats, u64), String>((stats, count))
            }));
        }
        for t in tasks {
            match t.await.unwrap() {
                Ok((stats, count)) => {
                    rtt_stats.samples.extend(stats.samples);
                    rtt_stats.count += stats.count;
                    rtt_stats.total_us += stats.total_us;
                    rtt_throughput += count;
                }
                Err(_) => failures += 1,
            }
        }
        println!(
            "\n[2] Signal round-trip (PingReq -> PongResp) over {}s",
            args.duration
        );
        rtt_stats.summarize("RTT");
        let conns = (args.clients - failures as usize).max(1);
        println!(
            "  total pongs={}  pong rate={:.0}/s across {conns} connections",
            rtt_throughput,
            rtt_throughput as f64 / args.duration as f64
        );
    }

    // --- Scenario 3: join throughput (join + leave loop) ---
    if args.scenario != "all" && args.scenario != "throughput" {
        println!("\n[3] throughput skipped");
    } else {
        let joins = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        for i in 0..args.clients {
            let target = args.target.clone();
            let key = args.key.clone();
            let secret = args.secret.clone();
            let joins = joins.clone();
            tasks.push(tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(args.duration);
                let mut n = 0u64;
                let mut room_idx = i;
                while Instant::now() < deadline {
                    let token = join_token(
                        &key,
                        &secret,
                        &format!("tp-{i}-{n}"),
                        &format!("tp-room-{}", room_idx % 10),
                    );
                    if let Ok((mut ws, _)) = join_once(&target, &token).await {
                        // Send leave and let the server tear down.
                        let _ = ws
                            .send(Message::Binary(
                                lk::SignalRequest {
                                    message: Some(lk::signal_request::Message::Leave(
                                        lk::LeaveRequest {
                                            reason: lk::DisconnectReason::ClientInitiated as i32,
                                            ..Default::default()
                                        },
                                    )),
                                }
                                .encode_to_vec()
                                .into(),
                            ))
                            .await;
                        let _ = ws.close(None).await;
                        n += 1;
                        room_idx += 1;
                    }
                }
                joins.fetch_add(n, Ordering::Relaxed);
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
        let total = joins.load(Ordering::Relaxed);
        println!("\n[3] Join/leave throughput over {}s", args.duration);
        println!(
            "  joins={total}  rate={:.0} joins/s",
            total as f64 / args.duration as f64
        );
    }

    println!("\n== done ==");
}
