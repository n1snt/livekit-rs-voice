# Benchmark: `livekit-rs-voice` vs Go `livekit-server`

Signaling performance of the Rust voice-only server against the reference Go `livekit-server`, measured on the same host.

**Summary:** for the signaling-heavy voice workload, the Rust server joins calls **~19x faster** (p50), sustains **~17x more joins/s**, uses **~2x less memory per connection**, and does **~2x more pings and ~7x more joins per CPU-second**.

## Environment

| | |
|---|---|
| Host | MacBook Pro (Apple M1), 32 GB RAM, 1 TB SSD, loopback networking |
| Rust server | `livekit-rs-voice`, `--release` (LTO, stripped), natively on the host |
| Go server | `livekit/livekit-server` v1.11.0, in Docker (Redis signaling relay) |
| Load | `load_test` binary, real WebSocket clients, 200 clients / 100 rooms |

> The Go server runs in Docker with a Redis relay and full multi-node capability; the Rust server is single-node and in-process in these runs. That tradeoff favors the Rust server on latency, but in-process state is not crash-durable, and multi-node (`redis.cluster: true`) puts Redis back in the request path with more basic failover than Go's. These numbers reflect the single-node topology.

## Headline numbers

| Metric | Rust | Go |
|---|---|---|
| Idle memory | **~6 MB**, ~0% CPU | ~12 MB anon, ~1.2% CPU |
| Memory, 200 stable connections | **~66 MB** (~330 KB/conn) | ~136 MB anon (~680 KB/conn) |
| Join latency (p50) | **14.5 ms** | 279 ms |
| Signal RTT (p50) | **1.27 ms** | 5.10 ms |
| Join/leave throughput | **12,199 joins/s** | 721 joins/s |

## Latency & throughput (sequential, warmed)

| Scenario | Rust | Go | Speedup |
|---|---|---|---|
| Join latency, p50 | **14.5 ms** | 279 ms | **19x** |
| Join latency, avg | **30.5 ms** | 291 ms | **9.5x** |
| Signal RTT, p50 | **1.27 ms** | 5.10 ms | **4.0x** |
| Pong rate | **142.8 k/s** | 36.2 k/s | **3.9x** |
| Joins/s (churn) | **12,199** | 721 | **16.9x** |

## Memory & CPU (both servers loaded simultaneously)

| Metric | Rust | Go | Ratio |
|---|---|---|---|
| Pongs per CPU-second | **~54 k** | ~30 k | **1.8x** |
| Joins per CPU-second | **~1,470** | ~199 | **7.4x** |
| Memory per connection | **~330 KB** | ~680 KB | **2.1x less** |

More rooms widen the gap: Go's per-room Redis allocation makes join latency jump from ~111 ms at 20 rooms to ~279 ms at 100 rooms, while the Rust server stays in the 10-15 ms range.

## Multi-node: Rust 2-node cluster vs Go livekit-server

The Rust server is multi-node over Redis (`redis.cluster: true`): rooms are claimed by exactly one node and signaling is transparently relayed to the hosting node. Compared here at the same per-node load: the 2-node cluster handles 200 clients (100 per node) sharing 100 rooms — so roughly half the joins relay cross-node — while the Go server handles 100 clients on its single node.

| Metric | Rust 2-node cluster | Go livekit-server |
|---|---|---|
| Join latency (p50) | **~30 ms** | 319 ms |
| Signal RTT (p50) | **~1.2 ms** | 3.3 ms |
| Pong rate | **~140 k/s** | 29 k/s |
| Join throughput (per node) | **~10.5 k & 8.8 k joins/s** | 590 joins/s |
| Join throughput (cluster aggregate) | **~19 k joins/s** | — |

At the same per-node load the cluster joins calls **~10x faster** and serves **~4.8x more pings** than the Go server, cross-node relay overhead included, with 0 failures.

## Reproducing

```bash
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core

cargo build --release -p lk-server --bin livekit-voice --bin load_test
./target/release/load_test --target ws://127.0.0.1:7880 \
  --key devkey --secret secret --clients 200 --rooms 100 --duration 8
```

`load_test` supports `--scenario all|join|rtt|throughput`. Memory/CPU can be measured side by side with `scripts/measure_resources.py`.
