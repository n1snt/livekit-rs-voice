# Benchmark: `livekit-rs-voice` vs Go `livekit-server`

Signaling performance of the Rust voice-only server against the reference Go
`livekit-server`, measured on the same host.

**Summary:** for the signaling-heavy voice workload, the Rust server joins
**~7-10x faster**, uses **~2.6x less memory per connection**, and does
**5-23x more work per CPU-second**.

## Environment

| | |
|---|---|
| Host | MacBook Pro (Apple M1), 32 GB RAM, 1 TB SSD, loopback networking |
| Rust server | `livekit-rs-voice`, `--release`, natively on the host |
| Go server | `livekit/livekit-server` v1.11.0, in Docker (Redis signaling relay) |
| Load | `load_test` binary, real WebSocket clients, identical scenarios on both |

> The Go server runs in Docker with a Redis relay in its signal path; the Rust
> server is single-node and in-process. That difference is real and favors the
> Rust server for this project's single-node deployment.

## Headline numbers

| Metric | Rust | Go |
|---|---|---|
| Idle memory | **~5 MB**, ~0% CPU | ~15 MB, ~1.4% CPU |
| Memory, 200 stable connections | **~69 MB** (~330 KB/conn) | ~190 MB (~870 KB/conn) |
| Signal throughput | **~58 k pongs/CPU-s** | ~7.8 k pongs/CPU-s |
| Join/leave throughput | **~2.9 k joins/CPU-s** | ~129 joins/CPU-s |

## Load tests (200 clients, 20 rooms)

| Scenario | Rust | Go | Speedup |
|---|---|---|---|
| Join latency, p50 | **10.4 ms** | 110.9 ms | **10.7x** |
| Join latency, avg | **16.3 ms** | 118.7 ms | **7.3x** |
| Signal RTT, p50 | **1.28 ms** | 7.12 ms | **5.6x** |
| Pong rate (aggregate) | **142.7 k/s** | 26.3 k/s | **5.4x** |
| Joins/s (churn) | **9,507** | 941 | **10.1x** |

## Reproducing

```bash
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core

cargo build --release -p lk-server --bin livekit-voice --bin load_test
./target/release/load_test --target ws://127.0.0.1:7880 \
  --key devkey --secret secret --clients 200 --rooms 20 --duration 8
```

`load_test` supports `--scenario all|join|rtt|throughput`.
