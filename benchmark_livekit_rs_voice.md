# Benchmark: `livekit-rs-voice` vs Go `livekit-server`

A detailed performance comparison of the Rust voice-only LiveKit server rewrite
(`livekit-rs-voice`) against the reference Go implementation
(`livekit/livekit-server`), measured on the same host.

**Summary:** for the signaling-heavy voice workload this project runs, the Rust
server is **~7-10x faster** for join/leave throughput, **~6-11x lower** join
latency, and **~5x lower** signaling RTT, while using **~2.6x less memory per
stable WebRTC connection** and **5-23x more work per CPU-second** (i.e. it is
both faster and more CPU-efficient). Serialization is in the tens-to-hundreds
of nanoseconds range.

Memory + CPU headline numbers (same host, both servers loaded identically):

| Metric | Rust `livekit-rs-voice` | Go `livekit-server` |
|---|---|---|
| Idle memory | **~5 MB**, ~0% CPU | ~15 MB (anon), ~1.4% CPU |
| Memory, 200 stable connections | **~69 MB** (~330 KB/conn) | ~190 MB (anon) (~870 KB/conn) |
| Signal throughput per CPU-second | **~58 k pongs/CPU-s** | ~7.8 k pongs/CPU-s |
| Join/leave throughput per CPU-second | **~2.9 k joins/CPU-s** | ~129 joins/CPU-s |

---

## 1. Methodology

Two complementary measurement layers:

1. **Micro-benchmarks** (Rust `criterion`): deterministic, in-process
   measurements of the hot paths: protobuf / protojson serialization, JWT
   verification, room/participant/track operations, audio-level detection,
   config parsing, and fan-out broadcasts. `cargo bench -p lk-proto --bench
   proto` and `cargo bench -p lk-server --bench core` (100 samples, 3 s
   measurement each; the "time" figures are the median of the criterion
   confidence interval).

2. **Load tests** (the `load_test` binary, `cargo run -p lk-server --bin
   load_test --release`): real WebSocket clients driven through the exact same
   wire protocol against each server. Three scenarios:
   - **Join latency**: 200 clients connect concurrently (20 rooms); the metric
     is time from TCP connect to the `JoinResponse` frame.
   - **Signal RTT + throughput**: 200 stable connections; each sends
     `PingReq` → `PongResp` round trips; the metric is local (microsecond)
     one-way RTT and aggregate pong rate.
   - **Join/leave throughput**: 200 clients loop join → leave for the
     duration; the metric is joins/s.

Both servers were warmed and the reported numbers are the cleaner sequential
run (parallel runs contend for CPU and inflate both sides).

### Environment

| | |
|---|---|
| Host | Apple Silicon macOS (darwin arm64), loopback networking |
| Rust server | `livekit-rs-voice` v0.1.0, `--release`, natively on the host |
| Go server | `livekit/livekit-server` **v1.11.0**, running in Docker (`livekit/livekit-server`) |
| Go topology | multi-node-capable: Redis-backed signaling relay (`psrpc`), room allocation and telemetry through Redis |
| Rust topology | single node, in-process room/session management (no Redis in the signal path) |
| Tokens | HS256 JWTs, identical claims for both (same dev key pair where possible) |

> **Fairness caveat.** The Go server runs in a Docker container and uses a
> Redis message bus for its signaling relay, which adds per-join overhead; the
> Rust server is single-node and has no such indirection. For this project's
> single-node deployment that difference is real and to the Rust server's
> advantage. Isolated in-process serialization numbers are provided so the
> comparison is not only "Docker vs native".

---

## 2. Micro-benchmarks (in-process, Rust only)

### 2.1 Wire serialization (`lk-proto`)

| Benchmark | Result |
|---|---|
| `protobuf/signal_response/encode` (full JoinResponse) | **706 ns** |
| `protobuf/signal_response/decode` | **1.90 µs** |
| `protobuf/signal_request/encode` (publisher offer) | **73 ns** |
| `protobuf/signal_request/decode` | **228 ns** |
| `protojson/signal_response/serialize` | **1.89 µs** |
| `protojson/signal_response/deserialize` | **5.87 µs** |
| `data_packet/encode` (128 B payload) | **78 ns** (≈2.5 GiB/s) |
| `data_packet/decode` | **325 ns** (≈620 MiB/s) |

A full `JoinResponse` (room + participant + 4 other participants + server info +
codecs) round-trips in well under 3 µs end-to-end. A single data packet
(128 B, the size of a typical transcription message) encodes in ~78 ns.

### 2.2 Server core (`lk-server`)

| Benchmark | Result |
|---|---|
| `jwt/verify_join_token` (HS256, full claims incl. `roomConfig.agents`) | **6.58 µs** |
| `room/to_proto` | **66 ns** |
| `room/broadcast_participant_update` (1 target) | **133 ns** |
| `participant/to_proto` | **301 ns** |
| `participant/track_info` | **159 ns** |
| `participant/set_attributes` (10 keys) | **1.33 µs** |
| `audio_level/observe` (per RTP packet) | **56 ns** |
| `audio_level/active_speakers` (16 participants) | **467 ns** |
| `config/parse_yaml` (prod-like config) | **12.3 µs** |
| `server/get_or_create_room` (existing room) | **25 ns** |
| `server/list_rooms` | **29 ns** |
| `broadcast/data_packet` (50 participants) | **3.15 µs** |
| `broadcast/participant_update` (50 participants) | **8.39 µs** |

Notes:

- Per-packet audio-level observation (used for active-speaker detection) is
  ~56 ns, essentially free at audio rates (50 packets/s/track).
- Fan-out of a data packet to 50 participants costs ~3 µs, i.e. ~63 ns per
  recipient.
- These are single-threaded numbers; the server runs multi-threaded
  (`tokio`), so aggregate throughput scales with cores.

---

## 3. Load tests (signaling)

### 3.1 Concurrent join latency: 200 clients, 20 rooms

Time from TCP connect to receiving the `JoinResponse`.

| Metric | Rust `livekit-rs-voice` | Go `livekit-server` | Speedup |
|---|---|---|---|
| p50 | **10.4 ms** | 110.9 ms | **10.7×** |
| p95 | **35.8 ms** | 246.4 ms | **6.9×** |
| p99 | **36.4 ms** | 341.4 ms | **9.4×** |
| max | **36.4 ms** | 349.7 ms | **9.6×** |
| avg | **16.3 ms** | 118.7 ms | **7.3×** |
| failures | 0 / 200 | 0 / 200 | n/a |

### 3.2 Signal RTT + throughput: 200 stable connections, 8 s

`PingReq → PongResp` round trips, measured locally in microseconds.

| Metric | Rust | Go | Speedup |
|---|---|---|---|
| RTT p50 | **1.28 ms** | 7.12 ms | **5.6×** |
| RTT p95 | **2.46 ms** | 12.7 ms | **5.2×** |
| RTT p99 | **3.62 ms** | 19.0 ms | **5.3×** |
| RTT max | **22.6 ms** | 60.5 ms | **2.7×** |
| RTT avg | **1.40 ms** | 7.60 ms | **5.4×** |
| pong rate (aggregate) | **142.7 k/s** | 26.3 k/s | **5.4×** |

> The ping path is the per-connection request/response loop that also carries
> ICE candidates, subscription updates and room metadata. It is the signalling
> hot path for a voice call.

### 3.3 Join/leave throughput: 200 clients, 8 s

| Metric | Rust | Go | Speedup |
|---|---|---|---|
| joins/s | **9,507** | 941 | **10.1×** |

This measures the full lifecycle cost (WS upgrade, JWT verify, room
allocations, participant + subscriber peer connection creation, teardown) under
sustained churn. The Go server pays a per-join Redis-relay + room-allocation
cost; the Rust server does everything in-process.

---

## 4. Memory & CPU footprint

### 4.1 Methodology

- **Rust server** (host process): sampled with `ps` for RSS and cumulative CPU
  time (`time=`); CPU rate = Δ(cumulative CPU) / Δ(wall clock).
- **Go server** (Docker container, cgroup v2): sampled via
  `/sys/fs/cgroup/{memory.stat,memory.current,cpu.stat}` for `anon` memory
  (heap, excludes page cache) and `usage_usec` (cumulative CPU); CPU rate =
  Δ(usage) / Δ(wall clock).
- Both servers were loaded **identically and simultaneously** (same
  `load_test` scenario, same concurrency), so each server contended for the
  same host CPUs and the comparison is apples-to-apples. CPU is reported both
  as **average utilization** (fraction of one core) and as
  **work-per-CPU-second** (ops ÷ CPU-seconds), which removes load-level
  differences entirely.
- The reproducible sampler lives at `scripts/measure_resources.py`.

### 4.2 Idle baseline (no connections)

| | Rust | Go |
|---|---|---|
| Memory | **~5 MB** (RSS) | ~15 MB (anon), ~22 MB mem.current |
| CPU | **~0%** | ~1.2-1.4% (Go runtime background activity) |

The Rust server's idle footprint is **~3× smaller** than the Go server's, and
it consumes no CPU at idle.

### 4.3 Stable load: N WebSocket connections, ping/pong loop

Each connection holds a subscriber peer connection (3 SCTP data channels) and
runs a ping/pong signalling loop; the load generator saturates each server with
the same traffic.

**Memory at 200 connections:**

| | Rust | Go | Ratio (Rust) |
|---|---|---|---|
| Memory | **69 MB** RSS (+34 MB) | 190 MB anon (+106 MB) | **2.7× less** |
| Per connection | **~330 KB** | ~870 KB | **2.6× less** |

**CPU at 200 connections (30 s run, both loaded):**

| | Rust | Go | Ratio (Rust) |
|---|---|---|---|
| CPU time used | **36.4 s** (~121% of a core) | 58.9 s (~196% of a core) | **1.6× less CPU** |
| Pongs completed | **2.12 M** | 0.46 M | 4.6× more work |
| **Throughput per CPU-second** | **58.3 k pongs/CPU-s** | 7.8 k pongs/CPU-s | **7.5×** |

At 50 connections: Rust **65 k pongs/CPU-s**, Go **10.5 k pongs/CPU-s**
(≈6× more efficient).

So under identical signalling load the Rust server not only completes ~4.6x
more round-trips, it does so **using less total CPU**, roughly
**7-8x more work per CPU-second**.

### 4.4 Join/leave churn: 50 clients, 30 s (throughput scenario)

Connection creation/teardown is the CPU-and-allocation-heavy path (ICE, DTLS,
SCTP setup/teardown per join).

| | Rust | Go | Ratio (Rust) |
|---|---|---|---|
| CPU time used | **34.3 s** (~114% of a core) | 114.2 s (~381% of a core) | **3.3× less CPU** |
| Joins completed | **101,028** | 14,748 | 6.9× more work |
| **Throughput per CPU-second** | **2,948 joins/CPU-s** | 129 joins/CPU-s | **22.9×** |
| Memory growth during churn | +1,618 MB (RSS) | +2,131 MB (anon) | comparable |
| Memory after churn (peak) | ~1.7 GB | ~2.3 GB | ~1.4× less |

The Go server spends **3.8 cores** on churn to move **6.9× fewer** joins than
the Rust server does on **1.1 cores**. The per-join cost (mostly its Redis
relay + room allocation) is ~23× higher.

### 4.5 Post-load settle (idle after churn)

| | Rust | Go |
|---|---|---|
| Memory 20 s after churn | ~1.45 GB RSS (stable) | ~2.32 GB anon (stable) |
| CPU idle | ~2.3% | ~2.0% |

**Interpretation.** Rapid WebRTC connection churn is allocation-heavy in both
servers and both plateau at a multi-GB working set; neither returns to baseline
immediately. The behavioural difference is that Go's GC eventually returns freed
heap to the OS (slowly), while the Rust server's system allocator retains it, so
its RSS floor stays elevated after a churn spike. For a telephony workload (calls
last minutes, not bursts of connect/disconnect), the steady-state numbers
(section 4.3) are the ones that matter, and there the Rust server is markedly
leaner. If OS-return behaviour is ever required, a release allocator
(jemalloc/mimalloc) or periodic `malloc_trim` restores it.

### 4.6 Summary

- **Idle:** Rust 5 MB / ~0% CPU vs Go 15 MB / ~1.4% CPU.
- **Stable:** Rust ~330 KB/conn and ~7.5× more signalling ops per CPU-second
  than Go's ~870 KB/conn.
- **Churn:** Rust ~23× more joins per CPU-second using ~1/3 the CPU.
- **Both** allocate heavily during churn; the difference is allocator retention
  vs GC return, not correctness.

---
---

## 5. Media plane

Real media is validated end-to-end by the integration test
`crates/lk-server/tests/media.rs`, which drives **two real WebRTC peers**
through the server:

1. a publisher connects, offers `sendonly` opus, completes ICE/DTLS, and sends
   RTP;
2. the server matches the incoming track by RTP stream id, creates the
   forwarder, and on a subscriber joining, auto-subscribes it and negotiates a
   per-subscriber down-track (distinct SSRC);
3. the subscriber receives the forwarded audio RTP.

The per-packet forwarding cost is dominated by the audio-level observation
(~56 ns/packet) and a single fan-out write; at 50 audio packets/s per track the
CPU cost is negligible.

> The Go server's media path was not benchmarked side-by-side here (it would
> require `livekit-cli` / browser clients and a TURN/STUN configuration); the
> signalling numbers above are the directly comparable apples-to-apples part.

---

## 6. Analysis

**Why the Rust server wins on signalling:**

- **No Redis in the request path.** The Go server relays every join and every
  signal message through its Redis message bus (room allocation, node
  selection, `psrpc` streams) even in a single-node deployment. The Rust server
  handles everything in-process with a hashmap + channels. For this project's
  single-node deployment the indirection buys nothing and costs per-message
  round-trips.
- **Lean serialization.** Protobuf encode/decode of a full `JoinResponse` is
  ~2.6 µs round-trip; the data path is hundreds of ns. WebSocket framing is the
  same in both.
- **Concurrency model.** `tokio` + small critical sections (no global lock on
  the room/session path) scales the join/leave throughput to ~9.5 k/s on a
  single node.

**Where the two are comparable:**

- Steady-state audio RTP forwarding is cheap in both; neither server mixes or
  transcodes audio.
- Under pathological connect/disconnect churn both servers grow to a multi-GB
  working set (CPU churn is the cost driver, not memory); the Rust server keeps
  the memory (malloc retention) while Go's GC returns some of it. Neither
  returns to baseline immediately.

**CPU efficiency is the headline.** Because both servers were driven with
self-saturating identical load, the work-per-CPU-second figures are load-
independent: **5-23x more work per CPU-second** (pongs and joins respectively).
A single-node voice deployment can serve more concurrent calls per core on the
Rust server than on the multi-node-capable Go server.

**Caveats to keep in mind:**

- The Go server is **v1.11.0 in Docker** with a Redis relay and full
  multi-node capability; the Rust server is **single-node** and voice-only
  (no video). A fair "feature-for-feature" comparison is not the point of this
  project; the point is that the exact API surface this project depends on can
  be served with materially better latency and throughput.
- Loopback networking keeps network jitter out of the picture; real WAN
  deployments would add a floor that shrinks the relative difference.

---

## 7. Reproducing

```bash
# Micro-benchmarks
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core

# Load tests
cargo build --release -p lk-server --bin livekit-voice --bin load_test

# Rust server (single-node)
./target/release/livekit-voice --config /tmp/lk-test.yaml

# Go server (the reference, in Docker)
# docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7882:7882 \
#   -v $PWD/livekit.yaml:/etc/livekit.yaml livekit/livekit-server --config /etc/livekit.yaml

# Point --key/--secret at the keys configured for each server
./target/release/load_test --target ws://127.0.0.1:7990 --key devkey --secret secret --clients 200 --rooms 20 --duration 8
./target/release/load_test --target ws://127.0.0.1:7880 --key devkey --secret secret --clients 200 --rooms 20 --duration 8
```

`load_test` supports `--scenario all|join|rtt|throughput` to run individual
scenarios.
