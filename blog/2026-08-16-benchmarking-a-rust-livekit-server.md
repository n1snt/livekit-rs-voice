# Forget the Go LiveKit Server — This Rust Drop-in Joins Calls 10x Faster

I spent a weekend benchmarking the LiveKit server we run for voice, and the
numbers made me re-check my test harness twice.

The Go reference server needed **279ms** to join a call at the median. My
Rust drop-in did it in **14.5ms**. Under identical load it sustained **17x
more joins per second**, used **~2x less memory per connection**, and did
**~7x more joins per CPU-second**. The Docker image is **74MB** — smaller
than the Go one, at 110MB.

This isn't a synthetic microbenchmark. It's a real WebSocket client hammering
both servers over the exact same wire protocol, on the same machine.

## What this is

`livekit-rs-voice` is a minimal, voice-only LiveKit server in Rust — a drop-in
replacement for the audio path of `livekit-server`. It speaks the same wire
protocol, so existing LiveKit clients, `livekit-agents`, and the
`livekit/sip` + `livekit/egress` containers connect and work unchanged.

No rewrites, no custom SDKs. Point your existing LiveKit tooling at it.

## The benchmark

Both servers ran on a MacBook Pro (Apple M1, 32 GB RAM, 1 TB SSD), loopback
networking. The Rust server ran natively in release mode. The Go server ran
`livekit-server` v1.11.0 in Docker — its normal production topology, complete
with a Redis message bus in the signal path.

The load generator (`load_test`) is a real WebSocket client that drives the
exact LiveKit protocol: connect, join a room, ping/pong, leave.

### Joining a call: 19x faster

200 clients connect concurrently across 100 rooms. Time from TCP connect to
receiving the `JoinResponse`:

| | Rust | Go |
|---|---|---|
| p50 | **14.5 ms** | 279 ms |
| avg | **30.5 ms** | 291 ms |
| p99 | **69.6 ms** | 395 ms |

More rooms hurt the Go server the most: its per-room Redis allocation pushed
join latency from ~111 ms at 20 rooms to ~279 ms at 100, while the Rust
server stayed in the 10-15 ms range.

### Signaling round-trip: 4x faster

The ping/pong loop is the hot path that also carries ICE candidates,
subscriptions, and room metadata. 200 stable connections:

| | Rust | Go |
|---|---|---|
| RTT p50 | **1.27 ms** | 5.10 ms |
| Pong rate | **142.8 k/s** | 36.2 k/s |

### Throughput: 17x more joins, 7x more per CPU-second

200 clients loop join → leave for 8 seconds:

| | Rust | Go |
|---|---|---|
| Joins/s | **12,199** | 721 |
| Joins per CPU-second | **~1,470** | ~199 |

The per-join cost difference is the story. Go's Redis room-allocation and
signal relay make each join orders of magnitude more expensive.

### Memory: 2x less per connection

200 stable connections, identical load:

| | Rust | Go |
|---|---|---|
| Total | **~66 MB** | ~136 MB |
| Per connection | **~330 KB** | ~680 KB |
| Idle | **~6 MB** | ~12 MB |

## Why it's faster

Three things, in rough order of impact:

1. **No Redis in the request path.** The Go server relays every join and every
   signal message through its Redis message bus — room allocation, node
   selection, psrpc streams — even in a single-node deployment. The Rust server
   handles it all in-process with a hashmap and channels. For a single node,
   that indirection buys nothing and costs a round-trip per message.
2. **Lean serialization.** A full `JoinResponse` (room + participant + others +
   codecs) encodes or decodes in a couple of microseconds. The data path is
   hundreds of nanoseconds.
3. **`tokio` + small critical sections.** No global lock on the room/session
   path, so join/leave throughput scales past 12k/s on one node.

## The honest caveats

- The Go server runs in Docker with a Redis relay and full multi-node
  capability. The Rust server is single-node and voice-only (video tracks are
  rejected). A feature-for-feature comparison isn't the point — the point is
  that the API surface a voice deployment actually uses can be served with
  materially better latency and throughput.
- Loopback networking keeps network jitter out of the picture. On a real WAN
  the relative gap will shrink.
- Rapid connect/disconnect churn is allocation-heavy in both servers; neither
  returns to baseline instantly.

For a telephony workload — calls that last minutes, not bursts of
connect/disconnect — the steady-state numbers are the ones that matter, and
there the Rust server is clearly leaner.

## The point

We didn't set out to beat Go for sport. We needed a voice-only SFU that was
fast, cheap to run, and a true drop-in. `livekit-rs-voice` is that: same
protocol, same containers, a fraction of the latency and memory.

The code is open source: [github.com/n1snt/livekit-rs-voice](https://github.com/n1snt/livekit-rs-voice)
