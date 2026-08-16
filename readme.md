# livekit-rs-voice

A voice-only LiveKit server in Rust. It is a drop-in replacement for the audio
path of [livekit-server](https://github.com/livekit/livekit): it speaks the
same LiveKit wire protocol, so existing LiveKit clients, `livekit-agents`, and
the `livekit/sip` + `livekit/egress` containers connect and work unchanged.

Everything not listed under [Differences from LiveKit](#differences-from-livekit)
behaves like the reference `livekit-server`; configure and use it the same way
(see the [LiveKit docs](https://docs.livekit.io)).

## Quick start

```bash
# Docker
docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882 \
  -v "$PWD/examples/livekit.example.yaml:/etc/livekit/livekit.yaml:ro" \
  ghcr.io/n1snt/livekit-rs-voice:latest --config /etc/livekit/livekit.yaml

# or from source (dev mode: loopback + devkey/secret)
cargo run --release -p lk-server -- --dev
```

Images are published to GHCR on every `v*` tag.

## Differences from LiveKit

- **Audio only.** Video tracks are rejected or ignored.
- **Multi-node over Redis.** Nodes register with a heartbeat, rooms are hosted
  on exactly one node, and a client connected to any node transparently joins
  rooms on other nodes via a signaling relay over Redis streams. Enable it with
  `redis.cluster: true`. This is Rust-native clustering; it does not
  interoperate with Go `livekit-server` nodes in a mixed cluster, and node
  failover is basic (rooms are reclaimed by other nodes, in-flight sessions
  reconnect).
- **No embedded TURN.** Media uses host candidates (`rtc.ips.includes`).
- **Full SIP** over the psrpc wire protocol (Redis PubSub, compatible with
  livekit psrpc v0.7). Configure `redis` and run a `livekit/sip` container on
  the same Redis: **outbound** calls via the `lk` CLI
  (`lk sip create-participant ...`) or the Twirp API, and **inbound** calls
  served by the embedded `IOInfoSIP` service (trunk auth + dispatch rules).
- **Egress** persists requests for the `livekit/egress` container to pick up;
  the container does the actual recording.
- **Webhooks** use the LiveKit-Cloud-style
  `X-Livekit-Signature: hex(HMAC-SHA256(...))` header instead of the
  self-hosted JWT scheme.
- **Metrics** are drop-in compatible with `livekit-server`: same names,
  labels, and histogram buckets for rooms, participants, connections, tracks,
  session latency/duration, connection quality, RTCP feedback (NACK/PLI/FIR),
  packet loss/out-of-order/jitter/RTT, and forwarding latency. See
  `metrics.rs`.

## Benchmarks

See [benchmark_livekit_rs_voice.md](benchmark_livekit_rs_voice.md). Against
the Go `livekit-server`, this server joins roughly 7-10x faster, uses ~2.6x
less memory per connection, and does 5-23x more work per CPU-second.

```bash
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core

cargo run -p lk-server --release --bin load_test -- \
  --target ws://127.0.0.1:7880 --key devkey --secret secret \
  --clients 200 --rooms 20 --duration 8
```

## Build & test

```bash
cargo build --workspace
cargo test --workspace        # includes a real WebRTC media loopback test
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
