# livekit-rs-voice

A minimal, voice-only LiveKit server. A drop-in replacement in Rust for the audio path of [livekit-server](https://github.com/livekit/livekit). It speaks the same LiveKit wire protocol, so existing LiveKit clients, `livekit-agents`, and the `livekit/sip` + `livekit/egress` containers connect and work unchanged.

Everything not listed under [Differences from LiveKit](#differences-from-livekit) behaves like the reference `livekit-server`; configure and use it the same way (see the [LiveKit docs](https://docs.livekit.io)).

## Quick start

```bash
# Docker
docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882 \
  -v "$PWD/examples/livekit.example.yaml:/etc/livekit/livekit.yaml:ro" \
  n1snt/livekit-rs-voice:latest --config /etc/livekit/livekit.yaml

# or from source (dev mode: loopback + devkey/secret)
cargo run --release -p lk-server -- --dev
```

Images are published to [Docker Hub](https://hub.docker.com/r/n1snt/livekit-rs-voice) on every `v*` tag.

## Differences from LiveKit

- **Audio only** (video tracks are rejected).
- **Multi-node over Redis** (`redis.cluster: true`); does not interoperate with Go `livekit-server` nodes.
- **SIP & Egress** interoperate with the `livekit/sip` and `livekit/egress` containers over Redis (psrpc).
- **Webhooks** signed with `X-Livekit-Signature: hex(HMAC-SHA256(...))`.
- **Metrics** are drop-in compatible with `livekit-server` (`metrics.rs`).

## Benchmarks

See [benchmark_livekit_rs_voice.md](benchmark_livekit_rs_voice.md). Measured on a MacBook Pro (Apple M1, 32 GB RAM, 1 TB SSD): ~19x faster joins (p50), ~17x more joins/s, ~2x less memory per connection, and ~2x more pings / ~7x more joins per CPU-second than the Go `livekit-server`.

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
