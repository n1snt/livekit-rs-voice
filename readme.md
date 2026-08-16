# livekit-rs-voice

A monorepo of minimal, voice-only LiveKit services in Rust, drop-in replacements for the audio path of [livekit-server](https://github.com/livekit/livekit): `livekit-voice` (the SFU) and `livekit-egress` (the voice recorder). They speak the same LiveKit wire protocol, so existing LiveKit clients, `livekit-agents`, and the `livekit/sip` container connect and work unchanged.

Everything not listed under [Differences from LiveKit](#differences-from-livekit) behaves like the reference `livekit-server`; configure and use it the same way (see the [LiveKit docs](https://docs.livekit.io)).

## Services

- **`livekit-voice`** — the server (SFU): signaling, media, SIP, metrics.
- **`livekit-egress`** — the voice-only recorder: receives egress jobs from `livekit-voice` over Redis (psrpc), joins rooms as a subscriber, and records audio to WAV/MP3.
## Quick start

```bash
# Docker
docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882 \
  -v "$PWD/examples/livekit.example.yaml:/etc/livekit/livekit.yaml:ro" \
  n1snt/livekit-rs-voice:latest --config /etc/livekit/livekit.yaml

# or from source (dev mode: loopback + devkey/secret)
cargo run --release -p lk-server -- --dev
```

Images are published to Docker Hub on every `v*` tag; each service ships its own small image:
[`n1snt/livekit-rs-voice`](https://hub.docker.com/r/n1snt/livekit-rs-voice) and
[`n1snt/livekit-rs-egress`](https://hub.docker.com/r/n1snt/livekit-rs-egress).

## Differences from LiveKit

- **Audio only** (video tracks are rejected).
- **Multi-node over Redis** (`redis.cluster: true`); does not interoperate with Go `livekit-server` nodes.
- **SIP** interoperates with the `livekit/sip` container over Redis (psrpc).
- **Egress** is served by the in-repo `livekit-egress` recorder over Redis (psrpc), instead of the `livekit/egress` container.
- **Webhooks** signed with `X-Livekit-Signature: hex(HMAC-SHA256(...))`.
- **Metrics** are drop-in compatible with `livekit-server` (`metrics.rs`).

## Benchmarks

See [benchmark_livekit_rs_voice.md](benchmark_livekit_rs_voice.md) (server)
and [benchmark_livekit_rs_egress.md](benchmark_livekit_rs_egress.md) (recorder). Measured on a MacBook Pro (Apple M1, 32 GB RAM, 1 TB SSD): ~19x faster joins (p50), ~17x more joins/s, ~2x less memory per connection, and ~2x more pings / ~7x more joins per CPU-second than the Go `livekit-server`.

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
