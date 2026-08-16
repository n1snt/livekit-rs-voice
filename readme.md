# livekit-rs-voice

A **voice-only, LiveKit-wire-compatible SFU server** in Rust. It speaks the
LiveKit protocol (WebSocket signaling, Twirp HTTP API, HS256 JWT auth, WebRTC
audio) so existing LiveKit clients, `livekit-agents`, and the `livekit/sip` +
`livekit/egress` containers work unchanged — minus the parts a voice stack
doesn't need.

Everything not listed under
[Differences from LiveKit](#differences-from-livekit) behaves like the
reference `livekit-server`; configure and use it the same way (see the
[LiveKit docs](https://docs.livekit.io)).

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

- **Audio only** — video tracks are rejected/ignored.
- **Single node** — no Redis-based clustering/room migration; signaling is
  in-process (no Redis relay).
- **No embedded TURN** — media uses host candidates (`rtc.ips.includes`).
- **Outbound SIP** (`CreateSIPParticipant`) is not bridged: it requires the
  `livekit/sip` psrpc bus this server doesn't embed. Trunk/dispatch CRUD and
  inbound calls via `livekit/sip` work.
- **Egress** persists requests for the `livekit/egress` container to pick up;
  the container does the actual recording.
- **Webhooks** use the LiveKit-Cloud-style `X-Livekit-Signature: hex(HMAC-SHA256(...))`
  header rather than the self-hosted JWT scheme.

## Benchmarks

[benchmark_livekit_rs_voice.md](benchmark_livekit_rs_voice.md) — join latency,
signaling RTT, join/leave throughput, and memory & CPU vs the Go
`livekit-server` (headline: ~7–10× faster joins, ~2.6× less memory per
connection, 5–23× more work per CPU-second).

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

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
