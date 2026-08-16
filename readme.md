# livekit-rs-voice

[![CI](https://github.com/n1snt/livekit-rs-voice/actions/workflows/ci.yml/badge.svg)](https://github.com/n1snt/livekit-rs-voice/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A **drop-in, voice-only rewrite of the LiveKit server** in Rust.

> **LiveKit-compatible** refers to wire-protocol compatibility only. This
> project is not affiliated with or endorsed by LiveKit, Inc.

It speaks the LiveKit wire protocol (WebSocket signaling, Twirp HTTP API,
protobuf/protojson, JWT auth, WebRTC media) so existing LiveKit clients,
SDKs, the `livekit-agents` worker, and voice-agent stacks can connect
without changes — but it only handles **audio** (opus). Video, simulcast,
dynacast, and multi-node clustering are deliberately out of scope.

```
┌────────────────┐   ┌──────────────┐   ┌────────────────────┐
│ browser / agent│──▶│  livekit-rs- │──▶│ livekit-agents     │
│ livekit-client │   │  voice (SFU) │   │ worker (/agent)    │
└────────────────┘   └──────┬───────┘   └────────────────────┘
                            │ 7880 HTTP/WS · Twirp · /rtc · /agent
                            │ 7881 UDP media (or range)
```

## Why

The Go `livekit/livekit-server` is the reference implementation. This crate is
a lean Rust equivalent that keeps the exact same API surface that LiveKit
clients, SDKs and the rest of a voice stack depend on:

- **WebSocket signaling** `/rtc` and `/rtc/v1` (protobuf-binary by default,
  JSON on text frames, exactly like the reference `wsprotocol`).
- **Twirp HTTP API** `/twirp/livekit.RoomService`, `.../AgentDispatchService`,
  `.../SIP`, `.../Egress` with protojson-compatible JSON.
- **Agent worker WebSocket** `/agent` (register / availability / assignment).
- **JWT auth** (HS256, `video` grants, `roomConfig.agents`).
- **Webhooks** signed with `X-Livekit-Signature: hex(HMAC-SHA256(...))` to the
  configured `webhook.urls`.
- **Prometheus** `livekit_room_total` on the dedicated metrics port.
- **Redis** interop with the `livekit/sip` and `livekit/egress` containers using
  the same hash keys and binary-protobuf encoding.

## Workspace layout

```
livekit-rs-voice/
├── Cargo.toml            # workspace
├── protos/               # vendored livekit .proto files (wire contract)
├── crates/
│   ├── lk-proto/         # prost-generated types + protojson serde (build.rs via protox)
│   └── lk-server/        # the server: config, auth, signaling, SFU, HTTP, agents
│       ├── src/
│       │   ├── main.rs        # CLI entry point
│       │   ├── config.rs      # YAML config (LiveKit-compatible keys/defaults)
│       │   ├── auth.rs        # HS256 JWT verification + grants
│       │   ├── http.rs        # axum router: Twirp, WS, health, auth, CORS
│       │   ├── signal.rs      # WS sessions, room join, signal request handlers
│       │   ├── media.rs       # WebRTC SFU: publisher/subscriber PCs, forwarding
│       │   ├── audio_level.rs # RFC 6464 active-speaker detection
│       │   ├── room.rs        # room lifecycle + broadcasts
│       │   ├── participant.rs # participant state + outbound signal channel
│       │   ├── agent.rs       # worker registry + job dispatch
│       │   ├── services.rs    # RoomService + AgentDispatchService (Twirp)
│       │   ├── services_sip.rs# SIP + Egress services (Twirp)
│       │   ├── redis_store.rs # Redis-backed SIP/egress store
│       │   ├── webhook.rs     # webhook notifier
│       │   └── server.rs      # room manager + background workers
│       └── tests/
│           ├── signaling.rs   # HTTP/Twirp/WS integration tests
│           └── media.rs       # real WebRTC media loopback test
```

## Documentation

- [Quickstart](docs/quickstart.md) — run it in ~5 minutes, locally or in Docker
- [Configuration reference](docs/configuration.md)
- [Deployment guide](docs/deployment.md) — Docker, Kubernetes, webhooks, metrics
- [Usage: clients, agents, SIP, egress](docs/usage.md)
- [Benchmark vs the Go server](benchmark_livekit_rs_voice.md)

## Docker

Images are published to GHCR on every version tag (`vX.Y.Z`):

```bash
docker run -d --name livekit-voice   -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882   -v "$PWD/examples/livekit.example.yaml:/etc/livekit/livekit.yaml:ro" \
  ghcr.io/n1snt/livekit-rs-voice:latest \
  --config /etc/livekit/livekit.yaml
```

## Building and testing

```bash
# build
cargo build --workspace

# run the full test suite (unit + integration + real-media loopback)
cargo test --workspace

# lint + format
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

## Benchmarks

See `benchmark_livekit_rs_voice.md` for a detailed comparison against the Go
`livekit-server` (join latency, signalling RTT, join/leave throughput, memory,
plus in-process micro-benchmarks).

```bash
# micro-benchmarks (criterion)
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core

# load test against any LiveKit-compatible server (ours or the Go one)
cargo build --release -p lk-server --bin livekit-voice --bin load_test
./target/release/load_test --target ws://127.0.0.1:7880     --key devkey     --secret secret     --clients 200 --rooms 20 --duration 8
```

The media test (`tests/media.rs`) spins up two real WebRTC peers through the
server and verifies audio RTP flows publisher → SFU → subscriber, which
exercises ICE/DTLS, publisher offer/answer, track matching, auto-subscription,
subscriber renegotiation, and per-subscriber down-track forwarding.

## Running the server

```bash
cargo run -p lk-server -- --config /path/to/livekit.yaml
# or dev mode (loopback + devkey/secret):
cargo run -p lk-server -- --dev
```

Config files are standard LiveKit YAML. The parser is strict about unknown
keys but accepts all keys used in typical LiveKit configs, including the legacy
top-level `log_level`.

Environment overrides: `LIVEKIT_KEYS`, `LIVEKIT_PORT`, `LIVEKIT_REGION`,
`LIVEKIT_CONFIG`, `LIVEKIT_REDIS_ADDRESS`, `LIVEKIT_RTC_TCP_PORT`,
`LIVEKIT_LOG_LEVEL`, `NODE_IP`, `UDP_PORT`.

## Drop-in compatibility surface

| Surface | Notes |
|---|---|
| `:7880` HTTP/WS | `/rtc`, `/rtc/v1`, `/rtc/validate`, `/rtc/v1/validate`, `/agent`, `/twirp/...`, `/` |
| Media | UDP on `rtc.tcp_port` (or `rtc.port_range_start..end`); voice-only (opus) |
| Metrics | `:prometheus_port/metrics` with `livekit_room_total` |
| Webhooks | `room_started`, `room_finished`, `participant_joined`, `participant_left`, `track_published`, `track_unpublished` — HMAC-signed |
| Redis | `sip_trunk`, `sip_inbound_trunk`, `sip_outbound_trunk`, `sip_dispatch_rule`, `egress`, `egress:room:<name>` |
| JWT | HS256, `iss`→api key, `sub`→identity, `video` grants, `roomConfig.agents` |

### Behavior details that matter for compatibility

- **Signaling**: binary protobuf by default; switches to protojson on text
  frames (and back on binary), matching the reference `wsprotocol`.
- **Join flow**: `JoinResponse` (subscriber-primary, protocol 17) → subscriber
  offer with `_reliable`/`_lossy`/`_data_track` data channels → publisher
  offer/answer → ICE (candidates arrive as JSON `RTCIceCandidateInit`, and are
  buffered until the peer connection exists).
- **Track matching**: the publisher's RTP stream id is matched against the
  client track id (`cid`); `midToTrackId` in offers is the fallback.
- **Per-subscriber SSRC**: each subscriber's down-track gets its own SSRC, so
  subscribers demux by SSRC.
- **Active speakers**: audio levels read from the RFC 6464 header extension;
  `SpeakersChanged` broadcasts change only.
- **Agent dispatch**: token `roomConfig.agents` and `CreateDispatch` launch
  room jobs; the worker handshake (`/agent`) assigns jobs with an agent join
  token (`kind=AGENT`, `video.agent`).

## Known limitations (by design)

- **Audio only** — video tracks are rejected/ignored (voice-only requirement).
- **Outbound SIP calls** (`CreateSIPParticipant` / `TransferSIPParticipant`)
  return a clear `failed_precondition` error: they require the LiveKit psrpc
  message bus to reach the external `livekit/sip` bridge. Trunk and dispatch
  rule CRUD (which the backend uses for number management) work fully, and
  inbound calls via the SIP container work through Redis + WS signaling.
- **Single node** — no Redis-based multi-node clustering / room migration.
- **No embedded TURN** — media relies on host candidates (`rtc.ips.includes`).
- **Egress** requests are persisted to Redis for the external `livekit/egress`
  container to pick up; the container must be running to actually record.
- **No renegotiation of publish layers** and no video-layer selection.
- **SIP participant auto-subscription / dispatches**: inbound dispatch rules
  stored in Redis are honored by the SIP container, which joins rooms via the
  standard signaling path.

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). The vendored `.proto`
files are Copyright LiveKit, Inc. (Apache-2.0).

## Versioning & releases

Semantic versioning (`vX.Y.Z` tags). See [CHANGELOG.md](CHANGELOG.md).

```bash
git tag v0.1.0 && git push origin v0.1.0   # CI builds + publishes the image
```

## Contributing

PRs are welcome. The CI suite (`.github/workflows/ci.yml`) runs
`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
`cargo test --workspace` — keep all three green. See
[coding_standards.md](coding_standards.md) for the project's standards.

- Scope: audio only, single node. Changes that keep the wire contract intact
  are in scope; video and clustering are not.
- Test every change: unit tests alongside the code, integration tests in
  `crates/lk-server/tests/`, and update `CHANGELOG.md`.
