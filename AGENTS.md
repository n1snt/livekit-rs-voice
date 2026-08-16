# AGENTS.md

Guidance for AI agents and contributors working in this repository.

## Project

`livekit-rs-voice` is a voice-only, LiveKit-wire-compatible SFU in Rust. It is
a drop-in for the audio path of `livekit-server`: existing LiveKit clients,
`livekit-agents`, and the `livekit/sip` + `livekit/egress` containers connect
unchanged.

### Hard constraints

- **Wire compatibility is the contract.** Never change proto field numbers,
  message names, protojson casing, enum string names, Twirp routes/error codes,
  or WebSocket framing. The vendored `.proto` files in `protos/` are the source
  of truth; if unsure about a wire name, check the generated code in
  `target/.../out/`.
- **Audio only.** Video is out of scope; reject or ignore video tracks.
- **Multi-node over Redis.** When `redis.cluster: true`, nodes register,
  rooms are hosted on one node, and signaling is relayed to the hosting node
  over Redis streams (`cluster.rs`). This node-to-node clustering is
  Rust-native and does not interoperate with Go `livekit-server` nodes. The
  exception is the SIP bridge: `psrpc.rs` implements the psrpc v0.7 wire
  protocol (Redis PubSub) so `CreateSIPParticipant`/`TransferSIPParticipant`
  reach a real `livekit/sip` container. When disabled (default), every room
  is local.
- **Lean docs.** The readme covers only benchmarks and the differences from
  `livekit-server`. Do not add docs that re-document LiveKit behavior.
- **Never add or commit secrets/API keys.**

## Commands

```bash
cargo build --workspace
cargo test --workspace              # includes a real WebRTC media loopback
cargo clippy --workspace --all-targets -- -D warnings   # must be clean
cargo fmt --all                                          # must be clean
cargo bench -p lk-proto --bench proto
cargo bench -p lk-server --bench core
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy, tests, and a release build on
every push/PR. Keep all four green.

## Layout

```
crates/lk-proto/    prost types + protojson serde, generated from protos/
                    (build.rs uses protox, no protoc binary needed).
                    Edit the .proto files, never the generated code.
crates/lk-server/   the server:
  config.rs         YAML config (LiveKit-compatible). Default impls must mirror
                    the reference defaults; partial YAML blocks must merge onto
                    them, not reset to zero.
  auth.rs           HS256 JWT verification + video grants (iss -> api key,
                    sub -> identity, canPublish/canSubscribe default true).
  http.rs           axum router: Twirp dispatch, WS upgrades, auth, CORS,
                    body/message-size limits.
  signal.rs         WS sessions, join flow, SignalRequest handlers, webhooks.
  media.rs          WebRTC SFU: publisher/subscriber PCs, RTP forwarding,
                    subscriber negotiation, data channels.
  audio_level.rs    RFC 6464 active-speaker detection.
  room.rs / participant.rs / track.rs   state models.
  agent.rs          worker registry + job dispatch (/agent).
  services.rs / services_sip.rs   Twirp method implementations.
  psrpc.rs          psrpc v0.7 wire protocol (Redis PubSub): client for
                    outbound SIP, server for the IOInfoSIP service.
  ioservice.rs      IOInfoSIP handlers (inbound trunk auth + dispatch rules).
  redis_store.rs    optional Redis store for SIP/egress container interop.
  webhook.rs / server.rs / metrics.rs  webhooks, room manager, metrics.
```

## Rules

- **Never hold a `std::sync::Mutex` guard across an `.await`.** Futures must
  stay `Send` (axum's `on_upgrade` requires it). Scope locks to blocks, extract
  what you need, drop before awaiting. Use `tokio::sync` types for state held
  across awaits.
- **WebRTC callback closures that capture `Arc<Participant>` must use
  `Arc::downgrade` (Weak).** Capturing a strong ref in `on_track` /
  `on_message` / `on_ice_candidate` creates a reference cycle that leaks the
  participant and its media (this was a real memory-leak bug in `media.rs`).
- **No `unwrap()`/`expect()` on user-controlled input.** Use `Result`.
- **Broadcasts are best-effort:** `send_update` (bounded `try_send`) for
  fan-out; awaited `send` for request/response messages.
- **Do not block the join path on agent availability round-trips.** Spawn them
  (see `signal.rs` room-agent launch).
- **Enforce configured limits** (`limit.*`, message/body sizes). Do not let a
  client push unbounded data.

## Testing

- Every change adds tests: unit tests beside the module, integration tests in
  `crates/lk-server/tests/signaling.rs`, and the real-media loopback in
  `crates/lk-server/tests/media.rs` must keep passing.
- Protocol invariants (protojson casing, `Timestamp`/`Duration` string forms,
  enum values, oneof key shapes) are asserted in `lk-proto` tests.

## Docs policy

Public docs are intentionally minimal: `readme.md` (quick start + "Differences
from LiveKit"), `benchmark_livekit_rs_voice.md`, `CHANGELOG.md`. When behavior
changes, update `CHANGELOG.md` (Keep a Changelog) and the "Differences from
LiveKit" section in `readme.md` if users are affected. Do not create new `.md`
docs unless asked.
