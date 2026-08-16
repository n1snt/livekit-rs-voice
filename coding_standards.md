# Coding Standards — livekit-rs-voice

These standards apply to every crate in this workspace. The goal is a lean,
readable, correct LiveKit-compatible server: prioritize wire compatibility and
test coverage over cleverness.

## 1. Rust style and toolchain

- Rust edition 2021, stable toolchain (see `rust-version` in the workspace
  manifest).
- Always `cargo fmt` and `cargo clippy --workspace -- -D warnings` clean before
  considering a change done.
- No `unsafe` unless absolutely necessary; if used, isolate it behind a safe
  API with a comment explaining the invariant.
- No `expect()`/`unwrap()` on user-controlled input. Server-side `.unwrap()` is
  only acceptable on infallible internal state (e.g. a `Mutex` we know is
  unpoisoned) and should be justified by context.

## 2. Workspace organization

Two crates, no more:

- `lk-proto` — generated protobuf types (`prost`) and protojson serde. Never
  hand-write wire types; edit the `.proto` files under `protos/` and let
  `build.rs` regenerate. Add protojson-compliance tests here
  (`Timestamp`/`Duration` string forms, oneof key shapes, int64-as-string).
- `lk-server` — everything else. Modules are grouped by responsibility:

  | Module | Responsibility |
  |---|---|
  | `config.rs` | YAML config, defaults, env overrides |
  | `auth.rs` | JWT verification and grants |
  | `http.rs` | axum router, Twirp dispatch, WS upgrades, auth middleware |
  | `signal.rs` | WS sessions, join flow, `SignalRequest` handlers, webhook callbacks |
  | `media.rs` | WebRTC SFU: peer connections, forwarding, negotiation, data channels |
  | `audio_level.rs` | RFC 6464 audio-level detection |
  | `room.rs` / `participant.rs` / `track.rs` | state models |
  | `agent.rs` | worker registry and job dispatch |
  | `services.rs` / `services_sip.rs` | Twirp method implementations |
  | `redis_store.rs` | optional Redis store (SIP/egress interop) |
  | `webhook.rs` | webhook notifier |
  | `server.rs` | room manager and background workers |

## 3. Wire compatibility is the contract

- The LiveKit protocol (field numbers, message names, protojson casing, enum
  string names, HTTP status/error codes, header names) is **fixed**. The Go
  reference Go implementation and the vendored `.proto`
  files are the source of truth.
- Prefer protojson semantics from the generated serde; do not hand-roll JSON
  for protocol messages.
- When in doubt about a field's wire name, check the generated code in
  `target/.../out/livekit.rs` before guessing.
- Twirp errors: use the existing `TwirpError` constructors (`invalid_argument`
  → 400, `not_found` → 404, `permission_denied` → 403, `unauthenticated` →
  401, `already_exists` → 409, `failed_precondition`, `bad_route` → 404,
  `internal` → 500). Keep the `{"code","msg","meta"}` body shape.

## 4. Concurrency rules

- Never hold a `std::sync::Mutex` guard across an `.await`. Scope the lock to a
  block, extract what you need (`clone()`), and drop before awaiting. The
  futures must remain `Send` (axum's `on_upgrade` requires it).
- Prefer `tokio::sync` types for state that must be held across awaits; use
  `std::sync::Mutex` only for short critical sections.
- Keep lock scope tight: `participant.media.lock().unwrap()` for a few field
  reads is fine; iterating the whole room while holding a lock is not.
- Broadcasting to participants is best-effort: use `send_update` (bounded
  `try_send`, drops on overflow) for fan-out, `send` (awaited) for
  request/response messages.
- Spawn long-running work (`tokio::spawn`) for anything that could block a
  session (e.g. agent availability round-trips must never block the join path).

## 5. Error handling

- Use `Result<T, String>` for internal plumbing, `Result<_, TwirpError>` at
  the Twirp boundary, and `thiserror` where richer errors help.
- Log with `tracing` at the right level:
  - `debug` — per-request/session flow (ICE states, negotiation, candidate drops)
  - `warn` — recoverable anomalies (decode failures, dropped candidate, slow
    subscriber write)
  - `error` — unrecoverable but non-fatal
- Never log secrets (API keys, tokens, SIP passwords).

## 6. Testing requirements

Every change must keep the suite green and add tests for new behavior:

- **Unit tests** live next to the module (`#[cfg(test)] mod tests`). Cover
  success, failure, and edge cases (e.g. `auth::tests` covers expired tokens,
  unknown keys, missing `exp`, snake_case claims, SIP grants).
- **Integration tests** in `crates/lk-server/tests/signaling.rs` exercise the
  real HTTP/WS surface: health, metrics, Twirp round-trips, permissions,
  JWT auth, the WS join/offer/answer/ping/leave flow, AddTrack, and the agent
  worker register→availability→assignment flow.
- **Media test** in `crates/lk-server/tests/media.rs` runs two real WebRTC
  peers through the server and asserts RTP is forwarded publisher → SFU →
  subscriber. Run it with `-- --nocapture` to see webrtc logs.
- Protocol invariants (protojson casing, oneof keys, Timestamp/Duration
  formats, enum values) are asserted in `lk-proto` tests.

## 7. Configuration and lifecycle

- New config keys must be added with a `Default` impl that matches the
  reference server's defaults; partial YAML blocks merge onto the canonical
  defaults (do not let `#[serde(default)]` reset a nested struct to zero).
- Rooms must be reclaimed: respect `empty_timeout`/`departure_timeout`,
  reset `empty_since` on join/leave, prune `speaker_states`, and terminate
  room-level agent jobs on close.
- Webhook events (`room_started`, `participant_joined`, `participant_left`,
  `track_published`, ...) fire exactly once per lifecycle transition, in the
  reference order (`room_started` before `participant_joined`).

## 8. Cleanup

- Remove dead code, unused imports, and unused config fields as part of the
  change that orphans them.
- Do not leave debugging `eprintln!`/`dbg!` in committed code; use `tracing`
  and prefer `debug`/`trace` levels.
