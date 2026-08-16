# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases are tagged `vX.Y.Z` and published as Docker images to GHCR.

## [Unreleased]

### Added

- Voice-only, LiveKit-wire-compatible SFU in Rust.
- WebSocket signaling: `/rtc`, `/rtc/v1`, `/rtc/validate`, `/rtc/v1/validate`
  (protobuf-binary by default, JSON on text frames).
- Twirp HTTP API: `livekit.RoomService`, `livekit.AgentDispatchService`,
  `livekit.SIP`, `livekit.Egress` with protojson-compatible JSON.
- Agent worker WebSocket `/agent` (register / availability / assignment) with
  `roomConfig.agents` and `CreateDispatch` job launching.
- HS256 JWT auth with `video` grants.
- Webhooks (`room_started`, `room_finished`, `participant_joined`,
  `participant_left`, `track_published`, `track_unpublished`) signed with
  `X-Livekit-Signature: hex(HMAC-SHA256(...))`.
- Prometheus `/metrics` (incl. `livekit_room_total`) on a dedicated port.
- Optional Redis store for SIP/egress container interop.
- Benchmarks: criterion micro-benchmarks + `load_test` harness, and
  `benchmark_livekit_rs_voice.md` comparing against the Go server.

### Fixed

- ICE candidate handling (JSON framing, buffering before PC creation).
- Publisher track matching by RTP stream id.
- Room empty-timeout, duplicate-identity teardown, agent-job lifecycle.
- Reference-cycle-free media teardown (Weak refs in webrtc callbacks).
