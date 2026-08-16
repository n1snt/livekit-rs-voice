# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases are tagged `vX.Y.Z` and published as multi-arch (linux/amd64 and
linux/arm64) Docker images to Docker Hub (`n1snt/livekit-rs-voice`) and GHCR.

## Versioning policy

Versions mirror [livekit-server](https://github.com/livekit/livekit) releases:
`1.13.5` means "wire/protocol level of livekit-server 1.13.5". We bump the
version to match whenever we pick up upstream protocol patches, so the version
always reflects the LiveKit protocol level the server implements.

## [Unreleased]

### Added

- TURN relay (RFC 8489/5766) with JoinResponse ICE server credentials.
- Full SIP over the psrpc wire protocol (v0.7 Redis PubSub): outbound
  `CreateSIPParticipant` / `TransferSIPParticipant` reach a real
  `livekit/sip` container, and the embedded `IOInfoSIP` service serves
  inbound calls (trunk authentication, dispatch-rule evaluation, call state).
- `lk` CLI: place outbound SIP calls and manage SIP trunks / dispatch rules
  through the Twirp API.

### Fixed

- Clustering (Redis) was still tracked as "planned"; it has shipped.

## [1.13.5] - 2026-08-16

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
