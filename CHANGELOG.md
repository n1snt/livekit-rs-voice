# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Releases are tagged `vX.Y.Z.R` and published as multi-arch (linux/amd64 and linux/arm64) Docker images to [Docker Hub](https://hub.docker.com/r/n1snt/livekit-rs-voice) and GHCR.

## Versioning policy

Versions mirror [livekit-server](https://github.com/livekit/livekit) releases. The first three components, `X.Y.Z`, are the wire/protocol level: `1.13.5` means "the protocol of livekit-server 1.13.5". We bump `X.Y.Z` whenever we pick up upstream protocol patches, so it always reflects the LiveKit protocol level the server implements.

The fourth component, `R`, is our own build/patch revision for changes that do not alter the wire protocol (bug fixes, tooling, tests). So `1.13.5.1` is protocol level 1.13.5 with our first post-release fix set. A single `vX.Y.Z.R` tag releases **both** services (`livekit-rs-voice` and `livekit-rs-egress`) as one multi-arch image pair.

The wire `server_version` advertised in `JoinResponse` stays the protocol level (`X.Y.Z`), independent of the release revision. See [docs/versioning.md](docs/versioning.md) for the full policy.

## [Unreleased]

### Added

- `livekit-egress` can now upload finished recordings to S3-compatible object storage (AWS S3, Cloudflare R2, MinIO) via the new `s3:` config block or per-request `EncodedFileOutput.s3`/`StorageConfig` upload config. `FileInfo.filename` is the storage key and `FileInfo.location` the object URL, matching the reference `livekit/egress`. GCP/Azure/AliOSS uploads return a clear "not supported" error instead of being ignored.
- S3 uploads also support `assume_role_arn`/`assume_role_external_id` (STS `AssumeRole` with temporary credentials; base keys fall back to the new `s3_assume_role_key`/`s3_assume_role_secret` config), `content_disposition`, `metadata`, `tagging`, and the MIME content type. GCP (`gcp:` block / request `gcp`) and Azure (`azure:` block / request `azure`) uploads are now supported too; `proxy` and `alioss` are parsed and rejected with a clear error instead of being silently ignored.
- Request-level advanced encoding options (`audio_bitrate`) now override the config `mp3_bitrate`.
- The recorder reports an `EGRESS_ACTIVE` update after connecting, so the server fires the reference `egress_updated` webhook during a recording (previously only `egress_started` and `egress_ended` fired).
- `livekit-egress` now logs structured JSON with a lowercase `level`, matching the server and the reference Go logs.
- `livekit-voice` now sends the reference `egress_started` / `egress_updated` / `egress_ended` webhooks when the recorder reports egress state (`CreateEgress`/`UpdateEgress`), with `started`/`ended` deduped per egress id.
- `livekit-egress` config accepts the Go `livekit/egress` keys so the same `egress.yaml` works unchanged: `s3` (default upload destination), `log_level` (top-level alias for `logging.level`), `insecure` and `cpu_cost` (parsed and logged as accepted-but-unused on the voice-only recorder, matching Go where they only affect web egress / job admission).

### Changed

- `livekit-voice` logs structured JSON (lowercase `level`, `ts`, `target`, `msg`, fields) matching the reference zap logs, so the promtail json stage extracts a `level` label and level-based filtering works.
- Abnormal media-connection failures (peer connection `Failed`) log the reference `livekit-server` message `dtls timeout: read/write timeout: context deadline exceeded` at warn level, restoring the DTLS-timeout alert; graceful closes stay a debug line.
- The no-worker agent condition now logs the reference `not dispatching agent job since no worker is available`, restoring the livekit-no-worker alert.

### Fixed

- The generic ERROR alert no longer fires spuriously on Rust error-level lines: the level is emitted lowercase (`"level":"error"`) like Go's zap, so uppercase `ERROR` never appears in server output.
- S3 `FileInfo.location` now matches where the object is actually stored. Custom endpoints (R2, MinIO) always report `{endpoint}/{bucket}/{key}` because `object_store` uploads path-style there regardless of `force_path_style`; real AWS reports virtual-hosted or path-style consistently with the PUT. Previously the URL omitted the bucket for custom endpoints, so a client signing a GET against it hit a non-existent bucket and playback broke.
- Failed recordings now report `EGRESS_FAILED` to the server (with the error message), so the `egress_ended` webhook fires and the stored `EgressInfo` is terminal instead of a stale STARTING/ACTIVE state a sweeper could adopt.
- `egress_started` is now deduped against the durable Redis store instead of an in-memory set, so an SFU restart cannot re-fire it for a retried `CreateEgress` (matches the reference `LoadEgress` check).
- Outbound calls now launch the agent: when a room is created after an agent dispatch targeted it (the backend dispatches the room before the SIP participant joins), pending dispatches for that room are launched. Previously outbound calls connected with no agent answering.
- A participant whose media plane fails (`Failed`) is now torn down, so `participant_left` / `room_finished` fire instead of the participant lingering while the signal socket stays alive (reference parity).
- The participant's `Active` transition is now broadcast as a `ParticipantUpdate`, so subscribers and an agent's `wait_for_participant` see the participant as active without waiting for media to publish.
- The RTC config now wires `rtc.node_ip` / `rtc.use_external_ip` (NAT 1:1 host candidate) and `rtc.ips.includes` / `rtc.ips.excludes` (candidate IP filter) into the webrtc-rs setting engine, so browser-visible media uses the configured public addresses instead of auto-discovered interfaces. `rtc.udp_port` / `rtc.port_range_*` still cannot be honored (webrtc-rs 0.12 has no UDP port-range API).
- Webhook delivery now retries with exponential backoff (1 + 5 attempts, 250 ms–4 s) instead of a single retry, so lifecycle events (`room_finished`, `egress_ended`) survive transient backend/network failures.
- A room holding only dependent participants (egress/agent) is now treated as empty, matching the reference `CloseIfEmpty`: it closes after the departure timeout instead of leaking. The recorder's join token now carries `kind: egress` so it is classified as a dependent.
- The recorder mixer drains on the largest track queue (a silent/DTX track can no longer stall the mix or grow other tracks' queues unboundedly) and prunes stale tracks (5 s) along with their decoders.
- The recorder now enforces `cpu_cost` admission: each active recording reserves `room_composite_cpu_cost` against the node's CPU count (Go parity), rejecting jobs when at capacity.
- Uploaded recordings are removed from local disk after a successful non-local upload.
- Active speakers now expire when their audio-level packets stop (`reset_if_stale` was never invoked, so a speaker who stopped talking stayed listed).
- The `room_total` metric counts only rooms this process actually created (a concurrent-join race no longer over-counts).

## [1.13.5.1] - 2026-08-22

### Added

- Benchmark harness for the recorder (`scripts/bench/bench_egress.sh`): runs the Rust and Go egresses against the same real-WebRTC workload, samples egress memory/CPU (process `ps` and Docker cgroup), and reports idle + recording + multi-stream numbers.
- `send_audio` publisher example and a new Go SDK publisher (`scripts/bench/go-publisher/`) so the Go stack can be driven by its native pion client.

### Changed

- `docs/benchmark_livekit_rs_egress.md` now measures both recorders end-to-end on the same host: the Go `livekit/egress` numbers are real recordings (~14% CPU and ~61 MB process RSS vs the Rust ~0.7% and ~12 MB while recording), not idle-only estimates.
- Benchmark docs moved to `docs/` (`docs/benchmark_livekit_rs_voice.md`, `docs/benchmark_livekit_rs_egress.md`).

### Fixed

- Egress websocket connect now times out (10 s) instead of hanging the recording task forever; recording start/end are logged.
- `send_audio` example: handles the Go server's `RefreshToken`/`ParticipantUpdate` interleaving and the `fastPublish` flow, so it works against both the Rust and Go servers.
- psrpc Redis bus: pub/sub subscriptions and the publish connection now reconnect automatically when dropped (e.g. a Redis restart or network blip), instead of silently dying and making subsequent RPCs (egress/SIP dispatch) time out forever. The server also keeps the `IOInfo` psrpc server alive for its full lifetime.
- `livekit-rs-voice:local` / `livekit-rs-egress:local` dev images now include the egress-dispatch and `IOInfo` code (they were built from source predating it).

### Compatibility fixes found by the wire-compat test suite

- Hidden participants no longer leak into `JoinResponse.other_participants`.
- Oversized signal frames now close the websocket with code 1009 (policy violation), matching the reference, instead of 1000.
- CORS headers (`Access-Control-Allow-Origin` echo) are now applied to API responses (the middleware was not wrapping the routes).
- Webhooks are signed with the API **secret** for the configured `webhook.api_key` (hex `HMAC-SHA256(secret, body)`), matching the reference — previously the key string was used.
- `CreateRoom`, `UpdateParticipant`, and `UpdateRoomMetadata` now enforce `limit.max_metadata` / `max_attributes` / `max_room_name_length`, returning `invalid_argument` over the limit.
- `MutePublishedTrack` returns the updated (muted) `TrackInfo`; the mute was applied asynchronously so the response carried the stale state.
- Egress `EgressInfo` now populates `file_results` (plus `ended_at` and the file size), which real clients read, instead of only the deprecated `file` oneof.

### Added

- Wire-compatibility integration test suite mirroring the reference `livekit-server` / `livekit/egress` tests: `crates/lk-server/tests/wire_compat_auth.rs` (token rejection + Twirp permission matrix), `wire_compat_signaling.rs` (join contract, ping, mute, leave, hidden/duplicate participants, attributes, message-size limit), `wire_compat_roomservice.rs` (RoomService lifecycle, not_found/invalid_argument, CORS), `wire_compat_webhook.rs` (lifecycle events + HMAC signature), and `crates/lk-egress/tests/recording.rs` (MP3, multi-track mixing, EGRESS_COMPLETE state reporting).

## [1.13.5] - 2026-08-16

### Added

- Voice-only, LiveKit-wire-compatible SFU in Rust.
- WebSocket signaling: `/rtc`, `/rtc/v1`, `/rtc/validate`, `/rtc/v1/validate` (protobuf-binary by default, JSON on text frames).
- Twirp HTTP API: `livekit.RoomService`, `livekit.AgentDispatchService`, `livekit.SIP`, `livekit.Egress` with protojson-compatible JSON.
- Agent worker WebSocket `/agent` (register / availability / assignment) with `roomConfig.agents` and `CreateDispatch` job launching.
- HS256 JWT auth with `video` grants.
- Webhooks (`room_started`, `room_finished`, `participant_joined`, `participant_left`, `track_published`, `track_unpublished`) signed with `X-Livekit-Signature: hex(HMAC-SHA256(...))`.
- Prometheus `/metrics` on a dedicated port.
- Optional Redis store for SIP/egress container interop.
- Benchmarks: criterion micro-benchmarks + `load_test` harness, and `docs/benchmark_livekit_rs_voice.md` comparing against the Go server.
- TURN relay (RFC 8489/5766) with JoinResponse ICE server credentials.
- Full SIP over the psrpc wire protocol (v0.7 Redis PubSub): outbound `CreateSIPParticipant` / `TransferSIPParticipant` reach a real `livekit/sip` container, and the embedded `IOInfoSIP` service serves inbound calls (trunk authentication, dispatch-rule evaluation, call state).
- `lk` CLI: place outbound SIP calls and manage SIP trunks / dispatch rules through the Twirp API.
- Drop-in Prometheus metrics matching the reference `livekit-server` names, labels, and histogram buckets: rooms, participants, connections, tracks, session latency/duration, connection-quality score, RTP packets, RTCP feedback (NACK/PLI/FIR), per-stream packet loss/out-of-order/jitter/RTT, and forwarding latency (from RTCP sender reports). Existing LiveKit Grafana dashboards work unchanged. See `crates/lk-server/src/metrics.rs`.
- Multi-node clustering over Redis (`redis.cluster: true`).
- `livekit-egress`: a voice-only recorder (WAV/MP3) hosted in this monorepo, dispatched over the psrpc bus and reporting state back via `IOInfo`.
- `docs/benchmark_livekit_rs_egress.md`: recorder footprint measured end-to-end against a real Go `livekit/egress` recording (both stacks in Docker: ~2-3 MB anon RSS / ~2% CPU vs ~38 MB / ~10% while recording, 67.6 MB image vs the Go egress's 4.76 GB).

### Fixed

- ICE candidate handling (JSON framing, buffering before PC creation).
- Publisher track matching by RTP stream id.
- Room empty-timeout, duplicate-identity teardown, agent-job lifecycle.
- Reference-cycle-free media teardown (Weak refs in webrtc callbacks).
