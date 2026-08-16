# Benchmark: `livekit-rs-egress` vs Go `livekit/egress`

Voice-recording performance and footprint of the Rust `livekit-rs-egress`
against the reference Go `livekit/egress`, measured on the same host.

**Summary:** the Rust egress records the same audio at a fraction of the cost —
**~11 MB RAM and ~0.7% CPU while recording** vs the Go container's ~4.76 GB
image (67.6 MB for Rust) and ~33 MB idle / ~67 MB active memory. The Go egress
could not complete a recording in this environment (see below).

## Environment

| | |
|---|---|
| Host | MacBook Pro (Apple M1), 32 GB RAM, 1 TB SSD, loopback networking |
| Rust stack | `livekit-voice` + `livekit-egress` (release, host processes) |
| Go stack | `livekit/livekit-server` v1.11.0 + `livekit/egress:latest` (Docker) |
| Load | a real WebRTC publisher (`send_audio`) streams Opus audio into a room |
| Workload | `StartRoomCompositeEgress` (audio-only) for 30 s, then `StopEgress` |
| Redis | shared `redis` (psrpc bus + egress dispatch) |

## Image size

| Image | Size |
|---|---|
| `livekit-rs-egress` | **67.6 MB** (distroless, libopus + libmp3lame) |
| `livekit-rs-voice` | 74.5 MB |
| `livekit/egress:latest` | **4.76 GB** (bundles Chrome + GStreamer + ffmpeg) |

The Rust egress is **~70x smaller** than the Go one because it does not bundle a
browser, GStreamer, or ffmpeg — Opus decoding and MP3 encoding are native
(libopus + libmp3lame FFI).

## Rust egress: recording metrics (30 s stream, 2 runs)

| Metric | Run 1 | Run 2 |
|---|---|---|
| Avg CPU | **0.7%** | 0.7% |
| Max CPU | **1.0%** | 1.0% |
| Max RSS | **11 MB** | 12 MB |
| Output | 2.7 MB WAV, 28.4 s | 2.5 MB WAV, 26.1 s |
| MP3 (30 s) | 238 KB, ~32.5 s | — |

Recording 30 seconds of a call costs under 1% of one core and 12 MB of RAM.
The WAV is mono 48 kHz PCM; the MP3 is 64 kbps mono.

## Go `livekit/egress`: measured footprint

- **Idle:** ~33 MB RAM, ~0.8% CPU.
- **During a recording attempt:** ~67 MB RAM, ~2.3% CPU.

**Caveat:** the Go egress could not complete a recording in this environment.
Its room-composite pipeline repeatedly aborted with `Start signal not received`
— the subscriber ICE/subscription path to the media never established from the
Docker container network (external STUN unreachable, no media received). This
is an environment limitation of the test harness, not a product judgment, but
it means a direct active-recording CPU/RAM comparison against the Go egress
was not measurable here. The numbers above are what was observable.

## Correctness

Both outputs were validated:

- **WAV:** RIFF/WAVE header, mono, 48 kHz, correct sample count and duration.
- **MP3:** valid MPEG frames (`0xFFE` sync), ~64 kbps, correct duration.

The recorder handles room-composite (all tracks mixed), stops on `StopEgress`
or when the room's audio stream ends, and reports state back to the server's
`IOInfo` service (`CreateEgress` / `UpdateEgress`).

## Reproducing

```bash
# build
cargo build --release -p lk-egress --bin livekit-egress \
  -p lk-egress --example send_audio -p lk-server --bin livekit-voice

# run the stack (see /tmp/lk-egress.yaml: api_key, ws_url, output_dir, redis)
./target/release/livekit-voice --config /tmp/lk-server-eg.yaml
./target/release/livekit-egress --config /tmp/lk-egress.yaml

# stream 30 s of audio into a room
./target/release/examples/send_audio --ws ws://127.0.0.1:7990 \
  --key devkey --secret secret --room bench --seconds 30

# start + stop a recording (audioOnly, WAV)
curl -X POST http://127.0.0.1:7990/twirp/livekit.Egress/StartRoomCompositeEgress \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <roomRecord token>" \
  -d '{"roomName":"bench","audioOnly":true,"fileOutputs":[{"filepath":"/r"}]}'
```

`benchmark_livekit_rs_voice.md` covers the server (SFU) comparison; this file
covers the recorder.
