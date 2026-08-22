# livekit-rs-egress

A minimal, voice-only LiveKit recorder in Rust — a drop-in replacement for `livekit/egress`. It receives egress jobs from a LiveKit server over Redis (psrpc), joins rooms as a subscriber, and records the room audio to WAV/MP3 files. It works with `livekit-rs-voice` or any LiveKit server that dispatches egress over the psrpc bus.

## How to use this image

The egress runs alongside a LiveKit server and a shared Redis instance. Mount a config:

```bash
docker run --rm \
  -v "$PWD/egress.yaml:/etc/livekit-egress/egress.yaml:ro" \
  -v "$PWD/out:/out" \
  n1snt/livekit-rs-egress:latest --config /etc/livekit-egress/egress.yaml
```

Minimal `egress.yaml`:

```yaml
api_key: devkey
api_secret: your-api-secret
ws_url: ws://livekit-voice:7880
output_dir: /out
redis:
  address: redis:6379
```

Start recordings through the server's `livekit.Egress` Twirp API — e.g. `StartRoomCompositeEgress` with `audioOnly: true`. The recorder subscribes to the room's audio, mixes it, and writes a mono 48 kHz WAV (or MP3 when requested) to `output_dir`. Recording jobs are dispatched and state is reported back over Redis (psrpc), so no HTTP exposure is needed on the egress itself.

## Tags

- `latest` — latest release
- `1.13.5.1` — versioned release, always matched to the same `livekit-rs-voice` release (see the [versioning policy](https://github.com/n1snt/livekit-rs-voice/blob/main/docs/versioning.md))

Multi-arch: `linux/amd64`, `linux/arm64`.

## Documentation

- [Project README](https://github.com/n1snt/livekit-rs-voice)
- [Benchmark: Rust vs Go livekit-egress](https://github.com/n1snt/livekit-rs-voice/blob/main/docs/benchmark_livekit_rs_egress.md)

## License

Apache-2.0. See [LICENSE](https://github.com/n1snt/livekit-rs-voice/blob/main/LICENSE) and [NOTICE](https://github.com/n1snt/livekit-rs-voice/blob/main/NOTICE).