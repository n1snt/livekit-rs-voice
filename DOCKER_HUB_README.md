# livekit-rs-voice

A minimal, voice-only LiveKit server in Rust — a drop-in replacement for the audio path of `livekit-server`. It speaks the same LiveKit wire protocol, so existing LiveKit clients, `livekit-agents`, and the `livekit/sip` + `livekit/egress` containers connect and work unchanged.

## How to use this image

```bash
docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882 \
  -v "$PWD/livekit.yaml:/etc/livekit/livekit.yaml:ro" \
  n1snt/livekit-rs-voice:latest --config /etc/livekit/livekit.yaml
```

Minimal `livekit.yaml`:

```yaml
port: 7880
keys:
  devkey: your-api-secret
```

> For production, generate a key with at least 32 characters. See the full [LiveKit configuration reference](https://docs.livekit.io).

## What's inside

- LiveKit-compatible signaling: `/rtc`, `/rtc/v1` WebSockets and the Twirp API
- Voice-only media plane (audio forwarding, active-speaker detection)
- SIP interoperability with the `livekit/sip` container (inbound + outbound) over Redis (psrpc)
- Egress support via the `livekit/egress` container
- Drop-in `livekit-server` Prometheus metrics

## Tags

- `latest` — latest release
- `1.13.5` — versioned release

Multi-arch: `linux/amd64`, `linux/arm64`.

## Documentation

- [Project README](https://github.com/n1snt/livekit-rs-voice)
- [Benchmark: Rust vs Go livekit-server](benchmark_livekit_rs_voice.md)

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
