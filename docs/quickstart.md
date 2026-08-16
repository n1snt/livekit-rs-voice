# Quickstart

Get a `livekit-rs-voice` server running in about five minutes, locally or in
Docker, and verify it with the reference `livekit-cli`.

## 1. Run the server

### From source

```bash
git clone https://github.com/n1snt/livekit-rs-voice.git
cd livekit-rs-voice

# dev mode: loopback bind + placeholder devkey/secret
cargo run --release -p lk-server -- --dev
```

### With Docker

```bash
docker run --rm -p 7880:7880 -p 7881:7881/udp -p 7882:7882 \
  -v "$PWD/examples/livekit.example.yaml:/etc/livekit/livekit.yaml:ro" \
  ghcr.io/n1snt/livekit-rs-voice:latest \
  --config /etc/livekit/livekit.yaml
```

### With docker compose

```bash
docker compose -f examples/docker-compose.yml up -d
```

## 2. Verify it's up

```bash
curl http://localhost:7880/            # -> OK
curl http://localhost:7880/metrics     # 404 on the main port (metrics live on prometheus_port)
```

## 3. Create a token

Install [`livekit-cli`](https://github.com/livekit/livekit-cli), then with the
`devkey`/`secret` dev pair:

```bash
livekit-cli token create \
  --api-key devkey --api-secret secret \
  --identity alice --room my-room --join
```

## 4. Join the room

```bash
livekit-cli join-room --url ws://localhost:7880 \
  --api-key devkey --api-secret secret --identity alice --room my-room
```

The client should connect, receive the `JoinResponse` (subscriber-primary,
protocol 17), negotiate the subscriber data channels, and be able to publish
opus audio. See [usage](./usage.md) for the full client/agent surface.

## Next steps

- [Configuration reference](./configuration.md)
- [Deployment guide](./deployment.md)
- [Benchmark vs the Go server](../benchmark_livekit_rs_voice.md)
