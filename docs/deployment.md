# Deployment

`livekit-rs-voice` is a single static binary. It needs no database at runtime
(Redis is optional, for SIP/egress container interop), so it deploys like a
regular service: Docker, Kubernetes, systemd, or a container orchestrator of
your choice.

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| `7880` | TCP | HTTP + WebSocket signaling, `/rtc`, `/rtc/v1`, `/agent`, Twirp API, health |
| `7881` | UDP + TCP | Media (UDP primary, TCP fallback) |
| `7882` | TCP | Optional second media port |
| `6789` | TCP | Prometheus `/metrics` (only when `prometheus_port` is set) |
| `5060` | — | Not served by this server (that's `livekit/sip`) |

## Health checks

- `GET /` returns `OK` (with a 406 when the server is shutting down).
- Metrics: `GET /metrics` on the configured `prometheus_port`, which includes
  the `livekit_room_total` gauge required by typical deploy healthchecks.

## Docker

See [quickstart](./quickstart.md). The image is published to GHCR on version
tags:

```bash
docker run -d --name livekit-voice \
  -p 7880:7880 -p 7881:7881/udp -p 7881:7881/tcp -p 7882:7882 \
  -v "$PWD/livekit.yaml:/etc/livekit/livekit.yaml:ro" \
  ghcr.io/n1snt/livekit-rs-voice:latest \
  --config /etc/livekit/livekit.yaml
```

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: livekit-voice
spec:
  replicas: 1
  selector:
    matchLabels: { app: livekit-voice }
  template:
    metadata:
      labels: { app: livekit-voice }
    spec:
      containers:
        - name: livekit-voice
          image: ghcr.io/n1snt/livekit-rs-voice:latest
          args: ["--config", "/etc/livekit/livekit.yaml"]
          ports:
            - { name: signal, containerPort: 7880 }
            - { name: media,  containerPort: 7881, protocol: UDP }
            - { name: media-tcp, containerPort: 7881 }
            - { name: media-tcp2, containerPort: 7882 }
          volumeMounts:
            - { name: config, mountPath: /etc/livekit }
          livenessProbe:
            httpGet: { path: /, port: 7880 }
          readinessProbe:
            httpGet: { path: /, port: 7880 }
      volumes:
        - name: config
          configMap:
            name: livekit-voice-config
```

> Media uses host candidates (`rtc.ips.includes` or `rtc.node_ip`), so clients
> must be able to reach the advertised IPs on the UDP media port. In
> `network_mode: host` / `hostNetwork` deployments this is automatic.

## Webhooks

Events are POSTed to `webhook.urls` with
`X-Livekit-Signature: hex(HMAC-SHA256(webhook.api_key, body))`. Verify the
signature before trusting events (see
[configuration](./configuration.md#webhook)).

## Metrics

Sample Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: livekit-voice
    metrics_path: /metrics
    static_configs:
      - targets: ["livekit-voice:6789"]
```

## Notes on scale

- This is a **single-node** server; there is no Redis-based clustering.
- Under sustained connection churn the system allocator retains freed memory
  (the RSS floor stays elevated). Steady-state memory is ~330 KB/connection.
  If OS-return behaviour matters, run with jemalloc/mimalloc or call
  `malloc_trim` periodically.
- For the reference `livekit/sip` and `livekit/egress` containers, configure
  `redis.address` and point those containers at the same Redis.
