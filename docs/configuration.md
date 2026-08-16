# Configuration reference

`livekit-rs-voice` reads standard LiveKit YAML. Any config file written for the
reference `livekit-server` loads as-is (unknown keys are rejected; legacy keys
such as the top-level `log_level` are accepted).

```bash
livekit-voice --config /path/to/livekit.yaml
# or inline:
LIVEKIT_CONFIG="$(cat livekit.yaml)" livekit-voice
# dev mode (loopback + devkey/secret):
livekit-voice --dev
```

## Top-level keys

| Key | Default | Description |
|---|---|---|
| `port` | `7880` | HTTP/WS signaling port (also serves the Twirp HTTP API). |
| `bind_addresses` | `[]` (all) | Addresses to bind. In `--dev` mode loopback is forced. |
| `keys` | — | Map of API key → secret used to verify HS256 access tokens. **Required.** |
| `logging.level` / legacy `log_level` | `info` | Log level. |
| `region` | — | Region advertised in `ServerInfo`. |
| `node_id` | random `LX…` | Override the advertised node id. |
| `prometheus_port` / `prometheus.port` | unset | Serve `/metrics` on this dedicated port (off the main port). |
| `dev` | `false` | Dev mode: loopback bind + `devkey`/`secret`. |

## `rtc`

| Key | Default | Description |
|---|---|---|
| `udp_port` | `0` | Dedicated UDP media port. When `0`, media uses `tcp_port`. |
| `tcp_port` | `7881` | ICE/TCP fallback port (also the UDP media port when `udp_port` is unset). |
| `port_range_start` / `port_range_end` | `0`/`0` | Inclusive UDP media range (overrides single-port mode). |
| `use_external_ip` | `false` | Prefer the external IP for host candidates. |
| `ips.includes` / `ips.excludes` | `[]` | Host candidate IPs to advertise (CIDRs), e.g. public + private NICs. |
| `node_ip` | — | When set, only this IP is advertised. |
| `packet_buffer_size_audio` | `200` | Jitter/NACK window size for audio. |
| `enable_datachannel_data_tracks` | `true` | Create the `_data_track` lossy channel. |

## `room`

| Key | Default | Description |
|---|---|---|
| `auto_create` | `true` | Create rooms on first join. |
| `empty_timeout` | `300` | Seconds to keep a never-joined room open. |
| `departure_timeout` | `20` | Seconds to keep a room open after the last participant leaves. |
| `max_participants` | `0` | Max non-dependent participants (0 = unlimited). |
| `enabled_codecs` | opus, RED, PCMU, PCMA | Advertised publish codecs. |

## `redis`

| Key | Default | Description |
|---|---|---|
| `address` | — | Redis address (`host:port`). When unset the server runs standalone (in-memory store). |
| `username` / `password` | — | Auth. |
| `db` | `0` | Database index. |
| `use_tls` | `false` | Use `rediss://`. |

When configured, SIP trunks, dispatch rules, and egress state are stored in
Redis using the same hash keys and binary-protobuf encoding as the reference
server (`sip_trunk`, `sip_inbound_trunk`, `sip_outbound_trunk`,
`sip_dispatch_rule`, `egress`, `egress:room:<name>`), so the external
`livekit/sip` and `livekit/egress` containers interoperate.

## `webhook`

| Key | Default | Description |
|---|---|---|
| `urls` | — | URLs to POST events to. |
| `api_key` | — | Secret used to sign webhooks (must match a key in `keys`). |

Webhooks are signed with
`X-Livekit-Signature: hex(HMAC-SHA256(webhook.api_key, raw_body))`. Events:
`room_started`, `room_finished`, `participant_joined`, `participant_left`,
`track_published`, `track_unpublished`.

## `turn`

Accepted for config compatibility (`enabled`, `udp_port`, `tls_port`,
`domain`, `cert_file`, `key_file`, `ttl`). No TURN server is embedded; media
uses host candidates.

## `limit`

Enforced limits: `max_metadata`, `max_attributes`, `max_room_name_length`,
`max_identity_length`, `signal_message_size_limit`,
`agent_signal_message_size_limit`, `max_api_request_body_size`. Go-compatible
aliases (`max_metadata_size`, `max_data_blobs_size`,
`signal_relay.min_retry_interval`, …) are accepted.

## Environment variables

| Variable | Effect |
|---|---|
| `LIVEKIT_CONFIG` | Inline YAML (same as `--config-body`). |
| `LIVEKIT_KEYS` | YAML map overriding `keys`. |
| `LIVEKIT_PORT` | Override `port`. |
| `LIVEKIT_REGION` | Override `region`. |
| `LIVEKIT_REDIS_ADDRESS` | Override `redis.address`. |
| `LIVEKIT_RTC_TCP_PORT` | Override `rtc.tcp_port`. |
| `LIVEKIT_LOG_LEVEL` | Override `logging.level`. |
| `NODE_IP` | Override `rtc.node_ip`. |
| `UDP_PORT` | Override `rtc.udp_port`. |
