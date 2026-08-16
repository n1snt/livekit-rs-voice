# Usage — clients, agents, SIP, egress

`livekit-rs-voice` is wire-compatible with the LiveKit protocol, so standard
clients and SDKs work unchanged.

## Clients

Any LiveKit client that speaks protocol version 17 (subscriber-primary,
protobuf signaling) connects as-is:

- **Browser**: `livekit-client` (`@livekit/client`, `livekit/components-react`)
  — audio only.
- **Server SDKs**: `livekit-server-sdk` (Node), `livekit-api` (Python), etc.
- **CLI**: `livekit-cli` (`join-room`, `load-test`).

```ts
import { Room, RoomEvent } from "livekit-client";
import { AccessToken } from "livekit-server-sdk";

const room = new Room();
await room.connect("ws://livekit-voice:7880", token);
```

## Agents (`livekit-agents`)

Workers connect over `wss://<host>/agent` and register with
`@server.rtc_session(agent_name=...)`. Room-level jobs are launched from the
join token's `roomConfig.agents` or via `AgentDispatchService.CreateDispatch`.

```python
from livekit.agents import AgentServer, cli, rtc_session

server = AgentServer()

@server.rtc_session(agent_name="voice-agent")
async def voice(ctx):
    # ctx.room, ctx.connect(), AudioSession, TranscriptionSession, ...
    pass

cli.run_app(server)
```

Set `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`,
`LIVEKIT_AGENT_NAME` for the worker.

## Data channels

`_reliable`, `_lossy`, and `_data_track` channels are negotiated on the
subscriber peer connection. `DataPacket`s (user messages, transcriptions,
metrics, RPC, chat, data streams) are forwarded between participants with
destination filtering. Standard topics such as `lk.transcription` work with the
reference transcription UX.

## SIP

- **Trunks & dispatch rules** are exposed via the Twirp `livekit.SIP` API and
  stored in Redis (when configured) so the reference `livekit/sip` container
  services inbound calls.
- **Outbound calls** (`CreateSIPParticipant`) require the LiveKit psrpc message
  bus to reach a `livekit/sip` bridge, which this server does not embed; the
  call returns a clear `failed_precondition` error. Use a `livekit/sip`
  deployment alongside, or place calls via the SIP trunk/dispatch API.

## Egress / recording

`Egress.StartRoomCompositeEgress` (audio-only), `ListEgress`, `StopEgress`, and
related RPCs are served and persisted to Redis so the reference
`livekit/egress` container can pick them up and upload to your storage.

## Webhooks

Events are delivered to `webhook.urls` signed with
`X-Livekit-Signature: hex(HMAC-SHA256(webhook.api_key, body))` — compatible
with LiveKit Cloud-style receivers.
