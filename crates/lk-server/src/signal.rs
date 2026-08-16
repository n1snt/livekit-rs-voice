//! Signaling: WebSocket transport for `/rtc` and `/rtc/v1`, message codec
//! (protobuf-binary default, JSON on text frames), participant request
//! handling, and room lifecycle callbacks used by the media plane.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lk_proto::livekit as lk;
use prost::Message as _;
use tokio::sync::mpsc;

use crate::core::unix_micros;
use crate::core::ParticipantKind;
use crate::media;
use crate::participant::{Participant, ParticipantState};
use crate::server::Server;
use crate::track::{PublishedTrack, TrackSource};
use crate::{auth, room::Room};

pub const PROTOCOL_VERSION: i32 = 17;
pub const SERVER_VERSION: &str = "1.13.5";
pub const AGENT_PROTOCOL: i32 = 1;
pub const PING_INTERVAL_SECS: i32 = 5;
pub const PING_TIMEOUT_SECS: i32 = 15;

/// Session parameters extracted from the HTTP request / join request.
/// Serialized (JSON) when relaying a join to another cluster node.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SessionParams {
    pub reconnect: bool,
    pub participant_sid: String,
    pub auto_subscribe: bool,
    pub publish: bool,
    pub metadata: String,
    pub attributes: std::collections::BTreeMap<String, String>,
    pub add_track_requests: Vec<lk::AddTrackRequest>,
    pub publisher_offer: Option<lk::SessionDescription>,
    pub sync_state: Option<lk::SyncState>,
}

// ---------------------------------------------------------------------------
// Room-level callbacks invoked by the media plane
// ---------------------------------------------------------------------------

/// Called when media for a published track starts flowing. Broadcasts the
/// participant update to others and auto-subscribes them.
pub async fn on_track_published(participant: &Arc<Participant>, track: &Arc<PublishedTrack>) {
    let Some(room) = participant.room() else {
        return;
    };
    let room_proto = room.to_proto();
    let publisher_info = participant.to_proto();
    let track_info = track.to_proto();

    room.broadcast_participant_update(vec![publisher_info.clone()], Some(&participant.sid));

    // Auto-subscribe all other participants.
    let others = room
        .participants()
        .into_iter()
        .filter(|p| p.sid != participant.sid);
    for subscriber in others {
        if subscriber.can_subscribe()
            && subscriber
                .auto_subscribe
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Err(e) = media::add_subscription(&subscriber, track, participant).await {
                tracing::debug!(subscriber = %subscriber.sid, track = %track.sid, "subscribe failed: {e}");
            }
        }
    }

    if let Some(ctx) = room.context() {
        ctx.metrics
            .track_published
            .with_label_values(&["audio"])
            .inc();
        ctx.metrics
            .track_publish_counter
            .with_label_values(&["audio", "started"])
            .inc();
        ctx.metrics
            .session_start_time
            .with_label_values(&["0", "false"])
            .observe(participant.session_age_ms() as f64);
        ctx.webhook
            .track_published(&room_proto, &publisher_info, &track_info)
            .await;
    }
}

/// Called when media for a published track stops. Unsubscribes everyone and
/// notifies the publisher.
pub async fn on_track_unpublished(participant: &Arc<Participant>, track_sid: &str) {
    let Some(room) = participant.room() else {
        return;
    };
    let Some(track) = participant.remove_track(track_sid) else {
        return;
    };

    let others = room
        .participants()
        .into_iter()
        .filter(|p| p.sid != participant.sid)
        .collect::<Vec<_>>();
    for subscriber in others {
        let _ = media::remove_subscription(&subscriber, track_sid).await;
    }

    room.broadcast_participant_update(vec![participant.to_proto()], Some(&participant.sid));
    participant
        .send(lk::SignalResponse {
            message: Some(lk::signal_response::Message::TrackUnpublished(
                lk::TrackUnpublishedResponse {
                    track_sid: track_sid.to_string(),
                },
            )),
        })
        .await;

    if let Some(ctx) = room.context() {
        ctx.metrics
            .track_published
            .with_label_values(&["audio"])
            .dec();
        ctx.metrics
            .track_publish_counter
            .with_label_values(&["audio", "ended"])
            .inc();
        ctx.webhook
            .track_unpublished(&room.to_proto(), &participant.to_proto(), &track.to_proto())
            .await;
    }
}

/// Best-effort handling of media connection failure. Voice sessions tolerate
/// brief ICE restarts, so we only log here; the signal connection drives
/// participant teardown.
pub async fn on_media_disconnected(_participant: &Arc<Participant>) {
    tracing::debug!(sid = %_participant.sid, "media connection lost");
}

/// Handles a data packet received on a data channel: validates permissions and
/// broadcasts to the room with destination filtering.
pub async fn handle_incoming_data(participant: &Arc<Participant>, data: &[u8]) {
    let Ok(packet) = lk::DataPacket::decode(data) else {
        return;
    };
    let can_publish_data = participant.permission.lock().unwrap().can_publish_data;
    if !can_publish_data {
        return;
    }
    let Some(room) = participant.room() else {
        return;
    };

    // Normalize sender fields and route to destinations.
    let mut out = packet.clone();
    if out.participant_identity.is_empty() {
        out.participant_identity = participant.identity.clone();
    }
    if out.participant_sid.is_empty() {
        out.participant_sid = participant.sid.clone();
    }
    // Never echo back to the sender. Route by explicit sid/identity destinations.
    let dest_identities = out.destination_identities.clone();
    let dest_sids: Vec<String> = match &out.value {
        Some(lk::data_packet::Value::User(user)) => {
            #[allow(deprecated)]
            {
                user.destination_sids.clone()
            }
        }
        _ => Vec::new(),
    };
    let broadcast = dest_identities.is_empty() && dest_sids.is_empty();
    let targets: Vec<Arc<Participant>> = room
        .participants()
        .into_iter()
        .filter(|p| p.sid != participant.sid)
        .filter(|p| {
            broadcast
                || dest_identities.iter().any(|d| *d == p.identity)
                || dest_sids.iter().any(|s| *s == p.sid)
        })
        .collect();
    for target in targets {
        media::send_data_to_participant(&target, &out);
    }
}

// ---------------------------------------------------------------------------
// Participant request handling
// ---------------------------------------------------------------------------

/// Handles a decoded SignalRequest from a participant.
pub async fn handle_participant_request(participant: &Arc<Participant>, req: lk::SignalRequest) {
    use lk::signal_request::Message as M;
    let Some(msg) = req.message else { return };
    match msg {
        M::Offer(offer) => handle_offer(participant, offer).await,
        M::Answer(answer) => handle_answer(participant, answer).await,
        M::Trickle(trickle) => {
            let _ = media::add_ice_candidate(participant, &trickle.candidate_init, trickle.target)
                .await;
        }
        M::AddTrack(at) => handle_add_track(participant, at).await,
        M::Mute(mute) => {
            set_track_muted(participant, &mute.sid, mute.muted, true).await;
        }
        M::Subscription(sub) => {
            handle_update_subscriptions(participant, sub).await;
        }
        M::TrackSetting(_settings) => {
            // Voice-only: no track settings to apply.
        }
        M::Leave(leave) => {
            handle_leave(participant, leave).await;
        }
        M::UpdateMetadata(upd) => {
            handle_update_metadata(participant, upd).await;
        }
        M::PingReq(ping) => {
            let resp = lk::SignalResponse {
                message: Some(lk::signal_response::Message::PongResp(lk::Pong {
                    last_ping_timestamp: ping.timestamp,
                    timestamp: unix_micros() / 1000,
                })),
            };
            participant.send(resp).await;
        }
        M::Ping(_ping) => {
            let resp = lk::SignalResponse {
                message: Some(lk::signal_response::Message::Pong(unix_micros() / 1000)),
            };
            participant.send(resp).await;
        }
        M::SyncState(state) => handle_sync_state(participant, state).await,
        M::SubscriptionPermission(perm) => {
            handle_subscription_permission(participant, perm).await;
        }
        M::UpdateAudioTrack(_)
        | M::UpdateVideoTrack(_)
        | M::PublishDataTrackRequest(_)
        | M::UnpublishDataTrackRequest(_)
        | M::UpdateDataSubscription(_)
        | M::StoreDataBlobRequest(_)
        | M::GetDataBlobRequest(_)
        | M::Simulate(_)
        | M::UpdateLayers(_) => {
            // Not applicable for voice-only; ignore.
        }
    }
}

async fn handle_offer(participant: &Arc<Participant>, offer: lk::SessionDescription) {
    participant.set_state(ParticipantState::Joined);
    if !participant.can_publish() {
        tracing::warn!(sid = %participant.sid, "publish permission denied; ignoring offer");
        return;
    }
    match media::handle_publisher_offer(participant, &offer).await {
        Ok(answer) => {
            let resp = lk::SignalResponse {
                message: Some(lk::signal_response::Message::Answer(answer)),
            };
            participant.send(resp).await;
        }
        Err(e) => tracing::warn!(sid = %participant.sid, "handle publisher offer: {e}"),
    }
}

async fn handle_answer(participant: &Arc<Participant>, answer: lk::SessionDescription) {
    if let Err(e) = media::handle_subscriber_answer(participant, &answer).await {
        tracing::warn!(sid = %participant.sid, "handle subscriber answer: {e}");
    }
}

async fn handle_add_track(participant: &Arc<Participant>, req: lk::AddTrackRequest) {
    if req.r#type != lk::TrackType::Audio as i32 {
        // Voice-only server: reject non-audio publishes politely.
        tracing::warn!(sid = %participant.sid, cid = %req.cid, "rejecting non-audio track publish");
        return;
    }
    if !participant.permission.lock().unwrap().can_publish {
        return;
    }
    let track = Arc::new(PublishedTrack::new(
        req.name,
        req.cid.clone(),
        TrackSource::from_proto(req.source),
        req.stream,
    ));
    participant.add_track(track.clone());
    let resp = lk::SignalResponse {
        message: Some(lk::signal_response::Message::TrackPublished(
            lk::TrackPublishedResponse {
                cid: req.cid,
                track: Some(track.to_proto()),
            },
        )),
    };
    participant.send(resp).await;
}

/// Mutes/unmutes a track. `from_server` is true for RoomService.MutePublishedTrack.
pub async fn set_track_muted(
    participant: &Arc<Participant>,
    sid: &str,
    muted: bool,
    from_server: bool,
) {
    let Some(track) = participant.get_track(sid) else {
        return;
    };
    if !track.set_muted(muted) {
        return;
    }
    if from_server {
        let resp = lk::SignalResponse {
            message: Some(lk::signal_response::Message::Mute(lk::MuteTrackRequest {
                sid: sid.to_string(),
                muted,
            })),
        };
        participant.send(resp).await;
    }
    if let Some(room) = participant.room() {
        room.broadcast_participant_update(vec![participant.to_proto()], Some(&participant.sid));
    }
}

pub async fn handle_update_subscriptions(
    participant: &Arc<Participant>,
    sub: lk::UpdateSubscription,
) {
    let can_subscribe = participant.permission.lock().unwrap().can_subscribe;
    if !can_subscribe {
        return;
    }
    let Some(room) = participant.room() else {
        return;
    };

    // Build a sid -> (publisher, track) map from participant_tracks and track_sids.
    let track_sids: Vec<String> = sub.track_sids;
    for pt in sub.participant_tracks.clone() {
        let Some(publisher) = room.get_participant(&pt.participant_sid) else {
            continue;
        };
        for tid in pt.track_sids {
            if let Some(track) = publisher.get_track(&tid) {
                let result = if sub.subscribe {
                    media::add_subscription(participant, &track, &publisher).await
                } else {
                    media::remove_subscription(participant, &tid).await
                };
                if let Err(e) = result {
                    tracing::warn!(sid = %participant.sid, track = %tid, "subscription: {e}");
                }
            }
        }
    }
    for tid in track_sids {
        // If a participant_tracks entry covered it, skip.
        if sub
            .participant_tracks
            .iter()
            .any(|pt| pt.track_sids.contains(&tid))
        {
            continue;
        }
        let Some((publisher, track)) = find_track(&room, &tid) else {
            continue;
        };
        let result = if sub.subscribe {
            media::add_subscription(participant, &track, &publisher).await
        } else {
            media::remove_subscription(participant, &tid).await
        };
        if let Err(e) = result {
            tracing::warn!(sid = %participant.sid, track = %tid, "subscription: {e}");
        }
    }
}

fn find_track(room: &Room, tid: &str) -> Option<(Arc<Participant>, Arc<PublishedTrack>)> {
    for p in room.participants() {
        if let Some(t) = p.get_track(tid) {
            return Some((p, t));
        }
    }
    None
}

async fn handle_leave(participant: &Arc<Participant>, leave: lk::LeaveRequest) {
    let reason = lk::DisconnectReason::try_from(leave.reason)
        .unwrap_or(lk::DisconnectReason::ClientInitiated);
    end_participant(participant, reason).await;
}

async fn handle_update_metadata(
    participant: &Arc<Participant>,
    upd: lk::UpdateParticipantMetadata,
) {
    let can_update = participant.permission.lock().unwrap().can_update_metadata;
    if !can_update {
        return;
    }
    let name = if upd.name.is_empty() {
        None
    } else {
        Some(upd.name)
    };
    if participant.update_metadata(upd.metadata, name) || !upd.attributes.is_empty() {
        let _ = participant.set_attributes(upd.attributes);
        if let Some(room) = participant.room() {
            room.broadcast_participant_update(vec![participant.to_proto()], None);
        }
    }
}

async fn handle_sync_state(participant: &Arc<Participant>, state: lk::SyncState) {
    // Reconnect: client is telling us its current subscribe/publish state.
    // We trust our own view; nothing to apply for voice-only beyond re-offers.
    for published in state.publish_tracks {
        // Ensure the track exists locally.
        if let Some(t) = &published.track {
            if participant.get_track_by_cid(&published.cid).is_none() {
                let track = Arc::new(PublishedTrack::new(
                    t.name.clone(),
                    published.cid.clone(),
                    TrackSource::from_proto(t.source),
                    t.stream.clone(),
                ));
                participant.add_track(track.clone());
            }
        }
    }
    // Trigger a subscriber renegotiation to resync data channels.
    media::request_subscriber_negotiation(participant);
}

async fn handle_subscription_permission(
    _participant: &Arc<Participant>,
    perm: lk::SubscriptionPermission,
) {
    // Voice-only: permission updates for specific tracks are not enforced
    // beyond the base can_subscribe permission.
    let _ = perm;
}

/// Removes a participant from their room with the given disconnect reason and
/// closes the room if empty.
pub async fn end_participant(participant: &Arc<Participant>, reason: lk::DisconnectReason) {
    tracing::debug!(sid = %participant.sid, reason = ?reason, "end_participant called");
    // If the participant is already marked disconnected AND no longer in the
    // room, the teardown already ran.
    let already_removed = participant
        .room()
        .map(|room| room.get_participant(&participant.sid).is_none())
        .unwrap_or(true);
    if already_removed {
        return;
    }
    participant.set_state(ParticipantState::Disconnected);
    participant
        .disconnected_reason
        .store(reason as i32, std::sync::atomic::Ordering::Relaxed);
    participant.clear_signal_sink();

    let Some(room) = participant.room() else {
        return;
    };
    let room_proto = room.to_proto();
    let info = participant.to_proto();

    room.remove_participant(&participant.sid);
    media::close_participant_media(participant).await;

    // Notify others.
    room.broadcast_participant_update(vec![info.clone()], Some(&participant.sid));
    room.on_participant_left();

    if let Some(ctx) = room.context() {
        ctx.metrics.participant_total.dec();
        ctx.metrics
            .session_duration
            .with_label_values(&["0"])
            .observe(participant.session_age_ms() as f64);
        ctx.webhook.participant_left(&room_proto, &info).await;
    }
}

// ---------------------------------------------------------------------------
// WebSocket transport
// ---------------------------------------------------------------------------

/// Everything that must be delivered in-order right after the websocket is
/// upgraded, before the reader loop starts.
pub struct SignalPrelude {
    pub join: lk::SignalResponse,
    pub publisher_offer: Option<lk::SessionDescription>,
    pub add_tracks: Vec<lk::AddTrackRequest>,
    pub sync_state: Option<lk::SyncState>,
    /// Room-level agent dispatches added by this join (launch once).
    pub launch_agents: Vec<crate::auth::RoomAgentDispatch>,
}

/// The read half of a signaling transport. Implemented over a websocket
/// (`WsIoReader`) or the Redis relay (`RelayIoReader` in `cluster.rs`).
#[async_trait::async_trait]
pub trait SignalIoReader: Send {
    async fn next_request(&mut self) -> Option<lk::SignalRequest>;
}

/// The write half of a signaling transport.
#[async_trait::async_trait]
pub trait SignalIoWriter: Send {
    async fn send(&mut self, resp: &lk::SignalResponse) -> bool;
    async fn close(&mut self);
}

/// A signaling transport with independent read and write halves, so reads (which
/// may block waiting for the next frame) never stall writes (responses, offers,
/// broadcasts).
pub struct SignalIo {
    reader: tokio::sync::Mutex<Box<dyn SignalIoReader>>,
    writer: tokio::sync::Mutex<Box<dyn SignalIoWriter>>,
}

impl SignalIo {
    pub fn new(reader: Box<dyn SignalIoReader>, writer: Box<dyn SignalIoWriter>) -> Self {
        SignalIo {
            reader: tokio::sync::Mutex::new(reader),
            writer: tokio::sync::Mutex::new(writer),
        }
    }

    pub async fn next_request(&self) -> Option<lk::SignalRequest> {
        self.reader.lock().await.next_request().await
    }

    pub async fn send(&self, resp: &lk::SignalResponse) -> bool {
        self.writer.lock().await.send(resp).await
    }

    pub async fn close(&self) {
        self.writer.lock().await.close().await;
    }
}

/// Encodes a response for the websocket wire format (binary or JSON).
pub fn ws_encode(resp: &lk::SignalResponse, json: bool) -> Message {
    if json {
        match serde_json::to_string(resp) {
            Ok(text) => Message::Text(text.into()),
            Err(_) => Message::Binary(resp.encode_to_vec().into()),
        }
    } else {
        Message::Binary(resp.encode_to_vec().into())
    }
}

/// Decodes a websocket frame into a `SignalRequest`, updating the wire mode.
/// Returns `None` for non-data frames or decode failures.
pub fn ws_decode(msg: Message, json: &mut bool) -> Option<lk::SignalRequest> {
    match msg {
        Message::Binary(bytes) => {
            *json = false;
            lk::SignalRequest::decode(bytes.as_ref()).ok()
        }
        Message::Text(text) => {
            *json = true;
            serde_json::from_str::<lk::SignalRequest>(&text).ok()
        }
        _ => None,
    }
}

/// Maps the token's `kind` claim to the participant kind used at join time.
pub fn participant_kind_from_token(token: &auth::VerifiedToken) -> ParticipantKind {
    match token.kind.to_uppercase().as_str() {
        "AGENT" => ParticipantKind::Agent,
        "EGRESS" => ParticipantKind::Egress,
        "SIP" => ParticipantKind::Sip,
        "INGRESS" => ParticipantKind::Ingress,
        _ => ParticipantKind::Standard,
    }
}

struct WsIoReader {
    stream: futures_util::stream::SplitStream<WebSocket>,
    mode: Arc<std::sync::Mutex<bool>>, // false = binary, true = json
}

#[async_trait::async_trait]
impl SignalIoReader for WsIoReader {
    async fn next_request(&mut self) -> Option<lk::SignalRequest> {
        loop {
            let frame = tokio::time::timeout(
                std::time::Duration::from_secs(PING_TIMEOUT_SECS as u64),
                self.stream.next(),
            )
            .await
            .ok()??;
            let frame = frame.ok()?;
            match frame {
                Message::Binary(bytes) => {
                    *self.mode.lock().unwrap() = false;
                    return lk::SignalRequest::decode(bytes.as_ref()).ok();
                }
                Message::Text(text) => {
                    *self.mode.lock().unwrap() = true;
                    return serde_json::from_str::<lk::SignalRequest>(&text).ok();
                }
                Message::Close(_) => return None,
                _ => continue, // ping/pong are answered by tungstenite
            }
        }
    }
}

struct WsIoWriter {
    sink: futures_util::stream::SplitSink<WebSocket, Message>,
    mode: Arc<std::sync::Mutex<bool>>,
}

#[async_trait::async_trait]
impl SignalIoWriter for WsIoWriter {
    async fn send(&mut self, resp: &lk::SignalResponse) -> bool {
        let json = *self.mode.lock().unwrap();
        self.sink.send(ws_encode(resp, json)).await.is_ok()
    }

    async fn close(&mut self) {
        let _ = self
            .sink
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1000,
                reason: "".into(),
            })))
            .await;
    }
}

/// Builds a websocket-backed signal transport.
pub fn ws_io(socket: WebSocket) -> SignalIo {
    let (sink, stream) = socket.split();
    let mode = Arc::new(std::sync::Mutex::new(false));
    SignalIo::new(
        Box::new(WsIoReader {
            stream,
            mode: mode.clone(),
        }),
        Box::new(WsIoWriter { sink, mode }),
    )
}

/// Runs the full signaling session over a `SignalIo`. Sends the
/// `SignalPrelude` (join response, then any publisher offer answer / track
/// acknowledgements), launches room-level agent jobs, negotiates the subscriber
/// connection, and then loops over incoming requests.
pub async fn run_signal_session(
    io: SignalIo,
    participant: Arc<Participant>,
    room: Arc<Room>,
    prelude: SignalPrelude,
) {
    let io = Arc::new(io);
    let (tx, mut rx) =
        mpsc::channel::<lk::SignalResponse>(crate::participant::SIGNAL_CHANNEL_CAPACITY);

    // Hand the outbound channel to the participant.
    let _old = participant.set_signal_sink(tx);
    if let Some(ctx) = room.context() {
        ctx.metrics
            .connection_total
            .with_label_values(&["incoming"])
            .inc();
    }

    // Writer task: the join response is written first (so it is always the
    // first frame the client receives), then the participant's outbound
    // channel is drained.
    let writer_io = io.clone();
    let join = prelude.join.clone();
    let sink_task = tokio::spawn(async move {
        if !writer_io.send(&join).await {
            return;
        }
        while let Some(resp) = rx.recv().await {
            if !writer_io.send(&resp).await {
                break;
            }
        }
    });

    // 1. Post-join responses. Tracks are registered before the publisher offer
    //    so incoming RTP can be matched to them.
    for at in prelude.add_tracks {
        handle_participant_request(
            &participant,
            lk::SignalRequest {
                message: Some(lk::signal_request::Message::AddTrack(at)),
            },
        )
        .await;
    }
    if let Some(offer) = &prelude.publisher_offer {
        handle_participant_request(
            &participant,
            lk::SignalRequest {
                message: Some(lk::signal_request::Message::Offer(offer.clone())),
            },
        )
        .await;
    }
    if let Some(state) = &prelude.sync_state {
        handle_participant_request(
            &participant,
            lk::SignalRequest {
                message: Some(lk::signal_request::Message::SyncState(state.clone())),
            },
        )
        .await;
    }

    // 2. Launch room-level agent dispatches added by this join (once each).
    //    Fire-and-forget: the availability round-trip with the worker must not
    //    block this participant's session.
    for dispatch in prelude.launch_agents {
        if let Some(ctx) = room.context() {
            let agent = ctx.agent.clone();
            let room = room.clone();
            tokio::spawn(async move {
                let _ = agent
                    .launch_room_job(
                        &dispatch.agent_name,
                        &room,
                        &dispatch.metadata,
                        &dispatch.deployment,
                        dispatch.attributes.clone(),
                        None,
                    )
                    .await;
            });
        }
    }

    // 3. Create the subscriber PC, subscribe to already-published tracks, and
    //    negotiate (data channels + subscriptions).
    if let Err(e) = media::setup_subscriber(&participant).await {
        tracing::warn!(sid = %participant.sid, "setup subscriber: {e}");
    }
    let existing: Vec<(Arc<Participant>, Arc<crate::track::PublishedTrack>)> = room
        .participants()
        .into_iter()
        .filter(|p| p.sid != participant.sid && participant.can_subscribe())
        .flat_map(|p| p.tracks().into_iter().map(move |t| (p.clone(), t)))
        .collect();
    tracing::debug!(sid = %participant.sid, existing = existing.len(), "subscribe existing tracks");
    for (publisher, track) in existing {
        if let Err(e) = media::add_subscription(&participant, &track, &publisher).await {
            tracing::debug!(sid = %participant.sid, track = %track.sid, "subscribe existing: {e}");
        }
    }
    media::request_subscriber_negotiation(&participant);

    // 4. Reader loop. The transport enforces its own deadline (websocket) or
    //    blocks until closed (relay); both surface as `None`.
    while let Some(req) = io.next_request().await {
        handle_participant_request(&participant, req).await;
        if participant.state() == ParticipantState::Disconnected {
            break;
        }
    }

    // Signal the transport we are done, then terminate the participant.
    io.close().await;
    if participant.state() != ParticipantState::Disconnected {
        end_participant(&participant, lk::DisconnectReason::SignalClose).await;
    }
    if let Some(ctx) = room.context() {
        ctx.metrics
            .connection_total
            .with_label_values(&["incoming"])
            .dec();
    }
    let _ = sink_task.await;
}

/// Joins a room and runs the session core over the given transport. Shared by
/// the local websocket path and the remote relay path (`cluster.rs`).
pub async fn run_session_with_io(
    io: SignalIo,
    server: &Arc<Server>,
    token: auth::VerifiedToken,
    params: SessionParams,
    kind: ParticipantKind,
) -> Result<(), String> {
    let room_name = token.video.room.clone();
    let (room, participant, launch_agents) = match join_room(server, &token, &params, kind).await {
        Ok(x) => x,
        Err(e) => {
            // The room registry claim may have been made for this node but the
            // room never came to exist here; release it so future joins can
            // reclaim the room elsewhere.
            if server.get_room(&room_name).is_none() {
                server.cluster.release_room(&room_name).await;
            }
            return Err(e);
        }
    };
    let join_response = build_join_response(&room, &participant, server);
    let prelude = SignalPrelude {
        join: lk::SignalResponse {
            message: Some(lk::signal_response::Message::Join(join_response)),
        },
        publisher_offer: params.publisher_offer.clone(),
        add_tracks: params.add_track_requests.clone(),
        sync_state: params.sync_state.clone(),
        launch_agents,
    };
    run_signal_session(io, participant, room, prelude).await;
    Ok(())
}

/// Closes the signal connection (used by RoomService.RemoveParticipant and
/// room shutdown).
pub fn close_signal(participant: &Arc<Participant>, reason: lk::DisconnectReason) {
    let resp = lk::SignalResponse {
        message: Some(lk::signal_response::Message::Leave(lk::LeaveRequest {
            reason: reason as i32,
            ..Default::default()
        })),
    };
    // Best-effort; the participant's state flips to disconnected, which stops
    // the reader loop after the next request.
    participant.send_update(resp);
    if participant.state() != ParticipantState::Disconnected {
        participant.set_state(ParticipantState::Disconnected);
        participant
            .disconnected_reason
            .store(reason as i32, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Joins a room for a verified token and returns the participant + room.
pub async fn join_room(
    server: &Arc<Server>,
    token: &auth::VerifiedToken,
    params: &SessionParams,
    kind: ParticipantKind,
) -> Result<
    (
        Arc<Room>,
        Arc<Participant>,
        Vec<crate::auth::RoomAgentDispatch>,
    ),
    String,
> {
    let grant = &token.video;
    if !grant.room_join {
        return Err("join permission denied".to_string());
    }
    if token.identity.is_empty() {
        return Err("identity is required".to_string());
    }
    let room_name = if grant.room.is_empty() {
        return Err("room is required".to_string());
    } else {
        grant.room.clone()
    };
    if !params.publish && !token.can_subscribe() {
        return Err("subscribe permission denied".to_string());
    }

    let room = server.get_or_create_room(&room_name);

    // Duplicate identity: terminate the existing session.
    if let Some(existing) = room.get_participant_by_identity(&token.identity) {
        tracing::info!(room = %room.name, identity = %token.identity, "duplicate identity joining");
        close_signal(&existing, lk::DisconnectReason::DuplicateIdentity);
        end_participant(&existing, lk::DisconnectReason::DuplicateIdentity).await;
    }

    let first_join = !room.ever_joined.load(std::sync::atomic::Ordering::Relaxed);

    let participant = Participant::new(
        token.identity.clone(),
        token.name.clone(),
        token.metadata.clone(),
        kind,
    );
    participant.attach_room(Arc::downgrade(&room));
    participant
        .auto_subscribe
        .store(params.auto_subscribe, std::sync::atomic::Ordering::Relaxed);
    participant.set_attributes(token.attributes.clone());
    *participant.permission.lock().unwrap() = Participant::permission_from_grant(grant);
    if !params.metadata.is_empty() {
        *participant.metadata.lock().unwrap() = params.metadata.clone();
    }

    // Room-level agent dispatches from the token (deduplicated).
    let mut new_dispatches = Vec::new();
    if let Some(rc) = &token.room_config {
        for agent in &rc.agents {
            let key = format!("{}\0{}", agent.agent_name, agent.metadata);
            if room.add_agent_dispatch(agent.clone()) {
                new_dispatches.push(agent.clone());
                room.mark_dispatch_launched(&key);
            }
        }
    }

    if !room.join(participant.clone()) {
        return Err("room is full or closed".to_string());
    }

    let room_proto = room.to_proto();
    let info = participant.to_proto();

    // Fire room_started before participant_joined (webhook ordering).
    if first_join {
        if let Some(ctx) = room.context() {
            ctx.webhook.room_started(&room_proto).await;
        }
    }
    if let Some(ctx) = room.context() {
        ctx.metrics.participant_total.inc();
        ctx.metrics
            .participant_join
            .with_label_values(&["signal_connected", ""])
            .inc();
    }

    // Notify other participants + webhook.
    room.broadcast_participant_update(vec![info.clone()], Some(&participant.sid));
    if let Some(ctx) = room.context() {
        ctx.webhook.participant_joined(&room_proto, &info).await;
    }

    Ok((room, participant, new_dispatches))
}

/// Builds the JoinResponse sent to a freshly joined participant.
pub fn build_join_response(
    room: &Arc<Room>,
    participant: &Arc<Participant>,
    server: &Arc<Server>,
) -> lk::JoinResponse {
    let others = room
        .participants()
        .into_iter()
        .filter(|p| p.sid != participant.sid)
        .map(|p| p.to_proto())
        .collect();

    let codecs = server
        .config
        .enabled_codec_mimes()
        .into_iter()
        .map(|mime| lk::Codec {
            mime: mime.to_string(),
            fmtp_line: String::new(),
        })
        .collect();

    lk::JoinResponse {
        room: Some(room.to_proto()),
        participant: Some(participant.to_proto()),
        other_participants: others,
        server_version: SERVER_VERSION.to_string(),
        ice_servers: crate::turn::ice_servers(
            &server.config,
            &server.keys.as_map(),
            &participant.sid,
        ),
        subscriber_primary: true,
        client_configuration: Some(lk::ClientConfiguration::default()),
        server_region: server.config.region.clone(),
        ping_timeout: PING_TIMEOUT_SECS,
        ping_interval: PING_INTERVAL_SECS,
        server_info: Some(lk::ServerInfo {
            edition: lk::server_info::Edition::Standard as i32,
            version: SERVER_VERSION.to_string(),
            protocol: PROTOCOL_VERSION,
            region: server.config.region.clone(),
            node_id: server.node_id.clone(),
            agent_protocol: AGENT_PROTOCOL,
            ..Default::default()
        }),
        enabled_publish_codecs: codecs,
        fast_publish: true,
        ..Default::default()
    }
}
