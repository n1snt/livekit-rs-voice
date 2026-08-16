//! Twirp service implementations: `livekit.RoomService` and
//! `livekit.AgentDispatchService`.

use std::sync::Arc;

use lk_proto::livekit as lk;

use crate::http::{Req, TwirpError, WireFormat};
use crate::server::Server;

/// Checks `roomAdmin` permission and room match (reference `EnsureAdminPermission`).
fn ensure_admin(req: &Req, room: &str) -> Result<(), TwirpError> {
    if req.token.video.room_admin && req.token.video.room == room {
        Ok(())
    } else {
        Err(TwirpError::permission_denied(
            "room admin permission denied",
        ))
    }
}

fn ensure_create(req: &Req) -> Result<(), TwirpError> {
    if req.token.video.room_create {
        Ok(())
    } else {
        Err(TwirpError::permission_denied(
            "roomCreate permission denied",
        ))
    }
}

fn ensure_list(req: &Req) -> Result<(), TwirpError> {
    if req.token.video.room_list {
        Ok(())
    } else {
        Err(TwirpError::permission_denied("roomList permission denied"))
    }
}

/// Dispatches a `livekit.RoomService` RPC.
pub fn room_service(
    server: &Arc<Server>,
    method: &str,
    req: &Req,
    body: &[u8],
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    let token = &req.token;
    let _ = token;
    match method {
        "CreateRoom" => {
            ensure_create(req)?;
            let r: lk::CreateRoomRequest = parse(body, format)?;
            let name = if r.name.is_empty() {
                return Err(TwirpError::invalid_argument("room name is required"));
            } else {
                r.name
            };
            let existed = server.get_room(&name).is_some();
            let room = server.get_or_create_room(&name);
            if !r.metadata.is_empty() {
                room.update_metadata(r.metadata);
            }
            if r.empty_timeout != 0 {
                room.set_empty_timeout(r.empty_timeout);
            }
            if r.departure_timeout != 0 {
                room.set_departure_timeout(r.departure_timeout);
            }
            if r.max_participants != 0 {
                room.set_max_participants(r.max_participants);
            }
            for agent in r.agents {
                room.add_agent_dispatch(crate::auth::RoomAgentDispatch {
                    agent_name: agent.agent_name,
                    metadata: agent.metadata,
                    deployment: agent.deployment,
                    attributes: agent.attributes,
                });
            }
            if !existed {
                let room = room.clone();
                let proto = room.to_proto();
                tokio::spawn(async move {
                    if let Some(ctx) = room.context() {
                        ctx.webhook.room_started(&proto).await;
                    }
                });
            }
            write(&room.to_proto(), format)
        }
        "ListRooms" => {
            ensure_list(req)?;
            let r: lk::ListRoomsRequest = parse(body, format)?;
            let mut rooms = Vec::new();
            for room in server.list_rooms() {
                if r.names.is_empty() || r.names.contains(&room.name) {
                    rooms.push(room.to_proto());
                }
            }
            write(&lk::ListRoomsResponse { rooms }, format)
        }
        "DeleteRoom" => {
            ensure_create(req)?;
            let r: lk::DeleteRoomRequest = parse(body, format)?;
            if r.room.is_empty() {
                return Err(TwirpError::invalid_argument("room is required"));
            }
            if !server.get_room(&r.room).is_some() {
                return Err(TwirpError::not_found("room not found"));
            }
            let room = server.get_room(&r.room).unwrap();
            let proto = room.to_proto();
            let info: lk::ParticipantInfo = Default::default();
            let _ = info;
            let server2 = server.clone();
            tokio::spawn(async move {
                if let Some(ctx) = room.context() {
                    ctx.webhook.room_finished(&proto).await;
                }
                let _ = server2
                    .close_room(&r.room, lk::DisconnectReason::RoomDeleted)
                    .await;
            });
            write(&lk::DeleteRoomResponse {}, format)
        }
        "ListParticipants" => {
            let r: lk::ListParticipantsRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participants = room.participants().iter().map(|p| p.to_proto()).collect();
            write(&lk::ListParticipantsResponse { participants }, format)
        }
        "GetParticipant" => {
            let r: lk::RoomParticipantIdentity = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            write(&participant.to_proto(), format)
        }
        "RemoveParticipant" => {
            let r: lk::RoomParticipantIdentity = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            let server2 = server.clone();
            tokio::spawn(async move {
                let _ = server2;
                crate::signal::end_participant(
                    &participant,
                    lk::DisconnectReason::ParticipantRemoved,
                )
                .await;
            });
            write(&lk::RemoveParticipantResponse {}, format)
        }
        "MutePublishedTrack" => {
            let r: lk::MuteRoomTrackRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            let track = participant
                .get_track(&r.track_sid)
                .ok_or_else(|| TwirpError::not_found("track not found"))?;
            let room2 = room.clone();
            let p = participant.clone();
            let sid = r.track_sid.clone();
            tokio::spawn(async move {
                crate::signal::set_track_muted(&p, &sid, r.muted, true).await;
                let _ = room2;
            });
            write(
                &lk::MuteRoomTrackResponse {
                    track: Some(track.to_proto()),
                },
                format,
            )
        }
        "UpdateParticipant" => {
            let r: lk::UpdateParticipantRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            let mut changed = participant.update_metadata(
                r.metadata.clone(),
                if r.name.is_empty() {
                    None
                } else {
                    Some(r.name.clone())
                },
            );
            if let Some(permission) = r.permission {
                changed |= participant.update_permission(permission);
            }
            if !r.attributes.is_empty() {
                changed |= participant.set_attributes(r.attributes);
            }
            if changed {
                room.broadcast_participant_update(vec![participant.to_proto()], None);
            }
            write(&participant.to_proto(), format)
        }
        "UpdateSubscriptions" => {
            let r: lk::UpdateSubscriptionsRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            let participant = room
                .get_participant_by_identity(&r.identity)
                .ok_or_else(|| TwirpError::not_found("participant not found"))?;
            let sub = lk::UpdateSubscription {
                track_sids: r.track_sids,
                subscribe: r.subscribe,
                participant_tracks: r.participant_tracks,
            };
            tokio::spawn(async move {
                crate::signal::handle_update_subscriptions(&participant, sub).await;
            });
            write(&lk::UpdateSubscriptionsResponse {}, format)
        }
        "SendData" => {
            let r: lk::SendDataRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            #[allow(deprecated)]
            let packet = lk::DataPacket {
                kind: r.kind,
                destination_identities: r.destination_identities,
                value: Some(lk::data_packet::Value::User(lk::UserPacket {
                    payload: r.data,
                    topic: r.topic,
                    id: Some(crate::core::generate_id("MSG_")),
                    ..Default::default()
                })),
                ..Default::default()
            };
            room.broadcast_data(packet, &[]);
            write(&lk::SendDataResponse {}, format)
        }
        "UpdateRoomMetadata" => {
            let r: lk::UpdateRoomMetadataRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let room = server
                .get_room(&r.room)
                .ok_or_else(|| TwirpError::not_found("room not found"))?;
            if room.update_metadata(r.metadata) {
                room.broadcast_room_update();
            }
            write(&room.to_proto(), format)
        }
        "ForwardParticipant" | "MoveParticipant" | "PerformRpc" => Err(
            TwirpError::failed_precondition("not supported on this server"),
        ),
        _ => Err(TwirpError::not_found(format!("method not found: {method}"))),
    }
}

/// Dispatches a `livekit.AgentDispatchService` RPC.
pub fn agent_dispatch_service(
    server: &Arc<Server>,
    method: &str,
    req: &Req,
    body: &[u8],
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    match method {
        "CreateDispatch" => {
            let r: lk::CreateAgentDispatchRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            if r.agent_name.is_empty() {
                return Err(TwirpError::invalid_argument("agent name is required"));
            }
            if r.room.is_empty() {
                return Err(TwirpError::invalid_argument("room is required"));
            }
            let d = server.context.agent.create_dispatch(
                r.agent_name,
                r.room,
                r.metadata,
                r.deployment,
                r.attributes,
            );
            let info = dispatch_to_proto(&d);
            // If the room exists, launch the job immediately.
            if let Some(room) = server.get_room(&d.room) {
                let agent = server.context.agent.clone();
                let room2 = room.clone();
                let metadata = d.metadata.clone();
                let deployment = d.deployment.clone();
                let attributes = d.attributes.clone();
                let agent_name = d.agent_name.clone();
                let dispatch_id = d.id.clone();
                tokio::spawn(async move {
                    if let Ok(job_id) = agent
                        .launch_room_job(
                            &agent_name,
                            &room2,
                            &metadata,
                            &deployment,
                            attributes,
                            Some(&dispatch_id),
                        )
                        .await
                    {
                        let _ = job_id;
                        let _ = dispatch_id;
                    }
                });
            }
            write(&info, format)
        }
        "DeleteDispatch" => {
            let r: lk::DeleteAgentDispatchRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let d = server
                .context
                .agent
                .delete_dispatch(&r.dispatch_id)
                .ok_or_else(|| TwirpError::not_found("dispatch not found"))?;
            // Terminate any running jobs for this dispatch.
            for job in &d.jobs {
                let workers = server.context.agent.workers_for(&job.agent_name);
                for w in workers {
                    w.send(lk::ServerMessage {
                        message: Some(lk::server_message::Message::Termination(
                            lk::JobTermination {
                                job_id: job.id.clone(),
                            },
                        )),
                    });
                }
            }
            write(&dispatch_to_proto(&d), format)
        }
        "ListDispatch" => {
            let r: lk::ListAgentDispatchRequest = parse(body, format)?;
            ensure_admin(req, &r.room)?;
            let dispatches = if r.dispatch_id.is_empty() {
                server.context.agent.list_dispatches(&r.room)
            } else {
                server
                    .context
                    .agent
                    .get_dispatch(&r.dispatch_id)
                    .into_iter()
                    .collect()
            };
            let agent_dispatches = dispatches.iter().map(dispatch_to_proto).collect();
            write(&lk::ListAgentDispatchResponse { agent_dispatches }, format)
        }
        _ => Err(TwirpError::not_found(format!("method not found: {method}"))),
    }
}

fn dispatch_to_proto(d: &crate::agent::AgentDispatch) -> lk::AgentDispatch {
    let state = lk::AgentDispatchState {
        jobs: d.jobs.clone(),
        created_at: d.created_at * 1_000_000_000,
        deleted_at: d.deleted_at.unwrap_or_default() * 1_000_000_000,
    };
    lk::AgentDispatch {
        id: d.id.clone(),
        agent_name: d.agent_name.clone(),
        room: d.room.clone(),
        metadata: d.metadata.clone(),
        state: Some(state),
        deployment: d.deployment.clone(),
        attributes: d.attributes.clone(),
        ..Default::default()
    }
}

fn parse<T: serde::de::DeserializeOwned + prost::Message + Default>(
    body: &[u8],
    format: WireFormat,
) -> Result<T, TwirpError> {
    crate::http::parse_body(body, format)
}

fn write<T: serde::Serialize + prost::Message>(
    msg: &T,
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    crate::http::write_body(msg, format)
}
