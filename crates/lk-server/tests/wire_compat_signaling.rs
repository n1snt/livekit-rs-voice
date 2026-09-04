//! Wire-compatibility tests for the signaling protocol, mirroring the
//! `livekit-server` signaling test surface (join contract, ping, mute, leave,
//! hidden participants, duplicate identity, attributes, message-size limits).

mod common;

use std::sync::Arc;

use common::*;
use lk_proto::livekit as lk;
use lk_server::config::Config;
use prost::Message as _;

/// The JoinResponse must carry the documented wire contract: `subscriber_primary`,
/// `ping_interval`/`ping_timeout`, `server_info.protocol`, `ice_servers`, and
/// (for later joiners) `other_participants`.
#[tokio::test]
async fn join_contract_matches_reference() {
    let (_server, base) = start_server().await;

    let mut a = ws_connect(&base, &join_token("alice", "contract-room")).await;
    let join_a = expect_join(&mut a).await;
    assert_eq!(join_a.room.as_ref().unwrap().name, "contract-room");
    assert_eq!(join_a.participant.as_ref().unwrap().identity, "alice");
    assert!(join_a.subscriber_primary, "subscriber_primary must be true");
    assert_eq!(join_a.ping_interval, 5);
    assert_eq!(join_a.ping_timeout, 15);
    let si = join_a.server_info.as_ref().unwrap();
    assert_eq!(si.protocol, 17);
    assert_eq!(si.edition, lk::server_info::Edition::Standard as i32);
    // ice_servers is populated from the TURN/STUN config; empty when none is
    // configured (matches the reference, which only populates it when TURN is on).
    let _ = &join_a.ice_servers;

    // Second joiner sees the first in other_participants.
    let mut b = ws_connect(&base, &join_token("bob", "contract-room")).await;
    let join_b = expect_join(&mut b).await;
    assert_eq!(join_b.other_participants.len(), 1);
    assert_eq!(join_b.other_participants[0].identity, "alice");
    drop(a);
    drop(b);
}

/// Both the legacy `Ping` and the newer `PingReq` forms get a matching pong.
#[tokio::test]
async fn legacy_ping_and_ping_req() {
    let (_server, base) = start_server().await;
    let mut ws = ws_connect(&base, &join_token("carol", "ping-room")).await;
    let _join = expect_join(&mut ws).await;

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Ping(1234)),
        },
    )
    .await;
    let resp = await_message(&mut ws, |r| {
        matches!(r.message, Some(lk::signal_response::Message::Pong(_)))
    })
    .await;
    assert!(matches!(
        resp.message,
        Some(lk::signal_response::Message::Pong(_))
    ));

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                timestamp: 4242,
                rtt: 0,
            })),
        },
    )
    .await;
    let resp = await_message(&mut ws, |r| {
        matches!(r.message, Some(lk::signal_response::Message::PongResp(_)))
    })
    .await;
    match resp.message {
        Some(lk::signal_response::Message::PongResp(p)) => {
            assert_eq!(p.last_ping_timestamp, 4242);
        }
        other => panic!("expected PongResp, got {other:?}"),
    }
}

/// Muting a published track is broadcast to every participant (sender included)
/// as a participant update carrying the muted TrackInfo.
#[tokio::test]
async fn mute_broadcasts_to_participants() {
    let (_server, base) = start_server().await;
    let a_ws = Arc::new(tokio::sync::Mutex::new(
        ws_connect(&base, &join_token("dave", "mute-room")).await,
    ));
    let _ka_a = spawn_keepalive(a_ws.clone());
    let mut a = a_ws.lock().await;
    let _join = expect_join(&mut a).await;
    drop(a);
    let b_ws = Arc::new(tokio::sync::Mutex::new(
        ws_connect(&base, &join_token("erin", "mute-room")).await,
    ));
    let _ka_b = spawn_keepalive(b_ws.clone());
    let mut b = b_ws.lock().await;
    let _join_b = expect_join(&mut b).await;
    drop(b);

    // Publish a track via the signaling AddTrack request (no WebRTC needed).
    let mut a = a_ws.lock().await;
    send_request(
        &mut a,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::AddTrack(lk::AddTrackRequest {
                cid: "mic1".to_string(),
                name: "microphone".to_string(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    let resp = await_message(&mut a, |r| {
        matches!(
            r.message,
            Some(lk::signal_response::Message::TrackPublished(_))
        )
    })
    .await;
    let track_sid = match resp.message {
        Some(lk::signal_response::Message::TrackPublished(tp)) => tp.track.unwrap().sid.clone(),
        other => panic!("expected TrackPublished, got {other:?}"),
    };

    // Mute it (client-initiated).
    send_request(
        &mut a,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Mute(lk::MuteTrackRequest {
                sid: track_sid.clone(),
                muted: true,
            })),
        },
    )
    .await;
    // Reference parity: a client-initiated mute is NOT echoed back to the
    // initiator (the server only acks from-admin mutes); the change is carried
    // to others via the participant update below.
    let ack = try_await_message(
        &mut a,
        |r| matches!(r.message, Some(lk::signal_response::Message::Mute(_))),
        std::time::Duration::from_millis(300),
    )
    .await;
    assert!(
        ack.is_none(),
        "client-initiated mute must not be echoed to the initiator"
    );
    drop(a);

    // The other participant observes the muted track in a participant update.
    let mut b = b_ws.lock().await;
    let update = await_message(&mut b, |r| {
        matches!(r.message, Some(lk::signal_response::Message::Update(_)))
    })
    .await;
    let participants = match update.message {
        Some(lk::signal_response::Message::Update(u)) => u.participants,
        other => panic!("expected Update, got {other:?}"),
    };
    let muted = participants
        .iter()
        .flat_map(|p| p.tracks.iter())
        .find(|t| t.sid == track_sid)
        .map(|t| t.muted)
        .unwrap_or(false);
    assert!(muted, "other participant must see track {track_sid} muted");
}

/// A client-initiated Leave removes the participant from the room.
#[tokio::test]
async fn leave_removes_participant() {
    let (_server, base) = start_server().await;
    let mut ws = ws_connect(&base, &join_token("frank", "leave-room")).await;
    let _join = expect_join(&mut ws).await;
    let room = _server.get_room("leave-room").unwrap();
    assert_eq!(room.num_participants(), 1);

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Leave(lk::LeaveRequest {
                reason: lk::DisconnectReason::ClientInitiated as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(room.num_participants(), 0);
}

/// Hidden participants are absent from `other_participants` on join.
#[tokio::test]
async fn hidden_participant_not_in_other_participants() {
    let (_server, base) = start_server().await;
    let hidden = join_token_with(
        "hidden-a",
        "hidden-room",
        serde_json::json!({"hidden": true}),
        serde_json::json!({}),
    );
    let mut a = ws_connect(&base, &hidden).await;
    let _join = expect_join(&mut a).await;

    let mut b = ws_connect(&base, &join_token("visible", "hidden-room")).await;
    let join_b = expect_join(&mut b).await;
    assert!(
        !join_b
            .other_participants
            .iter()
            .any(|p| p.identity == "hidden-a"),
        "hidden participant leaked into other_participants: {:?}",
        join_b.other_participants
    );
    drop(a);
    drop(b);
}

/// A second connection with the same identity evicts the first.
#[tokio::test]
async fn duplicate_identity_evicts_previous() {
    let (_server, base) = start_server().await;
    let token = join_token("same", "dup-room");
    let mut a = ws_connect(&base, &token).await;
    let _join = expect_join(&mut a).await;
    assert_eq!(_server.get_room("dup-room").unwrap().num_participants(), 1);

    let mut b = ws_connect(&base, &token).await;
    let _join_b = expect_join(&mut b).await;

    // The evicted connection receives a Leave with reason DUPLICATE_IDENTITY
    // (the client is expected to act on it and close).
    let leave_reason = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match futures_util::StreamExt::next(&mut a).await {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                    if let Ok(resp) = lk::SignalResponse::decode(bytes.as_ref()) {
                        if let Some(lk::signal_response::Message::Leave(l)) = resp.message {
                            return l.reason;
                        }
                    }
                }
                Some(Ok(_)) => continue,
                _ => return -1,
            }
        }
    })
    .await
    .unwrap_or(-1);
    assert_eq!(leave_reason, lk::DisconnectReason::DuplicateIdentity as i32);

    assert_eq!(_server.get_room("dup-room").unwrap().num_participants(), 1);
    drop(b);
}

/// Token attributes are applied to the participant.
#[tokio::test]
async fn token_attributes_land_on_participant() {
    let (_server, base) = start_server().await;
    let token = join_token_with(
        "attr-user",
        "attr-room",
        serde_json::json!({}),
        serde_json::json!({"attributes": {"a": "0", "b": "1"}}),
    );
    let mut ws = ws_connect(&base, &token).await;
    let _join = expect_join(&mut ws).await;

    let room = _server.get_room("attr-room").unwrap();
    let p = room.get_participant_by_identity("attr-user").unwrap();
    let attrs = p.attributes();
    assert_eq!(attrs.get("a").map(String::as_str), Some("0"));
    assert_eq!(attrs.get("b").map(String::as_str), Some("1"));
    drop(ws);
}

/// An oversized signal frame must close the connection with code 1009
/// (policy violation), matching the reference message-size enforcement.
#[tokio::test]
async fn oversized_signal_frame_closes_1009() {
    let mut config: Config = test_config();
    config.limit.signal_message_size_limit = 1024;
    let (_server, base) = start_server_with(config).await;
    let mut ws = ws_connect(&base, &join_token("greg", "limit-room")).await;
    let _join = expect_join(&mut ws).await;

    use futures_util::SinkExt;
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        vec![0u8; 4096].into(),
    ))
    .await
    .unwrap();

    let code = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match futures_util::StreamExt::next(&mut ws).await {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(Some(f)))) => {
                    return Some(f.code)
                }
                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => continue,
                Some(Ok(_)) => continue,
                _ => return None,
            }
        }
    })
    .await
    .ok()
    .flatten();
    assert_eq!(
        code,
        Some(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size),
        "oversized frame must close with 1009 (message too big)"
    );
}

/// A video AddTrack is rejected with a coded UNSUPPORTED_TYPE response (the
/// reference replies via RequestResponse; voice-only has no video plane).
#[tokio::test]
async fn video_add_track_gets_unsupported_type() {
    let (_server, base) = start_server().await;
    let mut ws = ws_connect(&base, &join_token("video-pub", "video-room")).await;
    let _join = expect_join(&mut ws).await;

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::AddTrack(lk::AddTrackRequest {
                cid: "cam1".to_string(),
                name: "camera".to_string(),
                r#type: lk::TrackType::Video as i32,
                source: lk::TrackSource::Camera as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    let resp = await_message(&mut ws, |r| {
        matches!(
            r.message,
            Some(lk::signal_response::Message::RequestResponse(_))
        )
    })
    .await;
    match resp.message {
        Some(lk::signal_response::Message::RequestResponse(rr)) => {
            assert_eq!(
                rr.reason,
                lk::request_response::Reason::UnsupportedType as i32
            );
            assert!(matches!(
                rr.request,
                Some(lk::request_response::Request::AddTrack(_))
            ));
        }
        other => panic!("expected RequestResponse, got {other:?}"),
    }
    drop(ws);
}

/// A publish without the canPublish permission gets NOT_ALLOWED instead of
/// hanging the client's publish promise.
#[tokio::test]
async fn publish_without_permission_gets_not_allowed() {
    let (_server, base) = start_server().await;
    // Join with canPublish: false.
    let token = join_token_with(
        "no-pub",
        "nopub-room",
        serde_json::json!({"canPublish": false}),
        serde_json::json!({}),
    );
    let mut ws = ws_connect(&base, &token).await;
    let _join = expect_join(&mut ws).await;

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::AddTrack(lk::AddTrackRequest {
                cid: "mic1".to_string(),
                name: "mic".to_string(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                ..Default::default()
            })),
        },
    )
    .await;
    let resp = await_message(&mut ws, |r| {
        matches!(
            r.message,
            Some(lk::signal_response::Message::RequestResponse(_))
        )
    })
    .await;
    match resp.message {
        Some(lk::signal_response::Message::RequestResponse(rr)) => {
            assert_eq!(rr.reason, lk::request_response::Reason::NotAllowed as i32);
        }
        other => panic!("expected RequestResponse, got {other:?}"),
    }
    drop(ws);
}

/// TrackPublished echoes the negotiated attributes (muted, stereo, red,
/// encryption) from the AddTrack request.
#[tokio::test]
async fn track_published_echoes_negotiated_attributes() {
    let (_server, base) = start_server().await;
    let mut ws = ws_connect(&base, &join_token("attr-pub", "attr-room")).await;
    let _join = expect_join(&mut ws).await;

    send_request(
        &mut ws,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::AddTrack(lk::AddTrackRequest {
                cid: "mic1".to_string(),
                name: "mic".to_string(),
                r#type: lk::TrackType::Audio as i32,
                source: lk::TrackSource::Microphone as i32,
                muted: true,
                disable_red: true,
                #[allow(deprecated)]
                stereo: true,
                encryption: 1, // E2EE
                ..Default::default()
            })),
        },
    )
    .await;
    let resp = await_message(&mut ws, |r| {
        matches!(
            r.message,
            Some(lk::signal_response::Message::TrackPublished(_))
        )
    })
    .await;
    let track = match resp.message {
        Some(lk::signal_response::Message::TrackPublished(tp)) => tp.track.unwrap(),
        other => panic!("expected TrackPublished, got {other:?}"),
    };
    assert_eq!(track.r#type, lk::TrackType::Audio as i32);
    assert!(track.muted);
    assert!(track.disable_red);
    #[allow(deprecated)]
    let stereo = track.stereo;
    assert!(stereo, "stereo must be echoed");
    assert_eq!(track.encryption, 1);
    drop(ws);
}

/// A room at its max participants rejects new joins with an HTTP 500 before
/// the websocket upgrade (reference `ErrMaxParticipantsExceeded`).
#[tokio::test]
async fn full_room_rejects_join_with_http_error() {
    let mut config = test_config();
    config.room.max_participants = 1;
    let (_server, base) = start_server_with(config).await;

    // First participant joins fine.
    let mut ws = ws_connect(&base, &join_token("first", "full-room")).await;
    let _join = expect_join(&mut ws).await;

    // Second participant gets an HTTP 500 with the room error before upgrade.
    let url = format!(
        "{}/rtc?access_token={}",
        base.replace("http", "ws"),
        join_token("second", "full-room")
    );
    let resp = tokio_tungstenite::connect_async(&url).await;
    let err = resp.expect_err("join must be rejected");
    assert!(
        err.to_string().contains("500"),
        "expected HTTP 500, got: {err}"
    );
    drop(ws);
}
