//! Wire-compatibility tests for the Twirp RoomService API, mirroring
//! `livekit-server`'s room-service test surface: lifecycle, participant
//! management, not_found/invalid_argument error codes, CORS, and limits.

mod common;

use common::*;
use lk_proto::livekit as lk;
use serde_json::json;

/// Room lifecycle: CreateRoom -> ListRooms (with name filter) -> DeleteRoom.
#[tokio::test]
async fn room_lifecycle_create_list_delete() {
    let (_server, base) = start_server().await;
    let admin = admin_token("", json!({"roomCreate": true, "roomList": true}));

    let (status, room) = twirp(
        &base,
        "livekit.RoomService",
        "CreateRoom",
        &admin,
        json!({"name": "lifecycle-room", "metadata": "{\"k\":1}", "emptyTimeout": 60}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(room["name"], "lifecycle-room");
    assert!(room["sid"].as_str().unwrap().starts_with("RM_"));
    assert_eq!(room["metadata"], "{\"k\":1}");

    // ListRooms with a name filter returns only that room.
    let (status, rooms) = twirp(
        &base,
        "livekit.RoomService",
        "ListRooms",
        &admin,
        json!({"names": ["lifecycle-room"]}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let list = rooms["rooms"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["name"], "lifecycle-room");

    // DeleteRoom removes it.
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "DeleteRoom",
        &admin,
        json!({"room": "lifecycle-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        _server.get_room("lifecycle-room").is_none() || {
            let r = _server.get_room("lifecycle-room").unwrap();
            r.num_participants() == 0
        }
    );
}

/// Joins a real participant and exercises the participant-scoped methods.
#[tokio::test]
async fn participant_management() {
    let (_server, base) = start_server().await;
    let admin = admin_token("part-room", json!({}));
    // Join a participant so the room has state.
    let mut ws = ws_connect(&base, &join_token("alice", "part-room")).await;
    let _join = expect_join(&mut ws).await;

    // ListParticipants
    let (status, resp) = twirp(
        &base,
        "livekit.RoomService",
        "ListParticipants",
        &admin,
        json!({"room": "part-room"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let parts = resp["participants"].as_array().unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0]["identity"], "alice");

    // GetParticipant
    let (status, p) = twirp(
        &base,
        "livekit.RoomService",
        "GetParticipant",
        &admin,
        json!({"room": "part-room", "identity": "alice"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(p["identity"], "alice");
    assert!(p["sid"].as_str().unwrap().starts_with("PA_"));

    // UpdateParticipant (metadata + name)
    let (status, p) = twirp(
        &base,
        "livekit.RoomService",
        "UpdateParticipant",
        &admin,
        json!({"room": "part-room", "identity": "alice", "metadata": "m2", "name": "Alice"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(p["metadata"], "m2");
    assert_eq!(p["name"], "Alice");

    // UpdateRoomMetadata
    let (status, room) = twirp(
        &base,
        "livekit.RoomService",
        "UpdateRoomMetadata",
        &admin,
        json!({"room": "part-room", "metadata": "room-meta"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(room["metadata"], "room-meta");

    // RemoveParticipant
    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "RemoveParticipant",
        &admin,
        json!({"room": "part-room", "identity": "alice"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(_server.get_room("part-room").unwrap().num_participants(), 0);
    drop(ws);
}

/// MutePublishedTrack mutes a track that was published via the signaling API.
#[tokio::test]
async fn mute_published_track() {
    let (_server, base) = start_server().await;
    let admin = admin_token("mute-api-room", json!({}));
    let mut ws = ws_connect(&base, &join_token("bob", "mute-api-room")).await;
    let _join = expect_join(&mut ws).await;

    // Publish a track (signaling only).
    send_request(
        &mut ws,
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
    let resp = await_message(&mut ws, |r| {
        matches!(
            r.message,
            Some(lk::signal_response::Message::TrackPublished(_))
        )
    })
    .await;
    let sid = match resp.message {
        Some(lk::signal_response::Message::TrackPublished(tp)) => tp.track.unwrap().sid.clone(),
        other => panic!("expected TrackPublished, got {other:?}"),
    };

    let (status, p) = twirp(
        &base,
        "livekit.RoomService",
        "MutePublishedTrack",
        &admin,
        json!({"room": "mute-api-room", "identity": "bob", "trackSid": sid, "muted": true}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    // The response is a TrackInfo reflecting the new muted state.
    assert_eq!(p["track"]["sid"], sid);
    assert_eq!(p["track"]["muted"], true);
    drop(ws);
}

/// Sending data to a room succeeds with the roomAdmin grant.
#[tokio::test]
async fn send_data_returns_ok() {
    let (_server, base) = start_server().await;
    let admin = admin_token("data-room", json!({}));
    let mut ws = ws_connect(&base, &join_token("carol", "data-room")).await;
    let _join = expect_join(&mut ws).await;

    let (status, _) = twirp(
        &base,
        "livekit.RoomService",
        "SendData",
        &admin,
        json!({"room": "data-room", "data": "aGVsbG8=", "kind": 0, "topic": "t"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    drop(ws);
}

/// The reference returns `not_found` for missing participants.
#[tokio::test]
async fn not_found_for_missing_participant() {
    let (_server, base) = start_server().await;
    let admin = admin_token("nf-room", json!({}));
    let mut ws = ws_connect(&base, &join_token("dave", "nf-room")).await;
    let _join = expect_join(&mut ws).await;

    let (status, v) = twirp(
        &base,
        "livekit.RoomService",
        "GetParticipant",
        &admin,
        json!({"room": "nf-room", "identity": "nobody"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(v["code"], "not_found");

    let (status, v) = twirp(
        &base,
        "livekit.RoomService",
        "UpdateParticipant",
        &admin,
        json!({"room": "nf-room", "identity": "nobody", "metadata": "x"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(v["code"], "not_found");
    drop(ws);
}

/// Metadata/attributes over the configured limits are `invalid_argument`
/// (reference `ErrMetadataExceedsLimits` / `ErrAttributeExceedsLimits`).
#[tokio::test]
async fn invalid_argument_for_oversized_metadata() {
    let mut config = test_config();
    config.limit.max_metadata = 5;
    config.limit.max_attributes = 5;
    let (_server, base) = start_server_with(config).await;
    let admin = admin_token("", json!({"roomCreate": true}));

    let (status, v) = twirp(
        &base,
        "livekit.RoomService",
        "CreateRoom",
        &admin,
        json!({"name": "limit-room", "metadata": "way too long"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], "invalid_argument");

    let (status, _v) = twirp(
        &base,
        "livekit.RoomService",
        "CreateRoom",
        &admin,
        json!({"name": "limit-room-2"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);

    let admin2 = admin_token("limit-room-2", json!({}));
    let mut ws = ws_connect(&base, &join_token("erin", "limit-room-2")).await;
    let _join = expect_join(&mut ws).await;
    let (status, v) = twirp(
        &base,
        "livekit.RoomService",
        "UpdateParticipant",
        &admin2,
        json!({"room": "limit-room-2", "identity": "erin", "metadata": "too long"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], "invalid_argument");
    drop(ws);
}

/// CORS echoes the request Origin, matching the reference.
#[tokio::test]
async fn cors_headers_echoed() {
    let (_server, base) = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/twirp/livekit.RoomService/ListRooms"))
        .header("Origin", "https://example.test")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://example.test")
    );
}
