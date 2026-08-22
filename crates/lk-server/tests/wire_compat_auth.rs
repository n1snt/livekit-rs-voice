//! Wire-compatibility tests for JWT auth and the Twirp permission matrix,
//! mirroring `livekit-server`'s `EnsureCreate/List/Admin/Record` semantics.

mod common;

use common::*;
use serde_json::json;

/// Invalid, expired, or otherwise unusable tokens must be rejected with 401.
#[tokio::test]
async fn rejects_invalid_tokens() {
    let (_server, base) = start_server().await;
    let body = json!({});
    // No token at all.
    assert_eq!(
        twirp_unauthed(&base, "livekit.RoomService", "ListRooms", body.clone()).await,
        reqwest::StatusCode::UNAUTHORIZED
    );
    // Wrong secret.
    let t = wrong_secret_token(json!({"roomList": true}));
    let (status, v) = twirp(&base, "livekit.RoomService", "ListRooms", &t, body.clone()).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(v["code"], "unauthenticated");
    // Expired.
    let t = expired_token();
    let (status, _) = twirp(&base, "livekit.RoomService", "ListRooms", &t, body.clone()).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    // Unknown API key (iss).
    let t = unknown_key_token();
    let (status, _) = twirp(&base, "livekit.RoomService", "ListRooms", &t, body.clone()).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    // Malformed JWT (not even three dot-separated parts).
    let (status, _) = twirp(&base, "livekit.RoomService", "ListRooms", "garbage", body).await;
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
}

/// Each RoomService method requires its documented grant; a token without it
/// gets `permission_denied` (403), and the reference error code string is
/// preserved in the JSON body.
#[tokio::test]
async fn room_service_permission_matrix() {
    let (_server, base) = start_server().await;

    // A room with one joined participant so the participant-scoped methods have
    // something to address.
    let join = join_token("bob", "perm-room");
    let mut ws = ws_connect(&base, &join).await;
    let _join_resp = read_response(&mut ws).await;

    // (method, body, grant that WOULD be required)
    let cases: Vec<(&str, serde_json::Value)> = vec![
        ("CreateRoom", json!({"name": "perm-room-2"})),
        ("ListRooms", json!({})),
        ("DeleteRoom", json!({"room": "perm-room"})),
        ("ListParticipants", json!({"room": "perm-room"})),
        (
            "GetParticipant",
            json!({"room": "perm-room", "identity": "bob"}),
        ),
        (
            "RemoveParticipant",
            json!({"room": "perm-room", "identity": "bob"}),
        ),
        (
            "MutePublishedTrack",
            json!({"room": "perm-room", "identity": "bob", "trackSid": "TR_x", "muted": true}),
        ),
        (
            "UpdateParticipant",
            json!({"room": "perm-room", "identity": "bob", "metadata": "m"}),
        ),
        (
            "UpdateSubscriptions",
            json!({"room": "perm-room", "identity": "bob", "trackSids": ["TR_x"], "subscribe": true}),
        ),
        (
            "SendData",
            json!({"room": "perm-room", "data": "aGk=", "kind": 0}),
        ),
        (
            "UpdateRoomMetadata",
            json!({"room": "perm-room", "metadata": "m"}),
        ),
    ];

    // A join-only token has none of the admin grants -> everything is denied.
    let join_only = join_token("alice", "perm-room");
    for (method, body) in &cases {
        let (status, v) = twirp(
            &base,
            "livekit.RoomService",
            method,
            &join_only,
            body.clone(),
        )
        .await;
        assert_eq!(
            status,
            reqwest::StatusCode::FORBIDDEN,
            "{method} should be forbidden for a join-only token"
        );
        assert_eq!(v["code"], "permission_denied", "{method} code mismatch");
    }
    drop(ws);
}

/// RoomAdmin must also match the token's room (reference `EnsureAdminPermission`).
#[tokio::test]
async fn admin_requires_matching_room() {
    let (_server, base) = start_server().await;
    // Admin for "room-a" only; act on "room-b".
    let token = admin_token("room-a", json!({}));
    let (status, v) = twirp(
        &base,
        "livekit.RoomService",
        "ListParticipants",
        &token,
        json!({"room": "room-b"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert_eq!(v["code"], "permission_denied");
}

/// Egress Twirp methods require the `roomRecord` grant.
#[tokio::test]
async fn egress_requires_record_grant() {
    let (_server, base) = start_server().await;
    // A join token has no roomRecord -> egress start is forbidden.
    let join_only = join_token("alice", "egress-room");
    let (status, v) = twirp(
        &base,
        "livekit.Egress",
        "StartRoomCompositeEgress",
        &join_only,
        json!({"roomName": "egress-room", "fileOutputs": [{"filepath": "/r"}]}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert_eq!(v["code"], "permission_denied");
}

/// A token carrying the right grant succeeds (sanity: ListRooms with roomList).
#[tokio::test]
async fn grants_grant_access() {
    let (_server, base) = start_server().await;
    let token = raw_token(json!({"roomList": true}));
    let (status, v) = twirp(&base, "livekit.RoomService", "ListRooms", &token, json!({})).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    // protojson omits empty repeated fields; an empty rooms list may be `{}`.
    assert!(v
        .get("rooms")
        .map(|r| r.as_array().unwrap().is_empty())
        .unwrap_or(true));
}
