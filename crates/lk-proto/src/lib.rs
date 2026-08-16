pub mod livekit {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/livekit.rs"));
    include!(concat!(env!("OUT_DIR"), "/livekit.serde.rs"));
}

pub mod google {
    #![allow(clippy::all)]
    pub mod protobuf {
        #![allow(clippy::all)]
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.serde.rs"));
    }
}

pub use google::protobuf as well_known;

mod wkt_serde;

#[cfg(test)]
mod tests {
    use crate::livekit as lk;
    use prost::Message;

    #[test]
    fn signal_request_join_round_trip() {
        let req = lk::SignalRequest {
            message: Some(lk::signal_request::Message::Offer(lk::SessionDescription {
                r#type: "offer".to_string(),
                sdp: "v=0\r\n...".to_string(),
                id: 42,
                mid_to_track_id: {
                    let mut m = std::collections::BTreeMap::new();
                    m.insert("0".to_string(), "TR_abc123".to_string());
                    m
                },
            })),
        };
        let mut buf = Vec::new();
        prost::Message::encode(&req, &mut buf).unwrap();
        let decoded = lk::SignalRequest::decode(buf.as_slice()).unwrap();
        assert_eq!(
            decoded.message,
            Some(lk::signal_request::Message::Offer(lk::SessionDescription {
                r#type: "offer".to_string(),
                sdp: "v=0\r\n...".to_string(),
                id: 42,
                mid_to_track_id: {
                    let mut m = std::collections::BTreeMap::new();
                    m.insert("0".to_string(), "TR_abc123".to_string());
                    m
                },
            }))
        );
    }

    #[test]
    fn join_response_json_matches_protojson_casing() {
        let resp = lk::SignalResponse {
            message: Some(lk::signal_response::Message::Join(lk::JoinResponse {
                room: Some(lk::Room {
                    sid: "RM_aaa".to_string(),
                    name: "test-room".to_string(),
                    creation_time_ms: 1_700_000_000_123,
                    ..Default::default()
                }),
                participant: Some(lk::ParticipantInfo {
                    sid: "PA_xyz".to_string(),
                    identity: "alice".to_string(),
                    state: lk::participant_info::State::Joined as i32,
                    joined_at_ms: 1_700_000_000_000,
                    ..Default::default()
                }),
                server_info: Some(lk::ServerInfo {
                    version: "1.13.5".to_string(),
                    protocol: 17,
                    ..Default::default()
                }),
                ping_interval: 5,
                ping_timeout: 15,
                ..Default::default()
            })),
        };

        // JSON field names must be lowerCamelCase (protojson), oneof wrapper key present,
        // and int64s serialized as strings.
        let json = serde_json::to_string(&resp).unwrap();
        assert!(
            json.contains("\"join\""),
            "expected join wrapper, got: {json}"
        );
        assert!(
            json.contains("\"room\":{\"sid\":\"RM_aaa\",\"name\":\"test-room\",\"creationTimeMs\":\"1700000000123\"}"),
            "unexpected room encoding: {json}"
        );
        assert!(
            json.contains("\"serverInfo\""),
            "missing serverInfo: {json}"
        );
        assert!(
            json.contains("\"pingInterval\":5"),
            "missing pingInterval: {json}"
        );

        // Decode must accept both camelCase and snake_case.
        let from_camel: lk::SignalResponse =
            serde_json::from_str(&json).expect("camelCase decode failed");
        assert_eq!(from_camel, resp);
        let snake = json.replace("\"creationTimeMs\"", "\"creation_time_ms\"");
        let from_snake: lk::SignalResponse =
            serde_json::from_str(&snake).expect("snake_case decode failed");
        assert_eq!(from_snake, resp);
    }

    #[test]
    fn participant_permission_defaults() {
        let perm = lk::ParticipantPermission::default();
        assert!(!perm.can_subscribe);
    }

    #[test]
    fn track_type_enum_matches_proto() {
        // AUDIO=0, VIDEO=1, DATA=2 (proto wire contract)
        assert_eq!(lk::TrackType::Audio as i32, 0);
        assert_eq!(lk::TrackType::Video as i32, 1);
        assert_eq!(lk::TrackType::Data as i32, 2);
    }

    #[test]
    fn well_known_timestamp_serializes_as_rfc3339() {
        let ts = crate::well_known::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_000_000,
        };
        let json = serde_json::to_string(&ts).unwrap();
        assert_eq!(json, "\"2023-11-14T22:13:20.123Z\"");
        let back: crate::well_known::Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ts);
    }

    #[test]
    fn well_known_duration_serializes_as_seconds_string() {
        let d = crate::well_known::Duration {
            seconds: 3,
            nanos: 500_000_000,
        };
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, "\"3.500s\"");
    }
}
