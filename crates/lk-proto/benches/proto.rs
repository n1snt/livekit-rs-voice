//! Wire-format micro-benchmarks: protobuf binary and protojson serialization.

use std::collections::BTreeMap;

use criterion::{criterion_group, criterion_main, Criterion};
use lk_proto::livekit as lk;
use prost::Message as _;

fn sample_join_response() -> lk::SignalResponse {
    lk::SignalResponse {
        message: Some(lk::signal_response::Message::Join(lk::JoinResponse {
            room: Some(lk::Room {
                sid: "RM_8Ld3Xf2mNpQr".to_string(),
                name: "bench-room".to_string(),
                empty_timeout: 300,
                departure_timeout: 20,
                max_participants: 0,
                creation_time_ms: 1_786_860_372_547,
                metadata: "{\"workspace\":\"bench\"}".to_string(),
                num_participants: 3,
                num_publishers: 1,
                version: Some(lk::TimedVersion {
                    unix_micro: 1_786_860_372_548_211,
                    ticks: 0,
                }),
                ..Default::default()
            }),
            participant: Some(lk::ParticipantInfo {
                sid: "PA_9kQ2wZx8vBnM".to_string(),
                identity: "caller-001".to_string(),
                state: lk::participant_info::State::Active as i32,
                tracks: vec![lk::TrackInfo {
                    sid: "TR_1aB2cD3eF4gH".to_string(),
                    r#type: lk::TrackType::Audio as i32,
                    name: "microphone".to_string(),
                    muted: false,
                    source: lk::TrackSource::Microphone as i32,
                    mime_type: "audio/opus".to_string(),
                    mid: "0".to_string(),
                    ..Default::default()
                }],
                metadata: "{\"agent_id\":\"bench\"}".to_string(),
                joined_at_ms: 1_786_860_371_000,
                name: "Alice".to_string(),
                version: 4,
                permission: Some(lk::ParticipantPermission {
                    can_publish: true,
                    can_subscribe: true,
                    can_publish_data: true,
                    ..Default::default()
                }),
                is_publisher: true,
                kind: lk::participant_info::Kind::Standard as i32,
                attributes: BTreeMap::from([(
                    "sip.phoneNumber".to_string(),
                    "+919000000000".to_string(),
                )]),
                client_protocol: 17,
                ..Default::default()
            }),
            other_participants: (0..4)
                .map(|i| lk::ParticipantInfo {
                    sid: format!("PA_other{i}"),
                    identity: format!("participant-{i}"),
                    state: lk::participant_info::State::Active as i32,
                    joined_at_ms: 1_786_860_371_000,
                    name: format!("User {i}"),
                    version: 1,
                    ..Default::default()
                })
                .collect(),
            server_version: "1.13.5".to_string(),
            subscriber_primary: true,
            client_configuration: Some(lk::ClientConfiguration::default()),
            ping_timeout: 15,
            ping_interval: 5,
            server_info: Some(lk::ServerInfo {
                edition: lk::server_info::Edition::Standard as i32,
                version: "1.13.5".to_string(),
                protocol: 17,
                region: "local".to_string(),
                node_id: "LXbenchmark".to_string(),
                agent_protocol: 1,
                ..Default::default()
            }),
            enabled_publish_codecs: vec![
                lk::Codec {
                    mime: "audio/opus".to_string(),
                    fmtp_line: String::new(),
                },
                lk::Codec {
                    mime: "audio/red".to_string(),
                    fmtp_line: String::new(),
                },
            ],
            fast_publish: true,
            ..Default::default()
        })),
    }
}

fn sample_signal_request() -> lk::SignalRequest {
    let mut mid = BTreeMap::new();
    mid.insert("0".to_string(), "mic1".to_string());
    lk::SignalRequest {
        message: Some(lk::signal_request::Message::Offer(lk::SessionDescription {
            r#type: "offer".to_string(),
            sdp: "v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\na=setup:actpass\r\na=ice-ufrag:aaaa\r\na=ice-pwd:bbbb\r\na=sendonly\r\na=rtpmap:111 opus/48000/2\r\na=ssrc:2294505326 cname:mic1\r\n".to_string(),
            id: 0,
            mid_to_track_id: mid,
        })),
    }
}

fn bench_protobuf_roundtrip(c: &mut Criterion) {
    let resp = sample_join_response();
    let req = sample_signal_request();
    let mut resp_bytes = Vec::new();
    let mut req_bytes = Vec::new();
    resp.encode(&mut resp_bytes).unwrap();
    req.encode(&mut req_bytes).unwrap();

    let mut group = c.benchmark_group("protobuf");
    group.throughput(criterion::Throughput::Bytes(resp_bytes.len() as u64));
    group.bench_function("signal_response/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(resp_bytes.len());
            resp.encode(&mut buf).unwrap();
            criterion::black_box(buf);
        })
    });
    group.bench_function("signal_response/decode", |b| {
        b.iter(|| {
            let decoded = lk::SignalResponse::decode(resp_bytes.as_slice()).unwrap();
            criterion::black_box(decoded);
        })
    });
    group.throughput(criterion::Throughput::Bytes(req_bytes.len() as u64));
    group.bench_function("signal_request/encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(req_bytes.len());
            req.encode(&mut buf).unwrap();
            criterion::black_box(buf);
        })
    });
    group.bench_function("signal_request/decode", |b| {
        b.iter(|| {
            let decoded = lk::SignalRequest::decode(req_bytes.as_slice()).unwrap();
            criterion::black_box(decoded);
        })
    });
    group.finish();
}

fn bench_protojson_roundtrip(c: &mut Criterion) {
    let resp = sample_join_response();

    let mut group = c.benchmark_group("protojson");
    group.bench_function("signal_response/serialize", |b| {
        b.iter(|| {
            let json = serde_json::to_string(&resp).unwrap();
            criterion::black_box(json);
        })
    });
    let json = serde_json::to_string(&resp).unwrap();
    group.bench_function("signal_response/deserialize", |b| {
        b.iter(|| {
            let parsed: lk::SignalResponse = serde_json::from_str(&json).unwrap();
            criterion::black_box(parsed);
        })
    });
    group.finish();
}

fn bench_data_packet(c: &mut Criterion) {
    let packet = lk::DataPacket {
        participant_identity: "caller-001".to_string(),
        participant_sid: "PA_9kQ2wZx8vBnM".to_string(),
        destination_identities: vec!["agent-xyz".to_string()],
        value: Some(lk::data_packet::Value::User(lk::UserPacket {
            payload: vec![0x01; 128],
            topic: Some("lk.transcription".to_string()),
            id: Some("msg-123".to_string()),
            ..Default::default()
        })),
        sequence: 42,
        ..Default::default()
    };
    let mut bytes = Vec::new();
    packet.encode(&mut bytes).unwrap();

    let mut group = c.benchmark_group("data_packet");
    group.throughput(criterion::Throughput::Bytes(bytes.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(bytes.len());
            packet.encode(&mut buf).unwrap();
            criterion::black_box(buf);
        })
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            let p = lk::DataPacket::decode(bytes.as_slice()).unwrap();
            criterion::black_box(p);
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_protobuf_roundtrip,
    bench_protojson_roundtrip,
    bench_data_packet
);
criterion_main!(benches);
