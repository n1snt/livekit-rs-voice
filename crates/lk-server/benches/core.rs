//! Micro-benchmarks for the server core: JWT verification, room/participant
//! operations, audio-level detection, and config parsing.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use lk_proto::livekit as lk;

use lk_server::auth::KeyProvider;
use lk_server::config::{load_config_from_yaml, Config};
use lk_server::core::ParticipantKind;
use lk_server::media::{active_speakers, Forwarder};
use lk_server::participant::{Participant, ParticipantState};
use lk_server::room::{Room, RoomContext};
use lk_server::server::Server;
use lk_server::track::{PublishedTrack, TrackSource};

const API_KEY: &str = "devkey";
const SECRET: &str = "secret";

fn make_join_token() -> String {
    let now = lk_server::core::unix_seconds();
    let payload = serde_json::json!({
        "iss": API_KEY,
        "sub": "caller-001",
        "name": "Alice",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {"roomJoin": true, "room": "bench-room", "canPublish": true, "canSubscribe": true, "canPublishData": true},
        "metadata": "{\"agent_id\":\"bench\"}",
        "roomConfig": {"agents": [{"agentName": "voice-agent", "metadata": "{}"}]}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

fn bench_jwt_verify(c: &mut Criterion) {
    let token = make_join_token();
    let provider =
        KeyProvider::from_map(std::iter::once((API_KEY.to_string(), SECRET.to_string())).collect());

    let mut group = c.benchmark_group("jwt");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("verify_join_token", |b| {
        b.iter(|| {
            let verified = provider.verify(&token).unwrap();
            criterion::black_box(verified);
        })
    });
    group.finish();
}

fn room_for_bench() -> Arc<Room> {
    let ctx = Arc::new(RoomContext::new(
        Arc::new(Config::default()),
        Arc::new(lk_server::media::RtcEngine::new()),
        lk_server::webhook::WebhookNotifier::disabled(),
        Arc::new(lk_server::metrics::Metrics::default()),
        Arc::new(lk_server::agent::AgentManager::new()),
        lk_server::cluster::Cluster::new_with_bus(
            Arc::new(lk_server::cluster::MemoryBus::default()),
            "bench",
            false,
        ),
    ));
    Room::new("bench-room".to_string(), Arc::downgrade(&ctx))
}

fn make_participant(i: usize) -> Arc<Participant> {
    let p = Participant::new(
        format!("caller-{i:04}"),
        format!("User {i}"),
        "{}".to_string(),
        ParticipantKind::Standard,
    );
    p.set_state(ParticipantState::Active);
    p
}

fn bench_room_ops(c: &mut Criterion) {
    let room = room_for_bench();
    let participant = make_participant(0);
    room.join(participant.clone());

    let mut group = c.benchmark_group("room");
    group.bench_function("to_proto", |b| {
        b.iter(|| {
            let proto = room.to_proto();
            criterion::black_box(proto);
        })
    });
    group.bench_function("broadcast_participant_update/1", |b| {
        let info = participant.to_proto();
        b.iter_batched(
            || info.clone(),
            |info| {
                room.broadcast_participant_update(vec![info], Some(&participant.sid));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_participant_ops(c: &mut Criterion) {
    let participant = make_participant(0);
    let track = Arc::new(PublishedTrack::new(
        "microphone".to_string(),
        "mic1".to_string(),
        TrackSource::Microphone,
        String::new(),
    ));
    participant.add_track(track);

    let mut group = c.benchmark_group("participant");
    group.bench_function("to_proto", |b| {
        b.iter(|| {
            let proto = participant.to_proto();
            criterion::black_box(proto);
        })
    });
    group.bench_function("track_info", |b| {
        b.iter(|| {
            let info = participant.track_infos();
            criterion::black_box(info);
        })
    });
    group.bench_function("set_attributes/10", |b| {
        b.iter(|| {
            let mut attrs = std::collections::BTreeMap::new();
            for i in 0..10 {
                attrs.insert(format!("key{i}"), format!("value{i}"));
            }
            participant.set_attributes(attrs);
        })
    });
    group.finish();
}

fn bench_audio_level(c: &mut Criterion) {
    let detector = lk_server::audio_level::AudioLevelDetector::new();
    let mut group = c.benchmark_group("audio_level");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("observe_loud", |b| {
        b.iter(|| detector.observe(20));
    });
    group.bench_function("observe_quiet", |b| {
        b.iter(|| detector.observe(120));
    });
    group.bench_function("active_speakers/16_participants", |b| {
        let participants: Vec<Arc<Participant>> = (0..16).map(make_participant).collect();
        for p in &participants {
            let f = Arc::new(Forwarder {
                track_sid: format!("TR_{}", p.sid),
                publisher_sid: p.sid.clone(),
                audio: lk_server::audio_level::AudioLevelDetector::new(),
                ext_id: None,
                subscribers: std::sync::Mutex::new(std::collections::HashMap::new()),
                closed: std::sync::atomic::AtomicBool::new(false),
                stats: std::sync::Mutex::new(lk_server::media::RtpStats::default()),
                metrics: std::sync::Arc::new(lk_server::metrics::Metrics::default()),
                track_source: "unknown".to_string(),
                sender_report: std::sync::Mutex::new(None),
                forward_jitter: std::sync::Mutex::new(0.0),
                last_forward_latency: std::sync::Mutex::new(None),
            });
            p.media
                .lock()
                .unwrap()
                .forwarders
                .insert(f.track_sid.clone(), f);
        }
        b.iter(|| {
            let speakers = active_speakers(&participants);
            criterion::black_box(speakers);
        })
    });
    group.finish();
}

fn bench_config_parse(c: &mut Criterion) {
    let yaml = r#"
port: 7880
rtc:
  tcp_port: 7881
  port_range_start: 50000
  port_range_end: 60000
  use_external_ip: false
  ips:
    includes: [203.0.113.10/32, 192.0.2.10/32]
redis:
  address: 127.0.0.1:6379
keys:
  devkey: secret
webhook:
  api_key: devkey
  urls: [https://api.example.com/livekit/webhook]
turn:
  enabled: true
  udp_port: 443
"#;
    let mut group = c.benchmark_group("config");
    group.bench_function("parse_yaml", |b| {
        b.iter(|| {
            let config = load_config_from_yaml(yaml).unwrap();
            criterion::black_box(config);
        })
    });
    group.finish();
}

fn bench_server_room_lookup(c: &mut Criterion) {
    let server = Server::new(Config::default());
    server.get_or_create_room("bench-room");

    let mut group = c.benchmark_group("server");
    group.bench_function("get_or_create_room/existing", |b| {
        b.iter(|| {
            let room = server.get_or_create_room("bench-room");
            criterion::black_box(room);
        })
    });
    group.bench_function("list_rooms", |b| {
        b.iter(|| {
            let rooms = server.list_rooms();
            criterion::black_box(rooms);
        })
    });
    group.finish();
}

fn bench_broadcast_spawn_room(c: &mut Criterion) {
    // A 50-participant room broadcasting a data packet and participant update.
    let room = room_for_bench();
    let participants: Vec<Arc<Participant>> = (0..50).map(make_participant).collect();
    for p in &participants {
        room.join(p.clone());
    }
    let packet = lk::DataPacket {
        participant_identity: "caller-0000".to_string(),
        participant_sid: "PA_x".to_string(),
        value: Some(lk::data_packet::Value::User(lk::UserPacket {
            payload: vec![0xAA; 64],
            ..Default::default()
        })),
        ..Default::default()
    };

    let mut group = c.benchmark_group("broadcast");
    group.bench_function("data_packet/50_participants", |b| {
        b.iter_batched(
            || packet.clone(),
            |p| {
                room.broadcast_data(p, &[]);
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("participant_update/50_participants", |b| {
        let infos: Vec<lk::ParticipantInfo> =
            participants.iter().take(1).map(|p| p.to_proto()).collect();
        b.iter_batched(
            || infos.clone(),
            |infos| {
                room.broadcast_participant_update(infos, None);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_jwt_verify,
    bench_room_ops,
    bench_participant_ops,
    bench_audio_level,
    bench_config_parse,
    bench_server_room_lookup,
    bench_broadcast_spawn_room
);
criterion_main!(benches);
