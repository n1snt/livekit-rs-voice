//! Publishes N seconds of Opus audio into a room, for egress testing and
//! benchmarking.
//!
//! Usage:
//! ```text
//! cargo run -p lk-egress --release --example send_audio -- \
//!   --ws ws://127.0.0.1:7880 --key devkey --secret secret \
//!   --room room-a --seconds 10
//! ```

use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio_tungstenite::tungstenite::Message;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_OPUS};
use webrtc::api::APIBuilder;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtp_transceiver::rtp_codec::{
    RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocalWriter;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn main() {
    let mut ws_url = "ws://127.0.0.1:7880".to_string();
    let mut key = "devkey".to_string();
    let mut secret = "secret".to_string();
    let mut room = "bench".to_string();
    let mut seconds = 10u64;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--ws" => ws_url = iter.next().unwrap(),
            "--key" => key = iter.next().unwrap(),
            "--secret" => secret = iter.next().unwrap(),
            "--room" => room = iter.next().unwrap(),
            "--seconds" => seconds = iter.next().unwrap().parse().unwrap(),
            "--help" => {
                println!(
                    "send_audio --ws <url> --key <k> --secret <s> --room <room> --seconds <n>"
                );
                return;
            }
            other => panic!("unknown arg {other}"),
        }
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(publish(&ws_url, &key, &secret, &room, seconds))
        .unwrap();
}

fn join_token(key: &str, secret: &str, identity: &str, room: &str) -> String {
    let now = lk_egress::now_secs();
    let payload = serde_json::json!({
        "iss": key, "sub": identity, "iat": now, "nbf": now - 5, "exp": now + 3600,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

async fn read_response(ws: &mut Ws) -> lk::SignalResponse {
    loop {
        match futures_util::StreamExt::next(ws).await {
            Some(Ok(Message::Binary(bytes))) => {
                return lk::SignalResponse::decode(bytes.as_ref()).unwrap()
            }
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => continue,
            _ => panic!("ws closed"),
        }
    }
}

async fn send_request(ws: &mut Ws, req: &lk::SignalRequest) {
    use futures_util::SinkExt;
    ws.send(Message::Binary(req.encode_to_vec().into()))
        .await
        .unwrap();
}

async fn client_pc(
    ws: Arc<tokio::sync::Mutex<Ws>>,
    target: i32,
) -> Arc<webrtc::peer_connection::RTCPeerConnection> {
    let mut m = MediaEngine::default();
    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48_000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )
    .unwrap();
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m).unwrap();
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();
    let pc = api
        .new_peer_connection(RTCConfiguration::default())
        .await
        .unwrap();
    let ws2 = ws.clone();
    pc.on_ice_candidate(Box::new(
        move |c: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let ws = ws2.clone();
            Box::pin(async move {
                if let Some(c) = c {
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
                            let mut ws = ws.lock().await;
                            let _ = send_request(
                                &mut ws,
                                &lk::SignalRequest {
                                    message: Some(lk::signal_request::Message::Trickle(
                                        lk::TrickleRequest {
                                            candidate_init: json,
                                            target,
                                            r#final: false,
                                        },
                                    )),
                                },
                            )
                            .await;
                        }
                    }
                }
            })
        },
    ));
    Arc::new(pc)
}

async fn await_message<F: Fn(&lk::SignalResponse) -> bool>(
    ws: &mut Ws,
    pc_pub: &Arc<webrtc::peer_connection::RTCPeerConnection>,
    pc_sub: &Arc<webrtc::peer_connection::RTCPeerConnection>,
    matches: F,
) -> lk::SignalResponse {
    for _ in 0..400 {
        let resp = read_response(ws).await;
        if let Some(lk::signal_response::Message::Trickle(t)) = &resp.message {
            if let Ok(init) = serde_json::from_str::<
                webrtc::ice_transport::ice_candidate::RTCIceCandidateInit,
            >(&t.candidate_init)
            {
                match t.target {
                    0 => {
                        let _ = pc_pub.add_ice_candidate(init.clone()).await;
                    }
                    _ => {
                        let _ = pc_sub.add_ice_candidate(init).await;
                    }
                }
            }
            continue;
        }
        if matches(&resp) {
            return resp;
        }
    }
    panic!("timed out");
}

async fn publish(
    ws_url: &str,
    key: &str,
    secret: &str,
    room: &str,
    seconds: u64,
) -> Result<(), String> {
    let token = join_token(key, secret, "sender", room);
    let url = format!("{}/rtc?access_token={token}", ws_url);
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| e.to_string())?;
    let ws = Arc::new(tokio::sync::Mutex::new(ws));

    // Keep the signal connection alive (the server closes sessions that send
    // no frames within its ~15s ping timeout).
    let ws_keepalive = ws.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut ws = ws_keepalive.lock().await;
            let _ = send_request(
                &mut ws,
                &lk::SignalRequest {
                    message: Some(lk::signal_request::Message::PingReq(lk::Ping {
                        timestamp: lk_egress::now_secs() * 1000,
                        rtt: 0,
                    })),
                },
            )
            .await;
        }
    });

    let mut guard = ws.lock().await;
    let _ = read_response(&mut guard).await; // join
    let sub_offer_sdp = match read_response(&mut guard).await.message {
        Some(lk::signal_response::Message::Offer(o)) => o.sdp,
        other => panic!("expected subscriber offer, got {other:?}"),
    };
    drop(guard);

    let pub_pc = client_pc(ws.clone(), 0).await;
    let pub_sub_pc = client_pc(ws.clone(), 1).await;
    let mut sub_offer = RTCSessionDescription::default();
    sub_offer.sdp_type = RTCSdpType::Offer;
    sub_offer.sdp = sub_offer_sdp;
    pub_sub_pc
        .set_remote_description(sub_offer)
        .await
        .map_err(|e| e.to_string())?;
    let sub_answer = pub_sub_pc
        .create_answer(None)
        .await
        .map_err(|e| e.to_string())?;
    pub_sub_pc
        .set_local_description(sub_answer.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut guard = ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Answer(
                lk::SessionDescription {
                    r#type: "answer".to_string(),
                    sdp: sub_answer.sdp.clone(),
                    ..Default::default()
                },
            )),
        },
    )
    .await;
    drop(guard);

    let out_track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            ..Default::default()
        },
        "bench-audio".to_string(),
        "bench-mic".to_string(),
    ));
    let _sender = pub_pc.add_transceiver_from_track(
        out_track.clone(),
        Some(webrtc::rtp_transceiver::RTCRtpTransceiverInit {
            direction: webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection::Sendonly,
            send_encodings: vec![],
        }),
    ).await.map_err(|e| e.to_string())?;
    let offer = pub_pc.create_offer(None).await.map_err(|e| e.to_string())?;
    pub_pc
        .set_local_description(offer.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut mid_to_track_id = std::collections::BTreeMap::new();
    mid_to_track_id.insert("0".to_string(), "bench-mic".to_string());
    let mut guard = ws.lock().await;
    send_request(
        &mut guard,
        &lk::SignalRequest {
            message: Some(lk::signal_request::Message::Offer(lk::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer.sdp.clone(),
                id: 0,
                mid_to_track_id,
            })),
        },
    )
    .await;
    let resp = await_message(&mut guard, &pub_pc, &pub_sub_pc, |r| {
        matches!(r.message, Some(lk::signal_response::Message::Answer(_)))
    })
    .await;
    let answer_sdp = match resp.message {
        Some(lk::signal_response::Message::Answer(a)) => a.sdp,
        _ => panic!("no answer"),
    };
    drop(guard);
    let mut sd = RTCSessionDescription::default();
    sd.sdp_type = RTCSdpType::Answer;
    sd.sdp = answer_sdp;
    pub_pc
        .set_remote_description(sd)
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    let enc = audiopus::coder::Encoder::new(
        audiopus::SampleRate::Hz48000,
        audiopus::Channels::Mono,
        audiopus::Application::Voip,
    )
    .unwrap();
    let silence = vec![0i16; 960];
    let mut opus = vec![0u8; 4000];
    let n = enc.encode(&silence, &mut opus).unwrap();
    let payload = opus[..n].to_vec();
    let mut seq = 1u16;
    let frames = seconds * 50;
    for _ in 0..frames {
        let pkt = webrtc::rtp::packet::Packet {
            header: webrtc::rtp::header::Header {
                version: 2,
                payload_type: 111,
                sequence_number: seq,
                timestamp: seq as u32 * 960,
                ssrc: 0xdeadbeef,
                ..Default::default()
            },
            payload: payload.clone().into(),
        };
        let _ = out_track.write_rtp(&pkt).await;
        seq = seq.wrapping_add(1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    println!("sent {frames} frames to room {room}");
    // Stay joined a moment so the recorder drains, then leave.
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    let _ = ws.lock().await;
    Ok(())
}
