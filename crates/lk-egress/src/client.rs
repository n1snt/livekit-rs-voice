//! A minimal LiveKit client that joins a room as a subscriber-only participant
//! and streams the Opus RTP payloads of the audio tracks it receives.
//!
//! Handles the `subscriber_primary` flow used by livekit-voice: join, accept
//! the server's subscriber offer, answer, exchange ICE candidates, and read
//! the incoming audio track(s).

use std::sync::Arc;

use lk_proto::livekit as lk;
use prost::Message as _;
use tokio::sync::mpsc;
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
use webrtc::track::track_remote::TrackRemote;

/// An Opus RTP payload from a room track, tagged with the track's cid.
#[derive(Debug, Clone)]
pub struct AudioPacket {
    pub track_cid: String,
    pub payload: Vec<u8>,
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Mints an HS256 join token for a subscriber-only participant.
fn join_token(
    api_key: &str,
    api_secret: &str,
    identity: &str,
    room: &str,
) -> Result<String, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let payload = serde_json::json!({
        "iss": api_key,
        "sub": identity,
        "iat": now,
        "nbf": now - 5,
        "exp": now + 3600,
        "video": {
            "roomJoin": true,
            "room": room,
            "canPublish": false,
            "canSubscribe": true,
            "canPublishData": true
        }
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(api_secret.as_bytes()),
    )
    .map_err(|e| e.to_string())
}

/// Builds a client peer connection. ICE candidates and the first audio track
/// are forwarded to the provided callbacks.
async fn client_pc<F1, F2>(
    on_ice: F1,
    on_track: F2,
) -> Result<Arc<webrtc::peer_connection::RTCPeerConnection>, String>
where
    F1: FnMut(String) + Send + Sync + Clone + 'static,
    F2: Fn(Arc<TrackRemote>) + Send + Sync + Clone + 'static,
{
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
    .map_err(|e| e.to_string())?;
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m).map_err(|e| e.to_string())?;
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();
    let pc = api
        .new_peer_connection(RTCConfiguration::default())
        .await
        .map_err(|e| e.to_string())?;

    pc.on_ice_candidate(Box::new(
        move |candidate: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>| {
            let mut on_ice = on_ice.clone();
            Box::pin(async move {
                if let Some(c) = candidate {
                    if let Ok(init) = c.to_json() {
                        if let Ok(json) = serde_json::to_string(&init) {
                            on_ice(json);
                        }
                    }
                }
            })
        },
    ));

    pc.on_track(Box::new(
        move |track: Arc<TrackRemote>,
              _receiver: Arc<webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver>,
              _transceiver: Arc<webrtc::rtp_transceiver::RTCRtpTransceiver>| {
            let on_track = on_track.clone();
            Box::pin(async move {
                if track.kind() == RTPCodecType::Audio {
                    on_track(track);
                }
            })
        },
    ));
    Ok(Arc::new(pc))
}

async fn ws_send(ws: &mut Ws, req: &lk::SignalRequest, json: bool) -> Result<(), String> {
    use futures_util::SinkExt;
    let msg = if json {
        Message::Text(
            serde_json::to_string(req)
                .map_err(|e| e.to_string())?
                .into(),
        )
    } else {
        Message::Binary(req.encode_to_vec().into())
    };
    ws.send(msg).await.map_err(|e| e.to_string())
}

fn ws_trickle(candidate_json: String) -> lk::SignalRequest {
    lk::SignalRequest {
        message: Some(lk::signal_request::Message::Trickle(lk::TrickleRequest {
            candidate_init: candidate_json,
            target: lk::SignalTarget::Subscriber as i32,
            r#final: false,
        })),
    }
}

/// Connects to a room and returns a receiver that yields the room's audio as
/// Opus RTP payloads (one `AudioPacket` per decoded frame, per track). The
/// underlying WebSocket + peer connections stay alive until the returned
/// receiver is dropped or `stop` is called.
pub async fn connect(
    api_key: &str,
    api_secret: &str,
    ws_url: &str,
    room: &str,
    identity: &str,
) -> Result<mpsc::Receiver<AudioPacket>, String> {
    let token = join_token(api_key, api_secret, identity, room)?;
    let url = format!(
        "{}/rtc?access_token={token}&publish=0&auto_subscribe=1&subscriber=1",
        ws_url
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| format!("ws connect: {e}"))?;

    let (audio_tx, audio_rx) = mpsc::channel::<AudioPacket>(256);
    let (out_tx, mut out_rx) = mpsc::channel::<lk::SignalRequest>(64);

    // Read until the JoinResponse arrives (first frame).
    loop {
        let frame = futures_util::StreamExt::next(&mut ws).await;
        let frame = frame.ok_or("signaling closed")?;
        let frame = frame.map_err(|e| e.to_string())?;
        let resp: lk::SignalResponse = match frame {
            Message::Binary(bytes) => {
                lk::SignalResponse::decode(bytes.as_ref()).map_err(|e| e.to_string())?
            }
            Message::Text(text) => serde_json::from_str(&text).map_err(|e| e.to_string())?,
            _ => continue,
        };
        if matches!(resp.message, Some(lk::signal_response::Message::Join(_))) {
            break;
        }
    }

    let mut pc: Option<Arc<webrtc::peer_connection::RTCPeerConnection>> = None;

    // Signaling loop: pump websocket frames and outbound requests.
    let audio_tx_inner = audio_tx.clone();
    tokio::spawn(async move {
        let mut json = true;
        loop {
            tokio::select! {
                req = out_rx.recv() => {
                    match req {
                        Some(req) => { if ws_send(&mut ws, &req, json).await.is_err() { break; } }
                        None => break,
                    }
                }
                frame = futures_util::StreamExt::next(&mut ws) => {
                    let Some(Ok(frame)) = frame else { break };
                    json = matches!(frame, Message::Text(_));
                    let resp: lk::SignalResponse = match frame {
                        Message::Binary(bytes) => match lk::SignalResponse::decode(bytes.as_ref()) { Ok(r) => r, Err(_) => continue },
                        Message::Text(text) => match serde_json::from_str(&text) { Ok(r) => r, Err(_) => continue },
                        _ => continue,
                    };
                    match resp.message {
                        Some(lk::signal_response::Message::Offer(offer)) => {
                            // Create the subscriber PC once, then reuse it for
                            // renegotiation offers (LiveKit adds tracks via
                            // subsequent offers on the same peer connection).
                            if pc.is_none() {
                                let out_tx_ice = out_tx.clone();
                                let audio_tx_track = audio_tx_inner.clone();
                                match client_pc(
                                    move |candidate| {
                                        let _ = out_tx_ice.try_send(ws_trickle(candidate));
                                    },
                                    move |track| {
                                        let audio_tx = audio_tx_track.clone();
                                        let cid = track.stream_id();
                                        tokio::spawn(async move {
                                            let mut buf = vec![0u8; 4096];
                                            while let Ok((pkt, _)) = track.read(&mut buf).await {
                                                if audio_tx
                                                    .send(AudioPacket {
                                                        track_cid: cid.clone(),
                                                        payload: pkt.payload.to_vec(),
                                                    })
                                                    .await
                                                    .is_err()
                                                {
                                                    break;
                                                }
                                            }
                                        });
                                    },
                                )
                                .await
                                {
                                    Ok(pc_new) => pc = Some(pc_new),
                                    Err(_) => break,
                                }
                            }
                            let pc_arc = pc.clone().unwrap();
                            let mut sd = RTCSessionDescription::default();
                            sd.sdp_type = RTCSdpType::Offer;
                            sd.sdp = offer.sdp.clone();
                            if pc_arc.set_remote_description(sd).await.is_err() { break; }
                            let Ok(answer) = pc_arc.create_answer(None).await else { break };
                            if pc_arc.set_local_description(answer.clone()).await.is_err() { break; }
                            let _ = out_tx.send(lk::SignalRequest {
                                message: Some(lk::signal_request::Message::Answer(
                                    lk::SessionDescription {
                                        r#type: "answer".to_string(),
                                        sdp: answer.sdp.clone(),
                                        ..Default::default()
                                    },
                                )),
                            }).await;
                        }
                        Some(lk::signal_response::Message::Trickle(t)) => {
                            if let Some(pc) = &pc {
                                if let Ok(init) = serde_json::from_str::<webrtc::ice_transport::ice_candidate::RTCIceCandidateInit>(&t.candidate_init) {
                                    let _ = pc.add_ice_candidate(init).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    Ok(audio_rx)
}
