//! `lk` — control-plane CLI for a `livekit-rs-voice` server.
//!
//! Speaks the Twirp JSON API with a short-lived API-key JWT. The most common
//! use is placing outbound SIP calls, which the server bridges to a
//! `livekit/sip` container over the psrpc message bus:
//!
//! ```bash
//! lk --url http://127.0.0.1:7880 --api-key devkey --api-secret secret \
//!   sip create-participant --trunk-id ST_xxx --room room-a --call-to +15551234567
//! ```

use clap::{Parser, Subcommand};
use serde_json::{json, Value};

#[derive(Parser)]
#[command(name = "lk", about = "LiveKit voice control-plane CLI", version)]
struct Cli {
    /// Base URL of the livekit-rs-voice server.
    #[arg(long, env = "LK_URL", default_value = "http://127.0.0.1:7880")]
    url: String,
    /// LiveKit API key.
    #[arg(long, env = "LK_API_KEY", default_value = "devkey")]
    api_key: String,
    /// LiveKit API secret.
    #[arg(long, env = "LK_API_SECRET", default_value = "secret")]
    api_secret: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(subcommand)]
    Sip(SipCommand),
}

#[derive(Subcommand)]
enum SipCommand {
    /// Place an outbound SIP call through the livekit/sip bridge.
    CreateParticipant {
        #[arg(long)]
        trunk_id: String,
        #[arg(long)]
        room: String,
        #[arg(long)]
        call_to: String,
        #[arg(long)]
        number: Option<String>,
        #[arg(long)]
        identity: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        metadata: Option<String>,
        #[arg(long)]
        dtmf: Option<String>,
        #[arg(long)]
        wait_until_answered: bool,
    },
    /// Transfer a SIP participant to another number.
    TransferParticipant {
        #[arg(long)]
        room: String,
        #[arg(long)]
        identity: String,
        #[arg(long)]
        transfer_to: String,
        #[arg(long)]
        play_dialtone: bool,
    },
    /// Create an outbound SIP trunk.
    CreateOutboundTrunk {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        address: String,
        #[arg(long)]
        numbers: Vec<String>,
        #[arg(long)]
        auth_username: Option<String>,
        #[arg(long)]
        auth_password: Option<String>,
    },
    /// List outbound trunks.
    ListOutboundTrunks,
    /// List inbound trunks.
    ListInboundTrunks,
    /// List dispatch rules.
    ListDispatchRules,
}

/// Mints an HS256 JWT carrying the requested grants.
fn mint_token(cli: &Cli, sip: Value) -> String {
    let now = lk_server::core::unix_seconds();
    let payload = json!({
        "iss": cli.api_key,
        "sub": "lk-cli",
        "iat": now,
        "nbf": now - 5,
        "exp": now + 600,
        "sip": sip,
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.typ = Some("JWT".to_string());
    jsonwebtoken::encode(
        &header,
        &payload,
        &jsonwebtoken::EncodingKey::from_secret(cli.api_secret.as_bytes()),
    )
    .expect("encode token")
}

/// POSTs a Twirp method and prints the JSON response (or the error).
async fn call(cli: &Cli, service: &str, method: &str, body: Value) -> Result<(), String> {
    let token = mint_token(cli, json!({"admin": true, "call": true}));
    let resp = reqwest::Client::new()
        .post(format!("{}/twirp/{service}/{method}", cli.url))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| format!("read body: {e}"))?;
    let raw = text.clone();
    let value: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    if status.is_success() {
        println!("{}", serde_json::to_string_pretty(&value).unwrap());
        Ok(())
    } else {
        let msg = value.get("msg").and_then(Value::as_str).unwrap_or(&raw);
        Err(format!(
            "{}: {msg}",
            value.get("code").and_then(Value::as_str).unwrap_or("error")
        ))
    }
}

async fn sip(cli: &Cli, cmd: &SipCommand) -> Result<(), String> {
    match cmd {
        SipCommand::CreateParticipant {
            trunk_id,
            room,
            call_to,
            number,
            identity,
            name,
            metadata,
            dtmf,
            wait_until_answered,
        } => {
            let mut body = json!({
                "sipTrunkId": trunk_id,
                "roomName": room,
                "sipCallTo": call_to,
            });
            if let Some(m) = body.as_object_mut() {
                if let Some(n) = number {
                    m.insert("sipNumber".into(), json!(n));
                }
                if let Some(i) = identity {
                    m.insert("participantIdentity".into(), json!(i));
                }
                if let Some(n) = name {
                    m.insert("participantName".into(), json!(n));
                }
                if let Some(md) = metadata {
                    m.insert("participantMetadata".into(), json!(md));
                }
                if let Some(d) = dtmf {
                    m.insert("dtmf".into(), json!(d));
                }
                m.insert("waitUntilAnswered".into(), json!(wait_until_answered));
            }
            call(cli, "livekit.SIP", "CreateSIPParticipant", body).await
        }
        SipCommand::TransferParticipant {
            room,
            identity,
            transfer_to,
            play_dialtone,
        } => {
            let body = json!({
                "roomName": room,
                "participantIdentity": identity,
                "transferTo": transfer_to,
                "playDialtone": play_dialtone,
            });
            call(cli, "livekit.SIP", "TransferSIPParticipant", body).await
        }
        SipCommand::CreateOutboundTrunk {
            name,
            address,
            numbers,
            auth_username,
            auth_password,
        } => {
            let mut trunk = json!({
                "address": address,
                "numbers": numbers,
            });
            if let Some(m) = trunk.as_object_mut() {
                if let Some(n) = name {
                    m.insert("name".into(), json!(n));
                }
                if let Some(u) = auth_username {
                    m.insert("authUsername".into(), json!(u));
                }
                if let Some(p) = auth_password {
                    m.insert("authPassword".into(), json!(p));
                }
            }
            call(
                cli,
                "livekit.SIP",
                "CreateSIPOutboundTrunk",
                json!({"trunk": trunk}),
            )
            .await
        }
        SipCommand::ListOutboundTrunks => {
            call(cli, "livekit.SIP", "ListSIPOutboundTrunk", json!({})).await
        }
        SipCommand::ListInboundTrunks => {
            call(cli, "livekit.SIP", "ListSIPInboundTrunk", json!({})).await
        }
        SipCommand::ListDispatchRules => {
            call(cli, "livekit.SIP", "ListSIPDispatchRule", json!({})).await
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Sip(cmd) => sip(&cli, cmd).await,
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
