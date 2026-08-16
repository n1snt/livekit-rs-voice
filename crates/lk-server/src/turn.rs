//! TURN support: an embedded TURN server (long-term credential auth, matching
//! the reference server's scheme) and per-participant ICE server credentials
//! advertised in `JoinResponse`.
//!
//! Credentials match the reference implementation:
//!   username = base62("{api_key}|{participant_id}|{expiry}")
//!   password = base62(sha256("{api_secret}|{participant_id}|{expiry}"))
//! and the TURN server authenticates with the long-term credential key
//! `md5("{username}:{realm}:{password}")`. The base62 codec mirrors
//! `github.com/jxskiss/base62` byte-for-byte.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sha2::Digest as _;
use tokio::net::UdpSocket;
use webrtc::turn::auth::{generate_auth_key, AuthHandler};
use webrtc::turn::relay::relay_static::RelayAddressGeneratorStatic;
use webrtc::turn::server::config::{ConnConfig, ServerConfig};
use webrtc::turn::server::Server;
use webrtc::util::vnet::net::Net;

use crate::config::Config;

pub const TURN_REALM: &str = "livekit";

// ---------------------------------------------------------------------------
// base62 (jxskiss-compatible)
// ---------------------------------------------------------------------------

const B62: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const COMPACT: u8 = 0x1E;
const MASK5: u8 = 0x1F;
const MASK6: u8 = 0x3F;

fn b62_encode(src: &[u8]) -> String {
    let mut pos = (src.len() * 8) as isize;
    let mut out = Vec::new();
    while pos > 0 {
        let mut r = (pos & 7) as usize;
        let mut i = (pos >> 3) as usize;
        if r == 0 {
            i -= 1;
            r = 8;
        }
        let mut b = src[i] >> (8 - r);
        if r < 6 && i > 0 {
            b |= src[i - 1] << r;
        }
        b &= MASK6;
        let mut size = 6;
        if b & COMPACT == COMPACT {
            if pos > 6 || b > MASK5 {
                size = 5;
            }
            b &= MASK5;
        }
        out.push(B62[b as usize]);
        pos -= size;
    }
    String::from_utf8(out).expect("base62 output is ASCII")
}

/// Decodes the reference base62 encoding (exposed for credential verification
/// in integration tests).
pub fn base62_decode(s: &str) -> Option<Vec<u8>> {
    b62_decode(s)
}

fn b62_decode(s: &str) -> Option<Vec<u8>> {
    let mut table = [0xFFu8; 256];
    for (i, c) in B62.iter().enumerate() {
        table[*c as usize] = i as u8;
    }
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    let dst_len = bytes.len() * 6 / 8 + 1;
    let mut dst = vec![0u8; dst_len];
    let mut idx = dst_len;
    let mut pos: u8 = 0;
    let mut b: u32 = 0;
    for (i, &c) in bytes.iter().enumerate() {
        let x = table[c as usize];
        if x == 0xFF {
            return None;
        }
        let x = u32::from(x);
        let last = i == bytes.len() - 1;
        if last {
            b |= x << pos;
            pos += bit_len(x as u8);
        } else if x & u32::from(COMPACT) == u32::from(COMPACT) {
            b |= x << pos;
            pos += 5;
        } else {
            b |= x << pos;
            pos += 6;
        }
        if pos >= 8 {
            idx -= 1;
            dst[idx] = (b & 0xFF) as u8;
            pos %= 8;
            b >>= 8;
        }
    }
    if pos > 0 {
        idx -= 1;
        dst[idx] = (b & 0xFF) as u8;
    }
    Some(dst[idx..].to_vec())
}

fn bit_len(x: u8) -> u8 {
    if x == 0 {
        0
    } else {
        8 - x.leading_zeros() as u8
    }
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

pub fn turn_username(api_key: &str, participant_id: &str, expiry_unix: i64) -> String {
    b62_encode(format!("{api_key}|{participant_id}|{expiry_unix}").as_bytes())
}

pub fn turn_password(secret: &str, participant_id: &str, expiry_unix: i64) -> String {
    let input = format!("{secret}|{participant_id}|{expiry_unix}");
    let digest = sha2::Sha256::digest(input.as_bytes());
    b62_encode(&digest)
}

/// Auth handler validating the reference server's long-term credentials.
/// Expired credentials are accepted so long-running calls can keep refreshing
/// their allocation past the TTL (matching the reference behavior for
/// non-ALLOCATE requests).
pub struct TurnAuthHandler {
    keys: BTreeMap<String, String>,
}

impl TurnAuthHandler {
    pub fn new(keys: BTreeMap<String, String>) -> Self {
        TurnAuthHandler { keys }
    }
}

impl AuthHandler for TurnAuthHandler {
    fn auth_handle(
        &self,
        username: &str,
        realm: &str,
        _src: SocketAddr,
    ) -> std::result::Result<Vec<u8>, webrtc::turn::Error> {
        let decoded = b62_decode(username).ok_or(webrtc::turn::Error::ErrNoSuchUser)?;
        let decoded = String::from_utf8(decoded).map_err(|_| webrtc::turn::Error::ErrNoSuchUser)?;
        let parts: Vec<&str> = decoded.split('|').collect();
        if parts.len() != 3 {
            return Err(webrtc::turn::Error::ErrNoSuchUser);
        }
        let expiry: i64 = parts[2]
            .parse()
            .map_err(|_| webrtc::turn::Error::ErrNoSuchUser)?;
        if expiry == 0 {
            return Err(webrtc::turn::Error::ErrNoSuchUser);
        }
        let secret = self
            .keys
            .get(parts[0])
            .ok_or(webrtc::turn::Error::ErrNoSuchUser)?;
        let password = turn_password(secret, parts[1], expiry);
        Ok(generate_auth_key(username, realm, &password))
    }
}

// ---------------------------------------------------------------------------
// Server startup + ice_servers
// ---------------------------------------------------------------------------

/// The external IP advertised in TURN URLs and used as the relay address.
fn external_ip(config: &Config) -> std::net::IpAddr {
    if !config.rtc.node_ip.is_empty() {
        if let Ok(ip) = config.rtc.node_ip.parse() {
            return ip;
        }
    }
    for cidr in &config.rtc.ips.includes {
        if let Some((ip, _)) = cidr.split_once('/') {
            if let Ok(ip) = ip.parse::<std::net::IpAddr>() {
                if !ip.is_private() {
                    return ip;
                }
            }
        }
    }
    for cidr in &config.rtc.ips.includes {
        if let Some((ip, _)) = cidr.split_once('/') {
            if let Ok(ip) = ip.parse() {
                return ip;
            }
        }
    }
    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
}

trait IsPrivate {
    fn is_private(&self) -> bool;
}

impl IsPrivate for std::net::IpAddr {
    fn is_private(&self) -> bool {
        match self {
            std::net::IpAddr::V4(v4) => v4.is_private() || v4.is_loopback(),
            // RFC 4193 unique-local (fc00::/7).
            std::net::IpAddr::V6(v6) => v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00,
        }
    }
}

/// Starts the embedded TURN server when `turn.enabled` is set. The server runs
/// for the lifetime of the process.
pub async fn start_turn_server(
    config: &Config,
    keys: BTreeMap<String, String>,
) -> Result<(), String> {
    if !config.turn.enabled {
        return Ok(());
    }
    if config.turn.udp_port == 0 {
        return Err("turn.udp_port must be set when turn is enabled".to_string());
    }
    let conn = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{}", config.turn.udp_port))
            .await
            .map_err(|e| format!("bind turn udp {}: {e}", config.turn.udp_port))?,
    );
    let relay_ip = external_ip(config);
    let server = Server::new(ServerConfig {
        conn_configs: vec![ConnConfig {
            conn,
            relay_addr_generator: Box::new(RelayAddressGeneratorStatic {
                relay_address: relay_ip,
                address: "0.0.0.0".to_owned(),
                net: Arc::new(Net::new(None)),
            }),
        }],
        realm: TURN_REALM.to_string(),
        auth_handler: Arc::new(TurnAuthHandler::new(keys)),
        channel_bind_timeout: Duration::from_secs(600),
        alloc_close_notify: None,
    })
    .await
    .map_err(|e| format!("start turn server: {e}"))?;

    // Keep the server alive for the process lifetime.
    tokio::spawn(async move {
        let _server = server;
        std::future::pending::<()>().await;
    });
    tracing::info!(port = config.turn.udp_port, relay_ip = %relay_ip, "TURN server started");
    Ok(())
}

/// Builds the `ice_servers` advertised to a joining participant. Returns an
/// empty list when TURN is disabled.
pub fn ice_servers(
    config: &Config,
    keys: &BTreeMap<String, String>,
    participant_id: &str,
) -> Vec<lk_proto::livekit::IceServer> {
    if !config.turn.enabled || config.turn.udp_port == 0 {
        return Vec::new();
    }
    let Some((api_key, secret)) = keys.iter().next() else {
        return Vec::new();
    };
    let expiry = crate::core::unix_seconds() + config.turn.ttl.max(1) as i64;
    let username = turn_username(api_key, participant_id, expiry);
    let password = turn_password(secret, participant_id, expiry);

    let host = if config.turn.domain.is_empty() {
        external_ip(config).to_string()
    } else {
        config.turn.domain.clone()
    };
    let url = format!("turn:{}:{}?transport=udp", host, config.turn.udp_port);
    vec![lk_proto::livekit::IceServer {
        urls: vec![url],
        username,
        credential: password,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base62_matches_jxskiss_vectors() {
        // Vectors generated by a faithful port of github.com/jxskiss/base62.
        assert_eq!(b62_encode(&[0xa3]), "jC");
        assert_eq!(b62_encode(&[0x1c, 0x06]), "GwB");
        assert_eq!(b62_encode(&[0xbd, 0x46, 0x3e]), "exoeC");
        assert_eq!(b62_encode(&[0x39, 0x23, 0xbc, 0x1a, 0xad]), "tqBvjkD");
        assert_eq!(
            b62_encode(&[0xbd, 0xe4, 0x8b, 0x16, 0x97, 0x6c, 0x08, 0x07, 0x17, 0x37]),
            "3cxBIw2lWsI59C"
        );
        assert_eq!(
            b62_encode(&[
                0x3b, 0x81, 0x9a, 0x06, 0x8f, 0x32, 0xb7, 0xa6, 0xb3, 0x8b, 0x6b, 0x38, 0x72, 0x96,
                0x47, 0xcf, 0xde, 0x01, 0xc2, 0xce,
            ]),
            "OLcAeezHZpc4s2iza6ty8oBaG4O"
        );
    }

    #[test]
    fn base62_round_trips() {
        for n in [1usize, 2, 3, 5, 10, 20, 40, 64] {
            for _ in 0..50 {
                let mut data = vec![0u8; n];
                for b in data.iter_mut() {
                    *b = rand::random();
                }
                let enc = b62_encode(&data);
                let dec = b62_decode(&enc).expect("decode");
                assert_eq!(dec, data, "round trip failed for {n} bytes");
            }
        }
    }

    #[test]
    fn credentials_round_trip_through_auth_handler() {
        let keys = BTreeMap::from([("key".to_string(), "secret".to_string())]);
        let handler = TurnAuthHandler::new(keys.clone());
        let expiry = crate::core::unix_seconds() + 300;
        let user = turn_username("key", "PA_abc", expiry);
        let pw = turn_password("secret", "PA_abc", expiry);
        // The handler must accept the credential.
        let key = handler
            .auth_handle(&user, TURN_REALM, "127.0.0.1:5000".parse().unwrap())
            .expect("auth accepted");
        assert_eq!(key, generate_auth_key(&user, TURN_REALM, &pw));
        // A wrong password (different expiry) must be rejected.
        let wrong = turn_password("secret", "PA_abc", expiry + 1);
        assert_ne!(
            key,
            generate_auth_key(&user, TURN_REALM, &wrong),
            "expired/other credentials must not match"
        );
    }

    #[test]
    fn invalid_username_rejected() {
        let handler =
            TurnAuthHandler::new(BTreeMap::from([("key".to_string(), "secret".to_string())]));
        assert!(handler
            .auth_handle(
                "not-base62!!",
                TURN_REALM,
                "127.0.0.1:5000".parse().unwrap()
            )
            .is_err());
    }
}
