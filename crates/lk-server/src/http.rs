//! HTTP surface: Twirp API (JSON/protobuf), signaling WebSockets (`/rtc`,
//! `/rtc/v1`), agent WebSocket (`/agent`), health, validation and metrics.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Form, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use lk_proto::livekit as lk;
use prost::Message as _;

use crate::agent;
use crate::auth::{self, VerifiedToken};
use crate::core::ParticipantKind;
use crate::server::Server;
use crate::signal::{self, SessionParams};

/// Response content types supported by Twirp.
#[derive(Clone, Copy, PartialEq)]
pub enum WireFormat {
    Json,
    Protobuf,
}

#[derive(Debug)]
pub struct TwirpError {
    pub code: &'static str,
    pub status: StatusCode,
    pub msg: String,
}

impl TwirpError {
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "invalid_argument",
            status: StatusCode::BAD_REQUEST,
            msg: msg.into(),
        }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "not_found",
            status: StatusCode::NOT_FOUND,
            msg: msg.into(),
        }
    }
    pub fn permission_denied(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "permission_denied",
            status: StatusCode::FORBIDDEN,
            msg: msg.into(),
        }
    }
    pub fn unauthenticated(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "unauthenticated",
            status: StatusCode::UNAUTHORIZED,
            msg: msg.into(),
        }
    }
    pub fn already_exists(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "already_exists",
            status: StatusCode::CONFLICT,
            msg: msg.into(),
        }
    }
    pub fn failed_precondition(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "failed_precondition",
            status: StatusCode::BAD_REQUEST,
            msg: msg.into(),
        }
    }
    pub fn bad_route(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "bad_route",
            status: StatusCode::NOT_FOUND,
            msg: msg.into(),
        }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        TwirpError {
            code: "internal",
            status: StatusCode::INTERNAL_SERVER_ERROR,
            msg: msg.into(),
        }
    }
}

impl IntoResponse for TwirpError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "code": self.code, "msg": self.msg, "meta": {} });
        (
            self.status,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(&body).unwrap_or_default(),
        )
            .into_response()
    }
}

/// The authenticated request context for Twirp handlers.
#[derive(Clone)]
pub struct Req {
    pub token: VerifiedToken,
    pub format: WireFormat,
}

fn format_from(headers: &HeaderMap) -> Result<WireFormat, TwirpError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.contains("application/protobuf") {
        Ok(WireFormat::Protobuf)
    } else if ct.is_empty() || ct.contains("application/json") {
        Ok(WireFormat::Json)
    } else {
        Err(TwirpError::bad_route(format!(
            "unsupported content type: {ct}"
        )))
    }
}

/// Authenticates a request from the `Authorization` header or `access_token`
/// query/form parameter.
fn authenticate(
    server: &Server,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<VerifiedToken, TwirpError> {
    let token: Option<String> = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let token = token
        .or_else(|| params.get("access_token").cloned())
        .ok_or_else(|| TwirpError::unauthenticated("missing authorization"))?;
    let token = auth::bearer_token(Some(&token)).unwrap_or(&token);
    server.keys.verify(token).map_err(|e| match e {
        auth::AuthError::MissingAuthorization => {
            TwirpError::unauthenticated("missing authorization")
        }
        auth::AuthError::InvalidAuthorization => {
            TwirpError::unauthenticated("invalid authorization")
        }
        auth::AuthError::InvalidApiKey => TwirpError::unauthenticated("invalid api key"),
        auth::AuthError::InvalidToken(msg) => TwirpError::unauthenticated(msg),
    })
}

pub fn parse_body<T: serde::de::DeserializeOwned + prost::Message + Default>(
    body: &[u8],
    format: WireFormat,
) -> Result<T, TwirpError> {
    match format {
        WireFormat::Json => serde_json::from_slice(body)
            .map_err(|e| TwirpError::invalid_argument(format!("invalid json: {e}"))),
        WireFormat::Protobuf => T::decode(body)
            .map_err(|e| TwirpError::invalid_argument(format!("invalid protobuf: {e}"))),
    }
}

pub fn write_body<T: serde::Serialize + prost::Message>(
    msg: &T,
    format: WireFormat,
) -> Result<Vec<u8>, TwirpError> {
    match format {
        WireFormat::Json => serde_json::to_vec(msg)
            .map_err(|e| TwirpError::internal(format!("serialize response: {e}"))),
        WireFormat::Protobuf => {
            let mut buf = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut buf)
                .map_err(|e| TwirpError::internal(format!("encode response: {e}")))?;
            Ok(buf)
        }
    }
}

/// Twirp handler dispatch: `POST /twirp/livekit.<Service>/<Method>`.
async fn twirp_handler(
    Path((service, method)): Path<(String, String)>,
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let format = match format_from(&headers) {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };
    let req = match authenticate(&server, &headers, &query) {
        Ok(token) => Req { token, format },
        Err(e) => return e.into_response(),
    };
    match dispatch_twirp(&server, &service, &method, &req, &body).await {
        Ok(bytes) => {
            let content_type = match format {
                WireFormat::Json => "application/json",
                WireFormat::Protobuf => "application/protobuf",
            };
            ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn dispatch_twirp(
    server: &Arc<Server>,
    service: &str,
    method: &str,
    req: &Req,
    body: &[u8],
) -> Result<Vec<u8>, TwirpError> {
    let format = req.format;
    match service {
        "livekit.RoomService" => crate::services::room_service(server, method, req, body, format),
        "livekit.AgentDispatchService" => {
            crate::services::agent_dispatch_service(server, method, req, body, format)
        }
        "livekit.SIP" => crate::services_sip::sip_service(server, method, req, body, format).await,
        "livekit.Egress" => {
            crate::services_sip::egress_service(server, method, req, body, format).await
        }
        _ => Err(TwirpError::not_found(format!(
            "service not found: {service}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Signaling WebSockets
// ---------------------------------------------------------------------------

fn max_signal_message_size(server: &Arc<Server>) -> usize {
    server.config.limit.signal_message_size_limit
}

/// Cap for the decompressed `/rtc/v1` join_request payload.
fn server_limit(_params: &HashMap<String, String>) -> usize {
    2 * 1024 * 1024
}

async fn rtc_ws(
    ws: WebSocketUpgrade,
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    rtc_ws_impl(ws, server, headers, query, form, false).await
}

async fn rtc_ws_v1(
    ws: WebSocketUpgrade,
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    rtc_ws_impl(ws, server, headers, query, form, true).await
}

async fn rtc_ws_impl(
    ws: WebSocketUpgrade,
    server: Arc<Server>,
    headers: HeaderMap,
    query: HashMap<String, String>,
    form: HashMap<String, String>,
    is_v1: bool,
) -> Response {
    let mut params = query.clone();
    for (k, v) in form {
        params.entry(k).or_insert(v);
    }
    let token = match authenticate(&server, &headers, &params) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    if !token.video.room_join {
        return TwirpError::permission_denied("join permission denied").into_response();
    }
    if token.video.room.is_empty() {
        return TwirpError::invalid_argument("room is required").into_response();
    }
    if token.identity.is_empty() {
        return TwirpError::invalid_argument("identity is required").into_response();
    }

    let session = match build_session_params(&token, &params, is_v1) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    ws.max_message_size(max_signal_message_size(&server))
        .on_upgrade(move |socket| async move {
            if let Err(e) = run_rtc_session(socket, server, token, session).await {
                tracing::warn!("rtc session ended: {e}");
            }
        })
}

/// Parses the query/form parameters (and, for `/rtc/v1`, the wrapped
/// `JoinRequest`) into session parameters.
fn build_session_params(
    token: &VerifiedToken,
    params: &HashMap<String, String>,
    is_v1: bool,
) -> Result<SessionParams, TwirpError> {
    let mut session = SessionParams {
        auto_subscribe: params
            .get("auto_subscribe")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(true),
        publish: params
            .get("publish")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(true),
        reconnect: params
            .get("reconnect")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false),
        participant_sid: params.get("sid").cloned().unwrap_or_default(),
        ..Default::default()
    };

    if is_v1 {
        let wrapped_b64 = params
            .get("join_request")
            .ok_or_else(|| TwirpError::invalid_argument("missing join_request"))?;
        let wrapped_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, wrapped_b64)
                .map_err(|_| TwirpError::invalid_argument("invalid join_request encoding"))?;
        let wrapped = lk::WrappedJoinRequest::decode(wrapped_bytes.as_slice())
            .map_err(|_| TwirpError::invalid_argument("invalid wrapped join request"))?;
        let raw = wrapped.join_request;
        let payload = match lk::wrapped_join_request::Compression::try_from(wrapped.compression) {
            Ok(lk::wrapped_join_request::Compression::Gzip) => {
                use std::io::Read;
                let cap = server_limit(params);
                let decoder = flate2::read::GzDecoder::new(raw.as_slice());
                let mut out = Vec::new();
                decoder
                    .take(cap as u64 + 1)
                    .read_to_end(&mut out)
                    .map_err(|_| {
                        TwirpError::invalid_argument("failed to decompress join request")
                    })?;
                if out.len() > cap {
                    return Err(TwirpError::invalid_argument("join request too large"));
                }
                out
            }
            _ => raw,
        };
        let join_req = lk::JoinRequest::decode(payload.as_slice())
            .map_err(|_| TwirpError::invalid_argument("invalid join request"))?;
        session.reconnect = join_req.reconnect;
        session.participant_sid = join_req.participant_sid;
        session.metadata = join_req.metadata;
        session.attributes = join_req.participant_attributes;
        session.add_track_requests = join_req.add_track_requests;
        session.publisher_offer = join_req.publisher_offer;
        session.sync_state = join_req.sync_state;
        if let Some(settings) = join_req.connection_settings {
            session.auto_subscribe = settings.auto_subscribe;
        }
    }

    // Metadata/attributes may also come from the token; the request overrides.
    let _ = token;
    Ok(session)
}

async fn run_rtc_session(
    socket: WebSocket,
    server: Arc<Server>,
    token: VerifiedToken,
    params: SessionParams,
) -> Result<(), String> {
    let kind = match token.kind.to_uppercase().as_str() {
        "AGENT" => ParticipantKind::Agent,
        "EGRESS" => ParticipantKind::Egress,
        "SIP" => ParticipantKind::Sip,
        "INGRESS" => ParticipantKind::Ingress,
        _ => ParticipantKind::Standard,
    };
    let (room, participant, launch_agents) =
        signal::join_room(&server, &token, &params, kind).await?;

    let join_response = signal::build_join_response(&room, &participant, &server);

    let prelude = signal::SignalPrelude {
        join: join_response_msg(join_response),
        publisher_offer: params.publisher_offer.clone(),
        add_tracks: params.add_track_requests.clone(),
        sync_state: params.sync_state.clone(),
        launch_agents,
    };

    signal::run_signal_session(socket, participant, room, prelude).await;
    Ok(())
}

fn join_response_msg(resp: lk::JoinResponse) -> lk::SignalResponse {
    lk::SignalResponse {
        message: Some(lk::signal_response::Message::Join(resp)),
    }
}

// ---------------------------------------------------------------------------
// Agent worker WebSocket
// ---------------------------------------------------------------------------

async fn agent_ws(
    ws: WebSocketUpgrade,
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let token = match authenticate(&server, &headers, &query) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    if !token.video.agent {
        return TwirpError::permission_denied("agent permission denied").into_response();
    }
    let max = server.config.limit.agent_signal_message_size_limit;
    ws.max_message_size(max)
        .on_upgrade(move |socket| async move {
            agent::run_worker_session(socket, token, server).await;
        })
}

// ---------------------------------------------------------------------------
// Health / validate / metrics
// ---------------------------------------------------------------------------

async fn health() -> &'static str {
    "OK"
}

async fn validate_rtc(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    match authenticate(&server, &headers, &query) {
        Ok(token) if token.video.room_join => (StatusCode::OK, "success").into_response(),
        Ok(_) => TwirpError::permission_denied("join permission denied").into_response(),
        Err(e) => e.into_response(),
    }
}

/// Builds the main application router.
pub fn router(server: Arc<Server>) -> Router {
    let body_limit = axum::extract::DefaultBodyLimit::max(
        server.config.limit.max_api_request_body_size.max(4096),
    );
    Router::new()
        .layer(axum::middleware::from_fn(cors_middleware))
        .layer(body_limit)
        .route("/", get(health))
        .route("/rtc/validate", get(validate_rtc))
        .route("/rtc/v1/validate", get(validate_rtc))
        .route(
            "/twirp/{service}/{method}",
            axum::routing::post(twirp_handler),
        )
        .route("/rtc", get(rtc_ws))
        .route("/rtc/v1", get(rtc_ws_v1))
        .route("/agent", get(agent_ws))
        .with_state(server)
}

/// Permissive CORS matching the reference server (token auth is the security
/// boundary). Enabled unconditionally for API compatibility.
async fn cors_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // Preflight short-circuit.
    if req.method() == axum::http::Method::OPTIONS {
        let mut resp = (StatusCode::NO_CONTENT, "").into_response();
        if let Some(origin) = origin {
            resp.headers_mut()
                .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.parse().unwrap());
        }
        resp.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            "POST, GET, OPTIONS".parse().unwrap(),
        );
        resp.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "authorization, content-type".parse().unwrap(),
        );
        return resp;
    }

    let mut resp = next.run(req).await;
    if let Some(origin) = origin {
        resp.headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.parse().unwrap());
        resp.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "authorization, content-type".parse().unwrap(),
        );
    }
    resp
}
