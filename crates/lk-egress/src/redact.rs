//! Secret redaction for the `EgressInfo.request` echo, mirroring
//! `livekit/protocol/egress/redact.go`. Upload credentials and RTMP stream
//! keys are replaced with placeholders before the request is stored or
//! returned to API clients so secrets never leak through `ListEgress`,
//! `StopEgress`, or webhook payloads.

use lk_proto::livekit as lk;

fn redact(s: &str, placeholder: &str) -> String {
    if s.is_empty() {
        String::new()
    } else {
        placeholder.to_string()
    }
}

/// Replaces the stream key in an RTMP/SRT URL with a short identifier
/// (`{aaa...bbb}`), mirroring `utils.RedactStreamKey`.
fn redact_stream_key(url: &str) -> (String, bool) {
    let Some(rest) = url
        .strip_prefix("rtmp://")
        .or_else(|| url.strip_prefix("rtmps://"))
    else {
        return (url.to_string(), false);
    };
    // rtmp://host/app/streamkey [ live=1]
    let mut parts = rest.split('/');
    let host = parts.next().unwrap_or_default();
    let app = parts.next();
    let key = parts.next();
    let Some((app, key)) = app.zip(key) else {
        return (url.to_string(), false);
    };
    let redacted = redact_identifier(key);
    let mut out = format!("rtmp://{host}/{app}/{redacted}");
    if let Some(tail) = parts.next() {
        out.push('/');
        out.push_str(tail);
    }
    (out, true)
}

fn redact_identifier(id: &str) -> String {
    let mut prefix = "";
    let mut suffix = "";
    for i in (1..=3).rev() {
        if id.len() >= i * 3 {
            prefix = &id[..i];
            suffix = &id[id.len() - i..];
            break;
        }
    }
    format!("{{{prefix}...{suffix}}}")
}

fn redact_s3(s3: &mut lk::S3Upload) {
    s3.access_key = redact(&s3.access_key, "{access_key}");
    s3.secret = redact(&s3.secret, "{secret}");
    s3.assume_role_external_id = redact(&s3.assume_role_external_id, "{external_id}");
    s3.session_token = redact(&s3.session_token, "{session_token}");
}

fn redact_gcp(gcp: &mut lk::GcpUpload) {
    gcp.credentials = redact(&gcp.credentials, "{credentials}");
}

fn redact_azure(azure: &mut lk::AzureBlobUpload) {
    azure.account_name = redact(&azure.account_name, "{account_name}");
    azure.account_key = redact(&azure.account_key, "{account_key}");
}

fn redact_alioss(ali: &mut lk::AliOssUpload) {
    ali.access_key = redact(&ali.access_key, "{access_key}");
    ali.secret = redact(&ali.secret, "{secret}");
}

/// Redacts a storage config (S3/GCP/Azure/AliOSS).
pub fn redact_storage(storage: &mut Option<lk::StorageConfig>) {
    if let Some(s) = storage {
        match &mut s.provider {
            Some(lk::storage_config::Provider::S3(s3)) => redact_s3(s3),
            Some(lk::storage_config::Provider::Gcp(g)) => redact_gcp(g),
            Some(lk::storage_config::Provider::Azure(a)) => redact_azure(a),
            Some(lk::storage_config::Provider::AliOss(ali)) => redact_alioss(ali),
            None => {}
        }
    }
}

fn redact_stream(stream: &mut lk::StreamOutput) {
    for url in &mut stream.urls {
        let (redacted, _) = redact_stream_key(url);
        *url = redacted;
    }
}

/// Redacts a v2 `Output` (per-output storage + stream keys).
fn redact_output(output: &mut lk::Output) {
    redact_storage(&mut output.storage);
    if let Some(lk::output::Config::Stream(stream)) = &mut output.config {
        redact_stream(stream);
    }
}

fn redact_encoded_output(output: &mut lk::EncodedFileOutput) {
    match &mut output.output {
        Some(lk::encoded_file_output::Output::S3(s3)) => redact_s3(s3),
        Some(lk::encoded_file_output::Output::Gcp(g)) => redact_gcp(g),
        Some(lk::encoded_file_output::Output::Azure(a)) => redact_azure(a),
        Some(lk::encoded_file_output::Output::AliOss(ali)) => redact_alioss(ali),
        None => {}
    }
}

fn redact_direct_output(output: &mut lk::DirectFileOutput) {
    match &mut output.output {
        Some(lk::direct_file_output::Output::S3(s3)) => redact_s3(s3),
        Some(lk::direct_file_output::Output::Gcp(g)) => redact_gcp(g),
        Some(lk::direct_file_output::Output::Azure(a)) => redact_azure(a),
        Some(lk::direct_file_output::Output::AliOss(ali)) => redact_alioss(ali),
        None => {}
    }
}

fn redact_segments(segments: &mut lk::SegmentedFileOutput) {
    match &mut segments.output {
        Some(lk::segmented_file_output::Output::S3(s3)) => redact_s3(s3),
        Some(lk::segmented_file_output::Output::Gcp(g)) => redact_gcp(g),
        Some(lk::segmented_file_output::Output::Azure(a)) => redact_azure(a),
        Some(lk::segmented_file_output::Output::AliOss(ali)) => redact_alioss(ali),
        None => {}
    }
}

fn redact_images(images: &mut lk::ImageOutput) {
    match &mut images.output {
        Some(lk::image_output::Output::S3(s3)) => redact_s3(s3),
        Some(lk::image_output::Output::Gcp(g)) => redact_gcp(g),
        Some(lk::image_output::Output::Azure(a)) => redact_azure(a),
        Some(lk::image_output::Output::AliOss(ali)) => redact_alioss(ali),
        None => {}
    }
}

/// Redacts a v1 encoded-output request (deprecated `EncodedOutput` surface).
pub fn redact_encoded(req: &mut impl EncodedOutput) {
    for f in req.file_outputs_mut() {
        redact_encoded_output(f);
    }
    for s in req.stream_outputs_mut() {
        redact_stream(s);
    }
    for s in req.segment_outputs_mut() {
        redact_segments(s);
    }
    for i in req.image_outputs_mut() {
        redact_images(i);
    }
    // Deprecated oneof outputs.
    if let Some(f) = req.file_mut() {
        redact_encoded_output(f);
    }
    if let Some(s) = req.stream_mut() {
        redact_stream(s);
    }
    if let Some(s) = req.segments_mut() {
        redact_segments(s);
    }
}

/// A request type that carries the deprecated `EncodedOutput` fields.
pub trait EncodedOutput {
    fn file_outputs_mut(&mut self) -> Vec<&mut lk::EncodedFileOutput>;
    fn stream_outputs_mut(&mut self) -> Vec<&mut lk::StreamOutput>;
    fn segment_outputs_mut(&mut self) -> Vec<&mut lk::SegmentedFileOutput>;
    fn image_outputs_mut(&mut self) -> Vec<&mut lk::ImageOutput>;
    fn file_mut(&mut self) -> Option<&mut lk::EncodedFileOutput>;
    fn stream_mut(&mut self) -> Option<&mut lk::StreamOutput>;
    fn segments_mut(&mut self) -> Option<&mut lk::SegmentedFileOutput>;
}

macro_rules! impl_encoded {
    ($t:ty, $file:ident, $stream:ident, $seg:ident, $img:ident) => {
        impl EncodedOutput for $t {
            fn file_outputs_mut(&mut self) -> Vec<&mut lk::EncodedFileOutput> {
                self.$file.iter_mut().collect()
            }
            fn stream_outputs_mut(&mut self) -> Vec<&mut lk::StreamOutput> {
                self.$stream.iter_mut().collect()
            }
            fn segment_outputs_mut(&mut self) -> Vec<&mut lk::SegmentedFileOutput> {
                self.$seg.iter_mut().collect()
            }
            fn image_outputs_mut(&mut self) -> Vec<&mut lk::ImageOutput> {
                self.$img.iter_mut().collect()
            }
            fn file_mut(&mut self) -> Option<&mut lk::EncodedFileOutput> {
                None
            }
            fn stream_mut(&mut self) -> Option<&mut lk::StreamOutput> {
                None
            }
            fn segments_mut(&mut self) -> Option<&mut lk::SegmentedFileOutput> {
                None
            }
        }
    };
}

impl_encoded!(
    lk::WebEgressRequest,
    file_outputs,
    stream_outputs,
    segment_outputs,
    image_outputs
);
impl_encoded!(
    lk::ParticipantEgressRequest,
    file_outputs,
    stream_outputs,
    segment_outputs,
    image_outputs
);
impl_encoded!(
    lk::TrackCompositeEgressRequest,
    file_outputs,
    stream_outputs,
    segment_outputs,
    image_outputs
);

// The deprecated oneof `file`/`stream`/`segments` fields exist only on
// RoomCompositeEgressRequest (and WebEgressRequest); wire them through.
impl EncodedOutput for lk::RoomCompositeEgressRequest {
    fn file_outputs_mut(&mut self) -> Vec<&mut lk::EncodedFileOutput> {
        self.file_outputs.iter_mut().collect()
    }
    fn stream_outputs_mut(&mut self) -> Vec<&mut lk::StreamOutput> {
        self.stream_outputs.iter_mut().collect()
    }
    fn segment_outputs_mut(&mut self) -> Vec<&mut lk::SegmentedFileOutput> {
        self.segment_outputs.iter_mut().collect()
    }
    fn image_outputs_mut(&mut self) -> Vec<&mut lk::ImageOutput> {
        self.image_outputs.iter_mut().collect()
    }
    fn file_mut(&mut self) -> Option<&mut lk::EncodedFileOutput> {
        self.output.as_mut().and_then(|o| {
            if let lk::room_composite_egress_request::Output::File(f) = o {
                Some(f)
            } else {
                None
            }
        })
    }
    fn stream_mut(&mut self) -> Option<&mut lk::StreamOutput> {
        self.output.as_mut().and_then(|o| {
            if let lk::room_composite_egress_request::Output::Stream(s) = o {
                Some(s)
            } else {
                None
            }
        })
    }
    fn segments_mut(&mut self) -> Option<&mut lk::SegmentedFileOutput> {
        self.output.as_mut().and_then(|o| {
            if let lk::room_composite_egress_request::Output::Segments(s) = o {
                Some(s)
            } else {
                None
            }
        })
    }
}

/// Redacts a v2 `StartEgressRequest` (request-level storage + per-output
/// storage and stream keys).
pub fn redact_start(req: &mut lk::StartEgressRequest) {
    redact_storage(&mut req.storage);
    for o in &mut req.outputs {
        redact_output(o);
    }
}

/// Redacts a v1 track-egress `DirectFileOutput` request.
pub fn redact_direct(req: &mut lk::TrackEgressRequest) {
    if let Some(lk::track_egress_request::Output::File(f)) = &mut req.output {
        redact_direct_output(f);
    }
}

/// Redacts the deprecated `RoomCompositeEgressRequest` (used by the v1 Twirp
/// `StartRoomCompositeEgress`).
pub fn redact_room_composite(req: &mut lk::RoomCompositeEgressRequest) {
    redact_encoded(req);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_s3_secrets() {
        let mut req = lk::StartEgressRequest {
            room_name: "r".into(),
            outputs: vec![lk::Output {
                storage: Some(lk::StorageConfig {
                    provider: Some(lk::storage_config::Provider::S3(lk::S3Upload {
                        access_key: "AKIA".into(),
                        secret: "secret".into(),
                        session_token: "token".into(),
                        assume_role_external_id: "ext".into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        redact_start(&mut req);
        let s3 = match &req.outputs[0].storage.as_ref().unwrap().provider {
            Some(lk::storage_config::Provider::S3(s3)) => s3,
            _ => panic!("expected s3"),
        };
        assert_eq!(s3.access_key, "{access_key}");
        assert_eq!(s3.secret, "{secret}");
        assert_eq!(s3.session_token, "{session_token}");
        assert_eq!(s3.assume_role_external_id, "{external_id}");
    }

    #[test]
    fn redacts_gcp_azure_alioss() {
        let mut gcp = lk::GcpUpload {
            credentials: "{}".into(),
            ..Default::default()
        };
        redact_gcp(&mut gcp);
        assert_eq!(gcp.credentials, "{credentials}");

        let mut az = lk::AzureBlobUpload {
            account_name: "acct".into(),
            account_key: "key".into(),
            ..Default::default()
        };
        redact_azure(&mut az);
        assert_eq!(az.account_name, "{account_name}");
        assert_eq!(az.account_key, "{account_key}");

        let mut ali = lk::AliOssUpload {
            access_key: "a".into(),
            secret: "b".into(),
            ..Default::default()
        };
        redact_alioss(&mut ali);
        assert_eq!(ali.access_key, "{access_key}");
        assert_eq!(ali.secret, "{secret}");
    }

    #[test]
    fn redacts_rtmp_stream_key() {
        let (out, ok) = redact_stream_key("rtmp://a.rtmp.youtube.com/live2/abc123def456");
        assert!(ok);
        assert_eq!(out, "rtmp://a.rtmp.youtube.com/live2/{abc...456}");
        let (out, ok) = redact_stream_key("https://example.com/stream");
        assert!(!ok);
        assert_eq!(out, "https://example.com/stream");
    }

    #[test]
    fn empty_secrets_stay_empty() {
        let mut req = lk::StartEgressRequest::default();
        redact_start(&mut req);
        assert!(req.outputs.is_empty());
    }
}
