//! Object-storage upload of finished recordings (S3-compatible: AWS S3,
//! Cloudflare R2, MinIO, ...). The Go egress uploads after recording with a
//! primary + backup uploader; this voice-only recorder uploads to one resolved
//! destination once the file is finalized.

use std::collections::HashMap;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{Attribute, Attributes, ObjectStore, PutMode, PutOptions, TagSet};

/// A resolved upload destination: local filesystem or S3-compatible object
/// storage. Request-level upload config wins; the container `s3:` config is the
/// default; otherwise recordings stay local.
#[derive(Debug, Clone)]
pub enum Destination {
    Local,
    S3(Box<S3Target>),
}

#[derive(Debug, Clone)]
pub struct S3Target {
    pub access_key: String,
    pub secret: String,
    pub session_token: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub force_path_style: bool,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
}

/// Uploads a finished recording. Returns the `(location, size)` pair reported
/// in `FileInfo`; for local storage the location is the local path.
pub async fn upload(
    local_path: &str,
    key: &str,
    dest: &Destination,
) -> Result<(String, u64), String> {
    let size = tokio::fs::metadata(local_path)
        .await
        .map(|m| m.len())
        .map_err(|e| format!("stat {local_path}: {e}"))?;
    match dest {
        Destination::Local => Ok((local_path.to_string(), size)),
        Destination::S3(s3) => {
            let bytes = tokio::fs::read(local_path)
                .await
                .map_err(|e| e.to_string())?;
            let location = put_s3(s3.as_ref(), key, bytes).await?;
            Ok((location, size))
        }
    }
}

async fn put_s3(s3: &S3Target, key: &str, bytes: Vec<u8>) -> Result<String, String> {
    let region = if s3.region.is_empty() {
        "auto".to_string()
    } else {
        s3.region.clone()
    };
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&s3.bucket)
        .with_access_key_id(&s3.access_key)
        .with_secret_access_key(&s3.secret)
        .with_region(region);
    if !s3.session_token.is_empty() {
        builder = builder.with_token(&s3.session_token);
    }
    if !s3.endpoint.is_empty() {
        builder = builder.with_endpoint(&s3.endpoint).with_allow_http(true);
    }
    if s3.force_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    let store = builder.build().map_err(|e| format!("s3 config: {e}"))?;
    let path = Path::from(key);
    let mut tags = TagSet::default();
    for (k, v) in parse_tagging(&s3.tagging) {
        tags.push(&k, &v);
    }
    let mut attributes = Attributes::new();
    for (k, v) in &s3.metadata {
        attributes.insert(Attribute::Metadata(k.clone().into()), v.clone().into());
    }
    let opts = PutOptions {
        mode: PutMode::Overwrite,
        tags,
        attributes,
        ..Default::default()
    };
    store
        .put_opts(&path, bytes.into(), opts)
        .await
        .map_err(|e| format!("s3 upload {key}: {e}"))?;
    Ok(object_url(s3, key))
}

/// Parses the S3 `x-amz-tagging` header format (`k1=v1&k2=v2`) into a map.
fn parse_tagging(tagging: &str) -> HashMap<String, String> {
    tagging
        .split('&')
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let k = it.next()?.trim();
            let v = it.next().unwrap_or("").trim();
            if k.is_empty() {
                None
            } else {
                Some((k.to_string(), v.to_string()))
            }
        })
        .collect()
}

/// Builds the `FileInfo.location` URL for an S3-compatible upload.
fn object_url(s3: &S3Target, key: &str) -> String {
    if !s3.endpoint.is_empty() {
        let base = s3.endpoint.trim_end_matches('/');
        if s3.force_path_style {
            format!("{base}/{}/{key}", s3.bucket)
        } else {
            format!("{base}/{key}")
        }
    } else {
        format!("https://{}.s3.{}.amazonaws.com/{key}", s3.bucket, s3.region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tagging() {
        let tags = parse_tagging("env=prod&team=voice");
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(tags.get("team").map(String::as_str), Some("voice"));
    }

    #[test]
    fn builds_endpoint_urls() {
        let s3 = S3Target {
            access_key: "ak".into(),
            secret: "sk".into(),
            session_token: String::new(),
            region: "auto".into(),
            endpoint: "https://example.r2.cloudflarestorage.com".into(),
            bucket: "voice-ai-recordings".into(),
            force_path_style: false,
            metadata: HashMap::new(),
            tagging: String::new(),
        };
        assert_eq!(
            object_url(&s3, "EG_1.wav"),
            "https://example.r2.cloudflarestorage.com/EG_1.wav"
        );
        let mut path_style = s3.clone();
        path_style.force_path_style = true;
        assert_eq!(
            object_url(&path_style, "EG_1.wav"),
            "https://example.r2.cloudflarestorage.com/voice-ai-recordings/EG_1.wav"
        );
    }

    #[test]
    fn builds_aws_url_without_endpoint() {
        let s3 = S3Target {
            access_key: "ak".into(),
            secret: "sk".into(),
            session_token: String::new(),
            region: "ap-south-1".into(),
            endpoint: String::new(),
            bucket: "voice-ai-recordings".into(),
            force_path_style: false,
            metadata: HashMap::new(),
            tagging: String::new(),
        };
        assert_eq!(
            object_url(&s3, "EG_1.mp3"),
            "https://voice-ai-recordings.s3.ap-south-1.amazonaws.com/EG_1.mp3"
        );
    }
}
