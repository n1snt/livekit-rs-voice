//! Object-storage upload of finished recordings (S3-compatible: AWS S3,
//! Cloudflare R2, MinIO, ...; Google Cloud Storage; Azure Blob). The Go egress
//! uploads after recording with a primary + backup uploader; this voice-only
//! recorder uploads to one resolved destination once the file is finalized.

use std::collections::HashMap;

use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{Attribute, Attributes, ObjectStore, PutMode, PutOptions, TagSet};

use crate::sts;

/// A resolved upload destination. Request-level upload config wins; the
/// container config (`s3:`/`gcp:`/`azure:`) is the default; otherwise
/// recordings stay local.
#[derive(Debug, Clone)]
pub enum Destination {
    Local,
    S3(Box<S3Target>),
    Gcp(Box<GcpTarget>),
    Azure(Box<AzureTarget>),
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
    pub content_disposition: String,
    pub assume_role_arn: String,
    pub assume_role_external_id: String,
}

#[derive(Debug, Clone)]
pub struct GcpTarget {
    pub credentials: String,
    pub bucket: String,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
}

#[derive(Debug, Clone)]
pub struct AzureTarget {
    pub account_name: String,
    pub account_key: String,
    pub container_name: String,
    pub metadata: HashMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
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
        Destination::Gcp(g) => {
            let bytes = tokio::fs::read(local_path)
                .await
                .map_err(|e| e.to_string())?;
            let location = put_gcp(g.as_ref(), key, bytes).await?;
            Ok((location, size))
        }
        Destination::Azure(a) => {
            let bytes = tokio::fs::read(local_path)
                .await
                .map_err(|e| e.to_string())?;
            let location = put_azure(a.as_ref(), key, bytes).await?;
            Ok((location, size))
        }
    }
}

async fn put_s3(s3: &S3Target, key: &str, bytes: Vec<u8>) -> Result<String, String> {
    let (access_key, secret, session_token) = if !s3.assume_role_arn.is_empty() {
        let temp = sts::assume_role(
            &s3.access_key,
            &s3.secret,
            &s3.session_token,
            &s3.region,
            &s3.assume_role_arn,
            &s3.assume_role_external_id,
        )
        .await?;
        (
            temp.access_key_id,
            temp.secret_access_key,
            temp.session_token,
        )
    } else {
        (
            s3.access_key.clone(),
            s3.secret.clone(),
            s3.session_token.clone(),
        )
    };

    let region = if s3.region.is_empty() || s3.region == "auto" {
        "auto".to_string()
    } else {
        s3.region.clone()
    };
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&s3.bucket)
        .with_access_key_id(&access_key)
        .with_secret_access_key(&secret)
        .with_region(region);
    if !session_token.is_empty() {
        builder = builder.with_token(&session_token);
    }
    if !s3.endpoint.is_empty() {
        // Custom endpoints (R2, MinIO) are account-scoped hosts, not
        // bucket-scoped subdomains: object_store must use path-style here, so
        // the object lands at {endpoint}/{bucket}/{key} regardless of
        // force_path_style (object_url reports the same shape).
        builder = builder
            .with_endpoint(&s3.endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    } else {
        // Real AWS: virtual-hosted by default, path-style when requested.
        builder = builder.with_virtual_hosted_style_request(!s3.force_path_style);
    }
    let store = builder.build().map_err(|e| format!("s3 config: {e}"))?;
    let path = Path::from(key);
    let opts = put_options(
        &s3.metadata,
        &s3.tagging,
        &s3.content_disposition,
        content_type_for(key),
    );
    store
        .put_opts(&path, bytes.into(), opts)
        .await
        .map_err(|e| format!("s3 upload {key}: {e}"))?;
    Ok(object_url(s3, key))
}

async fn put_gcp(g: &GcpTarget, key: &str, bytes: Vec<u8>) -> Result<String, String> {
    let store = GoogleCloudStorageBuilder::new()
        .with_bucket_name(&g.bucket)
        .with_service_account_key(&g.credentials)
        .build()
        .map_err(|e| format!("gcp config: {e}"))?;
    let path = Path::from(key);
    let opts = put_options(
        &g.metadata,
        &g.tagging,
        &g.content_disposition,
        content_type_for(key),
    );
    store
        .put_opts(&path, bytes.into(), opts)
        .await
        .map_err(|e| format!("gcp upload {key}: {e}"))?;
    Ok(format!("https://storage.googleapis.com/{}/{key}", g.bucket))
}

async fn put_azure(a: &AzureTarget, key: &str, bytes: Vec<u8>) -> Result<String, String> {
    let store = MicrosoftAzureBuilder::new()
        .with_account(&a.account_name)
        .with_access_key(&a.account_key)
        .with_container_name(&a.container_name)
        .build()
        .map_err(|e| format!("azure config: {e}"))?;
    let path = Path::from(key);
    let opts = put_options(
        &a.metadata,
        &a.tagging,
        &a.content_disposition,
        content_type_for(key),
    );
    store
        .put_opts(&path, bytes.into(), opts)
        .await
        .map_err(|e| format!("azure upload {key}: {e}"))?;
    Ok(format!(
        "https://{}.blob.core.windows.net/{}/{key}",
        a.account_name, a.container_name
    ))
}

fn put_options(
    metadata: &HashMap<String, String>,
    tagging: &str,
    content_disposition: &str,
    content_type: Option<&str>,
) -> PutOptions {
    let mut tags = TagSet::default();
    for (k, v) in parse_tagging(tagging) {
        tags.push(&k, &v);
    }
    let mut attributes = Attributes::new();
    for (k, v) in metadata {
        attributes.insert(Attribute::Metadata(k.clone().into()), v.clone().into());
    }
    if !content_disposition.is_empty() {
        attributes.insert(
            Attribute::ContentDisposition,
            content_disposition.to_string().into(),
        );
    }
    if let Some(ct) = content_type {
        attributes.insert(Attribute::ContentType, ct.to_string().into());
    }
    PutOptions {
        mode: PutMode::Overwrite,
        tags,
        attributes,
        ..Default::default()
    }
}

/// The MIME type for a recording key, when known.
fn content_type_for(key: &str) -> Option<&'static str> {
    if key.ends_with(".mp3") {
        Some("audio/mpeg")
    } else if key.ends_with(".wav") {
        Some("audio/wav")
    } else {
        None
    }
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
        format!("{}/{}/{key}", s3.endpoint.trim_end_matches('/'), s3.bucket)
    } else if s3.force_path_style {
        format!("https://s3.{}.amazonaws.com/{}/{key}", s3.region, s3.bucket)
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
            content_disposition: String::new(),
            assume_role_arn: String::new(),
            assume_role_external_id: String::new(),
        };
        // Custom endpoints are always path-style: the bucket must appear in the
        // reported URL, exactly where object_store PUT the object.
        assert_eq!(
            object_url(&s3, "EG_1.wav"),
            "https://example.r2.cloudflarestorage.com/voice-ai-recordings/EG_1.wav"
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
            content_disposition: String::new(),
            assume_role_arn: String::new(),
            assume_role_external_id: String::new(),
        };
        assert_eq!(
            object_url(&s3, "EG_1.mp3"),
            "https://voice-ai-recordings.s3.ap-south-1.amazonaws.com/EG_1.mp3"
        );
        let mut path_style = s3;
        path_style.force_path_style = true;
        assert_eq!(
            object_url(&path_style, "EG_1.mp3"),
            "https://s3.ap-south-1.amazonaws.com/voice-ai-recordings/EG_1.mp3"
        );
    }

    #[test]
    fn content_type_by_extension() {
        assert_eq!(content_type_for("a.mp3"), Some("audio/mpeg"));
        assert_eq!(content_type_for("a.wav"), Some("audio/wav"));
        assert_eq!(content_type_for("a.bin"), None);
    }
}
