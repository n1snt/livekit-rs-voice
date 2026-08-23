//! AWS STS `AssumeRole` for S3 uploads that use temporary credentials (the
//! request/config `assume_role_arn` path). Signs a Query-API POST with
//! SigV4 (via `aws-sigv4`) and parses the temporary credentials out of the
//! XML response.

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::time::SystemTime;

/// URL-encodes form values (space as `%20`; AWS decodes both forms).
const FORM_SAFE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'=');

fn form_encode(value: &str) -> String {
    utf8_percent_encode(value, FORM_SAFE).to_string()
}

/// Temporary credentials returned by `sts:AssumeRole`.
pub struct TempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

/// Calls `sts:AssumeRole` with `base_*` credentials and returns the temporary
/// credentials to use for the upload. `region == "auto"`/empty falls back to
/// `us-east-1` (STS has no `auto` region).
pub async fn assume_role(
    base_access_key: &str,
    base_secret: &str,
    base_session_token: &str,
    region: &str,
    role_arn: &str,
    external_id: &str,
) -> Result<TempCredentials, String> {
    let region = if region.is_empty() || region == "auto" {
        "us-east-1"
    } else {
        region
    };

    let mut body = String::new();
    body.push_str("Action=AssumeRole&Version=2011-06-15&RoleArn=");
    body.push_str(&form_encode(role_arn));
    body.push_str("&RoleSessionName=livekit-egress");
    if !external_id.is_empty() {
        body.push_str("&ExternalId=");
        body.push_str(&form_encode(external_id));
    }
    let uri = format!("https://sts.{region}.amazonaws.com/");

    let session_token = if base_session_token.is_empty() {
        None
    } else {
        Some(base_session_token.to_string())
    };
    let identity: Identity = Credentials::new(
        base_access_key,
        base_secret,
        session_token,
        None,
        "livekit-egress",
    )
    .into();
    let params: aws_sigv4::http_request::SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("sts")
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .map_err(|e| format!("sts signing params: {e}"))?
        .into();
    let request = SignableRequest::new(
        "POST",
        uri.clone(),
        std::iter::empty(),
        SignableBody::Bytes(body.as_bytes()),
    )
    .map_err(|e| format!("sts signable request: {e}"))?;
    let output = sign(request, &params).map_err(|e| format!("sts sign: {e}"))?;
    let (instructions, _signature) = output.into_parts();

    let mut http_req = reqwest::Client::new()
        .post(&uri)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body.clone());
    for (key, value) in instructions.headers() {
        http_req = http_req.header(key, value);
    }
    let resp = http_req
        .send()
        .await
        .map_err(|e| format!("sts request: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("sts response body: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "sts assume role failed ({status}): {}",
            error_message(&text)
        ));
    }
    parse_credentials(&text)
}

/// Extracts `AccessKeyId` / `SecretAccessKey` / `SessionToken` from an STS
/// `AssumeRoleResponse` document.
fn parse_credentials(xml: &str) -> Result<TempCredentials, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut access_key_id = String::new();
    let mut secret = String::new();
    let mut token = String::new();
    let mut in_access = false;
    let mut in_secret = false;
    let mut in_token = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"AccessKeyId" => in_access = true,
                b"SecretAccessKey" => in_secret = true,
                b"SessionToken" => in_token = true,
                _ => {}
            },
            Ok(Event::Text(t)) => {
                let text = t.unescape().map_err(|e| e.to_string())?;
                if in_access {
                    access_key_id = text.into_owned();
                } else if in_secret {
                    secret = text.into_owned();
                } else if in_token {
                    token = text.into_owned();
                }
            }
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"AccessKeyId" => in_access = false,
                b"SecretAccessKey" => in_secret = false,
                b"SessionToken" => in_token = false,
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("sts xml parse: {e}")),
            _ => {}
        }
        buf.clear();
    }

    if access_key_id.is_empty() || secret.is_empty() || token.is_empty() {
        return Err(format!(
            "sts response missing credentials (access={}, secret={}, token={})",
            !access_key_id.is_empty(),
            !secret.is_empty(),
            !token.is_empty()
        ));
    }
    Ok(TempCredentials {
        access_key_id,
        secret_access_key: secret,
        session_token: token,
    })
}

/// Extracts an `<Error><Message>` for failed STS calls.
fn error_message(xml: &str) -> String {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut in_message = false;
    let mut message = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) if e.name().as_ref() == b"Message" => in_message = true,
            Ok(Event::Text(t)) => {
                if in_message {
                    if let Ok(text) = t.unescape() {
                        message = text.into_owned();
                    }
                }
            }
            Ok(Event::End(e)) if e.name().as_ref() == b"Message" => in_message = false,
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        buf.clear();
    }
    if message.is_empty() {
        xml.chars().take(200).collect()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_credentials() {
        let xml = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>AKIA_TEMP</AccessKeyId>
      <SecretAccessKey>TEMP_SECRET</SecretAccessKey>
      <SessionToken>TEMP_TOKEN</SessionToken>
      <Expiration>2030-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser><Arn>arn:aws:sts::123:assumed-role/x/y</Arn></AssumedRoleUser>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;
        let creds = parse_credentials(xml).unwrap();
        assert_eq!(creds.access_key_id, "AKIA_TEMP");
        assert_eq!(creds.secret_access_key, "TEMP_SECRET");
        assert_eq!(creds.session_token, "TEMP_TOKEN");
    }

    #[test]
    fn rejects_incomplete_credentials() {
        let xml = "<AssumeRoleResponse><AssumeRoleResult></AssumeRoleResult></AssumeRoleResponse>";
        assert!(parse_credentials(xml).is_err());
    }

    #[test]
    fn extracts_error_message() {
        let xml = r#"<ErrorResponse><Error><Code>AccessDenied</Code><Message>not allowed</Message></Error></ErrorResponse>"#;
        assert_eq!(error_message(xml), "not allowed");
    }

    #[test]
    fn error_message_falls_back_to_snippet() {
        let xml = "<ErrorResponse><Error><Code>Throttling</Code></Error></ErrorResponse>";
        assert!(error_message(xml).contains("<ErrorResponse>"));
    }
}
