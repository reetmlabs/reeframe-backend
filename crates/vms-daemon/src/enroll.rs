//! `vms-daemon enroll`: registers this backend with a Coordinator using a
//! one-time enrollment code, and stores the resulting site identity.

use std::path::Path;
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use reqwest::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use crate::cli::EnrollArgs;
use crate::identity::{self, SiteIdentity};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Why the Coordinator did not enroll this backend.
#[derive(Debug, PartialEq, Eq)]
pub enum EnrollError {
    InvalidCode,
    Refused,
    NotFound,
    KeyInUse,
    Unexpected(StatusCode),
    Network(String),
}

impl std::fmt::Display for EnrollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCode => write!(
                f,
                "the enrollment code is invalid, already used, or expired"
            ),
            Self::Refused => write!(
                f,
                "this Coordinator does not accept enrollment (is it a replica?)"
            ),
            Self::NotFound => write!(f, "no enrollment endpoint at this URL, check --coordinator"),
            Self::KeyInUse => write!(
                f,
                "the Coordinator says this key already belongs to another site"
            ),
            Self::Unexpected(status) => write!(f, "the Coordinator answered with {status}"),
            Self::Network(e) => write!(f, "cannot reach the Coordinator: {e}"),
        }
    }
}

impl std::error::Error for EnrollError {}

#[derive(Deserialize)]
struct EnrollResponse {
    be_id: Uuid,
}

/// The message a backend signs to prove it holds the key it enrolls with.
/// Must match the Coordinator's format byte for byte.
fn enrollment_message(code: &str, public_key: &str) -> String {
    format!("reeframe-enroll-v1\n{code}\n{public_key}")
}

/// Redeems `code` at `coordinator_url` for `key` and returns the assigned
/// `be_id`.
pub async fn enroll(
    client: &reqwest::Client,
    coordinator_url: &str,
    code: &str,
    key: &SigningKey,
) -> Result<Uuid, EnrollError> {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let public_key = b64.encode(key.verifying_key().as_bytes());
    let signature = b64.encode(
        key.sign(enrollment_message(code, &public_key).as_bytes())
            .to_bytes(),
    );

    let response = client
        .post(format!("{}/enroll", coordinator_url.trim_end_matches('/')))
        .json(&serde_json::json!({
            "code": code,
            "public_key": public_key,
            "signature": signature,
        }))
        .send()
        .await
        .map_err(|e| EnrollError::Network(e.to_string()))?;

    match response.status() {
        StatusCode::OK => response
            .json::<EnrollResponse>()
            .await
            .map(|body| body.be_id)
            .map_err(|e| EnrollError::Network(format!("unreadable response: {e}"))),
        StatusCode::UNAUTHORIZED => Err(EnrollError::InvalidCode),
        StatusCode::FORBIDDEN => Err(EnrollError::Refused),
        StatusCode::NOT_FOUND => Err(EnrollError::NotFound),
        StatusCode::CONFLICT => Err(EnrollError::KeyInUse),
        status => Err(EnrollError::Unexpected(status)),
    }
}

/// Runs the `enroll` subcommand: generates a new key, enrolls it, and only
/// then replaces the identity file.
pub async fn run(args: EnrollArgs, identity_path: &Path) -> anyhow::Result<()> {
    if !args.force && identity::load(identity_path)?.is_some() {
        anyhow::bail!(
            "{} already holds a site identity. Pass --force to enroll again with a new key",
            identity_path.display()
        );
    }

    let coordinator_url = args.coordinator.trim_end_matches('/').to_string();
    let key = SigningKey::generate(&mut OsRng);
    let client = reqwest::Client::builder().timeout(TIMEOUT).build()?;
    let be_id = enroll(&client, &coordinator_url, &args.code, &key).await?;

    identity::save(
        identity_path,
        &SiteIdentity::new(be_id, coordinator_url.clone(), &key),
    )?;

    eprintln!(
        "Enrolled as site {be_id}. Identity written to {}.\n\
         Next, if not set yet: [auth] mode = \"oidc\" and \
         [auth] jwks_url = \"{coordinator_url}/.well-known/jwks.json\". Then restart the service.",
        identity_path.display()
    );
    println!("{be_id}");
    Ok(())
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, VerifyingKey};
    use std::path::PathBuf;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    struct Captured {
        request_line: String,
        body: serde_json::Value,
    }

    /// Answers one HTTP request with `status` and `body`, and reports what it
    /// received.
    async fn spawn_coordinator(
        status: u16,
        body: impl Into<String>,
    ) -> (String, tokio::sync::oneshot::Receiver<Captured>) {
        let body = body.into();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(socket);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut request_body = vec![0; content_length];
            reader.read_exact(&mut request_body).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            reader
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
            let _ = tx.send(Captured {
                request_line: request_line.trim_end().to_string(),
                body: serde_json::from_slice(&request_body).unwrap(),
            });
        });
        (url, rx)
    }

    fn decode(value: &serde_json::Value) -> Vec<u8> {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value.as_str().unwrap())
            .unwrap()
    }

    #[tokio::test]
    async fn enrollment_sends_a_signature_the_coordinator_can_verify() {
        let be_id = Uuid::new_v4();
        let (url, captured) = spawn_coordinator(200, format!(r#"{{"be_id":"{be_id}"}}"#)).await;
        let key = SigningKey::generate(&mut OsRng);

        let result = enroll(
            &reqwest::Client::new(),
            &format!("{url}/"),
            "the-code",
            &key,
        )
        .await;
        assert_eq!(result, Ok(be_id));

        let captured = captured.await.unwrap();
        assert_eq!(captured.request_line, "POST /enroll HTTP/1.1");
        assert_eq!(captured.body["code"], "the-code");
        let public_key: [u8; 32] = decode(&captured.body["public_key"]).try_into().unwrap();
        assert_eq!(public_key, key.verifying_key().to_bytes());
        let signature: [u8; 64] = decode(&captured.body["signature"]).try_into().unwrap();
        let message = enrollment_message("the-code", captured.body["public_key"].as_str().unwrap());
        VerifyingKey::from_bytes(&public_key)
            .unwrap()
            .verify_strict(message.as_bytes(), &Signature::from_bytes(&signature))
            .expect("signature must verify over the enrollment message");
    }

    #[tokio::test]
    async fn coordinator_refusals_map_to_clear_errors() {
        for (status, expected) in [
            (401, EnrollError::InvalidCode),
            (403, EnrollError::Refused),
            (404, EnrollError::NotFound),
            (409, EnrollError::KeyInUse),
            (
                500,
                EnrollError::Unexpected(StatusCode::INTERNAL_SERVER_ERROR),
            ),
        ] {
            let (url, _captured) = spawn_coordinator(status, r#"{"error":"no"}"#).await;
            let key = SigningKey::generate(&mut OsRng);
            let result = enroll(&reqwest::Client::new(), &url, "code", &key).await;
            assert_eq!(result, Err(expected), "status {status}");
        }
    }

    fn scratch_identity_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("vms-enroll-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("site_identity.json")
    }

    fn args(coordinator: &str, force: bool) -> EnrollArgs {
        EnrollArgs {
            coordinator: coordinator.to_string(),
            code: "code".to_string(),
            force,
        }
    }

    fn existing_identity(path: &Path) -> String {
        let key = SigningKey::generate(&mut OsRng);
        identity::save(path, &SiteIdentity::new(Uuid::new_v4(), "old".into(), &key)).unwrap();
        std::fs::read_to_string(path).unwrap()
    }

    #[tokio::test]
    async fn a_successful_run_writes_the_identity() {
        let be_id = Uuid::new_v4();
        let (url, _captured) = spawn_coordinator(200, format!(r#"{{"be_id":"{be_id}"}}"#)).await;
        let path = scratch_identity_path();

        run(args(&format!("{url}/"), false), &path).await.unwrap();

        let written = identity::load(&path).unwrap().unwrap();
        assert_eq!(written.be_id, be_id);
        assert_eq!(written.coordinator_url, url);
    }

    #[tokio::test]
    async fn an_existing_identity_needs_force() {
        let path = scratch_identity_path();
        let before = existing_identity(&path);

        let result = run(args("http://127.0.0.1:9", false), &path).await;

        assert!(result.unwrap_err().to_string().contains("--force"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[tokio::test]
    async fn a_failed_enrollment_leaves_the_existing_identity_untouched() {
        let (url, _captured) = spawn_coordinator(401, r#"{"error":"no"}"#).await;
        let path = scratch_identity_path();
        let before = existing_identity(&path);

        let result = run(args(&url, true), &path).await;

        assert!(result.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }
}
