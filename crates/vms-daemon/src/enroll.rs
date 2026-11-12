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
