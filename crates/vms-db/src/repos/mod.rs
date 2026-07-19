pub mod api_key;
pub mod camera;
pub mod contact;
pub mod contact_list;
pub mod destination;
pub mod pipeline;
pub mod pipeline_run;
pub mod recording;
pub mod source;
pub mod user;

pub use api_key::ApiKeyRepo;
pub use camera::CameraRepo;
pub use contact::ContactRepo;
pub use contact_list::ContactListRepo;
pub use destination::DestinationRepo;
pub use pipeline::PipelineRepo;
pub use pipeline_run::PipelineRunRepo;
pub use recording::RecordingRepo;
pub use source::SourceRepo;
pub use user::UserRepo;

use crate::crypto::Crypto;
use vms_core::VmsError;

/// Well-known JSON object keys whose string values are stored encrypted.
/// Applied to the top-level keys of `source.config` and `destination.config`.
pub(super) const CREDENTIAL_FIELDS: &[&str] = &[
    "password",
    "token",
    "api_key",
    "bearer_token",
    "secret_access_key",
    "private_key",
    "auth_token",
    "access_token",
    "bot_token",
    "webhook_url",
    "shared_secret",
    "client_key",
    "client_cert",
];

/// Encrypt all credential string fields in a JSON object (top-level keys only).
/// Already-encrypted values (detected by the `enc:v1:` prefix) are left as-is.
pub(super) fn encrypt_config(
    crypto: &Crypto,
    mut config: serde_json::Value,
) -> Result<serde_json::Value, VmsError> {
    if let serde_json::Value::Object(ref mut map) = config {
        for &key in CREDENTIAL_FIELDS {
            if let Some(serde_json::Value::String(s)) = map.get(key) {
                if !Crypto::is_encrypted(s) {
                    let enc = crypto.encrypt(s)?;
                    map.insert(key.to_owned(), serde_json::Value::String(enc));
                }
            }
        }
    }
    Ok(config)
}

/// Decrypt all credential string fields in a JSON object (top-level keys only).
/// Plain-text values (no `enc:v1:` prefix) are passed through unchanged.
pub(super) fn decrypt_config(
    crypto: &Crypto,
    mut config: serde_json::Value,
) -> Result<serde_json::Value, VmsError> {
    if let serde_json::Value::Object(ref mut map) = config {
        for &key in CREDENTIAL_FIELDS {
            if let Some(serde_json::Value::String(s)) = map.get(key) {
                if Crypto::is_encrypted(s) {
                    let plain = crypto.decrypt(s)?;
                    map.insert(key.to_owned(), serde_json::Value::String(plain));
                }
            }
        }
    }
    Ok(config)
}

pub(super) fn db_err(e: sea_orm::DbErr) -> VmsError {
    VmsError::Database(e.to_string())
}

pub(super) fn now() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Utc::now().fixed_offset()
}
