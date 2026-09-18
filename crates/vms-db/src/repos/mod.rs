pub mod api_key;
pub mod camera;
pub mod contact;
pub mod contact_list;
pub mod daily_recording_coverage;
pub mod destination;
pub mod event;
pub mod export_job;
pub mod pipeline;
pub mod pipeline_run;
pub mod pipeline_validation;
pub mod recording;
pub mod setting;
pub mod source;
pub mod tile_layout;
pub mod user;

pub use api_key::ApiKeyRepo;
pub use camera::CameraRepo;
pub use contact::ContactRepo;
pub use contact_list::ContactListRepo;
pub use daily_recording_coverage::DailyRecordingCoverageRepo;
pub use destination::DestinationRepo;
pub use event::EventsRepo;
pub use export_job::ExportJobRepo;
pub use pipeline::PipelineRepo;
pub use pipeline_run::PipelineRunRepo;
pub use pipeline_validation::{ValidationCategory, ValidationIssue, ValidationSeverity};
pub use recording::RecordingRepo;
pub use setting::SettingsRepo;
pub use source::SourceRepo;
pub use tile_layout::TileLayoutRepo;
pub use user::UserRepo;

use crate::crypto::Crypto;
use vms_core::VmsError;

/// Well-known JSON object keys whose string values are stored encrypted.
/// Applied to the top-level keys of `source.config` and `destination.config`.
/// Also the field list the API layer masks as `"***"` on read.
pub const CREDENTIAL_FIELDS: &[&str] = &[
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

/// Keeps a credential field's existing stored value on update when the
/// submitted value is missing, blank, or the `"***"` mask the API layer
/// sends back on read — otherwise a client that resubmits a masked value
/// would overwrite the real credential with the literal string `"***"`.
/// Drops the key if there's no existing value to fall back to. Non-credential
/// fields are left as submitted.
pub(super) fn preserve_masked_credentials(
    existing: &serde_json::Value,
    mut submitted: serde_json::Value,
) -> serde_json::Value {
    let serde_json::Value::Object(ref mut map) = submitted else {
        return submitted;
    };
    let existing_map = existing.as_object();
    for &key in CREDENTIAL_FIELDS {
        let is_masked = match map.get(key) {
            None => true,
            Some(serde_json::Value::String(s)) => s.is_empty() || s == "***",
            Some(_) => false,
        };
        if !is_masked {
            continue;
        }
        match existing_map.and_then(|m| m.get(key)) {
            Some(existing_value) => {
                map.insert(key.to_owned(), existing_value.clone());
            }
            None => {
                map.remove(key);
            }
        }
    }
    submitted
}

pub(super) fn db_err(e: sea_orm::DbErr) -> VmsError {
    VmsError::Database(e.to_string())
}

pub(super) fn now() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_value_falls_back_to_existing() {
        let existing = serde_json::json!({"password": "enc:v1:real"});
        let submitted = serde_json::json!({"password": "***"});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert_eq!(merged["password"], "enc:v1:real");
    }

    #[test]
    fn blank_value_falls_back_to_existing() {
        let existing = serde_json::json!({"password": "enc:v1:real"});
        let submitted = serde_json::json!({"password": ""});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert_eq!(merged["password"], "enc:v1:real");
    }

    #[test]
    fn missing_key_falls_back_to_existing() {
        let existing = serde_json::json!({"password": "enc:v1:real"});
        let submitted = serde_json::json!({"other_field": "unchanged"});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert_eq!(merged["password"], "enc:v1:real");
    }

    #[test]
    fn masked_value_with_no_existing_value_is_dropped() {
        let existing = serde_json::json!({});
        let submitted = serde_json::json!({"password": "***"});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert!(merged.get("password").is_none());
    }

    #[test]
    fn genuine_new_value_is_not_overwritten() {
        let existing = serde_json::json!({"password": "enc:v1:real"});
        let submitted = serde_json::json!({"password": "a-new-password"});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert_eq!(merged["password"], "a-new-password");
    }

    #[test]
    fn non_credential_fields_pass_through_unchanged() {
        let existing = serde_json::json!({"host": "old.example.com"});
        let submitted = serde_json::json!({"host": "new.example.com"});
        let merged = preserve_masked_credentials(&existing, submitted);
        assert_eq!(merged["host"], "new.example.com");
    }
}
