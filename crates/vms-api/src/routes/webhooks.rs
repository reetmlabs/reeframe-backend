//! Inbound webhook receiver for `webhook`-typed sources.
//!
//! A webhook has no persistent connection for `SourceManager` to own, so this
//! route is always mounted and decides per request whether to accept. The
//! source must exist, be `webhook`-typed, be enabled, and be acquired by an
//! enabled pipeline (`ResourceState::Running`). Accepted requests are published
//! as an `Event` directly onto the Event Bus.

use salvo::prelude::*;
use serde::Deserialize;
use vms_core::{
    event::{Event, TopicKey},
    ResourceId, ResourceState,
};

use crate::{
    error::{parse_id, ApiError},
    state::AppState,
};

/// Configuration for a `webhook`-typed source's `config` column.
#[derive(Debug, Deserialize, Default)]
struct WebhookSourceConfig {
    /// If set, inbound requests must send this value in the
    /// `X-Webhook-Secret` header. Omitted means no verification is applied.
    shared_secret: Option<String>,
}

/// Constant-time byte comparison, so a mismatched secret can't be brute
/// forced by timing how many leading bytes matched.
fn secrets_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_secrets_match() {
        assert!(secrets_match(b"correct-secret", b"correct-secret"));
    }

    #[test]
    fn different_content_does_not_match() {
        assert!(!secrets_match(b"correct-secret", b"wrong-secret!!"));
    }

    #[test]
    fn different_length_does_not_match() {
        assert!(!secrets_match(b"short", b"a-much-longer-secret"));
    }

    #[test]
    fn empty_secrets_match_each_other() {
        // Only reachable with an empty `shared_secret`, which is a
        // misconfiguration and not worth special-casing.
        assert!(secrets_match(b"", b""));
    }
}

/// POST /webhooks/{id}
#[handler]
pub async fn receive_webhook(req: &mut Request, depot: &mut Depot) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let source_id = parse_id(req)?;

    // Not found, wrong type, disabled and not acquired all return the same 404
    // so a caller can't tell whether the source exists.
    let not_found = || ApiError::not_found(format!("source {source_id} not found"));

    let source = state
        .source_repo
        .get_decrypted(source_id)
        .await?
        .filter(|s| s.source_type == vms_db::entities::source::SourceType::Webhook)
        .filter(|s| s.enabled)
        .ok_or_else(not_found)?;

    let is_running = state
        .resource_manager
        .status(&ResourceId::Source(source_id))
        .is_some_and(|entry| entry.state == ResourceState::Running);
    if !is_running {
        return Err(not_found());
    }

    let cfg: WebhookSourceConfig = serde_json::from_value(source.config).unwrap_or_default();
    if let Some(expected) = &cfg.shared_secret {
        let provided = req.header::<String>("X-Webhook-Secret").unwrap_or_default();
        if !secrets_match(expected.as_bytes(), provided.as_bytes()) {
            return Err(ApiError::unauthorized("invalid or missing webhook secret"));
        }
    }

    let payload: serde_json::Value = match req.payload().await {
        Ok(bytes) => serde_json::from_slice(bytes)
            .unwrap_or_else(|_| serde_json::json!({ "raw": String::from_utf8_lossy(bytes) })),
        Err(_) => serde_json::Value::Null,
    };

    let event = Event::new(&TopicKey::Source(source_id), "webhook_received", payload);
    state.event_bus.publish(&TopicKey::Source(source_id), event);

    Ok(())
}
