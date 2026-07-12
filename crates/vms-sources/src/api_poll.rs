//! API poller source adapter.
//!
//! Polls a REST endpoint on a fixed interval and publishes an [`Event`] per
//! poll — or, when `change_detect_field` is set, only when the extracted
//! field's value actually changes between polls.

use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vms_core::{
    event::{Event, TopicKey},
    VmsError,
};

/// Configuration for the [`vms_core::SourceType::ApiPoll`] adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiPollSourceConfig {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    /// Static request body, sent as-is (no templating).
    pub body: Option<String>,
    /// Sent as an `Authorization: Bearer <token>` header when set.
    pub bearer_token: Option<String>,
    pub interval_secs: u64,
    /// Dot-separated path into the (JSON-parsed) response body, e.g.
    /// `"data.status"`. When set, an event is only published when the value
    /// at this path changes between polls. When omitted, every successful
    /// poll publishes.
    pub change_detect_field: Option<String>,
}

fn default_method() -> String {
    "GET".to_string()
}

/// Per-request timeout — bounds how long a hung server can block a poll
/// cycle even without cancellation.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub fn spawn(
    source_id: Uuid,
    config: serde_json::Value,
    event_tx: UnboundedSender<Event>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, VmsError> {
    let cfg: ApiPollSourceConfig = serde_json::from_value(config)?;

    if cfg.interval_secs == 0 {
        return Err(VmsError::Source(
            "api_poll: interval_secs must be greater than 0".into(),
        ));
    }
    let method = reqwest::Method::from_bytes(cfg.method.as_bytes())
        .map_err(|_| VmsError::Source(format!("api_poll: invalid HTTP method {:?}", cfg.method)))?;
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|e| VmsError::Source(format!("api_poll: failed to build HTTP client: {e}")))?;

    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(cfg.interval_secs));
        let mut last_value: Option<serde_json::Value> = None;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }

            tokio::select! {
                _ = cancel.cancelled() => return,
                result = fetch_and_extract(&client, &method, &cfg, &mut last_value) => {
                    match result {
                        Ok(Some(payload)) => {
                            let event =
                                Event::new(&TopicKey::Source(source_id), "api_poll_result", payload);
                            if event_tx.send(event).is_err() {
                                return; // Event Bus forwarder is gone — nothing left to notify.
                            }
                        }
                        Ok(None) => {} // no change since the last poll
                        Err(e) => tracing::warn!(
                            source_id = %source_id,
                            error = %e,
                            "api_poll: request failed"
                        ),
                    }
                }
            }
        }
    });

    Ok(task)
}

/// Perform one poll. Returns `Ok(Some(payload))` when an event should be
/// published, `Ok(None)` when change detection determined nothing changed,
/// and updates `last_value` in place as a side effect of that comparison.
async fn fetch_and_extract(
    client: &reqwest::Client,
    method: &reqwest::Method,
    cfg: &ApiPollSourceConfig,
    last_value: &mut Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, VmsError> {
    let mut req = client.request(method.clone(), &cfg.url);
    if let Some(token) = &cfg.bearer_token {
        req = req.bearer_auth(token);
    }
    if let Some(body) = &cfg.body {
        req = req.body(body.clone());
    }

    let resp = req
        .send()
        .await
        .map_err(|e| VmsError::Source(format!("api_poll: request error: {e}")))?;
    let status = resp.status().as_u16();
    let text = resp
        .text()
        .await
        .map_err(|e| VmsError::Source(format!("api_poll: failed to read response body: {e}")))?;
    let body_value: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "raw": text }));

    if let Some(field) = &cfg.change_detect_field {
        let extracted = extract_field(&body_value, field);
        if *last_value == extracted {
            return Ok(None);
        }
        *last_value = extracted;
    }

    Ok(Some(
        serde_json::json!({ "status": status, "body": body_value }),
    ))
}

/// Walk a dot-separated path (e.g. `"data.status"`) into a JSON value.
/// Returns `None` if any segment is missing.
fn extract_field(value: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current.clone())
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_field_walks_nested_path() {
        let value = serde_json::json!({ "data": { "status": "open" } });
        assert_eq!(
            extract_field(&value, "data.status"),
            Some(serde_json::json!("open"))
        );
    }

    #[test]
    fn extract_field_missing_segment_returns_none() {
        let value = serde_json::json!({ "data": {} });
        assert_eq!(extract_field(&value, "data.status"), None);
    }

    #[test]
    fn extract_field_top_level() {
        let value = serde_json::json!({ "status": "ok" });
        assert_eq!(
            extract_field(&value, "status"),
            Some(serde_json::json!("ok"))
        );
    }

    // Zero interval is rejected synchronously — it would otherwise spin the
    // poll loop as fast as the HTTP client allows.
    #[test]
    fn zero_interval_returns_error() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let config = serde_json::json!({ "url": "http://127.0.0.1:1/", "interval_secs": 0 });
        let result = spawn(Uuid::new_v4(), config, tx, CancellationToken::new());
        assert!(result.is_err());
    }

    // An unsupported HTTP method is rejected synchronously, before any
    // request is attempted.
    #[test]
    fn invalid_method_returns_error() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let config = serde_json::json!({
            "url": "http://127.0.0.1:1/", "interval_secs": 5, "method": "NOT A METHOD"
        });
        let result = spawn(Uuid::new_v4(), config, tx, CancellationToken::new());
        assert!(result.is_err());
    }

    // Cancellation stops the task promptly even between poll attempts
    // against an unreachable server.
    #[tokio::test]
    async fn cancel_stops_task_without_a_reachable_server() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let config = serde_json::json!({
            "url": "http://127.0.0.1:1/", "interval_secs": 3600
        });
        let task = spawn(Uuid::new_v4(), config, tx, cancel.clone()).unwrap();

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not stop promptly after cancellation")
            .unwrap();
    }
}
