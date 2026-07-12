//! Home Assistant WebSocket source adapter.
//!
//! Connects to the HA WebSocket API (`/api/websocket`), authenticates with a
//! long-lived access token, subscribes to `state_changed` (or a configured
//! event type), and publishes one [`Event`] per HA event received.
//!
//! Unlike MQTT, `tokio-tungstenite` has no built-in reconnect — a
//! `WebSocketStream` represents exactly one connection. This adapter supplies
//! its own outer reconnect loop: each dropped/failed connection is logged and
//! retried after a fixed delay, until cancelled.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vms_core::{
    event::{Event, TopicKey},
    VmsError,
};

/// Configuration for the [`vms_core::SourceType::HaWebsocket`] adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct HaWebsocketSourceConfig {
    /// WebSocket URL, e.g. `"ws://homeassistant.local:8123/api/websocket"`.
    pub url: String,
    /// Long-lived access token created in the HA user profile.
    pub access_token: String,
    /// HA event type to subscribe to. Omit to subscribe to every event.
    pub event_type: Option<String>,
}

/// Delay before reconnecting after a dropped or failed connection.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

pub fn spawn(
    source_id: Uuid,
    config: serde_json::Value,
    event_tx: UnboundedSender<Event>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, VmsError> {
    let cfg: HaWebsocketSourceConfig = serde_json::from_value(config)?;

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                result = run_connection(source_id, &cfg, &event_tx, &cancel) => {
                    match result {
                        Ok(()) => return, // cancelled cleanly from inside the connection loop
                        Err(e) => tracing::warn!(
                            source_id = %source_id,
                            error = %e,
                            "ha_websocket: connection error, retrying"
                        ),
                    }
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(RECONNECT_DELAY) => {}
            }
        }
    });

    Ok(task)
}

/// Connect, authenticate, subscribe, and forward events until the connection
/// drops (`Err`) or `cancel` fires (`Ok`).
async fn run_connection(
    source_id: Uuid,
    cfg: &HaWebsocketSourceConfig,
    event_tx: &UnboundedSender<Event>,
    cancel: &CancellationToken,
) -> Result<(), VmsError> {
    let (mut ws, _) = connect_async(cfg.url.as_str())
        .await
        .map_err(|e| VmsError::Source(format!("ha_websocket: connect failed: {e}")))?;

    // First frame is `auth_required` — its content isn't inspected, only that
    // the server is actually speaking the HA protocol and didn't hang up.
    match ws.next().await {
        Some(Ok(_)) => {}
        Some(Err(e)) => return Err(VmsError::Source(format!("ha_websocket: {e}"))),
        None => {
            return Err(VmsError::Source(
                "ha_websocket: connection closed before auth_required".into(),
            ))
        }
    }

    let auth = serde_json::json!({ "type": "auth", "access_token": cfg.access_token });
    ws.send(Message::Text(auth.to_string()))
        .await
        .map_err(|e| VmsError::Source(format!("ha_websocket: send auth: {e}")))?;

    match ws.next().await {
        Some(Ok(Message::Text(text))) => {
            let reply: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            if reply.get("type").and_then(|t| t.as_str()) != Some("auth_ok") {
                return Err(VmsError::Source(format!(
                    "ha_websocket: authentication failed: {text}"
                )));
            }
        }
        Some(Ok(_)) => {
            return Err(VmsError::Source(
                "ha_websocket: unexpected message during authentication".into(),
            ))
        }
        Some(Err(e)) => return Err(VmsError::Source(format!("ha_websocket: {e}"))),
        None => {
            return Err(VmsError::Source(
                "ha_websocket: connection closed during authentication".into(),
            ))
        }
    }

    let mut subscribe = serde_json::json!({ "id": 1, "type": "subscribe_events" });
    if let Some(event_type) = &cfg.event_type {
        subscribe["event_type"] = serde_json::Value::String(event_type.clone());
    }
    ws.send(Message::Text(subscribe.to_string()))
        .await
        .map_err(|e| VmsError::Source(format!("ha_websocket: subscribe: {e}")))?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                        continue;
                    };
                    if value.get("type").and_then(|t| t.as_str()) == Some("event") {
                        let payload = value.get("event").cloned().unwrap_or(serde_json::Value::Null);
                        let event = Event::new(&TopicKey::Source(source_id), "ha_event", payload);
                        if event_tx.send(event).is_err() {
                            return Ok(()); // Event Bus forwarder is gone — nothing left to notify.
                        }
                    }
                    // "result" acks for the subscribe request and anything else are ignored.
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(VmsError::Source("ha_websocket: connection closed".into()))
                }
                Some(Ok(_)) => {} // ping/pong/binary frames are not meaningful here
                Some(Err(e)) => return Err(VmsError::Source(format!("ha_websocket: {e}"))),
            }
        }
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use tokio::time::timeout;

    use super::*;

    // Cancellation stops the task promptly even while the connection attempt
    // to an unreachable host is failing and retrying — mirrors the same
    // guarantee already proven for the file-watcher and MQTT adapters.
    #[tokio::test]
    async fn cancel_stops_task_without_a_reachable_server() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let config = serde_json::json!({
            "url": "ws://127.0.0.1:1/api/websocket",
            "access_token": "test-token",
        });
        let task = spawn(Uuid::new_v4(), config, tx, cancel.clone()).unwrap();

        cancel.cancel();
        timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not stop promptly after cancellation")
            .unwrap();
    }

    // Missing required config fields are rejected synchronously.
    #[test]
    fn missing_config_field_returns_error() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let config = serde_json::json!({ "url": "ws://localhost:8123/api/websocket" });
        let result = spawn(Uuid::new_v4(), config, tx, CancellationToken::new());
        assert!(result.is_err());
    }
}
