use std::sync::Arc;

use dashmap::DashMap;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vms_core::{event::Event, source::SourceType, VmsError};

use crate::{file_watcher, ha_websocket, mqtt};

/// Handle to a running source adapter task, held so [`SourceManager::stop`]
/// can cancel and join it.
struct RunningSource {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// Lazy-start / eager-stop lifecycle coordinator for external source adapters.
///
/// Mirrors the Resource Manager's Minimum Activation Principle: `start` spins
/// up an adapter's background task, `stop` cancels and joins it. Adapters
/// publish [`Event`]s onto the shared `event_tx` channel; the caller is
/// responsible for forwarding those events onto the Event Bus keyed by
/// `TopicKey::Source(source_id)`, which keeps this crate free of a dependency
/// on `vms-engine`.
pub struct SourceManager {
    running: DashMap<Uuid, RunningSource>,
    event_tx: mpsc::UnboundedSender<Event>,
}

impl SourceManager {
    pub fn new(event_tx: mpsc::UnboundedSender<Event>) -> Arc<Self> {
        Arc::new(Self {
            running: DashMap::new(),
            event_tx,
        })
    }

    /// Start the adapter for `source_id`. No-op if already running.
    pub async fn start(
        &self,
        source_id: Uuid,
        source_type: SourceType,
        config: serde_json::Value,
    ) -> Result<(), VmsError> {
        if self.running.contains_key(&source_id) {
            return Ok(());
        }

        let cancel = CancellationToken::new();
        let task = match source_type {
            SourceType::FileWatcher => {
                file_watcher::spawn(source_id, config, self.event_tx.clone(), cancel.clone())?
            }
            SourceType::Mqtt => {
                mqtt::spawn(source_id, config, self.event_tx.clone(), cancel.clone())?
            }
            SourceType::HaWebsocket => {
                ha_websocket::spawn(source_id, config, self.event_tx.clone(), cancel.clone())?
            }
            SourceType::Webhook => {
                // A webhook has no persistent connection to hold open — the
                // inbound `POST /webhooks/{id}` route (vms-api) publishes
                // events directly onto the Event Bus and never touches this
                // manager. This task exists only so `ResourceState::Running`
                // becomes true for this source, which is what that route
                // checks to decide whether it's currently accepting requests.
                let cancel_child = cancel.clone();
                tokio::spawn(async move { cancel_child.cancelled().await })
            }
            other => {
                return Err(VmsError::Source(format!(
                    "source adapter not implemented: {other:?}"
                )))
            }
        };

        self.running
            .insert(source_id, RunningSource { cancel, task });
        Ok(())
    }

    /// Stop the adapter for `source_id`. No-op if not currently running.
    pub async fn stop(&self, source_id: Uuid) -> Result<(), VmsError> {
        let Some((_, running)) = self.running.remove(&source_id) else {
            return Ok(());
        };
        running.cancel.cancel();
        running
            .task
            .await
            .map_err(|e| VmsError::Source(format!("adapter task panicked: {e}")))
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_watch_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("vms-sources-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // Starting and then stopping a file-watcher source succeeds and leaves no
    // entry behind — proves the Minimum Activation Principle lifecycle works
    // end-to-end through the manager, not just the adapter in isolation.
    #[tokio::test]
    async fn file_watcher_start_and_stop_round_trip() {
        let dir = temp_watch_dir();
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        let source_id = Uuid::new_v4();

        manager
            .start(
                source_id,
                SourceType::FileWatcher,
                serde_json::json!({ "path": dir.to_str().unwrap() }),
            )
            .await
            .unwrap();
        assert!(manager.running.contains_key(&source_id));

        manager.stop(source_id).await.unwrap();
        assert!(!manager.running.contains_key(&source_id));

        std::fs::remove_dir_all(&dir).ok();
    }

    // Stopping a source that was never started is a no-op, not an error —
    // callers (the Resource Manager) rely on this for idempotent release().
    #[tokio::test]
    async fn stop_unknown_source_is_a_noop() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        manager.stop(Uuid::new_v4()).await.unwrap();
    }

    // Source types without an adapter yet fail fast with a clear error
    // instead of silently doing nothing.
    #[tokio::test]
    async fn unimplemented_adapter_type_returns_error() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        let result = manager
            .start(Uuid::new_v4(), SourceType::ApiPoll, serde_json::json!({}))
            .await;
        assert!(result.is_err());
    }

    // Webhook has no connection of its own, but start/stop still round-trips
    // cleanly — this is what makes `ResourceState::Running` become true for
    // the inbound webhook route's "is this source currently acquired" check.
    #[tokio::test]
    async fn webhook_start_and_stop_round_trip() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        let source_id = Uuid::new_v4();

        manager
            .start(source_id, SourceType::Webhook, serde_json::json!({}))
            .await
            .unwrap();
        assert!(manager.running.contains_key(&source_id));

        manager.stop(source_id).await.unwrap();
        assert!(!manager.running.contains_key(&source_id));
    }

    // Same lifecycle round trip as the file watcher, through the MQTT
    // adapter — no broker needs to be reachable for start/stop bookkeeping
    // to work correctly.
    #[tokio::test]
    async fn mqtt_start_and_stop_round_trip() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        let source_id = Uuid::new_v4();

        manager
            .start(
                source_id,
                SourceType::Mqtt,
                serde_json::json!({ "host": "127.0.0.1", "port": 1, "topic": "test/topic" }),
            )
            .await
            .unwrap();
        assert!(manager.running.contains_key(&source_id));

        manager.stop(source_id).await.unwrap();
        assert!(!manager.running.contains_key(&source_id));
    }

    // Same lifecycle round trip through the HA WebSocket adapter — no server
    // needs to be reachable for start/stop bookkeeping to work correctly.
    #[tokio::test]
    async fn ha_websocket_start_and_stop_round_trip() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SourceManager::new(tx);
        let source_id = Uuid::new_v4();

        manager
            .start(
                source_id,
                SourceType::HaWebsocket,
                serde_json::json!({
                    "url": "ws://127.0.0.1:1/api/websocket",
                    "access_token": "test-token",
                }),
            )
            .await
            .unwrap();
        assert!(manager.running.contains_key(&source_id));

        manager.stop(source_id).await.unwrap();
        assert!(!manager.running.contains_key(&source_id));
    }
}
