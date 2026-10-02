//! Filesystem watcher source adapter.
//!
//! Watches a directory (or single file) and publishes an [`Event`] for every
//! create / modify / remove notification `notify` reports.

use std::path::Path;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vms_core::{
    event::{Event, TopicKey},
    VmsError,
};

/// Configuration for the [`vms_core::SourceType::FileWatcher`] adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct FileWatcherConfig {
    /// Directory or file to watch.
    pub path: String,
    /// Watch subdirectories recursively.
    #[serde(default)]
    pub recursive: bool,
}

/// How often the blocking watch loop checks for a cancellation request while
/// waiting on the next filesystem notification.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Start watching the path in `config` and return the task driving it.
///
/// The `notify` watcher is created synchronously so a bad path (or an
/// unsupported platform backend) surfaces as an `Err` to the caller
/// immediately, rather than failing silently inside the background task.
pub fn spawn(
    source_id: Uuid,
    config: serde_json::Value,
    event_tx: UnboundedSender<Event>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, VmsError> {
    let cfg: FileWatcherConfig = serde_json::from_value(config)?;

    let (notify_tx, notify_rx) = std_mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = RecommendedWatcher::new(notify_tx, NotifyConfig::default())
        .map_err(|e| VmsError::Source(format!("file_watcher: failed to create watcher: {e}")))?;

    let mode = if cfg.recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    watcher.watch(Path::new(&cfg.path), mode).map_err(|e| {
        VmsError::Source(format!("file_watcher: failed to watch {}: {e}", cfg.path))
    })?;

    let task = tokio::task::spawn_blocking(move || {
        // `watcher` is moved into the closure so it stays alive for the loop's
        // duration; dropping it when the closure returns stops the OS-level watch.
        let _watcher = watcher;
        loop {
            match notify_rx.recv_timeout(CANCEL_POLL_INTERVAL) {
                Ok(Ok(evt)) => {
                    for path in &evt.paths {
                        let payload = serde_json::json!({
                            "kind": format!("{:?}", evt.kind),
                            "path": path.display().to_string(),
                        });
                        let event = Event::new(&TopicKey::Source(source_id), "fs_change", payload);
                        if event_tx.send(event).is_err() {
                            return; // The Event Bus forwarder is gone, so nobody is listening.
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(source_id = %source_id, error = %e, "file_watcher: watch error");
                }
                Err(std_mpsc::RecvTimeoutError::Timeout) => {
                    if cancel.is_cancelled() {
                        return;
                    }
                }
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    });

    Ok(task)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    fn temp_watch_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("vms-sources-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // Creating a file inside the watched directory produces an Event on the
    // channel, tagged with the watching source's id.
    #[tokio::test]
    async fn file_create_publishes_event() {
        let dir = temp_watch_dir();
        let source_id = Uuid::new_v4();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = CancellationToken::new();

        let config = serde_json::json!({ "path": dir.to_str().unwrap() });
        let task = spawn(source_id, config, tx, cancel.clone()).unwrap();

        // Give the watcher a moment to install its OS-level hook before writing.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(dir.join("new_file.txt"), b"hello").unwrap();

        let event = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for fs_change event")
            .expect("channel closed unexpectedly");

        assert_eq!(event.event_type, "fs_change");
        assert_eq!(event.source_id, Some(source_id));

        cancel.cancel();
        task.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    // An invalid path is rejected synchronously instead of failing silently
    // inside the background task.
    #[test]
    fn nonexistent_path_returns_error() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let config = serde_json::json!({ "path": "/nonexistent/vms-sources-test-path" });
        let result = spawn(Uuid::new_v4(), config, tx, CancellationToken::new());
        assert!(result.is_err());
    }
}
