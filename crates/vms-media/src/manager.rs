use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use gstreamer::prelude::*;

use uuid::Uuid;
use vms_core::VmsError;

use crate::camera_stream::{build_camera_stream, spawn_monitor};
use crate::ring_buffer::RingBuffer;
use crate::ring_buffer_branch;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the Media Manager.
pub struct MediaConfig {
    /// Directory where MP4 chunk files are written.
    pub recording_dir: PathBuf,
    /// Duration of each recording chunk in seconds (default: 300 = 5 minutes).
    pub chunk_duration_secs: u64,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            recording_dir: PathBuf::from("/var/lib/onward/recordings"),
            chunk_duration_secs: 300,
        }
    }
}

// ── Internal per-camera handle ────────────────────────────────────────────────

struct CameraHandle {
    /// Keeps the pipeline alive alongside the monitor task.
    #[allow(dead_code)]
    pipeline: gstreamer::Pipeline,
    /// Send `()` to ask the monitor task to shut down cleanly.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Join handle for the bus-monitor / reconnect task.
    task: tokio::task::JoinHandle<()>,
}

// ── MediaManager ──────────────────────────────────────────────────────────────

/// Manages per-camera GStreamer pipelines.
///
/// Each started camera gets one pipeline:
/// `rtspsrc → rtph264depay → h264parse → tee → queue → splitmuxsink`
///
/// A background tokio task monitors the GStreamer bus for errors and EOS events
/// and automatically reconnects with exponential backoff (2 s → 60 s).
pub struct MediaManager {
    config: MediaConfig,
    cameras: Mutex<HashMap<Uuid, CameraHandle>>,
}

impl MediaManager {
    /// Create a new `MediaManager` and initialise GStreamer.
    ///
    /// `gstreamer::init()` is idempotent — safe to call multiple times.
    pub fn new(config: MediaConfig) -> Result<Self, VmsError> {
        gstreamer::init().map_err(|e| VmsError::Media(format!("GStreamer init failed: {e}")))?;
        std::fs::create_dir_all(&config.recording_dir)?;
        Ok(Self {
            config,
            cameras: Mutex::new(HashMap::new()),
        })
    }

    /// Start continuous recording for a camera.
    ///
    /// If the camera is already running this is a no-op.
    pub async fn start_camera(&self, camera_id: Uuid, rtsp_url: &str) -> Result<(), VmsError> {
        {
            let cameras = self.cameras.lock().unwrap();
            if cameras.contains_key(&camera_id) {
                return Ok(());
            }
        }

        let pipeline = build_camera_stream(
            camera_id,
            rtsp_url,
            &self.config.recording_dir,
            self.config.chunk_duration_secs,
        )?;

        pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| VmsError::Media(format!("start pipeline {camera_id}: {e}")))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = spawn_monitor(
            camera_id,
            pipeline.clone(),
            self.config.recording_dir.clone(),
            shutdown_rx,
        );

        self.cameras.lock().unwrap().insert(
            camera_id,
            CameraHandle {
                pipeline,
                shutdown_tx,
                task,
            },
        );

        tracing::info!(camera_id = %camera_id, rtsp_url, "Camera pipeline started");
        Ok(())
    }

    /// Stop recording and tear down the GStreamer pipeline for a camera.
    pub async fn stop_camera(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let handle = self.cameras.lock().unwrap().remove(&camera_id);

        if let Some(h) = handle {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
            tracing::info!(camera_id = %camera_id, "Camera pipeline stopped");
        }

        Ok(())
    }

    /// Stop all camera pipelines and wait for all monitor tasks to exit.
    pub async fn shutdown(&self) -> Result<(), VmsError> {
        let handles: Vec<CameraHandle> = {
            let mut cameras = self.cameras.lock().unwrap();
            cameras.drain().map(|(_, h)| h).collect()
        };

        for h in handles {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
        }

        tracing::info!("MediaManager shutdown complete");
        Ok(())
    }

    /// Attach a ring-buffer appsink branch to a running camera pipeline.
    ///
    /// Returns `VmsError::Media` if the camera is not currently running or if
    /// GStreamer fails to link the new branch.
    pub fn attach_ring_buffer(
        &self,
        camera_id: Uuid,
        ring_buffer: Arc<Mutex<RingBuffer>>,
    ) -> Result<(), VmsError> {
        let cameras = self.cameras.lock().unwrap();
        let handle = cameras.get(&camera_id).ok_or_else(|| {
            VmsError::Media(format!(
                "camera {camera_id} is not running — cannot attach ring buffer"
            ))
        })?;
        ring_buffer_branch::attach(&handle.pipeline, camera_id, ring_buffer)
    }

    /// Detach the ring-buffer branch from a running camera pipeline.
    ///
    /// No-op if the camera is not running or has no ring-buffer branch.
    /// Cleanup is asynchronous — see [`ring_buffer_branch::detach`].
    pub fn detach_ring_buffer(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let cameras = self.cameras.lock().unwrap();
        let Some(handle) = cameras.get(&camera_id) else {
            return Ok(());
        };
        ring_buffer_branch::detach(&handle.pipeline, camera_id)
    }

    /// Return `true` if a camera pipeline is currently running.
    pub fn is_running(&self, camera_id: Uuid) -> bool {
        self.cameras.lock().unwrap().contains_key(&camera_id)
    }

    /// Return the list of currently running camera IDs.
    pub fn running_cameras(&self) -> Vec<Uuid> {
        self.cameras.lock().unwrap().keys().copied().collect()
    }
}
