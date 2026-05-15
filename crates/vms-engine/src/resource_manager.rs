use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use uuid::Uuid;
use vms_core::{
    resource::{ResourceEntry, ResourceId, ResourceState},
    VmsError,
};
use vms_core::RingBufferMode;
use vms_db::CameraRepo;
use vms_media::{MediaManager, RingBufferManager};

use crate::PipelineRegistry;

/// Ref-counted lifecycle coordinator for every shared resource the engine manages.
///
/// Applies the *Minimum Activation Principle*: a resource is started when its
/// ref count goes 0 → 1 and stopped when it drops back 1 → 0. This prevents
/// duplicate GStreamer pipelines or connection pools when multiple VMS pipelines
/// reference the same camera or destination.
/// Default ring-buffer duration used when a pipeline requests a ring buffer
/// but no explicit duration is stored in the camera ref.
const DEFAULT_RING_BUFFER_SECS: u32 = 30;

pub struct ResourceManager {
    entries:      DashMap<ResourceId, ResourceEntry>,
    media:        Arc<MediaManager>,
    cameras:      CameraRepo,
    ring_buffers: Arc<RingBufferManager>,
}

impl ResourceManager {
    pub fn new(
        media:        Arc<MediaManager>,
        cameras:      CameraRepo,
        ring_buffers: Arc<RingBufferManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            media,
            cameras,
            ring_buffers,
        })
    }

    // ── Read API ──────────────────────────────────────────────────────────────

    /// Return a snapshot of the entry for `id`, or `None` if the resource has
    /// never been acquired.
    pub fn status(&self, id: &ResourceId) -> Option<ResourceEntry> {
        self.entries.get(id).map(|e| e.clone())
    }

    /// Return a snapshot of every tracked resource entry.
    ///
    /// Intended for diagnostics and the status API — not for hot paths.
    pub fn all(&self) -> Vec<(ResourceId, ResourceEntry)> {
        self.entries
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    // ── Startup recovery ─────────────────────────────────────────────────────

    /// Acquire all resources required by the currently enabled pipelines in `registry`.
    ///
    /// Called once at daemon startup after the pipeline registry is loaded.
    /// Ensures every camera pipeline and source adapter needed by an enabled
    /// pipeline is running before the trigger evaluator begins firing.
    pub async fn recover(&self, registry: &PipelineRegistry) -> Result<(), VmsError> {
        let snapshot = registry.snapshot();

        for pipeline in snapshot.values() {
            for cam_ref in &pipeline.camera_refs {
                self.acquire(ResourceId::CameraPipeline(cam_ref.camera_id)).await?;
                if cam_ref.needs_ring_buffer {
                    self.acquire(ResourceId::RingBuffer(cam_ref.camera_id)).await?;
                }
                if cam_ref.needs_analytics {
                    self.acquire(ResourceId::AnalyticsBranch(cam_ref.camera_id)).await?;
                }
            }
            for &source_id in &pipeline.source_refs {
                self.acquire(ResourceId::Source(source_id)).await?;
            }
        }

        tracing::info!(pipelines = snapshot.len(), "Resource manager recovery complete");
        Ok(())
    }

    // ── Ref-count mutations ───────────────────────────────────────────────────

    /// Increment the ref count for `id`. Starts the resource if the count goes
    /// from 0 → 1 (or the resource is in an `Error` state and needs a retry).
    pub async fn acquire(&self, id: ResourceId) -> Result<(), VmsError> {
        let should_start = {
            let mut entry = self.entries.entry(id.clone()).or_default();
            entry.ref_count += 1;
            // Start on first acquire or after a previous failure.
            entry.ref_count == 1 || matches!(entry.state, ResourceState::Error(_))
        }; // shard lock released here — safe to .await below

        if should_start {
            if let Some(mut e) = self.entries.get_mut(&id) {
                e.state = ResourceState::Starting;
            }
            match self.start(&id).await {
                Ok(()) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Running;
                        e.started_at = Some(Utc::now());
                        e.last_error = None;
                    }
                    tracing::info!(resource = ?id, "Resource started");
                }
                Err(err) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Error(err.to_string());
                        e.last_error = Some(err.to_string());
                    }
                    tracing::error!(resource = ?id, error = %err, "Resource failed to start");
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    /// Decrement the ref count for `id`. Stops the resource if the count reaches 0.
    /// Idempotent — returns `Ok(())` if the resource was never acquired.
    pub async fn release(&self, id: ResourceId) -> Result<(), VmsError> {
        let should_stop = {
            let Some(mut entry) = self.entries.get_mut(&id) else {
                return Ok(());
            };
            if entry.ref_count == 0 {
                return Ok(());
            }
            entry.ref_count -= 1;
            entry.ref_count == 0
        }; // shard lock released here

        if should_stop {
            if let Some(mut e) = self.entries.get_mut(&id) {
                e.state = ResourceState::Stopping;
            }
            match self.stop(&id).await {
                Ok(()) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Stopped;
                        e.started_at = None;
                    }
                    tracing::info!(resource = ?id, "Resource stopped");
                }
                Err(err) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Error(err.to_string());
                        e.last_error = Some(err.to_string());
                    }
                    tracing::error!(resource = ?id, error = %err, "Resource failed to stop");
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    // ── Start / stop dispatch ─────────────────────────────────────────────────

    async fn start(&self, id: &ResourceId) -> Result<(), VmsError> {
        match id {
            ResourceId::CameraPipeline(cam_id) => self.start_camera(*cam_id).await,
            ResourceId::RingBuffer(cam_id) => self
                .ring_buffers
                .start(*cam_id, DEFAULT_RING_BUFFER_SECS, RingBufferMode::Memory),
            ResourceId::Source(id) => {
                tracing::debug!(%id, "Source start — not yet implemented");
                Ok(())
            }
            ResourceId::DestinationPool(id) => {
                tracing::debug!(%id, "DestinationPool start — not yet implemented");
                Ok(())
            }
            ResourceId::AnalyticsBranch(id) => {
                tracing::debug!(%id, "AnalyticsBranch start — not yet implemented");
                Ok(())
            }
        }
    }

    async fn stop(&self, id: &ResourceId) -> Result<(), VmsError> {
        match id {
            ResourceId::CameraPipeline(cam_id) => self.media.stop_camera(*cam_id).await,
            ResourceId::RingBuffer(cam_id) => self.ring_buffers.stop(*cam_id),
            ResourceId::Source(id) => {
                tracing::debug!(%id, "Source stop — not yet implemented");
                Ok(())
            }
            ResourceId::DestinationPool(id) => {
                tracing::debug!(%id, "DestinationPool stop — not yet implemented");
                Ok(())
            }
            ResourceId::AnalyticsBranch(id) => {
                tracing::debug!(%id, "AnalyticsBranch stop — not yet implemented");
                Ok(())
            }
        }
    }

    async fn start_camera(&self, cam_id: Uuid) -> Result<(), VmsError> {
        let Some((cam, password)) = self.cameras.get_decrypted(cam_id).await? else {
            return Err(VmsError::CameraNotFound(cam_id));
        };
        let rtsp_url = build_rtsp_url(&cam.rtsp_url, cam.username.as_deref(), password.as_deref());
        self.media.start_camera(cam_id, &rtsp_url).await
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Inject credentials into an RTSP URL if both username and password are present.
/// `rtsp://host/path` + (user, pass) → `rtsp://user:pass@host/path`
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    if let (Some(u), Some(p)) = (username, password) {
        if let Some(rest) = base_url.strip_prefix("rtsp://") {
            return format!("rtsp://{u}:{p}@{rest}");
        }
    }
    base_url.to_string()
}
