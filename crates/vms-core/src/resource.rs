//! Resource lifecycle tracking for the Resource Manager.
//!
//! The Resource Manager applies a *Minimum Activation Principle*: a shared
//! resource (camera pipeline, ring-buffer, analytics branch, destination pool)
//! is started the first time its reference count goes from 0 -> 1 and stopped
//! the moment it drops back to 1 -> 0.  This avoids duplicate GStreamer
//! pipelines or connection pools when multiple VMS pipelines reference the
//! same camera or destination.
//!
//! [`ResourceId`] is the key into the Resource Manager's [`DashMap`].
//! [`ResourceEntry`] holds the mutable state for each resource.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Where a camera's ring buffer keeps its frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RingBufferMode {
    /// Frames are held in a `VecDeque` in process memory.
    Memory,
    /// Frames are written to a rolling temp file on disk.
    /// Not yet implemented — falls back to `Memory` at runtime.
    Disk,
}

/// Uniquely identifies a managed resource tracked by the Resource Manager.
///
/// The variant encodes both the *kind* of resource and the UUID of the entity
/// it belongs to, so the Resource Manager can fan-out correctly when a camera
/// or source is referenced by multiple pipelines.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceId {
    /// An external source adapter (MQTT, Home Assistant WebSocket, HTTP poller, etc.).
    Source(Uuid),
    /// The GStreamer pipeline for a camera (`rtspsrc -> rtph264depay -> tee`).
    CameraPipeline(Uuid),
    /// The ring-buffer `appsink` branch and its drain task for a camera.
    RingBuffer(Uuid),
    /// The analytics `appsink` branch for a camera (feeds the `AnalyticsProvider`).
    AnalyticsBranch(Uuid),
    /// A destination connection pool (S3 client, SMTP transport, webhook client, etc.).
    DestinationPool(Uuid),
}

/// Lifecycle state of a managed resource.
///
/// State transitions follow the sequence:
/// `Stopped` -> `Starting` -> `Running` -> `Stopping` -> `Stopped`
///
/// If an error occurs at any point the resource moves to `Error(reason)`.
/// The Resource Manager will attempt a restart the next time a pipeline
/// increments the reference count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResourceState {
    /// The resource is not running and holds no OS handles.
    Stopped,
    /// Startup is in progress (e.g. RTSP negotiation, TCP handshake).
    Starting,
    /// The resource is healthy and producing / accepting data.
    Running,
    /// A graceful shutdown is in progress.
    Stopping,
    /// The resource failed; the inner string is the last error message.
    Error(String),
}

/// Per-resource entry held in the Resource Manager's registry.
///
/// Stored in a [`DashMap<ResourceId, ResourceEntry>`] inside the Resource
/// Manager.  All mutations are protected by the DashMap shard lock.
#[derive(Debug, Clone)]
pub struct ResourceEntry {
    /// Current lifecycle state of this resource.
    pub state: ResourceState,
    /// Number of enabled pipelines currently referencing this resource.
    ///
    /// The resource is started when this increments from 0 to 1 and stopped
    /// when it decrements from 1 to 0 (Minimum Activation Principle).
    pub ref_count: usize,
    /// The last error message, preserved across a transition to [`ResourceState::Error`]
    /// so operators can diagnose transient failures.
    pub last_error: Option<String>,
    /// Wall-clock time at which the resource last entered [`ResourceState::Running`].
    pub started_at: Option<DateTime<Utc>>,
}

impl ResourceEntry {
    /// Create a new entry in the [`ResourceState::Stopped`] state with `ref_count` = 0.
    pub fn new() -> Self {
        Self {
            state: ResourceState::Stopped,
            ref_count: 0,
            last_error: None,
            started_at: None,
        }
    }
}

impl Default for ResourceEntry {
    fn default() -> Self {
        Self::new()
    }
}
