use std::sync::Arc;

use dashmap::DashMap;
use vms_core::resource::{ResourceEntry, ResourceId};
use vms_media::MediaManager;

/// Ref-counted lifecycle coordinator for every shared resource the engine manages.
///
/// Applies the *Minimum Activation Principle*: a resource is started when its
/// ref count goes 0 → 1 and stopped when it drops back 1 → 0. This prevents
/// duplicate GStreamer pipelines or connection pools when multiple VMS pipelines
/// reference the same camera or destination.
pub struct ResourceManager {
    entries: DashMap<ResourceId, ResourceEntry>,
    media:   Arc<MediaManager>,
}

impl ResourceManager {
    pub fn new(media: Arc<MediaManager>) -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            media,
        })
    }

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
}
