use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use uuid::Uuid;
use vms_core::RingBufferMode;
use vms_core::{
    resource::{ResourceEntry, ResourceId, ResourceState},
    NodeType, VmsError,
};
use vms_db::{CameraRepo, SourceRepo};
use vms_media::{MediaManager, RingBufferManager};
use vms_sources::SourceManager;

use crate::pipeline_registry::RegistrySnapshot;
use crate::PipelineRegistry;

/// Ref-counted lifecycle coordinator for every shared resource the engine manages.
///
/// Applies the *Minimum Activation Principle*: a resource is started when its
/// ref count goes 0 -> 1 and stopped when it drops back 1 -> 0. This prevents
/// duplicate GStreamer pipelines or connection pools when multiple VMS pipelines
/// reference the same camera or destination.
/// Default ring-buffer duration used when a pipeline requests a ring buffer
/// but no explicit duration is stored in the camera ref.
const DEFAULT_RING_BUFFER_SECS: u32 = 30;

pub struct ResourceManager {
    entries: DashMap<ResourceId, ResourceEntry>,
    /// Per-resource watch channel used to park concurrent `acquire` callers while
    /// a first caller is executing `start()`. The sender broadcasts the new
    /// `ResourceState` once startup completes (or fails).
    state_watches: DashMap<ResourceId, tokio::sync::watch::Sender<ResourceState>>,
    media: Arc<MediaManager>,
    cameras: CameraRepo,
    ring_buffers: Arc<RingBufferManager>,
    sources: SourceRepo,
    source_manager: Arc<SourceManager>,
}

impl ResourceManager {
    pub fn new(
        media: Arc<MediaManager>,
        cameras: CameraRepo,
        ring_buffers: Arc<RingBufferManager>,
        sources: SourceRepo,
        source_manager: Arc<SourceManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            state_watches: DashMap::new(),
            media,
            cameras,
            ring_buffers,
            sources,
            source_manager,
        })
    }

    // -- Read API --

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

    // -- Startup recovery --

    /// Acquire all resources required by the currently enabled pipelines in `registry`.
    ///
    /// Called once at daemon startup after the pipeline registry is loaded.
    /// Ensures every camera's live pipeline and source adapter needed by an
    /// enabled pipeline is running before the trigger evaluator begins
    /// firing. This does not resume recording — a camera that was recording
    /// before shutdown only comes back live; recording resumes once its
    /// pipeline's `StartRecording` action fires again (e.g. the trigger
    /// re-evaluates true) or an operator restarts it explicitly.
    pub async fn recover(&self, registry: &PipelineRegistry) -> Result<(), VmsError> {
        let snapshot = registry.snapshot();
        let empty = RegistrySnapshot {
            pipelines: HashMap::new(),
            trigger_index: HashMap::new(),
        };
        self.sync(&empty, &snapshot).await;

        tracing::info!(
            pipelines = snapshot.pipelines.len(),
            "Resource manager recovery complete"
        );
        Ok(())
    }

    // -- Hot reload --

    /// Reconcile ref-counts against a registry change, acquiring or releasing
    /// only the delta between `old` and `new` — a camera already running for a
    /// pipeline untouched by the change is neither stopped nor restarted.
    ///
    /// Every acquire/release below is logged-and-continued rather than
    /// `?`-propagated: one stale/unreachable resource must not block every
    /// other resource in the same batch from being reconciled.
    pub async fn sync(&self, old: &RegistrySnapshot, new: &RegistrySnapshot) {
        let before = resource_counts(old);
        let after = resource_counts(new);

        let mut ids: HashSet<ResourceId> = before.keys().cloned().collect();
        ids.extend(after.keys().cloned());

        for id in ids {
            let before = before.get(&id).copied().unwrap_or(0);
            let after = after.get(&id).copied().unwrap_or(0);

            if after > before {
                for _ in 0..(after - before) {
                    if let Err(e) = self.acquire(id.clone()).await {
                        tracing::error!(resource = ?id, error = %e,
                            "Failed to acquire resource during registry sync — continuing");
                    }
                }
            } else if before > after {
                for _ in 0..(before - after) {
                    if let Err(e) = self.release(id.clone()).await {
                        tracing::error!(resource = ?id, error = %e,
                            "Failed to release resource during registry sync — continuing");
                    }
                }
            }
        }
    }

    // -- Ref-count mutations --

    /// Increment the ref count for `id`. Starts the resource if the count goes
    /// from 0 -> 1 (or the resource is in an `Error` state and needs a retry).
    ///
    /// If a concurrent caller is already starting the same resource, this call
    /// parks on a `watch` channel and returns only once the resource reaches
    /// `Running` (or fails). This prevents a second caller from proceeding with
    /// a half-started resource.
    pub async fn acquire(&self, id: ResourceId) -> Result<(), VmsError> {
        enum Action {
            Start,
            Wait(tokio::sync::watch::Receiver<ResourceState>),
            Ready,
        }

        let action = {
            let mut entry = self.entries.entry(id.clone()).or_default();
            match entry.state.clone() {
                ResourceState::Starting => {
                    // Another task is starting this resource — subscribe to its watch
                    // and wait. Both DashMaps use independent shards; no deadlock.
                    entry.ref_count += 1;
                    let rx = self
                        .state_watches
                        .get(&id)
                        .expect("BUG: Starting state without a watch sender")
                        .subscribe();
                    Action::Wait(rx)
                }
                ResourceState::Running => {
                    entry.ref_count += 1;
                    Action::Ready
                }
                _ => {
                    // Stopped, Stopping, or Error — we are responsible for starting.
                    entry.ref_count += 1;
                    entry.state = ResourceState::Starting;
                    let (tx, _) = tokio::sync::watch::channel(ResourceState::Starting);
                    self.state_watches.insert(id.clone(), tx);
                    Action::Start
                }
            }
        }; // entries shard lock released

        match action {
            Action::Ready => Ok(()),

            Action::Start => match self.start(&id).await {
                Ok(()) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Running;
                        e.started_at = Some(Utc::now());
                        e.last_error = None;
                    }
                    if let Some(tx) = self.state_watches.get(&id) {
                        let _ = tx.send(ResourceState::Running);
                    }
                    tracing::info!(resource = ?id, "Resource started");
                    Ok(())
                }
                Err(err) => {
                    if let Some(mut e) = self.entries.get_mut(&id) {
                        e.state = ResourceState::Error(err.to_string());
                        e.last_error = Some(err.to_string());
                    }
                    if let Some(tx) = self.state_watches.get(&id) {
                        let _ = tx.send(ResourceState::Error(err.to_string()));
                    }
                    tracing::error!(resource = ?id, error = %err, "Resource failed to start");
                    Err(err)
                }
            },

            Action::Wait(mut rx) => loop {
                rx.changed()
                    .await
                    .map_err(|_| VmsError::Media("resource watch closed before ready".into()))?;
                match rx.borrow().clone() {
                    ResourceState::Running => return Ok(()),
                    ResourceState::Error(e) => {
                        return Err(VmsError::Media(format!("resource failed to start: {e}")))
                    }
                    _ => continue,
                }
            },
        }
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

    // -- Start / stop dispatch --

    async fn start(&self, id: &ResourceId) -> Result<(), VmsError> {
        match id {
            ResourceId::CameraPipeline(cam_id) => self.start_live_camera(*cam_id).await,
            ResourceId::RingBuffer(cam_id) => {
                self.ring_buffers
                    .start(*cam_id, DEFAULT_RING_BUFFER_SECS, RingBufferMode::Memory)
            }
            ResourceId::Source(id) => self.start_source(*id).await,
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
            ResourceId::CameraPipeline(cam_id) => self.media.stop_live(*cam_id).await,
            ResourceId::RingBuffer(cam_id) => self.ring_buffers.stop(*cam_id),
            ResourceId::Source(id) => self.source_manager.stop(*id).await,
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

    /// Acquire `ResourceId::CameraPipeline` for `cam_id` — brings up the
    /// camera's **live** pipeline only (no recording). Continuous recording
    /// is a separate, explicit concern handled by the `StartRecording`/
    /// `StopRecording` actions (see `vms-actions`), not implied by a camera
    /// merely being referenced by an enabled pipeline.
    async fn start_live_camera(&self, cam_id: Uuid) -> Result<(), VmsError> {
        let Some((cam, password)) = self.cameras.get_decrypted(cam_id).await? else {
            return Err(VmsError::CameraNotFound(cam_id));
        };
        let rtsp_url = build_rtsp_url(&cam.rtsp_url, cam.username.as_deref(), password.as_deref());
        let sub_rtsp_url = cam
            .sub_rtsp_url
            .as_deref()
            .map(|sub| build_rtsp_url(sub, cam.username.as_deref(), password.as_deref()));
        self.media
            .start_live(cam_id, &rtsp_url, sub_rtsp_url.as_deref())
            .await
    }

    async fn start_source(&self, source_id: Uuid) -> Result<(), VmsError> {
        let Some(src) = self.sources.get_decrypted(source_id).await? else {
            return Err(VmsError::SourceNotFound(source_id));
        };
        self.source_manager
            .start(source_id, source_type_from_db(src.source_type), src.config)
            .await
    }
}

// -- Helpers --

/// Count how many pipelines in `snapshot` reference each resource.
///
/// One count per (pipeline, reference) pair, so a camera referenced by two
/// enabled pipelines counts as 2. [`ResourceManager::sync`] diffs this against
/// another snapshot's counts to know how many times to acquire or release.
fn resource_counts(snapshot: &RegistrySnapshot) -> HashMap<ResourceId, usize> {
    let mut counts: HashMap<ResourceId, usize> = HashMap::new();
    for pipeline in snapshot.pipelines.values() {
        for cam_ref in &pipeline.camera_refs {
            *counts
                .entry(ResourceId::CameraPipeline(cam_ref.camera_id))
                .or_default() += 1;
            if cam_ref.needs_ring_buffer {
                *counts
                    .entry(ResourceId::RingBuffer(cam_ref.camera_id))
                    .or_default() += 1;
            }
            if cam_ref.needs_analytics {
                *counts
                    .entry(ResourceId::AnalyticsBranch(cam_ref.camera_id))
                    .or_default() += 1;
            }
        }
        for &source_id in &pipeline.source_refs {
            *counts.entry(ResourceId::Source(source_id)).or_default() += 1;
        }
        for node in pipeline.dag.nodes.values() {
            if node.node_type == NodeType::Transport {
                if let Some(dest_id) = node.destination_id {
                    *counts
                        .entry(ResourceId::DestinationPool(dest_id))
                        .or_default() += 1;
                }
            }
        }
    }
    counts
}

/// Map the DB-level source-type enum to the domain-level one, the same way
/// `PipelineRepo::trigger_from_db` maps `pipeline_trigger::TriggerType`.
fn source_type_from_db(db_type: vms_db::entities::source::SourceType) -> vms_core::SourceType {
    use vms_db::entities::source::SourceType as Db;
    match db_type {
        Db::Mqtt => vms_core::SourceType::Mqtt,
        Db::Webhook => vms_core::SourceType::Webhook,
        Db::ApiPoll => vms_core::SourceType::ApiPoll,
        Db::HaWebsocket => vms_core::SourceType::HaWebsocket,
        Db::FileWatcher => vms_core::SourceType::FileWatcher,
    }
}

/// Inject credentials into an RTSP URL if both username and password are present.
/// `rtsp://host/path` + (user, pass) -> `rtsp://user:pass@host/path`
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    if let (Some(u), Some(p)) = (username, password) {
        if let Some(rest) = base_url.strip_prefix("rtsp://") {
            return format!("rtsp://{u}:{p}@{rest}");
        }
    }
    base_url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vms_core::pipeline::{CompiledPipeline, PipelineCameraRef, PipelineDag, PipelineNode};

    fn empty_dag(root_id: Uuid, nodes: Vec<PipelineNode>) -> PipelineDag {
        PipelineDag {
            nodes: nodes.into_iter().map(|n| (n.id, n)).collect(),
            edges: vec![],
            topological_order: vec![],
            adjacency: HashMap::new(),
            parents: HashMap::new(),
            edge_types: HashMap::new(),
            root_id,
        }
    }

    fn transport_node(pipeline_id: Uuid, destination_id: Uuid) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id,
            node_type: NodeType::Transport,
            action_config: None,
            destination_id: Some(destination_id),
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
            unresolved_reference: false,
        }
    }

    fn pipeline_with(
        camera_refs: Vec<PipelineCameraRef>,
        source_refs: Vec<Uuid>,
        nodes: Vec<PipelineNode>,
    ) -> CompiledPipeline {
        let id = Uuid::new_v4();
        CompiledPipeline {
            id,
            name: "test".into(),
            enabled: true,
            dag: empty_dag(Uuid::new_v4(), nodes),
            triggers: vec![],
            camera_refs,
            source_refs,
        }
    }

    fn snapshot_of(pipelines: Vec<CompiledPipeline>) -> RegistrySnapshot {
        RegistrySnapshot {
            pipelines: pipelines.into_iter().map(|p| (p.id, Arc::new(p))).collect(),
            trigger_index: HashMap::new(),
        }
    }

    #[test]
    fn resource_counts_counts_camera_ring_buffer_analytics_source_and_destination() {
        let camera_id = Uuid::new_v4();
        let source_id = Uuid::new_v4();
        let destination_id = Uuid::new_v4();

        let pipeline = pipeline_with(
            vec![PipelineCameraRef {
                camera_id,
                needs_ring_buffer: true,
                needs_analytics: true,
            }],
            vec![source_id],
            vec![transport_node(Uuid::new_v4(), destination_id)],
        );
        let snapshot = snapshot_of(vec![pipeline]);

        let counts = resource_counts(&snapshot);

        assert_eq!(counts[&ResourceId::CameraPipeline(camera_id)], 1);
        assert_eq!(counts[&ResourceId::RingBuffer(camera_id)], 1);
        assert_eq!(counts[&ResourceId::AnalyticsBranch(camera_id)], 1);
        assert_eq!(counts[&ResourceId::Source(source_id)], 1);
        assert_eq!(counts[&ResourceId::DestinationPool(destination_id)], 1);
    }

    #[test]
    fn resource_counts_sums_a_camera_referenced_by_two_pipelines() {
        let camera_id = Uuid::new_v4();
        let cam_ref = || PipelineCameraRef {
            camera_id,
            needs_ring_buffer: false,
            needs_analytics: false,
        };
        let snapshot = snapshot_of(vec![
            pipeline_with(vec![cam_ref()], vec![], vec![]),
            pipeline_with(vec![cam_ref()], vec![], vec![]),
        ]);

        let counts = resource_counts(&snapshot);

        assert_eq!(counts[&ResourceId::CameraPipeline(camera_id)], 2);
    }

    #[test]
    fn resource_counts_ignores_non_transport_nodes_with_no_destination() {
        let mut action_node = transport_node(Uuid::new_v4(), Uuid::new_v4());
        action_node.node_type = NodeType::Action;
        action_node.destination_id = None;
        let snapshot = snapshot_of(vec![pipeline_with(vec![], vec![], vec![action_node])]);

        let counts = resource_counts(&snapshot);

        assert!(counts.is_empty());
    }
}
