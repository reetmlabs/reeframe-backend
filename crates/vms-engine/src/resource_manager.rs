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

/// Fallback ring-buffer duration for a `RingBuffer` resource started outside
/// of `sync`, when `ring_buffer_secs` has nothing recorded for that camera.
/// Normally every acquire follows a `sync` call that records the real
/// per-camera requirement.
const DEFAULT_RING_BUFFER_SECS: u32 = 30;

/// Ref-counted lifecycle coordinator for every shared resource the engine manages.
///
/// A resource is started when its ref count goes 0 -> 1 and stopped when it
/// drops back 1 -> 0, so multiple VMS pipelines referencing the same camera or
/// destination share one GStreamer pipeline or connection pool.
pub struct ResourceManager {
    entries: DashMap<ResourceId, ResourceEntry>,
    /// Per-resource watch channel used to park concurrent `acquire` callers while
    /// a first caller is executing `start()`. The sender broadcasts the new
    /// `ResourceState` once startup completes (or fails).
    state_watches: DashMap<ResourceId, tokio::sync::watch::Sender<ResourceState>>,
    /// Per-camera ring buffer duration required by the currently enabled
    /// pipelines, kept up to date by every `sync` call. Consulted by
    /// `start()` when a `RingBuffer` resource actually needs starting.
    ring_buffer_secs: DashMap<Uuid, u32>,
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
            ring_buffer_secs: DashMap::new(),
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
    /// Intended for diagnostics and the status API, not for hot paths.
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
    /// firing. This only brings cameras back live; recording is resumed
    /// separately by `StartRecording` actions and by
    /// [`crate::reconcile_recording_intent`].
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
    /// only the delta between `old` and `new`, so a camera already running for
    /// a pipeline untouched by the change is neither stopped nor restarted.
    ///
    /// Acquire/release failures are logged and skipped so one unreachable
    /// resource does not block the rest of the batch.
    pub async fn sync(&self, old: &RegistrySnapshot, new: &RegistrySnapshot) {
        let ring_buffer_secs = ring_buffer_durations(new);
        for (&camera_id, &secs) in &ring_buffer_secs {
            self.ring_buffer_secs.insert(camera_id, secs);
        }

        let before = resource_counts(old);
        let after = resource_counts(new);

        let mut ids: Vec<ResourceId> = {
            let set: HashSet<ResourceId> = before
                .keys()
                .cloned()
                .chain(after.keys().cloned())
                .collect();
            set.into_iter().collect()
        };
        ids.sort_by_key(ResourceId::acquire_rank);

        // Release every dependent (e.g. a camera's RingBuffer) before what it
        // depends on (that camera's CameraPipeline), then acquire the other
        // way around, so a resource can never start before, or outlive,
        // the resource it attaches to. HashSet iteration order alone can't
        // guarantee this.
        for id in ids.iter().rev() {
            let before = before.get(id).copied().unwrap_or(0);
            let after = after.get(id).copied().unwrap_or(0);
            if before > after {
                for _ in 0..(before - after) {
                    if let Err(e) = self.release(id.clone()).await {
                        tracing::error!(resource = ?id, error = %e,
                            "Failed to release resource during registry sync, continuing");
                    }
                }
            }
        }
        for id in ids.iter() {
            let before = before.get(id).copied().unwrap_or(0);
            let after = after.get(id).copied().unwrap_or(0);
            if after > before {
                for _ in 0..(after - before) {
                    if let Err(e) = self.acquire(id.clone()).await {
                        tracing::error!(resource = ?id, error = %e,
                            "Failed to acquire resource during registry sync, continuing");
                    }
                }
            }
        }

        // When a pipeline that already references a ring buffer needs a
        // larger window (e.g. `post_event_secs` raised), the ref count doesn't
        // change and the delta loop above skips it, so grow running buffers here.
        for (camera_id, secs) in ring_buffer_secs {
            let id = ResourceId::RingBuffer(camera_id);
            let already_running = matches!(
                self.entries.get(&id).map(|e| e.state.clone()),
                Some(ResourceState::Running)
            );
            if already_running {
                if let Err(e) = self
                    .ring_buffers
                    .start(camera_id, secs, RingBufferMode::Memory)
                {
                    tracing::error!(%camera_id, error = %e,
                        "Failed to grow ring buffer during registry sync, continuing");
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
                    // Another task is starting this resource, so wait on its watch.
                    // Both DashMaps use independent shards, so this can't deadlock.
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
                    // Stopped, Stopping, or Error: this caller starts it.
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

            Action::Start => {
                let result = self.start_and_record(&id).await;
                if let Some(tx) = self.state_watches.get(&id) {
                    let state = match &result {
                        Ok(()) => ResourceState::Running,
                        Err(err) => ResourceState::Error(err.to_string()),
                    };
                    let _ = tx.send(state);
                }
                result
            }

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
    /// Returns `Ok(())` if the resource was never acquired.
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

    /// Runs `start()` for `id` and records the resulting `Running`/`Error` state.
    /// Shared by [`Self::acquire`]'s first-caller path and [`Self::enable_source`],
    /// which both need to start a resource and persist the outcome the same way.
    async fn start_and_record(&self, id: &ResourceId) -> Result<(), VmsError> {
        match self.start(id).await {
            Ok(()) => {
                if let Some(mut e) = self.entries.get_mut(id) {
                    e.state = ResourceState::Running;
                    e.started_at = Some(Utc::now());
                    e.last_error = None;
                }
                tracing::info!(resource = ?id, "Resource started");
                Ok(())
            }
            Err(err) => {
                if let Some(mut e) = self.entries.get_mut(id) {
                    e.state = ResourceState::Error(err.to_string());
                    e.last_error = Some(err.to_string());
                }
                tracing::error!(resource = ?id, error = %err, "Resource failed to start");
                Err(err)
            }
        }
    }

    // -- Source enabled/disabled hard gate --

    /// Stops a source's resource outright, independent of its ref count, and
    /// leaves it unable to be reacquired (see the enabled check in
    /// [`Self::start_source`]) until [`Self::enable_source`] is called for it.
    /// Call whenever a source's `enabled` flag flips to `false`.
    pub async fn disable_source(&self, source_id: Uuid) {
        let id = ResourceId::Source(source_id);
        let running = matches!(
            self.entries.get(&id).map(|e| e.state.clone()),
            Some(ResourceState::Running) | Some(ResourceState::Starting)
        );
        if running {
            if let Err(e) = self.stop(&id).await {
                tracing::error!(%source_id, error = %e,
                    "Failed to stop source during disable, continuing");
            }
        }
        if let Some(mut e) = self.entries.get_mut(&id) {
            e.state = ResourceState::Stopped;
            e.started_at = None;
        }
    }

    /// Lifts the hard gate set by [`Self::disable_source`] and restarts the
    /// resource if it's still referenced by an enabled pipeline. If nothing
    /// references it, it starts on the next `acquire` instead.
    pub async fn enable_source(&self, source_id: Uuid) -> Result<(), VmsError> {
        let id = ResourceId::Source(source_id);
        let still_referenced = self
            .entries
            .get(&id)
            .map(|e| e.ref_count > 0)
            .unwrap_or(false);
        if !still_referenced {
            return Ok(());
        }
        self.start_and_record(&id).await
    }

    // -- Start / stop dispatch --

    async fn start(&self, id: &ResourceId) -> Result<(), VmsError> {
        match id {
            ResourceId::CameraPipeline(cam_id) => self.start_live_camera(*cam_id).await,
            ResourceId::RingBuffer(cam_id) => {
                let secs = self
                    .ring_buffer_secs
                    .get(cam_id)
                    .map(|s| *s)
                    .unwrap_or(DEFAULT_RING_BUFFER_SECS);
                self.ring_buffers
                    .start(*cam_id, secs, RingBufferMode::Memory)
            }
            ResourceId::Source(id) => self.start_source(*id).await,
            ResourceId::DestinationPool(id) => {
                tracing::debug!(%id, "DestinationPool start, not yet implemented");
                Ok(())
            }
            ResourceId::AnalyticsBranch(cam_id) => {
                self.media.require_motion(*cam_id).await;
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
                tracing::debug!(%id, "DestinationPool stop, not yet implemented");
                Ok(())
            }
            ResourceId::AnalyticsBranch(cam_id) => {
                self.media.release_motion(*cam_id).await;
                Ok(())
            }
        }
    }

    /// Starts the camera's **live** pipeline for `ResourceId::CameraPipeline`.
    /// Recording is started separately by the `StartRecording`/`StopRecording`
    /// actions (see `vms-actions`); being referenced by a pipeline doesn't imply it.
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
        if !src.enabled {
            return Err(VmsError::Conflict(format!(
                "source {source_id} is disabled"
            )));
        }
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

/// Per-camera ring buffer size required by the currently enabled pipelines
/// in `snapshot`: the max of `PipelineCameraRef::ring_buffer_secs` (already
/// capped, see `derive_camera_refs`) across every pipeline referencing that
/// camera. Cameras with no `needs_ring_buffer` reference are absent.
fn ring_buffer_durations(snapshot: &RegistrySnapshot) -> HashMap<Uuid, u32> {
    let mut durations: HashMap<Uuid, u32> = HashMap::new();
    for pipeline in snapshot.pipelines.values() {
        for cam_ref in &pipeline.camera_refs {
            if cam_ref.needs_ring_buffer {
                let entry = durations.entry(cam_ref.camera_id).or_insert(0);
                *entry = (*entry).max(cam_ref.ring_buffer_secs);
            }
        }
    }
    durations
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
                ring_buffer_secs: 30,
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
            ring_buffer_secs: 0,
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

    #[test]
    fn ring_buffer_durations_maxes_across_pipelines_referencing_the_same_camera() {
        let camera_id = Uuid::new_v4();
        let cam_ref = |secs| PipelineCameraRef {
            camera_id,
            needs_ring_buffer: true,
            needs_analytics: false,
            ring_buffer_secs: secs,
        };
        let snapshot = snapshot_of(vec![
            pipeline_with(vec![cam_ref(30)], vec![], vec![]),
            pipeline_with(vec![cam_ref(90)], vec![], vec![]),
        ]);

        let durations = ring_buffer_durations(&snapshot);

        assert_eq!(durations[&camera_id], 90);
    }

    #[test]
    fn ring_buffer_durations_omits_cameras_that_dont_need_one() {
        let camera_id = Uuid::new_v4();
        let snapshot = snapshot_of(vec![pipeline_with(
            vec![PipelineCameraRef {
                camera_id,
                needs_ring_buffer: false,
                needs_analytics: false,
                ring_buffer_secs: 0,
            }],
            vec![],
            vec![],
        )]);

        assert!(ring_buffer_durations(&snapshot).is_empty());
    }
}
