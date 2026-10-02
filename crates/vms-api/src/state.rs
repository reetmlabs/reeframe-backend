use std::path::PathBuf;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use vms_core::VmsError;
use vms_db::{
    ApiKeyRepo, CameraRepo, ContactListRepo, ContactRepo, DailyRecordingCoverageRepo,
    DestinationRepo, EventsRepo, ExportJobRepo, PipelineRepo, PipelineRunRepo, RecordingRepo,
    SettingsRepo, SourceRepo, TileLayoutRepo, UserRepo,
};
use vms_engine::{
    EventBus, Metrics, PipelineRegistry, ResourceManager, StatMonitor, TransportDispatcher,
    TriggerEvaluator,
};
use vms_media::{MediaManager, RingBufferManager};

use crate::auth::LocalJwtAuthProvider;
use crate::coordinator_auth::CoordinatorJwksAuthProvider;

/// Parses raw uploaded config-file bytes (`POST /system/config-file`) into
/// `(key, value)` pairs for every known dynamic setting, or an error message
/// if the upload doesn't deserialize onto `AppConfig`. Implemented in
/// `vms-daemon` (which owns `AppConfig`) and injected as a plain function
/// because `vms-daemon` already depends on `vms-api`, so the reverse
/// dependency is not possible.
pub type ConfigFileParser =
    dyn Fn(&[u8]) -> Result<Vec<(&'static str, serde_json::Value)>, String> + Send + Sync;

/// Shared application state injected into every Salvo handler via `affix-state`.
///
/// Constructed once in `main.rs` and cloned into the router. All inner types
/// are either `Clone` (repos hold an `Arc`-backed SeaORM pool) or wrapped in
/// `Arc` (MediaManager, EventBus, PipelineRegistry) so cloning is cheap.
#[derive(Clone)]
pub struct AppState {
    /// Raw connection, used only by the readiness probe (`GET /health/ready`) to
    /// check liveness and pending-migration status directly. Every other handler
    /// goes through a repo instead.
    pub db: DatabaseConnection,
    pub camera_repo: CameraRepo,
    pub source_repo: SourceRepo,
    pub dest_repo: DestinationRepo,
    pub contact_repo: ContactRepo,
    pub contact_list_repo: ContactListRepo,
    pub pipeline_repo: PipelineRepo,
    pub pipeline_run_repo: PipelineRunRepo,
    pub recording_repo: RecordingRepo,
    pub daily_coverage_repo: DailyRecordingCoverageRepo,
    pub export_job_repo: ExportJobRepo,
    pub settings_repo: SettingsRepo,
    pub tile_layout_repo: TileLayoutRepo,
    pub events_repo: EventsRepo,
    /// Resolved `media.recording_dir` as of this boot. Settings handlers need
    /// it for the disk-threshold check when rebuilding `RetentionConfig` after
    /// a hot retention update. The setting is cold (changing it needs a
    /// restart), so caching it once is correct for the process lifetime.
    pub media_recording_dir: PathBuf,
    /// On-disk path of the config file `POST /system/config-file` backs up
    /// and replaces. Must match `vms_daemon::config::CONFIG_FILE_PATH`.
    pub config_file_path: PathBuf,
    pub config_parser: Arc<ConfigFileParser>,
    pub user_repo: UserRepo,
    pub api_key_repo: ApiKeyRepo,
    pub auth_provider: LocalJwtAuthProvider,
    /// `None` unless `[auth] mode = "oidc"`; set once at boot. `AuthMiddleware`
    /// accepts it as a second credential path in addition to `auth_provider`.
    pub coordinator_auth_provider: Option<Arc<CoordinatorJwksAuthProvider>>,
    pub media_manager: Arc<MediaManager>,
    pub ring_buffer_manager: Arc<RingBufferManager>,
    pub event_bus: Arc<EventBus>,
    pub pipeline_registry: Arc<PipelineRegistry>,
    pub resource_manager: Arc<ResourceManager>,
    pub trigger_evaluator: Arc<TriggerEvaluator>,
    pub stat_monitor: Arc<StatMonitor>,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    /// Reloads the pipeline registry and reconciles resource ref-counts against
    /// the change, in one call. Every handler that mutates a pipeline's nodes,
    /// edges, or triggers, a pipeline's own enabled state, or a source/destination
    /// a pipeline depends on, must call this after the mutation instead of
    /// touching `pipeline_registry`/`resource_manager` directly.
    pub async fn refresh_pipelines(&self) -> Result<(), VmsError> {
        let old = self.pipeline_registry.reload().await?;
        let new = self.pipeline_registry.snapshot();
        self.resource_manager.sync(&old, &new).await;
        Ok(())
    }

    /// Evict a destination's cached transport client. Call after a destination
    /// config update so the next delivery builds a fresh client instead of
    /// reusing one built from stale credentials.
    pub fn invalidate_transport(&self, dest_id: uuid::Uuid) {
        TransportDispatcher::invalidate(dest_id);
    }
}
