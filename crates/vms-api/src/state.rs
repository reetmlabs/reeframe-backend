use std::path::PathBuf;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use vms_db::{
    ApiKeyRepo, CameraRepo, ContactListRepo, ContactRepo, DestinationRepo, ExportJobRepo,
    PipelineRepo, PipelineRunRepo, RecordingRepo, SettingsRepo, SourceRepo, UserRepo,
};
use vms_engine::{
    EventBus, Metrics, PipelineRegistry, ResourceManager, StatMonitor, TriggerEvaluator,
};
use vms_media::{MediaManager, RingBufferManager};

use crate::auth::LocalJwtAuthProvider;
use crate::coordinator_auth::CoordinatorJwksAuthProvider;

/// Parses raw uploaded config-file bytes (`POST /system/config-file`) into
/// `(key, value)` pairs for every known dynamic setting, or an error message
/// if the upload doesn't deserialize onto `AppConfig`. Implemented in
/// `vms-daemon` (which owns `AppConfig`) and injected here as a plain
/// function so `vms-api` never needs a dependency on `vms-daemon` — the
/// dependency already runs the other way (`vms-daemon` depends on `vms-api`
/// to build the router).
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
    pub export_job_repo: ExportJobRepo,
    pub settings_repo: SettingsRepo,
    /// Resolved `media.recording_dir` as of this boot — settings handlers
    /// need it for the retention disk-threshold check when rebuilding a
    /// `RetentionConfig` after a hot retention update; it's cold (a
    /// restart is needed to actually move where new chunks land), so
    /// caching it once here is always correct for the process lifetime.
    pub media_recording_dir: PathBuf,
    /// On-disk path of the config file `POST /system/config-file` backs up
    /// and replaces — must match `vms_daemon::config::CONFIG_FILE_PATH`.
    pub config_file_path: PathBuf,
    pub config_parser: Arc<ConfigFileParser>,
    pub user_repo: UserRepo,
    pub api_key_repo: ApiKeyRepo,
    pub auth_provider: LocalJwtAuthProvider,
    /// `None` unless `[auth] mode = "oidc"` — set once at boot. Consumed by
    /// `AuthMiddleware` as the second accepted credential path, alongside
    /// (never instead of) `auth_provider`.
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
