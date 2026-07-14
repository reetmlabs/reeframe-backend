use std::sync::Arc;

use vms_db::{
    ApiKeyRepo, CameraRepo, ContactListRepo, ContactRepo, DestinationRepo, PipelineRepo,
    PipelineRunRepo, SourceRepo, UserRepo,
};
use vms_engine::{EventBus, PipelineRegistry, ResourceManager, StatMonitor, TriggerEvaluator};
use vms_media::{MediaManager, RingBufferManager};

use crate::auth::LocalJwtAuthProvider;

/// Shared application state injected into every Salvo handler via `affix-state`.
///
/// Constructed once in `main.rs` and cloned into the router. All inner types
/// are either `Clone` (repos hold an `Arc`-backed SeaORM pool) or wrapped in
/// `Arc` (MediaManager, EventBus, PipelineRegistry) so cloning is cheap.
#[derive(Clone)]
pub struct AppState {
    pub camera_repo: CameraRepo,
    pub source_repo: SourceRepo,
    pub dest_repo: DestinationRepo,
    pub contact_repo: ContactRepo,
    pub contact_list_repo: ContactListRepo,
    pub pipeline_repo: PipelineRepo,
    pub pipeline_run_repo: PipelineRunRepo,
    pub user_repo: UserRepo,
    pub api_key_repo: ApiKeyRepo,
    pub auth_provider: LocalJwtAuthProvider,
    pub media_manager: Arc<MediaManager>,
    pub ring_buffer_manager: Arc<RingBufferManager>,
    pub event_bus: Arc<EventBus>,
    pub pipeline_registry: Arc<PipelineRegistry>,
    pub resource_manager: Arc<ResourceManager>,
    pub trigger_evaluator: Arc<TriggerEvaluator>,
    pub stat_monitor: Arc<StatMonitor>,
}
