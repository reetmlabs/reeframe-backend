use std::sync::Arc;

use vms_db::{CameraRepo, DestinationRepo, PipelineRepo, SourceRepo};
use vms_engine::{EventBus, PipelineRegistry};
use vms_media::MediaManager;

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
    pub pipeline_repo: PipelineRepo,
    pub media_manager: Arc<MediaManager>,
    pub event_bus: Arc<EventBus>,
    pub pipeline_registry: Arc<PipelineRegistry>,
}
