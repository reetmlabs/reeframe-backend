use vms_db::{CameraRepo, DestinationRepo, SourceRepo};
use vms_media::MediaManager;

/// Shared application state injected into every Salvo handler via `affix-state`.
///
/// Constructed once in `main.rs` and cloned into the router. All inner types
/// are either `Clone` (repos hold an `Arc`-backed SeaORM pool) or wrapped in
/// `Arc` (MediaManager) so cloning is cheap.
#[derive(Clone)]
pub struct AppState {
    pub camera_repo:   CameraRepo,
    pub source_repo:   SourceRepo,
    pub dest_repo:     DestinationRepo,
    pub media_manager: std::sync::Arc<MediaManager>,
}
