use std::path::PathBuf;
use std::sync::Arc;

use vms_core::{
    action::ActionConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_media::{MediaManager, RingBufferManager};

use crate::handlers::{delay, extract_clip, merge_clips, render_notification, snapshot, transcode, watermark};

// -- ActionContext --

/// Resources available to action handlers at runtime.
#[derive(Clone)]
pub struct ActionContext {
    /// Live camera pipeline manager — used by `snapshot`, `start_recording`, `stop_recording`.
    pub media: Option<Arc<MediaManager>>,
    /// Ring buffer manager — used by `extract_clip`.
    pub ring_buffer: Option<Arc<RingBufferManager>>,
    /// Directory where action output files (clips, snapshots) are written.
    pub recording_dir: PathBuf,
}

impl Default for ActionContext {
    fn default() -> Self {
        Self {
            media: None,
            ring_buffer: None,
            recording_dir: PathBuf::from("/tmp"),
        }
    }
}

// -- ActionDispatcher --

/// Dispatches `Action` and `DeviceControl` pipeline nodes to their handlers.
///
/// Handlers that are not yet implemented return [`NodeOutput::failure`] with a
/// clear "not yet implemented" message so in-progress pipelines degrade
/// gracefully rather than panicking.
pub struct ActionDispatcher;

impl ActionDispatcher {
    pub async fn dispatch(
        node_id: NodeId,
        config: &ActionConfig,
        input: &NodeInput,
        ctx: &ActionContext,
    ) -> NodeOutput {
        match config {
            ActionConfig::Delay(cfg) => delay::execute(node_id, cfg).await,
            ActionConfig::RenderNotification(cfg) => {
                render_notification::execute(node_id, cfg, input)
            }
            ActionConfig::ExtractClip(cfg) => extract_clip::execute(node_id, cfg, input, ctx).await,
            ActionConfig::Snapshot(cfg) => snapshot::execute(node_id, cfg, input, ctx).await,
            ActionConfig::Transcode(cfg) => transcode::execute(node_id, cfg, input, ctx).await,
            ActionConfig::Watermark(cfg) => watermark::execute(node_id, cfg, input, ctx).await,
            ActionConfig::MergeClips(cfg) => merge_clips::execute(node_id, cfg, input, ctx).await,
            other => NodeOutput::failure(
                node_id,
                format!("'{}' handler not yet implemented", other.action_type_str()),
            ),
        }
    }
}
