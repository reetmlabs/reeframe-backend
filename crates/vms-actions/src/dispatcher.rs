use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use uuid::Uuid;
use vms_core::{
    action::ActionConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_media::{MediaManager, RingBufferManager};

use crate::handlers::{
    compress, delay, encrypt, extract_clip, merge_clips, ptz_move, render_notification,
    set_stream_quality, snapshot, start_recording, stop_recording, transcode,
    trigger_alarm_output, watermark,
};

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
    /// Raw AES-256 key for the `encrypt` action handler.
    ///
    /// Sourced from the daemon's `VMS_ENCRYPTION_KEY`. Used when the action
    /// config specifies `key_ref = "default"`. Other key refs are resolved
    /// from `/etc/reeframe/keys/{name}` at runtime.
    pub encryption_key: Option<[u8; 32]>,
    /// Map of camera_id -> RTSP URL for all cameras in the DB.
    ///
    /// Populated by the executor before each pipeline run from `CameraRepo::list()`.
    /// Used by `start_recording` to start a camera that is not currently running.
    pub camera_rtsp_urls: HashMap<Uuid, String>,
}

impl Default for ActionContext {
    fn default() -> Self {
        Self {
            media: None,
            ring_buffer: None,
            recording_dir: PathBuf::from("/tmp"),
            encryption_key: None,
            camera_rtsp_urls: HashMap::new(),
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
            ActionConfig::Compress(cfg) => compress::execute(node_id, cfg, input, ctx).await,
            ActionConfig::Encrypt(cfg) => encrypt::execute(node_id, cfg, input, ctx).await,
            ActionConfig::StartRecording(cfg) => {
                start_recording::execute(node_id, cfg, input, ctx).await
            }
            ActionConfig::StopRecording(cfg) => {
                stop_recording::execute(node_id, cfg, input, ctx).await
            }
            ActionConfig::PtzMove(cfg) => ptz_move::execute(node_id, cfg, input, ctx).await,
            ActionConfig::SetStreamQuality(cfg) => {
                set_stream_quality::execute(node_id, cfg, input, ctx).await
            }
            ActionConfig::TriggerAlarmOutput(cfg) => {
                trigger_alarm_output::execute(node_id, cfg, input, ctx).await
            }
        }
    }
}
