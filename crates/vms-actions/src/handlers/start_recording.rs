use vms_core::{
    action::StartRecordingConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Ensure a camera is recording.
///
/// If the camera is already recording this is a no-op — the handler returns
/// success immediately so the pipeline can continue. Otherwise the RTSP URL
/// is looked up from `ctx.camera_rtsp_urls` (populated by the executor
/// before the run) and `MediaManager::start_recording` is called — it brings
/// the live pipeline up first if it isn't already (recording never implies
/// the reverse: a camera can be live without this action ever having run).
///
/// `duration_secs` and `quality` from [`StartRecordingConfig`] are recorded in
/// `NodeOutput::metadata` for downstream nodes but are not enforced here —
/// timed stop is the responsibility of a downstream `stop_recording` node or a
/// separate scheduled pipeline.
pub async fn execute(
    node_id: NodeId,
    cfg: &StartRecordingConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    // -- Resolve camera_id --
    let Some(camera_id) = cfg.camera_id.or(input.trigger_ctx.camera_id) else {
        return NodeOutput::failure(
            node_id,
            "start_recording: no camera_id in config or trigger context",
        );
    };

    // -- Resolve media manager --
    let Some(media) = ctx.media.as_ref() else {
        return NodeOutput::failure(node_id, "start_recording: media manager not configured");
    };

    // -- Already recording — no-op --
    if media.is_recording(camera_id) {
        tracing::debug!(
            node_id = %node_id,
            %camera_id,
            "StartRecording: camera already recording, no-op"
        );
        return NodeOutput::success(node_id).with_metadata(serde_json::json!({
            "camera_id":        camera_id,
            "already_recording": true,
            "duration_secs":    cfg.duration_secs,
            "quality":          cfg.quality,
        }));
    }

    // -- Look up RTSP URL --
    let Some(rtsp_url) = ctx.camera_rtsp_urls.get(&camera_id) else {
        return NodeOutput::failure(
            node_id,
            format!("start_recording: camera {camera_id} not found in camera map"),
        );
    };

    // -- Start recording --
    // `ctx.camera_rtsp_urls` only lists cameras whose pipeline is already
    // running, so this never starts one. The running camera keeps the sub
    // stream it was started with, and analytics use that, so no sub URL is
    // needed here.
    match media.start_recording(camera_id, rtsp_url, None).await {
        Ok(()) => {
            tracing::info!(
                node_id = %node_id,
                %camera_id,
                quality = %cfg.quality,
                duration_secs = cfg.duration_secs,
                "StartRecording: recording started"
            );
            NodeOutput::success(node_id).with_metadata(serde_json::json!({
                "camera_id":        camera_id,
                "already_recording": false,
                "duration_secs":    cfg.duration_secs,
                "quality":          cfg.quality,
            }))
        }
        Err(e) => NodeOutput::failure(node_id, format!("start_recording: {e}")),
    }
}
