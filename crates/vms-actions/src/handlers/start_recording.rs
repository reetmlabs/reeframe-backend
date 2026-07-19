use vms_core::{
    action::StartRecordingConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Ensure a camera's recording pipeline is running.
///
/// If the camera is already recording this is a no-op — the handler returns
/// success immediately so the pipeline can continue. If the camera is not
/// running, the RTSP URL is looked up from `ctx.camera_rtsp_urls` (populated
/// by the executor before the run) and `MediaManager::start_camera` is called.
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

    // -- Already running — no-op --
    if media.is_running(camera_id) {
        tracing::debug!(
            node_id = %node_id,
            %camera_id,
            "StartRecording: camera already running, no-op"
        );
        return NodeOutput::success(node_id).with_metadata(serde_json::json!({
            "camera_id":      camera_id,
            "already_running": true,
            "duration_secs":  cfg.duration_secs,
            "quality":        cfg.quality,
        }));
    }

    // -- Look up RTSP URL --
    let Some(rtsp_url) = ctx.camera_rtsp_urls.get(&camera_id) else {
        return NodeOutput::failure(
            node_id,
            format!("start_recording: camera {camera_id} not found in camera map"),
        );
    };

    // -- Start the pipeline --
    // `ctx.camera_rtsp_urls` only carries the main stream (populated from
    // `MediaManager::rtsp_urls()`, not a DB lookup) — this handler has no way
    // to resolve `sub_rtsp_url` the way the REST/ResourceManager start paths
    // do, so no sub-stream pipeline is requested here; motion detection
    // falls back to the main stream's tee automatically. A
    // pipeline-triggered recording start is a rarer path than
    // manual/pipeline-referenced start, so this is an acceptable trade-off
    // rather than plumbing DB access into the action-handler layer for it.
    match media.start_camera(camera_id, rtsp_url, None).await {
        Ok(()) => {
            tracing::info!(
                node_id = %node_id,
                %camera_id,
                quality = %cfg.quality,
                duration_secs = cfg.duration_secs,
                "StartRecording: camera pipeline started"
            );
            NodeOutput::success(node_id).with_metadata(serde_json::json!({
                "camera_id":     camera_id,
                "already_running": false,
                "duration_secs": cfg.duration_secs,
                "quality":       cfg.quality,
            }))
        }
        Err(e) => NodeOutput::failure(node_id, format!("start_recording: {e}")),
    }
}
