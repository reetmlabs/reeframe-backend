use vms_core::{
    action::StopRecordingConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Stop a camera's recording pipeline.
///
/// Tears down the GStreamer pipeline for the resolved camera via
/// `MediaManager::stop_camera`. If the camera is not currently running the
/// call is a no-op and the handler returns success.
pub async fn execute(
    node_id: NodeId,
    cfg: &StopRecordingConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    // -- Resolve camera_id --
    let Some(camera_id) = cfg.camera_id.or(input.trigger_ctx.camera_id) else {
        return NodeOutput::failure(
            node_id,
            "stop_recording: no camera_id in config or trigger context",
        );
    };

    // -- Resolve media manager --
    let Some(media) = ctx.media.as_ref() else {
        return NodeOutput::failure(node_id, "stop_recording: media manager not configured");
    };

    // -- Stop the pipeline --
    match media.stop_camera(camera_id).await {
        Ok(()) => {
            tracing::info!(
                node_id = %node_id,
                %camera_id,
                "StopRecording: camera pipeline stopped"
            );
            NodeOutput::success(node_id)
                .with_metadata(serde_json::json!({ "camera_id": camera_id }))
        }
        Err(e) => NodeOutput::failure(node_id, format!("stop_recording: {e}")),
    }
}
