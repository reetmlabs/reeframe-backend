use vms_core::{
    action::SnapshotConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

/// Capture a single still frame from a running camera.
///
/// Taps the camera's running main stream, waits up to 5 s for the first
/// decoded frame, encodes it as JPEG or PNG, and saves it to the action
/// output directory. Fails if the camera isn't running. Returns the file
/// path as the node artifact.
pub async fn execute(
    node_id: NodeId,
    cfg: &SnapshotConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    // -- Resolve camera_id --
    let Some(camera_id) = cfg.camera_id.or(input.trigger_ctx.camera_id) else {
        return NodeOutput::failure(
            node_id,
            "snapshot: no camera_id in config or trigger context",
        );
    };

    // -- Resolve media manager --
    let Some(media) = ctx.media.as_ref() else {
        return NodeOutput::failure(node_id, "snapshot: media manager not configured");
    };

    tracing::debug!(
        node_id   = %node_id,
        camera_id = %camera_id,
        format    = %cfg.format,
        quality   = cfg.quality,
        "Snapshot: capturing"
    );

    match media
        .capture_snapshot(camera_id, &cfg.format, cfg.quality, &ctx.recording_dir)
        .await
    {
        Ok(path) => {
            tracing::info!(node_id = %node_id, path = %path.display(), "Snapshot: done");
            NodeOutput::success(node_id).with_artifact(path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("snapshot: {e}")),
    }
}
