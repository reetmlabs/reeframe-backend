use std::time::Duration;

use vms_core::{
    action::ExtractClipConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

/// Extract a clip from the ring buffer around the moment the pipeline fired.
///
/// The extraction window is `[event_pts - pre_event_secs, event_pts + post_event_secs]`
/// where `event_pts` is the latest buffered PTS at the time of execution — a
/// reliable proxy for "now" in pipeline time.
///
/// # Limitations
///
/// - The ring buffer must be running for the target camera. If it is not,
///   the handler returns [`NodeOutput::failure`].
/// - `use_manual_range = true` is not yet supported. When set, it falls back
///   to the latest-PTS behaviour with a warning in the run log.
pub async fn execute(
    node_id: NodeId,
    cfg: &ExtractClipConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    // -- Resolve camera_id --
    let Some(camera_id) = cfg.camera_id.or(input.trigger_ctx.camera_id) else {
        return NodeOutput::failure(
            node_id,
            "extract_clip: no camera_id in config or trigger context",
        );
    };

    // -- Resolve ring buffer --
    let Some(ring_buffer) = ctx.ring_buffer.as_ref() else {
        return NodeOutput::failure(node_id, "extract_clip: ring buffer manager not configured");
    };

    if cfg.use_manual_range {
        tracing::warn!(
            node_id = %node_id,
            "extract_clip: use_manual_range is not yet supported; falling back to latest-PTS mode"
        );
    }

    // -- Anchor the extraction window to the latest buffered PTS --
    let event_pts = ring_buffer.latest_pts(camera_id).unwrap_or(Duration::ZERO);

    tracing::debug!(
        node_id = %node_id,
        camera_id = %camera_id,
        pre  = cfg.pre_event_secs,
        post = cfg.post_event_secs,
        event_pts_secs = event_pts.as_secs(),
        "ExtractClip: extracting"
    );

    match ring_buffer
        .extract_clip(
            camera_id,
            cfg.pre_event_secs,
            cfg.post_event_secs,
            event_pts,
            &ctx.recording_dir,
        )
        .await
    {
        Ok(path) => {
            tracing::info!(node_id = %node_id, path = %path.display(), "ExtractClip: done");
            NodeOutput::success(node_id).with_artifact(path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("extract_clip: {e}")),
    }
}
