use vms_core::{
    action::SetStreamQualityConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Switch a camera to a different streaming quality profile.
///
/// **Not yet implemented.** ONVIF device control will be implemented in a future release.
/// Returns [`NodeOutput::failure`] with a clear diagnostic so pipelines that
/// include stream-quality nodes degrade gracefully rather than panicking.
pub async fn execute(
    node_id: NodeId,
    cfg: &SetStreamQualityConfig,
    input: &NodeInput,
    _ctx: &ActionContext,
) -> NodeOutput {
    let camera_id = cfg.camera_id.or(input.trigger_ctx.camera_id);
    tracing::debug!(
        node_id   = %node_id,
        camera_id = ?camera_id,
        profile   = %cfg.profile,
        "SetStreamQuality: ONVIF not yet implemented"
    );
    NodeOutput::failure(
        node_id,
        "set_stream_quality: ONVIF device control is not yet implemented",
    )
}
