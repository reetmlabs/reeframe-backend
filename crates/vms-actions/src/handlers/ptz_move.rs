use vms_core::{
    action::PtzMoveConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Send a PTZ movement command to a camera.
///
/// **Not yet implemented.** ONVIF device control will be implemented in a future release.
/// Returns [`NodeOutput::failure`] with a clear diagnostic so pipelines that
/// include PTZ nodes degrade gracefully rather than panicking.
pub async fn execute(
    node_id: NodeId,
    cfg: &PtzMoveConfig,
    input: &NodeInput,
    _ctx: &ActionContext,
) -> NodeOutput {
    let camera_id = cfg.camera_id.or(input.trigger_ctx.camera_id);
    tracing::debug!(
        node_id   = %node_id,
        camera_id = ?camera_id,
        command   = ?cfg.command,
        "PtzMove: ONVIF not yet implemented"
    );
    NodeOutput::failure(
        node_id,
        "ptz_move: ONVIF device control is not yet implemented",
    )
}
