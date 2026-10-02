use vms_core::{
    action::TriggerAlarmOutputConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Pulse an alarm output relay on a camera or NVR for a fixed duration.
///
/// Not implemented: ONVIF device control is not available.
/// Returns [`NodeOutput::failure`] with a diagnostic, so pipelines that
/// include alarm-output nodes fail the node instead of panicking.
pub async fn execute(
    node_id: NodeId,
    cfg: &TriggerAlarmOutputConfig,
    _input: &NodeInput,
    _ctx: &ActionContext,
) -> NodeOutput {
    tracing::debug!(
        node_id      = %node_id,
        output_id    = %cfg.output_id,
        duration_secs = cfg.duration_secs,
        "TriggerAlarmOutput: ONVIF not yet implemented"
    );
    NodeOutput::failure(
        node_id,
        "trigger_alarm_output: ONVIF device control is not yet implemented",
    )
}
