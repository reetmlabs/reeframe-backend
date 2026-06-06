use vms_core::{
    action::ActionConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::handlers::{delay, render_notification};

/// Resources available to action handlers at runtime.
///
/// Starts as a unit struct — fields are added in subsequent sub-steps as
/// handlers that need shared resources are implemented.
#[derive(Clone, Default)]
pub struct ActionContext;

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
        _ctx: &ActionContext,
    ) -> NodeOutput {
        match config {
            ActionConfig::Delay(cfg) => delay::execute(node_id, cfg).await,
            ActionConfig::RenderNotification(cfg) => {
                render_notification::execute(node_id, cfg, input)
            }
            other => NodeOutput::failure(
                node_id,
                format!("'{}' handler not yet implemented", other.action_type_str()),
            ),
        }
    }
}
