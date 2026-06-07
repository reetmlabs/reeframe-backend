use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_db::entities::destination::{self, DestinationType};

use crate::adapters::local;

// -- TransportDispatcher --

/// Dispatches `Transport` pipeline nodes to the correct delivery adapter.
///
/// The executor calls [`dispatch`] after looking up and decrypting the
/// destination row.  Each adapter receives the destination config (credentials
/// already decrypted), the node-level template overrides, and the full
/// [`NodeInput`] (artifacts and rendered text from upstream nodes).
///
/// Adapters not yet implemented return [`NodeOutput::failure`] with a clear
/// diagnostic so pipelines degrade gracefully rather than panicking.
///
/// [`dispatch`]: TransportDispatcher::dispatch
pub struct TransportDispatcher;

impl TransportDispatcher {
    pub async fn dispatch(
        node_id: NodeId,
        dest: &destination::Model,
        transport_cfg: Option<&TransportConfig>,
        input: &NodeInput,
    ) -> NodeOutput {
        match dest.dest_type {
            DestinationType::Local => local::deliver(node_id, dest, transport_cfg, input).await,
            ref other => NodeOutput::failure(
                node_id,
                format!(
                    "{:?} transport is not yet implemented",
                    other
                ),
            ),
        }
    }
}
