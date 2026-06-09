use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination::{self, DestinationType};

use crate::adapters::{local, s3, sftp};

// -- TransportDispatcher --

/// Dispatches `Transport` pipeline nodes to the correct delivery adapter.
///
/// The executor calls [`dispatch`] after looking up and decrypting the
/// destination row.  Each adapter receives the destination config (credentials
/// already decrypted), the node-level template overrides, the full [`NodeInput`]
/// (artifacts and rendered text from upstream nodes), and an optional
/// `progress_tx` channel for mid-transfer progress reporting.
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
        progress_tx: Option<&UnboundedSender<TransferProgress>>,
    ) -> NodeOutput {
        match dest.dest_type {
            DestinationType::Local => local::deliver(node_id, dest, transport_cfg, input, progress_tx).await,
            DestinationType::S3    => s3::deliver(node_id, dest, transport_cfg, input, progress_tx).await,
            DestinationType::Sftp  => sftp::deliver(node_id, dest, transport_cfg, input, progress_tx).await,
            ref other => NodeOutput::failure(
                node_id,
                format!("{:?} transport is not yet implemented", other),
            ),
        }
    }
}
