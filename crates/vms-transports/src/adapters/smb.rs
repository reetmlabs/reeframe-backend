use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

use super::local;

// -- Adapter --

/// Deliver the upstream artifact to an SMB/CIFS share.
///
/// The share must be mounted on the host before the pipeline runs.
/// This adapter writes to the mount point exactly like the local filesystem
/// adapter, so it contains no SMB protocol code.
///
/// Destination config (stored in `dest.config`):
/// ```json
/// { "path": "/mnt/nas/reeframe" }
/// ```
///
/// Mount the share beforehand, e.g.:
/// ```sh
/// mount.cifs //nas/share /mnt/nas -o username=user,password=pass
/// ```
///
/// The `path` field must point to the mount point (or a subdirectory of it).
/// All template variables and chunked-streaming behaviour are identical to the
/// local adapter.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    if dest.config.get("path").and_then(|v| v.as_str()).is_none() {
        return NodeOutput::failure(
            node_id,
            "smb transport: destination config missing \"path\" (mount point)",
        );
    }
    local::deliver(node_id, dest, transport_cfg, input, progress_tx).await
}
