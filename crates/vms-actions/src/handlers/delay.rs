use tokio::time::{sleep, Duration};
use vms_core::{action::DelayConfig, node::NodeOutput, pipeline::NodeId};

/// Pause pipeline execution for [`DelayConfig::duration_secs`] seconds.
///
/// Returns a successful empty output once the sleep completes.
pub async fn execute(node_id: NodeId, cfg: &DelayConfig) -> NodeOutput {
    tracing::debug!(node_id = %node_id, secs = cfg.duration_secs, "Delay: sleeping");
    sleep(Duration::from_secs(cfg.duration_secs)).await;
    tracing::debug!(node_id = %node_id, "Delay: complete");
    NodeOutput::success(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn zero_delay_is_instant() {
        let id = Uuid::new_v4();
        let out = execute(id, &DelayConfig { duration_secs: 0 }).await;
        assert!(out.success);
        assert_eq!(out.node_id, id);
    }
}
