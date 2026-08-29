use vms_core::{node::NodeOutput, pipeline::NodeId};

/// No-op action: does nothing and always succeeds.
pub async fn execute(node_id: NodeId) -> NodeOutput {
    NodeOutput::success(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn skip_always_succeeds() {
        let id = Uuid::new_v4();
        let out = execute(id).await;
        assert!(out.success);
        assert_eq!(out.node_id, id);
    }
}
