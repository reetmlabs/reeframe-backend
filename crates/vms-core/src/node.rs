//! Data types exchanged between nodes during pipeline execution.
//!
//! The Pipeline Executor walks the compiled DAG and calls each node's handler
//! with a [`NodeInput`].  The handler produces a [`NodeOutput`] that is stored
//! in the run's result map and forwarded to every downstream node that depends
//! on it.
//!
//! # Data flow
//!
//! ```text
//! TriggerContext
//!       │
//!       ▼
//! NodeOutput::from_trigger_context   ← seed output for the trigger-root node
//!       │
//!       ▼  (executor stores in run_node_results)
//! NodeInput { parent_outputs: [root_output], trigger_ctx }
//!       │
//!       ▼
//! [action handler]  →  NodeOutput { artifact_path, text, metadata, … }
//!       │
//!       └─► forwarded to child nodes as their NodeInput::parent_outputs
//! ```

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::pipeline::NodeId;
use crate::trigger::TriggerContext;

// ── Node output ───────────────────────────────────────────────────────────────

/// The result produced by a node after execution.
///
/// Stored in the executor's `run_node_results` map and forwarded to all
/// immediate child nodes as their [`NodeInput::parent_outputs`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOutput {
    /// The node that produced this output.
    pub node_id: NodeId,
    /// File artifact produced by this node (clip, snapshot, compressed /
    /// encrypted file, etc.).
    ///
    /// Downstream nodes access this via [`NodeInput::first_artifact`] or
    /// [`NodeInput::all_artifacts`].
    pub artifact_path: Option<PathBuf>,
    /// Rendered text produced by a `render_notification` node.
    ///
    /// Transport nodes use this as the message body when no explicit
    /// `message_template` is configured.
    pub text: Option<String>,
    /// Arbitrary metadata: delivery URLs, message IDs, detection results, etc.
    ///
    /// Stored as JSON so transport adapters can attach structured receipts.
    pub metadata: serde_json::Value,
    /// `true` if the node completed without errors; `false` otherwise.
    pub success: bool,
    /// Human-readable error message when `success == false`.
    pub error: Option<String>,
}

impl NodeOutput {
    /// Construct a minimal successful output with no artifact, text, or metadata.
    pub fn success(node_id: NodeId) -> Self {
        Self {
            node_id,
            artifact_path: None,
            text: None,
            metadata: serde_json::Value::Null,
            success: true,
            error: None,
        }
    }

    /// Construct a failed output carrying a human-readable error message.
    pub fn failure(node_id: NodeId, error: impl Into<String>) -> Self {
        Self {
            node_id,
            artifact_path: None,
            text: None,
            metadata: serde_json::Value::Null,
            success: false,
            error: Some(error.into()),
        }
    }

    /// Attach a file artifact path to this output (builder pattern).
    pub fn with_artifact(mut self, path: PathBuf) -> Self {
        self.artifact_path = Some(path);
        self
    }

    /// Attach rendered text to this output (builder pattern).
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// Attach arbitrary metadata to this output (builder pattern).
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Construct the seed output for the trigger-root node from the firing context.
    ///
    /// The metadata JSON captures the key context fields so downstream
    /// `evalexpr` condition nodes can inspect them without holding a direct
    /// reference to the [`TriggerContext`].
    pub fn from_trigger_context(ctx: &TriggerContext) -> Self {
        Self {
            node_id: ctx.trigger_id,
            artifact_path: None,
            text: None,
            metadata: serde_json::json!({
                "trigger_type": ctx.trigger_type,
                "camera_id": ctx.camera_id,
                "source_id": ctx.source_id,
                "fired_at": ctx.fired_at,
            }),
            success: true,
            error: None,
        }
    }
}

// ── Node input ────────────────────────────────────────────────────────────────

/// What a node receives when the executor schedules it for execution.
///
/// The executor assembles this from the stored outputs of all immediate parent
/// nodes and the immutable [`TriggerContext`] for the current run.
#[derive(Debug, Clone)]
pub struct NodeInput {
    /// Outputs from all immediate parent nodes, in topological order.
    ///
    /// For the first action node after the trigger root there is exactly one
    /// parent output (the root's seed output).  For nodes downstream of a
    /// [`Fork`] or join point there may be multiple.
    ///
    /// [`Fork`]: crate::pipeline::NodeType::Fork
    pub parent_outputs: Vec<NodeOutput>,
    /// The trigger context propagated unchanged from the pipeline root.
    pub trigger_ctx: TriggerContext,
}

impl NodeInput {
    /// First artifact path found among parent outputs, if any.
    ///
    /// Used by single-input action nodes (transcode, compress, encrypt, …)
    /// that operate on exactly one upstream file.
    pub fn first_artifact(&self) -> Option<&PathBuf> {
        self.parent_outputs.iter().find_map(|o| o.artifact_path.as_ref())
    }

    /// All artifact paths from all parent outputs.
    ///
    /// Used by the `merge_clips` action which expects multiple upstream clips.
    pub fn all_artifacts(&self) -> Vec<&PathBuf> {
        self.parent_outputs
            .iter()
            .filter_map(|o| o.artifact_path.as_ref())
            .collect()
    }

    /// First rendered text found among parent outputs.
    ///
    /// Used by transport nodes to retrieve the message body produced by an
    /// upstream `render_notification` node when no explicit `message_template`
    /// is configured on the transport.
    pub fn first_text(&self) -> Option<&str> {
        self.parent_outputs.iter().find_map(|o| o.text.as_deref())
    }
}
