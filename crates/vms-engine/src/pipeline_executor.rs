use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use evalexpr::{ContextWithMutableVariables, HashMapContext, Value as EvalValue};
use tokio::task::JoinSet;
use uuid::Uuid;
use vms_core::{
    pipeline::{CompiledPipeline, EdgeType, NodeId, NodeType, PipelineNode},
    TriggerContext, VmsError,
};
use vms_db::{
    entities::{pipeline_run::RunStatus, run_node_result::NodeResultStatus},
    PipelineRunRepo,
};

// -- Executor ------------------------------------------------------------------

/// Executes a compiled pipeline DAG for a given trigger context.
///
/// On each invocation [`execute`] inserts a `pipeline_runs` row, pre-creates
/// one `run_node_results` row per DAG node (all start as `Pending`), walks
/// the DAG concurrently using a [`JoinSet`], and finalises every record as
/// execution proceeds.
///
/// Node handlers are stubs in this step — they log and return `null`.
/// The real action library is wired in future.
///
/// [`execute`]: PipelineExecutor::execute
#[derive(Clone)]
pub struct PipelineExecutor {
    repo: PipelineRunRepo,
}

impl PipelineExecutor {
    pub fn new(repo: PipelineRunRepo) -> Arc<Self> {
        Arc::new(Self { repo })
    }

    /// Execute `pipeline` for the given trigger `ctx`.
    ///
    /// Writes the complete audit trail (run row + one node-result row per node)
    /// whether the run succeeds or fails.
    pub async fn execute(
        &self,
        mut ctx: TriggerContext,
        pipeline: Arc<CompiledPipeline>,
    ) -> Result<(), VmsError> {
        let trigger_json = serde_json::to_value(&ctx)?;

        // -- 1. Create run row (Running) ---------------------------------------
        let run = self
            .repo
            .create_run(pipeline.id, Some(ctx.trigger_id), trigger_json)
            .await?;
        let run_id = run.id;
        ctx.run_id = Some(run_id);

        tracing::info!(
            %run_id,
            pipeline_id   = %pipeline.id,
            pipeline_name = %pipeline.name,
            trigger_type  = ?ctx.trigger_type,
            "Pipeline run started",
        );

        // -- 2. Pre-create node-result rows (Pending) --------------------------
        let mut result_ids: HashMap<NodeId, Uuid> = HashMap::new();
        for &node_id in &pipeline.dag.topological_order {
            let row = self.repo.create_node_result(run_id, node_id).await?;
            result_ids.insert(node_id, row.id);
        }

        // -- 3. Walk the DAG ---------------------------------------------------
        let outcome = self
            .walk_dag(&pipeline.dag, &ctx, run_id, &result_ids)
            .await;

        // -- 4. Finalise run ---------------------------------------------------
        match &outcome {
            Ok(()) => {
                self.repo
                    .finish_run(run_id, RunStatus::Completed, None)
                    .await?;
                tracing::info!(%run_id, pipeline_id = %pipeline.id, "Pipeline run completed");
            }
            Err(e) => {
                self.repo
                    .finish_run(run_id, RunStatus::Failed, Some(e.to_string()))
                    .await?;
                tracing::error!(
                    %run_id,
                    pipeline_id = %pipeline.id,
                    error       = %e,
                    "Pipeline run failed",
                );
            }
        }

        outcome
    }

    // -- DAG walk --------------------------------------------------------------

    /// Concurrent DAG walk using a [`JoinSet`].
    ///
    /// Each node tracks two counters:
    ///
    /// * `remaining[n]` — parents not yet finalised (completed *or* skipped).
    ///   Starts at `parents[n].len()`.  Decremented by every parent regardless
    ///   of outcome.  When it reaches 0 the node enters the ready queue.
    ///
    /// * `active_parents[n]` — parents that *completed* (not skipped).
    ///   Incremented only when a parent finishes successfully.  When a node
    ///   becomes ready and `active_parents[n] == 0` (but it has parents) all
    ///   its ancestor paths were skipped — it is skipped too.
    ///
    /// For `Condition` nodes only the taken-branch child gets an
    /// `active_parents` increment; the other branch's child only gets the
    /// `remaining` decrement, so it will be skipped when it becomes ready.
    async fn walk_dag(
        &self,
        dag: &vms_core::pipeline::PipelineDag,
        ctx: &TriggerContext,
        _run_id: Uuid,
        result_ids: &HashMap<NodeId, Uuid>,
    ) -> Result<(), VmsError> {
        let mut remaining: HashMap<NodeId, usize> = dag
            .nodes
            .keys()
            .map(|&id| (id, dag.parents[&id].len()))
            .collect();

        let mut active_parents: HashMap<NodeId, usize> =
            dag.nodes.keys().map(|&id| (id, 0usize)).collect();

        let mut outputs: HashMap<NodeId, serde_json::Value> = HashMap::new();
        let mut ready: VecDeque<NodeId> = VecDeque::new();
        ready.push_back(dag.root_id);

        let mut join_set: JoinSet<(NodeId, Result<serde_json::Value, String>)> = JoinSet::new();

        loop {
            // Schedule every currently ready node.
            while let Some(node_id) = ready.pop_front() {
                let has_parents = !dag.parents[&node_id].is_empty();
                let should_skip = has_parents && active_parents[&node_id] == 0;
                let result_id = result_ids[&node_id];

                if should_skip {
                    self.repo
                        .finish_node_result(
                            result_id,
                            NodeResultStatus::Skipped,
                            serde_json::Value::Null,
                            None,
                        )
                        .await?;

                    // Propagate: skipped counts as "finalised" for children.
                    for &child in &dag.adjacency[&node_id] {
                        let r = remaining.get_mut(&child).unwrap();
                        *r -= 1;
                        if *r == 0 {
                            ready.push_back(child);
                        }
                        // active_parents[child] is NOT incremented.
                    }
                    continue;
                }

                self.repo.start_node_result(result_id).await?;

                let node = dag.nodes[&node_id].clone();
                let parent_outputs: Vec<serde_json::Value> = dag.parents[&node_id]
                    .iter()
                    .filter_map(|pid| outputs.get(pid))
                    .cloned()
                    .collect();
                let trigger_ctx = ctx.clone();

                join_set.spawn(async move {
                    let result = execute_node(&node, &parent_outputs, &trigger_ctx).await;
                    (node_id, result)
                });
            }

            let Some(join_result) = join_set.join_next().await else {
                break;
            };

            let (node_id, node_outcome) =
                join_result.map_err(|e| VmsError::Config(format!("node task panicked: {e}")))?;

            let result_id = result_ids[&node_id];
            let node = &dag.nodes[&node_id];

            match node_outcome {
                Ok(output) => {
                    // For Condition nodes the output is Bool — derive routing.
                    let branch_taken = if node.node_type == NodeType::Condition {
                        output.as_bool()
                    } else {
                        None
                    };

                    outputs.insert(node_id, output.clone());

                    // Save output -> DB row -> completed
                    self.repo
                        .finish_node_result(
                            result_id,
                            NodeResultStatus::Completed,
                            output,
                            None,
                        )
                        .await?;

                    for &child in &dag.adjacency[&node_id] {
                        // Check if child is active based on result of condition node
                        if child_is_active(
                            &node.node_type,
                            branch_taken,
                            dag.edge_types.get(&(node_id, child)),
                        ) {
                            *active_parents.get_mut(&child).unwrap() += 1;
                        }

                        let r = remaining.get_mut(&child).unwrap();
                        *r -= 1;
                        if *r == 0 {
                            ready.push_back(child);
                        }
                    }
                }

                Err(error_msg) => {
                    self.repo
                        .finish_node_result(
                            result_id,
                            NodeResultStatus::Failed,
                            serde_json::Value::Null,
                            Some(error_msg.clone()),
                        )
                        .await?;

                    join_set.abort_all();
                    return Err(VmsError::Config(format!(
                        "node {node_id} failed: {error_msg}"
                    )));
                }
            }
        }

        Ok(())
    }
}

// -- Child-activation helper ---------------------------------------------------

/// Returns `true` if the child on `edge_type` should be treated as actively
/// receiving output from `parent_type`.
///
/// Non-`Condition` parents activate all children unconditionally.
/// A `Condition` parent activates only the branch that matched its boolean
/// output (`branch_taken`).
pub(crate) fn child_is_active(
    parent_type: &NodeType,
    branch_taken: Option<bool>,
    edge_type: Option<&EdgeType>,
) -> bool {
    if *parent_type != NodeType::Condition {
        return true;
    }
    match (branch_taken, edge_type) {
        (Some(true), Some(EdgeType::TrueBranch)) => true,
        (Some(false), Some(EdgeType::FalseBranch)) => true,
        _ => false,
    }
}

// -- Node execution stubs ------------------------------------------------------

/// Execute a single pipeline node and return its output.
///
/// `TriggerRoot` serialises the trigger context as JSON so downstream nodes
/// can reference it.  `Condition` evaluates its `evalexpr` expression and
/// returns a `Bool`.  `Fork` passes the first parent output through.
/// `Action`, `DeviceControl`, and `Transport` are stubs until future.
pub(crate) async fn execute_node(
    node: &PipelineNode,
    parent_outputs: &[serde_json::Value],
    ctx: &TriggerContext,
) -> Result<serde_json::Value, String> {
    match node.node_type {
        NodeType::TriggerRoot => serde_json::to_value(ctx).map_err(|e| e.to_string()),

        NodeType::Condition => {
            let expr = node.condition_expr.as_deref().unwrap_or("false");
            let eval_ctx = build_condition_context(parent_outputs);
            evalexpr::eval_boolean_with_context(expr, &eval_ctx)
                .map(serde_json::Value::Bool)
                .map_err(|e| format!("condition eval failed: {e}"))
        }

        NodeType::Fork => Ok(parent_outputs
            .first()
            .cloned()
            .unwrap_or(serde_json::Value::Null)),

        NodeType::Action | NodeType::DeviceControl | NodeType::Transport => {
            tracing::debug!(
                node_id   = %node.id,
                node_type = ?node.node_type,
                label     = ?node.label,
                "Node executed (stub — handler added in future)",
            );
            Ok(serde_json::Value::Null)
        }
    }
}

/// Build an `evalexpr` context from the first parent's JSON output so that
/// condition expressions can reference its top-level fields by name.
pub(crate) fn build_condition_context(parent_outputs: &[serde_json::Value]) -> HashMapContext {
    let mut ctx = HashMapContext::new();
    if let Some(serde_json::Value::Object(map)) = parent_outputs.first() {
        for (k, v) in map {
            let val = match v {
                serde_json::Value::String(s) => EvalValue::String(s.clone()),
                serde_json::Value::Number(n) => {
                    let Some(f) = n.as_f64() else { continue };
                    EvalValue::Float(f)
                }
                serde_json::Value::Bool(b) => EvalValue::Boolean(*b),
                _ => continue,
            };
            ctx.set_value(k.clone(), val).ok();
        }
    }
    ctx
}

// -- Tests ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use vms_core::pipeline::{PipelineEdge, PipelineNode};

    fn node(pid: Uuid, nt: NodeType) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id: pid,
            node_type: nt,
            action_config: None,
            destination_id: None,
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        }
    }

    fn edge(pid: Uuid, from: Uuid, to: Uuid, et: EdgeType) -> PipelineEdge {
        PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: pid,
            from_node_id: from,
            to_node_id: to,
            edge_type: et,
        }
    }

    fn schedule_ctx(pid: Uuid) -> TriggerContext {
        TriggerContext::for_schedule(Uuid::new_v4(), pid)
    }

    // -- child_is_active -------------------------------------------------------

    #[test]
    fn non_condition_activates_all_children() {
        for nt in [NodeType::Fork, NodeType::Action, NodeType::TriggerRoot] {
            assert!(child_is_active(&nt, None, Some(&EdgeType::Default)));
        }
    }

    #[test]
    fn condition_activates_taken_branch_only() {
        assert!(child_is_active(
            &NodeType::Condition,
            Some(true),
            Some(&EdgeType::TrueBranch)
        ));
        assert!(!child_is_active(
            &NodeType::Condition,
            Some(true),
            Some(&EdgeType::FalseBranch)
        ));
        assert!(child_is_active(
            &NodeType::Condition,
            Some(false),
            Some(&EdgeType::FalseBranch)
        ));
        assert!(!child_is_active(
            &NodeType::Condition,
            Some(false),
            Some(&EdgeType::TrueBranch)
        ));
    }

    // -- build_condition_context -----------------------------------------------

    #[test]
    fn condition_context_exposes_parent_fields() {
        let ctx =
            build_condition_context(&[serde_json::json!({"confidence": 0.92, "label": "person"})]);
        let ok = evalexpr::eval_boolean_with_context(r#"confidence > 0.85"#, &ctx);
        assert_eq!(ok, Ok(true));
    }

    #[test]
    fn condition_context_empty_on_no_parent_output() {
        let ctx = build_condition_context(&[]);
        // Expression that would need a variable — should fail, not panic.
        assert!(evalexpr::eval_boolean_with_context("x > 1", &ctx).is_err());
    }

    // -- execute_node ----------------------------------------------------------

    #[tokio::test]
    async fn trigger_root_returns_context_json() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::TriggerRoot);
        let ctx = schedule_ctx(pid);
        let out = execute_node(&n, &[], &ctx).await.unwrap();
        assert!(out.is_object());
        assert_eq!(out["pipeline_id"], serde_json::json!(pid.to_string()));
    }

    #[tokio::test]
    async fn fork_passes_through_parent_output() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::Fork);
        let parent = serde_json::json!({"key": "value"});
        let ctx = schedule_ctx(pid);
        let out = execute_node(&n, &[parent.clone()], &ctx).await.unwrap();
        assert_eq!(out, parent);
    }

    #[tokio::test]
    async fn condition_evaluates_true() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some("score > 0.5".into());
        let parent = serde_json::json!({"score": 0.9});
        let ctx = schedule_ctx(pid);
        let out = execute_node(&n, &[parent], &ctx).await.unwrap();
        assert_eq!(out, serde_json::Value::Bool(true));
    }

    #[tokio::test]
    async fn condition_evaluates_false() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some("score > 0.5".into());
        let parent = serde_json::json!({"score": 0.1});
        let ctx = schedule_ctx(pid);
        let out = execute_node(&n, &[parent], &ctx).await.unwrap();
        assert_eq!(out, serde_json::Value::Bool(false));
    }

    #[tokio::test]
    async fn condition_bad_expression_is_err() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some(">>>".into());
        let ctx = schedule_ctx(pid);
        assert!(execute_node(&n, &[], &ctx).await.is_err());
    }

    #[tokio::test]
    async fn action_stub_returns_null() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::Action);
        let ctx = schedule_ctx(pid);
        let out = execute_node(&n, &[], &ctx).await.unwrap();
        assert_eq!(out, serde_json::Value::Null);
    }

    // -- DAG structural sanity (no DB — compile only) --------------------------

    #[test]
    fn dag_compile_root_to_transport() {
        use vms_core::pipeline::PipelineDag;
        let pid = Uuid::new_v4();
        let root = node(pid, NodeType::TriggerRoot);
        let mut transport = node(pid, NodeType::Transport);
        transport.destination_id = Some(Uuid::new_v4());
        let e = edge(pid, root.id, transport.id, EdgeType::Default);
        let root_id = root.id;
        let dag = PipelineDag::compile(vec![root, transport], vec![e]).unwrap();
        assert_eq!(dag.root_id, root_id);
        assert_eq!(dag.topological_order.len(), 2);
    }

    #[test]
    fn dag_compile_condition_branch() {
        use vms_core::pipeline::PipelineDag;
        let pid = Uuid::new_v4();
        let root = node(pid, NodeType::TriggerRoot);
        let mut cond = node(pid, NodeType::Condition);
        cond.condition_expr = Some("x > 0".into());
        let mut ta = node(pid, NodeType::Transport);
        ta.destination_id = Some(Uuid::new_v4());
        let mut fb = node(pid, NodeType::Transport);
        fb.destination_id = Some(Uuid::new_v4());

        let edges = vec![
            edge(pid, root.id, cond.id, EdgeType::Default),
            edge(pid, cond.id, ta.id, EdgeType::TrueBranch),
            edge(pid, cond.id, fb.id, EdgeType::FalseBranch),
        ];
        let dag = PipelineDag::compile(vec![root, cond, ta, fb], edges).unwrap();
        assert_eq!(dag.topological_order.len(), 4);
    }
}
