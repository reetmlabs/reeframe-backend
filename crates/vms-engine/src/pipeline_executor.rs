use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use evalexpr::{ContextWithMutableVariables, HashMapContext, Value as EvalValue};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use uuid::Uuid;
use vms_actions::dispatcher::{ActionContext, ActionDispatcher};
use vms_core::{
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::{CompiledPipeline, EdgeType, NodeId, NodeType, PipelineNode},
    TriggerContext, VmsError,
};
use vms_db::{
    entities::{pipeline_run::RunStatus, run_node_result::NodeResultStatus},
    ContactListRepo, DestinationRepo, PipelineRunRepo,
};
use vms_media::{MediaManager, RingBufferManager};
use vms_transports::TransportDispatcher;

use crate::metrics::Metrics;
use crate::payload_path::{register_json_path, register_manual_path};

// -- Executor --

/// Key type for the progress map: `(run_id, node_id)`.
pub type ProgressKey = (Uuid, NodeId);

/// The repos a Transport node reads when it sends.
#[derive(Clone)]
pub struct TransportRepos {
    pub destinations: DestinationRepo,
    pub contact_lists: ContactListRepo,
}

/// Executes a compiled pipeline DAG for a given trigger context.
///
/// On each invocation [`execute`] inserts a `pipeline_runs` row, pre-creates
/// one `run_node_results` row per DAG node (all start as `Pending`), walks
/// the DAG concurrently using a [`JoinSet`], and finalises every record as
/// execution proceeds.
///
/// Action and device-control nodes are dispatched through [`ActionDispatcher`],
/// Transport nodes through [`TransportDispatcher`].
///
/// [`execute`]: PipelineExecutor::execute
#[derive(Clone)]
pub struct PipelineExecutor {
    repo: PipelineRunRepo,
    transport_repos: TransportRepos,
    media: Arc<MediaManager>,
    ring_buffer: Arc<RingBufferManager>,
    recording_dir: PathBuf,
    encryption_key: Option<[u8; 32]>,
    /// Live transfer progress for all active Transport nodes.
    ///
    /// Keyed by `(run_id, node_id)`. An entry is updated on every progress
    /// report from the transport and removed when the transfer ends.
    progress_map: Arc<DashMap<ProgressKey, TransferProgress>>,
    metrics: Arc<Metrics>,
}

impl PipelineExecutor {
    pub fn new(
        repo: PipelineRunRepo,
        transport_repos: TransportRepos,
        media: Arc<MediaManager>,
        ring_buffer: Arc<RingBufferManager>,
        recording_dir: PathBuf,
        encryption_key: Option<[u8; 32]>,
        metrics: Arc<Metrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            repo,
            transport_repos,
            media,
            ring_buffer,
            recording_dir,
            encryption_key,
            progress_map: Arc::new(DashMap::new()),
            metrics,
        })
    }

    /// Returns the shared progress map for use by the API layer.
    pub fn progress_map(&self) -> Arc<DashMap<ProgressKey, TransferProgress>> {
        self.progress_map.clone()
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

        // -- 1. Create run row (Running) --
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

        // -- 2. Pre-create node-result rows (Pending) in one batched INSERT --
        let result_ids = self
            .repo
            .create_node_results_batch(run_id, &pipeline.dag.topological_order)
            .await?;

        // -- 3. Walk the DAG --
        let camera_rtsp_urls = self.media.rtsp_urls();
        let action_ctx = ActionContext {
            media: Some(self.media.clone()),
            ring_buffer: Some(self.ring_buffer.clone()),
            recording_dir: self.recording_dir.clone(),
            encryption_key: self.encryption_key,
            camera_rtsp_urls,
        };
        let outcome = self
            .walk_dag(
                &pipeline.dag,
                &ctx,
                run_id,
                &result_ids,
                &action_ctx,
                &self.transport_repos,
                &self.progress_map,
            )
            .await;

        // -- 4. Finalise run --
        // Every step logs at info, failures included, because this is the
        // execution timeline operators follow.
        match &outcome {
            Ok(()) => {
                self.repo
                    .finish_run(run_id, RunStatus::Completed, None)
                    .await?;
                self.metrics.record_pipeline_run("completed");
                tracing::info!(%run_id, pipeline_id = %pipeline.id, "Pipeline run completed");
            }
            Err(e) => {
                self.repo
                    .finish_run(run_id, RunStatus::Failed, Some(e.to_string()))
                    .await?;
                self.metrics.record_pipeline_run("failed");
                tracing::info!(
                    %run_id,
                    pipeline_id = %pipeline.id,
                    error       = %e,
                    "Pipeline run failed",
                );
            }
        }

        outcome
    }

    // -- DAG walk --

    /// Concurrent DAG walk using a [`JoinSet`].
    ///
    /// Each node tracks two counters:
    ///
    /// * `remaining[n]`: parents not yet finalised (completed *or* skipped).
    ///   Starts at `parents[n].len()` and is decremented by every parent
    ///   regardless of outcome. When it reaches 0 the node enters the ready queue.
    ///
    /// * `active_parents[n]`: parents that completed and activated this node.
    ///   When a node with parents becomes ready and `active_parents[n] == 0`,
    ///   every path into it was skipped, so it is skipped too.
    ///
    /// For `Condition` nodes only the taken-branch child gets an
    /// `active_parents` increment; the other branch's child only gets the
    /// `remaining` decrement, so it will be skipped when it becomes ready.
    #[allow(clippy::too_many_arguments)]
    async fn walk_dag(
        &self,
        dag: &vms_core::pipeline::PipelineDag,
        ctx: &TriggerContext,
        run_id: Uuid,
        result_ids: &HashMap<NodeId, Uuid>,
        action_ctx: &ActionContext,
        transport_repos: &TransportRepos,
        progress_map: &Arc<DashMap<ProgressKey, TransferProgress>>,
    ) -> Result<(), VmsError> {
        let mut remaining: HashMap<NodeId, usize> = dag
            .nodes
            .keys()
            .map(|&id| (id, dag.parents[&id].len()))
            .collect();

        let mut active_parents: HashMap<NodeId, usize> =
            dag.nodes.keys().map(|&id| (id, 0usize)).collect();

        let mut outputs: HashMap<NodeId, NodeOutput> = HashMap::new();
        let mut ready: VecDeque<NodeId> = VecDeque::new();
        ready.push_back(dag.root_id);

        let mut join_set: JoinSet<(NodeId, NodeOutput)> = JoinSet::new();

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
                    tracing::info!(
                        %run_id,
                        %node_id,
                        node_type = ?dag.nodes[&node_id].node_type,
                        "Node skipped",
                    );

                    // Propagate: skipped counts as "finalised" for children.
                    for &child in &dag.adjacency[&node_id] {
                        let r = remaining.get_mut(&child).unwrap();
                        *r -= 1;
                        if *r == 0 {
                            ready.push_back(child);
                        }
                    }
                    continue;
                }

                self.repo.start_node_result(result_id).await?;

                let node = dag.nodes[&node_id].clone();
                tracing::info!(%run_id, %node_id, node_type = ?node.node_type, "Node started");
                let parent_outputs: Vec<NodeOutput> = dag.parents[&node_id]
                    .iter()
                    .filter_map(|pid| outputs.get(pid))
                    .cloned()
                    .collect();
                let trigger_ctx = ctx.clone();
                let action_ctx_clone = action_ctx.clone();
                let transport_repos_clone = transport_repos.clone();
                let progress_map_clone = progress_map.clone();

                join_set.spawn(async move {
                    let output = execute_node(
                        &node,
                        &parent_outputs,
                        &trigger_ctx,
                        &action_ctx_clone,
                        &transport_repos_clone,
                        &progress_map_clone,
                    )
                    .await;
                    (node_id, output)
                });
            }

            let Some(join_result) = join_set.join_next().await else {
                break;
            };

            let (node_id, node_output) =
                join_result.map_err(|e| VmsError::Config(format!("node task panicked: {e}")))?;

            let result_id = result_ids[&node_id];
            let node = &dag.nodes[&node_id];
            let output_json = serde_json::to_value(&node_output).unwrap_or(serde_json::Value::Null);

            if !node_output.success {
                let error_msg = node_output
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".into());
                self.repo
                    .finish_node_result(
                        result_id,
                        NodeResultStatus::Failed,
                        output_json,
                        Some(error_msg.clone()),
                    )
                    .await?;
                tracing::info!(
                    %run_id,
                    %node_id,
                    node_type = ?node.node_type,
                    error = %error_msg,
                    "Node failed",
                );
                join_set.abort_all();
                return Err(VmsError::Config(format!(
                    "node {node_id} failed: {error_msg}"
                )));
            }

            // For Condition nodes the branch is encoded in metadata.
            let branch_taken = if node.node_type == NodeType::Condition {
                node_output
                    .metadata
                    .get("condition_result")
                    .and_then(|v| v.as_bool())
            } else {
                None
            };

            outputs.insert(node_id, node_output);

            self.repo
                .finish_node_result(result_id, NodeResultStatus::Completed, output_json, None)
                .await?;
            tracing::info!(%run_id, %node_id, node_type = ?node.node_type, "Node completed");

            for &child in &dag.adjacency[&node_id] {
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

        Ok(())
    }
}

// -- Child-activation helper --

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
    matches!(
        (branch_taken, edge_type),
        (Some(true), Some(EdgeType::TrueBranch)) | (Some(false), Some(EdgeType::FalseBranch))
    )
}

// -- Transport delivery retry --

/// Attempts before a Transport node's delivery gives up: 1 initial try
/// plus 2 retries.
const TRANSPORT_MAX_ATTEMPTS: u32 = 3;

/// Delay before the first retry; doubles after each subsequent failure.
const TRANSPORT_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// Calls `attempt` up to [`TRANSPORT_MAX_ATTEMPTS`] times with exponential
/// backoff, retrying blindly (no failure classification) since a Transport
/// node is always a DAG leaf with nothing else waiting on it. Returns the
/// first successful output, or the last failed one once attempts are
/// exhausted.
async fn dispatch_with_retry<F, Fut>(run_id: Uuid, node_id: NodeId, mut attempt: F) -> NodeOutput
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = NodeOutput>,
{
    let mut delay = TRANSPORT_RETRY_BACKOFF;
    for attempt_num in 1..=TRANSPORT_MAX_ATTEMPTS {
        let output = attempt().await;
        if output.success {
            tracing::info!(%run_id, %node_id, attempt = attempt_num, "Transport delivery succeeded");
            return output;
        }
        tracing::info!(
            %run_id, %node_id, attempt = attempt_num, max_attempts = TRANSPORT_MAX_ATTEMPTS,
            error = ?output.error,
            "Transport delivery attempt failed",
        );
        if attempt_num == TRANSPORT_MAX_ATTEMPTS {
            return output;
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    }
    unreachable!("the loop above always returns by the final attempt");
}

// -- Node execution --

/// Execute a single pipeline node and return its [`NodeOutput`].
///
/// `TriggerRoot` seeds the output with trigger context metadata, including
/// the triggering Event's top-level payload fields as `event.*` and a manual
/// trigger's parameters as `manual.*` (see [`flatten_scalar_fields`]) so a
/// downstream `Condition` can reference them.
/// `Condition` evaluates its `evalexpr` expression and stores the boolean
/// result in `metadata["condition_result"]`.
/// `Fork` passes the first parent output through with this node's ID.
/// `Action` and `DeviceControl` are dispatched through [`ActionDispatcher`].
/// `Transport` looks up the destination and, if the node has one, its contact
/// list's addresses, then dispatches to [`TransportDispatcher`], retrying on
/// failure via [`dispatch_with_retry`].
pub(crate) async fn execute_node(
    node: &PipelineNode,
    parent_outputs: &[NodeOutput],
    ctx: &TriggerContext,
    action_ctx: &ActionContext,
    transport_repos: &TransportRepos,
    progress_map: &Arc<DashMap<ProgressKey, TransferProgress>>,
) -> NodeOutput {
    match node.node_type {
        NodeType::TriggerRoot => {
            let mut metadata = serde_json::json!({
                "trigger_type": format!("{:?}", ctx.trigger_type),
                "camera_id":    ctx.camera_id,
                "source_id":    ctx.source_id,
                "pipeline_id":  ctx.pipeline_id.to_string(),
                "fired_at":     ctx.fired_at.to_rfc3339(),
            });
            if let Some(map) = metadata.as_object_mut() {
                map.extend(flatten_scalar_fields("event", ctx.event_payload.as_ref()));
                map.extend(flatten_scalar_fields("manual", ctx.manual_params.as_ref()));
            }
            NodeOutput::success(node.id).with_metadata(metadata)
        }

        NodeType::Condition => {
            let expr = node.condition_expr.as_deref().unwrap_or("false");
            let eval_ctx = build_condition_context(parent_outputs, ctx);
            match evalexpr::eval_boolean_with_context(expr, &eval_ctx) {
                // Forwards the parent's artifact_path/text (like Fork) so an
                // artifact can pass through a Condition.
                Ok(result) => {
                    let base = parent_outputs
                        .first()
                        .cloned()
                        .unwrap_or_else(|| NodeOutput::success(node.id));
                    NodeOutput {
                        node_id: node.id,
                        metadata: serde_json::json!({ "condition_result": result }),
                        success: true,
                        error: None,
                        ..base
                    }
                }
                Err(e) => NodeOutput::failure(node.id, format!("condition eval failed: {e}")),
            }
        }

        NodeType::Fork => {
            let base = parent_outputs
                .first()
                .cloned()
                .unwrap_or_else(|| NodeOutput::success(node.id));
            NodeOutput {
                node_id: node.id,
                ..base
            }
        }

        NodeType::Action | NodeType::DeviceControl => {
            let Some(config) = &node.action_config else {
                return NodeOutput::failure(
                    node.id,
                    format!("{:?} node has no action_config", node.node_type),
                );
            };
            let input = NodeInput {
                parent_outputs: parent_outputs.to_vec(),
                trigger_ctx: ctx.clone(),
            };
            ActionDispatcher::dispatch(node.id, config, &input, action_ctx).await
        }

        NodeType::Transport => {
            let Some(dest_id) = node.destination_id else {
                return NodeOutput::failure(node.id, "transport node has no destination_id");
            };
            let dest = match transport_repos.destinations.get_decrypted(dest_id).await {
                Ok(Some(d)) => d,
                Ok(None) => {
                    return NodeOutput::failure(
                        node.id,
                        format!("transport: destination {dest_id} not found"),
                    )
                }
                Err(e) => {
                    return NodeOutput::failure(
                        node.id,
                        format!("transport: destination lookup failed: {e}"),
                    )
                }
            };
            let contact_list = match node.contact_list_id {
                Some(list_id) => {
                    match contact_list_emails(&transport_repos.contact_lists, node.id, list_id)
                        .await
                    {
                        Ok(emails) => Some(emails),
                        Err(e) => return NodeOutput::failure(node.id, format!("transport: {e}")),
                    }
                }
                None => None,
            };
            let input = NodeInput {
                parent_outputs: parent_outputs.to_vec(),
                trigger_ctx: ctx.clone(),
            };

            // -- Wire up progress channel --
            let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<TransferProgress>();
            let run_id = ctx.run_id.unwrap_or_default();
            let node_id = node.id;
            let map = progress_map.clone();
            tokio::spawn(async move {
                while let Some(p) = progress_rx.recv().await {
                    map.insert((run_id, node_id), p);
                }
                map.remove(&(run_id, node_id));
            });

            dispatch_with_retry(run_id, node_id, || {
                TransportDispatcher::dispatch(
                    node.id,
                    &dest,
                    node.transport_config.as_ref(),
                    contact_list.as_deref(),
                    &input,
                    Some(&progress_tx),
                )
            })
            .await
        }
    }
}

/// The email addresses of `list_id`'s members, read at send time so list edits
/// apply right away. Members without an email are skipped.
async fn contact_list_emails(
    repo: &ContactListRepo,
    node_id: NodeId,
    list_id: Uuid,
) -> Result<Vec<String>, String> {
    let members = repo
        .list_members(list_id)
        .await
        .map_err(|e| format!("contact list lookup failed: {e}"))?;
    let mut emails = Vec::with_capacity(members.len());
    for contact in members {
        match contact.email.filter(|e| !e.trim().is_empty()) {
            Some(email) => emails.push(email),
            None => tracing::warn!(
                %node_id,
                contact_id = %contact.id,
                contact_list_id = %list_id,
                "Contact has no email, skipping",
            ),
        }
    }
    if emails.is_empty() {
        return Err(format!(
            "contact list {list_id} has no members with an email"
        ));
    }
    Ok(emails)
}

/// Flattens the top-level string/number/bool fields of a JSON object into
/// `<prefix>.<key>` entries for a downstream `Condition`'s evalexpr context.
fn flatten_scalar_fields(
    prefix: &str,
    payload: Option<&serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    let Some(serde_json::Value::Object(map)) = payload else {
        return out;
    };
    for (k, v) in map {
        if matches!(
            v,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
        ) {
            out.insert(format!("{prefix}.{k}"), v.clone());
        }
    }
    out
}

/// Build an `evalexpr` context from the first parent's metadata so that
/// condition expressions can reference its top-level fields by name. The
/// trigger's raw event payload and manual parameters are reachable through
/// `json_path`/`manual_path` (see `payload_path`), whatever the parent is.
pub(crate) fn build_condition_context(
    parent_outputs: &[NodeOutput],
    trigger: &TriggerContext,
) -> HashMapContext {
    let mut ctx = HashMapContext::new();
    register_json_path(&mut ctx, trigger.event_payload.as_ref());
    register_manual_path(&mut ctx, trigger.manual_params.as_ref());
    let Some(parent) = parent_outputs.first() else {
        return ctx;
    };
    let serde_json::Value::Object(map) = &parent.metadata else {
        return ctx;
    };
    for (k, v) in map {
        let val = match v {
            serde_json::Value::String(s) => EvalValue::String(s.clone()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    EvalValue::Int(i)
                } else {
                    EvalValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::Bool(b) => EvalValue::Boolean(*b),
            _ => continue,
        };
        ctx.set_value(k.clone(), val).ok();
    }
    ctx
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;
    use vms_core::pipeline::{PipelineEdge, PipelineNode};
    use vms_core::TriggerType;

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
            unresolved_reference: false,
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

    fn event_ctx(pid: Uuid, payload: serde_json::Value) -> TriggerContext {
        let mut ctx = TriggerContext::for_schedule(Uuid::new_v4(), pid);
        ctx.trigger_type = TriggerType::Event;
        ctx.event_payload = Some(payload);
        ctx
    }

    fn manual_ctx(pid: Uuid, params: serde_json::Value) -> TriggerContext {
        let mut ctx = TriggerContext::for_schedule(Uuid::new_v4(), pid);
        ctx.trigger_type = TriggerType::Manual;
        ctx.manual_params = Some(params);
        ctx
    }

    fn action_ctx() -> ActionContext {
        ActionContext {
            recording_dir: std::path::PathBuf::from("/tmp"),
            ..Default::default()
        }
    }

    async fn transport_repos() -> TransportRepos {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        TransportRepos {
            destinations: DestinationRepo::new(db.clone(), vms_db::Crypto::from_key([0u8; 32])),
            contact_lists: ContactListRepo::new(db),
        }
    }

    fn progress_map() -> Arc<DashMap<ProgressKey, TransferProgress>> {
        Arc::new(DashMap::new())
    }

    // -- child_is_active --

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

    // -- build_condition_context --

    #[test]
    fn condition_context_exposes_parent_metadata() {
        let parent = NodeOutput::success(Uuid::new_v4())
            .with_metadata(serde_json::json!({"confidence": 0.92, "label": "person"}));
        let ctx = build_condition_context(&[parent], &schedule_ctx(Uuid::new_v4()));
        let ok = evalexpr::eval_boolean_with_context("confidence > 0.85", &ctx);
        assert_eq!(ok, Ok(true));
    }

    #[test]
    fn condition_context_empty_on_no_parent() {
        let ctx = build_condition_context(&[], &schedule_ctx(Uuid::new_v4()));
        assert!(evalexpr::eval_boolean_with_context("x > 1", &ctx).is_err());
    }

    // -- execute_node --

    #[tokio::test]
    async fn trigger_root_embeds_pipeline_id() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::TriggerRoot);
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["pipeline_id"], pid.to_string());
    }

    #[tokio::test]
    async fn trigger_root_forwards_event_payload_as_event_fields() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::TriggerRoot);
        let ctx = event_ctx(
            pid,
            serde_json::json!({"confidence": 0.92, "label": "person"}),
        );
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["event.confidence"], 0.92);
        assert_eq!(out.metadata["event.label"], "person");
    }

    #[tokio::test]
    async fn trigger_root_skips_non_scalar_event_payload_fields() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::TriggerRoot);
        let ctx = event_ctx(
            pid,
            serde_json::json!({"nested": {"a": 1}, "list": [1, 2], "score": 1}),
        );
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert!(out.metadata.get("event.nested").is_none());
        assert!(out.metadata.get("event.list").is_none());
        assert_eq!(out.metadata["event.score"], 1);
    }

    #[tokio::test]
    async fn condition_after_event_trigger_can_reference_event_payload() {
        let pid = Uuid::new_v4();
        let root = node(pid, NodeType::TriggerRoot);
        let ctx = event_ctx(pid, serde_json::json!({"confidence": 0.92}));
        let dr = transport_repos().await;
        let root_out = execute_node(&root, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;

        let mut cond = node(pid, NodeType::Condition);
        cond.condition_expr = Some("event.confidence > 0.85".into());
        let cond_out = execute_node(
            &cond,
            &[root_out],
            &ctx,
            &action_ctx(),
            &dr,
            &progress_map(),
        )
        .await;
        assert!(cond_out.success);
        assert_eq!(cond_out.metadata["condition_result"], true);
    }

    #[tokio::test]
    async fn trigger_root_forwards_manual_params_as_manual_fields() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::TriggerRoot);
        let ctx = manual_ctx(pid, serde_json::json!({"action": "run", "count": 3}));
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["manual.action"], "run");
        assert_eq!(out.metadata["manual.count"], 3);
    }

    #[tokio::test]
    async fn condition_after_manual_trigger_can_reference_manual_params() {
        let pid = Uuid::new_v4();
        let root = node(pid, NodeType::TriggerRoot);
        let ctx = manual_ctx(pid, serde_json::json!({"action": "run"}));
        let dr = transport_repos().await;
        let root_out = execute_node(&root, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;

        let mut cond = node(pid, NodeType::Condition);
        cond.condition_expr = Some("manual.action == \"run\"".into());
        let cond_out = execute_node(
            &cond,
            &[root_out],
            &ctx,
            &action_ctx(),
            &dr,
            &progress_map(),
        )
        .await;
        assert!(cond_out.success);
        assert_eq!(cond_out.metadata["condition_result"], true);
    }

    #[tokio::test]
    async fn condition_forwards_parent_artifact() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some("true".into());
        let parent = NodeOutput::success(Uuid::new_v4())
            .with_artifact(std::path::PathBuf::from("/tmp/clip.mp4"));
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[parent], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(
            out.artifact_path,
            Some(std::path::PathBuf::from("/tmp/clip.mp4"))
        );
    }

    #[tokio::test]
    async fn condition_failure_does_not_forward_artifact() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some(">>>".into());
        let parent = NodeOutput::success(Uuid::new_v4())
            .with_artifact(std::path::PathBuf::from("/tmp/clip.mp4"));
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[parent], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(!out.success);
        assert_eq!(out.artifact_path, None);
    }

    #[tokio::test]
    async fn fork_passes_through_parent_metadata() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::Fork);
        let parent =
            NodeOutput::success(Uuid::new_v4()).with_metadata(serde_json::json!({"key": "value"}));
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[parent], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["key"], "value");
        assert_eq!(out.node_id, n.id);
    }

    #[tokio::test]
    async fn condition_evaluates_true() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some("score > 0.5".into());
        let parent =
            NodeOutput::success(Uuid::new_v4()).with_metadata(serde_json::json!({"score": 0.9}));
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[parent], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["condition_result"], true);
    }

    #[tokio::test]
    async fn condition_evaluates_false() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some("score > 0.5".into());
        let parent =
            NodeOutput::success(Uuid::new_v4()).with_metadata(serde_json::json!({"score": 0.1}));
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[parent], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(out.success);
        assert_eq!(out.metadata["condition_result"], false);
    }

    #[tokio::test]
    async fn condition_bad_expression_returns_failure() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Condition);
        n.condition_expr = Some(">>>".into());
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(!out.success);
        assert!(out
            .error
            .as_deref()
            .unwrap_or("")
            .contains("condition eval failed"));
    }

    #[tokio::test]
    async fn action_without_config_returns_failure() {
        let pid = Uuid::new_v4();
        let n = node(pid, NodeType::Action);
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(!out.success);
        assert!(out
            .error
            .as_deref()
            .unwrap_or("")
            .contains("no action_config"));
    }

    #[tokio::test]
    async fn transport_unknown_destination_returns_failure() {
        let pid = Uuid::new_v4();
        let mut n = node(pid, NodeType::Transport);
        n.destination_id = Some(Uuid::new_v4());
        let ctx = schedule_ctx(pid);
        let dr = transport_repos().await;
        let out = execute_node(&n, &[], &ctx, &action_ctx(), &dr, &progress_map()).await;
        assert!(!out.success);
    }

    // -- DAG structural sanity (no DB, compile only) --

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
    }

    // -- dispatch_with_retry --

    #[tokio::test(start_paused = true)]
    async fn returns_immediately_on_first_success() {
        let node_id = Uuid::new_v4();
        let calls = std::sync::atomic::AtomicU32::new(0);

        let output = dispatch_with_retry(Uuid::new_v4(), node_id, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { NodeOutput::success(node_id) }
        })
        .await;

        assert!(output.success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_after_transient_failures_within_the_attempt_cap() {
        let node_id = Uuid::new_v4();
        let calls = std::sync::atomic::AtomicU32::new(0);

        let output = dispatch_with_retry(Uuid::new_v4(), node_id, || {
            let attempt_num = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if attempt_num < 2 {
                    NodeOutput::failure(node_id, "transient")
                } else {
                    NodeOutput::success(node_id)
                }
            }
        })
        .await;

        assert!(output.success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_exhausting_the_attempt_cap() {
        let node_id = Uuid::new_v4();
        let calls = std::sync::atomic::AtomicU32::new(0);

        let output = dispatch_with_retry(Uuid::new_v4(), node_id, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { NodeOutput::failure(node_id, "permanent") }
        })
        .await;

        assert!(!output.success);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            TRANSPORT_MAX_ATTEMPTS
        );
    }

    // -- contact_list_emails --

    async fn contact_list_with(emails: &[Option<&str>]) -> (ContactListRepo, Uuid) {
        use sea_orm_migration::MigratorTrait;
        use vms_db::repos::{contact::CreateContact, contact_list::CreateContactList};

        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        vms_db::Migrator::up(&db, None).await.unwrap();
        let contacts = vms_db::ContactRepo::new(db.clone());
        let lists = ContactListRepo::new(db);
        let list = lists
            .create(CreateContactList {
                name: "on call".into(),
                description: None,
            })
            .await
            .unwrap();
        for (i, email) in emails.iter().enumerate() {
            let contact = contacts
                .create(CreateContact {
                    name: format!("contact {i}"),
                    email: email.map(str::to_string),
                    phone: None,
                    telegram_chat_id: None,
                    extra: serde_json::json!({}),
                })
                .await
                .unwrap();
            lists.add_member(list.id, contact.id).await.unwrap();
        }
        (lists, list.id)
    }

    #[tokio::test]
    async fn contact_list_emails_skips_members_without_an_email() {
        let (repo, list_id) = contact_list_with(&[
            Some("a@example.com"),
            None,
            Some("  "),
            Some("b@example.com"),
        ])
        .await;
        let mut emails = contact_list_emails(&repo, Uuid::new_v4(), list_id)
            .await
            .unwrap();
        emails.sort();
        assert_eq!(emails, ["a@example.com", "b@example.com"]);
    }

    #[tokio::test]
    async fn contact_list_emails_fails_when_no_member_has_an_email() {
        let (repo, list_id) = contact_list_with(&[None]).await;
        let err = contact_list_emails(&repo, Uuid::new_v4(), list_id)
            .await
            .unwrap_err();
        assert!(err.contains("has no members with an email"));
    }

    #[tokio::test]
    async fn contact_list_emails_fails_for_an_empty_list() {
        let (repo, list_id) = contact_list_with(&[]).await;
        assert!(contact_list_emails(&repo, Uuid::new_v4(), list_id)
            .await
            .is_err());
    }
}
