//! Computes what's wrong with a pipeline's current definition — incomplete or
//! malformed node config, disconnected nodes, structural DAG violations, and
//! dangling camera references — independent of whether the pipeline can
//! currently be compiled and run. One reusable computation, called from
//! wherever a pipeline's validity needs checking, rather than reimplemented
//! per call site.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::{
    action::ActionConfig,
    pipeline::{
        EdgeType as CoreEdgeType, NodeType as CoreNodeType, PipelineDag, PipelineEdge,
        PipelineNode, PipelineTrigger,
    },
};

use super::pipeline::camera_id_from_action_config;

/// How serious a `ValidationIssue` is. `Error` should block a pipeline from
/// being enabled; `Warning` should not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationSeverity {
    Error,
    Warning,
}

/// What kind of problem a `ValidationIssue` describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationCategory {
    /// A required field for the node's type hasn't been set yet.
    ConfigIncomplete,
    /// The node's config doesn't match the shape its type allows at all.
    ConfigMalformed,
    /// The node exists but isn't reachable from the trigger root.
    Disconnected,
    /// A DAG-level structural rule is violated (root count, cycle, edge shape).
    StructuralViolation,
    /// An action node's `camera_id` points at a camera that no longer exists.
    DanglingCameraReference,
    /// A trigger's source was deleted or disabled.
    UnresolvedReference,
    /// A pass-through action node (Transcode/Compress/Encrypt/Watermark/
    /// MergeClips) has no Extract Clip or Snapshot node anywhere in its
    /// ancestor chain, on at least one path the executor could take — it
    /// will fail at runtime with "no upstream artifact".
    MissingArtifactAncestor,
    /// A `merge_clips` node can end up with only one upstream artifact on
    /// at least one path the executor could take — it passes that single
    /// clip through unchanged instead of merging anything.
    MergeSingleSource,
}

impl ValidationCategory {
    fn severity(self) -> ValidationSeverity {
        match self {
            ValidationCategory::Disconnected | ValidationCategory::MergeSingleSource => {
                ValidationSeverity::Warning
            }
            _ => ValidationSeverity::Error,
        }
    }
}

/// One problem found with a pipeline, naming which node (if any) it's about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub category: ValidationCategory,
    pub severity: ValidationSeverity,
    pub node_id: Option<Uuid>,
    pub message: String,
}

impl ValidationIssue {
    fn new(
        category: ValidationCategory,
        node_id: Option<Uuid>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            severity: category.severity(),
            category,
            node_id,
            message: message.into(),
        }
    }
}

/// A node whose config is the right shape for its type but still missing a
/// value it'll eventually need. Not a shape violation (see
/// `check_config_shape`) — this is the legitimate work-in-progress state
/// that `validate_create_shape`/`validate_update_shape` deliberately stopped
/// rejecting.
pub fn check_config_completeness(nodes: &[PipelineNode]) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for node in nodes {
        match node.node_type {
            CoreNodeType::Action | CoreNodeType::DeviceControl => {
                if node.action_config.is_none() {
                    issues.push(ValidationIssue::new(
                        ValidationCategory::ConfigIncomplete,
                        Some(node.id),
                        format!("{} node has no action_config", node.node_type.as_str()),
                    ));
                }
            }
            CoreNodeType::Transport => {
                if node.destination_id.is_none() {
                    issues.push(ValidationIssue::new(
                        ValidationCategory::ConfigIncomplete,
                        Some(node.id),
                        "transport node has no destination_id",
                    ));
                }
            }
            CoreNodeType::Condition => {
                if node
                    .condition_expr
                    .as_deref()
                    .is_none_or(|e| e.trim().is_empty())
                {
                    issues.push(ValidationIssue::new(
                        ValidationCategory::ConfigIncomplete,
                        Some(node.id),
                        "condition node has no condition_expr",
                    ));
                }
            }
            CoreNodeType::TriggerRoot | CoreNodeType::Fork => {}
        }
    }
    issues
}

/// A node carrying a config that's the wrong shape for its type entirely
/// (e.g. a Condition node with `action_config` set). Mirrors
/// `validate_create_shape`'s wrong-shape checks, but collects every
/// violation instead of failing on the first one, and runs against nodes
/// already persisted — so it also catches a bad record that predates that
/// check existing at all.
pub fn check_config_shape(nodes: &[PipelineNode]) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for node in nodes {
        let wrong_shape = match node.node_type {
            CoreNodeType::Action | CoreNodeType::DeviceControl => {
                node.transport_config.is_some()
                    || node.condition_expr.is_some()
                    || node.destination_id.is_some()
            }
            CoreNodeType::Transport => {
                node.action_config.is_some() || node.condition_expr.is_some()
            }
            CoreNodeType::Condition => {
                node.action_config.is_some()
                    || node.transport_config.is_some()
                    || node.destination_id.is_some()
            }
            CoreNodeType::TriggerRoot | CoreNodeType::Fork => {
                node.action_config.is_some()
                    || node.transport_config.is_some()
                    || node.condition_expr.is_some()
                    || node.destination_id.is_some()
            }
        };
        if wrong_shape {
            issues.push(ValidationIssue::new(
                ValidationCategory::ConfigMalformed,
                Some(node.id),
                format!(
                    "{} node carries a config that doesn't match its type",
                    node.node_type.as_str()
                ),
            ));
        }
    }
    issues
}

/// IDs reachable from the pipeline's trigger root by walking outgoing
/// edges, including the root itself. `None` if there isn't exactly one
/// `trigger_root` node — reachability isn't well-defined in that case, and
/// that's `check_structural_violations`' problem to report instead.
fn reachable_from_root(nodes: &[PipelineNode], edges: &[PipelineEdge]) -> Option<HashSet<Uuid>> {
    let roots: Vec<&PipelineNode> = nodes
        .iter()
        .filter(|n| n.node_type == CoreNodeType::TriggerRoot)
        .collect();
    let root = match roots.len() {
        1 => roots[0],
        _ => return None,
    };

    let mut adjacency: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for edge in edges {
        adjacency
            .entry(edge.from_node_id)
            .or_default()
            .push(edge.to_node_id);
    }

    let mut reached: HashSet<Uuid> = HashSet::new();
    reached.insert(root.id);
    let mut queue: VecDeque<Uuid> = VecDeque::from([root.id]);
    while let Some(id) = queue.pop_front() {
        for &child in adjacency.get(&id).into_iter().flatten() {
            if reached.insert(child) {
                queue.push_back(child);
            }
        }
    }
    Some(reached)
}

/// DAG-level structural rules, reusing `PipelineDag::compile` rather than
/// re-deriving them. Checked only against the subgraph reachable from the
/// trigger root, so an unrelated disconnected node doesn't also masquerade
/// as a "wrong root count" or "cycle" here.
pub fn check_structural_violations(
    nodes: &[PipelineNode],
    edges: &[PipelineEdge],
) -> Vec<ValidationIssue> {
    let (nodes, edges) = match reachable_from_root(nodes, edges) {
        Some(reached) => (
            nodes
                .iter()
                .filter(|n| reached.contains(&n.id))
                .cloned()
                .collect(),
            edges
                .iter()
                .filter(|e| reached.contains(&e.from_node_id) && reached.contains(&e.to_node_id))
                .cloned()
                .collect(),
        ),
        // Zero or multiple trigger_root nodes — there's no well-defined
        // reachable subgraph, so let compile() report that directly.
        None => (nodes.to_vec(), edges.to_vec()),
    };

    match PipelineDag::compile(nodes, edges) {
        Ok(_) => Vec::new(),
        Err(e) => vec![ValidationIssue::new(
            ValidationCategory::StructuralViolation,
            None,
            e.to_string(),
        )],
    }
}

/// A node that exists in the pipeline but can't be reached by walking
/// outgoing edges from the trigger root. Deliberately a dedicated check
/// rather than a side effect of `PipelineDag::compile`: today, compiling a
/// pipeline with a disconnected node only fails by accident, mislabeled as
/// either "wrong number of root nodes" (if the disconnected piece is
/// acyclic — it contributes its own parentless node) or "pipeline contains
/// a cycle" (if it isn't) — neither message names the actual problem.
pub fn check_disconnected(nodes: &[PipelineNode], edges: &[PipelineEdge]) -> Vec<ValidationIssue> {
    let Some(reached) = reachable_from_root(nodes, edges) else {
        return Vec::new();
    };

    nodes
        .iter()
        .filter(|n| !reached.contains(&n.id))
        .map(|n| {
            ValidationIssue::new(
                ValidationCategory::Disconnected,
                Some(n.id),
                format!(
                    "{} node is not reachable from the trigger root",
                    n.node_type.as_str()
                ),
            )
        })
        .collect()
}

/// An action node's `camera_id` (extract-clip, snapshot, PTZ move,
/// start/stop recording, set-stream-quality) pointing at a camera that no
/// longer exists. The only reference type that can go dangling with no
/// protection at any other layer — it's a plain UUID inside a JSON config
/// blob, unlike `destination_id`/trigger-level `camera_id` (FK-guarded) or
/// `contact_list_id` (cleared automatically on delete).
///
/// Takes the set of currently-existing camera IDs rather than a DB handle,
/// so it stays a plain, synchronously-testable function — fetching that set
/// is the caller's job.
pub fn check_dangling_camera_references(
    nodes: &[PipelineNode],
    existing_camera_ids: &HashSet<Uuid>,
) -> Vec<ValidationIssue> {
    nodes
        .iter()
        .filter_map(|node| {
            let camera_id = camera_id_from_action_config(node.action_config.as_ref()?)?;
            if existing_camera_ids.contains(&camera_id) {
                return None;
            }
            Some(ValidationIssue::new(
                ValidationCategory::DanglingCameraReference,
                Some(node.id),
                format!("references camera {camera_id}, which no longer exists"),
            ))
        })
        .collect()
}

/// A trigger whose source was deleted or disabled, per its own persisted
/// `unresolved_reference` flag — set and cleared by source lifecycle events
/// (`PipelineRepo::unlink_deleted_source`/`mark_source_disabled`/
/// `clear_source_unresolved`), not re-derived here.
pub fn check_unresolved_trigger_references(triggers: &[PipelineTrigger]) -> Vec<ValidationIssue> {
    triggers
        .iter()
        .filter(|t| t.unresolved_reference)
        .map(|t| {
            ValidationIssue::new(
                ValidationCategory::UnresolvedReference,
                None,
                format!(
                    "trigger {} references a source that no longer exists or is disabled",
                    t.id
                ),
            )
        })
        .collect()
}

/// A node whose destination was deleted or disabled, per its own persisted
/// `unresolved_reference` flag — set and cleared by destination lifecycle
/// events (`PipelineRepo::unlink_deleted_destination`/
/// `mark_destination_disabled`/`clear_destination_unresolved`), not
/// re-derived here.
pub fn check_unresolved_node_references(nodes: &[PipelineNode]) -> Vec<ValidationIssue> {
    nodes
        .iter()
        .filter(|n| n.unresolved_reference)
        .map(|n| {
            ValidationIssue::new(
                ValidationCategory::UnresolvedReference,
                Some(n.id),
                "node references a destination that no longer exists or is disabled",
            )
        })
        .collect()
}

// -- Artifact ancestor lineage --

/// Whether `cfg` only re-processes an artifact a prior node already
/// produced, rather than originating one itself.
fn is_artifact_pass_through(cfg: &ActionConfig) -> bool {
    matches!(
        cfg,
        ActionConfig::Transcode(_)
            | ActionConfig::Compress(_)
            | ActionConfig::Encrypt(_)
            | ActionConfig::Watermark(_)
            | ActionConfig::MergeClips(_)
    )
}

/// Whether `cfg` originates a new artifact from a camera, rather than
/// requiring one already produced upstream.
fn originates_artifact(cfg: &ActionConfig) -> bool {
    matches!(
        cfg,
        ActionConfig::ExtractClip(_) | ActionConfig::Snapshot(_)
    )
}

/// All ancestors of `target` (nodes with a path to `target`), not including
/// `target` itself. Used only to scope which `Condition` nodes' branch
/// outcomes can possibly affect `target`.
fn ancestors_of(target: Uuid, parents: &HashMap<Uuid, Vec<Uuid>>) -> HashSet<Uuid> {
    let mut seen = HashSet::new();
    let mut queue: VecDeque<Uuid> = parents.get(&target).cloned().unwrap_or_default().into();
    while let Some(id) = queue.pop_front() {
        if seen.insert(id) {
            for &p in parents.get(&id).into_iter().flatten() {
                queue.push_back(p);
            }
        }
    }
    seen
}

/// Topological order of `relevant`, via Kahn's algorithm restricted to
/// edges between members of `relevant`. If `relevant` contains a cycle
/// (already reported separately by `check_structural_violations`), the
/// cyclic nodes are simply left out of the order rather than looping
/// forever — callers that only process nodes appearing in the order treat
/// them the same as unreached.
fn topo_sort(
    relevant: &HashSet<Uuid>,
    children: &HashMap<Uuid, Vec<(Uuid, CoreEdgeType)>>,
    parents: &HashMap<Uuid, Vec<Uuid>>,
) -> Vec<Uuid> {
    let mut indegree: HashMap<Uuid, usize> = relevant
        .iter()
        .map(|&id| {
            let deg = parents
                .get(&id)
                .map(|ps| ps.iter().filter(|p| relevant.contains(p)).count())
                .unwrap_or(0);
            (id, deg)
        })
        .collect();

    let mut queue: VecDeque<Uuid> = indegree
        .iter()
        .filter(|&(_, &d)| d == 0)
        .map(|(&id, _)| id)
        .collect();
    let mut order = Vec::with_capacity(relevant.len());

    while let Some(id) = queue.pop_front() {
        order.push(id);
        for &(child, _) in children.get(&id).into_iter().flatten() {
            if !relevant.contains(&child) {
                continue;
            }
            if let Some(d) = indegree.get_mut(&child) {
                *d -= 1;
                if *d == 0 {
                    queue.push_back(child);
                }
            }
        }
    }
    order
}

/// Whether `target` was reached at all, and — if so — whether it received
/// an artifact and from how many distinct direct parents, for one fixed
/// combination of upstream `Condition` outcomes.
struct AssignmentOutcome {
    reached: bool,
    has_artifact: bool,
    artifact_parent_count: usize,
}

/// For every combination of true/false outcomes of the `Condition` nodes
/// that are `target`'s ancestors, walks forward from the root exactly as
/// the executor would — a `Condition` only activates the branch matching
/// that combination, every other node type activates all of its reached
/// children — and reports, per combination, whether `target` was reached
/// and whether it ended up with an artifact.
///
/// A pipeline can branch and reconverge through `Condition` nodes, so a
/// node's parents aren't always safe to combine with a simple OR: two
/// parents that are mutually-exclusive alternatives of the same upstream
/// `Condition` only ever have one of them active on any given run. Real
/// pipelines only realistically have a handful of `Condition` ancestors for
/// any one node, so enumerating every combination directly stays cheap
/// while still being exact — checking only *some* combination would still
/// let a real "no upstream artifact" runtime failure through on whichever
/// combination it missed.
fn simulate_artifact_reachability(
    target: Uuid,
    nodes_by_id: &HashMap<Uuid, &PipelineNode>,
    children: &HashMap<Uuid, Vec<(Uuid, CoreEdgeType)>>,
    parents: &HashMap<Uuid, Vec<Uuid>>,
    root_id: Uuid,
) -> Vec<AssignmentOutcome> {
    let ancestors = ancestors_of(target, parents);
    let relevant: HashSet<Uuid> = ancestors.iter().copied().chain([target]).collect();
    let topo_order = topo_sort(&relevant, children, parents);

    let condition_ancestors: Vec<Uuid> = ancestors
        .iter()
        .filter(|id| nodes_by_id.get(id).map(|n| &n.node_type) == Some(&CoreNodeType::Condition))
        .copied()
        .collect();

    let combos = 1usize << condition_ancestors.len();
    let mut outcomes = Vec::with_capacity(combos);

    for combo in 0..combos {
        let assignment: HashMap<Uuid, bool> = condition_ancestors
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, (combo >> i) & 1 == 1))
            .collect();

        let mut reached: HashSet<Uuid> = HashSet::from([root_id]);
        let mut has_artifact: HashMap<Uuid, bool> = HashMap::from([(root_id, false)]);
        let mut artifact_parent_count: HashMap<Uuid, usize> = HashMap::new();

        for &id in &topo_order {
            if !reached.contains(&id) {
                continue;
            }
            let Some(node) = nodes_by_id.get(&id) else {
                continue;
            };
            let self_originates = node.action_config.as_ref().is_some_and(originates_artifact);
            let self_has_artifact =
                self_originates || has_artifact.get(&id).copied().unwrap_or(false);

            for (child, edge_type) in children.get(&id).into_iter().flatten() {
                let &child = child;
                if !relevant.contains(&child) {
                    continue;
                }
                let edge_active = match node.node_type {
                    CoreNodeType::Condition => {
                        let took_true_branch = assignment.get(&id).copied().unwrap_or(true);
                        match edge_type {
                            CoreEdgeType::TrueBranch => took_true_branch,
                            CoreEdgeType::FalseBranch => !took_true_branch,
                            CoreEdgeType::Default => true,
                        }
                    }
                    _ => true,
                };
                if !edge_active {
                    continue;
                }
                reached.insert(child);
                let entry = has_artifact.entry(child).or_insert(false);
                *entry = *entry || self_has_artifact;
                if self_has_artifact {
                    *artifact_parent_count.entry(child).or_insert(0) += 1;
                }
            }
        }

        outcomes.push(AssignmentOutcome {
            reached: reached.contains(&target),
            has_artifact: has_artifact.get(&target).copied().unwrap_or(false),
            artifact_parent_count: artifact_parent_count.get(&target).copied().unwrap_or(0),
        });
    }
    outcomes
}

/// A `Transcode`/`Compress`/`Encrypt`/`Watermark`/`MergeClips` node with no
/// Extract Clip or Snapshot node anywhere in its ancestor chain, on at
/// least one path the executor could take (`MissingArtifactAncestor`,
/// error) — and, specifically for `MergeClips`, a node that can end up with
/// only one upstream artifact on at least one such path
/// (`MergeSingleSource`, warning): it merges nothing, just passes that
/// clip through.
pub fn check_artifact_lineage(
    nodes: &[PipelineNode],
    edges: &[PipelineEdge],
) -> Vec<ValidationIssue> {
    let roots: Vec<&PipelineNode> = nodes
        .iter()
        .filter(|n| n.node_type == CoreNodeType::TriggerRoot)
        .collect();
    let [root] = roots[..] else {
        // Zero or multiple trigger_root nodes — no well-defined lineage;
        // check_structural_violations reports that instead.
        return Vec::new();
    };

    let nodes_by_id: HashMap<Uuid, &PipelineNode> = nodes.iter().map(|n| (n.id, n)).collect();
    let mut children: HashMap<Uuid, Vec<(Uuid, CoreEdgeType)>> = HashMap::new();
    let mut parents: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for edge in edges {
        children
            .entry(edge.from_node_id)
            .or_default()
            .push((edge.to_node_id, edge.edge_type.clone()));
        parents
            .entry(edge.to_node_id)
            .or_default()
            .push(edge.from_node_id);
    }

    let mut issues = Vec::new();
    for node in nodes {
        let Some(cfg) = node.action_config.as_ref() else {
            continue;
        };
        if !is_artifact_pass_through(cfg) {
            continue;
        }

        let outcomes =
            simulate_artifact_reachability(node.id, &nodes_by_id, &children, &parents, root.id);
        let reached: Vec<&AssignmentOutcome> = outcomes.iter().filter(|o| o.reached).collect();
        if reached.is_empty() {
            // Never actually reachable — check_disconnected's problem, not ours.
            continue;
        }

        if reached.iter().any(|o| !o.has_artifact) {
            issues.push(ValidationIssue::new(
                ValidationCategory::MissingArtifactAncestor,
                Some(node.id),
                format!(
                    "{} node has no Extract Clip or Snapshot node in its ancestor chain on at \
                     least one reachable path — it will fail at runtime with \"no upstream \
                     artifact\"",
                    node.node_type.as_str()
                ),
            ));
        } else if matches!(cfg, ActionConfig::MergeClips(_))
            && reached.iter().any(|o| o.artifact_parent_count < 2)
        {
            issues.push(ValidationIssue::new(
                ValidationCategory::MergeSingleSource,
                Some(node.id),
                "merge_clips node can end up with only one upstream artifact on at least one \
                 reachable path — it will silently pass that clip through unchanged instead of \
                 merging anything",
            ));
        }
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use vms_core::pipeline::EdgeType as CoreEdgeType;

    fn base_node(node_type: CoreNodeType) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            node_type,
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

    #[test]
    fn incomplete_transport_node_is_flagged() {
        let node = base_node(CoreNodeType::Transport);
        let issues = check_config_completeness(std::slice::from_ref(&node));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::ConfigIncomplete);
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert_eq!(issues[0].node_id, Some(node.id));
    }

    #[test]
    fn incomplete_condition_node_is_flagged_for_missing_or_blank_expr() {
        let missing = base_node(CoreNodeType::Condition);
        assert_eq!(check_config_completeness(&[missing]).len(), 1);

        let mut blank = base_node(CoreNodeType::Condition);
        blank.condition_expr = Some("   ".into());
        assert_eq!(check_config_completeness(&[blank]).len(), 1);
    }

    #[test]
    fn complete_nodes_are_not_flagged() {
        let mut transport = base_node(CoreNodeType::Transport);
        transport.destination_id = Some(Uuid::new_v4());
        let mut condition = base_node(CoreNodeType::Condition);
        condition.condition_expr = Some("x > 1".into());
        let root = base_node(CoreNodeType::TriggerRoot);

        assert!(check_config_completeness(&[transport, condition, root]).is_empty());
    }

    #[test]
    fn condition_node_with_action_config_is_malformed() {
        use vms_core::action::ActionConfig;

        let mut node = base_node(CoreNodeType::Condition);
        node.condition_expr = Some("x > 1".into());
        node.action_config = Some(ActionConfig::Skip);

        let issues = check_config_shape(&[node.clone()]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::ConfigMalformed);
        assert_eq!(issues[0].node_id, Some(node.id));
    }

    #[test]
    fn well_shaped_but_incomplete_node_is_not_malformed() {
        let node = base_node(CoreNodeType::Transport);
        assert!(check_config_shape(&[node]).is_empty());
    }

    fn edge(from: Uuid, to: Uuid, edge_type: CoreEdgeType) -> PipelineEdge {
        PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            from_node_id: from,
            to_node_id: to,
            edge_type,
        }
    }

    #[test]
    fn well_formed_pipeline_has_no_structural_violations() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let mut action = base_node(CoreNodeType::Action);
        action.action_config = Some(vms_core::action::ActionConfig::Skip);

        let edges = [edge(root.id, action.id, CoreEdgeType::Default)];
        assert!(check_structural_violations(&[root, action], &edges).is_empty());
    }

    #[test]
    fn missing_trigger_root_is_a_structural_violation() {
        let mut action = base_node(CoreNodeType::Action);
        action.action_config = Some(vms_core::action::ActionConfig::Skip);

        let issues = check_structural_violations(&[action], &[]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::StructuralViolation);
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert_eq!(issues[0].node_id, None);
    }

    #[test]
    fn a_cycle_is_a_structural_violation() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let a = base_node(CoreNodeType::Fork);
        let b = base_node(CoreNodeType::Fork);

        let edges = [
            edge(root.id, a.id, CoreEdgeType::Default),
            edge(a.id, b.id, CoreEdgeType::Default),
            edge(b.id, a.id, CoreEdgeType::Default),
        ];
        let issues = check_structural_violations(&[root, a, b], &edges);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::StructuralViolation);
    }

    #[test]
    fn a_transport_node_with_an_outgoing_edge_is_a_structural_violation() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let mut transport = base_node(CoreNodeType::Transport);
        transport.destination_id = Some(Uuid::new_v4());
        let leaf = base_node(CoreNodeType::Fork);

        let edges = [
            edge(root.id, transport.id, CoreEdgeType::Default),
            edge(transport.id, leaf.id, CoreEdgeType::Default),
        ];
        let issues = check_structural_violations(&[root, transport, leaf], &edges);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::StructuralViolation);
    }

    #[test]
    fn fully_connected_pipeline_has_no_disconnected_nodes() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let action = base_node(CoreNodeType::Action);
        let edges = [edge(root.id, action.id, CoreEdgeType::Default)];
        assert!(check_disconnected(&[root, action], &edges).is_empty());
    }

    #[test]
    fn an_orphan_node_is_flagged_as_disconnected() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let action = base_node(CoreNodeType::Action);
        let orphan = base_node(CoreNodeType::Fork);
        let edges = [edge(root.id, action.id, CoreEdgeType::Default)];

        let issues = check_disconnected(&[root, action, orphan.clone()], &edges);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::Disconnected);
        assert_eq!(issues[0].severity, ValidationSeverity::Warning);
        assert_eq!(issues[0].node_id, Some(orphan.id));
    }

    #[test]
    fn an_orphan_cycle_is_flagged_as_disconnected_not_just_a_cycle() {
        // Two nodes that only reference each other, wired to nothing else —
        // disconnected *and* cyclic. Both facts are true and worth reporting;
        // this check specifically must not stay silent about disconnection
        // just because a cycle also exists.
        let root = base_node(CoreNodeType::TriggerRoot);
        let a = base_node(CoreNodeType::Fork);
        let b = base_node(CoreNodeType::Fork);
        let edges = [
            edge(a.id, b.id, CoreEdgeType::Default),
            edge(b.id, a.id, CoreEdgeType::Default),
        ];

        let issues = check_disconnected(&[root, a.clone(), b.clone()], &edges);
        let flagged: HashSet<Uuid> = issues.iter().filter_map(|i| i.node_id).collect();
        assert_eq!(flagged, HashSet::from([a.id, b.id]));
    }

    #[test]
    fn skips_entirely_without_exactly_one_trigger_root() {
        let a = base_node(CoreNodeType::Fork);
        let b = base_node(CoreNodeType::Fork);
        assert!(check_disconnected(&[a, b], &[]).is_empty());

        let root1 = base_node(CoreNodeType::TriggerRoot);
        let root2 = base_node(CoreNodeType::TriggerRoot);
        assert!(check_disconnected(&[root1, root2], &[]).is_empty());
    }

    fn snapshot_config(camera_id: Option<Uuid>) -> vms_core::action::ActionConfig {
        vms_core::action::ActionConfig::Snapshot(vms_core::action::SnapshotConfig {
            format: "jpeg".into(),
            quality: 90,
            camera_id,
        })
    }

    #[test]
    fn action_node_referencing_a_missing_camera_is_flagged() {
        let missing_camera = Uuid::new_v4();
        let mut node = base_node(CoreNodeType::Action);
        node.action_config = Some(snapshot_config(Some(missing_camera)));

        let issues = check_dangling_camera_references(&[node.clone()], &HashSet::new());
        assert_eq!(issues.len(), 1);
        assert_eq!(
            issues[0].category,
            ValidationCategory::DanglingCameraReference
        );
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert_eq!(issues[0].node_id, Some(node.id));
    }

    #[test]
    fn action_node_referencing_an_existing_camera_is_not_flagged() {
        let existing_camera = Uuid::new_v4();
        let mut node = base_node(CoreNodeType::Action);
        node.action_config = Some(snapshot_config(Some(existing_camera)));

        let issues = check_dangling_camera_references(&[node], &HashSet::from([existing_camera]));
        assert!(issues.is_empty());
    }

    #[test]
    fn action_node_with_inherited_camera_is_not_flagged() {
        // `camera_id: None` inherits from the TriggerContext at execution
        // time — nothing to check here, and definitely not "dangling".
        let mut node = base_node(CoreNodeType::Action);
        node.action_config = Some(snapshot_config(None));

        assert!(check_dangling_camera_references(&[node], &HashSet::new()).is_empty());
    }

    #[test]
    fn non_camera_scoped_action_is_not_flagged() {
        let mut node = base_node(CoreNodeType::Action);
        node.action_config = Some(vms_core::action::ActionConfig::Skip);

        assert!(check_dangling_camera_references(&[node], &HashSet::new()).is_empty());
    }

    fn base_trigger() -> PipelineTrigger {
        PipelineTrigger {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            trigger_type: vms_core::trigger::TriggerType::Manual,
            source_id: None,
            camera_id: None,
            config: vms_core::trigger::TriggerConfig::Manual {
                parameter_schema: None,
            },
            enabled: true,
            last_error: None,
            last_error_at: None,
            unresolved_reference: false,
        }
    }

    #[test]
    fn unresolved_trigger_is_flagged_as_an_error() {
        let mut trigger = base_trigger();
        trigger.unresolved_reference = true;

        let issues = check_unresolved_trigger_references(&[trigger.clone()]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::UnresolvedReference);
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn resolved_trigger_is_not_flagged() {
        let trigger = base_trigger();
        assert!(check_unresolved_trigger_references(&[trigger]).is_empty());
    }

    #[test]
    fn unresolved_node_is_flagged_as_an_error() {
        let mut node = base_node(CoreNodeType::Transport);
        node.unresolved_reference = true;

        let issues = check_unresolved_node_references(&[node.clone()]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::UnresolvedReference);
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert_eq!(issues[0].node_id, Some(node.id));
    }

    #[test]
    fn resolved_node_is_not_flagged() {
        let node = base_node(CoreNodeType::Transport);
        assert!(check_unresolved_node_references(&[node]).is_empty());
    }

    // -- check_artifact_lineage --

    fn transcode_config() -> vms_core::action::ActionConfig {
        vms_core::action::ActionConfig::Transcode(vms_core::action::TranscodeConfig {
            codec: "h264".into(),
            bitrate_kbps: 2000,
            resolution: None,
            preset: "fast".into(),
            output_format: "mp4".into(),
        })
    }

    fn extract_clip_config() -> vms_core::action::ActionConfig {
        vms_core::action::ActionConfig::ExtractClip(vms_core::action::ExtractClipConfig {
            pre_event_secs: 5,
            post_event_secs: 5,
            format: "mp4".into(),
            camera_id: None,
            use_manual_range: false,
        })
    }

    fn compress_config() -> vms_core::action::ActionConfig {
        vms_core::action::ActionConfig::Compress(vms_core::action::CompressConfig {
            algorithm: vms_core::action::CompressionAlgorithm::Zstd,
            level: 3,
        })
    }

    fn merge_clips_config() -> vms_core::action::ActionConfig {
        vms_core::action::ActionConfig::MergeClips(vms_core::action::MergeClipsConfig {
            order: vms_core::action::ClipOrder::Chronological,
            gap_fill: vms_core::action::GapFill::Skip,
            output_format: "mp4".into(),
        })
    }

    fn action_node(cfg: vms_core::action::ActionConfig) -> PipelineNode {
        let mut n = base_node(CoreNodeType::Action);
        n.action_config = Some(cfg);
        n
    }

    #[test]
    fn transcode_with_no_upstream_artifact_is_flagged() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let transcode = action_node(transcode_config());
        let edges = [edge(root.id, transcode.id, CoreEdgeType::Default)];

        let issues = check_artifact_lineage(&[root, transcode.clone()], &edges);
        assert_eq!(issues.len(), 1);
        assert_eq!(
            issues[0].category,
            ValidationCategory::MissingArtifactAncestor
        );
        assert_eq!(issues[0].severity, ValidationSeverity::Error);
        assert_eq!(issues[0].node_id, Some(transcode.id));
    }

    #[test]
    fn transcode_downstream_of_extract_clip_is_not_flagged() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let extract = action_node(extract_clip_config());
        let transcode = action_node(transcode_config());
        let edges = [
            edge(root.id, extract.id, CoreEdgeType::Default),
            edge(extract.id, transcode.id, CoreEdgeType::Default),
        ];

        let issues = check_artifact_lineage(&[root, extract, transcode], &edges);
        assert!(issues.is_empty());
    }

    #[test]
    fn merge_clips_with_two_extract_clip_ancestors_is_not_flagged() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let fork = base_node(CoreNodeType::Fork);
        let extract1 = action_node(extract_clip_config());
        let extract2 = action_node(extract_clip_config());
        let merge = action_node(merge_clips_config());
        let edges = [
            edge(root.id, fork.id, CoreEdgeType::Default),
            edge(fork.id, extract1.id, CoreEdgeType::Default),
            edge(fork.id, extract2.id, CoreEdgeType::Default),
            edge(extract1.id, merge.id, CoreEdgeType::Default),
            edge(extract2.id, merge.id, CoreEdgeType::Default),
        ];

        let issues = check_artifact_lineage(&[root, fork, extract1, extract2, merge], &edges);
        assert!(issues.is_empty());
    }

    #[test]
    fn merge_clips_with_one_extract_clip_ancestor_is_flagged_as_single_source() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let extract = action_node(extract_clip_config());
        let merge = action_node(merge_clips_config());
        let edges = [
            edge(root.id, extract.id, CoreEdgeType::Default),
            edge(extract.id, merge.id, CoreEdgeType::Default),
        ];

        let issues = check_artifact_lineage(&[root, extract, merge.clone()], &edges);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].category, ValidationCategory::MergeSingleSource);
        assert_eq!(issues[0].severity, ValidationSeverity::Warning);
        assert_eq!(issues[0].node_id, Some(merge.id));
    }

    #[test]
    fn merge_clips_fed_by_a_condition_branch_with_no_artifact_is_flagged() {
        // Condition -> [true: Extract Clip, false: Compress (no artifact of
        // its own)] -> both reconverge into the same Merge Clips node. Only
        // one branch is ever active on a given run, so Merge Clips is safe
        // when the true branch fires and unsafe when the false branch fires
        // — a check that just ORs across Merge Clips' direct parents
        // (ignoring that they're mutually exclusive alternatives of the same
        // Condition) would miss the false-branch failure entirely.
        let root = base_node(CoreNodeType::TriggerRoot);
        let mut condition = base_node(CoreNodeType::Condition);
        condition.condition_expr = Some("true".into());
        let extract = action_node(extract_clip_config());
        let compress = action_node(compress_config());
        let merge = action_node(merge_clips_config());
        let edges = [
            edge(root.id, condition.id, CoreEdgeType::Default),
            edge(condition.id, extract.id, CoreEdgeType::TrueBranch),
            edge(condition.id, compress.id, CoreEdgeType::FalseBranch),
            edge(extract.id, merge.id, CoreEdgeType::Default),
            edge(compress.id, merge.id, CoreEdgeType::Default),
        ];

        let issues = check_artifact_lineage(
            &[root, condition, extract, compress.clone(), merge.clone()],
            &edges,
        );
        let flagged: HashMap<Uuid, ValidationCategory> = issues
            .iter()
            .map(|i| (i.node_id.unwrap(), i.category))
            .collect();
        assert_eq!(
            flagged.get(&merge.id),
            Some(&ValidationCategory::MissingArtifactAncestor)
        );
        assert_eq!(
            flagged.get(&compress.id),
            Some(&ValidationCategory::MissingArtifactAncestor)
        );
    }

    #[test]
    fn unreachable_pass_through_node_is_not_flagged() {
        // Not wired to the root at all — check_disconnected's problem, not ours.
        let root = base_node(CoreNodeType::TriggerRoot);
        let orphan = action_node(transcode_config());
        assert!(check_artifact_lineage(&[root, orphan], &[]).is_empty());
    }

    #[test]
    fn non_pass_through_action_is_never_flagged() {
        let root = base_node(CoreNodeType::TriggerRoot);
        let skip = action_node(vms_core::action::ActionConfig::Skip);
        let edges = [edge(root.id, skip.id, CoreEdgeType::Default)];
        assert!(check_artifact_lineage(&[root, skip], &edges).is_empty());
    }
}
