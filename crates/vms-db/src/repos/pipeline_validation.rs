//! Computes what's wrong with a pipeline's current definition — incomplete or
//! malformed node config, disconnected nodes, structural DAG violations, and
//! dangling camera references — independent of whether the pipeline can
//! currently be compiled and run. One reusable computation, called from
//! wherever a pipeline's validity needs checking, rather than reimplemented
//! per call site.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::pipeline::{NodeType as CoreNodeType, PipelineDag, PipelineEdge, PipelineNode};

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
}

impl ValidationCategory {
    fn severity(self) -> ValidationSeverity {
        match self {
            ValidationCategory::Disconnected => ValidationSeverity::Warning,
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
    fn new(category: ValidationCategory, node_id: Option<Uuid>, message: impl Into<String>) -> Self {
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
                if !node
                    .condition_expr
                    .as_deref()
                    .is_some_and(|e| !e.trim().is_empty())
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

/// DAG-level structural rules: exactly one trigger_root node, no cycles,
/// condition nodes with exactly one true/false outgoing edge each,
/// transport/device-control nodes as leaves. Reuses `PipelineDag::compile`
/// rather than re-deriving these rules. Only checked against the subgraph
/// reachable from the trigger root (see `reachable_from_root`) — an
/// unrelated disconnected node is `check_disconnected`'s problem to report,
/// not grounds for a spurious "wrong root count" or "cycle" here too.
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
        }
    }

    #[test]
    fn incomplete_transport_node_is_flagged() {
        let node = base_node(CoreNodeType::Transport);
        let issues = check_config_completeness(&[node.clone()]);
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

        let issues = check_dangling_camera_references(
            &[node],
            &HashSet::from([existing_camera]),
        );
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
}
