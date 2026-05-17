//! Pipeline DAG model — compile, validate, and walk the execution graph.
//!
//! A VMS pipeline is a directed acyclic graph (DAG) of typed [`PipelineNode`]s
//! connected by [`PipelineEdge`]s.  The graph is compiled once from raw
//! database rows by [`PipelineDag::compile`], which validates structural rules
//! and pre-computes the adjacency and topological-order maps needed by the
//! executor.
//!
//! The compiled [`CompiledPipeline`] is held inside an `Arc` and stored in the
//! Pipeline Registry behind an `ArcSwap` so pipelines can be hot-reloaded
//! without pausing in-flight runs.
//!
//! # Graph structure rules (enforced at compile time)
//!
//! 1. Exactly one parentless node — the trigger root.
//! 2. The parentless node must have `node_type = trigger_root`.
//! 3. No cycles (detected via Kahn's algorithm).
//! 4. `Transport` and `DeviceControl` nodes must be leaves (no outgoing edges).
//! 5. `Condition` nodes must have exactly two outgoing edges: `true_branch` and `false_branch`.
//! 6. `Condition` nodes must carry a non-empty `condition_expr`.
//! 7. `Transport` nodes must have a `destination_id`.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::action::{ActionConfig, TransportConfig};
use crate::error::VmsError;
use crate::trigger::{TriggerConfig, TriggerType};

/// UUID alias used as the primary key for all nodes within a pipeline.
pub type NodeId = Uuid;

// ── Node / edge enums ─────────────────────────────────────────────────────────

/// Discriminator that determines how the executor processes a pipeline node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// The pipeline entry point — always the single parentless node.
    ///
    /// The executor seeds this node's output from the [`TriggerContext`] and
    /// then walks the DAG from here.
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    TriggerRoot,
    /// A media-processing or device-control action (see [`ActionConfig`]).
    Action,
    /// A camera/NVR device command (PTZ, recording, quality switch).
    ///
    /// Must be a leaf node — no outgoing edges allowed.
    DeviceControl,
    /// Delivers an artifact or message to an external destination.
    ///
    /// Must be a leaf node — no outgoing edges allowed.
    Transport,
    /// Fan-out: passes the same output to all child nodes unconditionally.
    Fork,
    /// Conditional branch: evaluates `condition_expr` and routes to either the
    /// `true_branch` or `false_branch` child.
    Condition,
}

/// The label attached to an edge that determines routing for [`NodeType::Condition`] nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Standard edge — used for all non-condition routing.
    Default,
    /// Followed when a condition node's expression evaluates to `true`.
    TrueBranch,
    /// Followed when a condition node's expression evaluates to `false`.
    FalseBranch,
}

// ── Core graph types ──────────────────────────────────────────────────────────

/// A raw pipeline node row loaded from the database before compilation.
///
/// Fields are a superset of all node types; the subset that is populated
/// depends on [`node_type`](PipelineNode::node_type).  The compiler checks
/// that required fields are present for each type via [`PipelineDag::compile`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineNode {
    /// Unique identifier for this node within the pipeline.
    pub id: NodeId,
    /// The pipeline this node belongs to.
    pub pipeline_id: Uuid,
    /// How the executor treats this node.
    pub node_type: NodeType,
    /// Set for `Action` and `DeviceControl` nodes; `None` otherwise.
    pub action_config: Option<ActionConfig>,
    /// Foreign key to the `destinations` table.  Required for `Transport` nodes.
    pub destination_id: Option<Uuid>,
    /// Optional foreign key to a `contact_lists` row used by messaging transports.
    pub contact_list_id: Option<Uuid>,
    /// Template overrides for `Transport` nodes.
    pub transport_config: Option<TransportConfig>,
    /// `evalexpr` expression string evaluated by `Condition` nodes.
    pub condition_expr: Option<String>,
    /// Human-readable label shown in the pipeline editor UI.
    pub label: Option<String>,
    /// Horizontal canvas position (ignored at runtime, used by the UI only).
    pub pos_x: Option<f64>,
    /// Vertical canvas position (ignored at runtime, used by the UI only).
    pub pos_y: Option<f64>,
}

/// A raw pipeline edge row loaded from the database before compilation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEdge {
    /// Unique identifier for this edge.
    pub id: Uuid,
    /// The pipeline this edge belongs to.
    pub pipeline_id: Uuid,
    /// The node this edge originates from.
    pub from_node_id: NodeId,
    /// The node this edge terminates at.
    pub to_node_id: NodeId,
    /// Routing label — only significant when `from_node_id` is a `Condition` node.
    pub edge_type: EdgeType,
}

/// A trigger definition belonging to a pipeline, loaded from `pipeline_triggers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTrigger {
    /// Unique identifier for this trigger row.
    pub id: Uuid,
    /// The pipeline this trigger activates.
    pub pipeline_id: Uuid,
    /// Which kind of trigger this is.
    pub trigger_type: TriggerType,
    /// Source adapter UUID for [`TriggerType::Event`] triggers.
    pub source_id: Option<Uuid>,
    /// Camera UUID scoped triggers (e.g. [`TriggerType::System`] signals).
    pub camera_id: Option<Uuid>,
    /// Full typed configuration serialized as JSONB in the database.
    pub config: TriggerConfig,
    /// Whether this trigger is currently active.  Disabled triggers are loaded
    /// but skipped by the Trigger Manager.
    pub enabled: bool,
}

// ── Compiled forms held in the registry ──────────────────────────────────────

/// Camera resource requirements for one pipeline, loaded from `pipeline_camera_refs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineCameraRef {
    pub camera_id: Uuid,
    /// Pipeline has an `extract_clip` node — needs the ring-buffer appsink branch.
    pub needs_ring_buffer: bool,
    /// Pipeline has an analytics trigger or action — needs the ONNX appsink branch.
    pub needs_analytics: bool,
}

/// A pipeline loaded, validated, and ready for execution.
///
/// Held behind `Arc` inside `ArcSwap<HashMap<Uuid, Arc<CompiledPipeline>>>` in
/// the Pipeline Registry.  Because the inner `Arc` is cheap to clone, worker
/// threads can take a snapshot of the registry without blocking reloads.
#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    /// UUID of the `pipelines` table row.
    pub id: Uuid,
    /// Human-readable name for logging and the UI.
    pub name: String,
    /// When `false`, the Trigger Manager will not activate any triggers for
    /// this pipeline even if they are individually enabled.
    pub enabled: bool,
    /// The validated, pre-computed execution graph.
    pub dag: PipelineDag,
    /// All triggers that can activate this pipeline.
    pub triggers: Vec<PipelineTrigger>,
    /// Cameras this pipeline references, with per-camera resource flags.
    /// Used by the Resource Manager to start/stop camera pipelines and branches.
    pub camera_refs: Vec<PipelineCameraRef>,
    /// Source adapter UUIDs this pipeline references.
    /// Used by the Resource Manager to start/stop source adapters.
    pub source_refs: Vec<Uuid>,
}

/// Topologically sorted DAG with precomputed adjacency and parent maps.
///
/// Built once by [`PipelineDag::compile`] from raw node and edge rows.  All
/// maps are `HashMap` keyed by [`NodeId`] so per-node lookups are O(1).
/// The struct is cheap to clone because it lives behind `Arc<CompiledPipeline>`.
#[derive(Debug, Clone)]
pub struct PipelineDag {
    /// All nodes in the pipeline, keyed by their UUID.
    pub nodes: HashMap<NodeId, PipelineNode>,
    /// All edges in the pipeline (retained for serialization and debugging).
    pub edges: Vec<PipelineEdge>,
    /// Nodes in topological order — root first, leaves last.
    ///
    /// The executor walks this slice to determine scheduling order.
    pub topological_order: Vec<NodeId>,
    /// `node → children` adjacency list (outgoing edges).
    pub adjacency: HashMap<NodeId, Vec<NodeId>>,
    /// `node → parents` adjacency list (incoming edges).
    ///
    /// Used to determine when all parent outputs are available before
    /// scheduling a node.
    pub parents: HashMap<NodeId, Vec<NodeId>>,
    /// `(from, to) → EdgeType` — required for condition-node routing.
    pub edge_types: HashMap<(NodeId, NodeId), EdgeType>,
    /// The single parentless node — always a [`NodeType::TriggerRoot`].
    pub root_id: NodeId,
}

impl PipelineDag {
    /// Validate raw nodes + edges and build the compiled DAG.
    ///
    /// This is the only constructor for [`PipelineDag`].  It enforces all
    /// structural rules from the design specification before returning:
    ///
    /// 1. Exactly one node with no parents (the trigger root).
    /// 2. That root node must be of type `trigger_root`.
    /// 3. No cycles (detected via Kahn's algorithm).
    /// 4. `Transport` and `DeviceControl` nodes must be leaves.
    /// 5. `Condition` nodes must have exactly two outgoing edges: `true_branch` and `false_branch`.
    /// 6. `Condition` nodes must carry an expression string.
    /// 7. `Transport` nodes must have a `destination_id`.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::DagValidation`] with a descriptive message when any
    /// rule is violated.
    pub fn compile(nodes: Vec<PipelineNode>, edges: Vec<PipelineEdge>) -> Result<Self, VmsError> {
        if nodes.is_empty() {
            return Err(VmsError::DagValidation("pipeline has no nodes".into()));
        }

        let node_map: HashMap<NodeId, PipelineNode> =
            nodes.into_iter().map(|n| (n.id, n)).collect();

        // ── Build adjacency, parents, and edge-type maps ──────────────────────
        let mut adjacency: HashMap<NodeId, Vec<NodeId>> =
            node_map.keys().map(|&id| (id, vec![])).collect();
        let mut parents: HashMap<NodeId, Vec<NodeId>> =
            node_map.keys().map(|&id| (id, vec![])).collect();
        let mut edge_types: HashMap<(NodeId, NodeId), EdgeType> = HashMap::new();

        for edge in &edges {
            if !node_map.contains_key(&edge.from_node_id) {
                return Err(VmsError::DagValidation(format!(
                    "edge references unknown from_node_id {}",
                    edge.from_node_id
                )));
            }
            if !node_map.contains_key(&edge.to_node_id) {
                return Err(VmsError::DagValidation(format!(
                    "edge references unknown to_node_id {}",
                    edge.to_node_id
                )));
            }
            adjacency
                .entry(edge.from_node_id)
                .or_default()
                .push(edge.to_node_id);
            parents
                .entry(edge.to_node_id)
                .or_default()
                .push(edge.from_node_id);
            edge_types.insert((edge.from_node_id, edge.to_node_id), edge.edge_type.clone());
        }

        // ── Rule 1 & 2: exactly one parentless node, must be trigger_root ────
        let roots: Vec<NodeId> = node_map
            .keys()
            .filter(|id| parents[id].is_empty())
            .copied()
            .collect();

        if roots.len() != 1 {
            return Err(VmsError::DagValidation(format!(
                "pipeline must have exactly 1 root node (no parents), found {}",
                roots.len()
            )));
        }
        let root_id = roots[0];

        if node_map[&root_id].node_type != NodeType::TriggerRoot {
            return Err(VmsError::DagValidation(
                "the parentless node must have node_type = trigger_root".into(),
            ));
        }

        // ── Rule 3: no cycles ────────────────────────────────────────────────
        let topological_order = kahn_topological_sort(&node_map, &adjacency, &parents)?;

        // ── Rules 4–7: per-node structural checks ────────────────────────────
        for node in node_map.values() {
            let children = &adjacency[&node.id];

            match node.node_type {
                NodeType::Transport => {
                    if !children.is_empty() {
                        return Err(VmsError::DagValidation(format!(
                            "transport node {} must be a leaf but has {} outgoing edge(s)",
                            node.id,
                            children.len()
                        )));
                    }
                    if node.destination_id.is_none() {
                        return Err(VmsError::DagValidation(format!(
                            "transport node {} has no destination_id",
                            node.id
                        )));
                    }
                }
                NodeType::DeviceControl => {
                    if !children.is_empty() {
                        return Err(VmsError::DagValidation(format!(
                            "device_control node {} must be a leaf but has {} outgoing edge(s)",
                            node.id,
                            children.len()
                        )));
                    }
                }
                NodeType::Condition => {
                    if node.condition_expr.is_none() {
                        return Err(VmsError::DagValidation(format!(
                            "condition node {} has no expression",
                            node.id
                        )));
                    }
                    let has_true = children
                        .iter()
                        .any(|&c| edge_types.get(&(node.id, c)) == Some(&EdgeType::TrueBranch));
                    let has_false = children
                        .iter()
                        .any(|&c| edge_types.get(&(node.id, c)) == Some(&EdgeType::FalseBranch));
                    if children.len() != 2 || !has_true || !has_false {
                        return Err(VmsError::DagValidation(format!(
                            "condition node {} must have exactly 2 outgoing edges \
                             tagged true_branch and false_branch",
                            node.id
                        )));
                    }
                }
                _ => {}
            }
        }

        Ok(PipelineDag {
            nodes: node_map,
            edges,
            topological_order,
            adjacency,
            parents,
            edge_types,
            root_id,
        })
    }

    /// Returns the children of `node_id` that should be enqueued for execution.
    ///
    /// For [`NodeType::Condition`] nodes, only the branch matching `branch_taken`
    /// is returned — `true` selects the `true_branch` edge, `false` selects
    /// `false_branch`.  For all other node types every child is returned.
    ///
    /// Returns an empty `Vec` if `node_id` is not in the graph.
    pub fn children_to_execute(&self, node_id: NodeId, branch_taken: Option<bool>) -> Vec<NodeId> {
        let Some(children) = self.adjacency.get(&node_id) else {
            return vec![];
        };

        if self.nodes.get(&node_id).map(|n| &n.node_type) == Some(&NodeType::Condition) {
            let target = if branch_taken.unwrap_or(false) {
                EdgeType::TrueBranch
            } else {
                EdgeType::FalseBranch
            };
            children
                .iter()
                .filter(|&&c| self.edge_types.get(&(node_id, c)) == Some(&target))
                .copied()
                .collect()
        } else {
            children.clone()
        }
    }
}

// ── Kahn's topological sort ───────────────────────────────────────────────────

/// Topologically sort `node_map` using Kahn's algorithm.
///
/// Returns the nodes in topological order (sources first) or
/// [`VmsError::DagValidation`] if a cycle is detected (i.e. the output length
/// is shorter than the node count after the queue empties).
fn kahn_topological_sort(
    node_map: &HashMap<NodeId, PipelineNode>,
    adjacency: &HashMap<NodeId, Vec<NodeId>>,
    parents: &HashMap<NodeId, Vec<NodeId>>,
) -> Result<Vec<NodeId>, VmsError> {
    let mut in_degree: HashMap<NodeId, usize> = node_map
        .keys()
        .map(|&id| (id, parents[&id].len()))
        .collect();

    let mut queue: VecDeque<NodeId> = in_degree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut order = Vec::with_capacity(node_map.len());

    while let Some(node_id) = queue.pop_front() {
        order.push(node_id);
        for &child in &adjacency[&node_id] {
            let deg = in_degree.get_mut(&child).expect("child in map");
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(child);
            }
        }
    }

    if order.len() != node_map.len() {
        return Err(VmsError::DagValidation("pipeline contains a cycle".into()));
    }

    Ok(order)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn root_node(pipeline_id: Uuid) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id,
            node_type: NodeType::TriggerRoot,
            action_config: None,
            destination_id: None,
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: Some("Root".into()),
            pos_x: None,
            pos_y: None,
        }
    }

    fn transport_node(pipeline_id: Uuid, dest_id: Uuid) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id,
            node_type: NodeType::Transport,
            action_config: None,
            destination_id: Some(dest_id),
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        }
    }

    fn edge(pipeline_id: Uuid, from: NodeId, to: NodeId) -> PipelineEdge {
        PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id,
            from_node_id: from,
            to_node_id: to,
            edge_type: EdgeType::Default,
        }
    }

    #[test]
    fn simple_root_to_transport() {
        let pid = Uuid::new_v4();
        let dest = Uuid::new_v4();
        let root = root_node(pid);
        let transport = transport_node(pid, dest);
        let e = edge(pid, root.id, transport.id);
        let root_id = root.id;

        let dag = PipelineDag::compile(vec![root, transport], vec![e]).unwrap();
        assert_eq!(dag.root_id, root_id);
        assert_eq!(dag.topological_order.len(), 2);
        assert_eq!(dag.topological_order[0], root_id);
    }

    #[test]
    fn cycle_detected() {
        let pid = Uuid::new_v4();
        let root = root_node(pid);
        let a = PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id: pid,
            node_type: NodeType::Fork,
            action_config: None,
            destination_id: None,
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        };
        let b_id = Uuid::new_v4();
        let mut b = a.clone();
        b.id = b_id;

        let e1 = edge(pid, root.id, a.id);
        let e2 = edge(pid, a.id, b.id);
        let e3 = edge(pid, b.id, a.id); // cycle

        let result = PipelineDag::compile(vec![root, a, b], vec![e1, e2, e3]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn transport_node_must_have_destination() {
        let pid = Uuid::new_v4();
        let root = root_node(pid);
        let bad_transport = PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id: pid,
            node_type: NodeType::Transport,
            action_config: None,
            destination_id: None, // missing
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        };
        let e = edge(pid, root.id, bad_transport.id);
        let result = PipelineDag::compile(vec![root, bad_transport], vec![e]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }
}
