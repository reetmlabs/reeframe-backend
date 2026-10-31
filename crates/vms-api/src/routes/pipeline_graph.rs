//! Whole-graph save: `PUT /pipelines/{id}/graph`. Applies every node, edge and
//! trigger change of an editor save in one transaction, so a rejected item
//! leaves the pipeline untouched instead of half saved.

use std::collections::HashMap;

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::{EdgeType, PipelineEdge, PipelineNode, VmsError};
use vms_db::repos::pipeline::{
    CreateNode, CreateTrigger, GraphEdge, GraphItem, GraphNode, GraphSaveError, SaveGraph,
};

use super::pipeline_nodes::CreateNodeBody;
use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Request and response bodies --

#[derive(Deserialize)]
pub struct GraphBody {
    pub nodes: Vec<GraphNodeBody>,
    #[serde(default)]
    pub edges: Vec<GraphEdgeBody>,
}

/// `key` is the node's id for an existing node, or any client-chosen string
/// for a new one. Existing nodes left out of the body are deleted.
#[derive(Deserialize)]
pub struct GraphNodeBody {
    pub key: String,
    #[serde(flatten)]
    pub node: CreateNodeBody,
}

/// `from`/`to` are node keys from the same body.
#[derive(Deserialize)]
pub struct GraphEdgeBody {
    pub from: String,
    pub to: String,
    pub edge_type: Option<EdgeType>,
}

#[derive(Serialize)]
pub struct GraphResponse {
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    /// Every node key from the request mapped to its saved node id.
    pub node_ids: HashMap<String, Uuid>,
}

/// An [`ApiError`] that also names the node or edge that was rejected, as a
/// `node` key or an `edge` `{from, to}` object next to `error`.
pub struct GraphApiError {
    error: ApiError,
    item: GraphItem,
}

impl From<ApiError> for GraphApiError {
    fn from(error: ApiError) -> Self {
        Self {
            error,
            item: GraphItem::Graph,
        }
    }
}

impl From<VmsError> for GraphApiError {
    fn from(e: VmsError) -> Self {
        ApiError::from(e).into()
    }
}

impl From<GraphSaveError> for GraphApiError {
    fn from(e: GraphSaveError) -> Self {
        Self {
            error: e.error.into(),
            item: e.item,
        }
    }
}

#[async_trait]
impl Writer for GraphApiError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        let mut body = serde_json::json!({ "error": self.error.message });
        match self.item {
            GraphItem::Graph => {}
            GraphItem::Node(key) => body["node"] = key.into(),
            GraphItem::Edge { from, to } => {
                body["edge"] = serde_json::json!({ "from": from, "to": to })
            }
        }
        res.status_code(self.error.status);
        res.render(Json(body));
    }
}

// -- Handlers --

/// PUT /pipelines/{id}/graph
#[handler]
pub async fn save_graph(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<GraphResponse>, GraphApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let body: GraphBody = parse_body(req).await?;

    let graph = SaveGraph {
        nodes: body.nodes.into_iter().map(graph_node).collect(),
        edges: body
            .edges
            .into_iter()
            .map(|e| GraphEdge {
                from: e.from,
                to: e.to,
                edge_type: e.edge_type.unwrap_or(EdgeType::Default),
            })
            .collect(),
    };
    let saved = state.pipeline_repo.save_graph(pipeline_id, graph).await?;

    state.refresh_pipelines().await?;
    Ok(Json(GraphResponse {
        nodes: saved.nodes,
        edges: saved.edges,
        node_ids: saved.node_ids,
    }))
}

fn graph_node(body: GraphNodeBody) -> GraphNode {
    let n = body.node;
    GraphNode {
        key: body.key,
        node: CreateNode {
            node_type: n.node_type,
            action_config: n.action_config,
            transport_config: n.transport_config,
            destination_id: n.destination_id,
            contact_list_id: n.contact_list_id,
            condition_expr: n.condition_expr,
            label: n.label,
            pos_x: n.pos_x,
            pos_y: n.pos_y,
        },
        trigger: n.trigger.map(|t| CreateTrigger {
            config: t.config,
            source_id: t.source_id,
            camera_id: t.camera_id,
            enabled: t.enabled,
        }),
    }
}
