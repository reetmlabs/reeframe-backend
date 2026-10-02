//! Pipeline node CRUD: `/pipelines/{id}/nodes[/{node_id}]`. Edges and
//! triggers have their own route modules.
//!
//! Responses return [`PipelineNode`] directly instead of a separate DTO: it
//! already derives `Serialize`, has no sensitive fields, and is the shape a
//! pipeline-editing client needs.

use salvo::prelude::*;
use serde::Deserialize;
use uuid::Uuid;
use vms_core::{ActionConfig, NodeType, PipelineNode, TransportConfig, TriggerConfig};
use vms_db::repos::pipeline::{CreateNode, CreateTrigger, UpdateNode};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Request bodies --

/// Trigger fields carried by a `trigger_root` node. Matches
/// `pipeline_triggers::CreateTriggerBody` because saving this node creates or
/// updates the pipeline's single trigger row (see
/// `PipelineRepo::upsert_node_trigger`).
#[derive(Deserialize)]
pub struct NodeTriggerBody {
    pub config: TriggerConfig,
    pub source_id: Option<Uuid>,
    pub camera_id: Option<Uuid>,
    #[serde(default = "default_trigger_enabled")]
    pub enabled: bool,
}

fn default_trigger_enabled() -> bool {
    true
}

#[derive(Deserialize)]
pub struct CreateNodeBody {
    pub node_type: NodeType,
    pub action_config: Option<ActionConfig>,
    pub transport_config: Option<TransportConfig>,
    pub destination_id: Option<Uuid>,
    pub contact_list_id: Option<Uuid>,
    pub condition_expr: Option<String>,
    pub trigger: Option<NodeTriggerBody>,
    pub label: Option<String>,
    pub pos_x: Option<f64>,
    pub pos_y: Option<f64>,
}

/// All fields are optional; only supplied fields are updated. `node_type`
/// cannot be changed after creation; see [`UpdateNode`]'s doc comment.
#[derive(Deserialize)]
pub struct UpdateNodeBody {
    pub action_config: Option<ActionConfig>,
    pub transport_config: Option<TransportConfig>,
    pub destination_id: Option<Uuid>,
    pub contact_list_id: Option<Uuid>,
    pub condition_expr: Option<String>,
    pub trigger: Option<NodeTriggerBody>,
    pub label: Option<String>,
    pub pos_x: Option<f64>,
    pub pos_y: Option<f64>,
}

// -- Handlers --

/// POST /pipelines/{id}/nodes
#[handler]
pub async fn create_node(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<PipelineNode>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let body: CreateNodeBody = parse_body(req).await?;

    if body.trigger.is_some() && body.node_type != NodeType::TriggerRoot {
        return Err(ApiError::bad_request(
            "trigger config is only valid on a trigger_root node",
        ));
    }
    let trigger = body.trigger;

    let node = state
        .pipeline_repo
        .create_node(
            pipeline_id,
            CreateNode {
                node_type: body.node_type,
                action_config: body.action_config,
                transport_config: body.transport_config,
                destination_id: body.destination_id,
                contact_list_id: body.contact_list_id,
                condition_expr: body.condition_expr,
                label: body.label,
                pos_x: body.pos_x,
                pos_y: body.pos_y,
            },
        )
        .await?;

    if let Some(t) = trigger {
        state
            .pipeline_repo
            .upsert_node_trigger(
                pipeline_id,
                CreateTrigger {
                    config: t.config,
                    source_id: t.source_id,
                    camera_id: t.camera_id,
                    enabled: t.enabled,
                },
            )
            .await?;
    }

    state.refresh_pipelines().await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(node))
}

/// GET /pipelines/{id}/nodes
#[handler]
pub async fn list_nodes(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<PipelineNode>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let nodes = state.pipeline_repo.load_nodes(pipeline_id).await?;
    Ok(Json(nodes))
}

/// GET /pipelines/{id}/nodes/{node_id}
#[handler]
pub async fn get_node(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineNode>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, node_id) = parse_pipeline_and_node_id(req)?;

    let node = state
        .pipeline_repo
        .get_node(node_id)
        .await?
        .filter(|n| n.pipeline_id == pipeline_id)
        .ok_or_else(|| ApiError::not_found(format!("node {node_id} not found")))?;

    Ok(Json(node))
}

/// PATCH /pipelines/{id}/nodes/{node_id}
#[handler]
pub async fn update_node(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineNode>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, node_id) = parse_pipeline_and_node_id(req)?;
    let body: UpdateNodeBody = parse_body(req).await?;

    // Confirm the node belongs to this pipeline, otherwise a valid node_id
    // under the wrong pipeline_id would update another pipeline's node. The
    // lookup also gives us node_type, since `trigger` is only valid on a
    // trigger_root node.
    let existing = require_node_in_pipeline(state, pipeline_id, node_id).await?;
    if body.trigger.is_some() && existing.node_type != NodeType::TriggerRoot {
        return Err(ApiError::bad_request(
            "trigger config is only valid on a trigger_root node",
        ));
    }
    let trigger = body.trigger;

    let node = state
        .pipeline_repo
        .update_node(
            node_id,
            UpdateNode {
                action_config: body.action_config,
                transport_config: body.transport_config,
                destination_id: body.destination_id.map(Some),
                contact_list_id: body.contact_list_id.map(Some),
                condition_expr: body.condition_expr,
                label: body.label.map(Some),
                pos_x: body.pos_x.map(Some),
                pos_y: body.pos_y.map(Some),
            },
        )
        .await?;

    if let Some(t) = trigger {
        state
            .pipeline_repo
            .upsert_node_trigger(
                pipeline_id,
                CreateTrigger {
                    config: t.config,
                    source_id: t.source_id,
                    camera_id: t.camera_id,
                    enabled: t.enabled,
                },
            )
            .await?;
    }

    state.refresh_pipelines().await?;
    Ok(Json(node))
}

/// DELETE /pipelines/{id}/nodes/{node_id}
///
/// Cascades to any edges referencing this node (see `PipelineRepo::delete_node`).
#[handler]
pub async fn delete_node(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, node_id) = parse_pipeline_and_node_id(req)?;

    let existing = require_node_in_pipeline(state, pipeline_id, node_id).await?;
    state.pipeline_repo.delete_node(node_id).await?;
    if existing.node_type == NodeType::TriggerRoot {
        // With no trigger_root node left, drop the pipeline's trigger rows so
        // an enabled trigger doesn't keep holding a source or camera open.
        state
            .pipeline_repo
            .delete_triggers_for_pipeline(pipeline_id)
            .await?;
    }
    state.refresh_pipelines().await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Helpers --

fn parse_pipeline_and_node_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let pipeline_id = parse_id(req)?;
    let node_id: Uuid = req
        .param::<String>("node_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid node_id: expected UUID"))?;
    Ok((pipeline_id, node_id))
}

async fn require_node_in_pipeline(
    state: &AppState,
    pipeline_id: Uuid,
    node_id: Uuid,
) -> Result<PipelineNode, ApiError> {
    state
        .pipeline_repo
        .get_node(node_id)
        .await?
        .filter(|n| n.pipeline_id == pipeline_id)
        .ok_or_else(|| ApiError::not_found(format!("node {node_id} not found")))
}
