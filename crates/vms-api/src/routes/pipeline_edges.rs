//! Pipeline edge CRUD — `/pipelines/{id}/edges[/{edge_id}]`.
//!
//! The second piece of the pipeline-graph-editing gap flagged in step 8 and
//! closed by step 9 (nodes in 9-5, edges here, triggers in 9-7). This is
//! also where the edge-dependent structural rules from `PipelineDag`'s doc
//! comment — no cycles, `Transport`/`DeviceControl` must be leaves,
//! `Condition` branch shape — finally become checkable; 9-5 could only
//! enforce the node-level rules.
//!
//! Responses return [`PipelineEdge`] directly, same rationale as
//! `pipeline_nodes.rs`: it already derives `Serialize`, has no sensitive
//! fields, and a hand-built DTO would just be the same shape twice.

use salvo::prelude::*;
use serde::Deserialize;
use uuid::Uuid;
use vms_core::{EdgeType, PipelineEdge};
use vms_db::repos::pipeline::{CreateEdge, UpdateEdge};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Request bodies --

#[derive(Deserialize)]
pub struct CreateEdgeBody {
    pub from_node_id: Uuid,
    pub to_node_id: Uuid,
    /// Omit for a plain data-flow edge. Only meaningful (and only valid) as
    /// `true_branch`/`false_branch` when `from_node_id` is a `condition` node.
    pub edge_type: Option<EdgeType>,
}

#[derive(Deserialize)]
pub struct UpdateEdgeBody {
    pub edge_type: EdgeType,
}

// -- Handlers --

/// POST /pipelines/{id}/edges
#[handler]
pub async fn create_edge(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<PipelineEdge>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let body: CreateEdgeBody = parse_body(req).await?;

    let edge = state
        .pipeline_repo
        .create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: body.from_node_id,
                to_node_id: body.to_node_id,
                edge_type: body.edge_type.unwrap_or(EdgeType::Default),
            },
        )
        .await?;

    res.status_code(StatusCode::CREATED);
    Ok(Json(edge))
}

/// GET /pipelines/{id}/edges
#[handler]
pub async fn list_edges(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<PipelineEdge>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let edges = state.pipeline_repo.load_edges(pipeline_id).await?;
    Ok(Json(edges))
}

/// GET /pipelines/{id}/edges/{edge_id}
#[handler]
pub async fn get_edge(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineEdge>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, edge_id) = parse_pipeline_and_edge_id(req)?;

    let edge = state
        .pipeline_repo
        .get_edge(edge_id)
        .await?
        .filter(|e| e.pipeline_id == pipeline_id)
        .ok_or_else(|| ApiError::not_found(format!("edge {edge_id} not found")))?;

    Ok(Json(edge))
}

/// PATCH /pipelines/{id}/edges/{edge_id}
#[handler]
pub async fn update_edge(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineEdge>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, edge_id) = parse_pipeline_and_edge_id(req)?;
    let body: UpdateEdgeBody = parse_body(req).await?;

    require_edge_in_pipeline(state, pipeline_id, edge_id).await?;

    let edge = state
        .pipeline_repo
        .update_edge(
            edge_id,
            UpdateEdge {
                edge_type: body.edge_type,
            },
        )
        .await?;

    Ok(Json(edge))
}

/// DELETE /pipelines/{id}/edges/{edge_id}
#[handler]
pub async fn delete_edge(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, edge_id) = parse_pipeline_and_edge_id(req)?;

    require_edge_in_pipeline(state, pipeline_id, edge_id).await?;
    state.pipeline_repo.delete_edge(edge_id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Helpers --

fn parse_pipeline_and_edge_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let pipeline_id = parse_id(req)?;
    let edge_id: Uuid = req
        .param::<String>("edge_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid edge_id: expected UUID"))?;
    Ok((pipeline_id, edge_id))
}

async fn require_edge_in_pipeline(
    state: &AppState,
    pipeline_id: Uuid,
    edge_id: Uuid,
) -> Result<(), ApiError> {
    let belongs = state
        .pipeline_repo
        .get_edge(edge_id)
        .await?
        .is_some_and(|e| e.pipeline_id == pipeline_id);
    if !belongs {
        return Err(ApiError::not_found(format!("edge {edge_id} not found")));
    }
    Ok(())
}
