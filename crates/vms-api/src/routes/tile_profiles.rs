//! Tile/matrix camera-layout profile CRUD: `/tile-profiles[/{id}]` with nested
//! `tiles`, `bindings` and `site-assignments` routes.
//!
//! The data mirrors the desktop frontend's `tile_profiles`/`tile_formations`/
//! `tile_camera_bindings`/`profile_site_assignments` tables (`AppDatabase.cpp`)
//! and is stored server-side so every client shares the same layouts.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::{tile_camera_binding, tile_formation, tile_profile},
    repos::tile_layout::{CreateTileFormation, CreateTileProfile, UpdateTileFormation},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Response DTOs --

#[derive(Serialize)]
pub struct TileProfileDto {
    pub id: Uuid,
    pub name: String,
    pub site_id: Uuid,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<tile_profile::Model> for TileProfileDto {
    fn from(m: tile_profile::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            site_id: m.site_id,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

/// `col`/`row` on the wire. The internal `grid_col`/`grid_row` names avoid a
/// SeaORM macro collision; see `entities::tile_formation`.
#[derive(Serialize)]
pub struct TileFormationDto {
    pub id: Uuid,
    pub profile_id: Uuid,
    pub col: i32,
    pub row: i32,
    pub col_span: i32,
    pub row_span: i32,
}

impl From<tile_formation::Model> for TileFormationDto {
    fn from(m: tile_formation::Model) -> Self {
        Self {
            id: m.id,
            profile_id: m.profile_id,
            col: m.grid_col,
            row: m.grid_row,
            col_span: m.col_span,
            row_span: m.row_span,
        }
    }
}

#[derive(Serialize)]
pub struct TileCameraBindingDto {
    pub profile_id: Uuid,
    pub tile_id: Uuid,
    pub site_id: Uuid,
    pub camera_id: Uuid,
}

impl From<tile_camera_binding::Model> for TileCameraBindingDto {
    fn from(m: tile_camera_binding::Model) -> Self {
        Self {
            profile_id: m.profile_id,
            tile_id: m.tile_id,
            site_id: m.site_id,
            camera_id: m.camera_id,
        }
    }
}

// -- Request bodies --

#[derive(Deserialize)]
pub struct CreateTileProfileBody {
    pub name: String,
    pub site_id: Uuid,
}

/// Rename only, matching the frontend's `renameTileProfile`. A profile has no
/// other mutable fields apart from its formations, bindings and assignments.
#[derive(Deserialize)]
pub struct UpdateTileProfileBody {
    pub name: String,
}

#[derive(Deserialize)]
pub struct AssignSiteBody {
    pub site_id: Uuid,
}

#[derive(Deserialize)]
pub struct CreateTileFormationBody {
    pub col: i32,
    pub row: i32,
    pub col_span: Option<i32>,
    pub row_span: Option<i32>,
}

/// All fields are optional; only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateTileFormationBody {
    pub col: Option<i32>,
    pub row: Option<i32>,
    pub col_span: Option<i32>,
    pub row_span: Option<i32>,
}

#[derive(Deserialize)]
pub struct SetBindingBody {
    pub camera_id: Uuid,
}

// -- Handlers: profiles --

/// GET /tile-profiles?site_id=<uuid>
#[handler]
pub async fn list_profiles(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<TileProfileDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let site_id = parse_site_id_query(req)?;
    let profiles = state
        .tile_layout_repo
        .list_profiles_for_site(site_id)
        .await?;
    Ok(Json(
        profiles.into_iter().map(TileProfileDto::from).collect(),
    ))
}

/// POST /tile-profiles
#[handler]
pub async fn create_profile(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<TileProfileDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateTileProfileBody = parse_body(req).await?;

    let profile = state
        .tile_layout_repo
        .create_profile(CreateTileProfile {
            name: body.name,
            site_id: body.site_id,
        })
        .await?;

    res.status_code(StatusCode::CREATED);
    Ok(Json(profile.into()))
}

/// GET /tile-profiles/{id}
#[handler]
pub async fn get_profile(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<TileProfileDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let profile = state
        .tile_layout_repo
        .get_profile(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tile profile {id} not found")))?;
    Ok(Json(profile.into()))
}

/// PATCH /tile-profiles/{id}
#[handler]
pub async fn rename_profile(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<TileProfileDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateTileProfileBody = parse_body(req).await?;
    let profile = state.tile_layout_repo.rename_profile(id, body.name).await?;
    Ok(Json(profile.into()))
}

/// DELETE /tile-profiles/{id}
///
/// Cascades to this profile's formations, camera bindings, and site
/// assignments.
#[handler]
pub async fn delete_profile(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    state.tile_layout_repo.delete_profile(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /tile-profiles/{id}/site-assignments
///
/// Idempotent, matching the frontend's `assignProfileToSite`. Neither side has
/// an "unassign" operation.
#[handler]
pub async fn assign_to_site(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: AssignSiteBody = parse_body(req).await?;
    state
        .tile_layout_repo
        .assign_profile_to_site(id, body.site_id)
        .await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Handlers: tiles (formations) --

/// GET /tile-profiles/{id}/tiles
#[handler]
pub async fn list_tiles(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<TileFormationDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let profile_id = parse_id(req)?;
    let tiles = state.tile_layout_repo.list_formations(profile_id).await?;
    Ok(Json(
        tiles.into_iter().map(TileFormationDto::from).collect(),
    ))
}

/// POST /tile-profiles/{id}/tiles
#[handler]
pub async fn create_tile(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<TileFormationDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let profile_id = parse_id(req)?;
    let body: CreateTileFormationBody = parse_body(req).await?;

    let tile = state
        .tile_layout_repo
        .create_formation(
            profile_id,
            CreateTileFormation {
                col: body.col,
                row: body.row,
                col_span: body.col_span.unwrap_or(1),
                row_span: body.row_span.unwrap_or(1),
            },
        )
        .await?;

    res.status_code(StatusCode::CREATED);
    Ok(Json(tile.into()))
}

/// GET /tile-profiles/{id}/tiles/{tile_id}
#[handler]
pub async fn get_tile(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<TileFormationDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (profile_id, tile_id) = parse_profile_and_tile_id(req)?;
    let tile = require_tile_in_profile(state, profile_id, tile_id).await?;
    Ok(Json(tile.into()))
}

/// PATCH /tile-profiles/{id}/tiles/{tile_id}
#[handler]
pub async fn update_tile(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<TileFormationDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (profile_id, tile_id) = parse_profile_and_tile_id(req)?;
    // Confirm the tile belongs to this profile, otherwise a valid tile_id
    // under the wrong profile_id would update another profile's tile.
    require_tile_in_profile(state, profile_id, tile_id).await?;
    let body: UpdateTileFormationBody = parse_body(req).await?;

    let tile = state
        .tile_layout_repo
        .update_formation(
            tile_id,
            UpdateTileFormation {
                col: body.col,
                row: body.row,
                col_span: body.col_span,
                row_span: body.row_span,
            },
        )
        .await?;

    Ok(Json(tile.into()))
}

/// DELETE /tile-profiles/{id}/tiles/{tile_id}
///
/// Cascades to any camera bindings for this tile.
#[handler]
pub async fn delete_tile(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (profile_id, tile_id) = parse_profile_and_tile_id(req)?;
    require_tile_in_profile(state, profile_id, tile_id).await?;
    state.tile_layout_repo.delete_formation(tile_id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Handlers: camera bindings --

/// GET /tile-profiles/{id}/bindings?site_id=<uuid>
#[handler]
pub async fn list_bindings(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<TileCameraBindingDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let profile_id = parse_id(req)?;
    let site_id = parse_site_id_query(req)?;
    let bindings = state
        .tile_layout_repo
        .list_bindings(profile_id, site_id)
        .await?;
    Ok(Json(
        bindings
            .into_iter()
            .map(TileCameraBindingDto::from)
            .collect(),
    ))
}

/// PUT /tile-profiles/{id}/tiles/{tile_id}/bindings/{site_id}
///
/// Upsert, matching the frontend's `upsertTileCameraBinding`.
#[handler]
pub async fn set_binding(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<TileCameraBindingDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (profile_id, tile_id, site_id) = parse_binding_path(req)?;
    let body: SetBindingBody = parse_body(req).await?;

    let binding = state
        .tile_layout_repo
        .set_binding(profile_id, tile_id, site_id, body.camera_id)
        .await?;

    Ok(Json(binding.into()))
}

/// DELETE /tile-profiles/{id}/tiles/{tile_id}/bindings/{site_id}
///
/// Clears the binding. A missing row means "unassigned".
#[handler]
pub async fn clear_binding(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (profile_id, tile_id, site_id) = parse_binding_path(req)?;
    state
        .tile_layout_repo
        .clear_binding(profile_id, tile_id, site_id)
        .await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Helpers --

fn parse_site_id_query(req: &mut Request) -> Result<Uuid, ApiError> {
    req.query::<String>("site_id")
        .ok_or_else(|| ApiError::bad_request("missing required query parameter: site_id"))?
        .parse()
        .map_err(|_| ApiError::bad_request("invalid site_id: expected UUID"))
}

fn parse_profile_and_tile_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let profile_id = parse_id(req)?;
    let tile_id: Uuid = req
        .param::<String>("tile_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid tile_id: expected UUID"))?;
    Ok((profile_id, tile_id))
}

fn parse_binding_path(req: &mut Request) -> Result<(Uuid, Uuid, Uuid), ApiError> {
    let (profile_id, tile_id) = parse_profile_and_tile_id(req)?;
    let site_id: Uuid = req
        .param::<String>("site_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid site_id: expected UUID"))?;
    Ok((profile_id, tile_id, site_id))
}

async fn require_tile_in_profile(
    state: &AppState,
    profile_id: Uuid,
    tile_id: Uuid,
) -> Result<tile_formation::Model, ApiError> {
    state
        .tile_layout_repo
        .get_formation(tile_id)
        .await?
        .filter(|f| f.profile_id == profile_id)
        .ok_or_else(|| ApiError::not_found(format!("tile {tile_id} not found")))
}
