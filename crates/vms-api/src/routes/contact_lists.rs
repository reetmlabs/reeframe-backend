//! Contact list CRUD + membership — `/contact-lists[/{id}][/members[/{contact_id}]]`.
//!
//! CRUD mirrors `destinations.rs`'s pattern exactly. Membership is a plain
//! join-table toggle (`add_member`/`remove_member` in
//! `vms_db::repos::contact_list`) — `POST` is idempotent (adding an existing
//! member is a no-op), `DELETE` never 404s on a membership that's already
//! gone, matching how every other idempotent-toggle endpoint in this API
//! behaves.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::contact_list,
    repos::contact_list::{CreateContactList, UpdateContactList},
};

use super::contacts::ContactDto;
use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- DTOs --

#[derive(Serialize)]
pub struct ContactListDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<contact_list::Model> for ContactListDto {
    fn from(m: contact_list::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateContactListBody {
    pub name: String,
    pub description: Option<String>,
}

/// All fields optional — only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateContactListBody {
    pub name: Option<String>,
    pub description: Option<String>,
}

// -- Handlers: CRUD --

/// GET /contact-lists
#[handler]
pub async fn list_contact_lists(depot: &mut Depot) -> Result<Json<Vec<ContactListDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let lists = state.contact_list_repo.list().await?;
    Ok(Json(lists.into_iter().map(ContactListDto::from).collect()))
}

/// POST /contact-lists
#[handler]
pub async fn create_contact_list(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<ContactListDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateContactListBody = parse_body(req).await?;

    let input = CreateContactList {
        name: body.name,
        description: body.description,
    };

    let list = state.contact_list_repo.create(input).await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(ContactListDto::from(list)))
}

/// GET /contact-lists/{id}
#[handler]
pub async fn get_contact_list(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<ContactListDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let list = state
        .contact_list_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("contact list {id} not found")))?;
    Ok(Json(ContactListDto::from(list)))
}

/// PATCH /contact-lists/{id}
#[handler]
pub async fn update_contact_list(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<ContactListDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateContactListBody = parse_body(req).await?;

    let input = UpdateContactList {
        name: body.name,
        description: body.description.map(Some),
    };

    let list = state.contact_list_repo.update(id, input).await?;
    Ok(Json(ContactListDto::from(list)))
}

/// DELETE /contact-lists/{id}
#[handler]
pub async fn delete_contact_list(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    state.contact_list_repo.delete(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Handlers: membership --

/// GET /contact-lists/{id}/members
#[handler]
pub async fn list_members(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<ContactDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let members = state.contact_list_repo.list_members(id).await?;
    Ok(Json(members.into_iter().map(ContactDto::from).collect()))
}

/// POST /contact-lists/{id}/members/{contact_id}
#[handler]
pub async fn add_member(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (list_id, contact_id) = parse_list_and_contact_id(req)?;
    state
        .contact_list_repo
        .add_member(list_id, contact_id)
        .await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// DELETE /contact-lists/{id}/members/{contact_id}
#[handler]
pub async fn remove_member(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (list_id, contact_id) = parse_list_and_contact_id(req)?;
    state
        .contact_list_repo
        .remove_member(list_id, contact_id)
        .await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Helpers --

fn parse_list_and_contact_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let list_id = parse_id(req)?;
    let contact_id: Uuid = req
        .param::<String>("contact_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid contact_id: expected UUID"))?;
    Ok((list_id, contact_id))
}
