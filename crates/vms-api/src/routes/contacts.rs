//! Contact CRUD — `/contacts[/{id}]`.
//!
//! Mirrors `destinations.rs`'s pattern exactly, minus credential
//! encryption/masking: a contact's `extra` field is free-form delivery
//! metadata (e.g. a Slack user ID), not a secret, so it round-trips as-is.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::contact,
    repos::contact::{CreateContact, UpdateContact},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- DTOs --

#[derive(Serialize)]
pub struct ContactDto {
    pub id: Uuid,
    pub name: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub telegram_chat_id: Option<i64>,
    pub extra: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<contact::Model> for ContactDto {
    fn from(m: contact::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            email: m.email,
            phone: m.phone,
            telegram_chat_id: m.telegram_chat_id,
            extra: m.extra,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateContactBody {
    pub name: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub telegram_chat_id: Option<i64>,
    pub extra: Option<serde_json::Value>,
}

/// All fields optional — only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateContactBody {
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub telegram_chat_id: Option<i64>,
    pub extra: Option<serde_json::Value>,
}

// -- Handlers --

/// GET /contacts
#[handler]
pub async fn list_contacts(depot: &mut Depot) -> Result<Json<Vec<ContactDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let contacts = state.contact_repo.list().await?;
    Ok(Json(contacts.into_iter().map(ContactDto::from).collect()))
}

/// POST /contacts
#[handler]
pub async fn create_contact(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<ContactDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateContactBody = parse_body(req).await?;

    let input = CreateContact {
        name: body.name,
        email: body.email,
        phone: body.phone,
        telegram_chat_id: body.telegram_chat_id,
        extra: body.extra.unwrap_or_else(|| serde_json::json!({})),
    };

    let contact = state.contact_repo.create(input).await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(ContactDto::from(contact)))
}

/// GET /contacts/{id}
#[handler]
pub async fn get_contact(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<ContactDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let contact = state
        .contact_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("contact {id} not found")))?;
    Ok(Json(ContactDto::from(contact)))
}

/// PATCH /contacts/{id}
#[handler]
pub async fn update_contact(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<ContactDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateContactBody = parse_body(req).await?;

    let input = UpdateContact {
        name: body.name,
        email: body.email.map(Some),
        phone: body.phone.map(Some),
        telegram_chat_id: body.telegram_chat_id.map(Some),
        extra: body.extra,
    };

    let contact = state.contact_repo.update(id, input).await?;
    Ok(Json(ContactDto::from(contact)))
}

/// DELETE /contacts/{id}
#[handler]
pub async fn delete_contact(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    state.contact_repo.delete(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}
