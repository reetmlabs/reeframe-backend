//! Event-marker history for the scrub timeline (`GET /cameras/{id}/events`).

use chrono::{DateTime, FixedOffset};
use salvo::prelude::*;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    error::{parse_id, ApiError},
    routes::recordings::parse_query_datetime,
    state::AppState,
};

// -- Response DTO --

#[derive(Serialize)]
pub struct EventDto {
    pub id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<FixedOffset>,
    pub payload: serde_json::Value,
}

impl From<vms_db::entities::event::Model> for EventDto {
    fn from(m: vms_db::entities::event::Model) -> Self {
        Self {
            id: m.id,
            event_type: m.event_type,
            occurred_at: m.occurred_at,
            payload: m.payload,
        }
    }
}

// -- Handlers --

/// GET /cameras/{id}/events?from=<rfc3339>&to=<rfc3339>&type=<event_type>
///
/// Markers for the scrub timeline, oldest first. `type` is optional and repeatable.
#[handler]
pub async fn list_events(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<EventDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let camera_id = parse_id(req)?;
    let from = parse_query_datetime(req, "from")?;
    let to = parse_query_datetime(req, "to")?;

    if from >= to {
        return Err(ApiError::bad_request("'from' must be earlier than 'to'"));
    }

    let event_types: Vec<String> = req
        .queries()
        .get_vec("type")
        .map(|v| v.iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let events = state
        .events_repo
        .list_for_camera(
            camera_id,
            from.fixed_offset(),
            to.fixed_offset(),
            &event_types,
        )
        .await?;

    Ok(Json(events.into_iter().map(EventDto::from).collect()))
}
