use salvo::prelude::*;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use crate::entities::settings;

#[handler]
pub async fn get_settings(dep: &mut Depot, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");

    match settings::Entity::find().one(db).await {
        Ok(Some(s)) => res.render(Json(s)),
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
            res.render("Settings not found");
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching settings: {}", e));
        }
    }
}

#[handler]
pub async fn update_settings(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let updated_settings = req.parse_json::<settings::Model>().await.expect("Failed to parse request");

    match settings::Entity::find().one(db).await {
        Ok(Some(s)) => {
            let mut active: settings::ActiveModel = s.into();
            active.recording_chunk_duration_mins = Set(updated_settings.recording_chunk_duration_mins);
            active.timezone = Set(updated_settings.timezone);
            active.ntp_server = Set(updated_settings.ntp_server);
            active.storage_path = Set(updated_settings.storage_path);
            active.reconnect_interval_secs = Set(updated_settings.reconnect_interval_secs);
            active.gst_latency_ms = Set(updated_settings.gst_latency_ms);
            active.gst_buffering_ms = Set(updated_settings.gst_buffering_ms);
            active.reserved_disk_space_mb = Set(updated_settings.reserved_disk_space_mb);

            match active.update(db).await {
                Ok(s) => res.render(Json(s)),
                Err(e) => {
                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render(format!("Error updating settings: {}", e));
                }
            }
        }
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
            res.render("Settings not found");
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching settings: {}", e));
        }
    }
}
