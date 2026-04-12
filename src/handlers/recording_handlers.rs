use salvo::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use crate::entities::{feed, settings};
use crate::recorder::RecorderManager;
use std::time::Duration;

#[handler]
pub async fn start_recording(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let recorder_manager = dep.get::<RecorderManager>("recorder_manager").expect("Recorder manager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let settings = match settings::Entity::find().one(db).await {
        Ok(Some(s)) => s,
        _ => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Failed to fetch settings");
            return;
        }
    };

    match feed::Entity::find_by_id(feed_id).one(db).await {
        Ok(Some(f)) => {
            match recorder_manager.start_recording(&f, &settings) {
                Ok(_) => res.render(format!("Started recording for feed {}", f.id)),
                Err(e) => {
                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render(format!("Error starting recording: {}", e));
                }
            }
        }
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

#[handler]
pub async fn stop_recording(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let recorder_manager = dep.get::<RecorderManager>("recorder_manager").expect("Recorder manager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    match recorder_manager.stop_recording(feed_id) {
        Ok(_) => res.render(format!("Stopped recording for feed {}", feed_id)),
        Err(e) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(format!("Error stopping recording: {}", e));
        }
    }
}

#[handler]
pub async fn handle_ai_event(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let recorder_manager = dep.get::<RecorderManager>("recorder_manager").expect("Recorder manager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let settings = match settings::Entity::find().one(db).await {
        Ok(Some(s)) => s,
        _ => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Failed to fetch settings");
            return;
        }
    };

    match feed::Entity::find_by_id(feed_id).one(db).await {
        Ok(Some(f)) => {
            let duration_secs = f.ai_recording_duration_secs
                .unwrap_or(settings.default_ai_recording_duration_secs);
            
            match recorder_manager.trigger_ai_recording(&f, &settings, Duration::from_secs(duration_secs as u64)) {
                Ok(_) => res.render(format!("Triggered AI recording for feed {} for {} seconds", f.id, duration_secs)),
                Err(e) => {
                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render(format!("Error triggering AI recording: {}", e));
                }
            }
        }
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}
