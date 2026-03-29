use salvo::prelude::*;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use crate::entities::feed;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Extractible, Debug)]
#[salvo(extract(default_source = "body"))]
pub struct CreateFeedRequest {
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub parameters: Option<String>,
}

#[handler]
pub async fn create_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let new_feed = req.parse_json::<CreateFeedRequest>().await.expect("Failed to parse request");

    let feed_active = feed::ActiveModel {
        name: Set(new_feed.name),
        description: Set(new_feed.description),
        rtsp_url: Set(new_feed.rtsp_url),
        parameters: Set(new_feed.parameters),
        ..Default::default()
    };

    match feed_active.insert(db).await {
        Ok(f) => res.render(Json(f)),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error inserting feed: {}", e));
        }
    }
}

#[handler]
pub async fn get_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();

    match feed::Entity::find_by_id(id).one(db).await {
        Ok(Some(f)) => res.render(Json(f)),
        Ok(None) => res.status_code(StatusCode::NOT_FOUND),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

#[handler]
pub async fn update_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();
    let updated_feed = req.parse_json::<CreateFeedRequest>().await.expect("Failed to parse request");

    match feed::Entity::find_by_id(id).one(db).await {
        Ok(Some(f)) => {
            let mut active: feed::ActiveModel = f.into();
            active.name = Set(updated_feed.name);
            active.description = Set(updated_feed.description);
            active.rtsp_url = Set(updated_feed.rtsp_url);
            active.parameters = Set(updated_feed.parameters);

            match active.update(db).await {
                Ok(f) => res.render(Json(f)),
                Err(e) => {
                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render(format!("Error updating feed: {}", e));
                }
            }
        }
        Ok(None) => res.status_code(StatusCode::NOT_FOUND),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

#[handler]
pub async fn delete_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();

    match feed::Entity::delete_by_id(id).exec(db).await {
        Ok(_) => res.render("Deleted"),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error deleting feed: {}", e));
        }
    }
}
