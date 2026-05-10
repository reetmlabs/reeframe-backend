mod cameras;
mod destinations;
mod health;
mod sources;

pub use cameras::{CameraDto, CreateCameraBody, UpdateCameraBody};
pub use destinations::{CreateDestinationBody, DestinationDto, UpdateDestinationBody};
pub use sources::{CreateSourceBody, SourceDto, UpdateSourceBody};

use salvo::prelude::*;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .hoop(salvo::affix_state::inject(state))
        .push(Router::with_path("health").get(health::health))
        .push(
            Router::with_path("cameras")
                .get(cameras::list_cameras)
                .post(cameras::create_camera)
                .push(
                    Router::with_path("{id}")
                        .get(cameras::get_camera)
                        .patch(cameras::update_camera)
                        .delete(cameras::delete_camera)
                        .push(
                            Router::with_path("recording")
                                .push(Router::with_path("start").post(cameras::start_recording))
                                .push(Router::with_path("stop").post(cameras::stop_recording)),
                        ),
                ),
        )
        .push(
            Router::with_path("sources")
                .get(sources::list_sources)
                .post(sources::create_source)
                .push(
                    Router::with_path("{id}")
                        .get(sources::get_source)
                        .patch(sources::update_source)
                        .delete(sources::delete_source),
                ),
        )
        .push(
            Router::with_path("destinations")
                .get(destinations::list_destinations)
                .post(destinations::create_destination)
                .push(
                    Router::with_path("{id}")
                        .get(destinations::get_destination)
                        .patch(destinations::update_destination)
                        .delete(destinations::delete_destination),
                ),
        )
}
