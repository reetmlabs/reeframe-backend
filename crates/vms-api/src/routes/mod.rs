mod cameras;
mod destinations;
mod health;
mod pipelines;
mod sources;
mod webhooks;

pub use cameras::{CameraDto, CreateCameraBody, UpdateCameraBody};
pub use destinations::{CreateDestinationBody, DestinationDto, UpdateDestinationBody};
pub use pipelines::{CreatePipelineBody, PipelineDto, UpdatePipelineBody};
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
                        )
                        .push(
                            Router::with_path("relay")
                                .push(Router::with_path("start").post(cameras::start_relay))
                                .push(Router::with_path("stop").post(cameras::stop_relay)),
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
            Router::with_path("webhooks")
                .push(Router::with_path("{id}").post(webhooks::receive_webhook)),
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
        .push(
            Router::with_path("pipelines")
                .get(pipelines::list_pipelines)
                .post(pipelines::create_pipeline)
                .push(
                    Router::with_path("{id}")
                        .get(pipelines::get_pipeline)
                        .patch(pipelines::update_pipeline)
                        .delete(pipelines::delete_pipeline)
                        .push(Router::with_path("enable").post(pipelines::enable_pipeline))
                        .push(Router::with_path("disable").post(pipelines::disable_pipeline))
                        .push(Router::with_path("trigger").post(pipelines::trigger_pipeline))
                        .push(
                            Router::with_path("runs")
                                .get(pipelines::list_runs)
                                .push(Router::with_path("{run_id}").get(pipelines::get_run)),
                        ),
                ),
        )
}
