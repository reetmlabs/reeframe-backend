mod auth;
mod cameras;
mod destinations;
mod health;
mod pipeline_edges;
mod pipeline_nodes;
mod pipeline_triggers;
mod pipelines;
mod sources;
mod users;
mod webhooks;

pub use auth::{AccessTokenResponse, AuthResponse, LoginBody, RefreshBody, SetupBody, UserDto};
pub use cameras::{CameraDto, CreateCameraBody, UpdateCameraBody};
pub use destinations::{CreateDestinationBody, DestinationDto, UpdateDestinationBody};
pub use pipeline_edges::{CreateEdgeBody, UpdateEdgeBody};
pub use pipeline_nodes::{CreateNodeBody, UpdateNodeBody};
pub use pipeline_triggers::{CreateTriggerBody, UpdateTriggerBody};
pub use pipelines::{CreatePipelineBody, PipelineDto, UpdatePipelineBody};
pub use sources::{CreateSourceBody, SourceDto, UpdateSourceBody};
pub use users::{ApiKeyDto, CreateApiKeyBody, CreatedApiKeyDto};

use salvo::prelude::*;

use crate::{middleware::AuthMiddleware, state::AppState};

/// Routes reachable without a valid access token: health, inbound webhooks
/// (step 8-4 has its own accept/reject logic instead), and the three auth
/// endpoints whose entire purpose is obtaining or refreshing a token.
fn public_routes() -> Router {
    Router::new()
        .push(Router::with_path("health").get(health::health))
        .push(
            Router::with_path("webhooks")
                .push(Router::with_path("{id}").post(webhooks::receive_webhook)),
        )
        .push(
            Router::with_path("auth")
                .push(Router::with_path("setup").post(auth::setup))
                .push(Router::with_path("login").post(auth::login))
                .push(Router::with_path("refresh").post(auth::refresh)),
        )
}

/// Everything else — gated behind [`AuthMiddleware`].
fn protected_routes() -> Router {
    Router::new()
        .hoop(AuthMiddleware)
        .push(Router::with_path("auth").push(Router::with_path("me").get(auth::me)))
        .push(
            Router::with_path("users").push(
                Router::with_path("{id}").push(
                    Router::with_path("api-keys")
                        .get(users::list_api_keys)
                        .post(users::create_api_key)
                        .push(Router::with_path("{key_id}").delete(users::delete_api_key)),
                ),
            ),
        )
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
                        )
                        .push(
                            Router::with_path("nodes")
                                .get(pipeline_nodes::list_nodes)
                                .post(pipeline_nodes::create_node)
                                .push(
                                    Router::with_path("{node_id}")
                                        .get(pipeline_nodes::get_node)
                                        .patch(pipeline_nodes::update_node)
                                        .delete(pipeline_nodes::delete_node),
                                ),
                        )
                        .push(
                            Router::with_path("edges")
                                .get(pipeline_edges::list_edges)
                                .post(pipeline_edges::create_edge)
                                .push(
                                    Router::with_path("{edge_id}")
                                        .get(pipeline_edges::get_edge)
                                        .patch(pipeline_edges::update_edge)
                                        .delete(pipeline_edges::delete_edge),
                                ),
                        )
                        .push(
                            Router::with_path("triggers")
                                .get(pipeline_triggers::list_triggers)
                                .post(pipeline_triggers::create_trigger)
                                .push(
                                    Router::with_path("{trigger_id}")
                                        .get(pipeline_triggers::get_trigger)
                                        .patch(pipeline_triggers::update_trigger)
                                        .delete(pipeline_triggers::delete_trigger),
                                ),
                        ),
                ),
        )
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .hoop(salvo::affix_state::inject(state))
        .push(public_routes())
        .push(protected_routes())
}
