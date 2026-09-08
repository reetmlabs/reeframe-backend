mod auth;
mod cameras;
mod contact_lists;
mod contacts;
mod destinations;
mod discovery;
mod events;
mod export_jobs;
mod health;
mod metrics;
mod pipeline_edges;
mod pipeline_nodes;
mod pipeline_triggers;
mod pipelines;
mod recordings;
mod retention;
mod settings;
mod sources;
mod thumbnails;
mod tile_profiles;
mod users;
mod webhooks;

pub use auth::{AccessTokenResponse, AuthResponse, LoginBody, RefreshBody, SetupBody, UserDto};
pub use cameras::{CameraDto, CreateCameraBody, UpdateCameraBody};
pub use contact_lists::{ContactListDto, CreateContactListBody, UpdateContactListBody};
pub use contacts::{ContactDto, CreateContactBody, UpdateContactBody};
pub use destinations::{CreateDestinationBody, DestinationDto, UpdateDestinationBody};
pub use discovery::{DiscoveredDeviceDto, ResolvedStreamsDto};
pub use events::EventDto;
pub use export_jobs::{CreateExportBody, ExportJobDto};
pub use pipeline_edges::{CreateEdgeBody, UpdateEdgeBody};
pub use pipeline_nodes::{CreateNodeBody, UpdateNodeBody};
pub use pipeline_triggers::{CreateTriggerBody, UpdateTriggerBody};
pub use pipelines::{CreatePipelineBody, PipelineDto, UpdatePipelineBody};
pub use recordings::{DailyCoverageDto, PlaybackDto, RecordingDto};
pub use retention::{RetentionPolicyDto, UpdateRetentionPolicyBody};
pub use settings::SettingDto;
pub use sources::{CreateSourceBody, SourceDto, UpdateSourceBody};
pub use thumbnails::ThumbnailDto;
pub use tile_profiles::{
    AssignSiteBody, CreateTileFormationBody, CreateTileProfileBody, SetBindingBody,
    TileCameraBindingDto, TileFormationDto, TileProfileDto, UpdateTileFormationBody,
    UpdateTileProfileBody,
};
pub use users::{ApiKeyDto, CreateApiKeyBody, CreatedApiKeyDto};

use salvo::prelude::*;

use crate::{
    middleware::{AuthMiddleware, MetricsMiddleware},
    state::AppState,
};

/// Routes reachable without a valid access token: health, inbound webhooks
/// (which have their own accept/reject logic instead), and the three auth
/// endpoints whose entire purpose is obtaining or refreshing a token.
fn public_routes() -> Router {
    Router::new()
        .push(
            Router::with_path("health")
                .get(health::health)
                .push(Router::with_path("ready").get(health::ready)),
        )
        .push(Router::with_path("metrics").get(metrics::scrape))
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
                        )
                        .push(
                            Router::with_path("recordings")
                                .get(recordings::list_recordings)
                                .push(
                                    Router::with_path("daily-summary")
                                        .get(recordings::list_daily_summary),
                                )
                                .push(
                                    Router::with_path("{recording_id}").push(
                                        Router::with_path("thumbnails")
                                            .get(thumbnails::list_thumbnails),
                                    ),
                                ),
                        )
                        .push(Router::with_path("playback").get(recordings::get_playback))
                        .push(Router::with_path("events").get(events::list_events))
                        .push(
                            Router::with_path("retention-policy")
                                .get(retention::get_retention_policy)
                                .patch(retention::update_retention_policy),
                        )
                        .push(
                            Router::with_path("thumbnails").push(
                                Router::with_path("{filename}").get(thumbnails::get_thumbnail),
                            ),
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
            Router::with_path("tile-profiles")
                .get(tile_profiles::list_profiles)
                .post(tile_profiles::create_profile)
                .push(
                    Router::with_path("{id}")
                        .get(tile_profiles::get_profile)
                        .patch(tile_profiles::rename_profile)
                        .delete(tile_profiles::delete_profile)
                        .push(
                            Router::with_path("site-assignments")
                                .post(tile_profiles::assign_to_site),
                        )
                        .push(Router::with_path("bindings").get(tile_profiles::list_bindings))
                        .push(
                            Router::with_path("tiles")
                                .get(tile_profiles::list_tiles)
                                .post(tile_profiles::create_tile)
                                .push(
                                    Router::with_path("{tile_id}")
                                        .get(tile_profiles::get_tile)
                                        .patch(tile_profiles::update_tile)
                                        .delete(tile_profiles::delete_tile)
                                        .push(
                                            Router::with_path("bindings").push(
                                                Router::with_path("{site_id}")
                                                    .put(tile_profiles::set_binding)
                                                    .delete(tile_profiles::clear_binding),
                                            ),
                                        ),
                                ),
                        ),
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
            Router::with_path("contacts")
                .get(contacts::list_contacts)
                .post(contacts::create_contact)
                .push(
                    Router::with_path("{id}")
                        .get(contacts::get_contact)
                        .patch(contacts::update_contact)
                        .delete(contacts::delete_contact),
                ),
        )
        .push(
            Router::with_path("contact-lists")
                .get(contact_lists::list_contact_lists)
                .post(contact_lists::create_contact_list)
                .push(
                    Router::with_path("{id}")
                        .get(contact_lists::get_contact_list)
                        .patch(contact_lists::update_contact_list)
                        .delete(contact_lists::delete_contact_list)
                        .push(
                            Router::with_path("members")
                                .get(contact_lists::list_members)
                                .push(
                                    Router::with_path("{contact_id}")
                                        .post(contact_lists::add_member)
                                        .delete(contact_lists::remove_member),
                                ),
                        ),
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
                            Router::with_path("validation").get(pipelines::get_pipeline_validation),
                        )
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
        .push(
            Router::with_path("recordings")
                .push(Router::with_path("export").post(export_jobs::create_export))
                .push(Router::with_path("daily-summary").get(recordings::list_daily_summary_bulk))
                .push(
                    Router::with_path("{id}")
                        .push(Router::with_path("stream").get(recordings::stream_recording)),
                ),
        )
        .push(
            Router::with_path("export-jobs").push(
                Router::with_path("{id}")
                    .get(export_jobs::get_export_job)
                    .push(Router::with_path("download").get(export_jobs::download_export)),
            ),
        )
        .push(
            Router::with_path("system")
                .push(
                    Router::with_path("settings")
                        .get(settings::list_settings)
                        .patch(settings::update_settings),
                )
                .push(Router::with_path("config-file").post(settings::upload_config_file)),
        )
        .push(
            Router::with_path("discovery").push(
                Router::with_path("onvif")
                    .push(Router::with_path("probe").post(discovery::probe))
                    .push(Router::with_path("resolve").post(discovery::resolve)),
            ),
        )
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .hoop(MetricsMiddleware::new(state.metrics.clone()))
        .hoop(salvo::affix_state::inject(state))
        .push(public_routes())
        .push(protected_routes())
}
