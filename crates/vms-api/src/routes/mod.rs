mod cameras;
mod health;

pub use cameras::{CameraDto, CreateCameraBody, UpdateCameraBody};

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
                            Router::with_path("recording").push(
                                Router::with_path("start").post(cameras::start_recording),
                            ).push(
                                Router::with_path("stop").post(cameras::stop_recording),
                            ),
                        ),
                ),
        )
}
