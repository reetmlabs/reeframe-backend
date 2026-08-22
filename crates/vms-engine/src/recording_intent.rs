//! Reconciles persisted recording intent (`cameras.desired_recording`)
//! against reality. `MediaManager::is_recording` alone can't survive a
//! restart, and `ResourceManager::recover()` only re-acquires pipelines for
//! cameras referenced by an enabled automation pipeline — a manually-started
//! recording is invisible to both. This is the piece
//! that resumes it anyway: called both from `MediaManager`'s
//! `pipeline_live_tx` notification (event-driven, fires the moment a
//! pipeline comes up or reconnects) and from a periodic sweep, as a
//! best-effort safety net for whatever the event misses.

use uuid::Uuid;
use vms_db::CameraRepo;
use vms_media::MediaManager;

/// If `camera_id` wants to be recording and isn't, resolve its RTSP URL(s)
/// and attach recording. No-op if it's already recording, disabled (mirrors
/// the gate in `POST /cameras/{id}/recording/start`), doesn't want to be
/// recording, or no longer exists. Failures are logged, never propagated —
/// this is always a best-effort reconciliation pass, never a request a
/// caller is blocked on.
pub async fn reconcile_recording_intent(
    camera_repo: &CameraRepo,
    media_manager: &MediaManager,
    camera_id: Uuid,
) {
    if media_manager.is_recording(camera_id) {
        return;
    }

    let (camera, password) = match camera_repo.get_decrypted(camera_id).await {
        Ok(Some(found)) => found,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(camera_id = %camera_id, error = %e, "Recording intent: failed to load camera");
            return;
        }
    };

    if !camera.enabled || !camera.desired_recording {
        return;
    }

    let rtsp_url = build_rtsp_url(
        &camera.rtsp_url,
        camera.username.as_deref(),
        password.as_deref(),
    );
    let sub_rtsp_url = camera
        .sub_rtsp_url
        .as_deref()
        .map(|sub| build_rtsp_url(sub, camera.username.as_deref(), password.as_deref()));

    match media_manager
        .start_recording(camera_id, &rtsp_url, sub_rtsp_url.as_deref())
        .await
    {
        Ok(()) => {
            tracing::info!(camera_id = %camera_id, "Recording intent reconciled — recording resumed")
        }
        Err(e) => {
            tracing::warn!(camera_id = %camera_id, error = %e, "Recording intent: failed to resume recording")
        }
    }
}

/// Inject credentials into an RTSP URL if both username and password are present.
/// `rtsp://host/path` + (user, pass) -> `rtsp://user:pass@host/path`
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    if let (Some(u), Some(p)) = (username, password) {
        if let Some(rest) = base_url.strip_prefix("rtsp://") {
            return format!("rtsp://{u}:{p}@{rest}");
        }
    }
    base_url.to_string()
}
