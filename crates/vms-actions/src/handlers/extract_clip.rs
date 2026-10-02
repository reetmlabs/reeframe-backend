use std::time::Duration;

use tokio::time::sleep;
use vms_core::{
    action::ExtractClipConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

use crate::dispatcher::ActionContext;

/// Extract a clip from the ring buffer around the moment the pipeline fired.
///
/// The extraction window is `[event_pts - pre_event_secs, event_pts + post_event_secs]`
/// where `event_pts` is the latest buffered PTS at the time of execution, used
/// as "now" in pipeline time. The post-event footage doesn't exist yet at that
/// instant, so the handler sleeps for `post_event_secs` before reading the
/// buffer. Without the wait the post-event side of the window would be empty.
///
/// # Limitations
///
/// - The ring buffer must be running for the target camera. If it is not,
///   the handler returns [`NodeOutput::failure`].
/// - `use_manual_range = true` is not yet supported. When set, it falls back
///   to the latest-PTS behaviour with a warning in the run log.
pub async fn execute(
    node_id: NodeId,
    cfg: &ExtractClipConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    // -- Resolve camera_id --
    let Some(camera_id) = cfg.camera_id.or(input.trigger_ctx.camera_id) else {
        return NodeOutput::failure(
            node_id,
            "extract_clip: no camera_id in config or trigger context",
        );
    };

    // -- Resolve ring buffer --
    let Some(ring_buffer) = ctx.ring_buffer.as_ref() else {
        return NodeOutput::failure(node_id, "extract_clip: ring buffer manager not configured");
    };

    if cfg.use_manual_range {
        tracing::warn!(
            node_id = %node_id,
            "extract_clip: use_manual_range is not yet supported; falling back to latest-PTS mode"
        );
    }

    // -- Anchor the extraction window to the latest buffered PTS --
    let event_pts = ring_buffer.latest_pts(camera_id).unwrap_or(Duration::ZERO);

    // Let the post-event footage be captured first. The window stays anchored
    // to `event_pts` above; the latest PTS is not re-read after the sleep.
    if cfg.post_event_secs > 0 {
        sleep(Duration::from_secs(cfg.post_event_secs.into())).await;
    }

    tracing::debug!(
        node_id = %node_id,
        camera_id = %camera_id,
        pre  = cfg.pre_event_secs,
        post = cfg.post_event_secs,
        event_pts_secs = event_pts.as_secs(),
        "ExtractClip: extracting"
    );

    match ring_buffer
        .extract_clip(
            camera_id,
            cfg.pre_event_secs,
            cfg.post_event_secs,
            event_pts,
            &ctx.recording_dir,
        )
        .await
    {
        Ok(path) => {
            tracing::info!(node_id = %node_id, path = %path.display(), "ExtractClip: done");
            NodeOutput::success(node_id).with_artifact(path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("extract_clip: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vms_core::TriggerContext;
    use vms_media::{MediaConfig, MediaManager, RingBufferManager};

    use super::*;

    fn ring_buffer_manager() -> Arc<RingBufferManager> {
        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let (chunk_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let (live_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let config = MediaConfig {
            recording_dir: std::env::temp_dir(),
            rtsp_bind: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let media = MediaManager::new(config, event_tx, chunk_tx, live_tx)
            .expect("MediaManager::new should succeed with a scratch recording dir");
        RingBufferManager::new(Arc::new(media))
    }

    fn config(pre: u32, post: u32) -> ExtractClipConfig {
        ExtractClipConfig {
            pre_event_secs: pre,
            post_event_secs: post,
            format: "mp4".into(),
            camera_id: Some(uuid::Uuid::new_v4()),
            use_manual_range: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_post_event_secs_before_extracting() {
        let cfg = config(5, 30);
        let input = NodeInput {
            parent_outputs: vec![],
            trigger_ctx: TriggerContext::for_schedule(uuid::Uuid::new_v4(), uuid::Uuid::new_v4()),
        };
        let ctx = ActionContext {
            ring_buffer: Some(ring_buffer_manager()),
            ..Default::default()
        };

        let started = tokio::time::Instant::now();
        // No ring buffer was started for this camera, so extraction fails
        // right after the wait. This test only checks that the wait happens first.
        let out = execute(uuid::Uuid::new_v4(), &cfg, &input, &ctx).await;

        assert!(!out.success);
        assert!(started.elapsed() >= Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_wait_when_post_event_secs_is_zero() {
        let cfg = config(5, 0);
        let input = NodeInput {
            parent_outputs: vec![],
            trigger_ctx: TriggerContext::for_schedule(uuid::Uuid::new_v4(), uuid::Uuid::new_v4()),
        };
        let ctx = ActionContext {
            ring_buffer: Some(ring_buffer_manager()),
            ..Default::default()
        };

        let started = tokio::time::Instant::now();
        let out = execute(uuid::Uuid::new_v4(), &cfg, &input, &ctx).await;

        assert!(!out.success);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
