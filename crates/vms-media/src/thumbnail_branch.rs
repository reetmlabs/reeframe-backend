//! Periodic thumbnail capture branch attached to a live camera tee — an
//! interval-driven, indexed extension of `manager.rs`'s one-shot
//! `capture_snapshot` branch, kept attached instead of detached after one
//! frame. One JPEG file per capture, named by its own timestamp.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::VmsError;

/// JPEG encode quality (0–100). Not currently configurable — nothing in
/// the roadmap step asked for it, only the capture interval.
const JPEG_QUALITY: i32 = 75;

fn queue_name(id: Uuid) -> String {
    format!("cam_{}_thumbqueue", id.as_simple())
}
fn decode_name(id: Uuid) -> String {
    format!("cam_{}_thumbdecode", id.as_simple())
}
fn convert_name(id: Uuid) -> String {
    format!("cam_{}_thumbconvert", id.as_simple())
}
fn encoder_name(id: Uuid) -> String {
    format!("cam_{}_thumbenc", id.as_simple())
}
fn sink_name(id: Uuid) -> String {
    format!("cam_{}_thumbsink", id.as_simple())
}

/// Handle to a running per-camera thumbnail-capture branch. Same shape as
/// `motion_branch::MotionHandle` — holds what [`stop`](Self::stop) needs
/// to detach the GStreamer elements itself.
pub struct ThumbnailHandle {
    pipeline: gstreamer::Pipeline,
    camera_id: Uuid,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl ThumbnailHandle {
    pub async fn stop(self) {
        if let Err(e) = detach(&self.pipeline, self.camera_id) {
            tracing::warn!(camera_id = %self.camera_id, error = %e, "Failed to detach thumbnail branch");
        }
        let _ = self.shutdown_tx.send(());
        self.task.await.ok();
    }
}

/// Attach a thumbnail-capture branch to `tee_name`'s tee on `pipeline`.
/// Writes one `{unix_ms}.jpg` file into `thumbnails_dir/cam_{id}/` at most
/// once per `interval`. Safe to call while the pipeline is `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    tee_name: &str,
    camera_id: Uuid,
    thumbnails_dir: PathBuf,
    interval: Duration,
) -> Result<ThumbnailHandle, VmsError> {
    let tee = pipeline.by_name(tee_name).ok_or_else(|| {
        VmsError::Media(format!("tee '{tee_name}' not found for camera {camera_id}"))
    })?;

    let queue = gstreamer::ElementFactory::make("queue")
        .name(queue_name(camera_id))
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail queue: {e}")))?;

    let decodebin = gstreamer::ElementFactory::make("decodebin")
        .name(decode_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail decodebin: {e}")))?;

    let convert = gstreamer::ElementFactory::make("videoconvert")
        .name(convert_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail videoconvert: {e}")))?;

    let encoder = gstreamer::ElementFactory::make("jpegenc")
        .name(encoder_name(camera_id))
        .property("quality", JPEG_QUALITY)
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail jpegenc: {e}")))?;

    let appsink = gstreamer_app::AppSink::builder()
        .name(sink_name(camera_id))
        .drop(true)
        .max_buffers(2u32)
        .sync(false)
        .build();

    pipeline
        .add_many([
            &queue,
            &decodebin,
            &convert,
            &encoder,
            appsink.upcast_ref::<gstreamer::Element>(),
        ])
        .map_err(|e| VmsError::Media(format!("thumbnail add_many: {e}")))?;

    gstreamer::Element::link_many([
        &convert,
        &encoder,
        appsink.upcast_ref::<gstreamer::Element>(),
    ])
    .map_err(|e| VmsError::Media(format!("thumbnail link chain: {e}")))?;

    queue
        .link(&decodebin)
        .map_err(|e| VmsError::Media(format!("thumbnail link queue->decodebin: {e}")))?;

    // Same dynamic-pad-added handling as `capture_snapshot`/`motion_branch`
    // — decodebin autoplugs off the tee's already-depayed elementary stream.
    let convert_weak = convert.downgrade();
    decodebin.connect_pad_added(move |_, src_pad| {
        let Some(caps) = src_pad.current_caps() else {
            return;
        };
        let Some(structure) = caps.structure(0) else {
            return;
        };
        if !structure.name().starts_with("video/") {
            return;
        }
        let Some(convert) = convert_weak.upgrade() else {
            return;
        };
        let Some(sink_pad) = convert.static_pad("sink") else {
            return;
        };
        if !sink_pad.is_linked() {
            src_pad.link(&sink_pad).ok();
        }
    });

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("thumbnail queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->thumbqueue: {e}")))?;

    // Appsink callback: throttle to `interval`, forward encoded JPEG bytes.
    // Encoding already happened in the pipeline (jpegenc) — nothing left
    // to do downstream but rate-limit and write the bytes to disk.
    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(2);
    let last_capture = Arc::new(Mutex::new(Instant::now() - interval));
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;

                {
                    let mut last = last_capture.lock().unwrap();
                    if last.elapsed() < interval {
                        return Ok(gstreamer::FlowSuccess::Ok);
                    }
                    *last = Instant::now();
                }

                let map = buffer
                    .map_readable()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let _ = frame_tx.try_send(map.as_slice().to_vec());
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    for el in [
        &queue,
        &decodebin,
        &convert,
        &encoder,
        appsink.upcast_ref::<gstreamer::Element>(),
    ] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync thumbnail element: {e}")))?;
    }

    let cam_dir = thumbnails_dir.join(format!("cam_{}", camera_id.as_simple()));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        if let Err(e) = tokio::fs::create_dir_all(&cam_dir).await {
            tracing::error!(camera_id = %camera_id, error = %e, "Failed to create thumbnail directory");
            return;
        }
        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else { break };
                    let path = cam_dir.join(format!("{}.jpg", chrono::Utc::now().timestamp_millis()));
                    if let Err(e) = tokio::fs::write(&path, &frame).await {
                        tracing::warn!(camera_id = %camera_id, path = %path.display(), error = %e, "Failed to write thumbnail");
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    tracing::info!(camera_id = %camera_id, tee_name, "Thumbnail capture branch attached");
    Ok(ThumbnailHandle {
        pipeline: pipeline.clone(),
        camera_id,
        shutdown_tx,
        task,
    })
}

/// Detach the thumbnail-capture branch from camera `camera_id`'s tee. Same
/// blocking-pad-probe pattern as `motion_branch::detach`. No-op if no
/// branch is attached.
fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(queue) = pipeline.by_name(&queue_name(camera_id)) else {
        return Ok(());
    };

    let names = [
        decode_name(camera_id),
        convert_name(camera_id),
        encoder_name(camera_id),
        sink_name(camera_id),
    ];
    let rest: Vec<gstreamer::Element> = names.iter().filter_map(|n| pipeline.by_name(n)).collect();

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("thumbnail queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("thumbnail queue sink has no peer pad".into()))?;
    let tee = tee_src
        .parent_element()
        .ok_or_else(|| VmsError::Media("thumbnail tee src pad has no parent element".into()))?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    let pipeline_clone = pipeline.clone();
    let queue_sink_clone = queue_sink.clone();
    let queue_clone = queue.clone();

    tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();

        queue_clone.set_state(gstreamer::State::Null).ok();
        pipeline_clone.remove(&queue_clone).ok();
        for el in &rest {
            el.set_state(gstreamer::State::Null).ok();
            pipeline_clone.remove(el).ok();
        }

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => tracing::info!(camera_id = %camera_id, "Thumbnail capture branch detached"),
            Err(_) => tracing::warn!(
                camera_id = %camera_id,
                "thumbnail detach probe timed out — releasing tee pad anyway",
            ),
        }
        tee.release_request_pad(&tee_src_clone);
    });

    Ok(())
}
