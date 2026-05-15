//! Dynamic `queue → appsink` branch attached to a live camera tee.
//!
//! GStreamer allows new branches to be added to a `tee` element while the
//! pipeline is in `Playing` state. The attach/detach functions here follow the
//! standard pattern:
//!
//! - **Attach**: request a new `src_%u` pad from the tee, create and link the
//!   branch elements, sync their state with the running pipeline.
//! - **Detach**: block the tee src pad via a downstream probe; inside the probe
//!   callback (GStreamer streaming thread) unlink and remove the elements; after
//!   the probe fires, release the tee request pad from a short-lived std thread.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gstreamer::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

use crate::ring_buffer::{RingBuffer, TimestampedFrame};

// ── Element name helpers ──────────────────────────────────────────────────────

pub(crate) fn queue_name(id: Uuid) -> String {
    format!("cam_{}_rbqueue", id.as_simple())
}

pub(crate) fn sink_name(id: Uuid) -> String {
    format!("cam_{}_rbsink", id.as_simple())
}

fn tee_name(id: Uuid) -> String {
    format!("cam_{}_tee", id.as_simple())
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Attach a `queue → appsink` branch to the live tee of camera `camera_id`.
///
/// Every encoded frame that arrives at the tee is pushed into `ring_buffer`
/// via the appsink's `new-sample` callback. The callback runs on a GStreamer
/// streaming thread and holds the `Mutex` only for the duration of one `push`.
///
/// Safe to call while the pipeline is `Playing`.
pub fn attach(
    pipeline:    &gstreamer::Pipeline,
    camera_id:   Uuid,
    ring_buffer: Arc<Mutex<RingBuffer>>,
) -> Result<(), VmsError> {
    let tee = pipeline
        .by_name(&tee_name(camera_id))
        .ok_or_else(|| VmsError::Media(format!("tee not found for camera {camera_id}")))?;

    // ── queue ─────────────────────────────────────────────────────────────────
    let queue = gstreamer::ElementFactory::make("queue")
        .name(&queue_name(camera_id))
        .property("max-size-buffers", 60u32) // ~2 s at 30 fps
        .property("max-size-bytes",   0u32)
        .property("max-size-time",    0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("ring buffer queue: {e}")))?;

    // ── appsink ───────────────────────────────────────────────────────────────
    // `drop = true` so a slow ring-buffer lock never stalls the recording branch.
    // `sync = false` so the appsink processes frames as fast as they arrive.
    let appsink = gstreamer_app::AppSink::builder()
        .name(&sink_name(camera_id))
        .drop(true)
        .max_buffers(30u32)
        .sync(false)
        .build();

    // ── Wire the appsink callback ─────────────────────────────────────────────
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;

                let pts = buffer
                    .pts()
                    .map(|t| Duration::from_nanos(t.nseconds()))
                    .unwrap_or(Duration::ZERO);

                let map = buffer.map_readable().map_err(|_| gstreamer::FlowError::Error)?;
                let data: Arc<[u8]> = Arc::from(map.as_slice());
                drop(map);

                ring_buffer
                    .lock()
                    .expect("ring buffer mutex poisoned")
                    .push(TimestampedFrame { pts, data });

                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    // ── Add elements to the pipeline ──────────────────────────────────────────
    pipeline
        .add(&queue)
        .map_err(|e| VmsError::Media(format!("add ring buffer queue: {e}")))?;
    pipeline
        .add(&appsink)
        .map_err(|e| VmsError::Media(format!("add ring buffer appsink: {e}")))?;

    // ── Link tee → queue → appsink ────────────────────────────────────────────
    let tee_src = tee
        .request_pad_simple("src_%u")
        .ok_or_else(|| VmsError::Media(format!("tee src pad request failed for camera {camera_id}")))?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("ring buffer queue has no sink pad".into()))?;

    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee→rbqueue: {e}")))?;
    queue
        .link(&appsink)
        .map_err(|e| VmsError::Media(format!("link rbqueue→appsink: {e}")))?;

    // ── Bring new elements to the pipeline's current state ────────────────────
    for el in [&queue, appsink.upcast_ref::<gstreamer::Element>()] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync ring buffer state: {e}")))?;
    }

    tracing::info!(camera_id = %camera_id, "Ring buffer branch attached");
    Ok(())
}

/// Detach the ring-buffer branch from camera `camera_id`'s tee.
///
/// Adds a blocking probe on the tee src pad. When the probe fires (next frame),
/// the branch elements are unlinked, set to `Null`, and removed from the
/// pipeline on the GStreamer streaming thread. A short-lived `std::thread` then
/// releases the tee request pad once the probe has completed.
///
/// Returns immediately — cleanup is asynchronous. Safe to call while `Playing`.
/// If no branch is attached for this camera, this is a no-op.
pub fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(queue)   = pipeline.by_name(&queue_name(camera_id)) else { return Ok(()) };
    let Some(appsink) = pipeline.by_name(&sink_name(camera_id))  else { return Ok(()) };
    let Some(tee)     = pipeline.by_name(&tee_name(camera_id))   else { return Ok(()) };

    // Locate the tee src pad connected to our queue's sink pad.
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("ring buffer queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("ring buffer queue sink has no peer pad".into()))?;

    // Block the tee src pad so no data flows while we tear down the branch.
    // Cleanup runs inside the probe (GStreamer streaming thread). The tee request
    // pad is released from a short-lived std thread once the probe signals done.
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    let pipeline_clone   = pipeline.clone();
    let queue_sink_clone = queue_sink.clone();
    let queue_clone      = queue.clone();
    let appsink_clone    = appsink.clone();

    tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();

        queue_clone.set_state(gstreamer::State::Null).ok();
        appsink_clone.set_state(gstreamer::State::Null).ok();

        pipeline_clone.remove(&queue_clone).ok();
        pipeline_clone.remove(&appsink_clone).ok();

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    // Release the tee request pad from a std thread after the probe fires.
    // We must not call release_request_pad from inside the probe callback —
    // it can deadlock because release_request_pad acquires the element lock
    // that the streaming thread already holds.
    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {
                tee.release_request_pad(&tee_src_clone);
                tracing::info!(camera_id = %camera_id, "Ring buffer branch detached");
            }
            Err(_) => {
                tracing::warn!(camera_id = %camera_id, "Ring buffer detach timed out waiting for probe");
            }
        }
    });

    Ok(())
}
