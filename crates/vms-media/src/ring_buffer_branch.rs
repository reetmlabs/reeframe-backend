//! Dynamic `appsink` branch attached to a live camera tee.
//!
//! GStreamer allows new branches to be added to a `tee` element while the
//! pipeline is in `Playing` state. The attach/detach functions here follow the
//! standard pattern:
//!
//! - Attach: request a new `src_%u` pad from the tee, create and link the
//!   branch elements, sync their state with the running pipeline.
//! - Detach: block the tee src pad via a downstream probe; inside the probe
//!   callback (GStreamer streaming thread) unlink and remove the elements; after
//!   the probe fires, release the tee request pad from a short-lived std thread.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gstreamer::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

use crate::ring_buffer::{RingBuffer, TimestampedFrame};

// -- Element name helpers --

pub(crate) fn sink_name(id: Uuid) -> String {
    format!("cam_{}_rbsink", id.as_simple())
}

fn tee_name(id: Uuid) -> String {
    format!("cam_{}_tee", id.as_simple())
}

// -- Public API --

/// Attach an `appsink` straight to the live tee of camera `camera_id`.
///
/// Every encoded frame that arrives at the tee is pushed into `ring_buffer`
/// via the appsink's `new-sample` callback, which runs on the tee's streaming
/// thread and so never blocks. While another holder has the `Mutex` (for
/// example `extract_clip` copying a clip), frames are dropped up to the next
/// keyframe, and the branch resumes there.
///
/// Safe to call while the pipeline is `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    camera_id: Uuid,
    ring_buffer: Arc<Mutex<RingBuffer>>,
) -> Result<(), VmsError> {
    let tee = pipeline
        .by_name(&tee_name(camera_id))
        .ok_or_else(|| VmsError::Media(format!("tee not found for camera {camera_id}")))?;

    // `sync = false` so the appsink processes frames as fast as they arrive.
    let appsink = gstreamer_app::AppSink::builder()
        .name(sink_name(camera_id))
        .sync(false)
        .build();

    let reporter = crate::branch_drop::DropReporter::new(camera_id, "ring_buffer");
    let mut skipping = false;
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;

                let is_keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
                if skipping && !is_keyframe {
                    reporter.dropped(1);
                    return Ok(gstreamer::FlowSuccess::Ok);
                }

                let pts = buffer
                    .pts()
                    .map(|t| Duration::from_nanos(t.nseconds()))
                    .unwrap_or(Duration::ZERO);
                let map = buffer
                    .map_readable()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let data: Arc<[u8]> = Arc::from(map.as_slice());
                drop(map);

                let Ok(mut rb) = ring_buffer.try_lock() else {
                    skipping = true;
                    reporter.dropped(1);
                    return Ok(gstreamer::FlowSuccess::Ok);
                };
                skipping = false;
                rb.push(TimestampedFrame {
                    pts,
                    data,
                    is_keyframe,
                });

                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    pipeline
        .add(&appsink)
        .map_err(|e| VmsError::Media(format!("add ring buffer appsink: {e}")))?;
    crate::camera_stream::sync_branch_state(appsink.upcast_ref())
        .map_err(|e| VmsError::Media(format!("sync ring buffer state: {e}")))?;

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let appsink_sink = appsink
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("ring buffer appsink has no sink pad".into()))?;
    tee_src
        .link(&appsink_sink)
        .map_err(|e| VmsError::Media(format!("link tee->rbsink: {e}")))?;

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
/// Returns immediately; cleanup is asynchronous. Safe to call while `Playing`.
/// If no branch is attached for this camera, this is a no-op.
pub fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(appsink) = pipeline.by_name(&sink_name(camera_id)) else {
        return Ok(());
    };
    let Some(tee) = pipeline.by_name(&tee_name(camera_id)) else {
        return Ok(());
    };

    // Locate the tee src pad connected to our appsink's sink pad.
    let appsink_sink = appsink
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("ring buffer appsink has no sink pad".into()))?;
    let tee_src = appsink_sink
        .peer()
        .ok_or_else(|| VmsError::Media("ring buffer appsink sink has no peer pad".into()))?;

    // Block the tee src pad so no data flows while we tear down the branch.
    // Cleanup runs inside the probe (GStreamer streaming thread). The tee request
    // pad is released from a short-lived std thread once the probe signals done.
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    let pipeline_clone = pipeline.clone();
    let appsink_sink_clone = appsink_sink.clone();
    let appsink_clone = appsink.clone();

    let probe_id = tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&appsink_sink_clone).ok();
        appsink_clone.set_state(gstreamer::State::Null).ok();
        pipeline_clone.remove(&appsink_clone).ok();

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    // Release the tee request pad from a std thread after the probe fires.
    // Calling release_request_pad inside the probe can deadlock, because it
    // takes the element lock the streaming thread already holds.
    let pipeline_clone2 = pipeline.clone();
    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        let fired = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        if fired {
            tracing::info!(camera_id = %camera_id, "Ring buffer branch detached");
        } else {
            // A BLOCK_DOWNSTREAM probe only fires when a buffer or event
            // crosses the pad, so it never fires if upstream is dead. Without
            // forced removal, `appsink` would stay in the pipeline
            // under its fixed name and every later `attach()` for this
            // camera would fail with a name collision. Five seconds with no
            // frame means nothing is flowing, so forcing the teardown is safe.
            tracing::warn!(
                camera_id = %camera_id,
                "detach probe timed out, forcing removal directly",
            );
            if let Some(id) = probe_id {
                tee_src_clone.remove_probe(id);
            }
            if let Some(peer) = appsink_sink.peer() {
                peer.unlink(&appsink_sink).ok();
            }
            appsink.set_state(gstreamer::State::Null).ok();
            pipeline_clone2.remove(&appsink).ok();
        }
        tee.release_request_pad(&tee_src_clone);
    });

    Ok(())
}
