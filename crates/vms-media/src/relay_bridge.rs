//! Bridges a running pipeline's tee into an RTSP relay mount without a second
//! connection to the camera.
//!
//! Cameras typically allow two RTSP sessions, already used by the main and
//! sub-stream pipelines, so a relay cannot open its own `rtspsrc`. This module
//! taps the pipeline's tee (same attach pattern as
//! [`crate::ring_buffer_branch`] and [`crate::motion_branch`]) and forwards the
//! buffers into an `appsrc` inside the RTSP media's pipeline, obtained through
//! `gstreamer_rtsp_server`'s `media-configure` signal.
//!
//! Shape: `tee -> queue -> appsink` on our side feeds
//! `appsrc name=src is-live=true format=time -> [parse] -> [pay] -> pay0` on the
//! relay media's side, built from a launch string. With `set_shared(true)` one
//! media pipeline serves every viewer of a mount, so `media-configure` only
//! fires again after all viewers have left and a new one connects. The
//! `unprepared` signal tells us when the cached `appsrc` handle is stale.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gstreamer::prelude::*;
use gstreamer_app::AppSrc;
use gstreamer_rtsp_server::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

/// Handle to a running relay bridge: the tee tap on the source pipeline and
/// the RTSP mount factory it feeds.
pub struct RelayBridgeHandle {
    mount_path: String,
    queue_name: String,
    sink_name: String,
}

/// Launch string for the relay media's pipeline for `codec`, sourced from
/// `appsrc name=src`. Returns `None` for unsupported codecs.
fn appsrc_launch_str(codec: &str) -> Option<String> {
    // No `do-timestamp` here: `attach`'s appsink callback stamps each buffer
    // itself. Tapped buffers carry timestamps from the source pipeline's
    // clock, which is unrelated to this media's clock, and `do-timestamp=true`
    // produced a stuck, non-monotonic DTS.
    let s = match codec.to_uppercase().as_str() {
        "H264" => {
            "( appsrc name=src is-live=true format=time \
             ! h264parse config-interval=-1 \
             ! video/x-h264,stream-format=byte-stream,alignment=nal \
             ! rtph264pay name=pay0 pt=96 )"
        }
        "H265" | "HEVC" => {
            "( appsrc name=src is-live=true format=time \
             ! h265parse config-interval=-1 \
             ! video/x-h265,stream-format=byte-stream,alignment=nal \
             ! rtph265pay name=pay0 pt=96 )"
        }
        "JPEG" => {
            "( appsrc name=src is-live=true format=time \
             ! jpegparse \
             ! rtpjpegpay name=pay0 pt=26 )"
        }
        "AV1" => {
            "( appsrc name=src is-live=true format=time \
             ! av1parse \
             ! rtpav1pay name=pay0 pt=96 )"
        }
        _ => return None,
    };
    Some(s.to_owned())
}

/// Build and immediately tear down a throwaway H264 relay pipeline so
/// GStreamer's plugin lookup and caps negotiation are warm before any viewer
/// connects.
///
/// The first H264 relay pipeline built in a process makes the client report a
/// few "corrupt decoded frame" errors for under a second; later pipelines are
/// clean. Doing it at startup means no real client sees that.
pub(crate) fn warmup() {
    let Some(launch) = appsrc_launch_str("H264") else {
        return;
    };
    match gstreamer::parse::launch(&launch) {
        Ok(el) => {
            let pipeline = el.downcast::<gstreamer::Pipeline>().unwrap_or_else(|el| {
                let pipeline = gstreamer::Pipeline::new();
                pipeline.add(&el).ok();
                pipeline
            });
            pipeline.set_state(gstreamer::State::Ready).ok();
            pipeline.set_state(gstreamer::State::Null).ok();
        }
        Err(e) => {
            tracing::warn!(error = %e, "relay bridge warmup pipeline failed to build");
        }
    }
}

/// Attach a relay bridge for camera `camera_id`: taps `tee_name`'s tee on
/// `pipeline` and registers a shared `RTSPMediaFactory` at `mount_path` on
/// `mounts`, fed from that tap.
///
/// `branch_suffix` (e.g. `"relay"` or `"subrelay"`) keeps this branch's
/// element names distinct from other taps on the same tee (ring buffer,
/// motion detection). Safe to call while the pipeline is `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    tee_name: &str,
    camera_id: Uuid,
    branch_suffix: &str,
    mounts: &gstreamer_rtsp_server::RTSPMountPoints,
    mount_path: &str,
    codec: &str,
) -> Result<RelayBridgeHandle, VmsError> {
    let tee = pipeline.by_name(tee_name).ok_or_else(|| {
        VmsError::Media(format!("tee '{tee_name}' not found for camera {camera_id}"))
    })?;

    let id = camera_id.as_simple().to_string();
    let queue_name = format!("cam_{id}_{branch_suffix}queue");
    let sink_name = format!("cam_{id}_{branch_suffix}sink");

    let queue = gstreamer::ElementFactory::make("queue")
        .name(&queue_name)
        .property("max-size-buffers", 30u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("relay bridge queue: {e}")))?;

    let appsink = gstreamer_app::AppSink::builder()
        .name(&sink_name)
        .drop(true)
        .max_buffers(30u32)
        .sync(false)
        .build();

    pipeline
        .add_many([&queue, appsink.upcast_ref::<gstreamer::Element>()])
        .map_err(|e| VmsError::Media(format!("relay bridge add_many: {e}")))?;

    queue
        .link(&appsink)
        .map_err(|e| VmsError::Media(format!("link relayqueue->appsink: {e}")))?;

    // -- Build the relay media factory --
    let launch = appsrc_launch_str(codec)
        .ok_or_else(|| VmsError::Media(format!("relay bridge: unsupported codec '{codec}'")))?;

    let factory = gstreamer_rtsp_server::RTSPMediaFactory::new();
    factory.set_launch(&launch);
    factory.set_shared(true);

    // Set by `media-configure` when the first client connects and cleared by
    // `unprepared` when the shared media is torn down. While it is `None`,
    // the appsink callback drops samples.
    //
    // `started_at` is reset for each new appsrc, and every pushed buffer gets
    // PTS/DTS = time elapsed since then. Using appsrc's `do-timestamp` instead
    // gave a stuck DTS (ffmpeg: "non monotonically increasing dts"), likely
    // because the tapped buffers come from an unrelated clock domain. Elapsed
    // wall-clock time is always monotonic.
    //
    // `seen_keyframe` starts `false` per connection, and buffers are dropped
    // until a keyframe (`!DELTA_UNIT`) arrives. A decoder starting mid-GOP has
    // no reference frame and shows corrupt frames.
    struct AppsrcState {
        appsrc: AppSrc,
        started_at: std::time::Instant,
        seen_keyframe: bool,
        caps_set: bool,
    }
    let appsrc_slot: Arc<Mutex<Option<AppsrcState>>> = Arc::new(Mutex::new(None));

    let slot_for_configure = appsrc_slot.clone();
    let cam_id = camera_id;
    factory.connect_media_configure(move |_factory, media| {
        let Ok(bin) = media.element().downcast::<gstreamer::Bin>() else {
            tracing::error!(camera_id = %cam_id, "relay bridge: media element is not a Bin");
            return;
        };
        let Some(appsrc) = bin
            .by_name("src")
            .and_then(|e| e.downcast::<AppSrc>().ok())
        else {
            tracing::error!(camera_id = %cam_id, "relay bridge: appsrc 'src' not found in media bin");
            return;
        };
        appsrc.set_format(gstreamer::Format::Time);
        *slot_for_configure.lock().unwrap() = Some(AppsrcState {
            appsrc,
            started_at: std::time::Instant::now(),
            seen_keyframe: false,
            caps_set: false,
        });

        let slot_for_unprepared = slot_for_configure.clone();
        media.connect_unprepared(move |_media| {
            *slot_for_unprepared.lock().unwrap() = None;
        });
    });

    // -- Forward tapped buffers into whichever appsrc is currently active --
    let slot_for_sink = appsrc_slot;
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;

                if let Some(state) = slot_for_sink.lock().unwrap().as_mut() {
                    // Set caps once per connection. Each `AppSrc::set_caps` call
                    // sends a new caps event downstream, and doing it per buffer
                    // made the media's `h264parse` resync repeatedly and corrupt
                    // the first frames. The tap's caps do not change mid-session.
                    if !state.caps_set {
                        if let Some(caps) = sample.caps() {
                            state.appsrc.set_caps(Some(&caps.to_owned()));
                            state.caps_set = true;
                        }
                    }
                    if let Some(buffer) = sample.buffer() {
                        let is_keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
                        if !state.seen_keyframe {
                            if !is_keyframe {
                                return Ok(gstreamer::FlowSuccess::Ok);
                            }
                            state.seen_keyframe = true;
                        }

                        let pts = gstreamer::ClockTime::from_nseconds(
                            state.started_at.elapsed().as_nanos() as u64,
                        );
                        let mut owned = buffer.to_owned();
                        {
                            let buf_mut = owned.make_mut();
                            buf_mut.set_pts(pts);
                            buf_mut.set_dts(pts);
                        }
                        if let Err(e) = state.appsrc.push_buffer(owned) {
                            tracing::warn!(camera_id = %cam_id, error = %e, "relay bridge: push_buffer failed");
                        }
                    }
                }

                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    // Sink first, tee last, so no buffer reaches an element still in `Null`.
    for el in [appsink.upcast_ref::<gstreamer::Element>(), &queue] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync relay bridge element: {e}")))?;
    }
    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("relay bridge queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->relayqueue: {e}")))?;

    mounts.add_factory(mount_path, factory);

    tracing::info!(camera_id = %camera_id, mount_path, codec, "Relay bridge attached");
    Ok(RelayBridgeHandle {
        mount_path: mount_path.to_owned(),
        queue_name,
        sink_name,
    })
}

/// Detach a relay bridge.
///
/// Removes the mount factory (existing viewers keep their session, new ones
/// get "not found") and tears down the tee tap with the same blocking pad
/// probe as [`crate::ring_buffer_branch::detach`]. If the probe never fires,
/// for example because upstream died, the removal is forced so a later
/// `attach()` does not hit a name collision. Returns immediately; element
/// cleanup is asynchronous. Safe to call while `Playing`.
pub fn detach(
    pipeline: &gstreamer::Pipeline,
    mounts: &gstreamer_rtsp_server::RTSPMountPoints,
    camera_id: Uuid,
    handle: &RelayBridgeHandle,
) -> Result<(), VmsError> {
    mounts.remove_factory(&handle.mount_path);

    let Some(queue) = pipeline.by_name(&handle.queue_name) else {
        return Ok(());
    };
    let Some(appsink) = pipeline.by_name(&handle.sink_name) else {
        return Ok(());
    };

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("relay bridge queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("relay bridge queue sink has no peer pad".into()))?;
    let tee = tee_src
        .parent_element()
        .ok_or_else(|| VmsError::Media("relay bridge tee src pad has no parent element".into()))?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    let pipeline_clone = pipeline.clone();
    let queue_sink_clone = queue_sink.clone();
    let queue_clone = queue.clone();
    let appsink_clone = appsink.clone();

    let probe_id = tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();

        queue_clone.set_state(gstreamer::State::Null).ok();
        appsink_clone.set_state(gstreamer::State::Null).ok();

        pipeline_clone.remove(&queue_clone).ok();
        pipeline_clone.remove(&appsink_clone).ok();

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    let pipeline_clone2 = pipeline.clone();
    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        let fired = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        if fired {
            tracing::info!(camera_id = %camera_id, "Relay bridge detached");
        } else {
            // A BLOCK_DOWNSTREAM probe only fires when a buffer or event
            // crosses the pad, so it never fires if the camera connection is
            // dead. Without forced removal, `queue`/`appsink` would stay in
            // the pipeline under their fixed names and every later `attach()`
            // for this camera and quality would fail at `add_many` until the
            // pipeline is rebuilt. Five seconds with no frame means nothing
            // is flowing, so forcing the teardown is safe.
            tracing::warn!(
                camera_id = %camera_id,
                "relay bridge detach probe timed out, forcing removal directly",
            );
            if let Some(id) = probe_id {
                tee_src_clone.remove_probe(id);
            }
            if let Some(peer) = queue_sink.peer() {
                peer.unlink(&queue_sink).ok();
            }
            queue.set_state(gstreamer::State::Null).ok();
            appsink.set_state(gstreamer::State::Null).ok();
            pipeline_clone2.remove(&queue).ok();
            pipeline_clone2.remove(&appsink).ok();
        }
        tee.release_request_pad(&tee_src_clone);
    });

    Ok(())
}
