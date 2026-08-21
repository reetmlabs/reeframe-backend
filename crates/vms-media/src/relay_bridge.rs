//! Bridges a running pipeline's tee into an RTSP relay mount — no second
//! connection to the camera.
//!
//! `RelayServer` previously gave each relay mount its own self-contained
//! `rtspsrc location=...` launch string, opening an independent camera
//! connection per quality. That doesn't scale to serving main *and*
//! sub-quality simultaneously within a camera's typical 2-session budget
//! (already spoken for by recording and the sub-stream pipeline). Instead,
//! this module taps the appropriate pipeline's tee (same attach pattern as
//! [`crate::ring_buffer_branch`]/[`crate::motion_branch`]) and forwards the
//! tapped buffers into an `appsrc` living inside the RTSP media's own
//! pipeline, via `gstreamer_rtsp_server`'s `media-configure` signal.
//!
//! Shape: `tee -> queue -> appsink` (this crate's side) feeding
//! `appsrc name=src is-live=true format=time -> [parse] -> [pay] -> pay0`
//! (the relay media's side, built from a launch string). `set_shared(true)`
//! means one such media pipeline instance is reused across every viewer of
//! that mount — `media-configure` only fires again after all viewers
//! disconnect and a new one connects (confirmed via `RTSPMedia`'s
//! `unprepared` signal, which this module uses to know its cached `appsrc`
//! handle is stale).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gstreamer::prelude::*;
use gstreamer_app::AppSrc;
use gstreamer_rtsp_server::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

/// Handle to a running relay bridge — the tee-tap on the source pipeline
/// plus the RTSP mount factory it feeds.
pub struct RelayBridgeHandle {
    mount_path: String,
    queue_name: String,
    sink_name: String,
}

/// Build the codec -> launch-string table for the relay media's own
/// pipeline. Same per-codec pay elements as the old `rtspsrc`-based launch
/// strings, but sourced from `appsrc name=src` instead of a second camera
/// connection.
fn appsrc_launch_str(codec: &str) -> Option<String> {
    // Timestamps are set explicitly in Rust before each `push_buffer` call
    // (see `attach`'s appsink callback) rather than via appsrc's own
    // `do-timestamp` — buffers arriving from the tee-tap carry PTS/DTS
    // stamped against the *source* pipeline's clock, which has no
    // relationship to this media's own independent pipeline clock, and
    // `do-timestamp=true` was tried first and produced a stuck,
    // non-monotonic DTS in practice. An explicit per-connection wall-clock
    // timestamp sidesteps that entirely.
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

/// Build and immediately tear down a throwaway `appsrc ! h264parse !
/// capsfilter ! rtph264pay` pipeline so GStreamer's element/plugin lookup
/// and caps-negotiation machinery is already warm before any real viewer
/// connects.
///
/// Confirmed live: the very first H264 relay pipeline built in this
/// process produces a handful of "corrupt decoded frame" errors on the
/// client for well under a second before self-healing — every pipeline
/// built afterwards (same process) is clean from frame one. Paying that
/// one-time cost here, at server startup with no viewer attached, means no
/// real client ever sees it.
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

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("relay bridge queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->relayqueue: {e}")))?;
    queue
        .link(&appsink)
        .map_err(|e| VmsError::Media(format!("link relayqueue->appsink: {e}")))?;

    // -- Build the relay media factory --
    let launch = appsrc_launch_str(codec)
        .ok_or_else(|| VmsError::Media(format!("relay bridge: unsupported codec '{codec}'")))?;

    let factory = gstreamer_rtsp_server::RTSPMediaFactory::new();
    factory.set_launch(&launch);
    factory.set_shared(true);

    // Populated by `media-configure` on first client connect, cleared by
    // `unprepared` once the last client disconnects and the shared media is
    // torn down. The appsink callback below only forwards buffers while
    // this is `Some` — with nobody watching, samples are just dropped.
    //
    // `started_at` is captured fresh each time a new appsrc is installed —
    // every pushed buffer gets PTS/DTS set to elapsed-time-since-that-moment
    // (computed here in Rust, not left to the appsrc's own `do-timestamp`).
    // `do-timestamp` was tried first and produced a stuck, non-monotonic
    // DTS in practice (confirmed live: ffmpeg reported "non monotonically
    // increasing dts... 295 >= 295" repeating) — plausibly an interaction
    // between the appsrc's internal clock/base-time tracking and buffers
    // arriving from a source with its own, unrelated clock domain. Explicit
    // per-connection wall-clock timestamps sidestep that entirely and are
    // trivially guaranteed monotonic.
    //
    // `seen_keyframe` starts `false` on every fresh connection and buffers
    // are dropped (not forwarded) until the tap delivers one — a decoder
    // starting mid-GOP has no reference frame for the P-slices it'd
    // otherwise receive first, and produces exactly the "corrupt decoded
    // frame" behavior confirmed live before this was added. Same
    // `!DELTA_UNIT` keyframe check `ring_buffer_branch.rs` already uses.
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
                    // Set once per connection, not per buffer — `AppSrc::set_caps`
                    // pushes a fresh caps/stream-start event downstream on every
                    // call, and doing that on every single buffer was observed
                    // live to corrupt the first several decoded frames of every
                    // new connection (the media's internal `h264parse` resyncing
                    // repeatedly right as a decoder is trying to lock on). The
                    // tap's caps don't change mid-session, so once is enough.
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

    for el in [&queue, appsink.upcast_ref::<gstreamer::Element>()] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync relay bridge element: {e}")))?;
    }

    mounts.add_factory(mount_path, factory);

    tracing::info!(camera_id = %camera_id, mount_path, codec, "Relay bridge attached");
    Ok(RelayBridgeHandle {
        mount_path: mount_path.to_owned(),
        queue_name,
        sink_name,
    })
}

/// Detach a relay bridge: removes the mount factory (existing viewers keep
/// their current session; new ones get "not found") and tears down the
/// tee-tap via the same blocking-pad-probe pattern as
/// [`crate::ring_buffer_branch::detach`], falling back to forcing the same
/// removal directly if the probe never fires (e.g. the tee has stopped
/// flowing data because the pipeline's upstream already died) — otherwise
/// the branch's elements are orphaned in the pipeline forever under their
/// fixed names, and every later `attach()` for this camera+quality fails.
/// Returns immediately — element cleanup is asynchronous either way. Safe
/// to call while `Playing`.
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
            // A BLOCK_DOWNSTREAM probe only fires when a buffer/event
            // actually tries to cross this pad — if the pipeline's upstream
            // (the camera connection) has already died, nothing ever will,
            // and the probe never fires. Giving up here without also
            // forcing removal left `queue`/`appsink` permanently stuck in
            // the pipeline under their fixed names — every later `attach()`
            // for this camera+quality then failed at `add_many` with a name
            // collision, forever, recoverable only by rebuilding the whole
            // pipeline (daemon restart; see the incident this fixes). Five
            // seconds without a single frame crossing a live tee tap is a
            // reliable enough signal that nothing is flowing through this
            // exact link for it to be safe to force the same teardown here.
            tracing::warn!(
                camera_id = %camera_id,
                "relay bridge detach probe timed out — forcing removal directly",
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
