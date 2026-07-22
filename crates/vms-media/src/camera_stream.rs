use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::{RecordingChunkEvent, VmsError};

// -- Supported codecs --

pub(crate) struct CodecElements {
    pub(crate) depay_factory: &'static str,
    pub(crate) parse_factory: &'static str,
}

pub(crate) fn codec_for(encoding_name: &str) -> Option<CodecElements> {
    match encoding_name.to_uppercase().as_str() {
        "H264" => Some(CodecElements {
            depay_factory: "rtph264depay",
            parse_factory: "h264parse",
        }),
        "H265" | "HEVC" => Some(CodecElements {
            depay_factory: "rtph265depay",
            parse_factory: "h265parse",
        }),
        // Motion JPEG — RTP encoding name per RFC 2435
        "JPEG" => Some(CodecElements {
            depay_factory: "rtpjpegdepay",
            parse_factory: "jpegparse",
        }),
        // AV1 — RTP encoding name per RFC 9671
        "AV1" => Some(CodecElements {
            depay_factory: "rtpav1depay",
            parse_factory: "av1parse",
        }),
        _ => None,
    }
}

// -- Camera stream builder --

/// Build a per-camera GStreamer pipeline for continuous recording.
///
/// The codec is **not** assumed at build time. When `rtspsrc` connects to the
/// camera and exposes an RTP src pad, the `pad-added` callback reads the
/// `encoding-name` field from the SDP caps and inserts the appropriate
/// depayloader + parser into the running pipeline:
///
/// ```text
/// rtspsrc --(pad-added)---> [rtph264depay|rtph265depay] ---> [h264parse|h265parse]
///                                                                      │
///                                                                      ▼
///                                                      tee ---> queue ---> splitmuxsink (MP4)
/// ```
///
/// Elements that vary per-camera are named `cam_{id}_{role}` so the reconnect
/// monitor can look them up by name instead of recreating them.
/// State shared between the codec-detection `pad-added` handler, the
/// `format-location-full` chunk-naming callback, and the reconnect
/// monitor's session-timestamp refresh — all three need to agree
/// on the current recording session's filename timestamp prefix, and the
/// naming callback additionally needs whichever codec the pad-added handler
/// most recently detected (recorded, not assumed, since it's negotiated
/// per-camera from the SDP).
pub(crate) struct ChunkNaming {
    session_ts: Mutex<String>,
    codec: Mutex<Option<String>>,
}

impl ChunkNaming {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            session_ts: Mutex::new(session_timestamp()),
            codec: Mutex::new(None),
        })
    }

    /// Called by the reconnect monitor so chunks opened after a
    /// reconnect get a fresh filename prefix — otherwise the fragment index
    /// restarting from 0 would collide with the previous session's chunk 0
    /// on disk.
    pub(crate) fn refresh_session(&self) {
        *self.session_ts.lock().unwrap() = session_timestamp();
    }
}

/// Build a per-camera GStreamer pipeline for continuous recording.
///
/// The codec is **not** assumed at build time. When `rtspsrc` connects to the
/// camera and exposes an RTP src pad, the `pad-added` callback reads the
/// `encoding-name` field from the SDP caps and inserts the appropriate
/// depayloader + parser into the running pipeline:
///
/// ```text
/// rtspsrc --(pad-added)---> [rtph264depay|rtph265depay] ---> [h264parse|h265parse]
///                                                                      │
///                                                                      ▼
///                                                      tee ---> queue ---> splitmuxsink (MP4)
/// ```
///
/// `chunk_event_tx` carries `RecordingChunkEvent::Opened`/`Closed` out to
/// whichever task actually has DB access (this crate deliberately has
/// none) — see `vms_core::RecordingChunkEvent`'s doc comment for why.
pub(crate) fn build_camera_stream(
    camera_id: Uuid,
    rtsp_url: &str,
    recording_dir: &std::path::Path,
    chunk_duration_secs: u64,
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
) -> Result<(gstreamer::Pipeline, Arc<ChunkNaming>), VmsError> {
    let gst_pipeline = gstreamer::Pipeline::new();
    let naming = ChunkNaming::new();

    // -- rtspsrc --
    let src = gstreamer::ElementFactory::make("rtspsrc")
        .name(format!("cam_{}_src", camera_id.as_simple()))
        .property("location", rtsp_url)
        .property("latency", 200u32)
        .build()
        .map_err(|e| VmsError::Media(format!("rtspsrc: {e}")))?;

    // -- Tee (fan-out point — recording branch now, ring buffer / analytics later)
    let tee = gstreamer::ElementFactory::make("tee")
        .name(format!("cam_{}_tee", camera_id.as_simple()))
        .build()
        .map_err(|e| VmsError::Media(format!("tee: {e}")))?;

    // -- Recording branch: queue -> splitmuxsink --
    let (queue, splitmux) = build_recording_branch(
        camera_id,
        recording_dir,
        chunk_duration_secs,
        &naming,
        &chunk_event_tx,
    )?;

    // -- Assemble static part of the pipeline --
    // Depayloader + parser are NOT added here — they are created dynamically
    // in the pad-added callback once we know the codec from the camera's SDP.
    gst_pipeline
        .add_many([&src, &tee, &queue, &splitmux])
        .map_err(|e| VmsError::Media(format!("add_many: {e}")))?;

    // tee ---> queue ---> splitmux (recording branch)
    let tee_src = tee
        .request_pad_simple("src_%u")
        .ok_or_else(|| VmsError::Media("tee: no src_%u pad template".into()))?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("queue: no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->queue: {e}")))?;

    queue
        .link(&splitmux)
        .map_err(|e| VmsError::Media(format!("link queue->splitmux: {e}")))?;

    // -- Dynamic codec wiring --
    // rtspsrc only exposes src pads after it receives the SDP from the camera,
    // so we must wire the depay+parse chain at pad-added time.
    //
    // On reconnect: rtspsrc removes and re-adds its src pad.  The depay/parse
    // elements already exist in the pipeline (added on first connection), so we
    // only need to re-link the rtspsrc src pad to the depay sink.
    let pipeline_weak = gst_pipeline.downgrade();
    let tee_weak = tee.downgrade();
    let cam_id = camera_id;
    let naming_for_pad_added = naming.clone();

    src.connect_pad_added(move |_src, src_pad| {
        // Only handle RTP src pads
        let caps = match src_pad.current_caps() {
            Some(c) => c,
            None => return,
        };
        let structure = match caps.structure(0) {
            Some(s) => s,
            None => return,
        };
        if !structure.name().starts_with("application/x-rtp") {
            return;
        }

        // Audio pads are intentionally ignored — this pipeline is video-only.
        // Checking `media=audio` here avoids spurious "unsupported encoding"
        // warnings for cameras that stream both video and audio over RTSP.
        if structure.get::<&str>("media").ok() == Some("audio") {
            tracing::debug!(
                camera_id = %cam_id,
                encoding = structure.get::<&str>("encoding-name").unwrap_or("unknown"),
                "Audio stream detected — skipped (video-only pipeline)",
            );
            return;
        }

        let encoding = match structure.get::<&str>("encoding-name") {
            Ok(e) => e.to_owned(),
            Err(_) => return,
        };
        *naming_for_pad_added.codec.lock().unwrap() = Some(encoding.clone());

        let Some(gst_pipeline) = pipeline_weak.upgrade() else {
            return;
        };
        let Some(tee) = tee_weak.upgrade() else { return };

        let depay_name = format!("cam_{}_depay", cam_id.as_simple());
        let parse_name = format!("cam_{}_parse", cam_id.as_simple());

        // -- Reconnect path: elements exist, just re-link the src pad --
        if let Some(depay) = gst_pipeline.by_name(&depay_name) {
            let sink = match depay.static_pad("sink") {
                Some(p) => p,
                None => return,
            };
            if !sink.is_linked() {
                if let Err(e) = src_pad.link(&sink) {
                    tracing::error!(camera_id = %cam_id, "re-link rtspsrc->depay: {e}");
                }
            }
            return;
        }

        // -- First connection: create depay + parse for the negotiated codec --
        let Some(codec) = codec_for(&encoding) else {
            tracing::warn!(camera_id = %cam_id, encoding, "Unsupported RTP encoding — camera feed ignored");
            return;
        };

        let depay = match gstreamer::ElementFactory::make(codec.depay_factory).name(&depay_name).build() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(camera_id = %cam_id, "create {}: {e}", codec.depay_factory);
                return;
            }
        };
        let parse = match gstreamer::ElementFactory::make(codec.parse_factory).name(&parse_name).build() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(camera_id = %cam_id, "create {}: {e}", codec.parse_factory);
                return;
            }
        };

        if let Err(e) = gst_pipeline.add_many([&depay, &parse]) {
            tracing::error!(camera_id = %cam_id, "add depay+parse: {e}");
            return;
        }

        // depay -> parse -> tee
        if let Err(e) = gstreamer::Element::link_many([&depay, &parse, &tee]) {
            tracing::error!(camera_id = %cam_id, "link depay->parse->tee: {e}");
            return;
        }

        // Bring new elements up to the pipeline's current state
        for el in [&depay, &parse] {
            el.sync_state_with_parent().ok();
        }

        // Link rtspsrc src pad -> depay sink
        let sink = match depay.static_pad("sink") {
            Some(p) => p,
            None => return,
        };
        if let Err(e) = src_pad.link(&sink) {
            tracing::error!(camera_id = %cam_id, encoding, "rtspsrc->depay link: {e}");
            return;
        }

        tracing::info!(camera_id = %cam_id, encoding, "Codec wired");
    });

    Ok((gst_pipeline, naming))
}

// -- Recording branch (queue -> splitmuxsink) --

// NOTE on faststart: `mp4mux`'s own `faststart=true` property looked like
// the cheap way to get progressively-servable chunks (HTTP Range streaming
// needs `moov` before `mdat`), but it only takes effect through
// `muxer-properties`, which itself only applies when `async-finalize=true`
// (confirmed via `gst-inspect-1.0 splitmuxsink`). Turning that on was tested
// live and reproducibly broke the reconnect path — every reconnect
// eventually errored with "Queued GOP time is negative" a couple of minutes
// later, because async-finalize's internal GOP queueing doesn't survive
// this pipeline's Null->Playing reconnect cycle cleanly. Continuous
// recording is the one thing that must never destabilize, so faststart is
// done as a separate pass on `fragment-closed` instead (see
// `remux_faststart`) — more disk I/O per chunk, but completely decoupled
// from the live pipeline's own state.

/// Build the recording branch — `queue -> splitmuxsink`, with
/// `format-location-full` wired to the naming/chunk-event machinery — as a
/// standalone, unattached pair of elements. Used both for the initial
/// pipeline construction (`build_camera_stream`) and to build a fresh
/// replacement on every reconnect (`rebuild_recording_branch`) — the caller
/// adds the returned elements to the pipeline and links them to `tee`.
fn build_recording_branch(
    camera_id: Uuid,
    recording_dir: &Path,
    chunk_duration_secs: u64,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
) -> Result<(gstreamer::Element, gstreamer::Element), VmsError> {
    let queue = gstreamer::ElementFactory::make("queue")
        .name(format!("cam_{}_recqueue", camera_id.as_simple()))
        .property("max-size-time", 10_000_000_000u64) // 10 s jitter buffer
        .property("max-size-buffers", 0u32)
        .property("max-size-bytes", 0u32)
        .build()
        .map_err(|e| VmsError::Media(format!("queue: {e}")))?;

    let location = recording_location(recording_dir, camera_id);
    let chunk_ns = chunk_duration_secs * 1_000_000_000;

    let splitmux = gstreamer::ElementFactory::make("splitmuxsink")
        .name(format!("cam_{}_splitmux", camera_id.as_simple()))
        .property("location", &location)
        .property("max-size-time", chunk_ns)
        .build()
        .map_err(|e| VmsError::Media(format!("splitmuxsink: {e}")))?;

    // -- Chunk-open naming + indexing --
    // `format-location-full` fires synchronously right before splitmuxsink
    // opens each new fragment — the one point that gives the real wall-clock
    // instant a specific chunk started (deriving it from filenames/chunk-index
    // arithmetic would drift silently across reconnects). Returning a path
    // here overrides the `location` property's own `%05d` pattern entirely.
    {
        let naming = naming.clone();
        let tx = chunk_event_tx.clone();
        let cam_id = camera_id;
        let recording_dir = recording_dir.to_path_buf();
        splitmux.connect("format-location-full", false, move |values| {
            let fragment_id = values[1].get::<u32>().unwrap_or(0);
            let session_ts = naming.session_ts.lock().unwrap().clone();
            let codec = naming.codec.lock().unwrap().clone();
            let file_path = chunk_location(&recording_dir, cam_id, &session_ts, fragment_id);

            let _ = tx.send(RecordingChunkEvent::Opened {
                camera_id: cam_id,
                file_path: file_path.clone(),
                chunk_index: fragment_id as i32,
                start_time: chrono::Utc::now(),
                codec,
            });

            Some(file_path.to_value())
        });
    }

    Ok((queue, splitmux))
}

/// Tear down and rebuild the recording branch (`queue` + `splitmuxsink`)
/// fresh on every reconnect, instead of reusing the same `splitmuxsink`
/// instance indefinitely.
///
/// Fixes a reproduced bug: reused across a bare `Null`->`Playing` reconnect
/// cycle, `splitmuxsink`'s internal GOP-collection state does not reset
/// cleanly — roughly 100s after any reconnect, newly-arriving buffers'
/// timestamps appear (from `splitmuxsink`'s perspective) to precede GOP
/// data it's still holding from before the reconnect, and
/// `gstsplitmuxsink.c`'s `handle_gathered_gop()` errors with "Queued GOP
/// time is negative" — which triggers another reconnect, forever. A fresh
/// `splitmuxsink` instance has no leftover GOP state to get confused by.
///
/// `tee`'s other consumers (relay, ring buffer, motion detection — see
/// `manager.rs`) are untouched: only the recording branch's own request pad
/// on `tee` is released and re-requested, nothing about `tee` itself or any
/// other branch hanging off it changes.
///
/// Must be called while `gst_pipeline` is in (or transitioning to) `Null` —
/// `Bin::remove` requires an element to already be in `Null` state.
fn rebuild_recording_branch(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
    recording_dir: &Path,
    chunk_duration_secs: u64,
) -> Result<(), VmsError> {
    let tee_name = format!("cam_{}_tee", camera_id.as_simple());
    let queue_name = format!("cam_{}_recqueue", camera_id.as_simple());
    let splitmux_name = format!("cam_{}_splitmux", camera_id.as_simple());

    let tee = gst_pipeline
        .by_name(&tee_name)
        .ok_or_else(|| VmsError::Media("rebuild recording branch: tee not found".into()))?;
    let old_queue = gst_pipeline
        .by_name(&queue_name)
        .ok_or_else(|| VmsError::Media("rebuild recording branch: recqueue not found".into()))?;
    let old_splitmux = gst_pipeline
        .by_name(&splitmux_name)
        .ok_or_else(|| VmsError::Media("rebuild recording branch: splitmux not found".into()))?;

    // Unlink and release the tee's request pad feeding the old queue, then
    // remove the old elements.
    let old_queue_sink = old_queue.static_pad("sink").ok_or_else(|| {
        VmsError::Media("rebuild recording branch: old queue has no sink pad".into())
    })?;
    if let Some(tee_src) = old_queue_sink.peer() {
        tee_src.unlink(&old_queue_sink).ok();
        tee.release_request_pad(&tee_src);
    }
    gst_pipeline
        .remove_many([&old_queue, &old_splitmux])
        .map_err(|e| VmsError::Media(format!("rebuild recording branch: remove_many: {e}")))?;

    let (queue, splitmux) = build_recording_branch(
        camera_id,
        recording_dir,
        chunk_duration_secs,
        naming,
        chunk_event_tx,
    )?;

    gst_pipeline
        .add_many([&queue, &splitmux])
        .map_err(|e| VmsError::Media(format!("rebuild recording branch: add_many: {e}")))?;

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media("rebuild recording branch: tee has no src_%u pad template".into())
    })?;
    let queue_sink = queue.static_pad("sink").ok_or_else(|| {
        VmsError::Media("rebuild recording branch: new queue has no sink pad".into())
    })?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("rebuild recording branch: link tee->queue: {e}")))?;
    queue.link(&splitmux).map_err(|e| {
        VmsError::Media(format!(
            "rebuild recording branch: link queue->splitmux: {e}"
        ))
    })?;

    for el in [&queue, &splitmux] {
        el.sync_state_with_parent().map_err(|e| {
            VmsError::Media(format!(
                "rebuild recording branch: sync_state_with_parent: {e}"
            ))
        })?;
    }

    Ok(())
}

// -- Reconnect backoff + circuit breaker --

/// What the reconnect monitor should do before its next reconnect attempt.
pub(crate) enum WaitPlan {
    /// Below the circuit-breaker threshold — normal exponential backoff.
    Backoff(Duration),
    /// At or above the circuit-breaker threshold — a persistently failing
    /// camera stops being retried at the tight backoff cap and instead gets
    /// one "probe" attempt per long cooldown.
    CircuitOpen(Duration),
}

/// Shared reconnect backoff + circuit-breaker policy for both the main
/// (`spawn_monitor`) and sub-stream (`sub_stream::spawn_sub_monitor`)
/// reconnect monitors.
///
/// A `set_state(Playing)` call returning `Ok` says nothing about whether the
/// RTSP connection actually re-established — the connection attempt happens
/// asynchronously downstream, so a camera that's persistently unreachable
/// will still get a bus `Error` shortly after. Resetting backoff on every
/// `Ok` from `set_state` (the previous behavior) therefore let a dead camera
/// hot-loop reconnects at the base backoff forever. Only staying up for
/// [`Self::STABLE_UPTIME`] before the *next* failure counts as real
/// recovery — that's what resets `backoff`/`consecutive_failures` back to
/// their base values, in [`Self::on_failure`].
pub(crate) struct ReconnectPolicy {
    connected_at: tokio::time::Instant,
    backoff: Duration,
    consecutive_failures: u32,
}

impl ReconnectPolicy {
    const BASE_BACKOFF: Duration = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    /// A reconnect attempt must stay up at least this long before the next
    /// failure is treated as the start of a fresh failure streak rather than
    /// a continuation of the current one.
    const STABLE_UPTIME: Duration = Duration::from_secs(30);
    /// Failures in a row (within one streak) before the circuit opens.
    const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
    /// How long the circuit stays open between single probe attempts.
    const CIRCUIT_OPEN_COOLDOWN: Duration = Duration::from_secs(300);

    pub(crate) fn new() -> Self {
        Self {
            connected_at: tokio::time::Instant::now(),
            backoff: Self::BASE_BACKOFF,
            consecutive_failures: 0,
        }
    }

    /// Call once per failure (a bus `Error`/`Eos`, a failed pipeline
    /// rebuild, or a failed `set_state(Playing)` call) to get the wait
    /// policy for the next attempt.
    pub(crate) fn on_failure(&mut self) -> WaitPlan {
        if self.connected_at.elapsed() >= Self::STABLE_UPTIME {
            self.consecutive_failures = 0;
            self.backoff = Self::BASE_BACKOFF;
        }
        self.consecutive_failures += 1;

        if self.consecutive_failures > Self::CIRCUIT_BREAKER_THRESHOLD {
            WaitPlan::CircuitOpen(Self::CIRCUIT_OPEN_COOLDOWN)
        } else {
            let backoff = self.backoff;
            self.backoff = (self.backoff * 2).min(Self::MAX_BACKOFF);
            WaitPlan::Backoff(backoff)
        }
    }

    /// Call right after `set_state(Playing)` itself returns `Ok` — starts
    /// the uptime clock [`Self::on_failure`] checks against. Deliberately
    /// does *not* reset `backoff`/`consecutive_failures`; see the type-level
    /// doc comment.
    pub(crate) fn record_attempt(&mut self) {
        self.connected_at = tokio::time::Instant::now();
    }

    pub(crate) fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }
}

// -- Reconnect monitor task --

/// Spawn a tokio task that watches the GStreamer bus and reconnects on error/EOS.
///
/// Backoff and circuit breaker: see [`ReconnectPolicy`]. On each reconnect
/// `naming`'s session timestamp is refreshed so chunks from different
/// sessions never collide on disk, and the recording branch (`queue` +
/// `splitmuxsink`) is torn down and rebuilt fresh — see
/// `rebuild_recording_branch` for why.
///
/// Also watches for `splitmuxsink-fragment-closed` bus (element) messages to
/// backfill each chunk's `end_time`/`size_bytes` via `chunk_event_tx` once the
/// file is finalized on disk.
pub(crate) fn spawn_monitor(
    camera_id: Uuid,
    gst_pipeline: gstreamer::Pipeline,
    naming: Arc<ChunkNaming>,
    recording_dir: PathBuf,
    chunk_duration_secs: u64,
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use futures::StreamExt as _;
        use gstreamer::MessageView;

        let bus = gst_pipeline.bus().expect("pipeline bus missing");
        let mut bus_stream = bus.stream();
        let mut policy = ReconnectPolicy::new();

        'outer: loop {
            // -- Watch bus until Error/EOS or shutdown --
            let needs_reconnect = 'watch: loop {
                tokio::select! {
                    maybe_msg = bus_stream.next() => {
                        let Some(msg) = maybe_msg else { break 'watch false };
                        match msg.view() {
                            MessageView::Error(err) => {
                                tracing::error!(
                                    camera_id = %camera_id,
                                    error = %err.error(),
                                    debug = ?err.debug(),
                                    "GStreamer error — will reconnect",
                                );
                                break 'watch true;
                            }
                            MessageView::Eos(_) => {
                                tracing::warn!(camera_id = %camera_id, "RTSP stream EOS — will reconnect");
                                break 'watch true;
                            }
                            MessageView::Warning(w) => {
                                tracing::warn!(
                                    camera_id = %camera_id,
                                    warning = %w.error(),
                                    "GStreamer warning",
                                );
                            }
                            MessageView::Element(elem) => {
                                handle_fragment_closed(camera_id, elem, &naming, &chunk_event_tx);
                            }
                            _ => {}
                        }
                    }
                    _ = &mut shutdown_rx => break 'outer,
                }
            };

            if !needs_reconnect {
                break;
            }

            gst_pipeline.set_state(gstreamer::State::Null).ok();

            // Fresh timestamp prefix -> no chunk filename collisions
            naming.refresh_session();

            // -- Retry until Playing genuinely starts, or shutdown --
            // Handles pipeline tear-down/rebuild failures and a failed
            // `set_state(Playing)` call directly, without waiting on a bus
            // message that would never arrive since the pipeline never
            // reached Playing in the first place. A failure *after*
            // Playing starts (e.g. the RTSP connection itself failing) is
            // instead caught by the bus watch above, on the next lap of
            // 'outer.
            'reconnect: loop {
                let wait_plan = policy.on_failure();
                match wait_plan {
                    WaitPlan::Backoff(d) => {
                        tracing::info!(
                            camera_id = %camera_id,
                            backoff_secs = d.as_secs(),
                            consecutive_failures = policy.consecutive_failures(),
                            "Reconnecting",
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            _ = &mut shutdown_rx => break 'outer,
                        }
                    }
                    WaitPlan::CircuitOpen(cooldown) => {
                        tracing::error!(
                            camera_id = %camera_id,
                            consecutive_failures = policy.consecutive_failures(),
                            cooldown_secs = cooldown.as_secs(),
                            "Circuit breaker open — too many reconnect failures in a row, \
                             cooling down before the next attempt",
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(cooldown) => {}
                            _ = &mut shutdown_rx => break 'outer,
                        }
                    }
                }

                if let Err(e) = rebuild_recording_branch(
                    camera_id,
                    &gst_pipeline,
                    &naming,
                    &chunk_event_tx,
                    &recording_dir,
                    chunk_duration_secs,
                ) {
                    tracing::error!(
                        camera_id = %camera_id,
                        error = %e,
                        "Failed to rebuild recording branch — will retry",
                    );
                    continue 'reconnect;
                }

                match gst_pipeline.set_state(gstreamer::State::Playing) {
                    Ok(_) => {
                        tracing::info!(camera_id = %camera_id, "Camera stream restarted");
                        policy.record_attempt();
                        break 'reconnect;
                    }
                    Err(e) => {
                        tracing::error!(camera_id = %camera_id, "Restart failed: {e}");
                        continue 'reconnect;
                    }
                }
            }
        }

        gst_pipeline.set_state(gstreamer::State::Null).ok();
        tracing::info!(camera_id = %camera_id, "Stream monitor exited");
    })
}

// -- Helpers --

/// Below this many bytes, a "closed" fragment is treated as empty/garbage
/// rather than a real chunk of footage — smaller than any valid MP4 could
/// plausibly be (`ftyp` + `moov` + `mdat` box headers alone already exceed
/// this). Observed live as a byproduct of a still-unresolved reconnect bug:
/// every reconnect that survives long enough eventually produces one
/// genuinely 0-byte fragment right before erroring out.
const MIN_PLAUSIBLE_CHUNK_BYTES: u64 = 1024;

/// Handle a `splitmuxsink-fragment-closed` element message: the previous
/// chunk file is finalized on disk. Runs the faststart remux (see
/// `remux_faststart`) on a blocking thread — off the live pipeline entirely,
/// so a slow remux can never stall bus-message processing or interact with
/// the reconnect path — then reads the final size and emits
/// `RecordingChunkEvent::Closed` (or `Discarded` for an empty/garbage
/// fragment — see `MIN_PLAUSIBLE_CHUNK_BYTES`).
fn handle_fragment_closed(
    camera_id: Uuid,
    elem: &gstreamer::message::Element,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
) {
    let Some(structure) = elem.structure() else {
        return;
    };
    if structure.name() != "splitmuxsink-fragment-closed" {
        return;
    }
    let Ok(file_path) = structure.get::<String>("location") else {
        tracing::warn!(camera_id = %camera_id, "fragment-closed message missing 'location'");
        return;
    };

    let codec = naming.codec.lock().unwrap().clone();
    let chunk_event_tx = chunk_event_tx.clone();

    tokio::task::spawn_blocking(move || {
        let on_disk_size = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
        if on_disk_size < MIN_PLAUSIBLE_CHUNK_BYTES {
            tracing::warn!(
                camera_id = %camera_id,
                file_path,
                size_bytes = on_disk_size,
                "Discarding empty/garbage fragment — not indexing it as a real chunk",
            );
            std::fs::remove_file(&file_path).ok();
            let _ = chunk_event_tx.send(RecordingChunkEvent::Discarded {
                camera_id,
                file_path,
            });
            return;
        }

        if let Some(codec) = codec.as_deref().and_then(codec_for) {
            if let Err(e) = remux_faststart(&file_path, codec.parse_factory) {
                tracing::warn!(
                    camera_id = %camera_id,
                    file_path,
                    error = %e,
                    "Faststart remux failed — chunk stays playable, just not progressively seekable",
                );
            }
        }

        let size_bytes = std::fs::metadata(&file_path)
            .map(|m| m.len() as i64)
            .unwrap_or(0);

        let _ = chunk_event_tx.send(RecordingChunkEvent::Closed {
            camera_id,
            file_path,
            end_time: chrono::Utc::now(),
            size_bytes,
        });
    });
}

/// Rewrite `path` in place so its `moov` atom sits before `mdat` — what lets
/// an HTTP Range request serve a chunk progressively instead of
/// needing the whole file downloaded first. `mp4mux`'s own `faststart=true`
/// does exactly this, but only takes effect through `splitmuxsink`'s
/// `muxer-properties`, which in turn requires `async-finalize=true` — tested
/// live and found to reproducibly break the reconnect path (see the comment
/// on `build_camera_stream`'s `splitmuxsink` construction). Running it here,
/// as a completely separate one-shot pipeline over the already-closed file,
/// costs an extra demux+remux pass per chunk but can never destabilize live
/// recording — it operates on a file that's already finished.
///
/// Blocking — call from `spawn_blocking`, never from the async bus-watcher.
fn remux_faststart(path: &str, parse_factory: &str) -> Result<(), VmsError> {
    let tmp_path = format!("{path}.faststart.tmp");

    let pipeline = gstreamer::Pipeline::new();
    let filesrc = gstreamer::ElementFactory::make("filesrc")
        .property("location", path)
        .build()
        .map_err(|e| VmsError::Media(format!("faststart filesrc: {e}")))?;
    let demux = gstreamer::ElementFactory::make("qtdemux")
        .build()
        .map_err(|e| VmsError::Media(format!("faststart qtdemux: {e}")))?;
    let parse = gstreamer::ElementFactory::make(parse_factory)
        .build()
        .map_err(|e| VmsError::Media(format!("faststart {parse_factory}: {e}")))?;
    let mux = gstreamer::ElementFactory::make("mp4mux")
        .property("faststart", true)
        .build()
        .map_err(|e| VmsError::Media(format!("faststart mp4mux: {e}")))?;
    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", &tmp_path)
        .build()
        .map_err(|e| VmsError::Media(format!("faststart filesink: {e}")))?;

    pipeline
        .add_many([&filesrc, &demux, &parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("faststart add_many: {e}")))?;

    filesrc
        .link(&demux)
        .map_err(|e| VmsError::Media(format!("faststart link filesrc->demux: {e}")))?;
    gstreamer::Element::link_many([&parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("faststart link parse->mux->sink: {e}")))?;

    // qtdemux only exposes its src pad once it has parsed the file's moov —
    // same dynamic-pad dance as the live pipeline's own codec wiring.
    let parse_weak = parse.downgrade();
    demux.connect_pad_added(move |_demux, pad| {
        let Some(parse) = parse_weak.upgrade() else {
            return;
        };
        let Some(sink_pad) = parse.static_pad("sink") else {
            return;
        };
        if sink_pad.is_linked() {
            return;
        }
        if let Err(e) = pad.link(&sink_pad) {
            tracing::error!("faststart remux: link demux->parse failed: {e}");
        }
    });

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("faststart pipeline start: {e}")))?;

    let bus = pipeline.bus().expect("pipeline bus missing");
    let result = loop {
        let Some(msg) = bus.timed_pop(gstreamer::ClockTime::from_seconds(30)) else {
            break Err(VmsError::Media("faststart remux timed out".into()));
        };
        match msg.view() {
            gstreamer::MessageView::Eos(_) => break Ok(()),
            gstreamer::MessageView::Error(e) => {
                break Err(VmsError::Media(format!(
                    "faststart remux error: {}",
                    e.error()
                )));
            }
            _ => {}
        }
    };

    pipeline.set_state(gstreamer::State::Null).ok();
    result?;

    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Wall-clock timestamp prefix for a fresh recording session — a new one is
/// generated by [`ChunkNaming::refresh_session`] on every reconnect so chunk
/// filenames never collide across sessions.
fn session_timestamp() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string()
}

/// Unique chunk file location string for a recording session — kept only as
/// the `location` property's fallback value (used if `format-location-full`
/// ever returns `None`); the real per-chunk path in normal operation comes
/// from [`chunk_location`], called from the naming signal in
/// `build_camera_stream`.
///
/// Example: `/var/recordings/cam_<id>_20260509T143022_chunk%05d.mp4`
pub(crate) fn recording_location(base_dir: &std::path::Path, camera_id: Uuid) -> String {
    base_dir
        .join(format!(
            "cam_{}_{}_chunk%05d.mp4",
            camera_id.as_simple(),
            session_timestamp()
        ))
        .to_string_lossy()
        .to_string()
}

/// The exact path for one chunk, given the session's current timestamp
/// prefix and this fragment's index — used by the `format-location-full`
/// callback, which needs a concrete filename per fragment rather than the
/// `%05d` pattern `recording_location` produces for the property fallback.
fn chunk_location(
    base_dir: &std::path::Path,
    camera_id: Uuid,
    session_ts: &str,
    fragment_id: u32,
) -> String {
    base_dir
        .join(format!(
            "cam_{}_{}_chunk{:05}.mp4",
            camera_id.as_simple(),
            session_ts,
            fragment_id
        ))
        .to_string_lossy()
        .to_string()
}

// -- Tests --

#[cfg(test)]
mod reconnect_policy_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn first_failure_uses_base_backoff() {
        let mut policy = ReconnectPolicy::new();
        match policy.on_failure() {
            WaitPlan::Backoff(d) => assert_eq!(d, ReconnectPolicy::BASE_BACKOFF),
            WaitPlan::CircuitOpen(_) => panic!("expected Backoff on the first failure"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_quick_failures_escalate_and_cap_backoff() {
        let mut policy = ReconnectPolicy::new();
        let expected = [2u64, 4, 8, 16, 32];
        for expected_secs in expected {
            match policy.on_failure() {
                WaitPlan::Backoff(d) => assert_eq!(d.as_secs(), expected_secs),
                WaitPlan::CircuitOpen(_) => panic!("should still be below the circuit threshold"),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn circuit_opens_after_threshold_failures_in_a_row() {
        let mut policy = ReconnectPolicy::new();
        for _ in 0..ReconnectPolicy::CIRCUIT_BREAKER_THRESHOLD {
            match policy.on_failure() {
                WaitPlan::Backoff(_) => {}
                WaitPlan::CircuitOpen(_) => panic!("circuit should still be closed"),
            }
        }
        match policy.on_failure() {
            WaitPlan::CircuitOpen(d) => assert_eq!(d, ReconnectPolicy::CIRCUIT_OPEN_COOLDOWN),
            WaitPlan::Backoff(_) => panic!("circuit should have opened by now"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn circuit_stays_open_on_repeated_quick_failures() {
        let mut policy = ReconnectPolicy::new();
        for _ in 0..=ReconnectPolicy::CIRCUIT_BREAKER_THRESHOLD {
            policy.on_failure();
        }
        // A probe attempt that fails again immediately (no stable uptime in
        // between) must not close the circuit.
        match policy.on_failure() {
            WaitPlan::CircuitOpen(_) => {}
            WaitPlan::Backoff(_) => panic!("circuit should stay open on a quick repeat failure"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stable_uptime_resets_backoff_and_failure_count() {
        let mut policy = ReconnectPolicy::new();
        policy.on_failure();
        policy.on_failure();
        policy.on_failure(); // backoff now escalated past base

        policy.record_attempt();
        tokio::time::advance(ReconnectPolicy::STABLE_UPTIME + Duration::from_secs(1)).await;

        match policy.on_failure() {
            WaitPlan::Backoff(d) => assert_eq!(d, ReconnectPolicy::BASE_BACKOFF),
            WaitPlan::CircuitOpen(_) => panic!("a stable run should have reset the circuit too"),
        }
        assert_eq!(policy.consecutive_failures(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn short_uptime_does_not_reset_backoff() {
        let mut policy = ReconnectPolicy::new();
        policy.on_failure(); // backoff -> 2s returned, escalates to 4s internally

        policy.record_attempt();
        tokio::time::advance(ReconnectPolicy::STABLE_UPTIME / 2).await;

        match policy.on_failure() {
            WaitPlan::Backoff(d) => assert_eq!(d.as_secs(), 4),
            WaitPlan::CircuitOpen(_) => panic!("should still be below the circuit threshold"),
        }
    }
}
