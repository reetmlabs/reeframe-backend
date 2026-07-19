use std::{
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
/// monitor's session-timestamp refresh (Step 10b) — all three need to agree
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

    /// Called by the reconnect monitor (Step 10b) so chunks opened after a
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
    let queue = gstreamer::ElementFactory::make("queue")
        .name(format!("cam_{}_recqueue", camera_id.as_simple()))
        .property("max-size-time", 10_000_000_000u64) // 10 s jitter buffer
        .property("max-size-buffers", 0u32)
        .property("max-size-bytes", 0u32)
        .build()
        .map_err(|e| VmsError::Media(format!("queue: {e}")))?;

    let location = recording_location(recording_dir, camera_id);
    let chunk_ns = chunk_duration_secs * 1_000_000_000;

    // `muxer-factory`/`muxer-properties` only take effect with
    // `async-finalize=true` (confirmed via `gst-inspect-1.0 splitmuxsink` —
    // both are documented "Valid only for async-finalize = TRUE") — without
    // it, `muxer-factory` was silently ignored and mp4mux was only ever
    // used because it happens to be the element's own default. Setting
    // `faststart=true` on the muxer itself (mp4mux/qtmux's own property —
    // "the header is written at the end, then rewritten at the start when
    // the file is closed") is what makes each finalized chunk immediately
    // servable via HTTP Range requests (Step 10d) with no separate remux
    // pass needed.
    let muxer_properties = gstreamer::Structure::builder("properties")
        .field("faststart", true)
        .build();

    let splitmux = gstreamer::ElementFactory::make("splitmuxsink")
        .name(format!("cam_{}_splitmux", camera_id.as_simple()))
        .property("location", &location)
        .property("max-size-time", chunk_ns)
        .property("async-finalize", true)
        .property("muxer-factory", "mp4mux")
        .property("muxer-properties", &muxer_properties)
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

        // Audio pads are intentionally ignored for now (planned for Phase 2).
        // Checking `media=audio` here avoids spurious "unsupported encoding"
        // warnings for cameras that stream both video and audio over RTSP.
        if structure.get::<&str>("media").ok() == Some("audio") {
            tracing::debug!(
                camera_id = %cam_id,
                encoding = structure.get::<&str>("encoding-name").unwrap_or("unknown"),
                "Audio stream detected — skipped (audio recording planned for Phase 2)",
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

// -- Reconnect monitor task --

/// Spawn a tokio task that watches the GStreamer bus and reconnects on error/EOS.
///
/// Backoff: 2 s -> 4 s -> … -> 60 s cap, reset to 2 s after a successful restart.
/// On each reconnect `naming`'s session timestamp is refreshed so chunks from
/// different sessions never collide on disk (Step 10b — chunk naming is now
/// driven by `format-location-full`, not the `location` property, so this is
/// the one place that needs updating instead of the splitmuxsink element).
///
/// Also watches for `splitmuxsink-fragment-closed` bus (element) messages to
/// backfill each chunk's `end_time`/`size_bytes` via `chunk_event_tx` once the
/// file is finalized on disk (Step 10b).
pub(crate) fn spawn_monitor(
    camera_id: Uuid,
    gst_pipeline: gstreamer::Pipeline,
    naming: Arc<ChunkNaming>,
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use futures::StreamExt as _;
        use gstreamer::MessageView;

        let bus = gst_pipeline.bus().expect("pipeline bus missing");
        let mut bus_stream = bus.stream();
        let mut backoff = Duration::from_secs(2);
        const MAX_BACKOFF: Duration = Duration::from_secs(60);

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
                                handle_fragment_closed(camera_id, elem, &chunk_event_tx);
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

            tracing::info!(
                camera_id = %camera_id,
                backoff_secs = backoff.as_secs(),
                "Reconnecting",
            );

            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = &mut shutdown_rx => break,
            }

            match gst_pipeline.set_state(gstreamer::State::Playing) {
                Ok(_) => {
                    tracing::info!(camera_id = %camera_id, "Camera stream restarted");
                    backoff = Duration::from_secs(2);
                }
                Err(e) => {
                    tracing::error!(camera_id = %camera_id, "Restart failed: {e}");
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }

        gst_pipeline.set_state(gstreamer::State::Null).ok();
        tracing::info!(camera_id = %camera_id, "Stream monitor exited");
    })
}

// -- Helpers --

/// Handle a `splitmuxsink-fragment-closed` element message: the previous
/// chunk file is finalized on disk (faststart-remuxed already, since that's
/// a muxer-side property, not a separate pass — see `build_camera_stream`),
/// so this is the point to read its final size and emit
/// `RecordingChunkEvent::Closed`.
fn handle_fragment_closed(
    camera_id: Uuid,
    elem: &gstreamer::message::Element,
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

    let size_bytes = std::fs::metadata(&file_path).map(|m| m.len() as i64).unwrap_or(0);

    let _ = chunk_event_tx.send(RecordingChunkEvent::Closed {
        camera_id,
        file_path,
        end_time: chrono::Utc::now(),
        size_bytes,
    });
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
