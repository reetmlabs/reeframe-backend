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
    /// Mime type for [`byte_stream_au_caps`]; `None` where no such
    /// distinction exists (e.g. JPEG).
    pub(crate) parse_caps_mime: Option<&'static str>,
}

pub(crate) fn codec_for(encoding_name: &str) -> Option<CodecElements> {
    match encoding_name.to_uppercase().as_str() {
        "H264" => Some(CodecElements {
            depay_factory: "rtph264depay",
            parse_factory: "h264parse",
            parse_caps_mime: Some("video/x-h264"),
        }),
        "H265" | "HEVC" => Some(CodecElements {
            depay_factory: "rtph265depay",
            parse_factory: "h265parse",
            parse_caps_mime: Some("video/x-h265"),
        }),
        // Motion JPEG, RTP encoding name per RFC 2435
        "JPEG" => Some(CodecElements {
            depay_factory: "rtpjpegdepay",
            parse_factory: "jpegparse",
            parse_caps_mime: None,
        }),
        // AV1, RTP encoding name per RFC 9671
        "AV1" => Some(CodecElements {
            depay_factory: "rtpav1depay",
            parse_factory: "av1parse",
            parse_caps_mime: None,
        }),
        _ => None,
    }
}

/// Forces inline, repeated SPS/PPS and one access unit per buffer. Left
/// unconstrained, a parser can negotiate `avc`, which carries config data
/// out-of-band and breaks re-muxing the buffers on their own later.
fn byte_stream_au_caps(mime: &str) -> gstreamer::Caps {
    gstreamer::Caps::builder(mime)
        .field("stream-format", "byte-stream")
        .field("alignment", "au")
        .build()
}

/// Repeats SPS/PPS before every keyframe, if the parser supports it.
fn set_config_interval_if_supported(parse: &gstreamer::Element) {
    if parse.has_property("config-interval") {
        parse.set_property("config-interval", -1);
    }
}

// -- Camera stream builder --

/// State shared by the codec-detection `pad-added` handler, the recording
/// branch's `format-location-full` naming callback (see
/// [`attach_recording_branch`]) and the reconnect monitor.
///
/// They must agree on the session's filename timestamp prefix, and the naming
/// callback also needs the codec the pad-added handler last detected from the
/// SDP. Populated as soon as the live pipeline connects, whether or not
/// recording is attached.
pub(crate) struct ChunkNaming {
    session_ts: Mutex<String>,
    codec: Mutex<Option<String>>,
    /// Path of the fragment currently open, if any, set by
    /// `format-location-full`. Lets a graceful drain (see
    /// `watch_current_close`) wait for this exact fragment instead of any
    /// fragment that happens to close at the same time.
    current_path: Mutex<Option<String>>,
    /// Registered by a graceful drain, fired by `handle_fragment_closed`
    /// once `current_path`'s fragment closes.
    close_ack: Mutex<Option<(String, std::sync::mpsc::SyncSender<()>)>>,
    /// Serializes attaching, rebuilding and detaching the recording branch.
    /// The API, recording-intent resume and reconnect monitor can all reach
    /// it at once, and its elements have fixed names.
    recording_branch: Mutex<()>,
    /// Set while a detached recording branch finalizes its file. The EOS
    /// pushed into it can be the pipeline's last sink going EOS, which the
    /// bus reports as the whole pipeline ending; the monitor must not treat
    /// that as the camera dropping.
    finalizing_recording: std::sync::atomic::AtomicBool,
}

impl ChunkNaming {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            session_ts: Mutex::new(session_timestamp()),
            codec: Mutex::new(None),
            current_path: Mutex::new(None),
            close_ack: Mutex::new(None),
            recording_branch: Mutex::new(()),
            finalizing_recording: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Called by the reconnect monitor so chunks opened after a reconnect
    /// get a new filename prefix. The fragment index restarts at 0, which
    /// would otherwise collide with the previous session's chunk 0 on disk.
    pub(crate) fn refresh_session(&self) {
        *self.session_ts.lock().unwrap() = session_timestamp();
    }

    /// Register interest in the open fragment's close, so a graceful
    /// stop/reconnect can wait for it before tearing the branch down. `None`
    /// if no fragment is open.
    fn watch_current_close(&self) -> Option<std::sync::mpsc::Receiver<()>> {
        let path = self.current_path.lock().unwrap().clone()?;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        *self.close_ack.lock().unwrap() = Some((path, tx));
        Some(rx)
    }

    /// The fragment currently open, if any, without registering a drain wait.
    /// Lets a timed-out drain still index the fragment it gave up on (see
    /// `drain_recording_branch`).
    fn current_path(&self) -> Option<String> {
        self.current_path.lock().unwrap().clone()
    }

    /// The main stream's RTP encoding name (e.g. `H264`), once its SDP has
    /// been negotiated.
    pub(crate) fn codec(&self) -> Option<String> {
        self.codec.lock().unwrap().clone()
    }
}

/// Build a per-camera live GStreamer pipeline, `rtspsrc -> [depay|parse] ->
/// tee`, with nothing written to disk.
///
/// The codec is not known at build time. When `rtspsrc` connects and exposes
/// an RTP src pad, the `pad-added` callback reads `encoding-name` from the SDP
/// caps and inserts the matching depayloader and parser:
///
/// ```text
/// rtspsrc --(pad-added)---> [rtph264depay|rtph265depay] ---> [h264parse|h265parse] ---> tee
/// ```
///
/// Every consumer attaches to the `tee` as an independent branch: recording
/// ([`attach_recording_branch`]), the RTSP relay (`relay_bridge::attach`),
/// motion detection (`motion_branch::attach`), thumbnail capture
/// (`thumbnail_branch::attach`) and the ring buffer
/// (`ring_buffer_branch::attach`). None of them requires recording.
pub(crate) fn build_camera_stream(
    camera_id: Uuid,
    rtsp_url: &str,
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

    // -- Tee (recording, relay, ring buffer and analytics all tap this) --
    // `allow-not-linked`: every consumer attaches at runtime, so the tee must
    // tolerate having none. Otherwise its first buffer fails with
    // `not-linked` and the pipeline reconnects.
    let tee = gstreamer::ElementFactory::make("tee")
        .name(format!("cam_{}_tee", camera_id.as_simple()))
        .property("allow-not-linked", true)
        .build()
        .map_err(|e| VmsError::Media(format!("tee: {e}")))?;

    // -- Assemble static part of the pipeline --
    // Depayloader and parser are created in the pad-added callback once the
    // codec is known from the camera's SDP.
    gst_pipeline
        .add_many([&src, &tee])
        .map_err(|e| VmsError::Media(format!("add_many: {e}")))?;

    // -- Dynamic codec wiring --
    // rtspsrc only exposes src pads after it receives the SDP from the camera,
    // so we must wire the depay+parse chain at pad-added time.
    //
    // On reconnect rtspsrc removes and re-adds its src pad. The depay/parse
    // elements from the first connection are still in the pipeline, so only
    // the rtspsrc src pad needs re-linking to the depay sink.
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

        // The pipeline is video-only. Skipping `media=audio` here avoids
        // "unsupported encoding" warnings for cameras that also stream audio.
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
        set_config_interval_if_supported(&parse);

        if let Err(e) = gst_pipeline.add_many([&depay, &parse]) {
            tracing::error!(camera_id = %cam_id, "add depay+parse: {e}");
            return;
        }

        // depay -> parse -> tee
        let link_result = match codec.parse_caps_mime {
            Some(mime) => depay
                .link(&parse)
                .and_then(|()| parse.link_filtered(&tee, &byte_stream_au_caps(mime))),
            None => gstreamer::Element::link_many([&depay, &parse, &tee]),
        };
        if let Err(e) = link_result {
            tracing::error!(camera_id = %cam_id, "link depay->parse->tee: {e}");
            // Remove the half-wired elements. Otherwise the next reconnect
            // finds `depay`, assumes the chain is wired (see the reconnect
            // path above) and never retries the link.
            for el in [&depay, &parse] {
                el.set_state(gstreamer::State::Null).ok();
                gst_pipeline.remove(el).ok();
            }
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

// Faststart: HTTP Range streaming needs `moov` before `mdat`. Setting
// `mp4mux`'s `faststart=true` inside splitmuxsink only works through
// `muxer-properties`, which requires `async-finalize=true`, and with that
// enabled every reconnect later fails with "Queued GOP time is negative"
// because the async-finalize GOP queue does not survive the Null->Playing
// cycle. So faststart runs as a separate pass on `fragment-closed` (see
// `remux_faststart`). It costs extra disk I/O per chunk but keeps recording
// independent of the live pipeline's state.

/// Build the unattached recording branch, `queue -> [recparse ->]
/// splitmuxsink`, with `format-location-full` wired to chunk naming and chunk
/// events. Used when attaching recording and when rebuilding it on reconnect
/// (`rebuild_recording_branch`); the caller adds the elements to the pipeline
/// and links them to `tee`.
///
/// `recparse` (`Some` for H264/H265) converts `tee`'s byte-stream/au feed back
/// to `avc`/`avc3`, the only H264/H265 format `splitmuxsink`'s muxer accepts.
/// The shared parser is forced to byte-stream for `extract_clip` (see
/// `byte_stream_au_caps`), so recording needs its own parser.
fn build_recording_branch(
    camera_id: Uuid,
    recording_dir: &Path,
    chunk_duration_secs: u64,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
) -> Result<
    (
        gstreamer::Element,
        Option<gstreamer::Element>,
        gstreamer::Element,
    ),
    VmsError,
> {
    // Clear any path left from a previous recording session, so a drain
    // issued before this branch opens its first fragment does not wait on a
    // fragment that will never close (see `watch_current_close`).
    *naming.current_path.lock().unwrap() = None;

    let recparse = match naming.codec.lock().unwrap().as_deref().and_then(codec_for) {
        Some(codec) if codec.parse_caps_mime.is_some() => Some(
            gstreamer::ElementFactory::make(codec.parse_factory)
                .name(format!("cam_{}_recparse", camera_id.as_simple()))
                .build()
                .map_err(|e| VmsError::Media(format!("recording {}: {e}", codec.parse_factory)))?,
        ),
        _ => None,
    };

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
    // opens each fragment, so it gives the real wall-clock start of the
    // chunk. Deriving it from the chunk index would drift across reconnects.
    // The returned path overrides the `location` property's `%05d` pattern.
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
            *naming.current_path.lock().unwrap() = Some(file_path.clone());

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

    Ok((queue, recparse, splitmux))
}

fn tee_name(camera_id: Uuid) -> String {
    format!("cam_{}_tee", camera_id.as_simple())
}

/// How long to wait for the video codec to link into `tee`. SDP negotiation
/// normally takes under a second.
const CODEC_WIRE_TIMEOUT: Duration = Duration::from_secs(5);
const CODEC_WIRE_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait until the live pipeline's video codec is linked into `tee`.
///
/// If the recording branch attaches first, `splitmuxsink` narrows `tee`'s
/// negotiable caps and the video link in `build_camera_stream`'s pad-added
/// handler can fail for good. Errors after `CODEC_WIRE_TIMEOUT`, because a
/// branch built then would also lack its converting parser and never write
/// a file.
pub(crate) async fn wait_for_codec_wired(
    gst_pipeline: &gstreamer::Pipeline,
    camera_id: Uuid,
) -> Result<(), VmsError> {
    let tee_name = tee_name(camera_id);
    let deadline = tokio::time::Instant::now() + CODEC_WIRE_TIMEOUT;
    loop {
        let linked = gst_pipeline
            .by_name(&tee_name)
            .and_then(|tee| tee.static_pad("sink"))
            .is_some_and(|p| p.is_linked());
        if linked {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(VmsError::Media(format!(
                "camera {camera_id} stream not connected after {}s",
                CODEC_WIRE_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(CODEC_WIRE_POLL_INTERVAL).await;
    }
}

fn recqueue_name(camera_id: Uuid) -> String {
    format!("cam_{}_recqueue", camera_id.as_simple())
}

fn recparse_name(camera_id: Uuid) -> String {
    format!("cam_{}_recparse", camera_id.as_simple())
}

fn splitmux_name(camera_id: Uuid) -> String {
    format!("cam_{}_splitmux", camera_id.as_simple())
}

/// Return `true` if a recording branch is attached to `camera_id`'s live
/// pipeline. The `splitmuxsink` element's presence is the state flag, since
/// only the attach/rebuild/detach functions here add or remove it.
pub(crate) fn is_recording_attached(camera_id: Uuid, gst_pipeline: &gstreamer::Pipeline) -> bool {
    gst_pipeline.by_name(&splitmux_name(camera_id)).is_some()
}

/// Attach the recording branch (`queue -> splitmuxsink`) to the tee of an
/// already-running live pipeline. No-op if a recording branch is already
/// attached. Safe to call while the pipeline is `Playing`, using the same
/// live-tee attach pattern as `ring_buffer_branch` and `motion_branch`.
pub(crate) fn attach_recording_branch(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
    recording_dir: &Path,
    chunk_duration_secs: u64,
) -> Result<(), VmsError> {
    let _guard = naming.recording_branch.lock().unwrap();
    attach_recording_branch_locked(
        camera_id,
        gst_pipeline,
        naming,
        chunk_event_tx,
        recording_dir,
        chunk_duration_secs,
    )
}

/// [`attach_recording_branch`] for a caller already holding
/// `naming.recording_branch`. Removes whatever it added if any step fails,
/// so a failed attach never leaves a half-linked branch that
/// [`is_recording_attached`] would report as recording.
fn attach_recording_branch_locked(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
    recording_dir: &Path,
    chunk_duration_secs: u64,
) -> Result<(), VmsError> {
    if is_recording_attached(camera_id, gst_pipeline) {
        return Ok(());
    }

    let tee = gst_pipeline
        .by_name(&tee_name(camera_id))
        .ok_or_else(|| VmsError::Media("attach recording branch: tee not found".into()))?;

    let (queue, recparse, splitmux) = build_recording_branch(
        camera_id,
        recording_dir,
        chunk_duration_secs,
        naming,
        chunk_event_tx,
    )?;

    let mut new_elements: Vec<&gstreamer::Element> = vec![&queue, &splitmux];
    if let Some(p) = &recparse {
        new_elements.push(p);
    }
    gst_pipeline
        .add_many(new_elements.iter().copied())
        .map_err(|e| VmsError::Media(format!("attach recording branch: add_many: {e}")))?;

    let linked = link_recording_branch(&tee, &queue, recparse.as_ref(), &splitmux);
    if let Err(e) = linked {
        if let Some(queue_sink) = queue.static_pad("sink") {
            if let Some(tee_src) = queue_sink.peer() {
                tee_src.unlink(&queue_sink).ok();
                tee.release_request_pad(&tee_src);
            }
        }
        for el in &new_elements {
            el.set_state(gstreamer::State::Null).ok();
        }
        gst_pipeline.remove_many(new_elements).ok();
        return Err(e);
    }

    tracing::info!(camera_id = %camera_id, "Recording branch attached");
    Ok(())
}

/// Link `queue -> [recparse ->] splitmuxsink`, bring the elements up to the
/// pipeline's state from the sink backwards, then connect the tee. Data
/// reaching an element still in `Null` gets `FLUSHING`, which stops the
/// queue's streaming task for good, and `splitmuxsink`'s first state change
/// can take seconds while its muxer plugin loads.
fn link_recording_branch(
    tee: &gstreamer::Element,
    queue: &gstreamer::Element,
    recparse: Option<&gstreamer::Element>,
    splitmux: &gstreamer::Element,
) -> Result<(), VmsError> {
    // Link pads directly. `Element::link()`'s generic pad search probes
    // splitmuxsink's request pad and wrongly rejects it as incompatible with
    // upstream's fixed caps.
    let queue_src = queue.static_pad("src").ok_or_else(|| {
        VmsError::Media("attach recording branch: new queue has no src pad".into())
    })?;
    let splitmux_sink = splitmux.request_pad_simple("video").ok_or_else(|| {
        VmsError::Media("attach recording branch: splitmuxsink has no video pad template".into())
    })?;

    if let Some(p) = recparse {
        let recparse_sink = p.static_pad("sink").ok_or_else(|| {
            VmsError::Media("attach recording branch: recparse has no sink pad".into())
        })?;
        let recparse_src = p.static_pad("src").ok_or_else(|| {
            VmsError::Media("attach recording branch: recparse has no src pad".into())
        })?;
        queue_src.link(&recparse_sink).map_err(|e| {
            VmsError::Media(format!(
                "attach recording branch: link queue->recparse: {e:?}"
            ))
        })?;
        recparse_src.link(&splitmux_sink).map_err(|e| {
            VmsError::Media(format!(
                "attach recording branch: link recparse->splitmux: {e:?}"
            ))
        })?;
    } else {
        queue_src.link(&splitmux_sink).map_err(|e| {
            VmsError::Media(format!(
                "attach recording branch: link queue->splitmux: {e:?}"
            ))
        })?;
    }

    let mut downstream_first = vec![splitmux];
    downstream_first.extend(recparse);
    downstream_first.push(queue);
    for el in downstream_first {
        el.sync_state_with_parent().map_err(|e| {
            VmsError::Media(format!(
                "attach recording branch: sync_state_with_parent: {e}"
            ))
        })?;
    }

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media("attach recording branch: tee has no src_%u pad template".into())
    })?;
    let queue_sink = queue.static_pad("sink").ok_or_else(|| {
        VmsError::Media("attach recording branch: new queue has no sink pad".into())
    })?;
    if let Err(e) = tee_src.link(&queue_sink) {
        tee.release_request_pad(&tee_src);
        return Err(VmsError::Media(format!(
            "attach recording branch: link tee->queue: {e}"
        )));
    }
    Ok(())
}

/// Detach the recording branch (`queue -> splitmuxsink`) from the tee of a
/// running live pipeline, giving its in-flight chunk a chance to close
/// cleanly first. No-op if no recording branch is attached.
///
/// Uses the same blocking pad probe as `ring_buffer_branch::detach`, but the
/// probe unlinks the branch and pushes EOS into it instead of setting
/// `splitmuxsink` to `Null`, which would truncate the file being written. A
/// short-lived `std::thread` then releases the tee's request pad, waits up to
/// 5 s for the resulting `splitmuxsink-fragment-closed` message, and tears the
/// elements down. Returns immediately. Safe to call while `Playing`.
pub(crate) fn detach_recording_branch(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
) -> Result<(), VmsError> {
    let _guard = naming.recording_branch.lock().unwrap();
    let Some(queue) = gst_pipeline.by_name(&recqueue_name(camera_id)) else {
        return Ok(());
    };
    let Some(splitmux) = gst_pipeline.by_name(&splitmux_name(camera_id)) else {
        return Ok(());
    };
    let recparse = gst_pipeline.by_name(&recparse_name(camera_id));

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("recording queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("recording queue sink has no peer pad".into()))?;
    let tee = tee_src
        .parent_element()
        .ok_or_else(|| VmsError::Media("recording tee src pad has no parent element".into()))?;

    let close_rx = naming.watch_current_close();
    naming
        .finalizing_recording
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
    let queue_sink_clone = queue_sink.clone();

    tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();
        queue_sink_clone.send_event(gstreamer::event::Eos::new());
        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    let tee_src_clone = tee_src.clone();
    let pipeline_clone = gst_pipeline.clone();
    let naming = naming.clone();
    std::thread::spawn(move || {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => tracing::info!(camera_id = %camera_id, "Recording branch unlinked from tee"),
            Err(_) => tracing::warn!(
                camera_id = %camera_id,
                "recording branch unlink probe timed out — releasing tee pad anyway",
            ),
        }
        tee.release_request_pad(&tee_src_clone);

        let drained = close_rx.is_some_and(|rx| rx.recv_timeout(Duration::from_secs(5)).is_ok());
        if drained {
            tracing::info!(camera_id = %camera_id, "Recording branch closed its chunk cleanly");
        } else {
            tracing::warn!(
                camera_id = %camera_id,
                "recording branch drain timed out — its last chunk may be truncated",
            );
        }

        queue.set_state(gstreamer::State::Null).ok();
        splitmux.set_state(gstreamer::State::Null).ok();
        pipeline_clone.remove(&queue).ok();
        pipeline_clone.remove(&splitmux).ok();
        if let Some(p) = recparse {
            p.set_state(gstreamer::State::Null).ok();
            pipeline_clone.remove(&p).ok();
        }
        naming
            .finalizing_recording
            .store(false, std::sync::atomic::Ordering::SeqCst);
    });

    Ok(())
}

/// Let the recording branch close its open chunk before the caller forces
/// the pipeline to `Null`, which would truncate it. No-op if no recording
/// branch is attached or no fragment has opened yet.
///
/// Polls `bus_stream` itself (the caller's watch loop is paused meanwhile),
/// because the `splitmuxsink-fragment-closed` message only reaches whoever
/// polls the bus, and `handle_fragment_closed` must still run for it. Only
/// fragment-closed element messages are handled; everything else is dropped
/// since the caller is already tearing down. A 5 s timeout covers a branch
/// that no longer responds; the open chunk is then indexed from disk.
async fn drain_recording_branch(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
    bus_stream: &mut (impl futures::Stream<Item = gstreamer::Message> + Unpin),
) {
    use futures::StreamExt as _;

    let Some(queue) = gst_pipeline.by_name(&recqueue_name(camera_id)) else {
        return;
    };
    let Some(close_rx) = naming.watch_current_close() else {
        return;
    };
    let Some(sink_pad) = queue.static_pad("sink") else {
        return;
    };
    sink_pad.send_event(gstreamer::event::Eos::new());

    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            maybe_msg = bus_stream.next() => {
                // The bus is gone because the pipeline was torn down
                // elsewhere, so there is nothing left to wait on.
                let Some(msg) = maybe_msg else {
                    tracing::warn!(camera_id = %camera_id, "recording branch drain: bus closed before its chunk closed");
                    return;
                };
                if let gstreamer::MessageView::Element(elem) = msg.view() {
                    handle_fragment_closed(camera_id, elem, naming, chunk_event_tx);
                }
                if close_rx.try_recv().is_ok() {
                    tracing::info!(camera_id = %camera_id, "Recording branch closed its chunk cleanly");
                    return;
                }
            }
            _ = &mut deadline => {
                tracing::warn!(
                    camera_id = %camera_id,
                    "recording branch drain timed out — indexing its last chunk from disk instead of leaving it open forever",
                );
                if let Some(file_path) = naming.current_path() {
                    naming.close_ack.lock().unwrap().take();
                    let codec = naming.codec.lock().unwrap().clone();
                    let chunk_event_tx = chunk_event_tx.clone();
                    tokio::task::spawn_blocking(move || {
                        finalize_fragment_file(camera_id, file_path, codec, chunk_event_tx);
                    });
                }
                return;
            }
        }
    }
}

/// How long to wait for the pipeline to finish its transition to `Null`
/// before giving up on a reconnect attempt. See [`wait_for_pipeline_null`].
const NULL_TRANSITION_TIMEOUT: gstreamer::ClockTime = gstreamer::ClockTime::from_seconds(5);

/// Block until `gst_pipeline` has actually finished transitioning to
/// `Null`, up to [`NULL_TRANSITION_TIMEOUT`].
///
/// `Element::state()` returns `Ok(Async)` rather than an error if the timeout
/// elapses mid-transition, so success means `state == Null`, not just `Ok`.
fn wait_for_pipeline_null(gst_pipeline: &gstreamer::Pipeline) -> Result<(), VmsError> {
    let (result, state, _pending) = gst_pipeline.state(NULL_TRANSITION_TIMEOUT);
    result.map_err(|e| VmsError::Media(format!("pipeline Null transition failed: {e:?}")))?;
    if state != gstreamer::State::Null {
        return Err(VmsError::Media(format!(
            "pipeline still in {state:?} after {NULL_TRANSITION_TIMEOUT} waiting for Null"
        )));
    }
    Ok(())
}

/// Replace the recording branch (`queue`, optional `recparse`,
/// `splitmuxsink`) with fresh elements on every reconnect. No-op if no
/// recording branch is attached.
///
/// A `splitmuxsink` reused across a `Null`->`Playing` cycle keeps its GOP
/// state. Roughly 100 s later new buffers appear to precede GOP data held
/// from before the reconnect, `handle_gathered_gop()` fails with "Queued GOP
/// time is negative", and that triggers another reconnect, repeatedly. A
/// fresh instance has no stale GOP state.
///
/// Only the recording branch's request pad on `tee` is released and
/// re-requested; the tee's other branches are untouched.
///
/// Call only after [`wait_for_pipeline_null`] confirmed the pipeline reached
/// `Null`. Removing elements that are still tearing down on their streaming
/// thread causes GStreamer-CRITICAL assertions and corrupted fragments.
fn rebuild_recording_branch(
    camera_id: Uuid,
    gst_pipeline: &gstreamer::Pipeline,
    naming: &Arc<ChunkNaming>,
    chunk_event_tx: &mpsc::UnboundedSender<RecordingChunkEvent>,
    recording_dir: &Path,
    chunk_duration_secs: u64,
) -> Result<(), VmsError> {
    let _guard = naming.recording_branch.lock().unwrap();
    if !is_recording_attached(camera_id, gst_pipeline) {
        return Ok(());
    }

    let tee = gst_pipeline
        .by_name(&tee_name(camera_id))
        .ok_or_else(|| VmsError::Media("rebuild recording branch: tee not found".into()))?;
    let old_queue = gst_pipeline
        .by_name(&recqueue_name(camera_id))
        .ok_or_else(|| VmsError::Media("rebuild recording branch: recqueue not found".into()))?;
    let old_splitmux = gst_pipeline
        .by_name(&splitmux_name(camera_id))
        .ok_or_else(|| VmsError::Media("rebuild recording branch: splitmux not found".into()))?;
    let old_recparse = gst_pipeline.by_name(&recparse_name(camera_id));

    // Unlink and release the tee's request pad feeding the old queue, then
    // remove the old elements.
    let old_queue_sink = old_queue.static_pad("sink").ok_or_else(|| {
        VmsError::Media("rebuild recording branch: old queue has no sink pad".into())
    })?;
    if let Some(tee_src) = old_queue_sink.peer() {
        tee_src.unlink(&old_queue_sink).ok();
        tee.release_request_pad(&tee_src);
    }
    let mut old_elements: Vec<&gstreamer::Element> = vec![&old_queue, &old_splitmux];
    if let Some(p) = &old_recparse {
        old_elements.push(p);
    }
    gst_pipeline
        .remove_many(old_elements)
        .map_err(|e| VmsError::Media(format!("rebuild recording branch: remove_many: {e}")))?;

    attach_recording_branch_locked(
        camera_id,
        gst_pipeline,
        naming,
        chunk_event_tx,
        recording_dir,
        chunk_duration_secs,
    )
}

// -- Reconnect backoff + circuit breaker --

/// What the reconnect monitor should do before its next reconnect attempt.
pub(crate) enum WaitPlan {
    /// Within the circuit-breaker threshold: normal exponential backoff.
    Backoff(Duration),
    /// Past the circuit-breaker threshold: a persistently failing camera gets
    /// one probe attempt per long cooldown instead of retrying at the backoff
    /// cap.
    CircuitOpen(Duration),
}

/// Reconnect backoff and circuit-breaker policy shared by the main
/// (`spawn_monitor`) and sub-stream (`sub_stream::spawn_sub_monitor`)
/// monitors.
///
/// `set_state(Playing)` returning `Ok` does not mean the RTSP connection is
/// back; it connects asynchronously, and an unreachable camera posts a bus
/// `Error` shortly after. Resetting backoff on that `Ok` would let a dead
/// camera reconnect at the base backoff forever. Only staying up for
/// [`Self::STABLE_UPTIME`] before the next failure resets the backoff and
/// failure count, in [`Self::on_failure`].
pub(crate) struct ReconnectPolicy {
    connected_at: tokio::time::Instant,
    backoff: Duration,
    consecutive_failures: u32,
}

impl ReconnectPolicy {
    const BASE_BACKOFF: Duration = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    /// A reconnect must stay up at least this long for the next failure to
    /// start a new failure streak.
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

    /// Call right after `set_state(Playing)` returns `Ok` to start the uptime
    /// clock [`Self::on_failure`] checks. It does not reset the backoff or
    /// failure count; see the type-level docs.
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
/// Backoff and circuit breaker are in [`ReconnectPolicy`]. On each reconnect
/// `naming`'s session timestamp is refreshed so chunks from different
/// sessions never collide on disk, and the recording branch is rebuilt (see
/// `rebuild_recording_branch` for why).
///
/// Also watches for `splitmuxsink-fragment-closed` bus (element) messages to
/// backfill each chunk's `end_time`/`size_bytes` via `chunk_event_tx` once the
/// file is finalized on disk.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_monitor(
    camera_id: Uuid,
    gst_pipeline: gstreamer::Pipeline,
    naming: Arc<ChunkNaming>,
    recording_dir: PathBuf,
    chunk_duration_secs: u64,
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
    pipeline_live_tx: mpsc::UnboundedSender<Uuid>,
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
                                if naming.finalizing_recording.load(std::sync::atomic::Ordering::SeqCst) {
                                    tracing::debug!(camera_id = %camera_id, "EOS from a finalizing recording branch, ignored");
                                    continue;
                                }
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

            drain_recording_branch(
                camera_id,
                &gst_pipeline,
                &naming,
                &chunk_event_tx,
                &mut bus_stream,
            )
            .await;
            gst_pipeline.set_state(gstreamer::State::Null).ok();

            // New timestamp prefix so chunk filenames do not collide
            naming.refresh_session();

            // -- Retry until Playing starts, or shutdown --
            // Failures before Playing (teardown, rebuild, `set_state`) are
            // retried here directly, since no bus message would arrive for
            // them. Failures after Playing starts, such as the RTSP
            // connection dropping, are caught by the bus watch above on the
            // next pass of 'outer.
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

                if let Err(e) = wait_for_pipeline_null(&gst_pipeline) {
                    tracing::error!(
                        camera_id = %camera_id,
                        error = %e,
                        "Pipeline not fully stopped yet — will retry rebuild",
                    );
                    continue 'reconnect;
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
                        // Lets the task that owns recording intent resume
                        // recording as soon as the stream is back.
                        let _ = pipeline_live_tx.send(camera_id);
                        break 'reconnect;
                    }
                    Err(e) => {
                        tracing::error!(camera_id = %camera_id, "Restart failed: {e}");
                        continue 'reconnect;
                    }
                }
            }
        }

        drain_recording_branch(
            camera_id,
            &gst_pipeline,
            &naming,
            &chunk_event_tx,
            &mut bus_stream,
        )
        .await;
        gst_pipeline.set_state(gstreamer::State::Null).ok();
        tracing::info!(camera_id = %camera_id, "Stream monitor exited");
    })
}

// -- Helpers --

/// A closed fragment smaller than this is treated as garbage, not footage.
/// Any valid MP4 is larger (`ftyp` + `moov` + `mdat` headers alone exceed
/// it). Some reconnects leave a 0-byte fragment behind just before erroring.
const MIN_PLAUSIBLE_CHUNK_BYTES: u64 = 1024;

/// Handle a `splitmuxsink-fragment-closed` element message, meaning the
/// chunk file is finalized on disk.
///
/// Wakes a drain waiting on this fragment, then on a blocking thread sends
/// `RecordingChunkEvent::Closed` (or `Discarded`, see
/// `MIN_PLAUSIBLE_CHUNK_BYTES`) and only afterwards runs the faststart remux.
/// The remux can take a long time and shutdown does not wait for it, so the
/// event must not depend on it: a chunk whose `end_time` is never written is
/// skipped by retention and the coverage sweep. The remux is best-effort; a
/// chunk without it still plays but cannot be streamed progressively.
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

    // Wake a graceful drain waiting on this fragment (see
    // `watch_current_close`). Matching by path keeps another fragment
    // closing at the same time from being mistaken for it.
    {
        let mut ack = naming.close_ack.lock().unwrap();
        if ack.as_ref().is_some_and(|(path, _)| path == &file_path) {
            if let Some((_, tx)) = ack.take() {
                let _ = tx.send(());
            }
        }
    }

    let codec = naming.codec.lock().unwrap().clone();
    let chunk_event_tx = chunk_event_tx.clone();

    tokio::task::spawn_blocking(move || {
        finalize_fragment_file(camera_id, file_path, codec, chunk_event_tx);
    });
}

/// `stat()` `file_path` and index it as `Closed` (or `Discarded` below
/// `MIN_PLAUSIBLE_CHUNK_BYTES`), then try the faststart remux.
///
/// Used by `handle_fragment_closed` and by `drain_recording_branch`'s timeout
/// fallback, so a fragment whose close message never arrives is still
/// indexed instead of leaving its `recording` row open. Blocking; run it on a
/// blocking thread.
fn finalize_fragment_file(
    camera_id: Uuid,
    file_path: String,
    codec: Option<String>,
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
) {
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

    let _ = chunk_event_tx.send(RecordingChunkEvent::Closed {
        camera_id,
        file_path: file_path.clone(),
        end_time: chrono::Utc::now(),
        size_bytes: on_disk_size as i64,
    });

    // Best-effort from here on; nothing depends on the remux succeeding.
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
}

/// Rewrite `path` in place so its `moov` atom comes before `mdat`, which lets
/// HTTP Range requests stream the chunk progressively.
///
/// Doing this inside `splitmuxsink` breaks reconnects (see the faststart note
/// above `build_recording_branch`), so it runs as a separate one-shot
/// pipeline over the closed file. That costs an extra demux and remux per
/// chunk but cannot affect live recording.
///
/// Blocking; call from `spawn_blocking`, never from the async bus watcher.
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

    // qtdemux only exposes its src pad once it has parsed the file's moov.
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

/// Wall-clock timestamp prefix for a recording session.
/// [`ChunkNaming::refresh_session`] generates a new one on every reconnect so
/// chunk filenames never collide across sessions.
fn session_timestamp() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string()
}

/// Chunk location pattern for a recording session, used only as the
/// `location` property's fallback if `format-location-full` returns `None`.
/// Real per-chunk paths come from [`chunk_location`], called from the naming
/// signal set up in `build_recording_branch`.
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

/// The concrete path for one chunk from the session's timestamp prefix and
/// the fragment index, used by the `format-location-full` callback.
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

#[cfg(test)]
mod wait_for_pipeline_null_tests {
    use super::*;

    #[test]
    fn succeeds_once_the_pipeline_has_actually_reached_null() {
        gstreamer::init().ok();
        let pipeline = gstreamer::Pipeline::new();
        // A freshly constructed pipeline starts in Null already, but go
        // through Ready first so this exercises a real transition, not a
        // no-op.
        pipeline.set_state(gstreamer::State::Ready).ok();
        pipeline.set_state(gstreamer::State::Null).ok();

        assert!(wait_for_pipeline_null(&pipeline).is_ok());
    }
}

#[cfg(test)]
mod config_interval_tests {
    use super::*;

    #[test]
    fn sets_config_interval_on_a_parser_that_supports_it() {
        gstreamer::init().ok();
        let parse = gstreamer::ElementFactory::make("h264parse")
            .build()
            .expect("h264parse should be available");

        set_config_interval_if_supported(&parse);

        let value: i32 = parse.property("config-interval");
        assert_eq!(value, -1);
    }

    #[test]
    fn skips_a_parser_with_no_such_property() {
        gstreamer::init().ok();
        let parse = gstreamer::ElementFactory::make("jpegparse")
            .build()
            .expect("jpegparse should be available");

        // Must not panic or error: jpegparse has no config-interval property.
        set_config_interval_if_supported(&parse);
    }
}

#[cfg(test)]
mod byte_stream_caps_tests {
    use super::*;

    #[test]
    fn h264_and_h265_get_byte_stream_caps_mime() {
        assert_eq!(
            codec_for("H264").unwrap().parse_caps_mime,
            Some("video/x-h264")
        );
        assert_eq!(
            codec_for("H265").unwrap().parse_caps_mime,
            Some("video/x-h265")
        );
    }

    #[test]
    fn jpeg_and_av1_have_no_byte_stream_distinction() {
        assert_eq!(codec_for("JPEG").unwrap().parse_caps_mime, None);
        assert_eq!(codec_for("AV1").unwrap().parse_caps_mime, None);
    }

    #[test]
    fn byte_stream_au_caps_has_the_expected_fields() {
        gstreamer::init().ok();
        let caps = byte_stream_au_caps("video/x-h264");
        let s = caps.structure(0).unwrap();
        assert_eq!(s.get::<&str>("stream-format").unwrap(), "byte-stream");
        assert_eq!(s.get::<&str>("alignment").unwrap(), "au");
    }
}

#[cfg(test)]
mod recording_branch_avc_bridge_tests {
    use super::*;

    /// `splitmuxsink`'s muxer only accepts H264/H265 as avc/avc3, while the
    /// shared parser forces byte-stream onto `tee` for `extract_clip`, so
    /// `build_recording_branch` must add its own converting parser.
    #[test]
    fn build_recording_branch_adds_a_recparse_for_h264() {
        gstreamer::init().ok();
        let naming = ChunkNaming::new();
        *naming.codec.lock().unwrap() = Some("H264".into());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (_queue, recparse, _splitmux) = build_recording_branch(
            Uuid::new_v4(),
            std::path::Path::new("/tmp"),
            60,
            &naming,
            &tx,
        )
        .unwrap();
        assert!(recparse.is_some(), "H264 must get a converting recparse");
        assert_eq!(recparse.unwrap().factory().unwrap().name(), "h264parse");
    }

    #[test]
    fn build_recording_branch_has_no_recparse_when_codec_is_unknown() {
        gstreamer::init().ok();
        let naming = ChunkNaming::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let (_queue, recparse, _splitmux) = build_recording_branch(
            Uuid::new_v4(),
            std::path::Path::new("/tmp"),
            60,
            &naming,
            &tx,
        )
        .unwrap();
        assert!(recparse.is_none());
    }

    /// Linking `queue` straight into `splitmuxsink` fails once `tee`'s feed
    /// is fixed to byte-stream/au, because `mp4mux` cannot mux byte-stream
    /// H264. Going through a converting `recparse` must succeed.
    #[test]
    fn recording_branch_links_end_to_end_despite_byte_stream_tee_feed() {
        gstreamer::init().ok();

        let camera_id = Uuid::new_v4();
        let pipeline = gstreamer::Pipeline::new();
        let tee = gstreamer::ElementFactory::make("tee")
            .name(tee_name(camera_id))
            .build()
            .unwrap();
        pipeline.add(&tee).unwrap();

        // Fix tee's sink to byte-stream/au, same as the live video chain does.
        let upstream = gstreamer::ElementFactory::make("capsfilter")
            .property("caps", byte_stream_au_caps("video/x-h264"))
            .build()
            .unwrap();
        pipeline.add(&upstream).unwrap();
        upstream.link(&tee).unwrap();

        let naming = ChunkNaming::new();
        *naming.codec.lock().unwrap() = Some("H264".into());
        let (tx, _rx) = mpsc::unbounded_channel();
        let dir = std::env::temp_dir();

        attach_recording_branch(camera_id, &pipeline, &naming, &tx, &dir, 60)
            .expect("recording branch must link even though tee's feed is byte-stream/au");
        assert!(is_recording_attached(camera_id, &pipeline));
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;

    fn pipeline_with_tee(camera_id: Uuid) -> gstreamer::Pipeline {
        gstreamer::init().ok();
        let pipeline = gstreamer::Pipeline::new();
        let tee = gstreamer::ElementFactory::make("tee")
            .name(tee_name(camera_id))
            .build()
            .unwrap();
        let upstream = gstreamer::ElementFactory::make("capsfilter")
            .property("caps", byte_stream_au_caps("video/x-h264"))
            .build()
            .unwrap();
        pipeline.add_many([&upstream, &tee]).unwrap();
        upstream.link(&tee).unwrap();
        pipeline
    }

    #[test]
    fn live_tee_tolerates_having_no_consumers() {
        gstreamer::init().ok();
        let (pipeline, _) = build_camera_stream(Uuid::new_v4(), "rtsp://127.0.0.1:1/x").unwrap();
        let tee = pipeline
            .iterate_elements()
            .into_iter()
            .flatten()
            .find(|e| e.factory().is_some_and(|f| f.name() == "tee"))
            .unwrap();
        assert!(tee.property::<bool>("allow-not-linked"));
    }

    #[test]
    fn concurrent_attaches_leave_exactly_one_recording_branch() {
        let camera_id = Uuid::new_v4();
        let pipeline = pipeline_with_tee(camera_id);
        let naming = ChunkNaming::new();
        *naming.codec.lock().unwrap() = Some("H264".into());
        let (tx, _rx) = mpsc::unbounded_channel();
        let dir = std::env::temp_dir();

        let results: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    s.spawn(|| {
                        attach_recording_branch(camera_id, &pipeline, &naming, &tx, &dir, 60)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        assert!(results.iter().all(Result::is_ok), "{results:?}");
        let tee = pipeline.by_name(&tee_name(camera_id)).unwrap();
        assert_eq!(tee.src_pads().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn codec_wait_fails_when_the_stream_never_connects() {
        gstreamer::init().ok();
        let camera_id = Uuid::new_v4();
        let pipeline = gstreamer::Pipeline::new();
        let tee = gstreamer::ElementFactory::make("tee")
            .name(tee_name(camera_id))
            .build()
            .unwrap();
        pipeline.add(&tee).unwrap();

        assert!(wait_for_codec_wired(&pipeline, camera_id).await.is_err());
    }
}
