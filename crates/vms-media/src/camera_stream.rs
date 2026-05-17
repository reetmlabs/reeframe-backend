use std::{path::PathBuf, time::Duration};

use gstreamer::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

// ── Supported codecs ──────────────────────────────────────────────────────────

struct CodecElements {
    depay_factory: &'static str,
    parse_factory: &'static str,
}

fn codec_for(encoding_name: &str) -> Option<CodecElements> {
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

// ── Camera stream builder ─────────────────────────────────────────────────────

/// Build a per-camera GStreamer pipeline for continuous recording.
///
/// The codec is **not** assumed at build time. When `rtspsrc` connects to the
/// camera and exposes an RTP src pad, the `pad-added` callback reads the
/// `encoding-name` field from the SDP caps and inserts the appropriate
/// depayloader + parser into the running pipeline:
///
/// ```text
/// rtspsrc ──(pad-added)──► [rtph264depay|rtph265depay] ──► [h264parse|h265parse]
///                                                                      │
///                                                                      ▼
///                                                      tee ──► queue ──► splitmuxsink (MP4)
/// ```
///
/// Elements that vary per-camera are named `cam_{id}_{role}` so the reconnect
/// monitor can look them up by name instead of recreating them.
pub(crate) fn build_camera_stream(
    camera_id: Uuid,
    rtsp_url: &str,
    recording_dir: &std::path::Path,
    chunk_duration_secs: u64,
) -> Result<gstreamer::Pipeline, VmsError> {
    let gst_pipeline = gstreamer::Pipeline::new();

    // ── rtspsrc ───────────────────────────────────────────────────────────────
    let src = gstreamer::ElementFactory::make("rtspsrc")
        .name(format!("cam_{}_src", camera_id.as_simple()))
        .property("location", rtsp_url)
        .property("latency", 200u32)
        .build()
        .map_err(|e| VmsError::Media(format!("rtspsrc: {e}")))?;

    // ── Tee (fan-out point — recording branch now, ring buffer / analytics later)
    let tee = gstreamer::ElementFactory::make("tee")
        .name(format!("cam_{}_tee", camera_id.as_simple()))
        .build()
        .map_err(|e| VmsError::Media(format!("tee: {e}")))?;

    // ── Recording branch: queue → splitmuxsink ────────────────────────────────
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
        .property("muxer-factory", "mp4mux")
        .build()
        .map_err(|e| VmsError::Media(format!("splitmuxsink: {e}")))?;

    // ── Assemble static part of the pipeline ──────────────────────────────────
    // Depayloader + parser are NOT added here — they are created dynamically
    // in the pad-added callback once we know the codec from the camera's SDP.
    gst_pipeline
        .add_many([&src, &tee, &queue, &splitmux])
        .map_err(|e| VmsError::Media(format!("add_many: {e}")))?;

    // tee ──► queue ──► splitmux (recording branch)
    let tee_src = tee
        .request_pad_simple("src_%u")
        .ok_or_else(|| VmsError::Media("tee: no src_%u pad template".into()))?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("queue: no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee→queue: {e}")))?;

    queue
        .link(&splitmux)
        .map_err(|e| VmsError::Media(format!("link queue→splitmux: {e}")))?;

    // ── Dynamic codec wiring ──────────────────────────────────────────────────
    // rtspsrc only exposes src pads after it receives the SDP from the camera,
    // so we must wire the depay+parse chain at pad-added time.
    //
    // On reconnect: rtspsrc removes and re-adds its src pad.  The depay/parse
    // elements already exist in the pipeline (added on first connection), so we
    // only need to re-link the rtspsrc src pad to the depay sink.
    let pipeline_weak = gst_pipeline.downgrade();
    let tee_weak = tee.downgrade();
    let cam_id = camera_id;

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

        let Some(gst_pipeline) = pipeline_weak.upgrade() else {
            return;
        };
        let Some(tee) = tee_weak.upgrade() else { return };

        let depay_name = format!("cam_{}_depay", cam_id.as_simple());
        let parse_name = format!("cam_{}_parse", cam_id.as_simple());

        // ── Reconnect path: elements exist, just re-link the src pad ─────────
        if let Some(depay) = gst_pipeline.by_name(&depay_name) {
            let sink = match depay.static_pad("sink") {
                Some(p) => p,
                None => return,
            };
            if !sink.is_linked() {
                if let Err(e) = src_pad.link(&sink) {
                    tracing::error!(camera_id = %cam_id, "re-link rtspsrc→depay: {e}");
                }
            }
            return;
        }

        // ── First connection: create depay + parse for the negotiated codec ───
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

        // depay → parse → tee
        if let Err(e) = gstreamer::Element::link_many([&depay, &parse, &tee]) {
            tracing::error!(camera_id = %cam_id, "link depay→parse→tee: {e}");
            return;
        }

        // Bring new elements up to the pipeline's current state
        for el in [&depay, &parse] {
            el.sync_state_with_parent().ok();
        }

        // Link rtspsrc src pad → depay sink
        let sink = match depay.static_pad("sink") {
            Some(p) => p,
            None => return,
        };
        if let Err(e) = src_pad.link(&sink) {
            tracing::error!(camera_id = %cam_id, encoding, "rtspsrc→depay link: {e}");
            return;
        }

        tracing::info!(camera_id = %cam_id, encoding, "Codec wired");
    });

    Ok(gst_pipeline)
}

// ── Reconnect monitor task ────────────────────────────────────────────────────

/// Spawn a tokio task that watches the GStreamer bus and reconnects on error/EOS.
///
/// Backoff: 2 s → 4 s → … → 60 s cap, reset to 2 s after a successful restart.
/// On each reconnect the splitmuxsink location gets a fresh timestamp so chunks
/// from different sessions never collide on disk.
pub(crate) fn spawn_monitor(
    camera_id: Uuid,
    gst_pipeline: gstreamer::Pipeline,
    recording_dir: PathBuf,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let bus = gst_pipeline.bus().expect("pipeline bus missing");
        let mut backoff = Duration::from_secs(2);
        const MAX_BACKOFF: Duration = Duration::from_secs(60);

        loop {
            // ── Shutdown check ────────────────────────────────────────────────
            match shutdown_rx.try_recv() {
                Ok(_) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => break,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }

            // ── Drain bus ─────────────────────────────────────────────────────
            let mut needs_reconnect = false;

            while let Some(msg) = bus.pop() {
                use gstreamer::MessageView;
                match msg.view() {
                    MessageView::Error(err) => {
                        tracing::error!(
                            camera_id = %camera_id,
                            error = %err.error(),
                            debug = ?err.debug(),
                            "GStreamer error — will reconnect",
                        );
                        needs_reconnect = true;
                        break;
                    }
                    MessageView::Eos(_) => {
                        tracing::warn!(camera_id = %camera_id, "RTSP stream EOS — will reconnect");
                        needs_reconnect = true;
                        break;
                    }
                    MessageView::Warning(w) => {
                        tracing::warn!(
                            camera_id = %camera_id,
                            warning = %w.error(),
                            "GStreamer warning",
                        );
                    }
                    _ => {}
                }
            }

            if needs_reconnect {
                gst_pipeline.set_state(gstreamer::State::Null).ok();

                // Fresh timestamp prefix → no chunk filename collisions
                let splitmux_name = format!("cam_{}_splitmux", camera_id.as_simple());
                if let Some(splitmux) = gst_pipeline.by_name(&splitmux_name) {
                    splitmux
                        .set_property("location", recording_location(&recording_dir, camera_id));
                }

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
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        gst_pipeline.set_state(gstreamer::State::Null).ok();
        tracing::info!(camera_id = %camera_id, "Stream monitor exited");
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Unique chunk file location string for a recording session.
///
/// Example: `/var/recordings/cam_<id>_20260509T143022_chunk%05d.mp4`
pub(crate) fn recording_location(base_dir: &std::path::Path, camera_id: Uuid) -> String {
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S");
    base_dir
        .join(format!(
            "cam_{}_{}_chunk%05d.mp4",
            camera_id.as_simple(),
            ts
        ))
        .to_string_lossy()
        .to_string()
}
