use std::path::Path;

use gstreamer::prelude::*;
use gstreamer_pbutils::prelude::*;
use vms_core::VmsError;

// -- Codec detection --

/// Probe the first video stream in `path` and return its GStreamer caps structure
/// name (e.g. `"video/x-h264"`, `"video/x-h265"`).
///
/// Returns `None` if the file cannot be discovered or contains no video stream.
/// Called from a blocking context only (`spawn_blocking`).
pub fn probe_video_codec(path: &Path) -> Option<String> {
    let uri = format!("file://{}", path.to_str()?);
    let timeout = gstreamer::ClockTime::from_seconds(5);
    let discoverer = gstreamer_pbutils::Discoverer::new(timeout).ok()?;
    let info = discoverer.discover_uri(&uri).ok()?;

    for stream in info.stream_list() {
        if let Ok(video) = stream.downcast::<gstreamer_pbutils::DiscovererVideoInfo>() {
            if let Some(caps) = video.caps() {
                if let Some(s) = caps.structure(0) {
                    return Some(s.name().to_string());
                }
            }
        }
    }
    None
}

/// Map a GStreamer caps structure name to an encoder element name.
///
/// `"video/x-h265"` → `x265enc`; everything else → `x264enc`.
pub fn codec_to_encoder(caps_name: &str) -> &'static str {
    match caps_name {
        "video/x-h265" => "x265enc",
        _ => "x264enc",
    }
}

// -- Pipeline helpers --

/// Connect a `decodebin` element's dynamic pads to the downstream pipeline.
///
/// - Video pads are linked to `video_sink` (must have a static `sink` pad).
/// - Audio pads are routed to a `fakesink` added on-the-fly so they do not
///   stall the pipeline when audio is not needed.
pub fn wire_decodebin(
    decode: &gstreamer::Element,
    video_sink: &gstreamer::Element,
    pipeline: &gstreamer::Pipeline,
) {
    let video_sink_weak = video_sink.downgrade();
    let pipeline_weak = pipeline.downgrade();

    decode.connect_pad_added(move |_, pad| {
        let caps = match pad.current_caps() {
            Some(c) => c,
            None => return,
        };
        let s = match caps.structure(0) {
            Some(s) => s,
            None => return,
        };

        if s.name().starts_with("video/") {
            let Some(sink) = video_sink_weak.upgrade() else {
                return;
            };
            let Some(sink_pad) = sink.static_pad("sink") else {
                return;
            };
            if !sink_pad.is_linked() {
                if let Err(e) = pad.link(&sink_pad) {
                    tracing::warn!("wire_decodebin: video link failed: {e}");
                }
            }
        } else if s.name().starts_with("audio/") {
            // Sink audio to fakesink — keeps the pipeline from stalling when
            // we only need the video stream.
            let Some(pl) = pipeline_weak.upgrade() else {
                return;
            };
            match gstreamer::ElementFactory::make("fakesink").build() {
                Ok(fs) => {
                    pl.add(&fs).ok();
                    fs.sync_state_with_parent().ok();
                    if let Some(sp) = fs.static_pad("sink") {
                        pad.link(&sp).ok();
                    }
                }
                Err(e) => tracing::warn!("wire_decodebin: could not create fakesink: {e}"),
            }
        }
    });
}

/// Wait for the pipeline to reach EOS or emit an error.
///
/// Called after `set_state(Playing)` to block until the file processing job
/// is complete. Returns `Ok(())` on EOS, `Err` on any GStreamer error message.
pub fn wait_for_eos(pipeline: &gstreamer::Pipeline) -> Result<(), VmsError> {
    use gstreamer::prelude::*;

    let bus = pipeline.bus().expect("pipeline has no bus");

    for msg in bus.iter_timed(gstreamer::ClockTime::NONE) {
        match msg.view() {
            gstreamer::MessageView::Eos(_) => return Ok(()),
            gstreamer::MessageView::Error(err) => {
                pipeline.set_state(gstreamer::State::Null).ok();
                return Err(VmsError::Media(format!("GStreamer error: {}", err.error())));
            }
            _ => {}
        }
    }

    Ok(())
}
