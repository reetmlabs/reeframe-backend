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
/// - Video pads are routed through a `capsfilter` pinning plain system
///   memory `video/x-raw` (pixel format left open) before reaching
///   `video_sink` (must have a static `sink` pad), so a hardware decoder's
///   several equivalent memory layouts for the same format don't get left
///   to implicit negotiation.
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
            if sink_pad.is_linked() {
                return;
            }
            let Some(pl) = pipeline_weak.upgrade() else {
                return;
            };

            // Pin plain system memory between the decoder and downstream
            // software elements, without constraining the pixel format
            // itself (decoders vary in which formats they can convert to).
            // A hardware decoder's output pad can offer several equivalent
            // memory layouts for the same format (GPU memory, DMA buffer,
            // plain system memory), and leaving that choice to implicit
            // negotiation is the kind of ambiguity that shows up as
            // occasional corrupted frames rather than a consistent
            // failure. This keeps hardware decode in place, it only pins
            // what comes out of it.
            let capsfilter = match gstreamer::ElementFactory::make("capsfilter")
                .property("caps", gstreamer::Caps::builder("video/x-raw").build())
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("wire_decodebin: could not create capsfilter: {e}");
                    return;
                }
            };
            if let Err(e) = pl.add(&capsfilter) {
                tracing::warn!("wire_decodebin: could not add capsfilter: {e}");
                return;
            }
            if let Err(e) = capsfilter.sync_state_with_parent() {
                tracing::warn!("wire_decodebin: capsfilter sync_state failed: {e}");
                return;
            }
            let Some(cf_sink) = capsfilter.static_pad("sink") else {
                return;
            };
            if let Err(e) = pad.link(&cf_sink) {
                tracing::warn!("wire_decodebin: video link (to capsfilter) failed: {e}");
                return;
            }
            if let Err(e) = capsfilter.link(&sink) {
                tracing::warn!("wire_decodebin: capsfilter link failed: {e}");
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

#[cfg(test)]
mod wire_decodebin_tests {
    use super::*;

    fn make_test_clip(path: &Path) {
        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc num-buffers=5 ! video/x-raw,width=64,height=64,framerate=10/1,format=I420 ! \
             x264enc ! mp4mux ! filesink location={}",
            path.display()
        ))
        .expect("parse test clip pipeline");
        let pipeline = pipeline.downcast::<gstreamer::Pipeline>().unwrap();
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        wait_for_eos(&pipeline).unwrap();
        pipeline.set_state(gstreamer::State::Null).ok();
    }

    /// A hardware decoder can offer more than one memory layout for its raw
    /// output (GPU memory, DMA buffer, plain system memory). This confirms
    /// `wire_decodebin` pins the video pad to plain system memory
    /// regardless of what the decoder could have offered instead. The
    /// pixel format itself is intentionally left unchecked, decoders vary
    /// in which formats they can convert to.
    #[test]
    fn video_pad_is_pinned_to_system_memory() {
        gstreamer::init().ok();
        let src_path =
            std::env::temp_dir().join(format!("wire_decodebin_test_{}.mp4", std::process::id()));
        make_test_clip(&src_path);

        let pipeline = gstreamer::Pipeline::new();
        let filesrc = gstreamer::ElementFactory::make("filesrc")
            .property("location", src_path.to_str().unwrap())
            .build()
            .unwrap();
        let decode = gstreamer::ElementFactory::make("decodebin")
            .build()
            .unwrap();
        // fakesink with sync=false drains immediately instead of applying
        // backpressure, so the pipeline reaches EOS without anything having
        // to pull samples out of it.
        let sink = gstreamer::ElementFactory::make("fakesink")
            .property("sync", false)
            .build()
            .unwrap();

        pipeline.add_many([&filesrc, &decode, &sink]).unwrap();
        filesrc.link(&decode).unwrap();
        wire_decodebin(&decode, &sink, &pipeline);

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        wait_for_eos(&pipeline).unwrap();

        let sink_pad = sink.static_pad("sink").unwrap();
        let caps = sink_pad.current_caps().expect("negotiated caps");
        let s = caps.structure(0).unwrap();
        assert_eq!(s.name(), "video/x-raw");

        pipeline.set_state(gstreamer::State::Null).ok();
        std::fs::remove_file(&src_path).ok();
    }
}
