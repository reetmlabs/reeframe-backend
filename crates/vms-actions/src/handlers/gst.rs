use std::path::Path;

use gstreamer::prelude::*;
use gstreamer_pbutils::prelude::*;
use vms_core::video_encoding::{
    check_encode_bitrate, default_h264_bitrate_kbps, fit_h264_frame, H264Complexity,
};
use vms_core::VmsError;

// -- Codec detection --

/// The first video stream of a file: its caps structure name (e.g.
/// `"video/x-h264"`) and frame size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoProbe {
    pub caps_name: String,
    pub width: u32,
    pub height: u32,
}

/// Probe the first video stream in `path`.
///
/// Returns `None` if the file cannot be discovered or contains no video stream.
/// Called from a blocking context only (`spawn_blocking`).
pub fn probe_video(path: &Path) -> Option<VideoProbe> {
    let uri = format!("file://{}", path.to_str()?);
    let timeout = gstreamer::ClockTime::from_seconds(5);
    let discoverer = gstreamer_pbutils::Discoverer::new(timeout).ok()?;
    let info = discoverer.discover_uri(&uri).ok()?;

    for stream in info.stream_list() {
        if let Ok(video) = stream.downcast::<gstreamer_pbutils::DiscovererVideoInfo>() {
            let caps_name = video.caps()?.structure(0)?.name().to_string();
            return Some(VideoProbe {
                caps_name,
                width: video.width(),
                height: video.height(),
            });
        }
    }
    None
}

/// The caps structure name of the first video stream in `path` (e.g.
/// `"video/x-h264"`, `"video/x-h265"`, `"video/x-av1"`). See [`probe_video`].
#[cfg(test)]
pub fn probe_video_codec(path: &Path) -> Option<String> {
    probe_video(path).map(|p| p.caps_name)
}

// -- Encoders --

/// A bin of `openh264enc` encoding at `bitrate_bps` and the `h264parse` that
/// lets any muxer take its output. Fails with a clear error if the element
/// (gst-plugins-bad with libopenh264) is not installed.
///
/// `qp-min=1` lets the rate control use the whole quantizer range; at the
/// default of 0 OpenH264 overshoots the bitrate several times over. Frames are
/// never skipped, so very noisy video can still exceed the bitrate.
pub fn make_h264_encoder(
    bitrate_bps: u32,
    complexity: H264Complexity,
) -> Result<gstreamer::Element, VmsError> {
    if gstreamer::ElementFactory::find("openh264enc").is_none() {
        return Err(VmsError::Media(
            "H.264 encoding needs the GStreamer element openh264enc \
             (gstreamer1.0-plugins-bad with libopenh264), which is not available"
                .into(),
        ));
    }
    let encoder = gstreamer::ElementFactory::make("openh264enc")
        .property_from_str("rate-control", "bitrate")
        .property("bitrate", bitrate_bps)
        .property("qp-min", 1u32)
        .property_from_str("complexity", complexity.as_str())
        .property_from_str("slice-mode", "auto")
        .build()
        .map_err(|e| VmsError::Media(format!("openh264enc: {e}")))?;
    let parse = gstreamer::ElementFactory::make("h264parse")
        .build()
        .map_err(|e| VmsError::Media(format!("h264parse: {e}")))?;

    let bin = gstreamer::Bin::new();
    bin.add_many([&encoder, &parse])
        .and_then(|()| encoder.link(&parse))
        .map_err(|e| VmsError::Media(format!("H.264 encoder bin: {e}")))?;
    for (element, pad) in [(&encoder, "sink"), (&parse, "src")] {
        let target = element.static_pad(pad).expect("static pad");
        let ghost = gstreamer::GhostPad::builder_with_target(&target)
            .map_err(|e| VmsError::Media(format!("H.264 encoder bin: {e}")))?
            .name(pad)
            .build();
        bin.add_pad(&ghost)
            .map_err(|e| VmsError::Media(format!("H.264 encoder bin: {e}")))?;
    }
    Ok(bin.upcast())
}

/// The encoder of a `watermark` or `merge_clips` job, and the size to scale
/// the video to before it, if any.
pub struct OutputEncoder {
    pub encoder: gstreamer::Element,
    pub scale_to: Option<(u32, u32)>,
}

/// The encoder for a job that keeps the source codec where it can: AV1 stays
/// AV1 (`svtav1enc`), everything else, H.265 included, is encoded to H.264.
/// H.264 is encoded at `bitrate_kbps`, or at a default for the output frame
/// size (1920x1080 when `probe` is `None`), after scaling to fit the encoder.
pub fn make_output_encoder(
    job: &str,
    probe: Option<&VideoProbe>,
    bitrate_kbps: Option<u32>,
) -> Result<OutputEncoder, VmsError> {
    check_encode_bitrate(bitrate_kbps).map_err(VmsError::Media)?;
    if probe.is_some_and(|p| !output_is_h264(&p.caps_name)) {
        let encoder = gstreamer::ElementFactory::make("svtav1enc")
            .build()
            .map_err(|e| VmsError::Media(format!("svtav1enc: {e}")))?;
        return Ok(OutputEncoder {
            encoder,
            scale_to: None,
        });
    }
    let scale_to = probe.and_then(|p| h264_fit(job, p.width, p.height));
    let (width, height) = scale_to
        .or(probe.map(|p| (p.width, p.height)))
        .unwrap_or((1920, 1080));
    let kbps = bitrate_kbps.unwrap_or_else(|| default_h264_bitrate_kbps(width, height));
    tracing::debug!(job, kbps, size = %format!("{width}x{height}"), "H.264 output bitrate");
    let encoder = make_h264_encoder(kbps * 1000, H264Complexity::Medium)?;
    Ok(OutputEncoder { encoder, scale_to })
}

/// Whether [`make_output_encoder`] encodes `caps_name` to H.264.
pub fn output_is_h264(caps_name: &str) -> bool {
    caps_name != "video/x-av1"
}

/// The size an H.264 output of `width`x`height` has to be scaled to, or
/// `None` if it fits the encoder. Logs when scaling is needed.
pub fn h264_fit(job: &str, width: u32, height: u32) -> Option<(u32, u32)> {
    let (w, h) = fit_h264_frame(width, height)?;
    tracing::info!(
        job,
        from = %format!("{width}x{height}"),
        to = %format!("{w}x{h}"),
        "Scaling the video down to fit the H.264 encoder's frame size limit",
    );
    Some((w, h))
}

/// A `videoscale` and the `capsfilter` after it that scale raw video to
/// `width`x`height`.
pub fn make_scaler(
    width: u32,
    height: u32,
) -> Result<(gstreamer::Element, gstreamer::Element), VmsError> {
    let scale = gstreamer::ElementFactory::make("videoscale")
        .build()
        .map_err(|e| VmsError::Media(format!("videoscale: {e}")))?;
    let caps = gstreamer::Caps::builder("video/x-raw")
        .field("width", width as i32)
        .field("height", height as i32)
        .build();
    let filter = gstreamer::ElementFactory::make("capsfilter")
        .property("caps", &caps)
        .build()
        .map_err(|e| VmsError::Media(format!("capsfilter: {e}")))?;
    Ok((scale, filter))
}

/// Map a GStreamer caps structure name to a parser element name.
///
/// `"video/x-h265"` → `h265parse`, `"video/x-av1"` → `av1parse`; everything
/// else → `h264parse`.
pub fn codec_to_parser(caps_name: &str) -> &'static str {
    match caps_name {
        "video/x-h265" => "h265parse",
        "video/x-av1" => "av1parse",
        _ => "h264parse",
    }
}

/// Map a GStreamer caps structure name to a software decoder element name.
///
/// `"video/x-h265"` → `avdec_h265`, `"video/x-av1"` → `av1dec`; everything
/// else → `avdec_h264`. Used where a hardware decoder is known to misbehave
/// with a specific downstream element (see `watermark.rs`), so decodebin's
/// own autoplug (which can pick a hardware decoder) is bypassed in favor of
/// an explicit software one.
pub fn codec_to_software_decoder(caps_name: &str) -> &'static str {
    match caps_name {
        "video/x-h265" => "avdec_h265",
        "video/x-av1" => "av1dec",
        _ => "avdec_h264",
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
            // plain system memory). Leaving that choice to implicit
            // negotiation can cause occasional corrupted frames. Hardware
            // decoding still happens; only its output memory is pinned.
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
            // Sink audio to fakesink so the pipeline doesn't stall when only
            // the video stream is needed.
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

/// Connect a demuxer element's (e.g. `qtdemux`) dynamic pads to an explicit
/// parse/decode chain, bypassing decodebin's own decoder autoplug.
///
/// Unlike [`wire_decodebin`], `video_sink` here receives still-encoded
/// caps (a parser, not a raw-video sink), so no capsfilter is inserted.
/// Audio pads are routed to a `fakesink` the same way.
pub fn wire_demux_video(
    demux: &gstreamer::Element,
    video_sink: &gstreamer::Element,
    pipeline: &gstreamer::Pipeline,
) {
    let video_sink_weak = video_sink.downgrade();
    let pipeline_weak = pipeline.downgrade();

    demux.connect_pad_added(move |_, pad| {
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
            if let Err(e) = pad.link(&sink_pad) {
                tracing::warn!("wire_demux_video: video link failed: {e}");
            }
        } else if s.name().starts_with("audio/") {
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
                Err(e) => tracing::warn!("wire_demux_video: could not create fakesink: {e}"),
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
fn make_test_clip(path: &Path) {
    make_test_clip_with(path, "openh264enc ! h264parse");
}

/// Writes a short 64x64 MP4 test clip encoded with `encoder`.
#[cfg(test)]
pub(crate) fn make_test_clip_with(path: &Path, encoder: &str) {
    let pipeline = gstreamer::parse::launch(&format!(
        "videotestsrc num-buffers=5 ! video/x-raw,width=64,height=64,framerate=10/1,format=I420 ! \
         {encoder} ! mp4mux ! filesink location={}",
        path.display()
    ))
    .expect("parse test clip pipeline");
    let pipeline = pipeline.downcast::<gstreamer::Pipeline>().unwrap();
    pipeline.set_state(gstreamer::State::Playing).unwrap();
    wait_for_eos(&pipeline).unwrap();
    pipeline.set_state(gstreamer::State::Null).ok();
}

/// A 3-frame 4000x3000 H.265 clip, larger than the H.264 encoder accepts.
#[cfg(test)]
pub(crate) fn large_h265_clip() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/h265_4000x3000.mp4")
}

/// Asserts that `path` is H.264 scaled down from [`large_h265_clip`]: even
/// sizes within the encoder's limit, at the source's 4:3 aspect ratio.
#[cfg(test)]
pub(crate) fn assert_scaled_h264(path: &Path) {
    let probe = probe_video(path).expect("a video stream");
    assert_eq!(probe.caps_name, "video/x-h264");
    let (w, h) = (probe.width, probe.height);
    assert!(vms_core::video_encoding::fits_h264(w, h), "{w}x{h}");
    assert_eq!((w % 2, h % 2), (0, 0), "{w}x{h}");
    assert!(
        (f64::from(w) / f64::from(h) - 4.0 / 3.0).abs() < 0.01,
        "{w}x{h}"
    );
}

#[cfg(test)]
mod wire_decodebin_tests {
    use super::*;

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

#[cfg(test)]
mod wire_demux_video_tests {
    use super::*;

    /// `wire_demux_video` links a demuxer's video pad straight to an
    /// explicit parse/decode chain, the path `watermark.rs` uses to avoid a
    /// hardware decoder. This confirms that wiring actually reaches EOS
    /// end to end (demux, parse, decode, and a sink all correctly linked).
    #[test]
    fn video_pad_reaches_an_explicit_decode_chain() {
        gstreamer::init().ok();
        let src_path =
            std::env::temp_dir().join(format!("wire_demux_test_{}.mp4", std::process::id()));
        make_test_clip(&src_path);

        let pipeline = gstreamer::Pipeline::new();
        let filesrc = gstreamer::ElementFactory::make("filesrc")
            .property("location", src_path.to_str().unwrap())
            .build()
            .unwrap();
        let demux = gstreamer::ElementFactory::make("qtdemux").build().unwrap();
        let parse = gstreamer::ElementFactory::make("h264parse")
            .build()
            .unwrap();
        let decoder = gstreamer::ElementFactory::make("avdec_h264")
            .build()
            .unwrap();
        let sink = gstreamer::ElementFactory::make("fakesink")
            .property("sync", false)
            .build()
            .unwrap();

        pipeline
            .add_many([&filesrc, &demux, &parse, &decoder, &sink])
            .unwrap();
        filesrc.link(&demux).unwrap();
        gstreamer::Element::link_many([&parse, &decoder, &sink]).unwrap();
        wire_demux_video(&demux, &parse, &pipeline);

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        wait_for_eos(&pipeline).unwrap();

        let caps = sink
            .static_pad("sink")
            .unwrap()
            .current_caps()
            .expect("negotiated caps");
        assert_eq!(caps.structure(0).unwrap().name(), "video/x-raw");

        pipeline.set_state(gstreamer::State::Null).ok();
        std::fs::remove_file(&src_path).ok();
    }
}

#[cfg(test)]
mod codec_mapping_tests {
    use super::*;

    #[test]
    fn only_av1_keeps_its_codec() {
        assert!(output_is_h264("video/x-h264"));
        assert!(output_is_h264("video/x-h265"));
        assert!(output_is_h264("video/x-vp9"));
        assert!(!output_is_h264("video/x-av1"));
    }

    fn h264_bitrate(output: &OutputEncoder) -> u32 {
        let bin = output.encoder.downcast_ref::<gstreamer::Bin>().unwrap();
        let encoder = bin
            .iterate_elements()
            .find(|e| e.factory().is_some_and(|f| f.name() == "openh264enc"))
            .unwrap();
        encoder.property("bitrate")
    }

    fn probe(caps_name: &str, width: u32, height: u32) -> VideoProbe {
        VideoProbe {
            caps_name: caps_name.into(),
            width,
            height,
        }
    }

    #[test]
    fn the_h264_bitrate_defaults_from_the_output_size() {
        gstreamer::init().unwrap();
        let uhd = probe("video/x-h265", 3840, 2160);
        let output = make_output_encoder("test", Some(&uhd), None).unwrap();
        assert_eq!(output.scale_to, None);
        assert_eq!(h264_bitrate(&output), 12_000_000);

        let output = make_output_encoder("test", None, None).unwrap();
        assert_eq!(h264_bitrate(&output), 4_000_000);

        let large = probe("video/x-h264", 4000, 3000);
        let output = make_output_encoder("test", Some(&large), None).unwrap();
        let (w, h) = output.scale_to.unwrap();
        assert_eq!(
            h264_bitrate(&output),
            default_h264_bitrate_kbps(w, h) * 1000
        );
    }

    #[test]
    fn an_explicit_bitrate_is_used_and_checked() {
        gstreamer::init().unwrap();
        let hd = probe("video/x-h264", 1920, 1080);
        let output = make_output_encoder("test", Some(&hd), Some(8000)).unwrap();
        assert_eq!(h264_bitrate(&output), 8_000_000);

        let err = make_output_encoder("test", Some(&hd), Some(50))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("out of range"), "{err}");
    }

    #[test]
    fn av1_maps_to_av1_elements() {
        assert_eq!(codec_to_parser("video/x-av1"), "av1parse");
        assert_eq!(codec_to_software_decoder("video/x-av1"), "av1dec");
        assert!(!output_is_h264("video/x-av1"));
    }
}
