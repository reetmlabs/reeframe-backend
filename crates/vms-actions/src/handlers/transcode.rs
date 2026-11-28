use std::path::Path;

use gstreamer::prelude::*;
use vms_core::{
    action::TranscodeConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    video_encoding::{parse_resolution, H264Complexity, TranscodeCodec},
    VmsError,
};

use super::gst as gst_util;
use crate::dispatcher::ActionContext;

// -- Handler --

/// Re-encode the upstream artifact using GStreamer.
///
/// Pipeline: `filesrc → decodebin → videoconvert → [videoscale → capsfilter] →
/// encoder → muxer → filesink`
///
/// The scaler is there when `resolution` is set, or when an H.264 output
/// would be larger than the encoder accepts. Audio is dropped (routed to
/// fakesink). Only the video stream is transcoded.
pub async fn execute(
    node_id: NodeId,
    cfg: &TranscodeConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    let Some(input_path) = input.first_artifact() else {
        return NodeOutput::failure(node_id, "transcode: no upstream artifact");
    };

    if let Err(e) = tokio::fs::create_dir_all(&ctx.recording_dir).await {
        return NodeOutput::failure(node_id, format!("transcode: create output dir: {e}"));
    }

    let stem = input_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("clip");
    let output_path = ctx
        .recording_dir
        .join(format!("{stem}_transcoded.{}", cfg.output_format));

    let cfg_clone = cfg.clone();
    let in_path = input_path.clone();
    let out_path = output_path.clone();

    let result =
        tokio::task::spawn_blocking(move || transcode_blocking(&in_path, &cfg_clone, &out_path))
            .await
            .map_err(|e| format!("transcode task panic: {e}"))
            .and_then(|r| r.map_err(|e| e.to_string()));

    match result {
        Ok(()) => {
            tracing::info!(node_id = %node_id, path = %output_path.display(), "Transcode: done");
            NodeOutput::success(node_id).with_artifact(output_path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("transcode: {e}")),
    }
}

// -- Blocking implementation --

fn transcode_blocking(input: &Path, cfg: &TranscodeConfig, output: &Path) -> Result<(), VmsError> {
    gstreamer::init().ok();

    let Some(input_str) = input.to_str() else {
        return Err(VmsError::Media("transcode: non-UTF-8 input path".into()));
    };
    let Some(output_str) = output.to_str() else {
        return Err(VmsError::Media("transcode: non-UTF-8 output path".into()));
    };

    let pipeline = gstreamer::Pipeline::new();

    let src = gstreamer::ElementFactory::make("filesrc")
        .property("location", input_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesrc: {e}")))?;

    let decode = gstreamer::ElementFactory::make("decodebin")
        .build()
        .map_err(|e| VmsError::Media(format!("decodebin: {e}")))?;

    let convert = gstreamer::ElementFactory::make("videoconvert")
        .build()
        .map_err(|e| VmsError::Media(format!("videoconvert: {e}")))?;

    vms_core::video_encoding::check_transcode(cfg).map_err(VmsError::Media)?;
    let codec = TranscodeCodec::parse(&cfg.codec).map_err(VmsError::Media)?;
    let size = match &cfg.resolution {
        Some(res) => Some(parse_resolution(res).map_err(VmsError::Media)?),
        None if codec == TranscodeCodec::H264 => gst_util::probe_video(input)
            .and_then(|p| gst_util::h264_fit("transcode", p.width, p.height)),
        None => None,
    };

    let encoder = make_encoder(codec, cfg)?;
    let muxer = make_muxer(&cfg.output_format)?;

    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesink: {e}")))?;

    // -- Assemble pipeline --
    // convert → [scale → capsfilter] → encoder → muxer → sink
    let mut chain = vec![convert.clone()];
    if let Some((w, h)) = size {
        let (scale, filter) = gst_util::make_scaler(w, h)?;
        chain.extend([scale, filter]);
    }
    chain.extend([encoder, muxer, sink]);

    pipeline
        .add_many([&src, &decode])
        .and_then(|()| pipeline.add_many(&chain))
        .map_err(|e| VmsError::Media(format!("add elements: {e}")))?;
    gstreamer::Element::link_many(&chain)
        .map_err(|e| VmsError::Media(format!("link chain: {e}")))?;

    src.link(&decode)
        .map_err(|e| VmsError::Media(format!("link src→decode: {e}")))?;

    gst_util::wire_decodebin(&decode, &convert, &pipeline);

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("set playing: {e}")))?;

    gst_util::wait_for_eos(&pipeline)?;

    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Helpers --

fn make_encoder(
    codec: TranscodeCodec,
    cfg: &TranscodeConfig,
) -> Result<gstreamer::Element, VmsError> {
    let bitrate_bps = cfg.bitrate_kbps.saturating_mul(1000);
    let builder = match codec {
        TranscodeCodec::H264 => {
            let complexity = H264Complexity::from_preset(&cfg.preset).map_err(VmsError::Media)?;
            return gst_util::make_h264_encoder(bitrate_bps, complexity);
        }
        // `vp9enc` takes a signed bits per second value, `av1enc` kilobits.
        TranscodeCodec::Vp9 => gstreamer::ElementFactory::make("vp9enc").property(
            "target-bitrate",
            i32::try_from(bitrate_bps).unwrap_or(i32::MAX),
        ),
        TranscodeCodec::Av1 => {
            gstreamer::ElementFactory::make("av1enc").property("target-bitrate", cfg.bitrate_kbps)
        }
    };
    builder
        .build()
        .map_err(|e| VmsError::Media(format!("{codec:?} encoder: {e}")))
}

fn make_muxer(format: &str) -> Result<gstreamer::Element, VmsError> {
    let factory = match format {
        "mp4" | "m4v" => "mp4mux",
        "mkv" => "matroskamux",
        "avi" => "avimux",
        "ts" | "mpeg-ts" => "mpegtsmux",
        "webm" => "webmmux",
        other => {
            tracing::warn!("transcode: unknown format '{other}', falling back to mp4mux");
            "mp4mux"
        }
    };

    gstreamer::ElementFactory::make(factory)
        .build()
        .map_err(|e| VmsError::Media(format!("{factory}: {e}")))
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn config(codec: &str, resolution: Option<&str>) -> TranscodeConfig {
        TranscodeConfig {
            codec: codec.into(),
            bitrate_kbps: 1000,
            resolution: resolution.map(Into::into),
            preset: "veryfast".into(),
            output_format: "mp4".into(),
        }
    }

    fn output(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("transcode_{name}_{}.mp4", std::process::id()))
    }

    #[test]
    fn a_large_h265_source_is_transcoded_to_scaled_h264() {
        let out = output("h265_to_h264");
        transcode_blocking(&gst_util::large_h265_clip(), &config("h264", None), &out).unwrap();
        gst_util::assert_scaled_h264(&out);
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn an_explicit_resolution_is_used() {
        let out = output("resolution");
        let cfg = config("h264", Some("320x240"));
        transcode_blocking(&gst_util::large_h265_clip(), &cfg, &out).unwrap();
        let probe = gst_util::probe_video(&out).unwrap();
        assert_eq!((probe.width, probe.height), (320, 240));
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn vp9_output_still_works() {
        let out = output("vp9");
        let cfg = TranscodeConfig {
            output_format: "webm".into(),
            ..config("vp9", Some("320x240"))
        };
        transcode_blocking(&gst_util::large_h265_clip(), &cfg, &out).unwrap();
        assert_eq!(
            gst_util::probe_video_codec(&out).as_deref(),
            Some("video/x-vp9")
        );
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn unsupported_settings_fail_before_encoding() {
        for (cfg, expected) in [
            (config("h265", None), "H.265 output is not available"),
            (config("x264enc", None), "unknown codec 'x264enc'"),
            (config("h264", Some("8192x4320")), "too large for H.264"),
        ] {
            let out = output("refused");
            let err = transcode_blocking(&gst_util::large_h265_clip(), &cfg, &out).unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
            assert!(!out.exists());
        }
    }
}
