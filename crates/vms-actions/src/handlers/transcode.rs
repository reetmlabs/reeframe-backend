use std::path::Path;

use gstreamer::prelude::*;
use vms_core::{
    action::TranscodeConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    VmsError,
};

use crate::dispatcher::ActionContext;
use super::gst as gst_util;

// -- Handler --

/// Re-encode the upstream artifact using GStreamer.
///
/// Pipeline: `filesrc → decodebin → videoconvert → [videoscale → capsfilter] →
/// encoder → muxer → filesink`
///
/// Audio is dropped (routed to fakesink). Only the video stream is transcoded.
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

    let result = tokio::task::spawn_blocking(move || {
        transcode_blocking(&in_path, &cfg_clone, &out_path)
    })
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

fn transcode_blocking(
    input: &Path,
    cfg: &TranscodeConfig,
    output: &Path,
) -> Result<(), VmsError> {
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

    let encoder = make_encoder(cfg)?;
    let muxer = make_muxer(&cfg.output_format)?;

    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesink: {e}")))?;

    // -- Assemble pipeline --
    // With optional resolution override: convert → [scale → capsfilter] → encoder → muxer → sink
    if let Some(ref res) = cfg.resolution {
        let (w, h) = parse_resolution(res)?;

        let scale = gstreamer::ElementFactory::make("videoscale")
            .build()
            .map_err(|e| VmsError::Media(format!("videoscale: {e}")))?;

        let caps = gstreamer::Caps::builder("video/x-raw")
            .field("width", w)
            .field("height", h)
            .build();
        let filter = gstreamer::ElementFactory::make("capsfilter")
            .property("caps", &caps)
            .build()
            .map_err(|e| VmsError::Media(format!("capsfilter: {e}")))?;

        pipeline
            .add_many([&src, &decode, &convert, &scale, &filter, &encoder, &muxer, &sink])
            .map_err(|e| VmsError::Media(format!("add elements: {e}")))?;

        gstreamer::Element::link_many([&convert, &scale, &filter, &encoder, &muxer, &sink])
            .map_err(|e| VmsError::Media(format!("link chain: {e}")))?;
    } else {
        pipeline
            .add_many([&src, &decode, &convert, &encoder, &muxer, &sink])
            .map_err(|e| VmsError::Media(format!("add elements: {e}")))?;

        gstreamer::Element::link_many([&convert, &encoder, &muxer, &sink])
            .map_err(|e| VmsError::Media(format!("link chain: {e}")))?;
    }

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

fn make_encoder(cfg: &TranscodeConfig) -> Result<gstreamer::Element, VmsError> {
    let (factory, bitrate_prop, bitrate_val, has_preset) = match cfg.codec.as_str() {
        "h264" => ("x264enc", "bitrate", cfg.bitrate_kbps, true),
        "h265" | "hevc" => ("x265enc", "bitrate", cfg.bitrate_kbps, true),
        "vp9" => ("vp9enc", "target-bitrate", cfg.bitrate_kbps * 1000, false),
        "av1" => ("av1enc", "target-bitrate", cfg.bitrate_kbps, false),
        other => {
            tracing::warn!("transcode: unknown codec '{other}', falling back to x264enc");
            ("x264enc", "bitrate", cfg.bitrate_kbps, true)
        }
    };

    let mut builder = gstreamer::ElementFactory::make(factory)
        .property(bitrate_prop, bitrate_val);

    if has_preset && !cfg.preset.is_empty() {
        builder = builder.property_from_str("speed-preset", &cfg.preset);
    }

    builder
        .build()
        .map_err(|e| VmsError::Media(format!("{factory}: {e}")))
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

fn parse_resolution(res: &str) -> Result<(i32, i32), VmsError> {
    let parts: Vec<&str> = res.split('x').collect();
    if parts.len() != 2 {
        return Err(VmsError::Media(format!(
            "invalid resolution '{res}' — expected WxH (e.g. 1920x1080)"
        )));
    }
    let w = parts[0]
        .trim()
        .parse::<i32>()
        .map_err(|_| VmsError::Media(format!("invalid width in resolution '{res}'")))?;
    let h = parts[1]
        .trim()
        .parse::<i32>()
        .map_err(|_| VmsError::Media(format!("invalid height in resolution '{res}'")))?;
    Ok((w, h))
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resolution_valid() {
        assert_eq!(parse_resolution("1920x1080").unwrap(), (1920, 1080));
        assert_eq!(parse_resolution("1280x720").unwrap(), (1280, 720));
    }

    #[test]
    fn parse_resolution_invalid() {
        assert!(parse_resolution("bad").is_err());
        assert!(parse_resolution("1920").is_err());
    }
}
