use std::path::Path;

use gstreamer::prelude::*;
use minijinja::Environment;
use vms_core::{
    action::{WatermarkConfig, WatermarkPosition},
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    VmsError,
};

use super::gst as gst_util;
use crate::dispatcher::ActionContext;

// -- Handler --

/// Burn a text watermark onto every frame of the upstream video artifact.
///
/// Pipeline: `filesrc → decodebin → videoconvert → textoverlay → videoconvert →
/// x264enc → mp4mux → filesink`
///
/// The text is a minijinja template rendered with `TriggerContext` variables.
/// Audio is dropped. Output is always H.264/MP4.
pub async fn execute(
    node_id: NodeId,
    cfg: &WatermarkConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    let Some(input_path) = input.first_artifact() else {
        return NodeOutput::failure(node_id, "watermark: no upstream artifact");
    };

    if let Err(e) = tokio::fs::create_dir_all(&ctx.recording_dir).await {
        return NodeOutput::failure(node_id, format!("watermark: create output dir: {e}"));
    }

    // -- Render template --
    let text = match render_text(&cfg.text_template, input) {
        Ok(t) => t,
        Err(e) => return NodeOutput::failure(node_id, format!("watermark template: {e}")),
    };

    let stem = input_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("clip");
    let output_path = ctx.recording_dir.join(format!("{stem}_watermarked.mp4"));

    let cfg_clone = cfg.clone();
    let in_path = input_path.clone();
    let out_path = output_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        watermark_blocking(&in_path, &text, &cfg_clone, &out_path)
    })
    .await
    .map_err(|e| format!("watermark task panic: {e}"))
    .and_then(|r| r.map_err(|e| e.to_string()));

    match result {
        Ok(()) => {
            tracing::info!(node_id = %node_id, path = %output_path.display(), "Watermark: done");
            NodeOutput::success(node_id).with_artifact(output_path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("watermark: {e}")),
    }
}

// -- Blocking implementation --

fn watermark_blocking(
    input: &Path,
    text: &str,
    cfg: &WatermarkConfig,
    output: &Path,
) -> Result<(), VmsError> {
    gstreamer::init().ok();

    let Some(input_str) = input.to_str() else {
        return Err(VmsError::Media("watermark: non-UTF-8 input path".into()));
    };
    let Some(output_str) = output.to_str() else {
        return Err(VmsError::Media("watermark: non-UTF-8 output path".into()));
    };

    let encoder_name = gst_util::probe_video_codec(input)
        .map(|c| gst_util::codec_to_encoder(&c))
        .unwrap_or("x264enc");

    let pipeline = gstreamer::Pipeline::new();

    let src = gstreamer::ElementFactory::make("filesrc")
        .property("location", input_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesrc: {e}")))?;

    let decode = gstreamer::ElementFactory::make("decodebin")
        .build()
        .map_err(|e| VmsError::Media(format!("decodebin: {e}")))?;

    let convert_in = gstreamer::ElementFactory::make("videoconvert")
        .build()
        .map_err(|e| VmsError::Media(format!("videoconvert (pre-overlay): {e}")))?;

    let (valign, halign) = position_to_alignment(&cfg.position);
    let alpha = (cfg.opacity.clamp(0.0, 1.0) * 255.0) as u32;
    let color: u32 = (alpha << 24) | 0x00FF_FFFF; // white with configurable alpha

    let overlay = gstreamer::ElementFactory::make("textoverlay")
        .property("text", text)
        .property_from_str("valignment", valign)
        .property_from_str("halignment", halign)
        .property("font-desc", format!("Sans {}", cfg.font_size))
        .property("color", color)
        .build()
        .map_err(|e| VmsError::Media(format!("textoverlay: {e}")))?;

    // Second videoconvert ensures the encoder gets a compatible raw format.
    let convert_out = gstreamer::ElementFactory::make("videoconvert")
        .build()
        .map_err(|e| VmsError::Media(format!("videoconvert (post-overlay): {e}")))?;

    let encoder = gstreamer::ElementFactory::make(encoder_name)
        .build()
        .map_err(|e| VmsError::Media(format!("{encoder_name}: {e}")))?;

    let muxer = gstreamer::ElementFactory::make("mp4mux")
        .build()
        .map_err(|e| VmsError::Media(format!("mp4mux: {e}")))?;

    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesink: {e}")))?;

    pipeline
        .add_many([
            &src,
            &decode,
            &convert_in,
            &overlay,
            &convert_out,
            &encoder,
            &muxer,
            &sink,
        ])
        .map_err(|e| VmsError::Media(format!("add elements: {e}")))?;

    gstreamer::Element::link_many([&convert_in, &overlay, &convert_out, &encoder, &muxer, &sink])
        .map_err(|e| VmsError::Media(format!("link chain: {e}")))?;

    src.link(&decode)
        .map_err(|e| VmsError::Media(format!("link src→decode: {e}")))?;

    gst_util::wire_decodebin(&decode, &convert_in, &pipeline);

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("set playing: {e}")))?;

    gst_util::wait_for_eos(&pipeline)?;

    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Helpers --

fn render_text(template: &str, input: &NodeInput) -> Result<String, String> {
    let ctx = &input.trigger_ctx;
    let env = Environment::new();
    env.render_str(
        template,
        minijinja::context! {
            camera_id    => ctx.camera_id.map(|id| id.to_string()),
            source_id    => ctx.source_id.map(|id| id.to_string()),
            fired_at     => ctx.fired_at.to_rfc3339(),
            trigger_type => format!("{:?}", ctx.trigger_type),
            run_id       => ctx.run_id.map(|id| id.to_string()),
        },
    )
    .map_err(|e| e.to_string())
}

fn position_to_alignment(pos: &WatermarkPosition) -> (&'static str, &'static str) {
    match pos {
        WatermarkPosition::TopLeft => ("top", "left"),
        WatermarkPosition::TopRight => ("top", "right"),
        WatermarkPosition::BottomLeft => ("bottom", "left"),
        WatermarkPosition::BottomRight => ("bottom", "right"),
        WatermarkPosition::Center => ("center", "center"),
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_top_left() {
        assert_eq!(
            position_to_alignment(&WatermarkPosition::TopLeft),
            ("top", "left")
        );
    }

    #[test]
    fn position_center() {
        assert_eq!(
            position_to_alignment(&WatermarkPosition::Center),
            ("center", "center")
        );
    }

    #[test]
    fn color_encoding_full_opacity() {
        let opacity: f32 = 1.0;
        let alpha = (opacity * 255.0) as u32;
        let color = (alpha << 24) | 0x00FF_FFFF;
        assert_eq!(color, 0xFFFF_FFFF);
    }

    #[test]
    fn color_encoding_half_opacity() {
        let opacity: f32 = 0.5;
        let alpha = (opacity * 255.0) as u32; // 127
        let color = (alpha << 24) | 0x00FF_FFFF;
        assert_eq!(color >> 24, 127);
    }
}
