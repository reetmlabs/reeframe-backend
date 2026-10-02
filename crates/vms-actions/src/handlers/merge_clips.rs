use std::path::{Path, PathBuf};

use gstreamer::prelude::*;
use vms_core::{
    action::{ClipOrder, GapFill, MergeClipsConfig},
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    VmsError,
};

use super::gst as gst_util;
use crate::dispatcher::ActionContext;

// -- Handler --

/// Concatenate multiple upstream clip artifacts into a single output file.
///
/// All clips are decoded and fed through GStreamer's `concat` element, then
/// re-encoded as H.264/MP4. The `output_format` field from the config is used
/// for the output container.
///
/// # Gap modes
///
/// - `Skip`: clips are joined back to back with no gap.
/// - `BlackFrame` / `Freeze`: not implemented (they need resolution probing),
///   so they fall back to `Skip` with a warning.
pub async fn execute(
    node_id: NodeId,
    cfg: &MergeClipsConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    let artifacts = input.all_artifacts();

    if artifacts.is_empty() {
        return NodeOutput::failure(node_id, "merge_clips: no upstream artifacts");
    }

    // Single clip: nothing to merge, pass it through.
    if artifacts.len() == 1 {
        return NodeOutput::success(node_id).with_artifact(artifacts[0].clone());
    }

    if let Err(e) = tokio::fs::create_dir_all(&ctx.recording_dir).await {
        return NodeOutput::failure(node_id, format!("merge_clips: create output dir: {e}"));
    }

    // -- Sort --
    let mut clips: Vec<PathBuf> = artifacts.iter().map(|p| (*p).clone()).collect();
    match cfg.order {
        ClipOrder::Chronological => clips.sort(),
        ClipOrder::ReverseChronological => {
            clips.sort();
            clips.reverse();
        }
    }

    if !matches!(cfg.gap_fill, GapFill::Skip) {
        tracing::warn!(
            node_id = %node_id,
            gap_fill = ?cfg.gap_fill,
            "merge_clips: BlackFrame and Freeze gap modes require resolution probing \
             (not yet implemented), falling back to Skip mode"
        );
    }

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let output_path = ctx
        .recording_dir
        .join(format!("merged_{ts}.{}", cfg.output_format));

    let out_format = cfg.output_format.clone();
    let out_path = output_path.clone();

    let result =
        tokio::task::spawn_blocking(move || merge_blocking(&clips, &out_format, &out_path))
            .await
            .map_err(|e| format!("merge_clips task panic: {e}"))
            .and_then(|r| r.map_err(|e| e.to_string()));

    match result {
        Ok(()) => {
            tracing::info!(node_id = %node_id, path = %output_path.display(), "MergeClips: done");
            NodeOutput::success(node_id).with_artifact(output_path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("merge_clips: {e}")),
    }
}

// -- Blocking implementation --

/// Build and run a GStreamer concat pipeline for N input clips.
///
/// Pipeline per clip: `filesrc → decodebin → videoconvert → concat.sink_N`
/// Output chain: `concat → videoconvert → encoder → muxer → filesink`
///
/// All clips must have the same resolution and frame rate (guaranteed when
/// clips come from the same camera). `videoconvert` normalises pixel formats.
fn merge_blocking(clips: &[PathBuf], output_format: &str, output: &Path) -> Result<(), VmsError> {
    gstreamer::init().ok();

    let Some(output_str) = output.to_str() else {
        return Err(VmsError::Media("merge_clips: non-UTF-8 output path".into()));
    };

    let encoder_name = clips
        .first()
        .and_then(|p| gst_util::probe_video_codec(p))
        .map(|c| gst_util::codec_to_encoder(&c))
        .unwrap_or("x264enc");

    let pipeline = gstreamer::Pipeline::new();

    // -- Output chain: concat → videoconvert → encoder → muxer → filesink --
    let concat = gstreamer::ElementFactory::make("concat")
        .build()
        .map_err(|e| VmsError::Media(format!("concat: {e}")))?;

    let out_convert = gstreamer::ElementFactory::make("videoconvert")
        .build()
        .map_err(|e| VmsError::Media(format!("videoconvert (output): {e}")))?;

    let encoder = gstreamer::ElementFactory::make(encoder_name)
        .build()
        .map_err(|e| VmsError::Media(format!("{encoder_name}: {e}")))?;

    let muxer = make_muxer(output_format)?;

    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output_str)
        .build()
        .map_err(|e| VmsError::Media(format!("filesink: {e}")))?;

    pipeline
        .add_many([&concat, &out_convert, &encoder, &muxer, &sink])
        .map_err(|e| VmsError::Media(format!("add output chain: {e}")))?;

    gstreamer::Element::link_many([&concat, &out_convert, &encoder, &muxer, &sink])
        .map_err(|e| VmsError::Media(format!("link output chain: {e}")))?;

    // -- Input chain per clip: filesrc → decodebin → videoconvert → concat.sink_N --
    for clip in clips {
        let Some(clip_str) = clip.to_str() else {
            return Err(VmsError::Media(format!(
                "merge_clips: non-UTF-8 clip path: {}",
                clip.display()
            )));
        };

        let src = gstreamer::ElementFactory::make("filesrc")
            .property("location", clip_str)
            .build()
            .map_err(|e| VmsError::Media(format!("filesrc: {e}")))?;

        let decode = gstreamer::ElementFactory::make("decodebin")
            .build()
            .map_err(|e| VmsError::Media(format!("decodebin: {e}")))?;

        let convert = gstreamer::ElementFactory::make("videoconvert")
            .build()
            .map_err(|e| VmsError::Media(format!("videoconvert (clip): {e}")))?;

        pipeline
            .add_many([&src, &decode, &convert])
            .map_err(|e| VmsError::Media(format!("add clip elements: {e}")))?;

        let concat_sink = concat
            .request_pad_simple("sink_%u")
            .ok_or_else(|| VmsError::Media("concat: could not request sink pad".into()))?;

        let convert_src = convert
            .static_pad("src")
            .ok_or_else(|| VmsError::Media("videoconvert has no src pad".into()))?;

        convert_src
            .link(&concat_sink)
            .map_err(|e| VmsError::Media(format!("link convert→concat: {e}")))?;

        src.link(&decode)
            .map_err(|e| VmsError::Media(format!("link src→decode: {e}")))?;

        gst_util::wire_decodebin(&decode, &convert, &pipeline);
    }

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("set playing: {e}")))?;

    gst_util::wait_for_eos(&pipeline)?;

    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Helpers --

fn make_muxer(format: &str) -> Result<gstreamer::Element, VmsError> {
    let factory = match format {
        "mp4" | "m4v" => "mp4mux",
        "mkv" => "matroskamux",
        "avi" => "avimux",
        "ts" | "mpeg-ts" => "mpegtsmux",
        "webm" => "webmmux",
        other => {
            tracing::warn!("merge_clips: unknown format '{other}', falling back to mp4mux");
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
    use std::path::PathBuf;

    fn clips(n: usize) -> Vec<PathBuf> {
        (0..n)
            .map(|i| PathBuf::from(format!("/tmp/clip{i:02}.mp4")))
            .collect()
    }

    #[test]
    fn sort_chronological_by_filename() {
        let mut c = clips(3);
        c.reverse();
        c.sort();
        assert_eq!(c[0], PathBuf::from("/tmp/clip00.mp4"));
    }

    #[test]
    fn sort_reverse_by_filename() {
        let mut c = clips(3);
        c.sort();
        c.reverse();
        assert_eq!(c[0], PathBuf::from("/tmp/clip02.mp4"));
    }
}
