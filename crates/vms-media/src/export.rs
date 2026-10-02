//! Concatenate recorded chunk files into one exported file, trimming the
//! first and last chunk where needed. This is the media side of
//! `POST /recordings/export`.
//!
//! All chunks of a camera share one known codec (cached on the `cameras` row
//! and carried on each `recordings` row), so unlike `merge_clips` there is no
//! probing or transcoding. Every stage is a stream copy (demux, parse, mux),
//! which is cheaper and loses no quality.
//!
//! A trimmed chunk gets its own single-source seek-and-remux pass into a temp
//! file before concatenation. Seeking one branch of the multi-source concat
//! pipeline would work too, but two simple stages (a single-file trim, then a
//! concat where every input plays start to finish) are easier to reason about.

use std::path::{Path, PathBuf};

use gstreamer::prelude::*;
use vms_core::VmsError;

use crate::camera_stream::codec_for;

/// One recorded chunk contributing to an export.
///
/// `trim_start_ns`/`trim_stop_ns` are nanosecond offsets into this chunk's own
/// timeline, not the export range. Both `None` means the whole file is used.
/// They are only set on the first or last chunk of a range that does not line
/// up with the chunk's boundaries.
pub struct ExportChunk {
    pub file_path: String,
    pub trim_start_ns: Option<u64>,
    pub trim_stop_ns: Option<u64>,
}

/// Build `output_path` from `chunks`, in order. Blocking, so call it via
/// `spawn_blocking`.
pub fn export_range(
    chunks: &[ExportChunk],
    codec: &str,
    output_path: &Path,
) -> Result<(), VmsError> {
    gstreamer::init().ok();

    let parse_factory = codec_for(codec)
        .ok_or_else(|| VmsError::Media(format!("export: unsupported codec '{codec}'")))?
        .parse_factory;

    let mut temp_files: Vec<PathBuf> = Vec::new();
    let mut pieces: Vec<String> = Vec::with_capacity(chunks.len());

    let result = (|| {
        for chunk in chunks {
            if chunk.trim_start_ns.is_some() || chunk.trim_stop_ns.is_some() {
                let tmp_path = format!("{}.export_trim.tmp.mp4", chunk.file_path);
                trim_single(
                    &chunk.file_path,
                    chunk.trim_start_ns.unwrap_or(0),
                    chunk.trim_stop_ns,
                    parse_factory,
                    &tmp_path,
                )?;
                temp_files.push(PathBuf::from(&tmp_path));
                pieces.push(tmp_path);
            } else {
                pieces.push(chunk.file_path.clone());
            }
        }

        concat_stream_copy(&pieces, parse_factory, output_path)
    })();

    for tmp in temp_files {
        std::fs::remove_file(&tmp).ok();
    }

    result
}

// -- Single-chunk trim --

/// Rewrite `input`'s `[start_ns, stop_ns)` window into `output` via
/// `filesrc -> qtdemux -> {codec}parse -> mp4mux -> filesink`. `stop_ns = None`
/// means "to the end". With a single source there is only one timeline to seek.
fn trim_single(
    input: &str,
    start_ns: u64,
    stop_ns: Option<u64>,
    parse_factory: &str,
    output: &str,
) -> Result<(), VmsError> {
    let pipeline = gstreamer::Pipeline::new();

    let filesrc = gstreamer::ElementFactory::make("filesrc")
        .property("location", input)
        .build()
        .map_err(|e| VmsError::Media(format!("export trim filesrc: {e}")))?;
    let demux = gstreamer::ElementFactory::make("qtdemux")
        .build()
        .map_err(|e| VmsError::Media(format!("export trim qtdemux: {e}")))?;
    let parse = gstreamer::ElementFactory::make(parse_factory)
        .build()
        .map_err(|e| VmsError::Media(format!("export trim {parse_factory}: {e}")))?;
    let mux = gstreamer::ElementFactory::make("mp4mux")
        .property("faststart", true)
        .build()
        .map_err(|e| VmsError::Media(format!("export trim mp4mux: {e}")))?;
    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output)
        .build()
        .map_err(|e| VmsError::Media(format!("export trim filesink: {e}")))?;

    pipeline
        .add_many([&filesrc, &demux, &parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("export trim add_many: {e}")))?;

    filesrc
        .link(&demux)
        .map_err(|e| VmsError::Media(format!("export trim link filesrc->demux: {e}")))?;
    gstreamer::Element::link_many([&parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("export trim link parse->mux->sink: {e}")))?;

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
            tracing::error!("export trim: link demux->parse failed: {e}");
        }
    });

    pipeline
        .set_state(gstreamer::State::Paused)
        .map_err(|e| VmsError::Media(format!("export trim pause: {e}")))?;
    let (state_result, _, _) = pipeline.state(gstreamer::ClockTime::from_seconds(10));
    state_result.map_err(|e| VmsError::Media(format!("export trim pause preroll: {e:?}")))?;

    let start = gstreamer::ClockTime::from_nseconds(start_ns);
    let seek_flags = gstreamer::SeekFlags::FLUSH | gstreamer::SeekFlags::ACCURATE;
    let seek_result = match stop_ns {
        Some(stop) => pipeline.seek(
            1.0,
            seek_flags,
            gstreamer::SeekType::Set,
            start,
            gstreamer::SeekType::Set,
            gstreamer::ClockTime::from_nseconds(stop),
        ),
        None => pipeline.seek_simple(seek_flags, start),
    };
    seek_result.map_err(|e| VmsError::Media(format!("export trim seek: {e}")))?;

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("export trim play: {e}")))?;

    wait_for_eos(&pipeline)?;
    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Multi-chunk concat --

/// Concatenate already-trimmed `pieces` into `output` via
/// `concat -> {codec}parse -> mp4mux -> filesink`, with one
/// `filesrc -> qtdemux -> {codec}parse` branch per piece feeding `concat`.
/// Every branch plays start to finish without seeking.
fn concat_stream_copy(
    pieces: &[String],
    parse_factory: &str,
    output: &Path,
) -> Result<(), VmsError> {
    let Some(output_str) = output.to_str() else {
        return Err(VmsError::Media("export: non-UTF-8 output path".into()));
    };

    let pipeline = gstreamer::Pipeline::new();

    let concat = gstreamer::ElementFactory::make("concat")
        .build()
        .map_err(|e| VmsError::Media(format!("export concat: {e}")))?;
    let out_parse = gstreamer::ElementFactory::make(parse_factory)
        .build()
        .map_err(|e| VmsError::Media(format!("export concat {parse_factory}: {e}")))?;
    let mux = gstreamer::ElementFactory::make("mp4mux")
        .property("faststart", true)
        .build()
        .map_err(|e| VmsError::Media(format!("export concat mp4mux: {e}")))?;
    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", output_str)
        .build()
        .map_err(|e| VmsError::Media(format!("export concat filesink: {e}")))?;

    pipeline
        .add_many([&concat, &out_parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("export concat add output chain: {e}")))?;
    gstreamer::Element::link_many([&concat, &out_parse, &mux, &sink])
        .map_err(|e| VmsError::Media(format!("export concat link output chain: {e}")))?;

    for piece in pieces {
        let filesrc = gstreamer::ElementFactory::make("filesrc")
            .property("location", piece.as_str())
            .build()
            .map_err(|e| VmsError::Media(format!("export concat filesrc: {e}")))?;
        let demux = gstreamer::ElementFactory::make("qtdemux")
            .build()
            .map_err(|e| VmsError::Media(format!("export concat qtdemux: {e}")))?;
        let parse = gstreamer::ElementFactory::make(parse_factory)
            .build()
            .map_err(|e| VmsError::Media(format!("export concat {parse_factory}: {e}")))?;

        pipeline
            .add_many([&filesrc, &demux, &parse])
            .map_err(|e| VmsError::Media(format!("export concat add branch: {e}")))?;

        let concat_sink = concat
            .request_pad_simple("sink_%u")
            .ok_or_else(|| VmsError::Media("export concat: could not request sink pad".into()))?;
        let parse_src = parse
            .static_pad("src")
            .ok_or_else(|| VmsError::Media("export concat: parse has no src pad".into()))?;
        parse_src
            .link(&concat_sink)
            .map_err(|e| VmsError::Media(format!("export concat link parse->concat: {e}")))?;

        filesrc
            .link(&demux)
            .map_err(|e| VmsError::Media(format!("export concat link filesrc->demux: {e}")))?;

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
                tracing::error!("export concat: link demux->parse failed: {e}");
            }
        });
    }

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("export concat play: {e}")))?;

    wait_for_eos(&pipeline)?;
    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Helpers --

fn wait_for_eos(pipeline: &gstreamer::Pipeline) -> Result<(), VmsError> {
    let bus = pipeline.bus().expect("pipeline bus missing");
    loop {
        let Some(msg) = bus.timed_pop(gstreamer::ClockTime::from_seconds(60)) else {
            return Err(VmsError::Media("export: pipeline timed out".into()));
        };
        match msg.view() {
            gstreamer::MessageView::Eos(_) => return Ok(()),
            gstreamer::MessageView::Error(e) => {
                return Err(VmsError::Media(format!(
                    "export pipeline error: {}",
                    e.error()
                )));
            }
            _ => {}
        }
    }
}
