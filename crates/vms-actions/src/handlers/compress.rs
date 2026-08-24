use std::io::BufReader;
use std::path::Path;

use vms_core::{
    action::{CompressConfig, CompressionAlgorithm},
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    VmsError,
};

use crate::dispatcher::ActionContext;

// -- Handler --

/// Compress the upstream artifact using the configured algorithm.
///
/// Uses streaming I/O — the input file is never fully loaded into memory.
/// Intended for non-video artifacts (JSON logs, CSV exports, text files).
/// Video files are already entropy-coded by their codec; applying these
/// algorithms yields no size reduction — use `transcode` for video instead.
///
/// Output: `{stem}.{ext}` in `recording_dir` where `ext` is `zst`, `gz`, or `lz4`.
pub async fn execute(
    node_id: NodeId,
    cfg: &CompressConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    let Some(input_path) = input.first_artifact() else {
        return NodeOutput::failure(node_id, "compress: no upstream artifact");
    };

    if let Err(e) = tokio::fs::create_dir_all(&ctx.recording_dir).await {
        return NodeOutput::failure(node_id, format!("compress: create output dir: {e}"));
    }

    let ext = file_extension(&cfg.algorithm);
    let stem = input_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("artifact");
    let output_path = ctx.recording_dir.join(format!("{stem}.{ext}"));

    let cfg_clone = cfg.clone();
    let in_path = input_path.clone();
    let out_path = output_path.clone();

    let result =
        tokio::task::spawn_blocking(move || compress_blocking(&in_path, &cfg_clone, &out_path))
            .await
            .map_err(|e| format!("compress task panic: {e}"))
            .and_then(|r| r.map_err(|e| e.to_string()));

    match result {
        Ok(()) => {
            tracing::info!(
                node_id = %node_id,
                algorithm = ?cfg.algorithm,
                path = %output_path.display(),
                "Compress: done"
            );
            NodeOutput::success(node_id).with_artifact(output_path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("compress: {e}")),
    }
}

// -- Blocking implementation --

fn compress_blocking(input: &Path, cfg: &CompressConfig, output: &Path) -> Result<(), VmsError> {
    let in_file =
        std::fs::File::open(input).map_err(|e| VmsError::Media(format!("open input: {e}")))?;
    let mut reader = BufReader::new(in_file);

    match cfg.algorithm {
        CompressionAlgorithm::Zstd => {
            let out_file = std::fs::File::create(output)
                .map_err(|e| VmsError::Media(format!("create output: {e}")))?;
            let mut encoder = zstd::Encoder::new(out_file, cfg.level as i32)
                .map_err(|e| VmsError::Media(format!("zstd encoder: {e}")))?;
            std::io::copy(&mut reader, &mut encoder)
                .map_err(|e| VmsError::Media(format!("zstd compress: {e}")))?;
            encoder
                .finish()
                .map_err(|e| VmsError::Media(format!("zstd finish: {e}")))?;
        }
        CompressionAlgorithm::Gzip => {
            let out_file = std::fs::File::create(output)
                .map_err(|e| VmsError::Media(format!("create output: {e}")))?;
            let level = flate2::Compression::new(cfg.level.min(9) as u32);
            let mut encoder = flate2::write::GzEncoder::new(out_file, level);
            std::io::copy(&mut reader, &mut encoder)
                .map_err(|e| VmsError::Media(format!("gzip compress: {e}")))?;
            encoder
                .finish()
                .map_err(|e| VmsError::Media(format!("gzip finish: {e}")))?;
        }
        CompressionAlgorithm::Lz4 => {
            let out_file = std::fs::File::create(output)
                .map_err(|e| VmsError::Media(format!("create output: {e}")))?;
            let mut encoder = lz4_flex::frame::FrameEncoder::new(out_file);
            std::io::copy(&mut reader, &mut encoder)
                .map_err(|e| VmsError::Media(format!("lz4 compress: {e}")))?;
            encoder
                .finish()
                .map_err(|e| VmsError::Media(format!("lz4 finish: {e}")))?;
        }
    }

    Ok(())
}

// -- Helpers --

fn file_extension(algorithm: &CompressionAlgorithm) -> &'static str {
    match algorithm {
        CompressionAlgorithm::Zstd => "zst",
        CompressionAlgorithm::Gzip => "gz",
        CompressionAlgorithm::Lz4 => "lz4",
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensions_are_correct() {
        assert_eq!(file_extension(&CompressionAlgorithm::Zstd), "zst");
        assert_eq!(file_extension(&CompressionAlgorithm::Gzip), "gz");
        assert_eq!(file_extension(&CompressionAlgorithm::Lz4), "lz4");
    }

    #[test]
    fn compress_and_verify_smaller_roundtrip() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let input = dir.join("compress_test_input.txt");
        let output = dir.join("compress_test_input.txt.zst");

        // Highly compressible input
        let mut f = std::fs::File::create(&input).unwrap();
        f.write_all(&b"hello world ".repeat(1000)).unwrap();
        drop(f);

        let cfg = CompressConfig {
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
        };
        compress_blocking(&input, &cfg, &output).unwrap();

        let original_size = std::fs::metadata(&input).unwrap().len();
        let compressed_size = std::fs::metadata(&output).unwrap().len();
        assert!(
            compressed_size < original_size,
            "compressed should be smaller"
        );

        std::fs::remove_file(&input).ok();
        std::fs::remove_file(&output).ok();
    }
}
