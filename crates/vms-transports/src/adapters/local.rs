use std::path::PathBuf;

use minijinja::Environment;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

const CHUNK_SIZE: usize = 256 * 1024;

// -- Adapter --

/// Copy the upstream artifact (or write the upstream text) to a local directory.
///
/// Destination config (stored in `dest.config`):
/// ```json
/// { "path": "/var/lib/reeframe/exports" }
/// ```
///
/// Files are copied in 256 KB chunks — the source file is never fully loaded
/// into memory.  Progress is reported via `progress_tx` after each chunk.
///
/// Template variables:
/// | Variable        | Value |
/// |-----------------|-------|
/// | `camera_id`     | UUID string, or empty string |
/// | `camera_name`   | Camera name, or empty string |
/// | `fired_at`      | ISO-8601 UTC string |
/// | `trigger_type`  | Debug repr of `TriggerType` |
/// | `run_id`        | UUID string, or empty string |
/// | `artifact_name` | Original filename of the artifact |
/// | `artifact_stem` | Filename without extension |
/// | `artifact_ext`  | Extension without leading dot |
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve base path from dest config --
    let base_path = match dest.config.get("path").and_then(|v| v.as_str()) {
        Some(p) => PathBuf::from(p),
        None => {
            return NodeOutput::failure(
                node_id,
                "local transport: destination config missing \"path\" field",
            )
        }
    };

    // -- Build template context --
    let ctx = &input.trigger_ctx;
    let artifact = input.first_artifact();

    let artifact_name = artifact
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("artifact");
    let artifact_stem = artifact
        .and_then(|p| p.file_stem())
        .and_then(|n| n.to_str())
        .unwrap_or("artifact");
    let artifact_ext = artifact
        .and_then(|p| p.extension())
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let tpl_ctx = minijinja::context! {
        camera_id    => ctx.camera_id.map(|id| id.to_string()).unwrap_or_default(),
        camera_name  => ctx.camera_name.as_deref().unwrap_or(""),
        fired_at     => ctx.fired_at.to_rfc3339(),
        trigger_type => format!("{:?}", ctx.trigger_type),
        run_id       => ctx.run_id.map(|id| id.to_string()).unwrap_or_default(),
        artifact_name => artifact_name,
        artifact_stem => artifact_stem,
        artifact_ext  => artifact_ext,
    };

    let env = Environment::new();

    // -- Render sub-directory path template --
    let subdir = match transport_cfg.and_then(|c| c.path_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => s,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("local transport: path_template render failed: {e}"),
                )
            }
        },
        None => String::new(),
    };

    // -- Render filename template --
    let filename = match transport_cfg.and_then(|c| c.filename_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => s,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("local transport: filename_template render failed: {e}"),
                )
            }
        },
        None => artifact_name.to_string(),
    };

    // -- Build final output path --
    let mut output_path = base_path;
    if !subdir.is_empty() {
        output_path.push(&subdir);
    }

    if let Err(e) = tokio::fs::create_dir_all(&output_path).await {
        return NodeOutput::failure(
            node_id,
            format!("local transport: create output dir: {e}"),
        );
    }

    output_path.push(&filename);

    // -- Deliver artifact or text --
    if let Some(src) = artifact {
        match copy_with_progress(node_id, src, &output_path, ctx.run_id, progress_tx).await {
            Ok(()) => {
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    src     = %src.display(),
                    dst     = %output_path.display(),
                    "Local transport: artifact delivered"
                );
                NodeOutput::success(node_id).with_artifact(output_path)
            }
            Err(e) => NodeOutput::failure(node_id, format!("local transport: copy artifact: {e}")),
        }
    } else if let Some(text) = input.first_text() {
        // -- Render message_template if provided, else use text verbatim --
        let content = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
            Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
                Ok(s) => s,
                Err(e) => {
                    return NodeOutput::failure(
                        node_id,
                        format!("local transport: message_template render failed: {e}"),
                    )
                }
            },
            None => text.to_string(),
        };

        match tokio::fs::write(&output_path, content.as_bytes()).await {
            Ok(()) => {
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    dst     = %output_path.display(),
                    "Local transport: text content written"
                );
                NodeOutput::success(node_id).with_artifact(output_path)
            }
            Err(e) => NodeOutput::failure(
                node_id,
                format!("local transport: write text: {e}"),
            ),
        }
    } else {
        NodeOutput::failure(
            node_id,
            "local transport: no artifact or text in parent outputs",
        )
    }
}

// -- Chunked copy with progress reporting --

async fn copy_with_progress(
    node_id: NodeId,
    src: &PathBuf,
    dst: &PathBuf,
    run_id: Option<uuid::Uuid>,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> Result<(), String> {
    let mut file = tokio::fs::File::open(src)
        .await
        .map_err(|e| format!("open source: {e}"))?;
    let total_bytes = file
        .metadata()
        .await
        .map(|m| m.len())
        .ok();

    let mut out = tokio::fs::File::create(dst)
        .await
        .map_err(|e| format!("create destination: {e}"))?;

    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut bytes_sent: u64 = 0;

    loop {
        let n = file.read(&mut buf).await.map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n]).await.map_err(|e| format!("write: {e}"))?;
        bytes_sent += n as u64;

        if let Some(tx) = progress_tx {
            let _ = tx.send(TransferProgress { node_id, run_id, bytes_sent, total_bytes });
        }
    }

    out.flush().await.map_err(|e| format!("flush: {e}"))?;
    Ok(())
}
