use std::path::PathBuf;

use minijinja::Environment;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_db::entities::destination;

// -- Adapter --

/// Copy the upstream artifact (or write the upstream text) to a local directory.
///
/// Destination config (stored in `dest.config`):
/// ```json
/// { "path": "/var/lib/reeframe/exports" }
/// ```
///
/// The node-level `transport_config` can override the sub-directory and filename
/// via minijinja templates.  When the templates are absent the artifact keeps its
/// original filename and lands directly in the base path.
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
        match tokio::fs::copy(src, &output_path).await {
            Ok(_) => {
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    src     = %src.display(),
                    dst     = %output_path.display(),
                    "Local transport: artifact delivered"
                );
                NodeOutput::success(node_id).with_artifact(output_path)
            }
            Err(e) => NodeOutput::failure(
                node_id,
                format!("local transport: copy artifact: {e}"),
            ),
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
