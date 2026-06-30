use minijinja::Environment;
use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

// -- Adapter --

/// Send a message or file to a Telegram chat via the Bot API.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "bot_token": "123456:ABC-DEF...",
///   "chat_id":   "-1001234567890"
/// }
/// ```
///
/// Behaviour:
/// - If the upstream node produced an **artifact file**, it is sent via
///   `sendDocument` as a multipart upload.  The optional `message_template`
///   is rendered and sent as the document caption.
/// - If there is **no artifact**, the rendered `message_template` (or the raw
///   upstream text from `render_notification`) is sent via `sendMessage`.
///
/// `chat_id` can be a numeric chat ID or a public channel username (`@channel`).
/// Progress is not applicable for Telegram (the upload is a single HTTP call via
/// reqwest multipart); `progress_tx` is accepted but unused.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    _progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve config --
    let cfg = &dest.config;

    let bot_token = match cfg.get("bot_token").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return NodeOutput::failure(node_id, "telegram: destination config missing \"bot_token\""),
    };
    let chat_id = match cfg.get("chat_id").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return NodeOutput::failure(node_id, "telegram: destination config missing \"chat_id\""),
    };

    // -- Render message template --
    let ctx = &input.trigger_ctx;
    let artifact = input.first_artifact();

    let artifact_name = artifact
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("artifact");

    let tpl_ctx = minijinja::context! {
        camera_id    => ctx.camera_id.map(|id| id.to_string()).unwrap_or_default(),
        camera_name  => ctx.camera_name.as_deref().unwrap_or(""),
        fired_at     => ctx.fired_at.to_rfc3339(),
        trigger_type => format!("{:?}", ctx.trigger_type),
        run_id       => ctx.run_id.map(|id| id.to_string()).unwrap_or_default(),
        artifact_name => artifact_name,
    };

    let env = Environment::new();

    let message_text: Option<String> = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => Some(s),
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("telegram: message_template render failed: {e}"),
                )
            }
        },
        None => input.first_text().map(str::to_string),
    };

    let client = reqwest::Client::new();
    let base_url = format!("https://api.telegram.org/bot{bot_token}");

    // -- Send document or message --
    if let Some(src) = artifact {
        // -- Stream file as document — no full buffer in memory --
        let filename = src
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("artifact")
            .to_string();

        let file_len = match tokio::fs::metadata(src).await {
            Ok(m) => m.len(),
            Err(e) => return NodeOutput::failure(node_id, format!("telegram: stat artifact: {e}")),
        };

        let file = match tokio::fs::File::open(src).await {
            Ok(f) => f,
            Err(e) => return NodeOutput::failure(node_id, format!("telegram: open artifact: {e}")),
        };

        let file_part = reqwest::multipart::Part::stream_with_length(file, file_len)
            .file_name(filename)
            .mime_str("application/octet-stream")
            .unwrap();

        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.clone())
            .part("document", file_part);

        if let Some(caption) = &message_text {
            form = form.text("caption", caption.clone());
        }

        let resp = client
            .post(format!("{base_url}/sendDocument"))
            .multipart(form)
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                tracing::info!(node_id = %node_id, dest_id = %dest.id, chat_id = %chat_id, "Telegram: document sent");
                NodeOutput::success(node_id)
                    .with_metadata(serde_json::json!({ "chat_id": chat_id, "type": "document" }))
            }
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                NodeOutput::failure(node_id, format!("telegram: sendDocument HTTP {status}: {body}"))
            }
            Err(e) => NodeOutput::failure(node_id, format!("telegram: sendDocument request failed: {e}")),
        }
    } else {
        // -- Send text message --
        let text = match message_text {
            Some(t) => t,
            None => return NodeOutput::failure(node_id, "telegram: no artifact or text in parent outputs"),
        };

        let body = serde_json::json!({
            "chat_id": chat_id,
            "text":    text,
        });

        let resp = client
            .post(format!("{base_url}/sendMessage"))
            .json(&body)
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                tracing::info!(node_id = %node_id, dest_id = %dest.id, chat_id = %chat_id, "Telegram: message sent");
                NodeOutput::success(node_id)
                    .with_metadata(serde_json::json!({ "chat_id": chat_id, "type": "message" }))
            }
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                NodeOutput::failure(node_id, format!("telegram: sendMessage HTTP {status}: {body}"))
            }
            Err(e) => NodeOutput::failure(node_id, format!("telegram: sendMessage request failed: {e}")),
        }
    }
}
