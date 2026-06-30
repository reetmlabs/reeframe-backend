use minijinja::Environment;
use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

// -- Adapter --

/// Post a message or upload a file to a Slack channel.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "webhook_url": "https://hooks.slack.com/services/T.../B.../...",
///   "bot_token":   "xoxb-...",
///   "channel":     "#alerts"
/// }
/// ```
///
/// Two modes — the adapter picks based on what config fields are present:
///
/// **Incoming Webhook mode** (`webhook_url` is set):
/// - Text-only. Posts the rendered `message_template` (or upstream text) as a
///   JSON payload to the webhook URL. Simplest setup, no OAuth required.
///   File uploads are not supported in this mode; if an artifact is present
///   the adapter falls back to sending its filename in the text.
///
/// **Bot Token mode** (`bot_token` + `channel` are set):
/// - If the upstream node produced an **artifact file** → `files.getUploadURLExternal`
///   + upload + `files.completeUploadExternal` (Slack's current upload flow).
///   The rendered `message_template` is posted as the file's initial comment.
/// - If no artifact → `chat.postMessage` with the rendered text.
///
/// `webhook_url` takes priority when both are present. `progress_tx` is accepted
/// but unused (no chunked protocol at this layer).
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    _progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    let cfg = &dest.config;

    let webhook_url = cfg.get("webhook_url").and_then(|v| v.as_str()).map(str::to_string);
    let bot_token   = cfg.get("bot_token").and_then(|v| v.as_str()).map(str::to_string);
    let channel     = cfg.get("channel").and_then(|v| v.as_str()).map(str::to_string);

    if webhook_url.is_none() && bot_token.is_none() {
        return NodeOutput::failure(
            node_id,
            "slack: destination config must have \"webhook_url\" or \"bot_token\"",
        );
    }

    // -- Template context --
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
            Err(e) => return NodeOutput::failure(node_id, format!("slack: message_template render failed: {e}")),
        },
        None => input.first_text().map(str::to_string),
    };

    let client = reqwest::Client::new();

    // -- Incoming Webhook mode --
    if let Some(url) = webhook_url {
        let text = match &message_text {
            Some(t) => {
                if artifact.is_some() {
                    format!("{t}\n_Attachment: {artifact_name}_")
                } else {
                    t.clone()
                }
            }
            None if artifact.is_some() => format!("_Attachment: {artifact_name}_"),
            None => return NodeOutput::failure(node_id, "slack: no text or artifact in parent outputs"),
        };

        let body = serde_json::json!({ "text": text });

        return match client.post(&url).json(&body).send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(node_id = %node_id, dest_id = %dest.id, "Slack webhook: message sent");
                NodeOutput::success(node_id)
                    .with_metadata(serde_json::json!({ "mode": "webhook" }))
            }
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                NodeOutput::failure(node_id, format!("slack webhook: HTTP {status}: {body}"))
            }
            Err(e) => NodeOutput::failure(node_id, format!("slack webhook: request failed: {e}")),
        };
    }

    // -- Bot Token mode --
    let token = bot_token.unwrap();
    let channel = match channel {
        Some(c) => c,
        None => return NodeOutput::failure(node_id, "slack: bot_token mode requires \"channel\""),
    };

    if let Some(src) = artifact {
        upload_file(&client, &token, &channel, src, artifact_name, message_text.as_deref(), node_id, dest).await
    } else {
        let text = match message_text {
            Some(t) => t,
            None => return NodeOutput::failure(node_id, "slack: no text or artifact in parent outputs"),
        };
        post_message(&client, &token, &channel, &text, node_id, dest).await
    }
}

// -- Bot API helpers --

async fn post_message(
    client: &reqwest::Client,
    token: &str,
    channel: &str,
    text: &str,
    node_id: NodeId,
    dest: &destination::Model,
) -> NodeOutput {
    let body = serde_json::json!({ "channel": channel, "text": text });

    match client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            match r.json::<serde_json::Value>().await {
                Ok(json) if json["ok"].as_bool() == Some(true) => {
                    tracing::info!(node_id = %node_id, dest_id = %dest.id, %channel, "Slack: message sent");
                    NodeOutput::success(node_id)
                        .with_metadata(serde_json::json!({ "mode": "bot", "channel": channel }))
                }
                Ok(json) => {
                    let err = json["error"].as_str().unwrap_or("unknown").to_string();
                    NodeOutput::failure(node_id, format!("slack chat.postMessage error: {err}"))
                }
                Err(e) => NodeOutput::failure(node_id, format!("slack: parse response: {e}")),
            }
        }
        Ok(r) => {
            let status = r.status().as_u16();
            NodeOutput::failure(node_id, format!("slack chat.postMessage HTTP {status}"))
        }
        Err(e) => NodeOutput::failure(node_id, format!("slack: request failed: {e}")),
    }
}

async fn upload_file(
    client: &reqwest::Client,
    token: &str,
    channel: &str,
    src: &std::path::PathBuf,
    filename: &str,
    initial_comment: Option<&str>,
    node_id: NodeId,
    dest: &destination::Model,
) -> NodeOutput {
    // -- Step 1: get upload URL --
    let file_len = match tokio::fs::metadata(src).await {
        Ok(m) => m.len(),
        Err(e) => return NodeOutput::failure(node_id, format!("slack: stat artifact: {e}")),
    };

    let url_resp = client
        .get("https://slack.com/api/files.getUploadURLExternal")
        .bearer_auth(token)
        .query(&[("filename", filename), ("length", &file_len.to_string())])
        .send()
        .await;

    let url_json = match url_resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(j) => j,
            Err(e) => return NodeOutput::failure(node_id, format!("slack: parse upload URL response: {e}")),
        },
        Err(e) => return NodeOutput::failure(node_id, format!("slack: getUploadURLExternal failed: {e}")),
    };

    if url_json["ok"].as_bool() != Some(true) {
        let err = url_json["error"].as_str().unwrap_or("unknown");
        return NodeOutput::failure(node_id, format!("slack getUploadURLExternal error: {err}"));
    }

    let upload_url = match url_json["upload_url"].as_str() {
        Some(u) => u.to_string(),
        None => return NodeOutput::failure(node_id, "slack: getUploadURLExternal missing upload_url"),
    };
    let file_id = match url_json["file_id"].as_str() {
        Some(id) => id.to_string(),
        None => return NodeOutput::failure(node_id, "slack: getUploadURLExternal missing file_id"),
    };

    // -- Step 2: stream file bytes --
    let file = match tokio::fs::File::open(src).await {
        Ok(f) => f,
        Err(e) => return NodeOutput::failure(node_id, format!("slack: open artifact: {e}")),
    };

    let upload_resp = client
        .post(&upload_url)
        .bearer_auth(token)
        .body(reqwest::Body::from(file))
        .send()
        .await;

    if let Err(e) = upload_resp {
        return NodeOutput::failure(node_id, format!("slack: file upload failed: {e}"));
    }

    // -- Step 3: complete upload --
    let mut complete_body = serde_json::json!({
        "files":   [{ "id": file_id }],
        "channel_id": channel,
    });
    if let Some(comment) = initial_comment {
        complete_body["initial_comment"] = serde_json::Value::String(comment.to_string());
    }

    match client
        .post("https://slack.com/api/files.completeUploadExternal")
        .bearer_auth(token)
        .json(&complete_body)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            match r.json::<serde_json::Value>().await {
                Ok(json) if json["ok"].as_bool() == Some(true) => {
                    tracing::info!(node_id = %node_id, dest_id = %dest.id, %channel, %filename, "Slack: file uploaded");
                    NodeOutput::success(node_id)
                        .with_metadata(serde_json::json!({ "mode": "bot", "channel": channel, "file_id": file_id }))
                }
                Ok(json) => {
                    let err = json["error"].as_str().unwrap_or("unknown").to_string();
                    NodeOutput::failure(node_id, format!("slack completeUploadExternal error: {err}"))
                }
                Err(e) => NodeOutput::failure(node_id, format!("slack: parse complete response: {e}")),
            }
        }
        Ok(r) => {
            let status = r.status().as_u16();
            NodeOutput::failure(node_id, format!("slack completeUploadExternal HTTP {status}"))
        }
        Err(e) => NodeOutput::failure(node_id, format!("slack: complete upload request failed: {e}")),
    }
}
