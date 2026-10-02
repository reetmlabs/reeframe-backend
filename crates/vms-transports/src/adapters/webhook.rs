use std::sync::OnceLock;

use minijinja::Environment;
use tokio::sync::mpsc::UnboundedSender;
use vms_core::{
    action::{NotificationFormat, TransportConfig},
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn client() -> &'static reqwest::Client {
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Read the HTTP method from a webhook destination config. Defaults to POST
/// for anything absent or unrecognized.
fn resolve_method(cfg: &serde_json::Value) -> reqwest::Method {
    match cfg.get("method").and_then(|v| v.as_str()) {
        Some(m) if m.eq_ignore_ascii_case("put") => reqwest::Method::PUT,
        _ => reqwest::Method::POST,
    }
}

/// Content-Type for the request body, based on the upstream
/// render_notification node's format, if any.
fn body_content_type(format: Option<&NotificationFormat>) -> &'static str {
    match format {
        Some(NotificationFormat::Html) => "text/html; charset=utf-8",
        _ => "text/plain; charset=utf-8",
    }
}

// -- Adapter --

/// Send the rendered message (or raw upstream text) to a configurable URL.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "url":     "https://hooks.example.com/event",
///   "method":  "PUT",
///   "headers": {
///     "Authorization": "Bearer secret",
///     "X-Source":      "reeframe"
///   }
/// }
/// ```
///
/// `method` is optional and defaults to POST; PUT is the only other value
/// recognized. `headers` is optional. The request body is the rendered `message_template`
/// from `transport_cfg`, or the raw upstream text if no template is set.
/// `Content-Type` defaults to `text/plain; charset=utf-8` unless overridden
/// in `headers`.
///
/// If the upstream node produced an **artifact file** and no text is available,
/// the raw file bytes are posted with `Content-Type: application/octet-stream`.
///
/// On success `NodeOutput::metadata` contains `url` and the HTTP status code.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    _progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve config --
    let cfg = &dest.config;

    let url = match cfg.get("url").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return NodeOutput::failure(node_id, "webhook: destination config missing \"url\""),
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

    // An explicit message_template is the transport's own text, so the upstream
    // render_notification format only applies to the fallback (first_text) case.
    let (message_text, message_format) =
        match transport_cfg.and_then(|c| c.message_template.as_deref()) {
            Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
                Ok(s) => (Some(s), None),
                Err(e) => {
                    return NodeOutput::failure(
                        node_id,
                        format!("webhook: message_template render failed: {e}"),
                    )
                }
            },
            None => (
                input.first_text().map(str::to_string),
                input.first_format().cloned(),
            ),
        };

    // -- Build request --
    let mut req = client().request(resolve_method(cfg), &url);

    // -- Apply custom headers --
    if let Some(headers) = cfg.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in headers {
            if let Some(val) = v.as_str() {
                req = req.header(k.as_str(), val);
            }
        }
    }

    // -- Attach body --
    let req = if let Some(text) = message_text {
        req.header("Content-Type", body_content_type(message_format.as_ref()))
            .body(text)
    } else if let Some(src) = artifact {
        let bytes = match tokio::fs::read(src).await {
            Ok(b) => b,
            Err(e) => return NodeOutput::failure(node_id, format!("webhook: read artifact: {e}")),
        };
        req.header("Content-Type", "application/octet-stream")
            .body(bytes)
    } else {
        return NodeOutput::failure(node_id, "webhook: no artifact or text in parent outputs");
    };

    // -- Send --
    match req.send().await {
        Ok(r) => {
            let status = r.status();
            if status.is_success() {
                tracing::info!(node_id = %node_id, dest_id = %dest.id, %url, http_status = %status, "Webhook: delivered");
                NodeOutput::success(node_id).with_metadata(
                    serde_json::json!({ "url": url, "http_status": status.as_u16() }),
                )
            } else {
                let body = r.text().await.unwrap_or_default();
                NodeOutput::failure(node_id, format!("webhook: HTTP {status}: {body}"))
            }
        }
        Err(e) => NodeOutput::failure(node_id, format!("webhook: request failed: {e}")),
    }
}

#[cfg(test)]
mod resolve_method_tests {
    use super::*;

    #[test]
    fn defaults_to_post_when_absent() {
        let cfg = serde_json::json!({});
        assert_eq!(resolve_method(&cfg), reqwest::Method::POST);
    }

    #[test]
    fn honors_put() {
        let cfg = serde_json::json!({ "method": "PUT" });
        assert_eq!(resolve_method(&cfg), reqwest::Method::PUT);
    }

    #[test]
    fn is_case_insensitive() {
        let cfg = serde_json::json!({ "method": "put" });
        assert_eq!(resolve_method(&cfg), reqwest::Method::PUT);
    }

    #[test]
    fn falls_back_to_post_for_unrecognized_values() {
        let cfg = serde_json::json!({ "method": "DELETE" });
        assert_eq!(resolve_method(&cfg), reqwest::Method::POST);
    }
}

#[cfg(test)]
mod body_content_type_tests {
    use super::*;

    #[test]
    fn html_format_sends_text_html() {
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Html)),
            "text/html; charset=utf-8"
        );
    }

    #[test]
    fn text_and_markdown_and_absent_send_text_plain() {
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Text)),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Markdown)),
            "text/plain; charset=utf-8"
        );
        assert_eq!(body_content_type(None), "text/plain; charset=utf-8");
    }
}
