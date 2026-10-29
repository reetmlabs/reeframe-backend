use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use lettre::{
    message::{header::ContentType, Attachment, Mailbox, MessageBuilder, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use minijinja::Environment;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use vms_core::{
    action::{NotificationFormat, TransportConfig},
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

static SMTP_CLIENTS: OnceLock<DashMap<Uuid, Arc<AsyncSmtpTransport<Tokio1Executor>>>> =
    OnceLock::new();

fn smtp_clients() -> &'static DashMap<Uuid, Arc<AsyncSmtpTransport<Tokio1Executor>>> {
    SMTP_CLIENTS.get_or_init(DashMap::new)
}

/// Evict the cached SMTP transport for `dest_id`. Call after a destination config update.
pub fn invalidate(dest_id: Uuid) {
    if let Some(m) = SMTP_CLIENTS.get() {
        m.remove(&dest_id);
    }
}

/// Resolve the sender address, falling back to `username` when `from` isn't
/// set. Most SMTP providers use the login username as the sender address.
fn resolve_from(cfg: &serde_json::Value, username: Option<&str>) -> Option<String> {
    cfg.get("from")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| username.map(str::to_string))
}

/// Content-Type for the message body, based on the upstream
/// render_notification node's format, if any.
fn body_content_type(format: Option<&NotificationFormat>) -> ContentType {
    match format {
        Some(NotificationFormat::Html) => ContentType::TEXT_HTML,
        _ => ContentType::TEXT_PLAIN,
    }
}

// -- Adapter --

/// Send an email via SMTP using `lettre`.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "smtp_host": "smtp.example.com",
///   "smtp_port": 587,
///   "username":  "user@example.com",
///   "password":  "secret",
///   "from":      "reeframe@example.com",
///   "to":        "ops@example.com",
///   "subject":   "Reeframe alert: {{ camera_name }}",
///   "tls":       "starttls"
/// }
/// ```
///
/// `smtp_port` defaults to 587. `from` defaults to `username` if not set.
/// `tls` accepts `"starttls"` (default), `"tls"`, or `"none"`.
/// `subject` is a minijinja template rendered with the same variables as other adapters.
/// `to` may be a single address or a comma-separated list. It's only required
/// when `contact_list` is `None`; a contact list replaces it (see `add_recipients`).
///
/// If the upstream node produced an **artifact file** it is attached to the email.
/// The body text comes from `message_template` in `transport_cfg`, or raw upstream
/// text from `render_notification` if no template is set.
/// `progress_tx` is accepted but unused, since the message is sent in one SMTP call.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    contact_list: Option<&[String]>,
    input: &NodeInput,
    _progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve config --
    let cfg = &dest.config;

    let smtp_host = match cfg.get("smtp_host").and_then(|v| v.as_str()) {
        Some(h) => h.to_string(),
        None => {
            return NodeOutput::failure(node_id, "email: destination config missing \"smtp_host\"")
        }
    };
    let smtp_port = cfg.get("smtp_port").and_then(|v| v.as_u64()).unwrap_or(587) as u16;
    let username = cfg
        .get("username")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let password = cfg
        .get("password")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let from_addr =
        match resolve_from(cfg, username.as_deref()) {
            Some(f) => f,
            None => return NodeOutput::failure(
                node_id,
                "email: destination config missing \"from\" (and no \"username\" to fall back to)",
            ),
        };
    let to = cfg.get("to").and_then(|v| v.as_str());
    let subject_tpl = cfg
        .get("subject")
        .and_then(|v| v.as_str())
        .unwrap_or("Reeframe notification")
        .to_string();
    let tls_mode = cfg
        .get("tls")
        .and_then(|v| v.as_str())
        .unwrap_or("starttls")
        .to_string();

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

    // -- Render subject --
    let subject = match env.render_str(&subject_tpl, &tpl_ctx) {
        Ok(s) => s,
        Err(e) => {
            return NodeOutput::failure(node_id, format!("email: subject render failed: {e}"))
        }
    };

    // -- Render body --
    // An explicit message_template is the transport's own text, so the upstream
    // render_notification format only applies to the fallback (first_text) case.
    let (body_text, body_format) = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => (s, None),
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("email: message_template render failed: {e}"),
                )
            }
        },
        None => (
            input.first_text().unwrap_or("").to_string(),
            input.first_format().cloned(),
        ),
    };

    // -- Parse addresses --
    let from_mailbox: Mailbox = match from_addr.parse() {
        Ok(m) => m,
        Err(e) => return NodeOutput::failure(node_id, format!("email: invalid from address: {e}")),
    };

    let builder = Message::builder()
        .from(from_mailbox.clone())
        .subject(subject.clone());
    let (builder, recipients) = match add_recipients(builder, &from_mailbox, to, contact_list) {
        Ok(r) => r,
        Err(e) => return NodeOutput::failure(node_id, format!("email: {e}")),
    };

    // -- Build message body --
    let content_type = body_content_type(body_format.as_ref());
    let text_part = SinglePart::builder()
        .header(content_type)
        .body(body_text.clone());

    let message = if let Some(src) = artifact {
        let file_bytes = match tokio::fs::read(src).await {
            Ok(b) => b,
            Err(e) => return NodeOutput::failure(node_id, format!("email: read artifact: {e}")),
        };
        let mime = mime_from_path(src);
        let attachment = Attachment::new(artifact_name.to_string()).body(file_bytes, mime);

        match builder.multipart(
            MultiPart::mixed()
                .singlepart(text_part)
                .singlepart(attachment),
        ) {
            Ok(m) => m,
            Err(e) => return NodeOutput::failure(node_id, format!("email: build message: {e}")),
        }
    } else {
        match builder.singlepart(text_part) {
            Ok(m) => m,
            Err(e) => return NodeOutput::failure(node_id, format!("email: build message: {e}")),
        }
    };

    // -- Build SMTP transport (cached by destination ID) --
    let transport: Arc<AsyncSmtpTransport<Tokio1Executor>> = match smtp_clients().get(&dest.id) {
        Some(cached) => cached.clone(),
        None => {
            let built = match build_transport(&smtp_host, smtp_port, &tls_mode, username, password)
            {
                Ok(t) => Arc::new(t),
                Err(e) => {
                    return NodeOutput::failure(
                        node_id,
                        format!("email: build smtp transport: {e}"),
                    )
                }
            };
            smtp_clients().insert(dest.id, built.clone());
            built
        }
    };

    // -- Send --
    match transport.send(message).await {
        Ok(_) => {
            tracing::info!(
                node_id  = %node_id,
                dest_id  = %dest.id,
                to       = %recipients,
                subject  = %subject,
                "Email: sent"
            );
            NodeOutput::success(node_id)
                .with_metadata(serde_json::json!({ "to": recipients, "subject": subject }))
        }
        Err(e) => NodeOutput::failure(node_id, format!("email: send failed: {e}")),
    }
}

// -- Helpers --

/// Adds the recipients and returns them summarized for logs. A contact list
/// goes in BCC with `from` in To, so members don't see each other's addresses.
/// Invalid list addresses are skipped so one bad contact doesn't block the rest.
fn add_recipients(
    mut builder: MessageBuilder,
    from: &Mailbox,
    to: Option<&str>,
    contact_list: Option<&[String]>,
) -> Result<(MessageBuilder, String), String> {
    if let Some(list) = contact_list {
        let mut count = 0;
        for addr in list {
            match addr.parse::<Mailbox>() {
                Ok(mailbox) => {
                    builder = builder.bcc(mailbox);
                    count += 1;
                }
                Err(e) => {
                    tracing::warn!(address = %addr, error = %e, "Email: skipping invalid contact address")
                }
            }
        }
        if count == 0 {
            return Err("contact list has no valid email addresses".into());
        }
        return Ok((builder.to(from.clone()), format!("{count} contacts (bcc)")));
    }

    let to = to.ok_or("destination config missing \"to\"")?;
    for addr in to.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let mailbox: Mailbox = addr
            .parse()
            .map_err(|e| format!("invalid to address \"{addr}\": {e}"))?;
        builder = builder.to(mailbox);
    }
    Ok((builder, to.to_string()))
}

fn build_transport(
    host: &str,
    port: u16,
    tls_mode: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<AsyncSmtpTransport<Tokio1Executor>, String> {
    let mut builder = match tls_mode {
        "tls" => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
            .map_err(|e| format!("relay init: {e}"))?,
        "none" => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host),
        _ => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
            .map_err(|e| format!("starttls init: {e}"))?,
    };

    builder = builder.port(port);

    if let (Some(user), Some(pass)) = (username, password) {
        builder = builder.credentials(Credentials::new(user, pass));
    }

    Ok(builder.build())
}

fn mime_from_path(path: &std::path::Path) -> ContentType {
    match path.extension().and_then(|e| e.to_str()) {
        Some("mp4") | Some("mov") | Some("avi") => ContentType::parse("video/mp4").unwrap(),
        Some("jpg") | Some("jpeg") => ContentType::parse("image/jpeg").unwrap(),
        Some("png") => ContentType::parse("image/png").unwrap(),
        Some("pdf") => ContentType::parse("application/pdf").unwrap(),
        Some("txt") => ContentType::TEXT_PLAIN,
        _ => ContentType::parse("application/octet-stream").unwrap(),
    }
}

#[cfg(test)]
mod resolve_from_tests {
    use super::*;

    #[test]
    fn reads_from() {
        let cfg = serde_json::json!({ "from": "reeframe@example.com" });
        assert_eq!(
            resolve_from(&cfg, Some("user@example.com")).as_deref(),
            Some("reeframe@example.com")
        );
    }

    #[test]
    fn falls_back_to_username_when_from_is_absent() {
        let cfg = serde_json::json!({});
        assert_eq!(
            resolve_from(&cfg, Some("user@example.com")).as_deref(),
            Some("user@example.com")
        );
    }

    #[test]
    fn prefers_from_over_username_when_both_are_present() {
        let cfg = serde_json::json!({ "from": "reeframe@example.com" });
        assert_eq!(
            resolve_from(&cfg, Some("user@example.com")).as_deref(),
            Some("reeframe@example.com")
        );
    }

    #[test]
    fn returns_none_when_neither_is_present() {
        let cfg = serde_json::json!({});
        assert_eq!(resolve_from(&cfg, None), None);
    }
}

#[cfg(test)]
mod body_content_type_tests {
    use super::*;

    #[test]
    fn html_format_sends_text_html() {
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Html)),
            ContentType::TEXT_HTML
        );
    }

    #[test]
    fn text_and_markdown_and_absent_send_text_plain() {
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Text)),
            ContentType::TEXT_PLAIN
        );
        assert_eq!(
            body_content_type(Some(&NotificationFormat::Markdown)),
            ContentType::TEXT_PLAIN
        );
        assert_eq!(body_content_type(None), ContentType::TEXT_PLAIN);
    }
}

#[cfg(test)]
mod add_recipients_tests {
    use lettre::Address;

    use super::*;

    fn from() -> Mailbox {
        "reeframe@example.com".parse().unwrap()
    }

    fn build(
        to: Option<&str>,
        contact_list: Option<&[String]>,
    ) -> Result<(Message, String), String> {
        let builder = Message::builder().from(from()).subject("s");
        let (builder, summary) = add_recipients(builder, &from(), to, contact_list)?;
        Ok((builder.body(String::new()).unwrap(), summary))
    }

    fn envelope_to(message: &Message) -> Vec<String> {
        message
            .envelope()
            .to()
            .iter()
            .map(Address::to_string)
            .collect()
    }

    #[test]
    fn contact_list_goes_in_bcc_with_from_in_to() {
        let list = vec!["a@example.com".to_string(), "b@example.com".to_string()];
        let (message, summary) = build(Some("ops@example.com"), Some(&list)).unwrap();

        assert_eq!(
            message.headers().get_raw("To"),
            Some("reeframe@example.com")
        );
        assert_eq!(
            envelope_to(&message),
            ["reeframe@example.com", "a@example.com", "b@example.com"]
        );
        assert_eq!(summary, "2 contacts (bcc)");
    }

    #[test]
    fn invalid_contact_addresses_are_skipped() {
        let list = vec!["not an address".to_string(), "a@example.com".to_string()];
        let (message, _) = build(None, Some(&list)).unwrap();
        assert_eq!(
            envelope_to(&message),
            ["reeframe@example.com", "a@example.com"]
        );
    }

    #[test]
    fn contact_list_without_valid_addresses_fails() {
        let list = vec!["not an address".to_string()];
        assert_eq!(
            build(None, Some(&list)).unwrap_err(),
            "contact list has no valid email addresses"
        );
    }

    #[test]
    fn without_contact_list_sends_to_destination_to() {
        let (message, summary) = build(Some("a@example.com, b@example.com"), None).unwrap();
        assert_eq!(envelope_to(&message), ["a@example.com", "b@example.com"]);
        assert_eq!(summary, "a@example.com, b@example.com");
    }

    #[test]
    fn without_contact_list_to_is_required() {
        assert_eq!(
            build(None, None).unwrap_err(),
            "destination config missing \"to\""
        );
    }
}
