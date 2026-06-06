use minijinja::Environment;
use vms_core::{
    action::RenderNotificationConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};

/// Render a minijinja template into a text string.
///
/// The rendered string is stored in [`NodeOutput::text`] and is available to
/// downstream transport nodes as the message body.
///
/// # Template variables
///
/// | Variable        | Type               | Source                          |
/// |-----------------|--------------------|---------------------------------|
/// | `camera_id`     | `string \| null`   | `TriggerContext::camera_id`     |
/// | `source_id`     | `string \| null`   | `TriggerContext::source_id`     |
/// | `fired_at`      | `string`           | ISO-8601 UTC timestamp          |
/// | `trigger_type`  | `string`           | Debug repr of `TriggerType`     |
/// | `run_id`        | `string \| null`   | `TriggerContext::run_id`        |
/// | `artifact_path` | `string \| null`   | First parent artifact path      |
/// | `message`       | `string \| null`   | First parent `NodeOutput::text` |
pub fn execute(node_id: NodeId, cfg: &RenderNotificationConfig, input: &NodeInput) -> NodeOutput {
    let ctx = &input.trigger_ctx;

    let artifact_path = input
        .first_artifact()
        .map(|p| p.to_string_lossy().into_owned());

    let template_ctx = minijinja::context! {
        camera_id    => ctx.camera_id.map(|id| id.to_string()),
        source_id    => ctx.source_id.map(|id| id.to_string()),
        fired_at     => ctx.fired_at.to_rfc3339(),
        trigger_type => format!("{:?}", ctx.trigger_type),
        run_id       => ctx.run_id.map(|id| id.to_string()),
        artifact_path,
        message      => input.first_text(),
    };

    let env = Environment::new();
    match env.render_str(&cfg.template, template_ctx) {
        Ok(rendered) => {
            tracing::debug!(node_id = %node_id, len = rendered.len(), "RenderNotification: rendered");
            NodeOutput::success(node_id).with_text(rendered)
        }
        Err(e) => NodeOutput::failure(node_id, format!("template render error: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use vms_core::{action::NotificationFormat, node::NodeInput, trigger::TriggerContext};

    fn make_input() -> NodeInput {
        NodeInput {
            parent_outputs: vec![],
            trigger_ctx: TriggerContext::for_schedule(Uuid::new_v4(), Uuid::new_v4()),
        }
    }

    #[test]
    fn renders_static_template() {
        let id = Uuid::new_v4();
        let cfg = RenderNotificationConfig {
            template: "hello world".into(),
            format: NotificationFormat::Text,
        };
        let out = execute(id, &cfg, &make_input());
        assert!(out.success);
        assert_eq!(out.text.as_deref(), Some("hello world"));
    }

    #[test]
    fn renders_fired_at_variable() {
        let id = Uuid::new_v4();
        let cfg = RenderNotificationConfig {
            template: "fired at {{ fired_at }}".into(),
            format: NotificationFormat::Text,
        };
        let out = execute(id, &cfg, &make_input());
        assert!(out.success);
        assert!(out.text.as_deref().unwrap_or("").starts_with("fired at "));
    }

    #[test]
    fn bad_template_returns_failure() {
        let id = Uuid::new_v4();
        let cfg = RenderNotificationConfig {
            template: "{{ unclosed".into(),
            format: NotificationFormat::Text,
        };
        let out = execute(id, &cfg, &make_input());
        assert!(!out.success);
        assert!(out
            .error
            .as_deref()
            .unwrap_or("")
            .contains("template render error"));
    }
}
