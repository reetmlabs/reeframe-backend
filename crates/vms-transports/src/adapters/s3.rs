use minijinja::Environment;
use object_store::{
    aws::AmazonS3Builder,
    path::Path as OsPath,
    ObjectStore, PutPayload,
};
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_db::entities::destination;

// -- Adapter --

/// Upload the upstream artifact to an S3-compatible bucket.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "bucket":             "my-bucket",
///   "region":             "us-east-1",
///   "access_key_id":      "AKIAIOSFODNN7EXAMPLE",
///   "secret_access_key":  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
///   "endpoint":           "https://minio.internal:9000",
///   "path_style_access":  true
/// }
/// ```
///
/// `endpoint` and `path_style_access` are optional — omit them for AWS S3.
/// Set `path_style_access: true` for MinIO and other S3-compatible stores that
/// require path-style URLs.
///
/// The object key is built as `{rendered_path}/{rendered_filename}`.
/// Both path and filename can be overridden with minijinja templates in
/// `transport_cfg`; if absent the artifact's original filename is used and
/// no prefix is added.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
) -> NodeOutput {
    // -- Resolve required config fields --
    let cfg = &dest.config;

    let bucket = match cfg.get("bucket").and_then(|v| v.as_str()) {
        Some(b) => b.to_string(),
        None => {
            return NodeOutput::failure(node_id, "s3 transport: destination config missing \"bucket\"")
        }
    };
    let access_key = match cfg.get("access_key_id").and_then(|v| v.as_str()) {
        Some(k) => k.to_string(),
        None => {
            return NodeOutput::failure(node_id, "s3 transport: destination config missing \"access_key_id\"")
        }
    };
    let secret_key = match cfg.get("secret_access_key").and_then(|v| v.as_str()) {
        Some(k) => k.to_string(),
        None => {
            return NodeOutput::failure(node_id, "s3 transport: destination config missing \"secret_access_key\"")
        }
    };

    let region = cfg
        .get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("us-east-1")
        .to_string();
    let endpoint = cfg.get("endpoint").and_then(|v| v.as_str()).map(str::to_string);
    let path_style = cfg
        .get("path_style_access")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // -- Build object store --
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(&region)
        .with_access_key_id(&access_key)
        .with_secret_access_key(&secret_key);

    if let Some(ep) = &endpoint {
        builder = builder.with_endpoint(ep);
    }
    if path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    let store = match builder.build() {
        Ok(s) => s,
        Err(e) => return NodeOutput::failure(node_id, format!("s3 transport: build client: {e}")),
    };

    // -- Template context (same variables as local adapter) --
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
        camera_id     => ctx.camera_id.map(|id| id.to_string()).unwrap_or_default(),
        camera_name   => ctx.camera_name.as_deref().unwrap_or(""),
        fired_at      => ctx.fired_at.to_rfc3339(),
        trigger_type  => format!("{:?}", ctx.trigger_type),
        run_id        => ctx.run_id.map(|id| id.to_string()).unwrap_or_default(),
        artifact_name => artifact_name,
        artifact_stem => artifact_stem,
        artifact_ext  => artifact_ext,
    };

    let env = Environment::new();

    // -- Render path prefix --
    let prefix = match transport_cfg.and_then(|c| c.path_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => s,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("s3 transport: path_template render failed: {e}"),
                )
            }
        },
        None => String::new(),
    };

    // -- Render filename --
    let filename = match transport_cfg.and_then(|c| c.filename_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => s,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("s3 transport: filename_template render failed: {e}"),
                )
            }
        },
        None => artifact_name.to_string(),
    };

    // -- Build object key --
    let key = if prefix.is_empty() {
        filename.clone()
    } else {
        format!("{}/{}", prefix.trim_end_matches('/'), filename)
    };

    let object_path = OsPath::from(key.as_str());

    // -- Upload artifact or text --
    if let Some(src) = artifact {
        let bytes = match tokio::fs::read(src).await {
            Ok(b) => b,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("s3 transport: read artifact: {e}"),
                )
            }
        };

        match store.put(&object_path, PutPayload::from_bytes(bytes.into())).await {
            Ok(_) => {
                let object_url = format!("s3://{}/{}", bucket, key);
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    bucket  = %bucket,
                    key     = %key,
                    "S3 transport: artifact uploaded"
                );
                NodeOutput::success(node_id)
                    .with_metadata(serde_json::json!({ "s3_url": object_url, "bucket": bucket, "key": key }))
            }
            Err(e) => NodeOutput::failure(node_id, format!("s3 transport: put object: {e}")),
        }
    } else if let Some(text) = input.first_text() {
        let content = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
            Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
                Ok(s) => s,
                Err(e) => {
                    return NodeOutput::failure(
                        node_id,
                        format!("s3 transport: message_template render failed: {e}"),
                    )
                }
            },
            None => text.to_string(),
        };

        match store
            .put(&object_path, PutPayload::from_bytes(content.into_bytes().into()))
            .await
        {
            Ok(_) => {
                let object_url = format!("s3://{}/{}", bucket, key);
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    bucket  = %bucket,
                    key     = %key,
                    "S3 transport: text content uploaded"
                );
                NodeOutput::success(node_id)
                    .with_metadata(serde_json::json!({ "s3_url": object_url, "bucket": bucket, "key": key }))
            }
            Err(e) => NodeOutput::failure(node_id, format!("s3 transport: put text: {e}")),
        }
    } else {
        NodeOutput::failure(node_id, "s3 transport: no artifact or text in parent outputs")
    }
}
