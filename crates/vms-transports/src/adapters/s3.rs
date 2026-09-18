use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use minijinja::Environment;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as OsPath,
    MultipartUpload, ObjectStore, PutPayload,
};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

static S3_CLIENTS: OnceLock<DashMap<Uuid, Arc<AmazonS3>>> = OnceLock::new();

fn s3_clients() -> &'static DashMap<Uuid, Arc<AmazonS3>> {
    S3_CLIENTS.get_or_init(DashMap::new)
}

/// Evict the cached S3 client for `dest_id`. Call after a destination config update.
pub fn invalidate(dest_id: Uuid) {
    if let Some(m) = S3_CLIENTS.get() {
        m.remove(&dest_id);
    }
}

// S3 requires every multipart part but the last to be at least 5 MiB; 8 MiB
// gives headroom and cuts the number of upload requests versus the minimum.
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

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
/// Files are streamed via multipart upload in `CHUNK_SIZE` parts, so the
/// artifact is never fully loaded into memory. Progress is reported via
/// `progress_tx` after each part. On success, `NodeOutput::metadata`
/// contains `s3_url`, `bucket`, and `key`.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve required config fields --
    let cfg = &dest.config;

    let bucket = match cfg.get("bucket").and_then(|v| v.as_str()) {
        Some(b) => b.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "s3 transport: destination config missing \"bucket\"",
            )
        }
    };
    let access_key = match cfg.get("access_key_id").and_then(|v| v.as_str()) {
        Some(k) => k.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "s3 transport: destination config missing \"access_key_id\"",
            )
        }
    };
    let secret_key = match cfg.get("secret_access_key").and_then(|v| v.as_str()) {
        Some(k) => k.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "s3 transport: destination config missing \"secret_access_key\"",
            )
        }
    };

    let region = cfg
        .get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("us-east-1")
        .to_string();
    let endpoint = cfg
        .get("endpoint")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let path_style = cfg
        .get("path_style_access")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // -- Build object store (cached by destination ID) --
    let store: Arc<AmazonS3> = match s3_clients().get(&dest.id) {
        Some(cached) => cached.clone(),
        None => {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_region(&region)
                .with_access_key_id(&access_key)
                .with_secret_access_key(&secret_key)
                // Some S3-compatible backends (Hetzner's Ceph RGW among them) reject the
                // signed-payload SigV4 mode object_store uses by default for multipart
                // requests. Unsigned payload is a standard SigV4 mode AWS S3 accepts too.
                .with_unsigned_payload(true);

            if let Some(ep) = &endpoint {
                builder = builder.with_endpoint(ep);
            }
            if path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }

            let built = match builder.build() {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    return NodeOutput::failure(node_id, format!("s3 transport: build client: {e}"))
                }
            };
            s3_clients().insert(dest.id, built.clone());
            built
        }
    };

    // -- Template context --
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
        match stream_file_to_s3(&*store, src, &object_path, node_id, ctx.run_id, progress_tx).await
        {
            Ok(()) => {
                let object_url = format!("s3://{}/{}", bucket, key);
                tracing::info!(
                    node_id = %node_id,
                    dest_id = %dest.id,
                    bucket  = %bucket,
                    key     = %key,
                    "S3 transport: artifact uploaded"
                );
                NodeOutput::success(node_id).with_metadata(
                    serde_json::json!({ "s3_url": object_url, "bucket": bucket, "key": key }),
                )
            }
            Err(e) => NodeOutput::failure(node_id, format!("s3 transport: {e}")),
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

        let raw = content.into_bytes();
        let total = raw.len() as u64;

        match store.put(&object_path, PutPayload::from(raw)).await {
            Ok(_) => {
                if let Some(tx) = progress_tx {
                    let _ = tx.send(TransferProgress {
                        node_id,
                        run_id: ctx.run_id,
                        bytes_sent: total,
                        total_bytes: Some(total),
                    });
                }
                let object_url = format!("s3://{}/{}", bucket, key);
                tracing::info!(node_id = %node_id, dest_id = %dest.id, bucket = %bucket, key = %key, "S3 transport: text content uploaded");
                NodeOutput::success(node_id).with_metadata(
                    serde_json::json!({ "s3_url": object_url, "bucket": bucket, "key": key }),
                )
            }
            Err(e) => NodeOutput::failure(node_id, format!("s3 transport: put text: {e}")),
        }
    } else {
        NodeOutput::failure(
            node_id,
            "s3 transport: no artifact or text in parent outputs",
        )
    }
}

// -- Streaming multipart upload --

/// Fill `buf` completely from `reader`, stopping early only at EOF.
/// A single `read()` call isn't guaranteed to fill the buffer, and a short
/// read here would silently produce an undersized multipart part.
async fn fill_buf<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

async fn stream_file_to_s3(
    store: &object_store::aws::AmazonS3,
    src: &std::path::PathBuf,
    path: &OsPath,
    node_id: NodeId,
    run_id: Option<uuid::Uuid>,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> Result<(), String> {
    let mut file = tokio::fs::File::open(src)
        .await
        .map_err(|e| format!("open artifact: {e}"))?;
    let total_bytes = file.metadata().await.map(|m| m.len()).ok();

    let mut upload = store
        .put_multipart(path)
        .await
        .map_err(|e| format!("init multipart upload: {e}"))?;

    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut bytes_sent: u64 = 0;

    loop {
        let n = fill_buf(&mut file, &mut buf)
            .await
            .map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        let chunk = PutPayload::from(buf[..n].to_vec());
        upload
            .put_part(chunk)
            .await
            .map_err(|e| format!("upload part: {e}"))?;
        bytes_sent += n as u64;

        if let Some(tx) = progress_tx {
            let _ = tx.send(TransferProgress {
                node_id,
                run_id,
                bytes_sent,
                total_bytes,
            });
        }
    }

    upload
        .complete()
        .await
        .map_err(|e| format!("finalize multipart: {e}"))?;
    Ok(())
}
