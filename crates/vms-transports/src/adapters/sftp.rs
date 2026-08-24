use std::{
    io::{Read as _, Write as _},
    net::TcpStream,
    path::PathBuf,
};

use minijinja::Environment;
use ssh2::Session;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput, TransferProgress},
    pipeline::NodeId,
};
use vms_db::entities::destination;

const CHUNK_SIZE: usize = 256 * 1024;

// -- Adapter --

/// Upload the upstream artifact (or text) to a remote host via SFTP.
///
/// Destination config (stored in `dest.config`, credentials already decrypted):
/// ```json
/// {
///   "host":                  "sftp.example.com",
///   "port":                  22,
///   "username":              "reeframe",
///   "password":              "s3cr3t",
///   "private_key":           "-----BEGIN OPENSSH PRIVATE KEY-----\n...",
///   "private_key_passphrase": "",
///   "remote_path":           "/var/exports/reeframe"
/// }
/// ```
///
/// Either `password` or `private_key` must be present. Files are uploaded in
/// 256 KB chunks inside `tokio::task::spawn_blocking` — the artifact is never
/// fully loaded into memory.  Progress is reported via `progress_tx` after each
/// chunk using `tokio::sync::mpsc::UnboundedSender::send`, which is safe to
/// call from a blocking thread.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> NodeOutput {
    // -- Resolve required config fields --
    let cfg = &dest.config;

    let host = match cfg.get("host").and_then(|v| v.as_str()) {
        Some(h) => h.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "sftp transport: destination config missing \"host\"",
            )
        }
    };
    let port = cfg.get("port").and_then(|v| v.as_u64()).unwrap_or(22) as u16;
    let username = match cfg.get("username").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "sftp transport: destination config missing \"username\"",
            )
        }
    };
    let password = cfg
        .get("password")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let private_key = cfg
        .get("private_key")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let key_passphrase = cfg
        .get("private_key_passphrase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let remote_base = match cfg.get("remote_path").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => {
            return NodeOutput::failure(
                node_id,
                "sftp transport: destination config missing \"remote_path\"",
            )
        }
    };

    if password.is_none() && private_key.is_none() {
        return NodeOutput::failure(
            node_id,
            "sftp transport: destination config must have \"password\" or \"private_key\"",
        );
    }

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

    // -- Render subdir --
    let subdir = match transport_cfg.and_then(|c| c.path_template.as_deref()) {
        Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
            Ok(s) => s,
            Err(e) => {
                return NodeOutput::failure(
                    node_id,
                    format!("sftp transport: path_template render failed: {e}"),
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
                    format!("sftp transport: filename_template render failed: {e}"),
                )
            }
        },
        None => artifact_name.to_string(),
    };

    // -- Build remote paths --
    let mut remote_dir = PathBuf::from(&remote_base);
    if !subdir.is_empty() {
        remote_dir.push(&subdir);
    }
    let remote_file = remote_dir.join(&filename);

    // -- Resolve payload before entering spawn_blocking --
    //
    // For artifact files we pass the source path and let the blocking thread
    // open and read it in chunks (no pre-loading into memory).
    // For text we pass the bytes directly — text payloads are small.
    let payload = if let Some(src) = artifact {
        // Get file size for progress reporting without reading the file.
        let total_bytes = tokio::fs::metadata(src).await.map(|m| m.len()).ok();
        SftpPayload::File {
            path: src.clone(),
            total_bytes,
        }
    } else if let Some(text) = input.first_text() {
        let content = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
            Some(tpl) => match env.render_str(tpl, &tpl_ctx) {
                Ok(s) => s,
                Err(e) => {
                    return NodeOutput::failure(
                        node_id,
                        format!("sftp transport: message_template render failed: {e}"),
                    )
                }
            },
            None => text.to_string(),
        };
        SftpPayload::Text(content.into_bytes())
    } else {
        return NodeOutput::failure(
            node_id,
            "sftp transport: no artifact or text in parent outputs",
        );
    };

    // -- tokio::sync::mpsc::UnboundedSender is Send — clone it into the blocking thread --
    let progress_tx_owned = progress_tx.cloned();
    let addr = format!("{host}:{port}");
    let run_id = ctx.run_id;

    let result = tokio::task::spawn_blocking(move || {
        upload_blocking(
            &addr,
            &username,
            password.as_deref(),
            private_key.as_deref(),
            &key_passphrase,
            &remote_dir,
            &remote_file,
            payload,
            node_id,
            run_id,
            progress_tx_owned.as_ref(),
        )
    })
    .await;

    match result {
        Ok(Ok(remote_path)) => {
            tracing::info!(
                node_id = %node_id,
                dest_id = %dest.id,
                host    = %host,
                path    = %remote_path,
                "SFTP transport: artifact uploaded"
            );
            let sftp_url = format!("sftp://{host}{remote_path}");
            NodeOutput::success(node_id).with_metadata(
                serde_json::json!({ "sftp_url": sftp_url, "remote_path": remote_path }),
            )
        }
        Ok(Err(e)) => NodeOutput::failure(node_id, format!("sftp transport: {e}")),
        Err(e) => NodeOutput::failure(
            node_id,
            format!("sftp transport: spawn_blocking panicked: {e}"),
        ),
    }
}

// -- Payload enum --

enum SftpPayload {
    File {
        path: PathBuf,
        total_bytes: Option<u64>,
    },
    Text(Vec<u8>),
}

// -- Blocking upload --

fn upload_blocking(
    addr: &str,
    username: &str,
    password: Option<&str>,
    private_key: Option<&str>,
    key_passphrase: &str,
    remote_dir: &PathBuf,
    remote_file: &PathBuf,
    payload: SftpPayload,
    node_id: NodeId,
    run_id: Option<Uuid>,
    progress_tx: Option<&UnboundedSender<TransferProgress>>,
) -> Result<String, String> {
    // -- Connect and handshake --
    let tcp = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;

    let mut session = Session::new().map_err(|e| format!("session init: {e}"))?;
    session.set_tcp_stream(tcp);
    session.handshake().map_err(|e| format!("handshake: {e}"))?;

    // -- Authenticate --
    if let Some(pem) = private_key {
        let passphrase = if key_passphrase.is_empty() {
            None
        } else {
            Some(key_passphrase)
        };
        session
            .userauth_pubkey_memory(username, None, pem, passphrase)
            .map_err(|e| format!("pubkey auth: {e}"))?;
    } else if let Some(pw) = password {
        session
            .userauth_password(username, pw)
            .map_err(|e| format!("password auth: {e}"))?;
    }

    if !session.authenticated() {
        return Err("authentication failed".into());
    }

    // -- Open SFTP subsystem --
    let sftp = session
        .sftp()
        .map_err(|e| format!("open sftp subsystem: {e}"))?;

    // -- Create remote directories --
    mkdir_all(&sftp, remote_dir)?;

    // -- Write in chunks --
    let mut remote = sftp
        .create(remote_file)
        .map_err(|e| format!("create remote file {}: {e}", remote_file.display()))?;

    match payload {
        SftpPayload::File { path, total_bytes } => {
            let mut file =
                std::fs::File::open(&path).map_err(|e| format!("open local file: {e}"))?;
            let mut buf = vec![0u8; CHUNK_SIZE];
            let mut bytes_sent: u64 = 0;

            loop {
                let n = file.read(&mut buf).map_err(|e| format!("read: {e}"))?;
                if n == 0 {
                    break;
                }
                remote
                    .write_all(&buf[..n])
                    .map_err(|e| format!("write: {e}"))?;
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
        }

        SftpPayload::Text(bytes) => {
            let total = bytes.len() as u64;
            remote
                .write_all(&bytes)
                .map_err(|e| format!("write text: {e}"))?;
            if let Some(tx) = progress_tx {
                let _ = tx.send(TransferProgress {
                    node_id,
                    run_id,
                    bytes_sent: total,
                    total_bytes: Some(total),
                });
            }
        }
    }

    Ok(remote_file.to_string_lossy().into_owned())
}

// -- mkdir -p over SFTP --

fn mkdir_all(sftp: &ssh2::Sftp, path: &PathBuf) -> Result<(), String> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        if sftp.stat(&current).is_ok() {
            continue;
        }
        sftp.mkdir(&current, 0o755)
            .map_err(|e| format!("mkdir {}: {e}", current.display()))?;
    }
    Ok(())
}
