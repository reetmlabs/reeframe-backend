use std::{
    io::Write as _,
    net::TcpStream,
    path::PathBuf,
};

use minijinja::Environment;
use ssh2::Session;
use vms_core::{
    action::TransportConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
};
use vms_db::entities::destination;

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
/// Either `password` or `private_key` must be present. If both are present,
/// private-key authentication is attempted first.
///
/// The remote file path is built as `{remote_path}/{rendered_subdir}/{rendered_filename}`.
/// Intermediate directories are created automatically. The blocking ssh2 session
/// runs inside `tokio::task::spawn_blocking` so it does not stall the async runtime.
pub async fn deliver(
    node_id: NodeId,
    dest: &destination::Model,
    transport_cfg: Option<&TransportConfig>,
    input: &NodeInput,
) -> NodeOutput {
    // -- Resolve required config fields --
    let cfg = &dest.config;

    let host = match cfg.get("host").and_then(|v| v.as_str()) {
        Some(h) => h.to_string(),
        None => return NodeOutput::failure(node_id, "sftp transport: destination config missing \"host\""),
    };
    let port = cfg.get("port").and_then(|v| v.as_u64()).unwrap_or(22) as u16;
    let username = match cfg.get("username").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return NodeOutput::failure(node_id, "sftp transport: destination config missing \"username\""),
    };
    let password = cfg.get("password").and_then(|v| v.as_str()).map(str::to_string);
    let private_key = cfg.get("private_key").and_then(|v| v.as_str()).map(str::to_string);
    let key_passphrase = cfg
        .get("private_key_passphrase")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let remote_base = match cfg.get("remote_path").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return NodeOutput::failure(node_id, "sftp transport: destination config missing \"remote_path\""),
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

    // -- Read content into memory before entering spawn_blocking --
    let content: Vec<u8> = if let Some(src) = artifact {
        match tokio::fs::read(src).await {
            Ok(b) => b,
            Err(e) => {
                return NodeOutput::failure(node_id, format!("sftp transport: read artifact: {e}"))
            }
        }
    } else if let Some(text) = input.first_text() {
        let body = match transport_cfg.and_then(|c| c.message_template.as_deref()) {
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
        body.into_bytes()
    } else {
        return NodeOutput::failure(node_id, "sftp transport: no artifact or text in parent outputs");
    };

    // -- Upload in spawn_blocking (ssh2 is synchronous) --
    let addr = format!("{host}:{port}");
    let result = tokio::task::spawn_blocking(move || {
        upload_blocking(&addr, &username, password.as_deref(), private_key.as_deref(), &key_passphrase, &remote_dir, &remote_file, &content)
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
            NodeOutput::success(node_id)
                .with_metadata(serde_json::json!({ "sftp_url": sftp_url, "remote_path": remote_path }))
        }
        Ok(Err(e)) => NodeOutput::failure(node_id, format!("sftp transport: {e}")),
        Err(e) => NodeOutput::failure(node_id, format!("sftp transport: spawn_blocking panicked: {e}")),
    }
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
    content: &[u8],
) -> Result<String, String> {
    // -- Connect and handshake --
    let tcp = TcpStream::connect(addr)
        .map_err(|e| format!("connect {addr}: {e}"))?;

    let mut session = Session::new()
        .map_err(|e| format!("session init: {e}"))?;
    session.set_tcp_stream(tcp);
    session.handshake()
        .map_err(|e| format!("handshake: {e}"))?;

    // -- Authenticate --
    if let Some(pem) = private_key {
        let passphrase = if key_passphrase.is_empty() { None } else { Some(key_passphrase) };
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
    let sftp = session.sftp()
        .map_err(|e| format!("open sftp subsystem: {e}"))?;

    // -- Create remote directories --
    mkdir_all(&sftp, remote_dir)?;

    // -- Write file --
    let mut remote = sftp
        .create(remote_file)
        .map_err(|e| format!("create remote file {}: {e}", remote_file.display()))?;
    remote
        .write_all(content)
        .map_err(|e| format!("write remote file: {e}"))?;

    Ok(remote_file.to_string_lossy().into_owned())
}

/// Create a remote directory path component by component.
/// Ignores errors on components that already exist.
fn mkdir_all(sftp: &ssh2::Sftp, path: &PathBuf) -> Result<(), String> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        // stat() succeeds if the directory exists — skip mkdir in that case.
        if sftp.stat(&current).is_ok() {
            continue;
        }
        sftp.mkdir(&current, 0o755)
            .map_err(|e| format!("mkdir {}: {e}", current.display()))?;
    }
    Ok(())
}
