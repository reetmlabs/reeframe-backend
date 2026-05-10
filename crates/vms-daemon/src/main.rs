mod config;

use tracing_subscriber::{fmt, EnvFilter};

fn main() {
    // ── Observability (minimal — replaced by full OTLP pipeline in step 8c) ──
    fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_current_span(true)
        .with_span_list(true)
        .init();

    // ── Config ────────────────────────────────────────────────────────────────
    let cfg = match config::load() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to load configuration");
            std::process::exit(1);
        }
    };

    tracing::info!(
        db_url       = %cfg.database.url,
        recording_dir = %cfg.media.recording_dir.display(),
        api_bind     = %cfg.api.bind,
        log_level    = %cfg.log_level,
        "Configuration loaded",
    );

    // Warn instead of hard-failing on missing key — later substeps will enforce it.
    if cfg.encryption_key.is_empty() {
        tracing::warn!(
            "VMS_ENCRYPTION_KEY is not set — credential encryption will be unavailable. \
             Generate a key with: openssl rand -base64 32"
        );
    }

    tracing::info!("VMS Daemon starting");
}
