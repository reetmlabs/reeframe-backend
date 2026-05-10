mod config;

use sea_orm::Database;
use sea_orm_migration::MigratorTrait;
use tracing_subscriber::{fmt, EnvFilter};
use vms_db::{CameraRepo, Crypto, DestinationRepo, Migrator, SourceRepo};
use vms_media::{MediaConfig, MediaManager};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Observability (minimal — replaced by full OTLP pipeline in step 8c) ──
    fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_current_span(true)
        .with_span_list(true)
        .init();

    // ── Config ────────────────────────────────────────────────────────────────
    let cfg = config::load().map_err(|e| {
        tracing::error!(error = %e, "Failed to load configuration");
        anyhow::anyhow!(e)
    })?;

    tracing::info!(
        db  = db_kind(&cfg.database.url),
        dir = %cfg.media.recording_dir.display(),
        api = %cfg.api.bind,
        "Configuration loaded",
    );

    if cfg.encryption_key.is_empty() {
        tracing::error!(
            "VMS_ENCRYPTION_KEY is not set. \
             Generate one with: openssl rand -base64 32"
        );
        return Err(anyhow::anyhow!("missing encryption key"));
    }

    // ── Database ──────────────────────────────────────────────────────────────
    tracing::info!(db = db_kind(&cfg.database.url), "Connecting to database");

    let db = Database::connect(&cfg.database.url)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Database connection failed");
            anyhow::anyhow!(e)
        })?;

    tracing::info!("Database connected");

    // ── Migrations ────────────────────────────────────────────────────────────
    let pending = Migrator::get_pending_migrations(&db).await.map_err(|e| {
        tracing::error!(error = %e, "Failed to check pending migrations");
        anyhow::anyhow!(e)
    })?;

    if pending.is_empty() {
        tracing::info!("Database schema is up to date");
    } else {
        tracing::info!(count = pending.len(), "Applying migrations");
        Migrator::up(&db, None).await.map_err(|e| {
            tracing::error!(error = %e, "Migration failed");
            anyhow::anyhow!(e)
        })?;
        tracing::info!(count = pending.len(), "Migrations applied");
    }

    // ── Crypto ────────────────────────────────────────────────────────────────
    let crypto = Crypto::from_b64(&cfg.encryption_key).map_err(|e| {
        tracing::error!(error = %e, "Invalid encryption key");
        anyhow::anyhow!(e)
    })?;
    tracing::info!("Encryption key loaded");

    // ── Repositories ──────────────────────────────────────────────────────────
    let camera_repo = CameraRepo::new(db.clone(), crypto.clone());
    let source_repo = SourceRepo::new(db.clone(), crypto.clone());
    let dest_repo   = DestinationRepo::new(db.clone(), crypto);
    tracing::info!("Repository layer ready");

    // ── Media Manager ─────────────────────────────────────────────────────────
    let media_manager = MediaManager::new(MediaConfig {
        recording_dir:       cfg.media.recording_dir.clone(),
        chunk_duration_secs: cfg.media.chunk_duration_secs,
    })
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to initialise media manager");
        anyhow::anyhow!(e)
    })?;

    tracing::info!("Media manager ready");
    tracing::info!("VMS Daemon starting");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract the database type from a connection URL for safe logging.
/// Strips credentials — logs `"postgres"` not `"postgres://user:pass@host/db"`.
fn db_kind(url: &str) -> &str {
    url.split("://").next().unwrap_or("unknown")
}
