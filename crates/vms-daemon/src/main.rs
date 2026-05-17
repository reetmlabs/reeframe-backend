mod config;

use std::sync::Arc;

use salvo::conn::TcpListener;
use salvo::server::Server;
use salvo::Listener;
use sea_orm::Database;
use sea_orm_migration::MigratorTrait;
use tracing_subscriber::{fmt, EnvFilter};
use vms_api::{routes::build_router, state::AppState};
use vms_db::{CameraRepo, Crypto, DestinationRepo, Migrator, PipelineRepo, SourceRepo};
use vms_engine::{EventBus, PipelineRegistry, ResourceManager, TriggerEvaluator};
use vms_media::{MediaConfig, MediaManager, RingBufferManager};

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

    let db = Database::connect(&cfg.database.url).await.map_err(|e| {
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
    let dest_repo = DestinationRepo::new(db.clone(), crypto);
    let pipeline_repo = PipelineRepo::new(db.clone());
    tracing::info!("Repository layer ready");

    // ── Media Manager ─────────────────────────────────────────────────────────
    let media_manager = Arc::new(
        MediaManager::new(MediaConfig {
            recording_dir: cfg.media.recording_dir.clone(),
            chunk_duration_secs: cfg.media.chunk_duration_secs,
        })
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to initialise media manager");
            anyhow::anyhow!(e)
        })?,
    );
    tracing::info!("Media manager ready");

    // ── Ring Buffer Manager ───────────────────────────────────────────────────
    let ring_buffer_manager = RingBufferManager::new(media_manager.clone());
    tracing::info!("Ring buffer manager ready");

    // ── Event Bus ─────────────────────────────────────────────────────────────
    let event_bus = EventBus::new(vms_engine::DEFAULT_CAPACITY);
    tracing::info!(capacity = vms_engine::DEFAULT_CAPACITY, "Event bus ready");

    // ── Pipeline Registry ─────────────────────────────────────────────────────
    let pipeline_registry = PipelineRegistry::new(pipeline_repo.clone());
    pipeline_registry.load().await.map_err(|e| {
        tracing::error!(error = %e, "Failed to load pipeline registry");
        anyhow::anyhow!(e)
    })?;

    // ── Resource Manager ──────────────────────────────────────────────────────
    let resource_manager = ResourceManager::new(
        media_manager.clone(),
        camera_repo.clone(),
        ring_buffer_manager.clone(),
    );
    resource_manager
        .recover(&pipeline_registry)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to recover resource manager");
            anyhow::anyhow!(e)
        })?;
    tracing::info!("Resource manager ready");

    // ── Trigger Evaluator ─────────────────────────────────────────────────────
    let trigger_evaluator = TriggerEvaluator::new(pipeline_registry.clone(), event_bus.clone());

    trigger_evaluator.clone().start_event_listener();
    tracing::info!("Trigger evaluator event listeners started");

    trigger_evaluator
        .clone()
        .start_schedulers()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to start trigger schedulers");
            anyhow::anyhow!(e)
        })?;
    tracing::info!("Trigger evaluator schedulers started");

    // ── HTTP API ──────────────────────────────────────────────────────────────
    let state = AppState {
        camera_repo,
        source_repo,
        dest_repo,
        pipeline_repo,
        media_manager: media_manager.clone(),
        ring_buffer_manager,
        event_bus,
        pipeline_registry,
        resource_manager,
        trigger_evaluator,
    };

    let router = build_router(state);

    tracing::info!(bind = %cfg.api.bind, "Starting HTTP API server");

    let acceptor = TcpListener::new(cfg.api.bind.clone()).bind().await;
    let server = Server::new(acceptor);
    let server_handle = server.handle();
    let server_task = tokio::spawn(server.serve(router));

    tracing::info!("VMS Daemon started — press Ctrl+C or send SIGTERM to stop");

    // ── Wait for shutdown signal ───────────────────────────────────────────────
    shutdown_signal().await;
    tracing::info!("Shutdown signal received — draining HTTP connections (10 s timeout)");

    server_handle.stop_graceful(std::time::Duration::from_secs(10));
    server_task.await.ok();
    tracing::info!("HTTP server stopped");

    // ── Graceful shutdown: media pipelines ────────────────────────────────────
    media_manager.shutdown().await.map_err(|e| {
        tracing::error!(error = %e, "Error during media manager shutdown");
        anyhow::anyhow!(e)
    })?;

    tracing::info!("VMS Daemon stopped");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Wait for SIGINT (Ctrl+C) or SIGTERM (systemd / docker stop).
/// Whichever arrives first triggers a clean shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    // On non-Unix platforms (Windows) only Ctrl+C is available.
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c  => tracing::info!("Received SIGINT"),
        _ = sigterm => tracing::info!("Received SIGTERM"),
    }
}

/// Extract the database scheme from a connection URL for safe logging.
/// Strips credentials — logs `"postgres"` not `"postgres://user:pass@host/db"`.
fn db_kind(url: &str) -> &str {
    url.split("://").next().unwrap_or("unknown")
}
