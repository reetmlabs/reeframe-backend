mod cli;
mod config;
mod gateway;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::{logs::SdkLoggerProvider, trace::SdkTracerProvider, Resource};
use salvo::conn::TcpListener;
use salvo::server::Server;
use salvo::Listener;
use sea_orm::Database;
use sea_orm_migration::MigratorTrait;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use vms_api::{
    auth::LocalJwtAuthProvider, coordinator_auth::CoordinatorJwksAuthProvider,
    routes::build_router, state::AppState,
};
use vms_db::{
    ApiKeyRepo, CameraRepo, ContactListRepo, ContactRepo, Crypto, DailyRecordingCoverageRepo,
    DestinationRepo, EventsRepo, ExportJobRepo, Migrator, PipelineRepo, PipelineRunRepo,
    RecordingRepo, SettingsRepo, SourceRepo, TileLayoutRepo, UserRepo,
};
use vms_engine::{
    CoverageConfig, EventBus, Metrics, PipelineExecutor, PipelineRegistry, RecordingIntentConfig,
    ResourceManager, RetentionConfig, StatMonitor, TriggerEvaluator,
};
use vms_media::{MediaConfig, MediaManager, RingBufferManager};
use vms_sources::SourceManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A subcommand (currently only `token ...`) runs a lightweight one-shot
    // path — connect to the DB, do one thing, exit — instead of starting the
    // full daemon below. No subcommand preserves the exact pre-existing
    // behavior (systemd/docker invoke the binary with no arguments).
    if let Some(command) = cli::Cli::parse().command {
        return cli::run(command).await;
    }

    eprintln!(
        "Reeframe VMS daemon v{} — starting",
        env!("CARGO_PKG_VERSION")
    );

    // -- Observability --
    let _otel_guard = init_tracing();

    // -- Config --
    // `mut`: dynamic settings resolution (below, once the DB is up) patches
    // this in place with any DB-stored override before anything downstream
    // — including the auth provider — is built from it.
    let mut cfg = config::load().map_err(|e| {
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

    // -- Database --
    tracing::info!(db = db_kind(&cfg.database.url), "Connecting to database");

    let db = Database::connect(&cfg.database.url).await.map_err(|e| {
        tracing::error!(error = %e, "Database connection failed");
        anyhow::anyhow!(e)
    })?;

    tracing::info!("Database connected");

    // -- Migrations --
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

    // -- Dynamic settings --
    // Must run before anything below is constructed from `cfg` — this is
    // the `defaults < file < env < DB` precedence step: a DB-stored
    // override (from a prior `PATCH /system/settings` or config-file
    // upload) wins over whatever was just loaded from the file/env, and a
    // key with no override yet gets seeded from that resolved value.
    let settings_repo = SettingsRepo::new(db.clone());
    config::resolve_dynamic_settings(&mut cfg, &settings_repo)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to resolve dynamic settings");
            anyhow::anyhow!(e)
        })?;
    tracing::info!(
        retention_days = cfg.recordings.retention_days,
        retention_disk_threshold_percent = cfg.recordings.retention_disk_threshold_percent,
        "Dynamic settings resolved",
    );

    // -- Auth config --
    if cfg.auth.mode != "local" && cfg.auth.mode != "oidc" {
        tracing::error!(
            mode = %cfg.auth.mode,
            "Unsupported [auth] mode — must be 'local' or 'oidc'"
        );
        return Err(anyhow::anyhow!("unsupported auth mode: {}", cfg.auth.mode));
    }
    if cfg.auth.jwt_secret.is_empty() {
        tracing::error!(
            "VMS_AUTH__JWT_SECRET is not set. \
             Generate one with: openssl rand -base64 32"
        );
        return Err(anyhow::anyhow!("missing JWT secret"));
    }
    let auth_provider = LocalJwtAuthProvider::new(
        cfg.auth.jwt_secret.clone(),
        cfg.auth.access_token_ttl_secs,
        cfg.auth.refresh_token_ttl_secs,
    );
    tracing::info!("Auth provider ready (local JWT)");

    // Local auth above is always active regardless of `mode` — Coordinator
    // trust is additive, never a replacement; the BE stays fully autonomous
    // with no `jwks_url` configured at all.
    let coordinator_auth_provider = if cfg.auth.mode == "oidc" {
        let jwks_url = cfg.auth.jwks_url.clone().ok_or_else(|| {
            tracing::error!("[auth] mode = \"oidc\" requires [auth] jwks_url to be set");
            anyhow::anyhow!("missing jwks_url for oidc auth mode")
        })?;
        let be_id = cfg.gateway.be_id.ok_or_else(|| {
            tracing::error!(
                "[auth] mode = \"oidc\" requires [gateway] be_id to be set — it's the expected \
                 `aud` for every Coordinator-issued token this BE accepts"
            );
            anyhow::anyhow!("missing gateway.be_id for oidc auth mode")
        })?;
        let provider = CoordinatorJwksAuthProvider::new(
            jwks_url.clone(),
            std::time::Duration::from_secs(cfg.auth.jwks_refresh_interval_secs),
            be_id,
        );
        provider.prefetch().await.map_err(|e| {
            tracing::error!(jwks_url = %jwks_url, error = %e, "Failed to fetch Coordinator JWKS");
            anyhow::anyhow!(e)
        })?;
        tracing::info!(jwks_url = %jwks_url, "Coordinator JWKS provider ready");
        Some(Arc::new(provider))
    } else {
        None
    };

    // -- Gateway (Relay/tunnel client for WAPP pairing) --
    // Unset by default — matches Coordinator's own "stays autonomous"
    // guarantee; with no `[gateway] url`, zero connection attempt is made.
    let gateway_task = gateway::resolve(&cfg.gateway)
        .map_err(|e| {
            tracing::error!(error = %e, "Invalid gateway configuration");
            anyhow::anyhow!(e)
        })?
        .map(|(url, be_id)| {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            tracing::info!(url = %url, %be_id, "Gateway client starting");
            let join_handle = tokio::spawn(gateway::run(url, be_id, shutdown_rx));
            (shutdown_tx, join_handle)
        });

    // -- Crypto --
    let crypto = Crypto::from_b64(&cfg.encryption_key).map_err(|e| {
        tracing::error!(error = %e, "Invalid encryption key");
        anyhow::anyhow!(e)
    })?;
    tracing::info!("Encryption key loaded");

    // -- Repositories --
    let encryption_key = crypto.key_bytes();
    let camera_repo = CameraRepo::new(db.clone(), crypto.clone());
    let source_repo = SourceRepo::new(db.clone(), crypto.clone());
    let dest_repo = DestinationRepo::new(db.clone(), crypto);
    let contact_repo = ContactRepo::new(db.clone());
    let contact_list_repo = ContactListRepo::new(db.clone());
    let pipeline_repo = PipelineRepo::new(db.clone());
    let pipeline_run_repo = PipelineRunRepo::new(db.clone());
    let recording_repo = RecordingRepo::new(db.clone());
    let daily_coverage_repo = DailyRecordingCoverageRepo::new(db.clone());
    let export_job_repo = ExportJobRepo::new(db.clone());
    let user_repo = UserRepo::new(db.clone());
    let api_key_repo = ApiKeyRepo::new(db.clone());
    let tile_layout_repo = TileLayoutRepo::new(db.clone());
    let events_repo = EventsRepo::new(db.clone());
    tracing::info!("Repository layer ready");

    // -- Media Manager --
    // `media_event_tx` is created up front because both `vms-media` (motion/
    // scene-change/tamper detection) and `vms-sources` adapters publish onto
    // the same channel; the bridge task below forwards each onto the Event
    // Bus by whichever ID it carries, keeping both crates free of a
    // vms-engine dependency. `chunk_event_tx` is the equivalent channel for
    // recording-chunk lifecycle bookkeeping — `vms-media` has no DB access,
    // so the consumer task below turns each event into a `RecordingRepo`
    // call instead.
    tracing::info!(bind = %cfg.rtsp.bind, "Starting RTSP relay server");
    let (media_event_tx, mut media_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (chunk_event_tx, mut chunk_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (pipeline_live_tx, mut pipeline_live_rx) = tokio::sync::mpsc::unbounded_channel();
    let media_manager = Arc::new(
        MediaManager::new(
            MediaConfig {
                recording_dir: cfg.media.recording_dir.clone(),
                chunk_duration_secs: cfg.media.chunk_duration_secs,
                rtsp_bind: cfg.rtsp.bind.clone(),
                thumbnail_interval_secs: cfg.media.thumbnail_interval_secs,
            },
            media_event_tx.clone(),
            chunk_event_tx,
            pipeline_live_tx,
        )
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to initialise media manager");
            anyhow::anyhow!(e)
        })?,
    );
    tracing::info!("Media manager and RTSP relay server ready");

    // -- Recording-chunk indexing bridge --
    // Turns each `RecordingChunkEvent` into a `recordings` row — insert on
    // open, backfill end_time/size_bytes on close.
    {
        let recording_repo = recording_repo.clone();
        tokio::spawn(async move {
            use vms_core::RecordingChunkEvent;

            while let Some(event) = chunk_event_rx.recv().await {
                match event {
                    RecordingChunkEvent::Opened {
                        camera_id,
                        file_path,
                        chunk_index,
                        start_time,
                        codec,
                    } => {
                        if let Err(e) = recording_repo
                            .open_chunk(vms_db::OpenChunk {
                                camera_id,
                                file_path,
                                chunk_index,
                                start_time: start_time.fixed_offset(),
                                codec,
                            })
                            .await
                        {
                            tracing::error!(camera_id = %camera_id, error = %e, "Failed to record chunk open");
                        }
                    }
                    RecordingChunkEvent::Closed {
                        camera_id,
                        file_path,
                        end_time,
                        size_bytes,
                    } => {
                        if let Err(e) = recording_repo
                            .close_chunk_by_path(
                                camera_id,
                                &file_path,
                                end_time.fixed_offset(),
                                size_bytes,
                            )
                            .await
                        {
                            tracing::error!(camera_id = %camera_id, error = %e, "Failed to record chunk close");
                        }
                    }
                    RecordingChunkEvent::Discarded {
                        camera_id,
                        file_path,
                    } => {
                        if let Err(e) = recording_repo
                            .discard_open_chunk(camera_id, &file_path)
                            .await
                        {
                            tracing::error!(camera_id = %camera_id, error = %e, "Failed to discard empty chunk row");
                        }
                    }
                }
            }
        });
    }

    // -- Recording-intent reconciliation bridge --
    // Fires every time a camera's pipeline comes up or reconnects
    // (`MediaManager::pipeline_live_tx`) — resumes recording if
    // `desired_recording` is set and it isn't already, without waiting for
    // the periodic sweep.
    {
        let camera_repo = camera_repo.clone();
        let media_manager = media_manager.clone();
        tokio::spawn(async move {
            while let Some(camera_id) = pipeline_live_rx.recv().await {
                vms_engine::reconcile_recording_intent(&camera_repo, &media_manager, camera_id)
                    .await;
            }
        });
    }

    // -- Ring Buffer Manager --
    let ring_buffer_manager = RingBufferManager::new(media_manager.clone());
    tracing::info!("Ring buffer manager ready");

    // -- Metrics --
    let metrics = Metrics::new();

    // -- Event Bus --
    let event_bus = EventBus::new_with_metrics(vms_engine::DEFAULT_CAPACITY, metrics.clone());
    tracing::info!(capacity = vms_engine::DEFAULT_CAPACITY, "Event bus ready");

    // -- Source Manager --
    // Camera-scoped events are also persisted into the `events` table
    // here, at the same tap point as the topic bridge — `TopicKey::Camera`
    // has no wildcard, so a separate subscriber can't see every camera.
    let source_manager = SourceManager::new(media_event_tx);
    let bridge_event_bus = event_bus.clone();
    let bridge_events_repo = events_repo.clone();
    tokio::spawn(async move {
        while let Some(event) = media_event_rx.recv().await {
            let topic = if let Some(source_id) = event.source_id {
                vms_core::TopicKey::Source(source_id)
            } else if let Some(camera_id) = event.camera_id {
                if let Err(e) = bridge_events_repo
                    .record(vms_db::repos::event::CreateEvent {
                        camera_id,
                        event_type: event.event_type.clone(),
                        payload: event.payload.clone(),
                        occurred_at: event.occurred_at.fixed_offset(),
                    })
                    .await
                {
                    tracing::error!(camera_id = %camera_id, error = %e, "Failed to record event history");
                }
                vms_core::TopicKey::Camera(camera_id)
            } else {
                continue;
            };
            bridge_event_bus.publish(&topic, event);
        }
    });
    tracing::info!("Source manager ready");

    // -- Pipeline Registry --
    let pipeline_registry = PipelineRegistry::new(pipeline_repo.clone());
    pipeline_registry.load().await.map_err(|e| {
        tracing::error!(error = %e, "Failed to load pipeline registry");
        anyhow::anyhow!(e)
    })?;

    // -- Resource Manager --
    let resource_manager = ResourceManager::new(
        media_manager.clone(),
        camera_repo.clone(),
        ring_buffer_manager.clone(),
        source_repo.clone(),
        source_manager,
    );
    resource_manager
        .recover(&pipeline_registry)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to recover resource manager");
            anyhow::anyhow!(e)
        })?;
    tracing::info!("Resource manager ready");

    // -- Auto-start relays for cameras that have a cached codec --
    // These start instantly (no probe needed). Relays bridge from a
    // pipeline's tee rather than opening a connection of their own (see
    // `vms-media`'s relay rework) and can now bring that pipeline up
    // on-demand — but at boot we still only pre-warm relays for cameras
    // whose live pipeline `resource_manager.recover()` (above) already
    // started; cameras not referenced by any enabled pipeline are left
    // alone rather than pre-warmed into a live pipeline nobody asked for
    // yet. They'll still start on-demand the first time a client actually
    // requests their relay at runtime.
    {
        let all_cameras = camera_repo.list().await.unwrap_or_default();
        let cached: Vec<_> = all_cameras
            .into_iter()
            .filter(|c| c.enabled && c.codec.is_some())
            .collect();

        if !cached.is_empty() {
            tracing::info!(
                count = cached.len(),
                "Auto-starting relays for cameras with cached codec"
            );
            for cam in cached {
                let id = cam.id;
                let codec = cam.codec.unwrap();

                if !media_manager.is_running(id) {
                    tracing::debug!(
                        camera_id = %id,
                        "Skipping relay auto-start — camera pipeline is not running"
                    );
                    continue;
                }

                // Already running (guarded above), so `start_relay`'s
                // rtsp_url/sub_rtsp_url arguments are never exercised here —
                // still resolved properly rather than passed as placeholders,
                // in case that guard's assumption ever stops holding.
                let Some((decrypted, password)) =
                    camera_repo.get_decrypted(id).await.unwrap_or(None)
                else {
                    continue;
                };
                let rtsp_url = build_rtsp_url(
                    &decrypted.rtsp_url,
                    decrypted.username.as_deref(),
                    password.as_deref(),
                );
                let sub_rtsp_url = decrypted.sub_rtsp_url.as_deref().map(|sub| {
                    build_rtsp_url(sub, decrypted.username.as_deref(), password.as_deref())
                });

                match media_manager
                    .start_relay(
                        id,
                        vms_media::RelayQuality::Main,
                        &rtsp_url,
                        sub_rtsp_url.as_deref(),
                        Some(&codec),
                    )
                    .await
                {
                    Ok(_) => tracing::info!(camera_id = %id, codec, "Main relay auto-started"),
                    Err(e) => {
                        tracing::warn!(camera_id = %id, error = %e, "Failed to auto-start main relay")
                    }
                }

                if sub_rtsp_url.is_some() {
                    match media_manager
                        .start_relay(
                            id,
                            vms_media::RelayQuality::Sub,
                            &rtsp_url,
                            sub_rtsp_url.as_deref(),
                            Some(&codec),
                        )
                        .await
                    {
                        Ok(_) => tracing::info!(camera_id = %id, codec, "Sub relay auto-started"),
                        Err(e) => {
                            tracing::warn!(camera_id = %id, error = %e, "Failed to auto-start sub relay")
                        }
                    }
                }
            }
        }
    }

    // -- Reconcile orphaned recording chunks from a previous process lifetime --
    // Must run before the resume loop below opens any fresh chunks of its
    // own, since every `end_time IS NULL` row found here is unconditionally
    // a leftover, never a chunk this process is currently writing.
    vms_engine::reconcile_orphaned_chunks(&recording_repo).await;

    // -- Resume recordings the operator started manually --
    // Parallel to the relay auto-start loop above, not folded into
    // `resource_manager.recover()` — that stays scoped to cameras
    // referenced by an enabled automation pipeline. A camera
    // recording only because an operator clicked "start" has no such
    // reference, so it needs its own pass over `desired_recording` here.
    {
        let wanted: Vec<_> = camera_repo
            .list()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.enabled && c.desired_recording)
            .collect();

        if !wanted.is_empty() {
            tracing::info!(
                count = wanted.len(),
                "Resuming manually-started recordings from persisted intent"
            );
            for cam in wanted {
                vms_engine::reconcile_recording_intent(&camera_repo, &media_manager, cam.id).await;
            }
        }
    }

    // -- Pipeline Executor --
    let pipeline_executor = PipelineExecutor::new(
        pipeline_run_repo.clone(),
        dest_repo.clone(),
        media_manager.clone(),
        ring_buffer_manager.clone(),
        cfg.media.recording_dir.clone(),
        Some(encryption_key),
        metrics.clone(),
    );
    tracing::info!("Pipeline executor ready");

    // -- Trigger Evaluator --
    let trigger_evaluator = TriggerEvaluator::new(
        pipeline_registry.clone(),
        event_bus.clone(),
        pipeline_executor,
        pipeline_repo.clone(),
    );

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

    // -- Stat Monitor --
    let stat_monitor = StatMonitor::new(trigger_evaluator.clone(), pipeline_registry.clone());
    stat_monitor.set_retention(RetentionConfig {
        recording_repo: recording_repo.clone(),
        camera_repo: camera_repo.clone(),
        recording_dir: cfg.media.recording_dir.clone(),
        retention_days: cfg.recordings.retention_days,
        retention_disk_threshold_percent: cfg.recordings.retention_disk_threshold_percent,
    });
    stat_monitor.set_coverage(CoverageConfig {
        recording_repo: recording_repo.clone(),
        camera_repo: camera_repo.clone(),
        coverage_repo: daily_coverage_repo.clone(),
        retention_days: cfg.recordings.retention_days,
        default_timezone: cfg.recordings.timezone.clone(),
    });
    stat_monitor.set_recording_intent(RecordingIntentConfig {
        camera_repo: camera_repo.clone(),
        media_manager: media_manager.clone(),
    });
    stat_monitor.clone().start();
    tracing::info!("Stat monitor started");

    // -- HTTP API --
    let trigger_evaluator_shutdown = trigger_evaluator.clone();
    let state = AppState {
        db: db.clone(),
        camera_repo,
        source_repo,
        dest_repo,
        contact_repo,
        contact_list_repo,
        pipeline_repo,
        pipeline_run_repo,
        recording_repo,
        daily_coverage_repo,
        export_job_repo,
        settings_repo,
        tile_layout_repo,
        events_repo,
        media_recording_dir: cfg.media.recording_dir.clone(),
        config_file_path: PathBuf::from(config::CONFIG_FILE_PATH),
        config_parser: Arc::new(config::parse_uploaded_config),
        user_repo,
        api_key_repo,
        auth_provider,
        coordinator_auth_provider,
        media_manager: media_manager.clone(),
        ring_buffer_manager,
        event_bus,
        pipeline_registry,
        resource_manager,
        trigger_evaluator,
        stat_monitor,
        metrics,
    };

    let router = build_router(state);

    tracing::info!(bind = %cfg.api.bind, "Starting HTTP API server");

    let acceptor = TcpListener::new(cfg.api.bind.clone()).bind().await;
    let server = Server::new(acceptor);
    let server_handle = server.handle();
    let server_task = tokio::spawn(server.serve(router));

    tracing::info!("VMS Daemon started — press Ctrl+C or send SIGTERM to stop");
    eprintln!(
        "Reeframe VMS daemon v{} — listening on {}",
        env!("CARGO_PKG_VERSION"),
        cfg.api.bind
    );

    // -- Wait for shutdown signal --
    shutdown_signal().await;
    tracing::info!("Shutdown signal received — draining HTTP connections (10 s timeout)");

    server_handle.stop_graceful(std::time::Duration::from_secs(10));
    server_task.await.ok();
    tracing::info!("HTTP server stopped");

    // -- Graceful shutdown: trigger schedulers and event listeners --
    trigger_evaluator_shutdown.stop_schedulers().await;
    trigger_evaluator_shutdown.stop_event_listener();
    tracing::info!("Trigger evaluator schedulers and event listeners stopped");

    // -- Graceful shutdown: media pipelines --
    media_manager.shutdown().await.map_err(|e| {
        tracing::error!(error = %e, "Error during media manager shutdown");
        anyhow::anyhow!(e)
    })?;

    // -- Graceful shutdown: gateway client --
    if let Some((shutdown_tx, join_handle)) = gateway_task {
        let _ = shutdown_tx.send(());
        join_handle.await.ok();
        tracing::info!("Gateway client stopped");
    }

    tracing::info!("VMS Daemon stopped");
    Ok(())
}

// -- Helpers --

/// Installs the global `tracing` subscriber: an always-on JSON layer to
/// stderr, plus — only if
/// `OTEL_EXPORTER_OTLP_ENDPOINT` or one of the OTel SDK's more specific
/// `OTEL_EXPORTER_OTLP_{TRACES,LOGS}_ENDPOINT` env vars is set — export of
/// spans and log events to that collector over OTLP. With none of those set,
/// behavior is identical to before this function existed: purely additive,
/// not a breaking change to the default (no env vars set) case.
///
/// Returns a guard that must be held for the lifetime of `main` — dropping
/// it flushes any pending OTLP batches and shuts the exporters down cleanly.
/// Held even on early-return via `?`, since Rust drops locals on unwind.
fn init_tracing() -> OtelGuard {
    let otlp_enabled = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    ]
    .iter()
    .any(|var| std::env::var(var).is_ok());

    let fmt_layer = fmt::layer()
        .json()
        .with_writer(std::io::stderr)
        .with_current_span(true)
        .with_span_list(true);

    if !otlp_enabled {
        tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(fmt_layer)
            .init();
        return OtelGuard::Disabled;
    }

    // Suppresses tracing/logs generated by the OTLP HTTP export client
    // itself from being re-captured and re-exported — without this, the
    // exporter's own request-handling spans/logs would recurse into more
    // export calls. Only applied to the OTel-facing layers; stderr JSON
    // output is unaffected and still governed solely by RUST_LOG.
    let otel_noise_filter = || {
        EnvFilter::new("info")
            .add_directive("hyper=off".parse().expect("valid directive"))
            .add_directive("reqwest=off".parse().expect("valid directive"))
            .add_directive("h2=off".parse().expect("valid directive"))
    };

    let resource = Resource::builder()
        .with_service_name("reeframe-vms-daemon")
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    let tracer = tracer_provider.tracer("reeframe-vms-daemon");
    let otel_trace_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(otel_noise_filter());

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .build()
        .expect("failed to build OTLP log exporter");
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();
    let otel_log_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_noise_filter());

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    OtelGuard::Enabled {
        tracer_provider,
        logger_provider,
    }
}

/// Held for the process lifetime by `main`. See [`init_tracing`].
enum OtelGuard {
    Disabled,
    Enabled {
        tracer_provider: SdkTracerProvider,
        logger_provider: SdkLoggerProvider,
    },
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Self::Enabled {
            tracer_provider,
            logger_provider,
        } = self
        {
            // eprintln, not tracing:: — this *is* the tracing infrastructure
            // shutting down, so routing through it here would be circular.
            if let Err(e) = tracer_provider.shutdown() {
                eprintln!("Error shutting down OTLP tracer provider: {e}");
            }
            if let Err(e) = logger_provider.shutdown() {
                eprintln!("Error shutting down OTLP logger provider: {e}");
            }
        }
    }
}

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

/// Inject credentials into an RTSP URL if both username and password are present.
/// `rtsp://host/path` + (user, pass) -> `rtsp://user:pass@host/path`
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    if let (Some(u), Some(p)) = (username, password) {
        if let Some(rest) = base_url.strip_prefix("rtsp://") {
            return format!("rtsp://{u}:{p}@{rest}");
        }
    }
    base_url.to_string()
}
