// vms-daemon — main entry point.
// Wires all subsystems together and starts the server.
//
// Observability bootstrap: installs a minimal tracing-subscriber that writes
// JSON-structured logs to stderr. This is replaced in step 8c with the full
// OTLP pipeline (opentelemetry-otlp + opentelemetry-sdk + tracing-opentelemetry).

use tracing_subscriber::{fmt, EnvFilter};

fn main() {
    // Initialise structured logging to stderr.
    // Log level is controlled by the RUST_LOG env var (default: info).
    // Example: RUST_LOG=vms_daemon=debug,vms_media=trace cargo run
    fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .with_current_span(true)
        .with_span_list(true)
        .init();

    tracing::info!("VMS Daemon starting");
}
