//! Prometheus metrics registry for the VMS daemon.
//!
//! One [`Metrics`] instance is created in `main.rs` and shared (via `Arc`)
//! with everything that needs to record a metric: the HTTP layer (requests
//! by route/method/status), [`crate::EventBus`] (events published, by topic
//! kind), and [`crate::PipelineExecutor`] (pipeline runs, by outcome).
//! Resource state gauges are a point-in-time snapshot, so they are synced
//! from [`crate::ResourceManager::all`] at scrape time instead of being
//! incremented as events happen.
//!
//! Label values are restricted to small, fixed vocabularies (route patterns
//! instead of raw URIs, topic *kind* and resource *type* instead of UUIDs) so
//! label cardinality stays bounded.

use std::sync::Arc;

use prometheus::{CounterVec, Encoder, GaugeVec, Opts, Registry, TextEncoder};

pub struct Metrics {
    registry: Registry,
    http_requests_total: CounterVec,
    pipeline_runs_total: CounterVec,
    resource_state: GaugeVec,
    events_published_total: CounterVec,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let http_requests_total = CounterVec::new(
            Opts::new(
                "http_requests_total",
                "Total HTTP requests handled, by matched route, method, and status code",
            ),
            &["route", "method", "status"],
        )
        .expect("valid metric definition");

        let pipeline_runs_total = CounterVec::new(
            Opts::new(
                "pipeline_runs_total",
                "Total pipeline runs finished, by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric definition");

        let resource_state = GaugeVec::new(
            Opts::new(
                "resource_state",
                "Current number of managed resources in each (type, state) combination",
            ),
            &["resource_type", "state"],
        )
        .expect("valid metric definition");

        let events_published_total = CounterVec::new(
            Opts::new(
                "event_bus_published_total",
                "Total events published on the event bus, by topic kind",
            ),
            &["topic"],
        )
        .expect("valid metric definition");

        registry
            .register(Box::new(http_requests_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(pipeline_runs_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(resource_state.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(events_published_total.clone()))
            .expect("unique metric name");

        Arc::new(Self {
            registry,
            http_requests_total,
            pipeline_runs_total,
            resource_state,
            events_published_total,
        })
    }

    /// Record one completed HTTP request. `route` should be the matched
    /// route *pattern* (e.g. `"/cameras/{id}"`), never the raw URI, or every
    /// distinct camera ID would mint a new label combination.
    pub fn record_http_request(&self, route: &str, method: &str, status: u16) {
        self.http_requests_total
            .with_label_values(&[route, method, &status.to_string()])
            .inc();
    }

    /// Record one finished pipeline run. `outcome` is a small fixed
    /// vocabulary (e.g. `"completed"`, `"failed"`).
    pub fn record_pipeline_run(&self, outcome: &str) {
        self.pipeline_runs_total.with_label_values(&[outcome]).inc();
    }

    /// Record one event published on the event bus. `topic_kind` is the
    /// *kind* of topic (e.g. `"camera"`, `"source"`, `"system"`, `"stat"`),
    /// never the topic's embedded UUID.
    pub fn record_event_published(&self, topic_kind: &str) {
        self.events_published_total
            .with_label_values(&[topic_kind])
            .inc();
    }

    /// Replace the resource-state gauge's contents with `counts`, pairs of
    /// `((resource_type, state), count)` tallied by the caller from a fresh
    /// [`crate::ResourceManager::all`] snapshot. Resets first so a
    /// (type, state) combination that no longer has any resources doesn't
    /// linger at its last nonzero value.
    pub fn sync_resource_states(&self, counts: &[((&str, &str), i64)]) {
        self.resource_state.reset();
        for ((resource_type, state), count) in counts {
            self.resource_state
                .with_label_values(&[resource_type, state])
                .set(*count as f64);
        }
    }

    /// Render the current state of every registered metric in Prometheus
    /// text exposition format.
    pub fn render(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .expect("encoding to an in-memory buffer cannot fail");
        String::from_utf8(buffer).expect("Prometheus text encoder always produces valid UTF-8")
    }
}
