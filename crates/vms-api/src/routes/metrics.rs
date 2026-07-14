//! `GET /metrics` — Prometheus text-exposition scrape endpoint.
//!
//! `http_requests_total`, `pipeline_runs_total`, and
//! `event_bus_published_total` are counters accumulated as requests/runs/
//! events happen (see `MetricsMiddleware`, `PipelineExecutor`, `EventBus`).
//! `resource_state` is different: it's a point-in-time snapshot, so it's
//! recomputed here from `ResourceManager::all()` on every scrape rather than
//! incremented anywhere.
//!
//! Mounted under `public_routes()` alongside `GET /health` — for the same
//! reason a liveness/readiness probe has no credentials to present, neither
//! does a Prometheus scraper in a typical self-hosted deployment of this
//! daemon. The metric labels are all small fixed vocabularies (route
//! patterns, resource *types*, topic *kinds*) with no camera/pipeline UUIDs
//! or credential material, so there's nothing sensitive to gate.

use std::collections::HashMap;

use salvo::prelude::*;
use vms_core::{ResourceId, ResourceState};

use crate::state::AppState;

/// GET /metrics
#[handler]
pub async fn scrape(depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");

    let mut counts: HashMap<(&'static str, &'static str), i64> = HashMap::new();
    for (resource_id, entry) in state.resource_manager.all() {
        let resource_type = match resource_id {
            ResourceId::Source(_) => "source",
            ResourceId::CameraPipeline(_) => "camera_pipeline",
            ResourceId::RingBuffer(_) => "ring_buffer",
            ResourceId::AnalyticsBranch(_) => "analytics_branch",
            ResourceId::DestinationPool(_) => "destination_pool",
        };
        let state_label = match entry.state {
            ResourceState::Stopped => "stopped",
            ResourceState::Starting => "starting",
            ResourceState::Running => "running",
            ResourceState::Stopping => "stopping",
            ResourceState::Error(_) => "error",
        };
        *counts.entry((resource_type, state_label)).or_insert(0) += 1;
    }
    let counts: Vec<_> = counts.into_iter().collect();
    state.metrics.sync_resource_states(&counts);

    res.render(Text::Plain(state.metrics.render()));
}
