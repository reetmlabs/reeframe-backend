//! `GET /metrics`: Prometheus text-exposition scrape endpoint.
//!
//! `http_requests_total`, `pipeline_runs_total` and `event_bus_published_total`
//! are counters incremented as requests, runs and events happen (see
//! `MetricsMiddleware`, `PipelineExecutor`, `EventBus`). `resource_state` is a
//! point-in-time snapshot, so it is recomputed from `ResourceManager::all()` on
//! every scrape.
//!
//! Mounted under `public_routes()` next to `GET /health` because a Prometheus
//! scraper in a typical self-hosted deployment has no credentials to present.
//! The labels are small fixed vocabularies (route patterns, resource types,
//! topic kinds) with no camera or pipeline UUIDs or credentials, so nothing
//! sensitive is exposed.

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
