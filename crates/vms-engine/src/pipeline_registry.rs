use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use uuid::Uuid;
use vms_core::{pipeline::CompiledPipeline, VmsError};
use vms_db::PipelineRepo;

/// Hot-reloadable in-memory registry of compiled pipeline DAGs.
///
/// Backed by an `ArcSwap<HashMap<Uuid, Arc<CompiledPipeline>>>`. Readers take a
/// single atomic load and never block writers; a `reload()` atomically swaps the
/// entire map so in-flight executor runs that already hold an `Arc<CompiledPipeline>`
/// continue against the old snapshot uninterrupted.
pub struct PipelineRegistry {
    store: ArcSwap<HashMap<Uuid, Arc<CompiledPipeline>>>,
    repo: PipelineRepo,
}

impl PipelineRegistry {
    pub fn new(repo: PipelineRepo) -> Arc<Self> {
        Arc::new(Self {
            store: ArcSwap::from_pointee(HashMap::new()),
            repo,
        })
    }

    /// Populate the registry from DB. Call once at daemon startup before serving requests.
    pub async fn load(&self) -> Result<(), VmsError> {
        self.reload().await
    }

    /// Re-fetch all enabled pipelines from DB and atomically swap in a fresh snapshot.
    ///
    /// Pipelines that fail DAG validation or deserialization are logged at WARN and
    /// skipped — the reload always succeeds as long as the DB query itself succeeds.
    /// Callers (API mutation handlers) should invoke this after any change to a
    /// pipeline, its nodes, edges, or triggers.
    pub async fn reload(&self) -> Result<(), VmsError> {
        let rows = self.repo.list_enabled().await?;
        let mut map = HashMap::with_capacity(rows.len());

        for row in &rows {
            match self.repo.load_compiled(row.id).await {
                Ok(Some(compiled)) => {
                    map.insert(compiled.id, Arc::new(compiled));
                }
                Ok(None) => {
                    // Pipeline deleted between list_enabled and load_compiled — ignore.
                    tracing::debug!(
                        pipeline_id = %row.id,
                        "Pipeline disappeared during registry reload"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline_id = %row.id,
                        name         = %row.name,
                        error        = %e,
                        "Pipeline failed to compile — skipping"
                    );
                }
            }
        }

        let loaded  = map.len();
        let skipped = rows.len() - loaded;
        self.store.store(Arc::new(map));

        if skipped > 0 {
            tracing::info!(loaded, skipped, "Pipeline registry reloaded");
        } else {
            tracing::info!(loaded, "Pipeline registry reloaded");
        }

        Ok(())
    }

    /// Look up a single compiled pipeline by UUID.
    ///
    /// Lock-free O(1) read. Returns `None` if the pipeline is not enabled or
    /// does not exist.
    pub fn get(&self, id: Uuid) -> Option<Arc<CompiledPipeline>> {
        self.store.load().get(&id).cloned()
    }

    /// Return the full registry snapshot as an `Arc<HashMap<...>>`.
    ///
    /// Prefer this over repeated `get()` calls when iterating all pipelines
    /// (e.g. the trigger evaluator on startup). Takes a single atomic load.
    pub fn snapshot(&self) -> Arc<HashMap<Uuid, Arc<CompiledPipeline>>> {
        self.store.load_full()
    }
}
