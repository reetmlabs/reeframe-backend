use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use uuid::Uuid;
use vms_core::{pipeline::CompiledPipeline, TopicKey, TriggerConfig, TriggerType, VmsError};
use vms_db::PipelineRepo;

/// Atomic snapshot of the pipeline registry — pipeline map plus trigger index.
///
/// Both fields are rebuilt together on every [`PipelineRegistry::reload`] and
/// stored in a single `Arc` so readers always see a consistent pair.
pub struct RegistrySnapshot {
    /// All currently enabled compiled pipelines, keyed by pipeline UUID.
    pub pipelines: HashMap<Uuid, Arc<CompiledPipeline>>,
    /// Pre-built index for O(1) event dispatch.
    ///
    /// Key: `(topic, trigger_type)` — the topic the event arrived on and the
    /// kind of trigger it should fire.  Value: list of `(pipeline_id, trigger_id)`
    /// pairs that must be evaluated when a matching event arrives.
    pub trigger_index: HashMap<(TopicKey, TriggerType), Vec<(Uuid, Uuid)>>,
}

/// Hot-reloadable in-memory registry of compiled pipeline DAGs.
///
/// Backed by an `ArcSwap<RegistrySnapshot>`. Readers take a single atomic load
/// and never block writers; a `reload()` atomically swaps the entire snapshot so
/// in-flight executor runs that already hold an `Arc<CompiledPipeline>` continue
/// against the old snapshot uninterrupted.
pub struct PipelineRegistry {
    store: ArcSwap<RegistrySnapshot>,
    repo: Option<PipelineRepo>,
}

impl PipelineRegistry {
    pub fn new(repo: PipelineRepo) -> Arc<Self> {
        Arc::new(Self {
            store: ArcSwap::from_pointee(RegistrySnapshot {
                pipelines: HashMap::new(),
                trigger_index: HashMap::new(),
            }),
            repo: Some(repo),
        })
    }

    /// Construct a registry pre-loaded with `pipelines`. Only for unit tests —
    /// avoids a live database connection.  `load`/`reload` are no-ops.
    #[cfg(test)]
    pub fn new_test(pipelines: Vec<CompiledPipeline>) -> Arc<Self> {
        let map: HashMap<Uuid, Arc<CompiledPipeline>> =
            pipelines.into_iter().map(|p| (p.id, Arc::new(p))).collect();
        let index = build_trigger_index(&map);
        Arc::new(Self {
            store: ArcSwap::from_pointee(RegistrySnapshot {
                pipelines: map,
                trigger_index: index,
            }),
            repo: None,
        })
    }

    /// Populate the registry from DB. Call once at daemon startup before serving requests.
    pub async fn load(&self) -> Result<(), VmsError> {
        self.reload().await?;
        Ok(())
    }

    /// Re-fetch all enabled pipelines from DB and atomically swap in a fresh snapshot.
    ///
    /// Pipelines that fail DAG validation or deserialization are logged at WARN and
    /// skipped — the reload always succeeds as long as the DB query itself succeeds.
    /// Callers (API mutation handlers) should invoke this after any change to a
    /// pipeline, its nodes, edges, or triggers.
    ///
    /// Returns the snapshot that was in effect just before the swap, so a caller
    /// can diff it against the new one to reconcile resource ref-counts.
    pub async fn reload(&self) -> Result<Arc<RegistrySnapshot>, VmsError> {
        let Some(repo) = &self.repo else {
            return Ok(self.store.load_full());
        };
        let rows = repo.list_enabled().await?;
        let mut pipelines = HashMap::with_capacity(rows.len());

        for row in &rows {
            match repo.load_compiled(row.id).await {
                Ok(Some(compiled)) => {
                    pipelines.insert(compiled.id, Arc::new(compiled));
                }
                Ok(None) => {
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

        let loaded = pipelines.len();
        let skipped = rows.len() - loaded;
        let trigger_index = build_trigger_index(&pipelines);
        let previous = self.store.swap(Arc::new(RegistrySnapshot {
            pipelines,
            trigger_index,
        }));

        if skipped > 0 {
            tracing::info!(loaded, skipped, "Pipeline registry reloaded");
        } else {
            tracing::info!(loaded, "Pipeline registry reloaded");
        }

        Ok(previous)
    }

    /// Look up a single compiled pipeline by UUID.
    ///
    /// Lock-free O(1) read. Returns `None` if the pipeline is not enabled or
    /// does not exist.
    pub fn get(&self, id: Uuid) -> Option<Arc<CompiledPipeline>> {
        self.store.load().pipelines.get(&id).cloned()
    }

    /// Return the full registry snapshot (pipeline map + trigger index).
    ///
    /// Takes a single atomic load. Both fields are always consistent with each other.
    pub fn snapshot(&self) -> Arc<RegistrySnapshot> {
        self.store.load_full()
    }
}

// -- Trigger index builder --

fn build_trigger_index(
    pipelines: &HashMap<Uuid, Arc<CompiledPipeline>>,
) -> HashMap<(TopicKey, TriggerType), Vec<(Uuid, Uuid)>> {
    let mut index: HashMap<(TopicKey, TriggerType), Vec<(Uuid, Uuid)>> = HashMap::new();

    for pipeline in pipelines.values() {
        if !pipeline.enabled {
            continue;
        }
        for trigger in &pipeline.triggers {
            if !trigger.enabled {
                continue;
            }
            match &trigger.config {
                TriggerConfig::Event { .. } => {
                    let topics: Vec<TopicKey> = if let Some(src_id) = trigger.source_id {
                        vec![TopicKey::Source(src_id)]
                    } else if let Some(cam_id) = trigger.camera_id {
                        vec![TopicKey::Camera(cam_id)]
                    } else {
                        // Unscoped: index under every camera and source the pipeline touches.
                        let mut t = Vec::new();
                        for cam_ref in &pipeline.camera_refs {
                            t.push(TopicKey::Camera(cam_ref.camera_id));
                        }
                        for &src_id in &pipeline.source_refs {
                            t.push(TopicKey::Source(src_id));
                        }
                        t
                    };
                    for topic in topics {
                        index
                            .entry((topic, TriggerType::Event))
                            .or_default()
                            .push((pipeline.id, trigger.id));
                    }
                }
                TriggerConfig::System { .. } => {
                    index
                        .entry((TopicKey::System, TriggerType::System))
                        .or_default()
                        .push((pipeline.id, trigger.id));
                }
                // Schedule, Manual, Stat — not dispatched via the event bus.
                _ => {}
            }
        }
    }

    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reload_without_a_repo_returns_the_current_snapshot_unchanged() {
        let registry = PipelineRegistry::new_test(vec![]);
        let before = registry.snapshot();

        let previous = registry.reload().await.unwrap();

        assert!(Arc::ptr_eq(&previous, &before));
        assert!(Arc::ptr_eq(&registry.snapshot(), &before));
    }
}
