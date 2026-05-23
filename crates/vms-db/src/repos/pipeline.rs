use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter,
};
use uuid::Uuid;
use vms_core::{
    action::{ActionConfig, TransportConfig},
    pipeline::{
        CompiledPipeline, PipelineCameraRef, PipelineDag, PipelineEdge, PipelineNode,
        PipelineTrigger,
    },
    pipeline::{EdgeType as CoreEdgeType, NodeType as CoreNodeType},
    trigger::{TriggerConfig, TriggerType as CoreTriggerType},
    VmsError,
};

use crate::entities::{
    pipeline_camera_ref, pipeline_edge, pipeline_node, pipeline_source_ref, pipeline_trigger,
};

use super::{db_err, now};
use crate::entities::pipeline::{self, ActiveModel, PipelineType};

// -- Input types ---------------------------------------------------------------

pub struct CreatePipeline {
    pub name: String,
    pub description: Option<String>,
    pub pipeline_type: PipelineType,
}

pub struct UpdatePipeline {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
}

// -- Repository ----------------------------------------------------------------

#[derive(Clone)]
pub struct PipelineRepo {
    db: DatabaseConnection,
}

impl PipelineRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreatePipeline) -> Result<pipeline::Model, VmsError> {
        let ts = now();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            description: Set(input.description),
            pipeline_type: Set(input.pipeline_type),
            enabled: Set(false),
            created_by: Set(None),
            created_at: Set(ts),
            updated_at: Set(ts),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<pipeline::Model>, VmsError> {
        pipeline::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn list_all(&self) -> Result<Vec<pipeline::Model>, VmsError> {
        pipeline::Entity::find().all(&self.db).await.map_err(db_err)
    }

    pub async fn list_enabled(&self) -> Result<Vec<pipeline::Model>, VmsError> {
        pipeline::Entity::find()
            .filter(pipeline::Column::Enabled.eq(true))
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn update(&self, id: Uuid, input: UpdatePipeline) -> Result<(), VmsError> {
        let mut model = ActiveModel {
            id: Set(id),
            updated_at: Set(now()),
            ..Default::default()
        };
        if let Some(name) = input.name {
            model.name = Set(name);
        }
        if let Some(description) = input.description {
            model.description = Set(description);
        }
        model.update(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// Enable or disable a pipeline. Callers should follow this with
    /// `PipelineRegistry::reload()` to reconcile resource ref-counts.
    pub async fn set_enabled(&self, id: Uuid, enabled: bool) -> Result<(), VmsError> {
        ActiveModel {
            id: Set(id),
            enabled: Set(enabled),
            updated_at: Set(now()),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        pipeline::Entity::delete_by_id(id)
            .exec(&self.db)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // -- Compiled loader -------------------------------------------------------

    /// Load and compile a single pipeline by id.
    ///
    /// Fetches the pipeline header, nodes, edges, and triggers in four queries,
    /// then calls `compile_pipeline`. Returns `None` if the pipeline row is gone
    /// (race between `list_enabled` and this call).
    pub async fn load_compiled(&self, id: Uuid) -> Result<Option<CompiledPipeline>, VmsError> {
        let Some(p) = self.get(id).await? else {
            return Ok(None);
        };
        let nodes = self.load_nodes(id).await?;
        let edges = self.load_edges(id).await?;
        let triggers = self.load_triggers(id).await?;
        let camera_refs = self.load_camera_refs(id).await?;
        let source_refs = self.load_source_refs(id).await?;
        compile_pipeline(&p, nodes, edges, triggers, camera_refs, source_refs).map(Some)
    }

    pub async fn load_camera_refs(
        &self,
        pipeline_id: Uuid,
    ) -> Result<Vec<PipelineCameraRef>, VmsError> {
        let rows = pipeline_camera_ref::Entity::find()
            .filter(pipeline_camera_ref::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        Ok(rows
            .into_iter()
            .map(|r| PipelineCameraRef {
                camera_id: r.camera_id,
                needs_ring_buffer: r.needs_ring_buffer,
                needs_analytics: r.needs_analytics,
            })
            .collect())
    }

    pub async fn load_source_refs(&self, pipeline_id: Uuid) -> Result<Vec<Uuid>, VmsError> {
        let rows = pipeline_source_ref::Entity::find()
            .filter(pipeline_source_ref::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        Ok(rows.into_iter().map(|r| r.source_id).collect())
    }

    // -- Graph loaders ---------------------------------------------------------

    pub async fn load_nodes(&self, pipeline_id: Uuid) -> Result<Vec<PipelineNode>, VmsError> {
        let rows = pipeline_node::Entity::find()
            .filter(pipeline_node::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        rows.into_iter().map(node_from_db).collect()
    }

    pub async fn load_edges(&self, pipeline_id: Uuid) -> Result<Vec<PipelineEdge>, VmsError> {
        let rows = pipeline_edge::Entity::find()
            .filter(pipeline_edge::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        Ok(rows.into_iter().map(edge_from_db).collect())
    }

    pub async fn load_triggers(&self, pipeline_id: Uuid) -> Result<Vec<PipelineTrigger>, VmsError> {
        let rows = pipeline_trigger::Entity::find()
            .filter(pipeline_trigger::Column::PipelineId.eq(pipeline_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        rows.into_iter().map(trigger_from_db).collect()
    }
}

// -- DB-to-domain translation --------------------------------------------------

fn node_from_db(m: pipeline_node::Model) -> Result<PipelineNode, VmsError> {
    use pipeline_node::NodeType as Db;

    let node_type = match m.node_type {
        Db::TriggerRoot => CoreNodeType::TriggerRoot,
        Db::Action => CoreNodeType::Action,
        Db::DeviceControl => CoreNodeType::DeviceControl,
        Db::Transport => CoreNodeType::Transport,
        Db::Fork => CoreNodeType::Fork,
        Db::Condition => CoreNodeType::Condition,
    };

    // The config JSON column stores different payloads depending on node_type.
    let (action_config, transport_config, condition_expr) = match node_type {
        CoreNodeType::Action | CoreNodeType::DeviceControl => {
            let ac = serde_json::from_value::<ActionConfig>(m.config).map_err(|e| {
                VmsError::Serialization(format!("node {}: action_config: {e}", m.id))
            })?;
            (Some(ac), None, None)
        }
        CoreNodeType::Transport => {
            let tc = serde_json::from_value::<TransportConfig>(m.config).map_err(|e| {
                VmsError::Serialization(format!("node {}: transport_config: {e}", m.id))
            })?;
            (None, Some(tc), None)
        }
        CoreNodeType::Condition => {
            let expr = m
                .config
                .get("condition_expr")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            (None, None, expr)
        }
        _ => (None, None, None),
    };

    Ok(PipelineNode {
        id: m.id,
        pipeline_id: m.pipeline_id,
        node_type,
        action_config,
        destination_id: m.destination_id,
        contact_list_id: m.contact_list_id,
        transport_config,
        condition_expr,
        label: m.label,
        pos_x: m.pos_x,
        pos_y: m.pos_y,
    })
}

fn edge_from_db(m: pipeline_edge::Model) -> PipelineEdge {
    use pipeline_edge::EdgeType as Db;

    let edge_type = match m.edge_type {
        Db::Default => CoreEdgeType::Default,
        Db::TrueBranch => CoreEdgeType::TrueBranch,
        Db::FalseBranch => CoreEdgeType::FalseBranch,
    };

    PipelineEdge {
        id: m.id,
        pipeline_id: m.pipeline_id,
        from_node_id: m.from_node_id,
        to_node_id: m.to_node_id,
        edge_type,
    }
}

fn trigger_from_db(m: pipeline_trigger::Model) -> Result<PipelineTrigger, VmsError> {
    use pipeline_trigger::TriggerType as Db;

    let trigger_type = match m.trigger_type {
        Db::Schedule => CoreTriggerType::Schedule,
        Db::Event => CoreTriggerType::Event,
        Db::System => CoreTriggerType::System,
        Db::Manual => CoreTriggerType::Manual,
        Db::Stat => CoreTriggerType::Stat,
    };

    let config = serde_json::from_value::<TriggerConfig>(m.config)
        .map_err(|e| VmsError::Serialization(format!("trigger {}: config: {e}", m.id)))?;

    Ok(PipelineTrigger {
        id: m.id,
        pipeline_id: m.pipeline_id,
        trigger_type,
        source_id: m.source_id,
        camera_id: m.camera_id,
        config,
        enabled: m.enabled,
    })
}

pub(crate) fn compile_pipeline(
    p: &pipeline::Model,
    nodes: Vec<PipelineNode>,
    edges: Vec<PipelineEdge>,
    triggers: Vec<PipelineTrigger>,
    camera_refs: Vec<PipelineCameraRef>,
    source_refs: Vec<Uuid>,
) -> Result<CompiledPipeline, VmsError> {
    let dag = PipelineDag::compile(nodes, edges)?;
    Ok(CompiledPipeline {
        id: p.id,
        name: p.name.clone(),
        enabled: p.enabled,
        dag,
        triggers,
        camera_refs,
        source_refs,
    })
}
