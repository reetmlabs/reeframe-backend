use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    PaginatorTrait, QueryFilter,
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

// -- Input types --

pub struct CreatePipeline {
    pub name: String,
    pub description: Option<String>,
    pub pipeline_type: PipelineType,
}

pub struct UpdatePipeline {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
}

pub struct CreateNode {
    pub node_type: CoreNodeType,
    pub action_config: Option<ActionConfig>,
    pub transport_config: Option<TransportConfig>,
    pub destination_id: Option<Uuid>,
    pub contact_list_id: Option<Uuid>,
    pub condition_expr: Option<String>,
    pub label: Option<String>,
    pub pos_x: Option<f64>,
    pub pos_y: Option<f64>,
}

/// `node_type` is intentionally absent — changing a node's type would also
/// invalidate whichever of `action_config`/`transport_config`/`condition_expr`
/// used to apply to it. Callers who need a different type delete and
/// recreate the node.
pub struct UpdateNode {
    pub action_config: Option<ActionConfig>,
    pub transport_config: Option<TransportConfig>,
    pub destination_id: Option<Option<Uuid>>,
    pub contact_list_id: Option<Option<Uuid>>,
    pub condition_expr: Option<String>,
    pub label: Option<Option<String>>,
    pub pos_x: Option<Option<f64>>,
    pub pos_y: Option<Option<f64>>,
}

// -- Repository --

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

    // -- Compiled loader --

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

    // -- Graph loaders --

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

    // -- Node CRUD --

    /// Create a node. Validates the node-level structural rules from the
    /// `PipelineDag` doc comment that don't require edges to check (rules
    /// 6 and 7: a `Condition` node needs a non-empty `condition_expr`, a
    /// `Transport` node needs a `destination_id`) plus root uniqueness
    /// (rules 1 and 2). The edge-dependent rules (3, 4, 5 — no cycles,
    /// `Transport`/`DeviceControl` must be leaves, `Condition` needs exactly
    /// two outgoing edges) can't be checked until step 9-6 adds edges.
    pub async fn create_node(
        &self,
        pipeline_id: Uuid,
        input: CreateNode,
    ) -> Result<PipelineNode, VmsError> {
        validate_create_shape(
            &input.node_type,
            &input.action_config,
            &input.transport_config,
            input.destination_id,
            &input.condition_expr,
        )?;

        if input.node_type == CoreNodeType::TriggerRoot {
            let existing_roots = pipeline_node::Entity::find()
                .filter(pipeline_node::Column::PipelineId.eq(pipeline_id))
                .filter(pipeline_node::Column::NodeType.eq(pipeline_node::NodeType::TriggerRoot))
                .count(&self.db)
                .await
                .map_err(db_err)?;
            if existing_roots > 0 {
                return Err(VmsError::DagValidation(
                    "pipeline already has a trigger_root node".into(),
                ));
            }
        }

        let config = config_json_for(
            &input.node_type,
            &input.action_config,
            &input.transport_config,
            &input.condition_expr,
        )?;
        let action_type = input.action_config.as_ref().map(action_type_from_config);

        let model = pipeline_node::ActiveModel {
            id: Set(Uuid::new_v4()),
            pipeline_id: Set(pipeline_id),
            node_type: Set(node_type_to_db(&input.node_type)),
            action_type: Set(action_type),
            destination_id: Set(input.destination_id),
            contact_list_id: Set(input.contact_list_id),
            config: Set(config),
            label: Set(input.label),
            pos_x: Set(input.pos_x),
            pos_y: Set(input.pos_y),
            created_at: Set(now()),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;

        node_from_db(model)
    }

    pub async fn get_node(&self, node_id: Uuid) -> Result<Option<PipelineNode>, VmsError> {
        let Some(m) = pipeline_node::Entity::find_by_id(node_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
        else {
            return Ok(None);
        };
        node_from_db(m).map(Some)
    }

    /// Partial update. Only `action_config`, `transport_config`, or
    /// `condition_expr` matching the node's existing (immutable) type may be
    /// provided — supplying the wrong one is rejected rather than silently
    /// ignored, same as `create_node`.
    pub async fn update_node(
        &self,
        node_id: Uuid,
        input: UpdateNode,
    ) -> Result<PipelineNode, VmsError> {
        let existing = pipeline_node::Entity::find_by_id(node_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::NodeNotFound(node_id))?;

        let node_type = node_type_from_db(&existing.node_type);
        validate_update_shape(
            &node_type,
            &input.action_config,
            &input.transport_config,
            &input.condition_expr,
        )?;

        let mut active: pipeline_node::ActiveModel = existing.into();

        match &node_type {
            CoreNodeType::Action | CoreNodeType::DeviceControl => {
                if let Some(ac) = &input.action_config {
                    active.config = Set(serde_json::to_value(ac)?);
                    active.action_type = Set(Some(action_type_from_config(ac)));
                }
            }
            CoreNodeType::Transport => {
                if let Some(tc) = &input.transport_config {
                    active.config = Set(serde_json::to_value(tc)?);
                }
            }
            CoreNodeType::Condition => {
                if let Some(expr) = &input.condition_expr {
                    active.config = Set(serde_json::json!({ "condition_expr": expr }));
                }
            }
            CoreNodeType::TriggerRoot | CoreNodeType::Fork => {}
        }

        if let Some(v) = input.destination_id {
            active.destination_id = Set(v);
        }
        if let Some(v) = input.contact_list_id {
            active.contact_list_id = Set(v);
        }
        if let Some(v) = input.label {
            active.label = Set(v);
        }
        if let Some(v) = input.pos_x {
            active.pos_x = Set(v);
        }
        if let Some(v) = input.pos_y {
            active.pos_y = Set(v);
        }

        let updated = active.update(&self.db).await.map_err(db_err)?;
        node_from_db(updated)
    }

    /// Delete a node. Cascades to any edges referencing it (the
    /// `pipeline_edges` foreign keys are `ON DELETE CASCADE`, set up in
    /// step 5's migration) — deleting a node mid-graph silently prunes its
    /// edges rather than leaving dangling references.
    pub async fn delete_node(&self, node_id: Uuid) -> Result<(), VmsError> {
        let node = pipeline_node::Entity::find_by_id(node_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::NodeNotFound(node_id))?;
        node.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }
}

// -- DB-to-domain translation --

fn node_type_from_db(db_type: &pipeline_node::NodeType) -> CoreNodeType {
    use pipeline_node::NodeType as Db;
    match db_type {
        Db::TriggerRoot => CoreNodeType::TriggerRoot,
        Db::Action => CoreNodeType::Action,
        Db::DeviceControl => CoreNodeType::DeviceControl,
        Db::Transport => CoreNodeType::Transport,
        Db::Fork => CoreNodeType::Fork,
        Db::Condition => CoreNodeType::Condition,
    }
}

fn node_type_to_db(core_type: &CoreNodeType) -> pipeline_node::NodeType {
    use pipeline_node::NodeType as Db;
    match core_type {
        CoreNodeType::TriggerRoot => Db::TriggerRoot,
        CoreNodeType::Action => Db::Action,
        CoreNodeType::DeviceControl => Db::DeviceControl,
        CoreNodeType::Transport => Db::Transport,
        CoreNodeType::Fork => Db::Fork,
        CoreNodeType::Condition => Db::Condition,
    }
}

fn action_type_from_config(cfg: &ActionConfig) -> pipeline_node::ActionType {
    use pipeline_node::ActionType as Db;
    match cfg {
        ActionConfig::Transcode(_) => Db::Transcode,
        ActionConfig::ExtractClip(_) => Db::ExtractClip,
        ActionConfig::Snapshot(_) => Db::Snapshot,
        ActionConfig::MergeClips(_) => Db::MergeClips,
        ActionConfig::Compress(_) => Db::Compress,
        ActionConfig::Encrypt(_) => Db::Encrypt,
        ActionConfig::Watermark(_) => Db::Watermark,
        ActionConfig::RenderNotification(_) => Db::RenderNotification,
        ActionConfig::Delay(_) => Db::Delay,
        ActionConfig::PtzMove(_) => Db::PtzMove,
        ActionConfig::StartRecording(_) => Db::StartRecording,
        ActionConfig::StopRecording(_) => Db::StopRecording,
        ActionConfig::SetStreamQuality(_) => Db::SetStreamQuality,
        ActionConfig::TriggerAlarmOutput(_) => Db::TriggerAlarmOutput,
    }
}

/// Serialize whichever of `action_config`/`transport_config`/`condition_expr`
/// applies to `node_type` into the single JSON blob `pipeline_nodes.config`
/// stores. Callers must run `validate_create_shape` first — this assumes the
/// combination is already known-valid and will panic via `expect` if not.
fn config_json_for(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    condition_expr: &Option<String>,
) -> Result<serde_json::Value, VmsError> {
    let value = match node_type {
        CoreNodeType::Action | CoreNodeType::DeviceControl => serde_json::to_value(
            action_config
                .as_ref()
                .expect("validated: action_config present"),
        )?,
        CoreNodeType::Transport => {
            serde_json::to_value(transport_config.clone().unwrap_or_default())?
        }
        CoreNodeType::Condition => serde_json::json!({
            "condition_expr": condition_expr.as_ref().expect("validated: condition_expr present"),
        }),
        CoreNodeType::TriggerRoot | CoreNodeType::Fork => serde_json::json!({}),
    };
    Ok(value)
}

/// Rules 6 and 7 from the `PipelineDag` doc comment, plus the requirement
/// that a node only carries the one config shape its type actually uses —
/// checked eagerly here instead of only failing much later when the
/// pipeline's DAG is compiled.
fn validate_create_shape(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    destination_id: Option<Uuid>,
    condition_expr: &Option<String>,
) -> Result<(), VmsError> {
    match node_type {
        CoreNodeType::Action | CoreNodeType::DeviceControl => {
            if action_config.is_none() {
                return Err(VmsError::DagValidation(format!(
                    "{node_type:?} node requires action_config"
                )));
            }
            if transport_config.is_some() || condition_expr.is_some() {
                return Err(VmsError::DagValidation(format!(
                    "{node_type:?} node must not set transport_config or condition_expr"
                )));
            }
        }
        CoreNodeType::Transport => {
            if destination_id.is_none() {
                return Err(VmsError::DagValidation(
                    "transport node requires destination_id".into(),
                ));
            }
            if action_config.is_some() || condition_expr.is_some() {
                return Err(VmsError::DagValidation(
                    "transport node must not set action_config or condition_expr".into(),
                ));
            }
        }
        CoreNodeType::Condition => {
            if !condition_expr
                .as_deref()
                .is_some_and(|e| !e.trim().is_empty())
            {
                return Err(VmsError::DagValidation(
                    "condition node requires a non-empty condition_expr".into(),
                ));
            }
            if action_config.is_some() || transport_config.is_some() {
                return Err(VmsError::DagValidation(
                    "condition node must not set action_config or transport_config".into(),
                ));
            }
        }
        CoreNodeType::TriggerRoot | CoreNodeType::Fork => {
            if action_config.is_some() || transport_config.is_some() || condition_expr.is_some() {
                return Err(VmsError::DagValidation(format!(
                    "{node_type:?} node must not set any config"
                )));
            }
        }
    }
    Ok(())
}

/// Same idea as `validate_create_shape`, but for a partial update: fields
/// left unset (`None`) are always fine — this only rejects a field that
/// *was* provided but doesn't belong to the node's (immutable) type.
fn validate_update_shape(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    condition_expr: &Option<String>,
) -> Result<(), VmsError> {
    if action_config.is_some()
        && !matches!(
            node_type,
            CoreNodeType::Action | CoreNodeType::DeviceControl
        )
    {
        return Err(VmsError::DagValidation(format!(
            "{node_type:?} node does not accept action_config"
        )));
    }
    if transport_config.is_some() && !matches!(node_type, CoreNodeType::Transport) {
        return Err(VmsError::DagValidation(format!(
            "{node_type:?} node does not accept transport_config"
        )));
    }
    if let Some(expr) = condition_expr {
        if !matches!(node_type, CoreNodeType::Condition) {
            return Err(VmsError::DagValidation(format!(
                "{node_type:?} node does not accept condition_expr"
            )));
        }
        if expr.trim().is_empty() {
            return Err(VmsError::DagValidation(
                "condition_expr must not be empty".into(),
            ));
        }
    }
    Ok(())
}

fn node_from_db(m: pipeline_node::Model) -> Result<PipelineNode, VmsError> {
    let node_type = node_type_from_db(&m.node_type);

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

// -- Tests --

#[cfg(test)]
mod tests {
    use vms_core::action::DelayConfig;

    use super::*;

    fn delay_config() -> ActionConfig {
        ActionConfig::Delay(DelayConfig { duration_secs: 5 })
    }

    #[test]
    fn action_node_requires_action_config() {
        let result = validate_create_shape(&CoreNodeType::Action, &None, &None, None, &None);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn action_node_rejects_transport_config() {
        let result = validate_create_shape(
            &CoreNodeType::Action,
            &Some(delay_config()),
            &Some(TransportConfig::default()),
            None,
            &None,
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn valid_action_node_passes() {
        let result = validate_create_shape(
            &CoreNodeType::Action,
            &Some(delay_config()),
            &None,
            None,
            &None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn transport_node_requires_destination_id() {
        let result = validate_create_shape(
            &CoreNodeType::Transport,
            &None,
            &Some(TransportConfig::default()),
            None,
            &None,
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn transport_node_with_destination_id_passes() {
        let result = validate_create_shape(
            &CoreNodeType::Transport,
            &None,
            &None,
            Some(Uuid::new_v4()),
            &None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn condition_node_requires_non_empty_expr() {
        let empty = validate_create_shape(
            &CoreNodeType::Condition,
            &None,
            &None,
            None,
            &Some("   ".into()),
        );
        assert!(matches!(empty, Err(VmsError::DagValidation(_))));

        let missing = validate_create_shape(&CoreNodeType::Condition, &None, &None, None, &None);
        assert!(matches!(missing, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn condition_node_with_expr_passes() {
        let result = validate_create_shape(
            &CoreNodeType::Condition,
            &None,
            &None,
            None,
            &Some("x > 1".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn trigger_root_and_fork_reject_any_config() {
        for node_type in [CoreNodeType::TriggerRoot, CoreNodeType::Fork] {
            let result =
                validate_create_shape(&node_type, &Some(delay_config()), &None, None, &None);
            assert!(matches!(result, Err(VmsError::DagValidation(_))));
        }
    }

    #[test]
    fn update_rejects_config_for_the_wrong_node_type() {
        let result = validate_update_shape(
            &CoreNodeType::Transport,
            &Some(delay_config()),
            &None,
            &None,
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn update_allows_leaving_every_field_unset() {
        let result = validate_update_shape(&CoreNodeType::Action, &None, &None, &None);
        assert!(result.is_ok());
    }

    #[test]
    fn update_rejects_blanking_condition_expr_to_whitespace() {
        let result =
            validate_update_shape(&CoreNodeType::Condition, &None, &None, &Some("  ".into()));
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn action_type_round_trips_through_node_type_conversions() {
        assert_eq!(
            action_type_from_config(&delay_config()),
            pipeline_node::ActionType::Delay
        );
        assert_eq!(
            node_type_from_db(&node_type_to_db(&CoreNodeType::Condition)),
            CoreNodeType::Condition
        );
    }

    #[test]
    fn config_json_for_trigger_root_is_empty_object() {
        let value = config_json_for(&CoreNodeType::TriggerRoot, &None, &None, &None).unwrap();
        assert_eq!(value, serde_json::json!({}));
    }

    #[test]
    fn config_json_for_condition_wraps_expr() {
        let value = config_json_for(
            &CoreNodeType::Condition,
            &None,
            &None,
            &Some("x > 1".into()),
        )
        .unwrap();
        assert_eq!(value, serde_json::json!({ "condition_expr": "x > 1" }));
    }
}
