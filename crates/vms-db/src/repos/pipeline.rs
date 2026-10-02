use std::collections::{HashMap, HashSet};

use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    PaginatorTrait, QueryFilter, TransactionTrait,
};
use uuid::Uuid;
use vms_core::{
    action::{ActionConfig, TransportConfig},
    pipeline::{
        check_no_cycle, CompiledPipeline, PipelineCameraRef, PipelineDag, PipelineEdge,
        PipelineNode, PipelineTrigger,
    },
    pipeline::{EdgeType as CoreEdgeType, NodeType as CoreNodeType},
    trigger::{TriggerConfig, TriggerType as CoreTriggerType},
    VmsError,
};

use crate::entities::{
    camera, contact_list, destination, pipeline_camera_ref, pipeline_edge, pipeline_node,
    pipeline_source_ref, pipeline_trigger, source,
};

use super::pipeline_validation::{
    check_artifact_lineage, check_config_completeness, check_config_shape,
    check_dangling_camera_references, check_disconnected, check_structural_violations,
    check_unresolved_node_references, check_unresolved_trigger_references, ValidationIssue,
    ValidationSeverity,
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

/// Result of `PipelineRepo::set_enabled`.
pub enum EnableOutcome {
    /// The `enabled` flag was written as requested.
    Applied,
    /// Enabling was refused; these are the blocking errors.
    BlockedByErrors(Vec<ValidationIssue>),
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

/// `node_type` is intentionally absent: changing a node's type would also
/// invalidate whichever of `action_config`/`transport_config`/`condition_expr`
/// applied to it. To change the type, delete and recreate the node.
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

pub struct CreateEdge {
    pub from_node_id: Uuid,
    pub to_node_id: Uuid,
    pub edge_type: CoreEdgeType,
}

/// `from_node_id`/`to_node_id` are intentionally absent: moving an edge's
/// endpoints makes it a different edge. Delete and recreate it instead, as
/// with `UpdateNode`'s `node_type`.
pub struct UpdateEdge {
    pub edge_type: CoreEdgeType,
}

pub struct CreateTrigger {
    pub config: TriggerConfig,
    pub source_id: Option<Uuid>,
    pub camera_id: Option<Uuid>,
    pub enabled: bool,
}

/// `config`, if supplied, must be the same `TriggerConfig` variant the trigger
/// already has. Changing trigger type means delete and recreate, as with
/// `UpdateNode`'s immutable `node_type`.
pub struct UpdateTrigger {
    pub config: Option<TriggerConfig>,
    pub source_id: Option<Option<Uuid>>,
    pub camera_id: Option<Option<Uuid>>,
    pub enabled: Option<bool>,
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
            validation_issues: Set(serde_json::json!([])),
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

    /// Disable always succeeds unconditionally. Enable revalidates first and
    /// refuses (leaving `enabled` untouched) if any error-severity issue is
    /// found.
    pub async fn set_enabled(&self, id: Uuid, enabled: bool) -> Result<EnableOutcome, VmsError> {
        if enabled {
            let issues = self.validate_pipeline(id).await?;
            self.persist_validation_issues(id, &issues).await?;

            let errors: Vec<ValidationIssue> = issues
                .into_iter()
                .filter(|i| i.severity == ValidationSeverity::Error)
                .collect();
            if !errors.is_empty() {
                return Ok(EnableOutcome::BlockedByErrors(errors));
            }
        }

        ActiveModel {
            id: Set(id),
            enabled: Set(enabled),
            updated_at: Set(now()),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;

        Ok(EnableOutcome::Applied)
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
                ring_buffer_secs: r.ring_buffer_secs.max(0) as u32,
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

    /// Create a node. Rejects a config shape the node type doesn't use and a second
    /// `trigger_root` in the same pipeline, and checks that a referenced
    /// destination and contact list exist. Missing values (an empty
    /// `condition_expr`, no `destination_id`) are allowed while the pipeline is
    /// being edited and show up as validation issues instead. The edge-dependent
    /// rules (no cycles, `Transport`/`DeviceControl` as leaves, `Condition` with
    /// both branches) can't be checked until edges exist.
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
        let mut unresolved_reference = false;
        if let Some(dest_id) = input.destination_id {
            let dest = self.require_destination_exists(dest_id).await?;
            unresolved_reference = !dest.enabled;
        }
        if let Some(cl_id) = input.contact_list_id {
            self.require_contact_list_exists(cl_id).await?;
        }

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
            unresolved_reference: Set(unresolved_reference),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;

        let created = node_from_db(model)?;
        self.recompute_refs(pipeline_id).await?;
        Ok(created)
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

    /// Partial update. Only the `action_config`, `transport_config`, or
    /// `condition_expr` matching the node's existing (immutable) type may be
    /// provided; supplying the wrong one is rejected instead of ignored, same as
    /// `create_node`.
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

        let pipeline_id = existing.pipeline_id;
        let node_type = node_type_from_db(&existing.node_type);
        validate_update_shape(
            &node_type,
            &input.action_config,
            &input.transport_config,
            &input.destination_id,
            &input.condition_expr,
        )?;
        // Only recompute unresolved_reference when destination_id is
        // actually being changed by this call, same rationale as
        // update_trigger's source_id handling.
        let mut new_unresolved_reference = None;
        match input.destination_id {
            Some(Some(dest_id)) => {
                let dest = self.require_destination_exists(dest_id).await?;
                new_unresolved_reference = Some(!dest.enabled);
            }
            Some(None) => new_unresolved_reference = Some(false),
            None => {}
        }
        if let Some(Some(cl_id)) = input.contact_list_id {
            self.require_contact_list_exists(cl_id).await?;
        }

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
        if let Some(v) = new_unresolved_reference {
            active.unresolved_reference = Set(v);
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
        let updated = node_from_db(updated)?;
        self.recompute_refs(pipeline_id).await?;
        Ok(updated)
    }

    /// Delete a node. Its edges go with it (the `pipeline_edges` foreign keys are
    /// `ON DELETE CASCADE`), so deleting a node mid-graph prunes its edges instead
    /// of leaving dangling references.
    pub async fn delete_node(&self, node_id: Uuid) -> Result<(), VmsError> {
        let node = pipeline_node::Entity::find_by_id(node_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::NodeNotFound(node_id))?;
        let pipeline_id = node.pipeline_id;
        node.delete(&self.db).await.map_err(db_err)?;
        self.recompute_refs(pipeline_id).await
    }

    // -- Edge CRUD --

    /// Create an edge. Validates everything from the `PipelineDag` doc comment
    /// that can be checked before the graph is complete: rule 3 (no cycle, via
    /// `check_no_cycle` on the graph *with* this edge added), rule 4
    /// (`Transport`/`DeviceControl` nodes can never have an outgoing edge), and
    /// the per-edge part of rule 5 (a `Condition` source only takes
    /// `true_branch`/`false_branch`, each at most once, and at most two outgoing
    /// edges in total). Whether a `Condition` node ends up with both branches,
    /// and whether every node is reachable from the root, are only checked at
    /// compile time, since the graph may still be half wired.
    pub async fn create_edge(
        &self,
        pipeline_id: Uuid,
        input: CreateEdge,
    ) -> Result<PipelineEdge, VmsError> {
        if input.from_node_id == input.to_node_id {
            return Err(VmsError::DagValidation(
                "an edge cannot connect a node to itself".into(),
            ));
        }

        let from_node = self
            .require_node_in_pipeline(pipeline_id, input.from_node_id)
            .await?;
        self.require_node_in_pipeline(pipeline_id, input.to_node_id)
            .await?;

        let nodes = self.load_nodes(pipeline_id).await?;
        let mut edges = self.load_edges(pipeline_id).await?;

        validate_new_edge(&from_node, &input.edge_type, input.to_node_id, &edges)?;

        edges.push(PipelineEdge {
            id: Uuid::new_v4(), // placeholder id, only used for the cycle check below
            pipeline_id,
            from_node_id: input.from_node_id,
            to_node_id: input.to_node_id,
            edge_type: input.edge_type.clone(),
        });
        check_no_cycle(&nodes, &edges)?;

        let model = pipeline_edge::ActiveModel {
            id: Set(Uuid::new_v4()),
            pipeline_id: Set(pipeline_id),
            from_node_id: Set(input.from_node_id),
            to_node_id: Set(input.to_node_id),
            edge_type: Set(edge_type_to_db(&input.edge_type)),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;

        self.revalidate(pipeline_id).await?;
        Ok(edge_from_db(model))
    }

    pub async fn get_edge(&self, edge_id: Uuid) -> Result<Option<PipelineEdge>, VmsError> {
        let m = pipeline_edge::Entity::find_by_id(edge_id)
            .one(&self.db)
            .await
            .map_err(db_err)?;
        Ok(m.map(edge_from_db))
    }

    /// Update an edge's `edge_type`. Endpoints are immutable (see
    /// [`UpdateEdge`]), so the cycle and duplicate-pair checks from `create_edge`
    /// don't apply; only the source-node/edge-type compatibility and
    /// branch-uniqueness checks do.
    pub async fn update_edge(
        &self,
        edge_id: Uuid,
        input: UpdateEdge,
    ) -> Result<PipelineEdge, VmsError> {
        let existing = pipeline_edge::Entity::find_by_id(edge_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::EdgeNotFound(edge_id))?;

        let from_node = self
            .require_node_in_pipeline(existing.pipeline_id, existing.from_node_id)
            .await?;
        let other_edges: Vec<PipelineEdge> = self
            .load_edges(existing.pipeline_id)
            .await?
            .into_iter()
            .filter(|e| e.id != edge_id)
            .collect();

        validate_new_edge(
            &from_node,
            &input.edge_type,
            existing.to_node_id,
            &other_edges,
        )?;

        let pipeline_id = existing.pipeline_id;
        let mut active: pipeline_edge::ActiveModel = existing.into();
        active.edge_type = Set(edge_type_to_db(&input.edge_type));
        let updated = active.update(&self.db).await.map_err(db_err)?;
        self.revalidate(pipeline_id).await?;
        Ok(edge_from_db(updated))
    }

    pub async fn delete_edge(&self, edge_id: Uuid) -> Result<(), VmsError> {
        let edge = pipeline_edge::Entity::find_by_id(edge_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::EdgeNotFound(edge_id))?;
        let pipeline_id = edge.pipeline_id;
        edge.delete(&self.db).await.map_err(db_err)?;
        self.revalidate(pipeline_id).await?;
        Ok(())
    }

    async fn require_node_in_pipeline(
        &self,
        pipeline_id: Uuid,
        node_id: Uuid,
    ) -> Result<PipelineNode, VmsError> {
        self.get_node(node_id)
            .await?
            .filter(|n| n.pipeline_id == pipeline_id)
            .ok_or(VmsError::NodeNotFound(node_id))
    }

    // -- Cross-entity FK existence checks --
    //
    // `pipeline_nodes.destination_id`/`contact_list_id` and
    // `pipeline_triggers.source_id`/`camera_id` are all foreign keys (`ON DELETE
    // RESTRICT`, except `contact_list_id`, which is `SET NULL`), so an unknown id
    // is never accepted. Without a check here, though, it surfaces as a raw
    // "FOREIGN KEY constraint failed" `VmsError::Database` (a `500`) instead of
    // the `404`-mapped not-found error the other cross-entity references in this
    // file return.

    /// Returns the destination row so callers can also check `enabled`. A node
    /// created or repointed against a disabled destination is allowed, but is
    /// immediately marked `unresolved_reference`.
    async fn require_destination_exists(
        &self,
        destination_id: Uuid,
    ) -> Result<destination::Model, VmsError> {
        destination::Entity::find_by_id(destination_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::DestinationNotFound(destination_id))
    }

    async fn require_contact_list_exists(&self, contact_list_id: Uuid) -> Result<(), VmsError> {
        contact_list::Entity::find_by_id(contact_list_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .map(|_| ())
            .ok_or(VmsError::ContactListNotFound(contact_list_id))
    }

    /// Returns the source row so callers can also check `enabled`. A trigger
    /// created or repointed against a disabled source is allowed, but is
    /// immediately marked `unresolved_reference`.
    async fn require_source_exists(&self, source_id: Uuid) -> Result<source::Model, VmsError> {
        source::Entity::find_by_id(source_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::SourceNotFound(source_id))
    }

    async fn require_camera_exists(&self, camera_id: Uuid) -> Result<(), VmsError> {
        camera::Entity::find_by_id(camera_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .map(|_| ())
            .ok_or(VmsError::CameraNotFound(camera_id))
    }

    // -- Resource Manager ref-table derivation --

    /// Recompute `pipeline_camera_refs`/`pipeline_source_refs` for `pipeline_id`
    /// from its current nodes and triggers, replacing the stored rows in one
    /// transaction. Called after every node/trigger write. Recomputing the few
    /// rows one pipeline has is simpler than diffing, and this only runs on
    /// graph edits, never during pipeline execution.
    pub async fn recompute_refs(&self, pipeline_id: Uuid) -> Result<(), VmsError> {
        let nodes = self.load_nodes(pipeline_id).await?;
        let triggers = self.load_triggers(pipeline_id).await?;

        let camera_refs = derive_camera_refs(&nodes, &triggers);
        let source_refs: HashSet<Uuid> = triggers
            .iter()
            .filter(|t| t.enabled)
            .filter_map(|t| t.source_id)
            .collect();

        let txn = self.db.begin().await.map_err(db_err)?;

        pipeline_camera_ref::Entity::delete_many()
            .filter(pipeline_camera_ref::Column::PipelineId.eq(pipeline_id))
            .exec(&txn)
            .await
            .map_err(db_err)?;
        for cam_ref in &camera_refs {
            pipeline_camera_ref::ActiveModel {
                pipeline_id: Set(pipeline_id),
                camera_id: Set(cam_ref.camera_id),
                needs_ring_buffer: Set(cam_ref.needs_ring_buffer),
                needs_analytics: Set(cam_ref.needs_analytics),
                ring_buffer_secs: Set(cam_ref.ring_buffer_secs as i32),
            }
            .insert(&txn)
            .await
            .map_err(db_err)?;
        }

        pipeline_source_ref::Entity::delete_many()
            .filter(pipeline_source_ref::Column::PipelineId.eq(pipeline_id))
            .exec(&txn)
            .await
            .map_err(db_err)?;
        for &source_id in &source_refs {
            pipeline_source_ref::ActiveModel {
                pipeline_id: Set(pipeline_id),
                source_id: Set(source_id),
            }
            .insert(&txn)
            .await
            .map_err(db_err)?;
        }

        txn.commit().await.map_err(db_err)?;
        self.revalidate(pipeline_id).await?;
        Ok(())
    }

    // -- Trigger CRUD --

    /// Create a trigger. `config`'s own serde tag determines the stored
    /// `trigger_type`; `source_id`/`camera_id` are validated against it by
    /// `validate_trigger_shape`, and the stored `camera_id` is re-derived
    /// from `config` for `System` triggers by `effective_camera_id`.
    pub async fn create_trigger(
        &self,
        pipeline_id: Uuid,
        input: CreateTrigger,
    ) -> Result<PipelineTrigger, VmsError> {
        validate_trigger_shape(&input.config, input.source_id, input.camera_id)?;
        let mut unresolved_reference = false;
        if let Some(src_id) = input.source_id {
            let source = self.require_source_exists(src_id).await?;
            unresolved_reference = !source.enabled;
        }
        let camera_id = effective_camera_id(&input.config, input.camera_id);
        if let Some(cam_id) = camera_id {
            self.require_camera_exists(cam_id).await?;
        }
        let trigger_type = trigger_type_from_config(&input.config);

        let model = pipeline_trigger::ActiveModel {
            id: Set(Uuid::new_v4()),
            pipeline_id: Set(pipeline_id),
            trigger_type: Set(trigger_type_to_db(&trigger_type)),
            source_id: Set(input.source_id),
            camera_id: Set(camera_id),
            config: Set(serde_json::to_value(&input.config)?),
            enabled: Set(input.enabled),
            created_at: Set(now()),
            last_error: Set(None),
            last_error_at: Set(None),
            unresolved_reference: Set(unresolved_reference),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;

        let created = trigger_from_db(model)?;
        self.recompute_refs(pipeline_id).await?;
        Ok(created)
    }

    pub async fn get_trigger(&self, trigger_id: Uuid) -> Result<Option<PipelineTrigger>, VmsError> {
        let Some(m) = pipeline_trigger::Entity::find_by_id(trigger_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
        else {
            return Ok(None);
        };
        trigger_from_db(m).map(Some)
    }

    /// Record (or clear, with `None`) a trigger's most recent filter-evaluation
    /// failure, so a bad `evalexpr` filter shows up in the trigger's API
    /// representation instead of only in a `tracing::warn!` line. A no-op if the
    /// trigger has since been deleted; the evaluator stops reporting it on its
    /// next tick.
    pub async fn set_trigger_error(
        &self,
        trigger_id: Uuid,
        error: Option<String>,
    ) -> Result<(), VmsError> {
        let Some(existing) = pipeline_trigger::Entity::find_by_id(trigger_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
        else {
            return Ok(());
        };

        let mut active: pipeline_trigger::ActiveModel = existing.into();
        active.last_error_at = Set(error.is_some().then(now));
        active.last_error = Set(error);
        active.update(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// Partial update. `config` cannot change trigger type (see
    /// [`UpdateTrigger`]). Changed fields are re-validated together with the
    /// unchanged ones: changing only `camera_id` on an existing `Event` trigger is
    /// still checked against that trigger's existing `source_id`.
    pub async fn update_trigger(
        &self,
        trigger_id: Uuid,
        input: UpdateTrigger,
    ) -> Result<PipelineTrigger, VmsError> {
        let existing = pipeline_trigger::Entity::find_by_id(trigger_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::TriggerNotFound(trigger_id))?;

        let pipeline_id = existing.pipeline_id;
        let existing_type = trigger_type_from_db(&existing.trigger_type);
        let existing_config = serde_json::from_value::<TriggerConfig>(existing.config.clone())
            .map_err(|e| VmsError::Serialization(format!("trigger {trigger_id}: config: {e}")))?;

        let effective_config = input.config.clone().unwrap_or(existing_config);
        if trigger_type_from_config(&effective_config) != existing_type {
            return Err(VmsError::DagValidation(format!(
                "cannot change trigger type from {} to {} — delete and recreate instead",
                existing_type.as_str(),
                trigger_type_from_config(&effective_config).as_str()
            )));
        }

        let effective_source_id = input.source_id.unwrap_or(existing.source_id);
        let effective_camera_id_input = input.camera_id.unwrap_or(existing.camera_id);
        validate_trigger_shape(
            &effective_config,
            effective_source_id,
            effective_camera_id_input,
        )?;
        // Only recompute unresolved_reference when this call changes source_id.
        // An update that leaves it alone shouldn't clear or reassert a status
        // set by something else.
        let mut new_unresolved_reference = None;
        if let Some(src_id) = effective_source_id {
            let source = self.require_source_exists(src_id).await?;
            if input.source_id.is_some() {
                new_unresolved_reference = Some(!source.enabled);
            }
        }
        let derived_camera_id = effective_camera_id(&effective_config, effective_camera_id_input);
        if let Some(cam_id) = derived_camera_id {
            self.require_camera_exists(cam_id).await?;
        }

        let mut active: pipeline_trigger::ActiveModel = existing.into();
        if let Some(cfg) = &input.config {
            active.config = Set(serde_json::to_value(cfg)?);
        }
        if let Some(v) = input.source_id {
            active.source_id = Set(v);
        }
        if let Some(v) = new_unresolved_reference {
            active.unresolved_reference = Set(v);
        }
        active.camera_id = Set(derived_camera_id);
        if let Some(v) = input.enabled {
            active.enabled = Set(v);
        }

        let updated = active.update(&self.db).await.map_err(db_err)?;
        let updated = trigger_from_db(updated)?;
        self.recompute_refs(pipeline_id).await?;
        Ok(updated)
    }

    pub async fn delete_trigger(&self, trigger_id: Uuid) -> Result<(), VmsError> {
        let trigger = pipeline_trigger::Entity::find_by_id(trigger_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::TriggerNotFound(trigger_id))?;
        let pipeline_id = trigger.pipeline_id;
        trigger.delete(&self.db).await.map_err(db_err)?;
        self.recompute_refs(pipeline_id).await
    }

    /// Create or update the pipeline's one trigger to match `input`. By convention
    /// a `pipeline_triggers` row represents its pipeline's single `trigger_root`
    /// node, with no stored FK, since the DAG compiler already enforces exactly
    /// one `trigger_root` node per pipeline.
    pub async fn upsert_node_trigger(
        &self,
        pipeline_id: Uuid,
        input: CreateTrigger,
    ) -> Result<PipelineTrigger, VmsError> {
        // No unique constraint limits a pipeline to one trigger row, so if
        // several exist, update the first and leave the rest alone instead of
        // erroring.
        let mut existing = self.load_triggers(pipeline_id).await?;
        let Some(current) = existing.drain(..).next() else {
            return self.create_trigger(pipeline_id, input).await;
        };

        // update_trigger can't change a trigger's variant (by design, see its doc
        // comment), so switching trigger_type means delete and recreate.
        if trigger_type_from_config(&input.config) != current.trigger_type {
            self.delete_trigger(current.id).await?;
            return self.create_trigger(pipeline_id, input).await;
        }

        self.update_trigger(
            current.id,
            UpdateTrigger {
                config: Some(input.config),
                source_id: Some(input.source_id),
                camera_id: Some(input.camera_id),
                enabled: Some(input.enabled),
            },
        )
        .await
    }

    /// Deletes every trigger row for `pipeline_id`. Called when its `trigger_root`
    /// node is deleted, so no enabled trigger outlives the node representing it.
    pub async fn delete_triggers_for_pipeline(&self, pipeline_id: Uuid) -> Result<(), VmsError> {
        for t in self.load_triggers(pipeline_id).await? {
            self.delete_trigger(t.id).await?;
        }
        Ok(())
    }

    /// Called before deleting a source: clears `source_id` and marks
    /// `unresolved_reference` on every trigger that pointed at it, so the triggers
    /// are kept and `source_id`'s `ON DELETE RESTRICT` doesn't block the delete.
    /// Returns the affected triggers.
    pub async fn unlink_deleted_source(
        &self,
        source_id: Uuid,
    ) -> Result<Vec<PipelineTrigger>, VmsError> {
        let triggers = pipeline_trigger::Entity::find()
            .filter(pipeline_trigger::Column::SourceId.eq(source_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let mut updated = Vec::with_capacity(triggers.len());
        for t in triggers {
            let mut active: pipeline_trigger::ActiveModel = t.into();
            active.source_id = Set(None);
            active.unresolved_reference = Set(true);
            let saved = active.update(&self.db).await.map_err(db_err)?;
            updated.push(trigger_from_db(saved)?);
        }
        Ok(updated)
    }

    /// Marks every trigger pointing at `source_id` as having an unresolved
    /// reference, leaving `source_id` in place since the source still exists, just
    /// disabled. Returns the triggers that changed.
    pub async fn mark_source_disabled(
        &self,
        source_id: Uuid,
    ) -> Result<Vec<PipelineTrigger>, VmsError> {
        self.set_source_unresolved(source_id, true).await
    }

    /// Clears the unresolved-reference status on every trigger pointing at
    /// `source_id`. Called when the source is re-enabled. Returns the triggers that
    /// changed.
    pub async fn clear_source_unresolved(
        &self,
        source_id: Uuid,
    ) -> Result<Vec<PipelineTrigger>, VmsError> {
        self.set_source_unresolved(source_id, false).await
    }

    async fn set_source_unresolved(
        &self,
        source_id: Uuid,
        unresolved: bool,
    ) -> Result<Vec<PipelineTrigger>, VmsError> {
        let triggers = pipeline_trigger::Entity::find()
            .filter(pipeline_trigger::Column::SourceId.eq(source_id))
            .filter(pipeline_trigger::Column::UnresolvedReference.ne(unresolved))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let mut updated = Vec::with_capacity(triggers.len());
        for t in triggers {
            let mut active: pipeline_trigger::ActiveModel = t.into();
            active.unresolved_reference = Set(unresolved);
            let saved = active.update(&self.db).await.map_err(db_err)?;
            updated.push(trigger_from_db(saved)?);
        }
        Ok(updated)
    }

    /// Called before deleting a destination: clears `destination_id` and
    /// marks `unresolved_reference` on every node that pointed at it,
    /// scoped to the individual node rather than the whole pipeline.
    /// Returns the affected nodes.
    pub async fn unlink_deleted_destination(
        &self,
        destination_id: Uuid,
    ) -> Result<Vec<PipelineNode>, VmsError> {
        let nodes = pipeline_node::Entity::find()
            .filter(pipeline_node::Column::DestinationId.eq(destination_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let mut updated = Vec::with_capacity(nodes.len());
        for n in nodes {
            let mut active: pipeline_node::ActiveModel = n.into();
            active.destination_id = Set(None);
            active.unresolved_reference = Set(true);
            let saved = active.update(&self.db).await.map_err(db_err)?;
            updated.push(node_from_db(saved)?);
        }
        Ok(updated)
    }

    /// Marks every node pointing at `destination_id` as having an unresolved
    /// reference, leaving `destination_id` in place since the destination still
    /// exists, just disabled. Returns the nodes that changed.
    pub async fn mark_destination_disabled(
        &self,
        destination_id: Uuid,
    ) -> Result<Vec<PipelineNode>, VmsError> {
        self.set_destination_unresolved(destination_id, true).await
    }

    /// Clears the unresolved-reference status on every node pointing at
    /// `destination_id`. Called when the destination is re-enabled. Returns the
    /// nodes that changed.
    pub async fn clear_destination_unresolved(
        &self,
        destination_id: Uuid,
    ) -> Result<Vec<PipelineNode>, VmsError> {
        self.set_destination_unresolved(destination_id, false).await
    }

    async fn set_destination_unresolved(
        &self,
        destination_id: Uuid,
        unresolved: bool,
    ) -> Result<Vec<PipelineNode>, VmsError> {
        let nodes = pipeline_node::Entity::find()
            .filter(pipeline_node::Column::DestinationId.eq(destination_id))
            .filter(pipeline_node::Column::UnresolvedReference.ne(unresolved))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let mut updated = Vec::with_capacity(nodes.len());
        for n in nodes {
            let mut active: pipeline_node::ActiveModel = n.into();
            active.unresolved_reference = Set(unresolved);
            let saved = active.update(&self.db).await.map_err(db_err)?;
            updated.push(node_from_db(saved)?);
        }
        Ok(updated)
    }

    // -- Pipeline validation --

    /// Every problem currently found with `pipeline_id`'s definition, across every
    /// category (see `pipeline_validation`). This is the only place that combines
    /// the individual checks.
    pub async fn validate_pipeline(
        &self,
        pipeline_id: Uuid,
    ) -> Result<Vec<ValidationIssue>, VmsError> {
        let nodes = self.load_nodes(pipeline_id).await?;
        let edges = self.load_edges(pipeline_id).await?;
        let triggers = self.load_triggers(pipeline_id).await?;
        let camera_ids: HashSet<Uuid> = camera::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)?
            .into_iter()
            .map(|c| c.id)
            .collect();

        let mut issues = check_config_completeness(&nodes);
        issues.extend(check_config_shape(&nodes));
        issues.extend(check_structural_violations(&nodes, &edges));
        issues.extend(check_disconnected(&nodes, &edges));
        issues.extend(check_dangling_camera_references(&nodes, &camera_ids));
        issues.extend(check_unresolved_trigger_references(&triggers));
        issues.extend(check_unresolved_node_references(&nodes));
        issues.extend(check_artifact_lineage(&nodes, &edges));
        Ok(issues)
    }

    /// Recomputes and persists `pipeline_id`'s validation issues, so a read
    /// never has to re-run graph analysis. A no-op returning an empty list
    /// if the pipeline no longer exists.
    pub async fn revalidate(&self, pipeline_id: Uuid) -> Result<Vec<ValidationIssue>, VmsError> {
        let issues = self.validate_pipeline(pipeline_id).await?;
        self.persist_validation_issues(pipeline_id, &issues).await?;
        Ok(issues)
    }

    /// Writes an already-computed issue list onto the pipeline row without
    /// recomputing it. Shared by `revalidate` and `set_enabled`, which needs the
    /// issues before deciding whether to also flip `enabled`. A no-op if the
    /// pipeline no longer exists.
    async fn persist_validation_issues(
        &self,
        pipeline_id: Uuid,
        issues: &[ValidationIssue],
    ) -> Result<(), VmsError> {
        let Some(existing) = pipeline::Entity::find_by_id(pipeline_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
        else {
            return Ok(());
        };

        let mut active: ActiveModel = existing.into();
        active.validation_issues = Set(serde_json::to_value(issues)?);
        active.update(&self.db).await.map_err(db_err)?;
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
        ActionConfig::Skip => Db::Skip,
    }
}

/// The camera an action config explicitly targets, for the six action types
/// that carry a `camera_id: Option<Uuid>` field. Every other action type works
/// on an upstream artifact or has no camera at all (e.g. `trigger_alarm_output`
/// addresses an alarm output by `output_id`). The match is exhaustive, with no
/// wildcard, so a new camera-scoped variant forces a decision here instead of
/// silently defaulting to "no camera". Also used by `pipeline_validation`'s
/// dangling-camera-reference check.
pub(super) fn camera_id_from_action_config(config: &ActionConfig) -> Option<Uuid> {
    match config {
        ActionConfig::ExtractClip(c) => c.camera_id,
        ActionConfig::Snapshot(c) => c.camera_id,
        ActionConfig::PtzMove(c) => c.camera_id,
        ActionConfig::StartRecording(c) => c.camera_id,
        ActionConfig::StopRecording(c) => c.camera_id,
        ActionConfig::SetStreamQuality(c) => c.camera_id,
        ActionConfig::Transcode(_)
        | ActionConfig::MergeClips(_)
        | ActionConfig::Compress(_)
        | ActionConfig::Encrypt(_)
        | ActionConfig::Watermark(_)
        | ActionConfig::RenderNotification(_)
        | ActionConfig::Delay(_)
        | ActionConfig::TriggerAlarmOutput(_)
        | ActionConfig::Skip => None,
    }
}

/// Which cameras a pipeline's nodes/triggers reference, and what each one
/// needs. `needs_ring_buffer` is set by an `extract_clip` node.
/// `needs_analytics` is set by an enabled `Event` trigger that listens on a
/// camera topic (no `source_id`), because motion detection is what publishes
/// there. A camera-scoped one marks its own camera; an unscoped one marks
/// every camera the pipeline references, since it subscribes to all of them.
///
/// A camera is referenced by (a) any *enabled* trigger's resolved
/// `camera_id`, or (b) any node whose action config carries an explicit
/// `camera_id` (`camera_id_from_action_config`). For the camera-scoped action
/// types, `None` means "inherit from the `TriggerContext` at runtime" (see each
/// config's doc comment in `vms-core::action`). Such a node's ring-buffer
/// requirement is applied to the camera of *every* enabled camera-scoped
/// trigger, because over-provisioning a ring buffer on a candidate camera is
/// far cheaper than silently missing pre-event footage on the real one. If no
/// trigger resolves a camera, the requirement is dropped: a pipeline that only
/// fires from `Manual`/`Schedule`/unscoped `Event` triggers has no statically
/// knowable camera for that node until it actually fires.
fn derive_camera_refs(
    nodes: &[PipelineNode],
    triggers: &[PipelineTrigger],
) -> Vec<PipelineCameraRef> {
    let trigger_cameras: HashSet<Uuid> = triggers
        .iter()
        .filter(|t| t.enabled)
        .filter_map(|t| t.camera_id)
        .collect();

    let mut refs: HashMap<Uuid, (u32, bool)> = HashMap::new();
    for &camera_id in &trigger_cameras {
        refs.entry(camera_id).or_insert((0, false));
    }

    for node in nodes {
        let Some(action_config) = &node.action_config else {
            continue;
        };
        let ring_buffer_secs = extract_clip_ring_buffer_secs(action_config);

        match camera_id_from_action_config(action_config) {
            Some(camera_id) => {
                let entry = refs.entry(camera_id).or_insert((0, false));
                entry.0 = entry.0.max(ring_buffer_secs);
            }
            None if ring_buffer_secs > 0 => {
                for &camera_id in &trigger_cameras {
                    let entry = refs.entry(camera_id).or_insert((0, false));
                    entry.0 = entry.0.max(ring_buffer_secs);
                }
            }
            None => {}
        }
    }

    let camera_event_triggers = triggers
        .iter()
        .filter(|t| t.enabled && t.trigger_type == CoreTriggerType::Event && t.source_id.is_none());
    for trigger in camera_event_triggers {
        match trigger.camera_id {
            Some(camera_id) => refs.entry(camera_id).or_insert((0, false)).1 = true,
            None => refs.values_mut().for_each(|entry| entry.1 = true),
        }
    }

    refs.into_iter()
        .map(
            |(camera_id, (ring_buffer_secs, needs_analytics))| PipelineCameraRef {
                camera_id,
                needs_ring_buffer: ring_buffer_secs > 0,
                ring_buffer_secs,
                needs_analytics,
            },
        )
        .collect()
}

/// Ring buffer seconds an `ExtractClip` node requires (`0` for every other
/// action type): `pre_event_secs + EXTRACT_CLIP_KEYFRAME_SEARCH_SECS +
/// post_event_secs + RING_BUFFER_EVICTION_SAFETY_SECS`, capped at
/// `MAX_RING_BUFFER_SECS` so one misconfigured node can't blow up per-camera
/// memory use.
fn extract_clip_ring_buffer_secs(action_config: &ActionConfig) -> u32 {
    let ActionConfig::ExtractClip(cfg) = action_config else {
        return 0;
    };
    cfg.pre_event_secs
        .saturating_add(vms_core::action::EXTRACT_CLIP_KEYFRAME_SEARCH_SECS)
        .saturating_add(cfg.post_event_secs)
        .saturating_add(vms_core::action::RING_BUFFER_EVICTION_SAFETY_SECS)
        .min(vms_core::action::MAX_RING_BUFFER_SECS)
}

/// Serialize whichever of `action_config`/`transport_config`/`condition_expr`
/// applies to `node_type` into the single JSON blob stored in
/// `pipeline_nodes.config`. Callers run `validate_create_shape` first; this
/// doesn't re-check the combination and ignores fields `node_type` doesn't use.
fn config_json_for(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    condition_expr: &Option<String>,
) -> Result<serde_json::Value, VmsError> {
    let value = match node_type {
        // `action_config` may be absent while the node isn't fully configured.
        // It's stored as JSON null, which `node_from_db` reads back as `None`
        // instead of trying to deserialize it as an `ActionConfig`.
        CoreNodeType::Action | CoreNodeType::DeviceControl => match action_config {
            Some(ac) => serde_json::to_value(ac)?,
            None => serde_json::Value::Null,
        },
        CoreNodeType::Transport => {
            serde_json::to_value(transport_config.clone().unwrap_or_default())?
        }
        CoreNodeType::Condition => serde_json::json!({ "condition_expr": condition_expr }),
        CoreNodeType::TriggerRoot | CoreNodeType::Fork => serde_json::json!({}),
    };
    Ok(value)
}

/// Rejects a config the node's type doesn't use (e.g. a `transport_config` on
/// an `Action` node). A missing value the node will eventually need (an empty
/// `condition_expr`, no `destination_id`) is deliberately *not* checked here:
/// that's the normal work-in-progress state while a pipeline is being edited.
/// Root uniqueness depends on sibling nodes already in the pipeline, so the
/// caller checks it.
fn validate_create_shape(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    destination_id: Option<Uuid>,
    condition_expr: &Option<String>,
) -> Result<(), VmsError> {
    match node_type {
        CoreNodeType::Action | CoreNodeType::DeviceControl => {
            if transport_config.is_some() || condition_expr.is_some() || destination_id.is_some() {
                return Err(VmsError::DagValidation(format!(
                    "{} node must not set transport_config, condition_expr, or destination_id",
                    node_type.as_str()
                )));
            }
        }
        CoreNodeType::Transport => {
            if action_config.is_some() || condition_expr.is_some() {
                return Err(VmsError::DagValidation(
                    "transport node must not set action_config or condition_expr".into(),
                ));
            }
        }
        CoreNodeType::Condition => {
            if action_config.is_some() || transport_config.is_some() || destination_id.is_some() {
                return Err(VmsError::DagValidation(
                    "condition node must not set action_config, transport_config, or destination_id"
                        .into(),
                ));
            }
        }
        CoreNodeType::TriggerRoot | CoreNodeType::Fork => {
            if action_config.is_some()
                || transport_config.is_some()
                || condition_expr.is_some()
                || destination_id.is_some()
            {
                return Err(VmsError::DagValidation(format!(
                    "{} node must not set any config",
                    node_type.as_str()
                )));
            }
        }
    }
    Ok(())
}

/// Same idea as `validate_create_shape`, for a partial update: unset (`None`)
/// fields are always fine, and only a provided field that doesn't belong to
/// the node's (immutable) type is rejected. `destination_id` uses the
/// double-`Option` update shape (see `UpdateNode`): only a provided value
/// (`Some(Some(_))`) is shape-checked, and clearing it (`Some(None)`) is
/// allowed on any node type.
fn validate_update_shape(
    node_type: &CoreNodeType,
    action_config: &Option<ActionConfig>,
    transport_config: &Option<TransportConfig>,
    destination_id: &Option<Option<Uuid>>,
    condition_expr: &Option<String>,
) -> Result<(), VmsError> {
    if action_config.is_some()
        && !matches!(
            node_type,
            CoreNodeType::Action | CoreNodeType::DeviceControl
        )
    {
        return Err(VmsError::DagValidation(format!(
            "{} node does not accept action_config",
            node_type.as_str()
        )));
    }
    if transport_config.is_some() && !matches!(node_type, CoreNodeType::Transport) {
        return Err(VmsError::DagValidation(format!(
            "{} node does not accept transport_config",
            node_type.as_str()
        )));
    }
    if matches!(destination_id, Some(Some(_))) && !matches!(node_type, CoreNodeType::Transport) {
        return Err(VmsError::DagValidation(format!(
            "{} node does not accept destination_id",
            node_type.as_str()
        )));
    }
    if condition_expr.is_some() && !matches!(node_type, CoreNodeType::Condition) {
        return Err(VmsError::DagValidation(format!(
            "{} node does not accept condition_expr",
            node_type.as_str()
        )));
    }
    Ok(())
}

fn node_from_db(m: pipeline_node::Model) -> Result<PipelineNode, VmsError> {
    let node_type = node_type_from_db(&m.node_type);

    // The config JSON column stores different payloads depending on node_type.
    let (action_config, transport_config, condition_expr) = match node_type {
        CoreNodeType::Action | CoreNodeType::DeviceControl => {
            if m.config.is_null() {
                (None, None, None)
            } else {
                let ac = serde_json::from_value::<ActionConfig>(m.config).map_err(|e| {
                    VmsError::Serialization(format!("node {}: action_config: {e}", m.id))
                })?;
                (Some(ac), None, None)
            }
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
        unresolved_reference: m.unresolved_reference,
    })
}

fn edge_type_from_db(db_type: &pipeline_edge::EdgeType) -> CoreEdgeType {
    use pipeline_edge::EdgeType as Db;
    match db_type {
        Db::Default => CoreEdgeType::Default,
        Db::TrueBranch => CoreEdgeType::TrueBranch,
        Db::FalseBranch => CoreEdgeType::FalseBranch,
    }
}

fn edge_type_to_db(core_type: &CoreEdgeType) -> pipeline_edge::EdgeType {
    use pipeline_edge::EdgeType as Db;
    match core_type {
        CoreEdgeType::Default => Db::Default,
        CoreEdgeType::TrueBranch => Db::TrueBranch,
        CoreEdgeType::FalseBranch => Db::FalseBranch,
    }
}

fn edge_from_db(m: pipeline_edge::Model) -> PipelineEdge {
    PipelineEdge {
        edge_type: edge_type_from_db(&m.edge_type),
        id: m.id,
        pipeline_id: m.pipeline_id,
        from_node_id: m.from_node_id,
        to_node_id: m.to_node_id,
    }
}

/// Per-edge checks shared by `create_edge` and `update_edge`:
/// - `Transport`/`DeviceControl` sources can never have an outgoing edge
///   (rule 4, which holds at every stage of editing).
/// - A `Condition` source's edges must be `true_branch`/`false_branch`,
///   never `default`; any other source's edges must be `default`, never a
///   branch tag (the per-edge half of rule 5).
/// - A `Condition` source may not have two edges of the same branch, or more
///   than two outgoing edges at all.
///
/// `sibling_edges` is every *other* outgoing edge already recorded for
/// `from_node` (for an update, excluding the one being replaced).
fn validate_new_edge(
    from_node: &PipelineNode,
    edge_type: &CoreEdgeType,
    to_node_id: Uuid,
    sibling_edges: &[PipelineEdge],
) -> Result<(), VmsError> {
    if matches!(
        from_node.node_type,
        CoreNodeType::Transport | CoreNodeType::DeviceControl
    ) {
        return Err(VmsError::DagValidation(format!(
            "{} node {} must be a leaf and cannot have outgoing edges",
            from_node.node_type.as_str(),
            from_node.id
        )));
    }

    let is_condition = from_node.node_type == CoreNodeType::Condition;
    let is_branch = matches!(
        edge_type,
        CoreEdgeType::TrueBranch | CoreEdgeType::FalseBranch
    );
    if is_condition && !is_branch {
        return Err(VmsError::DagValidation(
            "edges from a condition node must be true_branch or false_branch".into(),
        ));
    }
    if !is_condition && is_branch {
        return Err(VmsError::DagValidation(
            "true_branch/false_branch edges are only valid from a condition node".into(),
        ));
    }

    let siblings_from_this_node: Vec<&PipelineEdge> = sibling_edges
        .iter()
        .filter(|e| e.from_node_id == from_node.id)
        .collect();

    if is_condition {
        if siblings_from_this_node.len() >= 2 {
            return Err(VmsError::DagValidation(format!(
                "condition node {} already has 2 outgoing edges",
                from_node.id
            )));
        }
        if siblings_from_this_node
            .iter()
            .any(|e| &e.edge_type == edge_type)
        {
            return Err(VmsError::DagValidation(format!(
                "condition node {} already has a {} edge",
                from_node.id,
                edge_type.as_str()
            )));
        }
    }

    if sibling_edges
        .iter()
        .any(|e| e.from_node_id == from_node.id && e.to_node_id == to_node_id)
    {
        return Err(VmsError::DagValidation(
            "an edge already exists between these two nodes".into(),
        ));
    }

    Ok(())
}

fn trigger_type_from_db(db_type: &pipeline_trigger::TriggerType) -> CoreTriggerType {
    use pipeline_trigger::TriggerType as Db;
    match db_type {
        Db::Schedule => CoreTriggerType::Schedule,
        Db::Event => CoreTriggerType::Event,
        Db::System => CoreTriggerType::System,
        Db::Manual => CoreTriggerType::Manual,
        Db::Stat => CoreTriggerType::Stat,
    }
}

fn trigger_type_to_db(core_type: &CoreTriggerType) -> pipeline_trigger::TriggerType {
    use pipeline_trigger::TriggerType as Db;
    match core_type {
        CoreTriggerType::Schedule => Db::Schedule,
        CoreTriggerType::Event => Db::Event,
        CoreTriggerType::System => Db::System,
        CoreTriggerType::Manual => Db::Manual,
        CoreTriggerType::Stat => Db::Stat,
    }
}

/// The `trigger_type` a given `TriggerConfig` implies. `PipelineTrigger` and
/// the `pipeline_triggers.trigger_type` column store it separately from
/// `config` even though `config`'s serde tag already encodes it, just as
/// `pipeline_nodes.action_type` mirrors `ActionConfig`'s tag, so it can be
/// queried without deserializing `config`.
fn trigger_type_from_config(config: &TriggerConfig) -> CoreTriggerType {
    match config {
        TriggerConfig::Schedule { .. } => CoreTriggerType::Schedule,
        TriggerConfig::Event { .. } => CoreTriggerType::Event,
        TriggerConfig::System { .. } => CoreTriggerType::System,
        TriggerConfig::Manual { .. } => CoreTriggerType::Manual,
        TriggerConfig::Stat { .. } => CoreTriggerType::Stat,
    }
}

/// The top-level `camera_id` column to store for a given config and
/// caller-supplied top-level `camera_id`.
///
/// For `System` triggers this is always derived from
/// `TriggerConfig::System::camera_id`, never from the caller's top-level value
/// (which `validate_trigger_shape` already rejects). `evaluate_event`
/// (`vms-engine`) reads the config's own `camera_id` for `System` triggers, not
/// the top-level column, so the two must never diverge. For every other
/// trigger type the top-level value passes through unchanged.
fn effective_camera_id(config: &TriggerConfig, camera_id: Option<Uuid>) -> Option<Uuid> {
    match config {
        TriggerConfig::System {
            camera_id: cfg_camera_id,
            ..
        } => *cfg_camera_id,
        _ => camera_id,
    }
}

/// Rejects `source_id`/`camera_id` combinations that `vms-engine`'s
/// `TriggerEvaluator` can't act on correctly for `config`'s trigger type:
///
/// - `Schedule`/`Manual` triggers never look at either field, so both are
///   rejected.
/// - `Event` triggers reject having *both* set. `start_event_listener`
///   subscribes to the source topic when `source_id` is set and falls back to
///   the camera topic only when it isn't, so a `camera_id` next to a
///   `source_id` would never be subscribed to, and would never match at
///   evaluation time either (an event on a source topic has no `camera_id`).
///   The trigger would be permanently unreachable. Both unset is valid and
///   broadens the subscription to every resource the pipeline touches.
/// - `System` triggers scope to a camera through `config`'s own field (see
///   `effective_camera_id`), so a top-level `camera_id` is rejected to keep a
///   single place to set it. `source_id` is rejected outright; System
///   triggers always listen on the global `TopicKey::System`.
/// - `Stat` triggers have no `source_id` concept. `camera_id` is the *only*
///   way to scope a per-feed metric (`FeedBitrateKbps`/`FeedPacketLossPercent`),
///   so it's accepted freely.
fn validate_trigger_shape(
    config: &TriggerConfig,
    source_id: Option<Uuid>,
    camera_id: Option<Uuid>,
) -> Result<(), VmsError> {
    match config {
        TriggerConfig::Schedule { .. } | TriggerConfig::Manual { .. } => {
            if source_id.is_some() || camera_id.is_some() {
                return Err(VmsError::DagValidation(format!(
                    "{} trigger must not set source_id or camera_id",
                    trigger_type_from_config(config).as_str()
                )));
            }
        }
        TriggerConfig::Event { .. } => {
            if source_id.is_some() && camera_id.is_some() {
                return Err(VmsError::DagValidation(
                    "event trigger cannot set both source_id and camera_id — only one \
                     is ever used to subscribe, so the other would silently never match"
                        .into(),
                ));
            }
        }
        TriggerConfig::System { .. } => {
            if source_id.is_some() {
                return Err(VmsError::DagValidation(
                    "system trigger must not set source_id".into(),
                ));
            }
            if camera_id.is_some() {
                return Err(VmsError::DagValidation(
                    "system trigger scopes to a camera via config.camera_id, \
                     not the top-level camera_id field"
                        .into(),
                ));
            }
        }
        TriggerConfig::Stat { .. } => {
            if source_id.is_some() {
                return Err(VmsError::DagValidation(
                    "stat trigger must not set source_id".into(),
                ));
            }
        }
    }
    Ok(())
}

fn trigger_from_db(m: pipeline_trigger::Model) -> Result<PipelineTrigger, VmsError> {
    let trigger_type = trigger_type_from_db(&m.trigger_type);

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
        last_error: m.last_error,
        last_error_at: m.last_error_at.map(|dt| dt.with_timezone(&chrono::Utc)),
        unresolved_reference: m.unresolved_reference,
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

    fn skip_config() -> ActionConfig {
        ActionConfig::Skip
    }

    #[test]
    fn action_node_without_action_config_passes() {
        let result = validate_create_shape(&CoreNodeType::Action, &None, &None, None, &None);
        assert!(result.is_ok());
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
    fn action_node_rejects_destination_id() {
        let result = validate_create_shape(
            &CoreNodeType::Action,
            &Some(delay_config()),
            &None,
            Some(Uuid::new_v4()),
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
    fn transport_node_without_destination_id_passes() {
        let result = validate_create_shape(
            &CoreNodeType::Transport,
            &None,
            &Some(TransportConfig::default()),
            None,
            &None,
        );
        assert!(result.is_ok());
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
    fn condition_node_without_expr_passes() {
        let empty = validate_create_shape(
            &CoreNodeType::Condition,
            &None,
            &None,
            None,
            &Some("   ".into()),
        );
        assert!(empty.is_ok());

        let missing = validate_create_shape(&CoreNodeType::Condition, &None, &None, None, &None);
        assert!(missing.is_ok());
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
            &None,
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn update_allows_leaving_every_field_unset() {
        let result = validate_update_shape(&CoreNodeType::Action, &None, &None, &None, &None);
        assert!(result.is_ok());
    }

    #[test]
    fn update_allows_setting_condition_expr_to_whitespace() {
        let result = validate_update_shape(
            &CoreNodeType::Condition,
            &None,
            &None,
            &None,
            &Some("  ".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn update_rejects_destination_id_on_a_non_transport_node() {
        let result = validate_update_shape(
            &CoreNodeType::Action,
            &None,
            &None,
            &Some(Some(Uuid::new_v4())),
            &None,
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn update_allows_clearing_destination_id_on_any_node_type() {
        let result = validate_update_shape(&CoreNodeType::Action, &None, &None, &Some(None), &None);
        assert!(result.is_ok());
    }

    #[test]
    fn action_type_round_trips_through_node_type_conversions() {
        assert_eq!(
            action_type_from_config(&delay_config()),
            pipeline_node::ActionType::Delay
        );
        assert_eq!(
            action_type_from_config(&skip_config()),
            pipeline_node::ActionType::Skip
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

    #[test]
    fn config_json_for_condition_without_expr_is_null_expr() {
        let value = config_json_for(&CoreNodeType::Condition, &None, &None, &None).unwrap();
        assert_eq!(value, serde_json::json!({ "condition_expr": null }));
    }

    #[test]
    fn config_json_for_action_without_action_config_is_null() {
        let value = config_json_for(&CoreNodeType::Action, &None, &None, &None).unwrap();
        assert_eq!(value, serde_json::Value::Null);
    }

    #[test]
    fn node_from_db_reads_null_action_config_as_none() {
        let model = pipeline_node::Model {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            node_type: node_type_to_db(&CoreNodeType::Action),
            action_type: None,
            destination_id: None,
            contact_list_id: None,
            config: serde_json::Value::Null,
            label: None,
            pos_x: None,
            pos_y: None,
            created_at: now(),
            unresolved_reference: false,
        };
        let node = node_from_db(model).unwrap();
        assert!(node.action_config.is_none());
    }

    // -- validate_new_edge --

    fn node_of_type(node_type: CoreNodeType) -> PipelineNode {
        PipelineNode {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            node_type,
            action_config: None,
            destination_id: None,
            contact_list_id: None,
            transport_config: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
            unresolved_reference: false,
        }
    }

    #[test]
    fn transport_source_rejects_any_outgoing_edge() {
        let from = node_of_type(CoreNodeType::Transport);
        let result = validate_new_edge(&from, &CoreEdgeType::Default, Uuid::new_v4(), &[]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn device_control_source_rejects_any_outgoing_edge() {
        let from = node_of_type(CoreNodeType::DeviceControl);
        let result = validate_new_edge(&from, &CoreEdgeType::Default, Uuid::new_v4(), &[]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn non_condition_source_rejects_branch_edge_type() {
        let from = node_of_type(CoreNodeType::Action);
        let result = validate_new_edge(&from, &CoreEdgeType::TrueBranch, Uuid::new_v4(), &[]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn condition_source_rejects_default_edge_type() {
        let from = node_of_type(CoreNodeType::Condition);
        let result = validate_new_edge(&from, &CoreEdgeType::Default, Uuid::new_v4(), &[]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn condition_source_accepts_first_branch_edge() {
        let from = node_of_type(CoreNodeType::Condition);
        let result = validate_new_edge(&from, &CoreEdgeType::TrueBranch, Uuid::new_v4(), &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn condition_source_rejects_duplicate_branch() {
        let from = node_of_type(CoreNodeType::Condition);
        let existing = PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: from.pipeline_id,
            from_node_id: from.id,
            to_node_id: Uuid::new_v4(),
            edge_type: CoreEdgeType::TrueBranch,
        };
        let result = validate_new_edge(
            &from,
            &CoreEdgeType::TrueBranch,
            Uuid::new_v4(),
            &[existing],
        );
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn condition_source_rejects_a_third_outgoing_edge() {
        let from = node_of_type(CoreNodeType::Condition);
        let e1 = PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: from.pipeline_id,
            from_node_id: from.id,
            to_node_id: Uuid::new_v4(),
            edge_type: CoreEdgeType::TrueBranch,
        };
        let e2 = PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: from.pipeline_id,
            from_node_id: from.id,
            to_node_id: Uuid::new_v4(),
            edge_type: CoreEdgeType::FalseBranch,
        };
        // A third edge, even with a nonsense repeated branch, must still be
        // rejected on the "already has 2" check before the "duplicate
        // branch" check would also apply.
        let result = validate_new_edge(&from, &CoreEdgeType::TrueBranch, Uuid::new_v4(), &[e1, e2]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn rejects_duplicate_edge_between_the_same_pair() {
        let from = node_of_type(CoreNodeType::Action);
        let to_id = Uuid::new_v4();
        let existing = PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id: from.pipeline_id,
            from_node_id: from.id,
            to_node_id: to_id,
            edge_type: CoreEdgeType::Default,
        };
        let result = validate_new_edge(&from, &CoreEdgeType::Default, to_id, &[existing]);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn valid_default_edge_from_a_non_condition_node_passes() {
        let from = node_of_type(CoreNodeType::Action);
        let result = validate_new_edge(&from, &CoreEdgeType::Default, Uuid::new_v4(), &[]);
        assert!(result.is_ok());
    }

    // -- validate_trigger_shape / effective_camera_id --

    fn schedule_config() -> TriggerConfig {
        TriggerConfig::Schedule {
            mode: vms_core::trigger::ScheduleMode::Interval { interval_secs: 60 },
            timezone: "UTC".into(),
        }
    }

    fn event_config() -> TriggerConfig {
        TriggerConfig::Event {
            filter: None,
            duration_secs: None,
        }
    }

    fn system_config(camera_id: Option<Uuid>) -> TriggerConfig {
        TriggerConfig::System {
            signal: vms_core::trigger::SystemSignal::FeedDisconnected,
            camera_id,
        }
    }

    fn stat_config() -> TriggerConfig {
        TriggerConfig::Stat {
            metric: vms_core::trigger::StatMetric::FeedBitrateKbps,
            path: None,
            operator: vms_core::trigger::CompareOperator::LessThan,
            threshold: 100.0,
            sustained_secs: 0,
            cooldown_secs: 0,
        }
    }

    #[test]
    fn schedule_trigger_rejects_source_and_camera_id() {
        let result = validate_trigger_shape(&schedule_config(), Some(Uuid::new_v4()), None);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn event_trigger_allows_neither_id_set() {
        assert!(validate_trigger_shape(&event_config(), None, None).is_ok());
    }

    #[test]
    fn event_trigger_allows_source_id_only() {
        assert!(validate_trigger_shape(&event_config(), Some(Uuid::new_v4()), None).is_ok());
    }

    #[test]
    fn event_trigger_allows_camera_id_only() {
        assert!(validate_trigger_shape(&event_config(), None, Some(Uuid::new_v4())).is_ok());
    }

    #[test]
    fn event_trigger_rejects_both_source_and_camera_id() {
        let result =
            validate_trigger_shape(&event_config(), Some(Uuid::new_v4()), Some(Uuid::new_v4()));
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn system_trigger_rejects_source_id() {
        let result = validate_trigger_shape(&system_config(None), Some(Uuid::new_v4()), None);
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn system_trigger_rejects_top_level_camera_id() {
        let result = validate_trigger_shape(&system_config(None), None, Some(Uuid::new_v4()));
        assert!(matches!(result, Err(VmsError::DagValidation(_))));
    }

    #[test]
    fn system_trigger_with_only_config_camera_id_passes() {
        assert!(validate_trigger_shape(&system_config(Some(Uuid::new_v4())), None, None).is_ok());
    }

    #[test]
    fn stat_trigger_rejects_source_id_but_allows_camera_id() {
        let rejected = validate_trigger_shape(&stat_config(), Some(Uuid::new_v4()), None);
        assert!(matches!(rejected, Err(VmsError::DagValidation(_))));

        let allowed = validate_trigger_shape(&stat_config(), None, Some(Uuid::new_v4()));
        assert!(allowed.is_ok());
    }

    #[test]
    fn effective_camera_id_derives_from_system_config_not_the_input() {
        let cfg_cam = Uuid::new_v4();
        let caller_supplied = Uuid::new_v4();
        assert_eq!(
            effective_camera_id(&system_config(Some(cfg_cam)), Some(caller_supplied)),
            Some(cfg_cam)
        );
        assert_eq!(
            effective_camera_id(&system_config(None), Some(caller_supplied)),
            None
        );
    }

    #[test]
    fn effective_camera_id_passes_through_for_non_system_triggers() {
        let caller_supplied = Uuid::new_v4();
        assert_eq!(
            effective_camera_id(&event_config(), Some(caller_supplied)),
            Some(caller_supplied)
        );
    }

    #[test]
    fn trigger_type_from_config_matches_each_variant() {
        assert_eq!(
            trigger_type_from_config(&schedule_config()),
            CoreTriggerType::Schedule
        );
        assert_eq!(
            trigger_type_from_config(&event_config()),
            CoreTriggerType::Event
        );
        assert_eq!(
            trigger_type_from_config(&system_config(None)),
            CoreTriggerType::System
        );
        assert_eq!(
            trigger_type_from_config(&stat_config()),
            CoreTriggerType::Stat
        );
    }

    #[test]
    fn trigger_type_round_trips_through_db_conversions() {
        assert_eq!(
            trigger_type_from_db(&trigger_type_to_db(&CoreTriggerType::Stat)),
            CoreTriggerType::Stat
        );
    }

    // -- derive_camera_refs / camera_id_from_action_config --

    fn action_node(action_config: ActionConfig) -> PipelineNode {
        PipelineNode {
            action_config: Some(action_config),
            ..node_of_type(CoreNodeType::Action)
        }
    }

    fn trigger_row(
        config: TriggerConfig,
        source_id: Option<Uuid>,
        camera_id: Option<Uuid>,
        enabled: bool,
    ) -> PipelineTrigger {
        PipelineTrigger {
            id: Uuid::new_v4(),
            pipeline_id: Uuid::new_v4(),
            trigger_type: trigger_type_from_config(&config),
            source_id,
            camera_id,
            config,
            enabled,
            last_error: None,
            last_error_at: None,
            unresolved_reference: false,
        }
    }

    fn extract_clip_config(camera_id: Option<Uuid>) -> ActionConfig {
        ActionConfig::ExtractClip(vms_core::action::ExtractClipConfig {
            pre_event_secs: 5,
            post_event_secs: 5,
            format: "mp4".into(),
            camera_id,
            use_manual_range: false,
        })
    }

    fn ptz_move_config(camera_id: Option<Uuid>) -> ActionConfig {
        ActionConfig::PtzMove(vms_core::action::PtzMoveConfig {
            camera_id,
            command: vms_core::action::PtzCommand::Preset { preset_id: 1 },
        })
    }

    #[test]
    fn camera_id_from_action_config_reads_the_six_camera_scoped_variants() {
        let cam = Uuid::new_v4();
        assert_eq!(
            camera_id_from_action_config(&extract_clip_config(Some(cam))),
            Some(cam)
        );
        assert_eq!(
            camera_id_from_action_config(&ptz_move_config(Some(cam))),
            Some(cam)
        );
    }

    #[test]
    fn camera_id_from_action_config_returns_none_for_artifact_only_actions() {
        assert_eq!(camera_id_from_action_config(&delay_config()), None);
        assert_eq!(camera_id_from_action_config(&skip_config()), None);
    }

    #[test]
    fn extract_clip_with_explicit_camera_sets_needs_ring_buffer() {
        let cam = Uuid::new_v4();
        let nodes = vec![action_node(extract_clip_config(Some(cam)))];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].camera_id, cam);
        assert!(refs[0].needs_ring_buffer);
        assert!(!refs[0].needs_analytics);
    }

    #[test]
    fn ptz_move_with_explicit_camera_creates_a_bare_ref() {
        // No ring buffer needed, but the camera's pipeline still needs to
        // be running for the PTZ command to have somewhere to go.
        let cam = Uuid::new_v4();
        let nodes = vec![action_node(ptz_move_config(Some(cam)))];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].camera_id, cam);
        assert!(!refs[0].needs_ring_buffer);
    }

    #[test]
    fn implicit_camera_extract_clip_inherits_from_a_scoped_trigger() {
        let cam = Uuid::new_v4();
        let nodes = vec![action_node(extract_clip_config(None))];
        let triggers = vec![trigger_row(system_config(Some(cam)), None, Some(cam), true)];

        let refs = derive_camera_refs(&nodes, &triggers);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].camera_id, cam);
        assert!(refs[0].needs_ring_buffer);
    }

    #[test]
    fn implicit_camera_extract_clip_with_no_scoped_trigger_is_dropped() {
        // Nothing statically identifies which camera this would run
        // against: a Manual/Schedule-only pipeline, or an unscoped Event
        // trigger, can't resolve it until the moment it actually fires.
        let nodes = vec![action_node(extract_clip_config(None))];
        let triggers = vec![trigger_row(schedule_config(), None, None, true)];

        let refs = derive_camera_refs(&nodes, &triggers);
        assert!(refs.is_empty());
    }

    #[test]
    fn disabled_trigger_camera_is_not_referenced() {
        let cam = Uuid::new_v4();
        let triggers = vec![trigger_row(
            system_config(Some(cam)),
            None,
            Some(cam),
            false,
        )];
        let refs = derive_camera_refs(&[], &triggers);
        assert!(refs.is_empty());
    }

    #[test]
    fn camera_event_trigger_needs_analytics_on_its_camera() {
        let cam = Uuid::new_v4();
        let triggers = vec![trigger_row(event_config(), None, Some(cam), true)];
        let refs = derive_camera_refs(&[], &triggers);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].camera_id, cam);
        assert!(refs[0].needs_analytics);
    }

    #[test]
    fn unscoped_event_trigger_needs_analytics_on_every_referenced_camera() {
        let (trigger_cam, node_cam) = (Uuid::new_v4(), Uuid::new_v4());
        let triggers = vec![
            trigger_row(
                system_config(Some(trigger_cam)),
                None,
                Some(trigger_cam),
                true,
            ),
            trigger_row(event_config(), None, None, true),
        ];
        let nodes = vec![action_node(extract_clip_config(Some(node_cam)))];
        let refs = derive_camera_refs(&nodes, &triggers);

        assert_eq!(refs.len(), 2);
        assert!(refs.iter().all(|r| r.needs_analytics));
    }

    #[test]
    fn source_or_disabled_event_triggers_do_not_need_analytics() {
        let cam = Uuid::new_v4();
        let triggers = vec![
            trigger_row(system_config(Some(cam)), None, Some(cam), true),
            trigger_row(event_config(), Some(Uuid::new_v4()), None, true),
            trigger_row(event_config(), None, Some(cam), false),
        ];
        let refs = derive_camera_refs(&[], &triggers);

        assert_eq!(refs.len(), 1);
        assert!(!refs[0].needs_analytics);
    }

    #[test]
    fn trigger_only_reference_gets_a_bare_ref_row() {
        let cam = Uuid::new_v4();
        let triggers = vec![trigger_row(system_config(Some(cam)), None, Some(cam), true)];
        let refs = derive_camera_refs(&[], &triggers);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].camera_id, cam);
        assert!(!refs[0].needs_ring_buffer);
        assert!(!refs[0].needs_analytics);
    }

    #[test]
    fn needs_ring_buffer_ors_across_multiple_nodes_on_the_same_camera() {
        let cam = Uuid::new_v4();
        let nodes = vec![
            action_node(ptz_move_config(Some(cam))),
            action_node(extract_clip_config(Some(cam))),
        ];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert!(refs[0].needs_ring_buffer);
    }

    fn extract_clip_config_with_window(
        camera_id: Option<Uuid>,
        pre_event_secs: u32,
        post_event_secs: u32,
    ) -> ActionConfig {
        ActionConfig::ExtractClip(vms_core::action::ExtractClipConfig {
            pre_event_secs,
            post_event_secs,
            format: "mp4".into(),
            camera_id,
            use_manual_range: false,
        })
    }

    #[test]
    fn ring_buffer_secs_is_pre_plus_keyframe_margin_plus_post() {
        let cam = Uuid::new_v4();
        let nodes = vec![action_node(extract_clip_config_with_window(
            Some(cam),
            5,
            10,
        ))];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].ring_buffer_secs,
            5 + vms_core::action::EXTRACT_CLIP_KEYFRAME_SEARCH_SECS
                + 10
                + vms_core::action::RING_BUFFER_EVICTION_SAFETY_SECS
        );
    }

    #[test]
    fn ring_buffer_secs_is_capped_at_the_maximum() {
        let cam = Uuid::new_v4();
        let nodes = vec![action_node(extract_clip_config_with_window(
            Some(cam),
            600,
            600,
        ))];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].ring_buffer_secs,
            vms_core::action::MAX_RING_BUFFER_SECS
        );
    }

    #[test]
    fn ring_buffer_secs_maxes_across_multiple_extract_clip_nodes_on_the_same_camera() {
        let cam = Uuid::new_v4();
        let nodes = vec![
            action_node(extract_clip_config_with_window(Some(cam), 1, 1)),
            action_node(extract_clip_config_with_window(Some(cam), 20, 30)),
        ];
        let refs = derive_camera_refs(&nodes, &[]);

        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0].ring_buffer_secs,
            20 + vms_core::action::EXTRACT_CLIP_KEYFRAME_SEARCH_SECS
                + 30
                + vms_core::action::RING_BUFFER_EVICTION_SAFETY_SECS
        );
    }

    // -- upsert_node_trigger / delete_triggers_for_pipeline --

    use crate::migration::Migrator;
    use sea_orm_migration::MigratorTrait;
    use vms_core::trigger::ScheduleMode;

    async fn test_repo() -> PipelineRepo {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        PipelineRepo::new(db)
    }

    async fn make_pipeline(repo: &PipelineRepo) -> Uuid {
        repo.create(CreatePipeline {
            name: "test".into(),
            description: None,
            pipeline_type: PipelineType::User,
        })
        .await
        .unwrap()
        .id
    }

    fn manual_trigger() -> CreateTrigger {
        CreateTrigger {
            config: TriggerConfig::Manual {
                parameter_schema: None,
            },
            source_id: None,
            camera_id: None,
            enabled: true,
        }
    }

    fn schedule_trigger(expr: &str) -> CreateTrigger {
        CreateTrigger {
            config: TriggerConfig::Schedule {
                mode: ScheduleMode::Cron {
                    expression: expr.into(),
                },
                timezone: "UTC".into(),
            },
            source_id: None,
            camera_id: None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn upsert_node_trigger_creates_when_none_exists() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let trigger = repo
            .upsert_node_trigger(pipeline_id, manual_trigger())
            .await
            .unwrap();

        assert_eq!(trigger.pipeline_id, pipeline_id);
        assert_eq!(trigger.trigger_type, CoreTriggerType::Manual);
        assert_eq!(repo.load_triggers(pipeline_id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn upsert_node_trigger_updates_the_existing_row_of_the_same_variant() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let first = repo
            .upsert_node_trigger(pipeline_id, schedule_trigger("0 * * * *"))
            .await
            .unwrap();

        let updated = repo
            .upsert_node_trigger(pipeline_id, schedule_trigger("*/5 * * * *"))
            .await
            .unwrap();

        assert_eq!(updated.id, first.id);
        assert_eq!(repo.load_triggers(pipeline_id).await.unwrap().len(), 1);
        match updated.config {
            TriggerConfig::Schedule {
                mode: ScheduleMode::Cron { expression },
                ..
            } => assert_eq!(expression, "*/5 * * * *"),
            other => panic!("expected schedule/cron config, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn upsert_node_trigger_recreates_on_variant_change() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let first = repo
            .upsert_node_trigger(pipeline_id, schedule_trigger("0 * * * *"))
            .await
            .unwrap();

        let second = repo
            .upsert_node_trigger(pipeline_id, manual_trigger())
            .await
            .unwrap();

        assert_ne!(second.id, first.id);
        assert_eq!(second.trigger_type, CoreTriggerType::Manual);
        assert_eq!(repo.load_triggers(pipeline_id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delete_triggers_for_pipeline_removes_every_row() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        repo.upsert_node_trigger(pipeline_id, manual_trigger())
            .await
            .unwrap();

        repo.delete_triggers_for_pipeline(pipeline_id)
            .await
            .unwrap();

        assert!(repo.load_triggers(pipeline_id).await.unwrap().is_empty());
    }

    // -- set_trigger_error --

    #[tokio::test]
    async fn set_trigger_error_records_message_and_timestamp() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let trigger = repo
            .create_trigger(pipeline_id, manual_trigger())
            .await
            .unwrap();
        assert_eq!(trigger.last_error, None);

        repo.set_trigger_error(trigger.id, Some("boom".into()))
            .await
            .unwrap();

        let reloaded = repo.get_trigger(trigger.id).await.unwrap().unwrap();
        assert_eq!(reloaded.last_error.as_deref(), Some("boom"));
        assert!(reloaded.last_error_at.is_some());
    }

    #[tokio::test]
    async fn set_trigger_error_with_none_clears_it() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let trigger = repo
            .create_trigger(pipeline_id, manual_trigger())
            .await
            .unwrap();
        repo.set_trigger_error(trigger.id, Some("boom".into()))
            .await
            .unwrap();

        repo.set_trigger_error(trigger.id, None).await.unwrap();

        let reloaded = repo.get_trigger(trigger.id).await.unwrap().unwrap();
        assert_eq!(reloaded.last_error, None);
        assert_eq!(reloaded.last_error_at, None);
    }

    #[tokio::test]
    async fn set_trigger_error_on_deleted_trigger_is_a_noop() {
        let repo = test_repo().await;
        let result = repo
            .set_trigger_error(Uuid::new_v4(), Some("boom".into()))
            .await;
        assert!(result.is_ok());
    }

    // -- validate_pipeline --

    fn bare_node(node_type: CoreNodeType) -> CreateNode {
        CreateNode {
            node_type,
            action_config: None,
            transport_config: None,
            destination_id: None,
            contact_list_id: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        }
    }

    /// An `UpdateNode` that leaves every field untouched. Start from this
    /// and override just the field(s) a test cares about.
    fn bare_update_node() -> UpdateNode {
        UpdateNode {
            action_config: None,
            transport_config: None,
            destination_id: None,
            contact_list_id: None,
            condition_expr: None,
            label: None,
            pos_x: None,
            pos_y: None,
        }
    }

    #[tokio::test]
    async fn validate_pipeline_reports_all_five_categories_at_once() {
        use crate::repos::pipeline_validation::ValidationCategory;

        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();

        // Incomplete: no destination_id set yet.
        let transport = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::Transport))
            .await
            .unwrap();

        // Dangling: references a camera that existed when the node was
        // created (so recompute_refs' FK-guarded pipeline_camera_ref write
        // succeeds) but is deleted afterward. That's how this happens in
        // practice; it can't be created directly.
        let camera = camera::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set("test cam".into()),
            description: Set(None),
            rtsp_url: Set("rtsp://example/test".into()),
            sub_rtsp_url: Set(None),
            codec: Set(None),
            manufacturer: Set(None),
            model: Set(None),
            username: Set(None),
            password_enc: Set(None),
            extra_config: Set(serde_json::json!({})),
            ring_buffer_duration_secs: Set(30),
            ring_buffer_storage: Set(camera::RingBufferStorage::Memory),
            enabled: Set(true),
            created_at: Set(now()),
            updated_at: Set(now()),
            retention_days: Set(None),
            retention_disk_threshold_percent: Set(None),
            desired_recording: Set(false),
            timezone: Set(None),
            motion_detection_enabled: Set(true),
            thumbnails_enabled: Set(false),
            live_view_stream: Set(crate::entities::camera::LiveViewStream::Sub),
        }
        .insert(&repo.db)
        .await
        .unwrap();

        let action = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    action_config: Some(extract_clip_config(Some(camera.id))),
                    ..bare_node(CoreNodeType::Action)
                },
            )
            .await
            .unwrap();

        camera::Entity::delete_by_id(camera.id)
            .exec(&repo.db)
            .await
            .unwrap();

        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: transport.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();
        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: action.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();

        // Malformed + disconnected: a bad record inserted directly, bypassing
        // validate_create_shape, since the repo API can't produce a wrong-shape
        // row. This stands in for a row written before that check existed. It
        // has no edges, so it's unreachable from the root, and as an extra
        // parentless node it also trips PipelineDag::compile's root-count rule.
        let dest = destination::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set("test".into()),
            description: Set(None),
            dest_type: Set(destination::DestinationType::Local),
            config: Set(serde_json::json!({})),
            enabled: Set(true),
            created_at: Set(now()),
            updated_at: Set(now()),
        }
        .insert(&repo.db)
        .await
        .unwrap();

        pipeline_node::ActiveModel {
            id: Set(Uuid::new_v4()),
            pipeline_id: Set(pipeline_id),
            node_type: Set(node_type_to_db(&CoreNodeType::Condition)),
            action_type: Set(None),
            destination_id: Set(Some(dest.id)),
            contact_list_id: Set(None),
            config: Set(serde_json::json!({ "condition_expr": "x > 1" })),
            label: Set(None),
            pos_x: Set(None),
            pos_y: Set(None),
            created_at: Set(now()),
            unresolved_reference: Set(false),
        }
        .insert(&repo.db)
        .await
        .unwrap();

        let issues = repo.validate_pipeline(pipeline_id).await.unwrap();
        let categories: HashSet<ValidationCategory> = issues.iter().map(|i| i.category).collect();
        assert_eq!(
            categories,
            HashSet::from([
                ValidationCategory::ConfigIncomplete,
                ValidationCategory::ConfigMalformed,
                ValidationCategory::StructuralViolation,
                ValidationCategory::Disconnected,
                ValidationCategory::DanglingCameraReference,
            ])
        );
    }

    #[tokio::test]
    async fn revalidate_persists_issues_onto_the_pipeline_row() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        // No nodes at all yet, so the missing trigger_root is a structural violation.
        let issues = repo.revalidate(pipeline_id).await.unwrap();
        assert_eq!(issues.len(), 1);

        let stored = repo.get(pipeline_id).await.unwrap().unwrap();
        let stored_issues: Vec<crate::repos::pipeline_validation::ValidationIssue> =
            serde_json::from_value(stored.validation_issues).unwrap();
        assert_eq!(stored_issues, issues);
    }

    #[tokio::test]
    async fn revalidate_on_a_missing_pipeline_is_a_noop() {
        // "No-op" means no row gets written, not that issues comes back empty.
        let repo = test_repo().await;
        let missing_id = Uuid::new_v4();
        assert!(repo.revalidate(missing_id).await.is_ok());
        assert!(repo.get(missing_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_freshly_created_pipeline_has_no_stored_issues() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let stored = repo.get(pipeline_id).await.unwrap().unwrap();
        assert_eq!(stored.validation_issues, serde_json::json!([]));
    }

    async fn stored_issue_count(repo: &PipelineRepo, pipeline_id: Uuid) -> usize {
        let stored = repo.get(pipeline_id).await.unwrap().unwrap();
        let issues: Vec<crate::repos::pipeline_validation::ValidationIssue> =
            serde_json::from_value(stored.validation_issues).unwrap();
        issues.len()
    }

    async fn stored_categories(
        repo: &PipelineRepo,
        pipeline_id: Uuid,
    ) -> HashSet<crate::repos::pipeline_validation::ValidationCategory> {
        let stored = repo.get(pipeline_id).await.unwrap().unwrap();
        let issues: Vec<crate::repos::pipeline_validation::ValidationIssue> =
            serde_json::from_value(stored.validation_issues).unwrap();
        issues.into_iter().map(|i| i.category).collect()
    }

    #[tokio::test]
    async fn creating_a_node_automatically_updates_stored_validation() {
        use crate::repos::pipeline_validation::ValidationCategory;

        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        // Incomplete Transport node. repo.revalidate() is never called
        // directly, so this only passes if create_node triggers it itself.
        repo.create_node(pipeline_id, bare_node(CoreNodeType::Transport))
            .await
            .unwrap();
        assert!(stored_categories(&repo, pipeline_id)
            .await
            .contains(&ValidationCategory::ConfigIncomplete));

        // Disconnected only appears once a root exists to be unreachable
        // from, which proves the second create_node call re-ran validation too.
        repo.create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        assert!(stored_categories(&repo, pipeline_id)
            .await
            .contains(&ValidationCategory::Disconnected));
    }

    #[tokio::test]
    async fn edge_crud_automatically_updates_stored_validation() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let dest = destination::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set("test".into()),
            description: Set(None),
            dest_type: Set(destination::DestinationType::Local),
            config: Set(serde_json::json!({})),
            enabled: Set(true),
            created_at: Set(now()),
            updated_at: Set(now()),
        }
        .insert(&repo.db)
        .await
        .unwrap();

        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        let transport = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest.id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();

        // Not asserting the exact count: the disconnected node and the
        // extra parentless node each trip their own check.
        let disconnected_count = stored_issue_count(&repo, pipeline_id).await;
        assert!(disconnected_count > 0);

        let edge = repo
            .create_edge(
                pipeline_id,
                CreateEdge {
                    from_node_id: root.id,
                    to_node_id: transport.id,
                    edge_type: CoreEdgeType::Default,
                },
            )
            .await
            .unwrap();
        assert_eq!(stored_issue_count(&repo, pipeline_id).await, 0);

        repo.delete_edge(edge.id).await.unwrap();
        assert_eq!(
            stored_issue_count(&repo, pipeline_id).await,
            disconnected_count
        );
    }

    #[tokio::test]
    async fn enabling_catches_drift_since_the_last_save() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let camera = camera::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set("test cam".into()),
            description: Set(None),
            rtsp_url: Set("rtsp://example/test".into()),
            sub_rtsp_url: Set(None),
            codec: Set(None),
            manufacturer: Set(None),
            model: Set(None),
            username: Set(None),
            password_enc: Set(None),
            extra_config: Set(serde_json::json!({})),
            ring_buffer_duration_secs: Set(30),
            ring_buffer_storage: Set(camera::RingBufferStorage::Memory),
            enabled: Set(true),
            created_at: Set(now()),
            updated_at: Set(now()),
            retention_days: Set(None),
            retention_disk_threshold_percent: Set(None),
            desired_recording: Set(false),
            timezone: Set(None),
            motion_detection_enabled: Set(true),
            thumbnails_enabled: Set(false),
            live_view_stream: Set(crate::entities::camera::LiveViewStream::Sub),
        }
        .insert(&repo.db)
        .await
        .unwrap();

        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        let action = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    action_config: Some(extract_clip_config(Some(camera.id))),
                    ..bare_node(CoreNodeType::Action)
                },
            )
            .await
            .unwrap();
        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: action.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();
        assert_eq!(stored_issue_count(&repo, pipeline_id).await, 0);

        // The camera is deleted without touching this pipeline again, so its
        // stored issues stay stale until something re-checks them.
        camera::Entity::delete_by_id(camera.id)
            .exec(&repo.db)
            .await
            .unwrap();
        assert_eq!(stored_issue_count(&repo, pipeline_id).await, 0);

        let outcome = repo.set_enabled(pipeline_id, true).await.unwrap();
        assert!(matches!(outcome, EnableOutcome::BlockedByErrors(_)));
        assert_eq!(stored_issue_count(&repo, pipeline_id).await, 1);
    }

    #[tokio::test]
    async fn enabling_with_only_warnings_succeeds() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        // root -> wired is a complete, valid pipeline on its own. The second
        // action node is left unwired, so its only problem is being
        // disconnected, which is a warning, not an error.
        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        let wired = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    action_config: Some(vms_core::action::ActionConfig::Skip),
                    ..bare_node(CoreNodeType::Action)
                },
            )
            .await
            .unwrap();
        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: wired.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();
        repo.create_node(
            pipeline_id,
            CreateNode {
                action_config: Some(vms_core::action::ActionConfig::Skip),
                ..bare_node(CoreNodeType::Action)
            },
        )
        .await
        .unwrap();

        let outcome = repo.set_enabled(pipeline_id, true).await.unwrap();
        assert!(matches!(outcome, EnableOutcome::Applied));
        assert!(repo.get(pipeline_id).await.unwrap().unwrap().enabled);
    }

    #[tokio::test]
    async fn enabling_blocked_by_errors_leaves_the_flag_untouched() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        // No nodes at all: a structural-violation error.
        let outcome = repo.set_enabled(pipeline_id, true).await.unwrap();
        assert!(matches!(outcome, EnableOutcome::BlockedByErrors(_)));
        assert!(!repo.get(pipeline_id).await.unwrap().unwrap().enabled);
    }

    #[tokio::test]
    async fn disabling_always_succeeds_regardless_of_errors() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;

        let outcome = repo.set_enabled(pipeline_id, false).await.unwrap();
        assert!(matches!(outcome, EnableOutcome::Applied));
    }

    // -- source lifecycle: unlink_deleted_source / mark_source_disabled / clear_source_unresolved --

    async fn test_source(repo: &PipelineRepo, enabled: bool) -> Uuid {
        let id = Uuid::new_v4();
        source::ActiveModel {
            id: Set(id),
            name: Set("test source".into()),
            description: Set(None),
            source_type: Set(source::SourceType::Mqtt),
            config: Set(serde_json::json!({})),
            enabled: Set(enabled),
            created_at: Set(now()),
            updated_at: Set(now()),
        }
        .insert(&repo.db)
        .await
        .unwrap();
        id
    }

    fn event_trigger(source_id: Option<Uuid>) -> CreateTrigger {
        CreateTrigger {
            config: TriggerConfig::Event {
                filter: None,
                duration_secs: None,
            },
            source_id,
            camera_id: None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn creating_a_trigger_against_a_disabled_source_is_immediately_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, false).await;

        let trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();
        assert!(trigger.unresolved_reference);
    }

    #[tokio::test]
    async fn creating_a_trigger_against_an_enabled_source_is_not_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;

        let trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();
        assert!(!trigger.unresolved_reference);
    }

    #[tokio::test]
    async fn unlink_deleted_source_marks_and_unlinks_referencing_triggers() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;
        let trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();

        let affected = repo.unlink_deleted_source(source_id).await.unwrap();
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].id, trigger.id);

        let reloaded = repo.get_trigger(trigger.id).await.unwrap().unwrap();
        assert_eq!(reloaded.source_id, None);
        assert!(reloaded.unresolved_reference);
    }

    #[tokio::test]
    async fn mark_source_disabled_keeps_source_id_and_marks_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;
        let trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();

        let affected = repo.mark_source_disabled(source_id).await.unwrap();
        assert_eq!(affected.len(), 1);

        let reloaded = repo.get_trigger(trigger.id).await.unwrap().unwrap();
        assert_eq!(reloaded.source_id, Some(source_id));
        assert!(reloaded.unresolved_reference);
    }

    #[tokio::test]
    async fn clear_source_unresolved_clears_only_that_sources_triggers() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;
        let other_source_id = test_source(&repo, true).await;
        let trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();
        let other_trigger = repo
            .create_trigger(pipeline_id, event_trigger(Some(other_source_id)))
            .await
            .unwrap();
        repo.mark_source_disabled(source_id).await.unwrap();
        repo.mark_source_disabled(other_source_id).await.unwrap();

        let cleared = repo.clear_source_unresolved(source_id).await.unwrap();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].id, trigger.id);

        assert!(
            !repo
                .get_trigger(trigger.id)
                .await
                .unwrap()
                .unwrap()
                .unresolved_reference
        );
        assert!(
            repo.get_trigger(other_trigger.id)
                .await
                .unwrap()
                .unwrap()
                .unresolved_reference
        );
    }

    #[tokio::test]
    async fn set_source_unresolved_is_idempotent() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;
        repo.create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();

        assert_eq!(repo.mark_source_disabled(source_id).await.unwrap().len(), 1);
        // Already marked, so nothing changes.
        assert_eq!(repo.mark_source_disabled(source_id).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn enable_is_blocked_once_its_trigger_source_is_disabled() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let source_id = test_source(&repo, true).await;

        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        let action = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    action_config: Some(vms_core::action::ActionConfig::Skip),
                    ..bare_node(CoreNodeType::Action)
                },
            )
            .await
            .unwrap();
        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: action.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();
        repo.create_trigger(pipeline_id, event_trigger(Some(source_id)))
            .await
            .unwrap();

        // Complete and valid on its own. Confirm it enables before the source
        // is touched, then disable it again to test the actual case.
        assert!(matches!(
            repo.set_enabled(pipeline_id, true).await.unwrap(),
            EnableOutcome::Applied
        ));
        repo.set_enabled(pipeline_id, false).await.unwrap();

        repo.mark_source_disabled(source_id).await.unwrap();

        assert!(matches!(
            repo.set_enabled(pipeline_id, true).await.unwrap(),
            EnableOutcome::BlockedByErrors(_)
        ));
    }

    // -- destination lifecycle: create_node / update_node unresolved_reference --

    async fn test_destination(repo: &PipelineRepo, enabled: bool) -> Uuid {
        let id = Uuid::new_v4();
        destination::ActiveModel {
            id: Set(id),
            name: Set("test destination".into()),
            description: Set(None),
            dest_type: Set(destination::DestinationType::Local),
            config: Set(serde_json::json!({})),
            enabled: Set(enabled),
            created_at: Set(now()),
            updated_at: Set(now()),
        }
        .insert(&repo.db)
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn creating_a_transport_node_against_a_disabled_destination_is_immediately_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, false).await;

        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        assert!(node.unresolved_reference);
    }

    #[tokio::test]
    async fn creating_a_transport_node_against_an_enabled_destination_is_not_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;

        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        assert!(!node.unresolved_reference);
    }

    #[tokio::test]
    async fn repointing_a_node_to_a_disabled_destination_marks_it_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let enabled_dest = test_destination(&repo, true).await;
        let disabled_dest = test_destination(&repo, false).await;

        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(enabled_dest),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        assert!(!node.unresolved_reference);

        let updated = repo
            .update_node(
                node.id,
                UpdateNode {
                    destination_id: Some(Some(disabled_dest)),
                    ..bare_update_node()
                },
            )
            .await
            .unwrap();
        assert!(updated.unresolved_reference);
    }

    #[tokio::test]
    async fn clearing_a_nodes_destination_id_clears_unresolved_reference() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let disabled_dest = test_destination(&repo, false).await;

        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(disabled_dest),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        assert!(node.unresolved_reference);

        let updated = repo
            .update_node(
                node.id,
                UpdateNode {
                    destination_id: Some(None),
                    ..bare_update_node()
                },
            )
            .await
            .unwrap();
        assert!(!updated.unresolved_reference);
    }

    #[tokio::test]
    async fn updating_unrelated_fields_leaves_unresolved_reference_untouched() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let disabled_dest = test_destination(&repo, false).await;

        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(disabled_dest),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        assert!(node.unresolved_reference);

        let updated = repo
            .update_node(
                node.id,
                UpdateNode {
                    label: Some(Some("renamed".into())),
                    ..bare_update_node()
                },
            )
            .await
            .unwrap();
        assert!(updated.unresolved_reference);
    }

    #[tokio::test]
    async fn unlink_deleted_destination_marks_and_unlinks_referencing_nodes() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;
        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();

        let affected = repo.unlink_deleted_destination(dest_id).await.unwrap();
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].id, node.id);

        let reloaded = repo.get_node(node.id).await.unwrap().unwrap();
        assert_eq!(reloaded.destination_id, None);
        assert!(reloaded.unresolved_reference);
    }

    #[tokio::test]
    async fn mark_destination_disabled_keeps_destination_id_and_marks_unresolved() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;
        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();

        let affected = repo.mark_destination_disabled(dest_id).await.unwrap();
        assert_eq!(affected.len(), 1);

        let reloaded = repo.get_node(node.id).await.unwrap().unwrap();
        assert_eq!(reloaded.destination_id, Some(dest_id));
        assert!(reloaded.unresolved_reference);
    }

    #[tokio::test]
    async fn clear_destination_unresolved_clears_only_that_destinations_nodes() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;
        let other_dest_id = test_destination(&repo, true).await;
        let node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        let other_node = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(other_dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        repo.mark_destination_disabled(dest_id).await.unwrap();
        repo.mark_destination_disabled(other_dest_id).await.unwrap();

        let cleared = repo.clear_destination_unresolved(dest_id).await.unwrap();
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].id, node.id);

        assert!(
            !repo
                .get_node(node.id)
                .await
                .unwrap()
                .unwrap()
                .unresolved_reference
        );
        assert!(
            repo.get_node(other_node.id)
                .await
                .unwrap()
                .unwrap()
                .unresolved_reference
        );
    }

    #[tokio::test]
    async fn set_destination_unresolved_is_idempotent() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;
        repo.create_node(
            pipeline_id,
            CreateNode {
                destination_id: Some(dest_id),
                ..bare_node(CoreNodeType::Transport)
            },
        )
        .await
        .unwrap();

        assert_eq!(
            repo.mark_destination_disabled(dest_id).await.unwrap().len(),
            1
        );
        // Already marked, so nothing changes.
        assert_eq!(
            repo.mark_destination_disabled(dest_id).await.unwrap().len(),
            0
        );
    }

    #[tokio::test]
    async fn enable_is_blocked_once_its_transport_nodes_destination_is_disabled() {
        let repo = test_repo().await;
        let pipeline_id = make_pipeline(&repo).await;
        let dest_id = test_destination(&repo, true).await;

        let root = repo
            .create_node(pipeline_id, bare_node(CoreNodeType::TriggerRoot))
            .await
            .unwrap();
        let transport = repo
            .create_node(
                pipeline_id,
                CreateNode {
                    destination_id: Some(dest_id),
                    ..bare_node(CoreNodeType::Transport)
                },
            )
            .await
            .unwrap();
        repo.create_edge(
            pipeline_id,
            CreateEdge {
                from_node_id: root.id,
                to_node_id: transport.id,
                edge_type: CoreEdgeType::Default,
            },
        )
        .await
        .unwrap();

        // Complete and valid on its own. Confirm it enables before the
        // destination is touched, then disable it again to test the actual case.
        assert!(matches!(
            repo.set_enabled(pipeline_id, true).await.unwrap(),
            EnableOutcome::Applied
        ));
        repo.set_enabled(pipeline_id, false).await.unwrap();

        repo.mark_destination_disabled(dest_id).await.unwrap();

        assert!(matches!(
            repo.set_enabled(pipeline_id, true).await.unwrap(),
            EnableOutcome::BlockedByErrors(_)
        ));
    }
}
