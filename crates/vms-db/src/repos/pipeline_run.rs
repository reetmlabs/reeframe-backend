use sea_orm::{ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use uuid::Uuid;
use vms_core::VmsError;

use crate::entities::{
    pipeline_run::{self, ActiveModel as RunActiveModel, RunStatus},
    run_node_result::{self, ActiveModel as NodeActiveModel, NodeResultStatus},
};

use super::{db_err, now};

// -- Repository ----------------------------------------------------------------

#[derive(Clone)]
pub struct PipelineRunRepo {
    db: DatabaseConnection,
}

impl PipelineRunRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    // -- Run lifecycle ---------------------------------------------------------

    /// Insert a new `pipeline_runs` row in `Running` state.
    ///
    /// `trigger_context` is the serialised [`TriggerContext`] snapshot — callers
    /// use `serde_json::to_value(&ctx)` before calling this method.
    pub async fn create_run(
        &self,
        pipeline_id: Uuid,
        trigger_id: Option<Uuid>,
        trigger_context: serde_json::Value,
    ) -> Result<pipeline_run::Model, VmsError> {
        RunActiveModel {
            id: Set(Uuid::new_v4()),
            pipeline_id: Set(pipeline_id),
            trigger_id: Set(trigger_id),
            triggered_at: Set(now()),
            trigger_context: Set(trigger_context),
            status: Set(RunStatus::Running),
            completed_at: Set(None),
            error: Set(None),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    /// Transition a run to a terminal state (`Completed`, `Failed`, or
    /// `Cancelled`).  Sets `completed_at` to the current timestamp.
    pub async fn finish_run(
        &self,
        run_id: Uuid,
        status: RunStatus,
        error: Option<String>,
    ) -> Result<(), VmsError> {
        RunActiveModel {
            id: Set(run_id),
            status: Set(status),
            completed_at: Set(Some(now())),
            error: Set(error),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // -- Node result lifecycle -------------------------------------------------

    /// Insert a `run_node_results` row in `Pending` state.
    ///
    /// Called once per DAG node before execution begins so every node is
    /// represented in the audit trail even if the process crashes mid-run.
    pub async fn create_node_result(
        &self,
        run_id: Uuid,
        node_id: Uuid,
    ) -> Result<run_node_result::Model, VmsError> {
        NodeActiveModel {
            id: Set(Uuid::new_v4()),
            run_id: Set(run_id),
            node_id: Set(node_id),
            status: Set(NodeResultStatus::Pending),
            started_at: Set(None),
            completed_at: Set(None),
            output: Set(serde_json::Value::Null),
            error: Set(None),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    /// Transition a node result to `Running` and stamp `started_at`.
    pub async fn start_node_result(&self, id: Uuid) -> Result<(), VmsError> {
        NodeActiveModel {
            id: Set(id),
            status: Set(NodeResultStatus::Running),
            started_at: Set(Some(now())),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Transition a node result to a terminal state (`Completed`, `Failed`, or
    /// `Skipped`).  Sets `completed_at`, stores the output JSON, and records
    /// any error message.
    pub async fn finish_node_result(
        &self,
        id: Uuid,
        status: NodeResultStatus,
        output: serde_json::Value,
        error: Option<String>,
    ) -> Result<(), VmsError> {
        NodeActiveModel {
            id: Set(id),
            status: Set(status),
            completed_at: Set(Some(now())),
            output: Set(output),
            error: Set(error),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // -- Queries ---------------------------------------------------------------

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<pipeline_run::Model>, VmsError> {
        pipeline_run::Entity::find_by_id(run_id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    /// Most-recent runs for a pipeline, newest first.
    pub async fn list_runs_for_pipeline(
        &self,
        pipeline_id: Uuid,
        limit: u64,
    ) -> Result<Vec<pipeline_run::Model>, VmsError> {
        pipeline_run::Entity::find()
            .filter(pipeline_run::Column::PipelineId.eq(pipeline_id))
            .order_by_desc(pipeline_run::Column::TriggeredAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn list_node_results_for_run(
        &self,
        run_id: Uuid,
    ) -> Result<Vec<run_node_result::Model>, VmsError> {
        run_node_result::Entity::find()
            .filter(run_node_result::Column::RunId.eq(run_id))
            .all(&self.db)
            .await
            .map_err(db_err)
    }
}
