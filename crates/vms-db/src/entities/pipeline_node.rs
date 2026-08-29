use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    #[sea_orm(string_value = "trigger_root")]
    TriggerRoot,
    #[sea_orm(string_value = "action")]
    Action,
    #[sea_orm(string_value = "device_control")]
    DeviceControl,
    #[sea_orm(string_value = "transport")]
    Transport,
    #[sea_orm(string_value = "fork")]
    Fork,
    #[sea_orm(string_value = "condition")]
    Condition,
}

/// Discriminator for `Action` and `DeviceControl` nodes; mirrors the `action_type`
/// CHECK constraint. `None` for non-action node types.
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    #[sea_orm(string_value = "transcode")]
    Transcode,
    #[sea_orm(string_value = "extract_clip")]
    ExtractClip,
    #[sea_orm(string_value = "snapshot")]
    Snapshot,
    #[sea_orm(string_value = "merge_clips")]
    MergeClips,
    #[sea_orm(string_value = "compress")]
    Compress,
    #[sea_orm(string_value = "encrypt")]
    Encrypt,
    #[sea_orm(string_value = "watermark")]
    Watermark,
    #[sea_orm(string_value = "render_notification")]
    RenderNotification,
    #[sea_orm(string_value = "delay")]
    Delay,
    #[sea_orm(string_value = "ptz_move")]
    PtzMove,
    #[sea_orm(string_value = "start_recording")]
    StartRecording,
    #[sea_orm(string_value = "stop_recording")]
    StopRecording,
    #[sea_orm(string_value = "set_stream_quality")]
    SetStreamQuality,
    #[sea_orm(string_value = "trigger_alarm_output")]
    TriggerAlarmOutput,
    #[sea_orm(string_value = "skip")]
    Skip,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_nodes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub node_type: NodeType,
    /// Only populated for `Action` and `DeviceControl` nodes.
    pub action_type: Option<ActionType>,
    /// FK to destinations; required for `Transport` nodes.
    pub destination_id: Option<Uuid>,
    /// FK to contact_lists; optional for messaging transport nodes.
    pub contact_list_id: Option<Uuid>,
    /// Action or transport configuration serialised as JSONB.
    /// For `Action` nodes this deserialises to `ActionConfig`.
    /// For `Transport` nodes this deserialises to `TransportConfig`.
    /// For `Condition` nodes this contains `{ "condition_expr": "..." }`.
    pub config: Json,
    /// Human-readable label shown in the pipeline editor UI.
    pub label: Option<String>,
    /// Horizontal canvas position (UI only, ignored at runtime).
    pub pos_x: Option<f64>,
    /// Vertical canvas position (UI only, ignored at runtime).
    pub pos_y: Option<f64>,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipeline::Entity",
        from = "Column::PipelineId",
        to = "super::pipeline::Column::Id"
    )]
    Pipeline,
    #[sea_orm(
        belongs_to = "super::destination::Entity",
        from = "Column::DestinationId",
        to = "super::destination::Column::Id"
    )]
    Destination,
    #[sea_orm(
        belongs_to = "super::contact_list::Entity",
        from = "Column::ContactListId",
        to = "super::contact_list::Column::Id"
    )]
    ContactList,
    #[sea_orm(has_many = "super::run_node_result::Entity")]
    RunNodeResult,
}

impl Related<super::pipeline::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl Related<super::destination::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Destination.def()
    }
}

impl Related<super::contact_list::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ContactList.def()
    }
}

impl Related<super::run_node_result::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RunNodeResult.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
