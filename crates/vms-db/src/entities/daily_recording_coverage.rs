use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "daily_recording_coverage")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub camera_id: Uuid,
    /// Calendar day this row summarizes, in this camera's own configured
    /// timezone, not the viewer's (see `coverage.rs`'s module doc for why).
    pub day: Date,
    /// Sum of merged session spans for `day`. An in-progress session contributes 0
    /// until it closes, matching `RecordingModel::dailySummaries` in the Qt frontend.
    pub coverage_seconds: i64,
    /// `[{start, end, chunk_count, size_bytes}]`. `end: null` marks the session
    /// that's still being recorded, if any.
    pub session_ranges: Json,
    pub chunk_count: i32,
    /// `None` if any contributing chunk's size is still unknown.
    pub total_size_bytes: Option<i64>,
    /// `true` once `day` is fully in the past and will never be recomputed
    /// again, other than by the retention-purge hook.
    pub is_finalized: bool,
    /// `true` once retention has deleted every chunk `day` had. The row is kept,
    /// zeroed, so the UI can tell "recorded, then purged" apart from "never recorded".
    pub purged_by_retention: bool,
    pub computed_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::camera::Entity",
        from = "Column::CameraId",
        to = "super::camera::Column::Id"
    )]
    Camera,
}

impl Related<super::camera::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Camera.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
