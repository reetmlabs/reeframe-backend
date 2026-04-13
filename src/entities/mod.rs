pub mod feed;
pub mod settings;
pub mod recording_segment;

use sea_orm::{ConnectionTrait, DatabaseConnection, Schema};

/// Create all application database tables if they do not already exist.
///
/// This is called once at startup before any handlers run.  All statements use
/// `IF NOT EXISTS` so they are safe to call on an already-populated database.
///
/// # Arguments
/// * `db` — Active SeaORM database connection.
///
/// # Panics
/// Panics with a descriptive message if any table cannot be created.
pub async fn create_tables(db: &DatabaseConnection) {
    let builder = db.get_database_backend();
    let schema = Schema::new(builder);

    let tables: &[(&str, sea_orm::Statement)] = &[
        ("feeds", builder.build(schema.create_table_from_entity(feed::Entity).if_not_exists())),
        ("settings", builder.build(schema.create_table_from_entity(settings::Entity).if_not_exists())),
        ("recording_segments", builder.build(schema.create_table_from_entity(recording_segment::Entity).if_not_exists())),
    ];

    for (name, stmt) in tables {
        db.execute_unprepared(stmt.to_string().as_str())
            .await
            .unwrap_or_else(|e| panic!("Failed to create {} table: {}", name, e));
    }
}
