use salvo::prelude::*;
use sea_orm_migration::MigratorTrait;
use vms_db::Migrator;

use crate::state::AppState;

/// GET /health — liveness. Only proves the HTTP server itself is up; no DB
/// or downstream checks, so it never blocks behind a slow database.
#[handler]
pub async fn health(res: &mut Response) {
    res.render(Json(serde_json::json!({"status": "ok"})));
}

/// GET /health/ready — readiness. Additionally checks the DB connection is
/// alive and no migrations are pending, returning `503` until both are true.
///
/// In practice the daemon always applies pending migrations at startup
/// before it starts serving requests (see `main.rs`), so the pending-check
/// here mostly guards against someone pointing two daemon versions at the
/// same database rather than a normal day-2 failure mode — but it's cheap
/// and matches what the endpoint's name promises.
#[handler]
pub async fn ready(depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");

    if let Err(e) = state.db.ping().await {
        return unavailable(res, format!("database unreachable: {e}"));
    }

    match Migrator::get_pending_migrations(&state.db).await {
        Ok(pending) if pending.is_empty() => {
            res.render(Json(serde_json::json!({"status": "ready"})));
        }
        Ok(pending) => unavailable(res, format!("{} pending migration(s)", pending.len())),
        Err(e) => unavailable(res, format!("failed to check migrations: {e}")),
    }
}

fn unavailable(res: &mut Response, reason: String) {
    res.status_code(StatusCode::SERVICE_UNAVAILABLE);
    res.render(Json(
        serde_json::json!({"status": "unavailable", "reason": reason}),
    ));
}
