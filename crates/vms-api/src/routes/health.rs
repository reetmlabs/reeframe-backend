use salvo::prelude::*;

#[handler]
pub async fn health(res: &mut Response) {
    res.render(Json(serde_json::json!({"status": "ok"})));
}
