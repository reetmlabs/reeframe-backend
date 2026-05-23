use salvo::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

/// API-level error: an HTTP status code plus a JSON `{"error": "..."}` body.
///
/// Implements `Writer` so it can be used as the `Err` arm of a handler's
/// `Result` return type directly — Salvo renders it without any extra wiring.
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
}

#[async_trait]
impl Writer for ApiError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        res.status_code(self.status);
        res.render(Json(serde_json::json!({"error": self.message})));
    }
}

impl From<VmsError> for ApiError {
    fn from(e: VmsError) -> Self {
        let status = match &e {
            VmsError::CameraNotFound(_)
            | VmsError::SourceNotFound(_)
            | VmsError::DestinationNotFound(_)
            | VmsError::PipelineNotFound(_)
            | VmsError::NotFound(_) => StatusCode::NOT_FOUND,

            VmsError::Unauthorized(_) => StatusCode::UNAUTHORIZED,

            VmsError::DagValidation(_) | VmsError::ExpressionEval(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }

            VmsError::Serialization(_) => StatusCode::BAD_REQUEST,

            VmsError::Transport { .. } => StatusCode::BAD_GATEWAY,

            VmsError::ActorDied => StatusCode::SERVICE_UNAVAILABLE,

            VmsError::Database(_)
            | VmsError::Media(_)
            | VmsError::Encryption(_)
            | VmsError::Config(_)
            | VmsError::Io(_)
            | VmsError::Template(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %e, "internal server error");
        }

        Self {
            status,
            message: e.to_string(),
        }
    }
}

// -- Shared handler helpers ----------------------------------------------------

/// Extract and parse the `{id}` path parameter as a UUID.
pub(crate) fn parse_id(req: &mut Request) -> Result<Uuid, ApiError> {
    let s: String = req.param("id").unwrap_or_default();
    s.parse::<Uuid>()
        .map_err(|_| ApiError::bad_request("invalid id: expected UUID"))
}

/// Deserialize the request body as JSON, returning a 400 on parse failure.
pub(crate) async fn parse_body<T: serde::de::DeserializeOwned>(
    req: &mut Request,
) -> Result<T, ApiError> {
    req.parse_json::<T>()
        .await
        .map_err(|e| ApiError::bad_request(e.to_string()))
}
