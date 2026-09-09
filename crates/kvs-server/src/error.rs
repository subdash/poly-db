use axum::{Json, http::StatusCode, response::IntoResponse};
use kvs_engine::EngineError;
use serde_json::json;

pub enum AppError {
    Engine(EngineError),
    BadRequest(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            AppError::Engine(engine_error) => match engine_error {
                EngineError::Io(error) => {
                    tracing::error!(error = %error, "io error while serving request");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "internal error".to_string(),
                    )
                }
                EngineError::Corrupt { offset } => {
                    tracing::error!(offset, "corrupt record");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "internal error".to_string(),
                    )
                }
                EngineError::KeyNotFound => (
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "key not found".to_string(),
                ),
                EngineError::KeyTooLarge { len } => (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    format!("key too large: {len} bytes"),
                ),
                EngineError::ValueTooLarge { len } => (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    format!("value too large: {len} bytes"),
                ),
                EngineError::ShuttingDown => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    "shutting down".to_string(),
                ),
                EngineError::Encode(error) => {
                    tracing::error!(error = %error, "encoding error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "internal error".to_string(),
                    )
                }
            },
            AppError::BadRequest(error) => {
                tracing::debug!(error = %error, "bad request");
                (StatusCode::BAD_REQUEST, "bad_request", error)
            }
        };
        (
            status,
            Json(json!({"error": { "code": code, "message": message}})),
        )
            .into_response()
    }
}

impl From<EngineError> for AppError {
    fn from(value: EngineError) -> Self {
        AppError::Engine(value)
    }
}
