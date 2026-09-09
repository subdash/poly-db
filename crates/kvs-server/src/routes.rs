use axum::{
    Json,
    extract::{DefaultBodyLimit, Path, State},
    routing::{Router, get},
};
use kvs_engine::MAX_VALUE_BYTES;
use serde_json::json;

use crate::{dto::ValueResponse, error::AppError, writer::KvHandle};

pub fn router(handle: KvHandle) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/kv/{key}", get(get_key))
        .with_state(handle)
        .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES + 8 * 1024))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn get_key(
    State(handle): State<KvHandle>,
    Path(key): Path<String>,
) -> Result<Json<ValueResponse>, AppError> {
    let value = handle.get(key.clone()).await?;

    Ok(Json(ValueResponse { key, value }))
}
