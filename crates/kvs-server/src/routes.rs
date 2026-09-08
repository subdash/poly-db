use axum::{
    Json,
    extract::DefaultBodyLimit,
    routing::{Router, get},
};
use kvs_engine::MAX_VALUE_BYTES;
use serde_json::json;

use crate::writer::KvHandle;

pub fn router(handle: KvHandle) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .with_state(handle)
        .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES + 8 * 1024))
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}
