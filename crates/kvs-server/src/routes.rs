use axum::{
    Json,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{Router, get},
};
use kvs_engine::{EngineError, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use serde_json::json;

use crate::{
    dto::{SetRequest, ValueResponse},
    error::AppError,
    writer::KvHandle,
};

pub fn router(handle: KvHandle) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/kv/{key}", get(get_key).put(put_key).delete(delete_key))
        // Routes must be registered before .layer(), or the layer won't apply to them
        .with_state(handle)
        .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES + 8 * 1024))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready() -> Json<serde_json::Value> {
    Json(json!({ "status": "ready" }))
}

async fn get_key(
    State(handle): State<KvHandle>,
    Path(key): Path<String>,
) -> Result<Json<ValueResponse>, AppError> {
    let value = handle.get(key.clone()).await?;

    Ok(Json(ValueResponse { key, value }))
}

async fn put_key(
    State(handle): State<KvHandle>,
    Path(key): Path<String>,
    payload: Result<Json<SetRequest>, JsonRejection>,
) -> Result<StatusCode, AppError> {
    check_key(&key)?;

    match payload {
        Ok(Json(payload)) => {
            let value = payload.value;

            let val_len = value.len();
            if val_len > MAX_VALUE_BYTES {
                return Err(AppError::Engine(EngineError::ValueTooLarge {
                    len: val_len,
                }));
            }

            handle.set(key, value).await?;

            Ok(StatusCode::NO_CONTENT)
        }
        Err(rejection) => Err(AppError::BadRequest(rejection.body_text())),
    }
}

async fn delete_key(
    State(handle): State<KvHandle>,
    Path(key): Path<String>,
) -> Result<StatusCode, AppError> {
    check_key(&key)?;

    handle.remove(key).await?;

    Ok(StatusCode::NO_CONTENT)
}

fn check_key(key: &str) -> Result<(), AppError> {
    // Validate key length in the handler so it is rejected at the HTTP layer rather than
    // taking up space in the bounded channel.
    let key_len = key.len();
    if key_len > MAX_KEY_BYTES {
        Err(AppError::Engine(EngineError::KeyTooLarge { len: key_len }))
    } else {
        Ok(())
    }
}
