use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use kvs_engine::Engine;
use kvs_server::{routes, writer};
use tempfile::TempDir;
use tower::ServiceExt; // brings `oneshot` into scope

struct TestApp {
    _dir: TempDir,
    router: Router,
}

fn test_app() -> TestApp {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    // The writer thread outlives the test; the process exit reaps it.
    let (handle, _join) = writer::spawn(engine, 64);
    let router = routes::router(handle);
    TestApp { _dir: dir, router }
}

async fn send(app: &TestApp, request: Request<Body>) -> (StatusCode, Bytes) {
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("the router must always produce a response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("read body");
    (status, body)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

#[tokio::test]
async fn health_reports_ok() {
    let app = test_app();
    let (status, body) = send(&app, get("/health")).await;

    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn an_unknown_route_is_404() {
    let app = test_app();
    let (status, _) = send(&app, get("/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_router_is_cloneable_for_concurrent_requests() {
    let app = test_app();
    let (first, second) = tokio::join!(send(&app, get("/health")), send(&app, get("/health")));
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
}

fn error_code(body: &Bytes) -> String {
    let json: serde_json::Value = serde_json::from_slice(body).expect("json body");
    json["error"]["code"]
        .as_str()
        .expect("error.code must be a string")
        .to_string()
}

#[tokio::test]
async fn get_on_an_unknown_key_is_404_with_the_standard_envelope() {
    let app = test_app();
    let (status, body) = send(&app, get("/v1/kv/ghost")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");

    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert!(
        json["error"]["message"].is_string(),
        "every error carries a human-readable message"
    );
}

#[tokio::test]
async fn a_key_containing_percent_encoding_is_decoded() {
    let app = test_app();
    // The handler must receive "a b", not "a%20b".
    let (status, body) = send(&app, get("/v1/kv/a%20b")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
}

fn put(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .expect("request")
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let app = test_app();

    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":"one"}"#)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(&app, get("/v1/kv/alpha")).await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["key"], "alpha");
    assert_eq!(json["value"], "one");
}

#[tokio::test]
async fn put_is_idempotent_and_overwrites() {
    let app = test_app();

    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":"one"}"#)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":"two"}"#)).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "an update is not a
  different status"
    );

    let (_, body) = send(&app, get("/v1/kv/alpha")).await;
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["value"], "two");
}

#[tokio::test]
async fn put_accepts_an_empty_value() {
    let app = test_app();
    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":""}"#)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(&app, get("/v1/kv/alpha")).await;
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["value"], "");
}

#[tokio::test]
async fn put_rejects_a_malformed_body_with_the_standard_envelope() {
    let app = test_app();
    let (status, body) = send(
        &app,
        put(
            "/v1/kv/alpha",
            "not json at
  all",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "bad_request");
}

#[tokio::test]
async fn put_rejects_a_body_missing_the_value_field() {
    let app = test_app();
    let (status, body) = send(&app, put("/v1/kv/alpha", r#"{"vlaue":"typo"}"#)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "bad_request");
}

#[tokio::test]
async fn put_rejects_an_oversized_value_with_the_standard_envelope() {
    let app = test_app();
    let value = "v".repeat(kvs_engine::MAX_VALUE_BYTES + 1);
    let body = serde_json::to_string(&serde_json::json!({ "value": value
    }))
    .expect("body");

    let (status, body) = send(&app, put("/v1/kv/alpha", &body)).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        error_code(&body),
        "payload_too_large",
        "the body limit must leave headroom so our own check produces this
  response"
    );
}

#[tokio::test]
async fn put_rejects_an_oversized_key() {
    let app = test_app();
    let key = "k".repeat(kvs_engine::MAX_KEY_BYTES + 1);
    let (status, body) = send(&app, put(&format!("/v1/kv/{key}"), r#"{"value":"one"}"#)).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error_code(&body), "payload_too_large");
}

use kvs_server::writer::KvHandle;

fn delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

#[tokio::test]
async fn delete_removes_a_key() {
    let app = test_app();

    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":"one"}"#)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(&app, delete("/v1/kv/alpha")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = send(&app, get("/v1/kv/alpha")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
}

#[tokio::test]
async fn delete_on_an_unknown_key_is_404() {
    let app = test_app();
    let (status, body) = send(&app, delete("/v1/kv/ghost")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "not_found");
}

#[tokio::test]
async fn a_key_can_be_written_again_after_deletion() {
    let app = test_app();

    send(&app, put("/v1/kv/alpha", r#"{"value":"one"}"#)).await;
    send(&app, delete("/v1/kv/alpha")).await;
    let (status, _) = send(&app, put("/v1/kv/alpha", r#"{"value":"two"}"#)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(&app, get("/v1/kv/alpha")).await;
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["value"], "two");
}

#[tokio::test]
async fn writes_are_503_when_the_writer_is_gone_but_reads_still_work() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let reader = engine.reader();

    let (tx, rx) = writer::channel(16);
    drop(rx); // the writer thread is gone
    let app = TestApp {
        _dir: dir,
        router: routes::router(KvHandle::new(tx, reader)),
    };

    let (status, body) = send(&app, put("/v1/kv/alpha", r#"{"value":"one"}"#)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error_code(&body), "unavailable");

    let (status, body) = send(&app, delete("/v1/kv/alpha")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error_code(&body), "unavailable");

    // Reads bypass the writer entirely, so the store still serves what it has.
    let (status, _) = send(&app, get("/v1/kv/alpha")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a read must answer, not
  fail"
    );

    let (status, _) = send(&app, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
}
