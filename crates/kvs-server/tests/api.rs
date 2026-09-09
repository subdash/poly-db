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
