use kvs_engine::Engine;
use kvs_server::{routes, writer};
use tempfile::TempDir;

#[tokio::test]
async fn the_service_answers_over_a_real_socket() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, _join) = writer::spawn(engine, 64);
    let router = routes::router(handle);

    // Port 0 asks the OS for any free port, so tests never collide.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });

    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let response = client
        .put(format!("{base}/v1/kv/alpha"))
        .json(&serde_json::json!({ "value": "one" }))
        .send()
        .await
        .expect("put");
    assert_eq!(response.status().as_u16(), 204);

    let response = client
        .get(format!("{base}/v1/kv/alpha"))
        .send()
        .await
        .expect("get");
    assert_eq!(response.status().as_u16(), 200);
    let json: serde_json::Value = response.json().await.expect("json");
    assert_eq!(json["key"], "alpha");
    assert_eq!(json["value"], "one");

    let response = client
        .delete(format!("{base}/v1/kv/alpha"))
        .send()
        .await
        .expect("delete");
    assert_eq!(response.status().as_u16(), 204);

    let response = client
        .get(format!("{base}/v1/kv/alpha"))
        .send()
        .await
        .expect("get after delete");
    assert_eq!(response.status().as_u16(), 404);
}
