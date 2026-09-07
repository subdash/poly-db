use kvs_engine::{Engine, EngineError};
use kvs_server::writer::{self, KvHandle};
use tempfile::TempDir;

#[tokio::test]
async fn a_write_through_the_handle_is_visible_to_a_read() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, join) = writer::spawn(engine, 16);

    handle.set("alpha".into(), "one".into()).await.expect("set");
    assert_eq!(handle.get("alpha".into()).await.expect("get"), "one");

    handle.remove("alpha".into()).await.expect("remove");
    assert!(matches!(
        handle.get("alpha".into()).await,
        Err(EngineError::KeyNotFound)
    ));

    drop(handle);
    join.join().expect("the writer thread must exit cleanly");
}

#[tokio::test]
async fn writes_are_durable_once_the_writer_thread_has_stopped() {
    let dir = TempDir::new().expect("tempdir");
    {
        let engine = Engine::open(dir.path()).expect("open");
        let (handle, join) = writer::spawn(engine, 16);
        handle.set("alpha".into(), "one".into()).await.expect("set");
        drop(handle);
        join.join().expect("writer thread");
    }

    let engine = Engine::open(dir.path()).expect("reopen");
    assert_eq!(engine.get("alpha").expect("get"), "one");
}

#[tokio::test]
async fn errors_from_the_engine_reach_the_caller() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, join) = writer::spawn(engine, 16);

    let err = handle
        .remove("ghost".into())
        .await
        .expect_err("removing an unknown key must fail");
    assert!(matches!(err, EngineError::KeyNotFound));

    let err = handle
        .set("k".repeat(kvs_engine::MAX_KEY_BYTES + 1), "one".into())
        .await
        .expect_err("an oversized key must fail");
    assert!(matches!(err, EngineError::KeyTooLarge { .. }));

    drop(handle);
    join.join()
        .expect("the writer thread must survive rejected writes");
}

#[tokio::test]
async fn a_handle_whose_writer_is_gone_reports_shutting_down() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let reader = engine.reader();

    let (tx, rx) = writer::channel(16);
    drop(rx); // nothing will ever service this channel
    let handle = KvHandle::new(tx, reader);

    let err = handle
        .set("alpha".into(), "one".into())
        .await
        .expect_err("a write with no writer must fail");
    assert!(matches!(err, EngineError::ShuttingDown));

    // Reads do not go through the channel, so they still work.
    assert!(matches!(
        handle.get("alpha".into()).await,
        Err(EngineError::KeyNotFound)
    ));
}
