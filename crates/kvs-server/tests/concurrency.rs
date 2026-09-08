use kvs_engine::Engine;
use kvs_server::writer;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_do_not_lose_updates() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 50;

    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, join) = writer::spawn(engine, 64);

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                handle
                    .set(format!("key-{w}-{i}"), format!("value-{w}-{i}"))
                    .await
                    .expect("set");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer task must not panic");
    }

    for w in 0..WRITERS {
        for i in 0..PER_WRITER {
            assert_eq!(
                handle.get(format!("key-{w}-{i}")).await.expect("get"),
                format!("value-{w}-{i}"),
                "every acknowledged write must be readable"
            );
        }
    }

    drop(handle);
    join.join().expect("writer thread");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sequential_reader_never_observes_a_version_going_backwards() {
    const ROUNDS: usize = 300;

    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, join) = writer::spawn(engine, 64);

    handle.set("hot".into(), "v-0".into()).await.expect("seed");

    let writer_task = {
        let handle = handle.clone();
        tokio::spawn(async move {
            for round in 1..=ROUNDS {
                handle
                    .set("hot".into(), format!("v-{round}"))
                    .await
                    .expect("set");
            }
        })
    };

    let reader_task = {
        let handle = handle.clone();
        tokio::spawn(async move {
            let mut previous = 0usize;
            for _ in 0..ROUNDS {
                let observed = handle.get("hot".into()).await.expect(
                    "a read concurrent with writes must never
  fail",
                );
                let round: usize = observed
                    .strip_prefix("v-")
                    .expect("a read must never observe a partial value")
                    .parse()
                    .expect("a read must never observe a partial value");
                assert!(
                    round >= previous,
                    "a sequential reader saw {round} after {previous}"
                );
                previous = round;
            }
        })
    };

    writer_task.await.expect("writer task");
    reader_task.await.expect("reader task");

    assert_eq!(
        handle.get("hot".into()).await.expect("get"),
        format!("v-{ROUNDS}"),
        "the last write must win"
    );

    drop(handle);
    join.join().expect("writer thread");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_readers_run_against_a_live_writer() {
    const READERS: usize = 16;
    const ROUNDS: usize = 100;

    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    let (handle, join) = writer::spawn(engine, 64);

    handle.set("hot".into(), "v-0".into()).await.expect("seed");

    let writer_task = {
        let handle = handle.clone();
        tokio::spawn(async move {
            for round in 1..=ROUNDS {
                handle
                    .set("hot".into(), format!("v-{round}"))
                    .await
                    .expect("set");
            }
        })
    };

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let handle = handle.clone();
            tokio::spawn(async move {
                for _ in 0..ROUNDS {
                    let observed = handle.get("hot".into()).await.expect("get");
                    assert!(
                        observed.starts_with("v-"),
                        "reads must never observe garbage, saw
  {observed:?}"
                    );
                }
            })
        })
        .collect();

    writer_task.await.expect("writer task");
    for reader in readers {
        reader.await.expect("reader task");
    }

    drop(handle);
    join.join().expect("writer thread");
}
