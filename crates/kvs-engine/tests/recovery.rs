use std::{fs::OpenOptions, io::Write};

use kvs_engine::{Engine, EngineError};
use tempfile::TempDir;

#[test]
fn state_survives_a_reopen() {
    let dir = TempDir::new().expect("tempdir");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set alpha");
        engine.set("beta".into(), "two".into()).expect("set beta");
        engine
            .set("alpha".into(), "three".into())
            .expect("overwrite alpha");
        engine
            .set("gamma".into(), "four".into())
            .expect("set gamma");
        engine.remove("gamma").expect("remove gamma");
    } // dropped: the writer must have flushed

    let engine = Engine::open(dir.path()).expect("reopen");
    assert_eq!(
        engine.get("alpha").expect("get"),
        "three",
        "the last write wins"
    );
    assert_eq!(engine.get("beta").expect("get"), "two");
    assert!(
        matches!(engine.get("gamma"), Err(EngineError::KeyNotFound)),
        "a tombstone must survive replay"
    );
}

#[test]
fn writes_continue_after_a_reopen() {
    let dir = TempDir::new().expect("tempdir");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
    }

    {
        let mut engine = Engine::open(dir.path()).expect("reopen");
        engine
            .set("beta".into(), "two".into())
            .expect("set after reopen");
    }

    let engine = Engine::open(dir.path()).expect("third open");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(engine.get("beta").expect("get"), "two");
}

#[test]
fn opening_an_empty_directory_yields_an_empty_store() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    assert!(matches!(
        engine.get("anything"),
        Err(EngineError::KeyNotFound)
    ));
}

#[test]
fn a_maximal_record_survives_a_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let key = "k".repeat(kvs_engine::MAX_KEY_BYTES);
    let value = "v".repeat(kvs_engine::MAX_VALUE_BYTES);

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine
            .set(key.clone(), value.clone())
            .expect("a maximal record is legal");
    }

    let engine = Engine::open(dir.path()).expect("reopen must accept a maximal record");
    assert_eq!(engine.get(&key).expect("get"), value);
}

#[test]
fn a_truncated_tail_is_discarded_and_earlier_writes_survive() {
    let dir = TempDir::new().expect("tempdir");
    let log = dir.path().join("0.log");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set alpha");
        engine.set("beta".into(), "two".into()).expect("set beta");
    }
    let intact_len = std::fs::metadata(&log).expect("metadata").len();

    // Simulate a crash partway through a third append: a header promising more
    // payload bytes than actually reached the disk.
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&log)
            .expect("reopen for append");
        f.write_all(&0u32.to_le_bytes()).expect("crc");
        f.write_all(&64u32.to_le_bytes())
            .expect("length claiming 64 payload bytes");
        f.write_all(b"only twelve.").expect("but only 12 arrive");
        f.flush().expect("flush");
    }

    let mut engine = Engine::open(dir.path()).expect("reopen must not fail on a torn tail");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(engine.get("beta").expect("get"), "two");
    assert_eq!(
        std::fs::metadata(&log).expect("metadata").len(),
        intact_len,
        "the torn record must be truncated away"
    );

    // And the log must be usable again from the truncated offset.
    engine
        .set("gamma".into(), "three".into())
        .expect("set after recovery");
    drop(engine);

    let engine = Engine::open(dir.path()).expect("third open");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(engine.get("gamma").expect("get"), "three");
}

#[test]
fn a_record_with_a_bad_checksum_ends_the_log() {
    let dir = TempDir::new().expect("tempdir");
    let log = dir.path().join("0.log");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set alpha");
    }
    let intact_len = std::fs::metadata(&log).expect("metadata").len();

    {
        let mut engine = Engine::open(dir.path()).expect("reopen");
        engine.set("beta".into(), "two".into()).expect("set beta");
    }

    // Flip the final payload byte of the second record: complete, but corrupt.
    {
        let mut bytes = std::fs::read(&log).expect("read log");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&log, &bytes).expect("write log");
    }

    let engine = Engine::open(dir.path()).expect("reopen must not fail on a bad checksum");
    assert_eq!(
        engine.get("alpha").expect("get"),
        "one",
        "records before the corruption survive"
    );
    assert!(
        matches!(engine.get("beta"), Err(EngineError::KeyNotFound)),
        "the corrupt record must not be applied"
    );
    assert_eq!(
        std::fs::metadata(&log).expect("metadata").len(),
        intact_len,
        "the corrupt record must be truncated away"
    );
}

#[test]
fn a_header_claiming_an_absurd_length_ends_the_log() {
    let dir = TempDir::new().expect("tempdir");
    let log = dir.path().join("0.log");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set alpha");
    }
    let intact_len = std::fs::metadata(&log).expect("metadata").len();

    // A crash mid-header can leave a length field full of garbage.
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&log)
            .expect("reopen for append");
        f.write_all(&0u32.to_le_bytes()).expect("crc");
        f.write_all(&u32::MAX.to_le_bytes())
            .expect("a length claiming 4 GiB");
        f.flush().expect("flush");
    }

    let engine = Engine::open(dir.path()).expect("reopen must survive an absurd length");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(
        std::fs::metadata(&log).expect("metadata").len(),
        intact_len,
        "the bogus header must be truncated away"
    );
}
