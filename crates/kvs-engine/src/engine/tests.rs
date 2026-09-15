use super::*;
use crate::EngineError;
use tempfile::TempDir;

fn open_temp() -> (TempDir, Engine) {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    (dir, engine)
}

//
// get/set
//
#[test]
fn get_returns_a_value_that_was_set() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("set");
    assert_eq!(engine.get("alpha").expect("get"), "one");
}

#[test]
fn set_overwrites_an_existing_key() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("first set");
    engine
        .set("alpha".into(), "two".into())
        .expect("second set");
    assert_eq!(engine.get("alpha").expect("get"), "two");
}

#[test]
fn keys_are_independent() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("set alpha");
    engine.set("beta".into(), "two".into()).expect("set beta");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(engine.get("beta").expect("get"), "two");
}

#[test]
fn get_on_an_unknown_key_reports_key_not_found() {
    let (_dir, engine) = open_temp();
    let err = engine
        .get("ghost")
        .expect_err("an unset key must not resolve");
    assert!(matches!(err, EngineError::KeyNotFound));
}

#[test]
fn empty_values_round_trip() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), String::new()).expect("set");
    assert_eq!(engine.get("alpha").expect("get"), "");
}

#[test]
fn open_creates_a_missing_data_directory() {
    let dir = TempDir::new().expect("tempdir");
    let nested = dir.path().join("not-yet");
    let _engine = Engine::open(&nested).expect("open must create the directory");
    assert!(
        nested.join("0.log").exists(),
        "the log file must exist after open"
    );
}

#[test]
fn every_write_appends_to_the_log() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open(dir.path()).expect("open");
    let log = dir.path().join("0.log");

    engine.set("alpha".into(), "one".into()).expect("set");
    let after_first = std::fs::metadata(&log).expect("metadata").len();
    assert!(after_first > 0, "the first write must reach the file");

    engine.set("alpha".into(), "two".into()).expect("overwrite");
    let after_second = std::fs::metadata(&log).expect("metadata").len();
    assert!(
        after_second > after_first,
        "an overwrite appends a new record rather than editing in place"
    );
}

//
// remove
//
#[test]
fn remove_makes_a_key_unreadable() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("set");
    engine.remove("alpha").expect("remove");
    let err = engine
        .get("alpha")
        .expect_err("a removed key must not resolve");
    assert!(matches!(err, EngineError::KeyNotFound));
}

#[test]
fn remove_on_an_unknown_key_reports_key_not_found() {
    let (_dir, mut engine) = open_temp();
    let err = engine
        .remove("ghost")
        .expect_err("removing nothing must fail");
    assert!(matches!(err, EngineError::KeyNotFound));
}

#[test]
fn a_rejected_remove_writes_nothing_to_the_log() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open(dir.path()).expect("open");
    let log = dir.path().join("0.log");

    engine.set("alpha".into(), "one".into()).expect("set");
    let before = std::fs::metadata(&log).expect("metadata").len();
    engine
        .remove("ghost")
        .expect_err("removing nothing must fail");
    let after = std::fs::metadata(&log).expect("metadata").len();

    assert_eq!(
        before, after,
        "a rejected remove must not append a tombstone"
    );
}

#[test]
fn a_key_can_be_set_again_after_removal() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("set");
    engine.remove("alpha").expect("remove");
    engine.set("alpha".into(), "two".into()).expect("set again");
    assert_eq!(engine.get("alpha").expect("get"), "two");
}

#[test]
fn remove_leaves_other_keys_alone() {
    let (_dir, mut engine) = open_temp();
    engine.set("alpha".into(), "one".into()).expect("set alpha");
    engine.set("beta".into(), "two".into()).expect("set beta");
    engine.remove("alpha").expect("remove alpha");
    assert_eq!(engine.get("beta").expect("get"), "two");
}

#[test]
fn an_accepted_remove_appends_a_tombstone() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open(dir.path()).expect("open");
    let log = dir.path().join("0.log");

    engine.set("alpha".into(), "one".into()).expect("set");

    let before = std::fs::metadata(&log).expect("metadata").len();
    engine.remove("alpha").expect("remove");
    let after = std::fs::metadata(&log).expect("metadata").len();
    assert!(
        after > before,
        "an accepted remove must append a tombstone to the log"
    );
}

#[test]
fn remove_rejects_an_oversized_key() {
    let (_dir, mut engine) = open_temp();
    let key = "k".repeat(MAX_KEY_BYTES + 1);
    let err = engine
        .remove(&key)
        .expect_err("an oversized key must be rejected");
    assert!(matches!(err, EngineError::KeyTooLarge { .. }));
}

#[test]
fn set_rejects_an_oversized_key() {
    let (_dir, mut engine) = open_temp();
    let key = "k".repeat(MAX_KEY_BYTES + 1);
    let err = engine
        .set(key, "one".into())
        .expect_err("an oversized key must be rejected");
    assert!(matches!(err, EngineError::KeyTooLarge { .. }));
}

#[test]
fn set_rejects_an_oversized_value() {
    let (_dir, mut engine) = open_temp();
    let value = "v".repeat(MAX_VALUE_BYTES + 1);
    let err = engine
        .set("alpha".into(), value)
        .expect_err("an oversized value must be rejected");
    assert!(matches!(err, EngineError::ValueTooLarge { .. }));
}

#[test]
fn a_key_exactly_at_the_limit_is_accepted() {
    let (_dir, mut engine) = open_temp();
    let key = "k".repeat(MAX_KEY_BYTES);
    engine
        .set(key.clone(), "one".into())
        .expect("a key at the limit is legal");
    assert_eq!(engine.get(&key).expect("get"), "one");
}

#[test]
fn limits_are_measured_in_bytes_not_characters() {
    let (_dir, mut engine) = open_temp();
    // 'é' is two bytes in UTF-8, so this is over the limit despite being
    // MAX_KEY_BYTES characters long.
    let key = "é".repeat(MAX_KEY_BYTES);
    let err = engine
        .set(key, "one".into())
        .expect_err("byte length is what counts");
    assert!(matches!(err, EngineError::KeyTooLarge { .. }));
}

#[test]
fn a_rejected_write_leaves_the_log_untouched() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open(dir.path()).expect("open");
    let log = dir.path().join("0.log");

    engine.set("alpha".into(), "one".into()).expect("set");
    let before = std::fs::metadata(&log).expect("metadata").len();

    engine
        .set("k".repeat(MAX_KEY_BYTES + 1), "one".into())
        .expect_err("must be rejected");
    let after = std::fs::metadata(&log).expect("metadata").len();

    assert_eq!(before, after, "validation must happen before the append");
}

#[test]
fn the_never_policy_still_round_trips() {
    let dir = TempDir::new().expect("tempdir");
    {
        let mut engine = Engine::open_with(
            dir.path(),
            EngineConfig {
                fsync: FsyncPolicy::Never,
                ..Default::default()
            },
        )
        .expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
    }
    let engine = Engine::open(dir.path()).expect("reopen");
    assert_eq!(engine.get("alpha").expect("get"), "one");
}

#[test]
fn the_default_policy_is_always() {
    assert_eq!(FsyncPolicy::default(), FsyncPolicy::Always);
}

use crate::{EngineConfig, FsyncPolicy, layout::log_ids};

fn small_files(max_file_bytes: u64) -> EngineConfig {
    EngineConfig {
        max_file_bytes,
        ..EngineConfig::default()
    }
}

#[test]
fn the_default_config_is_fsync_always_and_64_mib_files() {
    let config = EngineConfig::default();
    assert_eq!(config.fsync, FsyncPolicy::Always);
    assert_eq!(config.max_file_bytes, 64 * 1024 * 1024);
    assert_eq!(config.dead_ratio, 0.5);
    assert_eq!(config.min_merge_bytes, 1024 * 1024);
}

#[test]
fn a_fresh_store_has_exactly_one_log_file() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(64)).expect("open");
    engine.set("alpha".into(), "one".into()).expect("set");
    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0]);
}

#[test]
fn crossing_the_size_threshold_rolls_to_a_new_file() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(32)).expect("open");

    // Each record is well over 32 bytes, so every write rolls.
    for i in 0..3 {
        engine
            .set(format!("key-{i}"), "a-value-long-enough-to-cross".into())
            .expect("set");
    }

    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0, 1, 2]);
}

#[test]
fn a_file_may_exceed_the_threshold_by_one_record() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
    engine.set("alpha".into(), "one".into()).expect("set");

    let len = std::fs::metadata(crate::layout::log_path(dir.path(), 0))
        .expect("metadata")
        .len();
    assert!(
        len > 8,
        "the record that crossed the line is still written whole"
    );
    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0]);
}

#[test]
fn rolling_does_not_happen_while_the_active_file_is_empty() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
    engine.set("alpha".into(), "one".into()).expect("set");
    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0]);

    // File 0 is over the threshold, so this write lands in a fresh  file 1 —
    // and must not then roll again on top of an empty file 1.
    engine.set("beta".into(), "two".into()).expect("set");
    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0, 1]);
}

#[test]
fn a_key_written_before_a_roll_is_still_readable_after_it() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
    engine.set("alpha".into(), "one".into()).expect("set");
    engine.set("beta".into(), "two".into()).expect("set");
    engine.set("gamma".into(), "three".into()).expect("set");

    assert_eq!(engine.get("alpha").expect("alpha"), "one");
    assert_eq!(engine.get("beta").expect("beta"), "two");
    assert_eq!(engine.get("gamma").expect("gamma"), "three");
}

#[test]
fn a_key_overwritten_after_a_roll_reads_back_the_newer_value() {
    let dir = TempDir::new().expect("tempdir");
    let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
    engine.set("alpha".into(), "old".into()).expect("set");
    engine.set("alpha".into(), "new".into()).expect("overwrite");

    assert_eq!(engine.get("alpha").expect("alpha"), "new");
    let entry = *engine
        .reader()
        .store
        .read()
        .expect("lock")
        .key_dir
        .get("alpha")
        .expect("alpha");
    assert_eq!(entry.file_id, 1, "the newer record lives in the newer file");
}

#[test]
fn reopening_a_rolled_store_replays_every_file() {
    let dir = TempDir::new().expect("tempdir");
    {
        let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
        for i in 0..5 {
            engine
                .set(format!("key-{i}"), format!("value-{i}"))
                .expect("set");
        }
        engine.sync().expect("sync");
    }

    let engine = Engine::open_with(dir.path(), small_files(8)).expect("reopen");
    for i in 0..5 {
        assert_eq!(
            engine.get(&format!("key-{i}")).expect("get"),
            format!("value-{i}")
        );
    }
}
#[test]
fn a_write_after_reopening_a_rolled_store_appends_to_the_highest_file() {
    let dir = TempDir::new().expect("tempdir");
    {
        let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("beta".into(), "two".into()).expect("set");
        engine.sync().expect("sync");
    }

    let mut engine = Engine::open_with(dir.path(), EngineConfig::default()).expect("reopen");
    engine.set("gamma".into(), "three".into()).expect("set");

    assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0, 1]);
    assert_eq!(engine.get("gamma").expect("gamma"), "three");
    assert_eq!(engine.get("alpha").expect("alpha"), "one");
}
