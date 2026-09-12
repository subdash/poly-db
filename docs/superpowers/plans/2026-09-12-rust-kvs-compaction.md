# rust-kvs Log Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single append-only log with a multi-file log, and add a
background merge that reclaims the space held by overwritten and deleted records.

**Architecture:** Files are `<id>.log` with ascending `u32` ids; the highest is
the active file and the rest are immutable. The keydir and a `file_id → handle`
table live together in one `Store` behind a single `RwLock`, so an entry and the
file it names can never disagree. A dedicated merge thread holds a clone of that
`Arc` and rewrites the immutable prefix into a single file, patching the keydir
under the write lock — it never touches `Engine`, so the writer thread keeps the
exclusive ownership that makes the current code easy to reason about. Merged
files get a value-free `<id>.hint` sidecar so startup reads an index rather than
the data.

**Tech Stack:** No new dependencies. `std::sync::mpsc` for the merge channel,
`std::sync::atomic` for the byte counters, existing `bincode`/`crc32fast` framing
for the hint format.

**Spec:** `docs/superpowers/specs/2026-09-12-rust-kvs-compaction-design.md`

## Global Constraints

- **You write the implementation.** This plan gives complete failing tests and
  exact signatures. It gives no function bodies. That is the project convention
  and the thing being learned.
- **The record format does not change.** Last-write-wins comes from ascending
  file-id replay order, not from timestamps or sequence numbers in records.
- **`Engine::open(path)` keeps working unchanged.** Most of the MVP's 70 tests
  call it; none of them should need rewriting. `Engine::open_with` is the only
  constructor whose signature changes.
- **File names:** `<id>.log`, `<id>.hint`, `<id>.log.tmp`, `<id>.hint.tmp`, with
  `id: u32`. Ids ascend; the highest existing id is the active file.
- **The merge output takes the highest input id**, never a fresh one — its records
  are older than the active file's and must replay before them.
- **The merge may only replace a keydir entry it recognizes, never insert one.**
  Absent key means it was removed mid-merge; `file_id > output_id` means it was
  overwritten mid-merge. Both are silent, ordinary skips.
- **Cleanup order is load-bearing:** write tmp and fsync, swap in memory, unlink
  inputs and fsync the directory, rename hint then log and fsync the directory.
  The log's rename is the commit point.
- **`fsync` directories, not just files.** Renames and unlinks are directory
  metadata operations.
- **Hints are a cache, never authoritative.** Any problem at all means replay the
  log for that file.
- **Defaults:** `max_file_bytes` 64 MiB, `dead_ratio` 0.5, `min_merge_bytes`
  1 MiB, `fsync` `Always`.
- **Every task ends green:** `cargo fmt`, `cargo clippy --all-targets -- -D
  warnings`, `cargo test --workspace`.

---

## File Structure

```
crates/kvs-engine/src/
  lib.rs        re-exports; gains EngineConfig, MergeOutcome        (modified)
  command.rs    unchanged
  error.rs      gains MergeInProgress                              (modified)
  keydir.rs     Entry, KeyDir — unchanged
  config.rs     NEW  EngineConfig and FsyncPolicy (moved here)
  store.rs      NEW  Store: the keydir and the file table together
  stats.rs      NEW  Stats: the two atomic byte counters
  layout.rs     NEW  file naming, id discovery, interrupted-merge recovery
  record.rs     framing split out from Command encoding             (modified)
  hint.rs       NEW  the hint record format and its file I/O
  replay.rs     NEW  build a Store from a directory
  merge.rs      NEW  the merge itself, plus MergeOutcome
  reader.rs     resolves handles through Store                      (modified)
  engine.rs     open, set/get/remove/sync/compact, append, roll     (modified)

crates/kvs-server/src/
  config.rs     four new env-backed flags                           (modified)
  dto.rs        CompactResponse                                     (modified)
  error.rs      MergeInProgress -> 409                              (modified)
  writer.rs     Request::Compact and KvHandle::compact              (modified)
  routes.rs     POST /v1/admin/compact                              (modified)
  main.rs       builds an EngineConfig                              (modified)
```

`engine.rs` is 492 lines before this plan and would roughly double if replay,
the merge, and layout logic landed inside it. Splitting by responsibility keeps
each file something you can hold in your head at once, and it puts the three
pieces with the subtlest rules — `merge.rs`, `layout.rs`, `replay.rs` — in files
whose whole contents are about those rules.

## Shared test helpers

Several tasks need to build log files by hand. Put this in `layout.rs` at Task 2
and use it from every later task's tests.

```rust
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::{Command, record};
    use std::io::Write;

    /// Write `cmds` to `<dir>/<id>.log` as framed records. Returns the file length.
    pub(crate) fn write_log(dir: &Path, id: u32, cmds: &[Command]) -> u64 {
        let path = log_path(dir, id);
        let mut file = std::fs::File::create(&path).expect("create log");
        for cmd in cmds {
            let bytes = record::encode(cmd).expect("encode");
            file.write_all(&bytes).expect("write record");
        }
        file.flush().expect("flush");
        file.metadata().expect("metadata").len()
    }

    /// The offset and payload length of record `index` in a log built from `cmds`.
    pub(crate) fn locate(cmds: &[Command], index: usize) -> (u64, u32) {
        let mut offset = 0u64;
        for cmd in &cmds[..index] {
            offset += record::encode(cmd).expect("encode").len() as u64;
        }
        let len = record::encode(&cmds[index]).expect("encode").len() - record::HEADER_LEN;
        (offset, len as u32)
    }

    pub(crate) fn set(key: &str, value: &str) -> Command {
        Command::Set { key: key.into(), value: value.into() }
    }

    pub(crate) fn remove(key: &str) -> Command {
        Command::Remove { key: key.into() }
    }
}
```

`record::HEADER_LEN` and `record::encode` are already `pub(crate)`, so this
compiles without widening any visibility.

---

## Task 1: `Store` — the keydir and the file table under one lock

**Files:**
- Create: `crates/kvs-engine/src/store.rs`
- Modify: `crates/kvs-engine/src/reader.rs`
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Entry`, `KeyDir` from `keydir.rs`; `EngineError`.
- Produces: `Store { key_dir, files }` with
  `Store::new()`, `Store::lookup(&self, key: &str) -> Result<(Entry, Arc<File>)>`;
  `Reader { store: Arc<RwLock<Store>> }`.

**Why this task exists and why it is first.** Nothing in it is new behaviour —
the store still writes one file and still passes exactly the tests it passes
today. What it buys is that every later task operates on a data structure where
the keydir and the file table cannot drift apart. Doing this refactor while there
is only ever one file in the table means any mistake shows up against a test
suite you already trust.

`lookup` returns a `Result` rather than an `Option` so the two failure modes stay
distinguishable: a key that is absent is `KeyNotFound`, which is routine, while an
entry naming a file the table lacks is `Corrupt`, which is an invariant violation
and should never be reported to a client as a missing key.

- [ ] **Step 1: Write the failing tests**

`crates/kvs-engine/src/store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use tempfile::TempDir;

    fn store_with_one_file(dir: &TempDir, file_id: u32) -> Store {
        let path = dir.path().join(format!("{file_id}.log"));
        std::fs::write(&path, b"eight-by").expect("write");
        let handle = Arc::new(File::open(&path).expect("open"));

        let mut store = Store::new();
        store.files.insert(file_id, handle);
        store
    }

    #[test]
    fn a_new_store_is_empty() {
        let store = Store::new();
        assert!(store.key_dir.is_empty());
        assert!(store.files.is_empty());
    }

    #[test]
    fn a_lookup_returns_the_entry_and_its_file_together() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 7);
        store.key_dir.insert(
            "alpha".to_string(),
            Entry { file_id: 7, pos: 0, len: 3 },
        );

        let (entry, file) = store.lookup("alpha").expect("alpha must resolve");
        assert_eq!(entry.file_id, 7);
        assert_eq!(entry.pos, 0);
        assert_eq!(file.metadata().expect("metadata").len(), 8);
    }

    #[test]
    fn a_lookup_of_an_unknown_key_reports_key_not_found() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_with_one_file(&dir, 0);
        assert!(matches!(
            store.lookup("ghost"),
            Err(EngineError::KeyNotFound)
        ));
    }

    #[test]
    fn an_entry_naming_a_file_the_table_lacks_is_corrupt_not_missing() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 0);
        store.key_dir.insert(
            "alpha".to_string(),
            Entry { file_id: 9, pos: 128, len: 3 },
        );

        assert!(matches!(
            store.lookup("alpha"),
            Err(EngineError::Corrupt { offset: 128 })
        ));
    }

    #[test]
    fn two_keys_in_different_files_each_resolve_to_their_own_handle() {
        let dir = TempDir::new().expect("tempdir");
        let mut store = store_with_one_file(&dir, 0);

        let second = dir.path().join("1.log");
        std::fs::write(&second, b"sixteen-bytes-!!").expect("write");
        store.files.insert(1, Arc::new(File::open(&second).expect("open")));

        store.key_dir.insert("alpha".into(), Entry { file_id: 0, pos: 0, len: 1 });
        store.key_dir.insert("beta".into(), Entry { file_id: 1, pos: 0, len: 1 });

        let (_, first_file) = store.lookup("alpha").expect("alpha");
        let (_, second_file) = store.lookup("beta").expect("beta");
        assert_eq!(first_file.metadata().expect("metadata").len(), 8);
        assert_eq!(second_file.metadata().expect("metadata").len(), 16);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p kvs-engine store::`
Expected: FAIL — `store` is not a module, `Store` is not defined.

- [ ] **Step 3: Write `Store`**

Declare `mod store;` in `lib.rs`. In `store.rs`:

```rust
use crate::{
    EngineError, Result,
    keydir::{Entry, KeyDir},
};
use std::{collections::HashMap, fs::File, sync::Arc};

pub(crate) struct Store {
    pub(crate) key_dir: KeyDir,
    pub(crate) files: HashMap<u32, Arc<File>>,
}

impl Store {
    pub(crate) fn new() -> Store { /* your code */ }

    /// Resolve a key to its entry and the handle for the file holding it, in one
    /// borrow of `self`, so the two can never come from different moments.
    pub(crate) fn lookup(&self, key: &str) -> Result<(Entry, Arc<File>)> { /* your code */ }
}
```

`lookup` copies the `Entry` (it is `Copy`) and clones the `Arc<File>`. A key
that is not in `key_dir` is `EngineError::KeyNotFound`. An entry whose `file_id`
is not in `files` is `EngineError::Corrupt { offset: entry.pos }`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p kvs-engine store::`
Expected: PASS, 5 tests.

- [ ] **Step 5: Move `Reader` onto `Store`**

```rust
#[derive(Clone)]
pub struct Reader {
    pub(crate) store: Arc<RwLock<Store>>,
}

impl Reader {
    pub fn get(&self, key: &str) -> Result<String> { /* your code */ }
}
```

`get` keeps the shape you arrived at in the MVP: one chained expression that
takes the read lock, calls `store.lookup(key)`, and lets the guard die at the
semicolon — so the `read_exact_at` below it runs with no lock held. The only
change is that the handle now comes out of `lookup` alongside the entry instead
of being a field.

- [ ] **Step 6: Move `Engine` onto `Store`**

`Engine::open_with` builds a `Store`, inserts the single `0.log` handle under
`file_id: 0`, wraps it in `Arc<RwLock<_>>`, and hands it to the `Reader`.
`Engine::set` and `Engine::remove` take the write lock on the store and mutate
`store.key_dir` instead of a bare keydir. `Engine::replay` keeps its current
shape for now; it still returns a keydir and an offset, and `open_with` assembles
the `Store` around it. Task 2 replaces it.

- [ ] **Step 7: Run the whole suite**

Run: `cargo test --workspace`
Expected: PASS. Every MVP test still passes — this task changed no behaviour.
If a reader test fails, the likely cause is `lookup` being called after the
guard was dropped rather than within the same expression.

- [ ] **Step 8: Commit**

```bash
git add crates/kvs-engine/src/store.rs crates/kvs-engine/src/reader.rs \
        crates/kvs-engine/src/engine.rs crates/kvs-engine/src/lib.rs
git commit -m "refactor(engine): hold the keydir and file handles in one Store"
```

---

## Task 2: File discovery and multi-file replay

**Files:**
- Create: `crates/kvs-engine/src/layout.rs`
- Create: `crates/kvs-engine/src/replay.rs`
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Store`, `Entry`, `record::{decode, payload_len, HEADER_LEN, MAX_PAYLOAD_BYTES}`.
- Produces:
  - `layout::{log_path, hint_path, tmp_log_path, tmp_hint_path}: fn(&Path, u32) -> PathBuf`
  - `layout::log_ids(dir: &Path) -> Result<Vec<u32>>` — ascending
  - `layout::test_support` (the shared helpers above)
  - `replay::Replayed { store, active_id, write_offset, total_bytes }`
  - `replay::build(dir: &Path) -> Result<Replayed>`

**Why this task exists.** Replay is the only thing that decides what the store
contains after a restart, and it is about to go from "read one file" to "read a
set of files in the right order, some of which may be corrupt in ways the active
file cannot be." Building it before anything writes a second file means the tests
construct their own multi-file directories and the production write path stays
constant.

Only the **active file** may have a torn tail — it was the one being appended to
when the process died. An immutable file was flushed and `sync_data`d at roll
time, so corruption in one is real damage: log it, stop reading *that* file, and
carry on with the next. Abandoning replay entirely would silently discard every
file above the damaged one.

`build` does not truncate. It returns `write_offset` for the active file and
`engine::open_with` does the truncation, because `open_with` is what holds the
append handle — the MVP already learned that `set_len` on a read-only handle
fails with `EINVAL` on macOS.

- [ ] **Step 1: Write the failing layout tests**

`crates/kvs-engine/src/layout.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn paths_are_named_by_id_and_extension() {
        let dir = Path::new("/data");
        assert_eq!(log_path(dir, 0), Path::new("/data/0.log"));
        assert_eq!(log_path(dir, 42), Path::new("/data/42.log"));
        assert_eq!(hint_path(dir, 42), Path::new("/data/42.hint"));
        assert_eq!(tmp_log_path(dir, 42), Path::new("/data/42.log.tmp"));
        assert_eq!(tmp_hint_path(dir, 42), Path::new("/data/42.hint.tmp"));
    }

    #[test]
    fn log_ids_of_an_empty_directory_is_empty() {
        let dir = TempDir::new().expect("tempdir");
        assert!(log_ids(dir.path()).expect("log_ids").is_empty());
    }

    #[test]
    fn log_ids_are_returned_in_ascending_numeric_order() {
        let dir = TempDir::new().expect("tempdir");
        for id in [10u32, 2, 0, 9] {
            std::fs::write(log_path(dir.path(), id), b"").expect("write");
        }
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0, 2, 9, 10]);
    }

    #[test]
    fn log_ids_ignores_everything_that_is_not_a_numbered_log() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(log_path(dir.path(), 3), b"").expect("write");
        for name in ["notes.txt", "foo.log", "3.hint", "4.log.tmp", "4.hint.tmp", "-1.log"] {
            std::fs::write(dir.path().join(name), b"").expect("write");
        }
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![3]);
    }
}
```

`4.log.tmp` is the one that catches a naive implementation: splitting on the
first `.` and parsing the stem gives `4`, which would make an uncommitted merge
output look like a live log file. The extension must be exactly `log`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine layout::`
Expected: FAIL — `layout` is not a module.

- [ ] **Step 3: Write `layout.rs`**

Declare `mod layout;` in `lib.rs`. Signatures:

```rust
use crate::Result;
use std::path::{Path, PathBuf};

pub(crate) const LOG_EXT: &str = "log";
pub(crate) const HINT_EXT: &str = "hint";

pub(crate) fn log_path(dir: &Path, id: u32) -> PathBuf { /* your code */ }
pub(crate) fn hint_path(dir: &Path, id: u32) -> PathBuf { /* your code */ }
pub(crate) fn tmp_log_path(dir: &Path, id: u32) -> PathBuf { /* your code */ }
pub(crate) fn tmp_hint_path(dir: &Path, id: u32) -> PathBuf { /* your code */ }

/// Every `<id>.log` in `dir`, ascending. Anything else is ignored.
pub(crate) fn log_ids(dir: &Path) -> Result<Vec<u32>> { /* your code */ }
```

Also add the `test_support` module from the "Shared test helpers" section above.

- [ ] **Step 4: Run them to verify they pass**

Run: `cargo test -p kvs-engine layout::`
Expected: PASS, 4 tests.

- [ ] **Step 5: Write the failing replay tests**

`crates/kvs-engine/src/replay.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{log_path, test_support::{locate, remove, set, write_log}};
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn replaying_an_empty_directory_yields_an_empty_store() {
        let dir = TempDir::new().expect("tempdir");
        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.is_empty());
        assert_eq!(replayed.active_id, 0);
        assert_eq!(replayed.write_offset, 0);
        assert_eq!(replayed.total_bytes, 0);
    }

    #[test]
    fn a_single_file_replays_every_record() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        let len = write_log(dir.path(), 0, &cmds);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.store.key_dir.len(), 2);
        assert_eq!(replayed.active_id, 0);
        assert_eq!(replayed.write_offset, len);
        assert_eq!(replayed.total_bytes, len);
    }

    #[test]
    fn a_later_file_wins_over_an_earlier_one() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "old")]);
        write_log(dir.path(), 1, &[set("alpha", "new")]);

        let replayed = build(dir.path()).expect("build");
        let entry = replayed.store.key_dir.get("alpha").expect("alpha");
        assert_eq!(entry.file_id, 1);
    }

    #[test]
    fn file_ids_replay_in_numeric_order_not_lexicographic_order() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 2, &[set("alpha", "second")]);
        write_log(dir.path(), 10, &[set("alpha", "tenth")]);

        let replayed = build(dir.path()).expect("build");
        let entry = replayed.store.key_dir.get("alpha").expect("alpha");
        assert_eq!(entry.file_id, 10, "\"10\" sorts before \"2\" as a string");
        assert_eq!(replayed.active_id, 10);
    }

    #[test]
    fn a_remove_in_a_later_file_shadows_a_set_in_an_earlier_one() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[remove("alpha")]);

        let replayed = build(dir.path()).expect("build");
        assert!(!replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn a_set_after_a_remove_brings_the_key_back() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one"), remove("alpha")]);
        write_log(dir.path(), 1, &[set("alpha", "again")]);

        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn entries_record_the_file_and_offset_they_came_from() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        write_log(dir.path(), 4, &cmds);

        let replayed = build(dir.path()).expect("build");
        let (pos, len) = locate(&cmds, 1);
        let entry = replayed.store.key_dir.get("beta").expect("beta");
        assert_eq!((entry.file_id, entry.pos, entry.len), (4, pos, len));
    }

    #[test]
    fn every_replayed_file_gets_a_read_handle() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[set("beta", "two")]);
        write_log(dir.path(), 2, &[set("gamma", "three")]);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.store.files.len(), 3);
        for id in [0, 1, 2] {
            assert!(replayed.store.files.contains_key(&id), "missing handle for {id}");
        }
    }

    #[test]
    fn total_bytes_is_the_sum_of_every_file() {
        let dir = TempDir::new().expect("tempdir");
        let first = write_log(dir.path(), 0, &[set("alpha", "one")]);
        let second = write_log(dir.path(), 1, &[set("beta", "two")]);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.total_bytes, first + second);
    }

    #[test]
    fn a_torn_tail_on_the_active_file_sets_the_write_offset_below_its_length() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one")];
        let good = write_log(dir.path(), 0, &cmds);

        // Append half a header — the shape a crash mid-write leaves behind.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(log_path(dir.path(), 0))
            .expect("reopen");
        file.write_all(&[0u8; 4]).expect("append garbage");
        drop(file);

        let replayed = build(dir.path()).expect("build");
        assert_eq!(replayed.write_offset, good, "the tail must not be counted");
        assert!(replayed.store.key_dir.contains_key("alpha"));
    }

    #[test]
    fn corruption_in_an_immutable_file_does_not_discard_the_files_above_it() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("beta", "two")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("gamma", "three")]);

        // Corrupt the payload of the *second* record in file 0.
        let (pos, _) = locate(&cmds, 1);
        let path = log_path(dir.path(), 0);
        let mut bytes = std::fs::read(&path).expect("read");
        let target = pos as usize + crate::record::HEADER_LEN;
        bytes[target] ^= 0xff;
        std::fs::write(&path, &bytes).expect("rewrite");

        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.contains_key("alpha"), "the good prefix survives");
        assert!(!replayed.store.key_dir.contains_key("beta"), "the bad record is dropped");
        assert!(replayed.store.key_dir.contains_key("gamma"), "file 1 is still replayed");
        assert_eq!(replayed.active_id, 1);
    }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `cargo test -p kvs-engine replay::`
Expected: FAIL — `replay` is not a module.

- [ ] **Step 7: Write `replay.rs`**

Declare `mod replay;` in `lib.rs`. Signatures:

```rust
use crate::{Result, store::Store};
use std::path::Path;

pub(crate) struct Replayed {
    pub(crate) store: Store,
    /// The highest log id present, or 0 for an empty directory.
    pub(crate) active_id: u32,
    /// Where the next append to the active file belongs.
    pub(crate) write_offset: u64,
    /// The summed length of every log file, for the merge trigger.
    pub(crate) total_bytes: u64,
}

pub(crate) fn build(dir: &Path) -> Result<Replayed> { /* your code */ }

/// Replay one log file into `store`, returning the offset just past the last
/// record it could read. `is_active` selects the corruption policy.
fn replay_file(store: &mut Store, dir: &Path, id: u32, is_active: bool) -> Result<u64> {
    /* your code */
}
```

The per-record loop is the one you already wrote in `Engine::replay` — header,
`payload_len` bounds check against `MAX_PAYLOAD_BYTES`, assemble one buffer,
`read_exact` into `&mut buf[HEADER_LEN..]`, `record::decode`. Two things change:
the `Entry` gets the real `file_id` instead of a hardcoded `0`, and a decode
failure logs a `tracing::warn!` naming the file and offset before it stops
reading that file.

This is also where `Entry.file_id` stops being `#[allow(dead_code)]`: replay is
the first thing to write a value other than `0` into it, and `Store::lookup` from
Task 1 is the first thing to read it. Drop the attribute in `keydir.rs` and let
the compiler confirm the field is load-bearing now.

`build` opens a read handle per file and inserts it into `store.files` whether or
not the file replayed cleanly — a partially readable file still holds the records
that did replay, and their entries point into it.

- [ ] **Step 8: Run them to verify they pass**

Run: `cargo test -p kvs-engine replay::`
Expected: PASS, 11 tests.

- [ ] **Step 9: Point `Engine::open_with` at `replay::build`**

Delete `Engine::replay`. `open_with` now: `create_dir_all`, `replay::build`, open
an append handle on `log_path(dir, active_id)`, truncate it to `write_offset` if
its length exceeds that (keeping the existing `tracing::warn!`), install the
append handle's read counterpart, and store `active_id`.

The truncation check must become `saturating_sub` or an explicit comparison.
`metadata()?.len() - write_offset` underflows and panics in debug if a hint file
ever lets replay report an offset beyond the file's length, which Task 7 makes
possible.

- [ ] **Step 10: Run the whole suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add crates/kvs-engine/src/layout.rs crates/kvs-engine/src/replay.rs \
        crates/kvs-engine/src/engine.rs crates/kvs-engine/src/lib.rs
git commit -m "feat(engine): discover and replay a multi-file log"
```

---

## Task 3: `EngineConfig` and rolling the active file

**Files:**
- Create: `crates/kvs-engine/src/config.rs`
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`
- Modify: `crates/kvs-server/src/main.rs`

**Interfaces:**
- Consumes: `layout::log_path`, `Store`.
- Produces: `EngineConfig { fsync, max_file_bytes, dead_ratio, min_merge_bytes }`
  with `Default`; `FsyncPolicy` moved into `config.rs`;
  `Engine::open_with(path, config: EngineConfig)`.

**Why this task exists.** Without rolling there is one file and it is the one
being appended to, so there is nothing a merge could ever run against. This is
also the point where the engine's knob count outgrows positional arguments, so
`EngineConfig` lands here with all four fields even though only two are used
before Task 8.

**The size check runs at the start of the next append, not at the end of the
previous one.** A record that crosses the threshold is written whole into the file
it overflows, and the roll happens before the record *after* it. So a file may
exceed `max_file_bytes` by at most one record — a threshold, not a cap — and no
empty trailing file is created merely because the last write happened to be the
one that crossed the line.

Refusing an append that *would* cross the limit is the alternative, and it is
worse: a record larger than `max_file_bytes` could then never be written at all.

**`roll` itself is unconditional**; only the size-triggered `maybe_roll` carries
the empty-file guard. `compact` needs a definite `output_id` even on an idle
store, so it always rolls. That does not make files accumulate: every merge
collapses the whole prefix, so a repeatedly compacted idle store stays at two
files.

- [ ] **Step 1: Write the failing tests**

`crates/kvs-engine/src/engine.rs`, in the existing `mod tests`:

```rust
    use crate::{EngineConfig, FsyncPolicy, layout::log_ids};

    fn small_files(max_file_bytes: u64) -> EngineConfig {
        EngineConfig { max_file_bytes, ..EngineConfig::default() }
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
        assert!(len > 8, "the record that crossed the line is still written whole");
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0]);
    }

    #[test]
    fn rolling_does_not_happen_while_the_active_file_is_empty() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open_with(dir.path(), small_files(8)).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(log_ids(dir.path()).expect("log_ids"), vec![0]);

        // File 0 is over the threshold, so this write lands in a fresh file 1 —
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
                engine.set(format!("key-{i}"), format!("value-{i}")).expect("set");
            }
            engine.sync().expect("sync");
        }

        let engine = Engine::open_with(dir.path(), small_files(8)).expect("reopen");
        for i in 0..5 {
            assert_eq!(engine.get(&format!("key-{i}")).expect("get"), format!("value-{i}"));
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine engine::`
Expected: FAIL — `EngineConfig` is not defined and `open_with` takes a
`FsyncPolicy`.

- [ ] **Step 3: Write `config.rs`**

Declare `mod config;` in `lib.rs` and re-export both types. Move `FsyncPolicy`
out of `engine.rs`.

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    #[default]
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineConfig {
    pub fsync: FsyncPolicy,
    /// Roll the active file once it passes this many bytes. A threshold, not a
    /// cap: the record that crosses it is still written whole.
    pub max_file_bytes: u64,
    /// Merge when dead bytes reach this fraction of total bytes.
    pub dead_ratio: f64,
    /// Never merge a log smaller than this, whatever the ratio says.
    pub min_merge_bytes: u64,
}

impl Default for EngineConfig { /* your code: Always, 64 MiB, 0.5, 1 MiB */ }
```

`EngineConfig` cannot derive `Eq` because `dead_ratio` is an `f64`. `PartialEq`
alone is correct and is all the tests need.

- [ ] **Step 4: Add rolling to `Engine`**

`Engine` gains `active_id: u32` and swaps `policy: FsyncPolicy` for
`config: EngineConfig`. Two new private methods:

```rust
    /// Close the active file, mark it immutable, and open `active_id + 1`.
    /// Returns the id of the file that was just closed.
    fn roll(&mut self) -> Result<u32> { /* your code */ }

    /// Roll if the active file has passed `max_file_bytes` and is not empty.
    fn maybe_roll(&mut self) -> Result<()> { /* your code */ }
```

`roll` flushes the `BufWriter`, calls `sync_data` on the file beneath it
regardless of `FsyncPolicy` (an immutable file must be durable before anything
treats it as immutable), opens `log_path(dir, active_id + 1)` with
`.append(true).create(true)`, replaces `self.writer`, inserts the new file's read
handle into the store under the write lock, sets `write_offset` to 0, and
increments `active_id`.

`maybe_roll` is called at the **start** of `append`, before the record is
encoded. The guard is
`self.write_offset > 0 && self.write_offset >= self.config.max_file_bytes`; the
first clause only matters if `max_file_bytes` is set to 0.

- [ ] **Step 5: Run them to verify they pass**

Run: `cargo test -p kvs-engine engine::`
Expected: PASS.

- [ ] **Step 6: Update the callers**

`Engine::open` becomes `Engine::open_with(path, EngineConfig::default())`. The
MVP's fsync tests that called `open_with(path, FsyncPolicy::Never)` become
`open_with(path, EngineConfig { fsync: FsyncPolicy::Never, ..Default::default() })`.
In `crates/kvs-server/src/main.rs`, build an `EngineConfig` from `Config` — for
now only `fsync` is wired; Task 9 adds the other three.

- [ ] **Step 7: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/kvs-engine/src/config.rs crates/kvs-engine/src/engine.rs \
        crates/kvs-engine/src/lib.rs crates/kvs-server/src/main.rs
git commit -m "feat(engine): add EngineConfig and roll the active log file"
```

---

## Task 4: Dead-byte accounting

**Files:**
- Create: `crates/kvs-engine/src/stats.rs`
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Stats` with `record_append`, `record_dead`, `reclaim`,
  `should_merge`, `total_bytes`, `dead_bytes`; `Engine` holds an `Arc<Stats>`.

**Why this task exists, and why it is separate from the merge.** The counters
decide *when* a merge happens, and they are the one part of this project that can
be wrong without any test failing — a ratio that never crosses its threshold just
means compaction silently never runs. Isolating them means they get tested for
what they are: arithmetic.

**They are a heuristic, not an invariant.** Drift means the next merge fires a
little early or late. Nothing here should be built to keep them exact.

**A tombstone counts as dead the moment it is written.** It carries no live data,
so counting it is what makes a delete-heavy workload eventually trigger a merge
instead of growing forever.

- [ ] **Step 1: Write the failing tests**

`crates/kvs-engine/src/stats.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_stats_is_all_zero() {
        let stats = Stats::default();
        assert_eq!(stats.total_bytes(), 0);
        assert_eq!(stats.dead_bytes(), 0);
    }

    #[test]
    fn appends_accumulate_into_total_bytes() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_append(50);
        assert_eq!(stats.total_bytes(), 150);
        assert_eq!(stats.dead_bytes(), 0);
    }

    #[test]
    fn should_merge_is_false_below_the_minimum_size() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_dead(100);
        assert!(!stats.should_merge(1024, 0.5), "100% dead but far too small");
    }

    #[test]
    fn should_merge_is_false_below_the_ratio() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(400);
        assert!(!stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_is_true_once_both_conditions_hold() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(500);
        assert!(stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_divides_in_floating_point() {
        // Integer division would floor 600/1000 to 0 and never fire.
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(600);
        assert!(stats.should_merge(100, 0.5));
    }

    #[test]
    fn should_merge_is_false_on_an_empty_log_rather_than_dividing_by_zero() {
        let stats = Stats::default();
        assert!(!stats.should_merge(0, 0.0));
        assert_eq!(stats.total_bytes(), 0);
    }

    #[test]
    fn reclaiming_subtracts_from_both_counters() {
        let stats = Stats::default();
        stats.record_append(1000);
        stats.record_dead(600);

        stats.reclaim(600, 700);
        assert_eq!(stats.dead_bytes(), 0);
        assert_eq!(stats.total_bytes(), 300);
    }

    #[test]
    fn reclaiming_saturates_rather_than_wrapping() {
        let stats = Stats::default();
        stats.record_append(100);
        stats.record_dead(10);

        stats.reclaim(999, 999);
        assert_eq!(stats.dead_bytes(), 0);
        assert_eq!(stats.total_bytes(), 0);
    }
}
```

The last test matters more than it looks. `reclaim` takes values a merge computed
from a snapshot taken earlier, so a concurrent `record_dead` or an off-by-a-record
estimate can hand it a number larger than the counter. `fetch_sub` on a `u64`
wraps to something near `u64::MAX`, which would make `should_merge` true forever
and put the store in a permanent merge loop. Saturating is the whole defence.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine stats::`
Expected: FAIL — `stats` is not a module.

- [ ] **Step 3: Write `stats.rs`**

Declare `mod stats;` in `lib.rs`.

```rust
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct Stats {
    total: AtomicU64,
    dead: AtomicU64,
}

impl Stats {
    pub(crate) fn total_bytes(&self) -> u64 { /* your code */ }
    pub(crate) fn dead_bytes(&self) -> u64 { /* your code */ }

    /// A record was appended: `len` is its framed length, header included.
    pub(crate) fn record_append(&self, len: u64) { /* your code */ }

    /// `len` bytes became unreachable.
    pub(crate) fn record_dead(&self, len: u64) { /* your code */ }

    /// A merge committed: it reclaimed `dead` counted dead bytes and shrank the
    /// log by `bytes_freed`. Both subtractions saturate.
    pub(crate) fn reclaim(&self, dead: u64, bytes_freed: u64) { /* your code */ }

    pub(crate) fn should_merge(&self, min_merge_bytes: u64, dead_ratio: f64) -> bool {
        /* your code */
    }
}
```

`Ordering::Relaxed` is correct throughout. These counters coordinate nothing —
they are read to make a heuristic decision, never to establish that some other
write is visible.

- [ ] **Step 4: Run them to verify they pass**

Run: `cargo test -p kvs-engine stats::`
Expected: PASS, 9 tests.

- [ ] **Step 5: Write the failing engine-level tests**

In `engine.rs`'s `mod tests`:

```rust
    fn framed_len(cmd: &Command) -> u64 {
        crate::record::encode(cmd).expect("encode").len() as u64
    }

    #[test]
    fn a_fresh_key_adds_no_dead_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");

        assert_eq!(engine.stats.dead_bytes(), 0);
        assert_eq!(
            engine.stats.total_bytes(),
            framed_len(&Command::Set { key: "alpha".into(), value: "one".into() })
        );
    }

    #[test]
    fn an_overwrite_marks_the_previous_record_dead() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let first = Command::Set { key: "alpha".into(), value: "one".into() };
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");

        assert_eq!(engine.stats.dead_bytes(), framed_len(&first));
    }

    #[test]
    fn a_remove_marks_both_the_record_and_its_tombstone_dead() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let record = Command::Set { key: "alpha".into(), value: "one".into() };
        let tombstone = Command::Remove { key: "alpha".into() };

        engine.set("alpha".into(), "one".into()).expect("set");
        engine.remove("alpha").expect("remove");

        assert_eq!(
            engine.stats.dead_bytes(),
            framed_len(&record) + framed_len(&tombstone)
        );
        assert_eq!(engine.stats.dead_bytes(), engine.stats.total_bytes());
    }

    #[test]
    fn a_failed_remove_changes_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        assert!(matches!(engine.remove("ghost"), Err(EngineError::KeyNotFound)));
        assert_eq!(engine.stats.total_bytes(), 0);
        assert_eq!(engine.stats.dead_bytes(), 0);
    }

    #[test]
    fn reopening_a_store_recounts_total_bytes_from_the_files() {
        let dir = TempDir::new().expect("tempdir");
        {
            let mut engine = Engine::open(dir.path()).expect("open");
            engine.set("alpha".into(), "one".into()).expect("set");
            engine.set("alpha".into(), "two".into()).expect("overwrite");
            engine.sync().expect("sync");
        }

        let engine = Engine::open(dir.path()).expect("reopen");
        let on_disk = std::fs::metadata(crate::layout::log_path(dir.path(), 0))
            .expect("metadata")
            .len();
        assert_eq!(engine.stats.total_bytes(), on_disk);
        assert_eq!(
            engine.stats.dead_bytes(), 0,
            "dead bytes are not persisted; replay starts the estimate over"
        );
    }
```

That last test states a real limitation rather than a bug: dead bytes are not
recorded on disk, so a restart forgets them and the store may carry an
already-compactable log until enough new writes re-cross the threshold. Writing
them down would mean a manifest, which section 9 of the spec defers. The manual
`compact` endpoint is the escape hatch in the meantime.

- [ ] **Step 6: Run them to verify they fail**

Run: `cargo test -p kvs-engine engine::`
Expected: FAIL — `Engine` has no `stats` field.

- [ ] **Step 7: Wire `Stats` into `Engine`**

`Engine` gains `stats: Arc<Stats>`, built in `open_with` with `total_bytes`
seeded from `replay::build`'s `total_bytes` and `dead_bytes` left at zero.

`append` calls `record_append(record_len)`. `set` looks up the existing entry
under the same write lock it already takes, and if one was there calls
`record_dead(HEADER_LEN + old.len)`. `remove` calls `record_dead` with the
removed record's framed length *plus* the tombstone's.

`set`'s existing lock acquisition is enough: take the write lock once, use
`HashMap::insert`'s returned `Option<Entry>` to learn whether a record was
displaced, and record it dead after releasing. Do not add a second lookup.

- [ ] **Step 8: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/kvs-engine/src/stats.rs crates/kvs-engine/src/engine.rs \
        crates/kvs-engine/src/lib.rs
git commit -m "feat(engine): track live and dead bytes"
```

---

## Task 5: Hint files

**Files:**
- Create: `crates/kvs-engine/src/hint.rs`
- Modify: `crates/kvs-engine/src/record.rs`
- Modify: `crates/kvs-engine/src/replay.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `record::{frame, unframe, HEADER_LEN}`, `layout::hint_path`, `Entry`.
- Produces:
  - `record::frame(payload: &[u8]) -> Vec<u8>`
  - `record::unframe(buffer: &[u8], offset: u64) -> Result<&[u8]>`
  - `hint::Hint { key, pos, len }`
  - `hint::write_file(path: &Path, hints: &[Hint]) -> Result<()>`
  - `hint::read_file(path: &Path) -> Result<Vec<Hint>>`

**Why this task comes before the merge.** The hint format is pure, standalone,
and testable against hand-built files. Writing it first means the merge emits
hints from the start rather than having them retrofitted into a function whose
crash-ordering is already delicate.

**Framing gets split out of `record.rs` first.** `encode` currently does two
things: bincode a `Command`, and wrap the result in `[crc][len]`. Only the second
half is reusable. Pulling `frame`/`unframe` out means the hint format inherits
CRC checking, length validation, and torn-tail detection for free instead of
reimplementing them — and `record::encode`/`decode` become two-line compositions.

**Hints are used for immutable files only.** Not a policy choice: replay must
report a `write_offset` for the active file, and only scanning the log tells you
where its records actually stop. A hint cannot supply that, so the active file is
always replayed from its log.

**Any problem with a hint means replay the log.** Missing, short, bad CRC,
undecodable — one rule, and it means a corrupt hint can never cost data.

- [ ] **Step 1: Write the failing framing tests**

In `record.rs`'s `mod tests`:

```rust
    #[test]
    fn framing_round_trips_an_arbitrary_payload() {
        let payload = b"not a command at all".as_slice();
        let framed = frame(payload);
        assert_eq!(framed.len(), HEADER_LEN + payload.len());
        assert_eq!(unframe(&framed, 0).expect("unframe"), payload);
    }

    #[test]
    fn framing_round_trips_an_empty_payload() {
        let framed = frame(&[]);
        assert_eq!(unframe(&framed, 0).expect("unframe"), b"");
    }

    #[test]
    fn unframing_rejects_a_tampered_payload() {
        let mut framed = frame(b"payload");
        let last = framed.len() - 1;
        framed[last] ^= 0xff;
        assert!(matches!(
            unframe(&framed, 512),
            Err(EngineError::Corrupt { offset: 512 })
        ));
    }

    #[test]
    fn unframing_rejects_a_buffer_shorter_than_its_header_claims() {
        let framed = frame(b"payload");
        assert!(matches!(
            unframe(&framed[..framed.len() - 1], 0),
            Err(EngineError::Corrupt { offset: 0 })
        ));
    }

    #[test]
    fn unframing_rejects_a_runt_buffer() {
        assert!(matches!(
            unframe(&[0u8; 3], 0),
            Err(EngineError::Corrupt { offset: 0 })
        ));
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine record::`
Expected: FAIL — `frame` and `unframe` are not defined. The five existing
`record` tests still pass.

- [ ] **Step 3: Split framing out of `record.rs`**

```rust
/// Wrap `payload` as `[crc: u32][len: u32][payload]`, little-endian.
pub(crate) fn frame(payload: &[u8]) -> Vec<u8> { /* your code */ }

/// Validate a framed buffer and return its payload. `offset` is only used to
/// describe where a corrupt record was found.
pub(crate) fn unframe(buffer: &[u8], offset: u64) -> Result<&[u8]> { /* your code */ }
```

`frame` is the body of today's `encode` from `let crc = ...` onward. `unframe` is
today's `decode` up to and including the CRC comparison, returning
`&buffer[HEADER_LEN..]`. Then `encode` becomes bincode-then-`frame`, and `decode`
becomes `unframe`-then-bincode. Neither public signature changes, and all five
existing `record` tests must keep passing untouched — they are the evidence the
split preserved behaviour.

- [ ] **Step 4: Run them to verify they pass**

Run: `cargo test -p kvs-engine record::`
Expected: PASS, 10 tests.

- [ ] **Step 5: Write the failing hint tests**

`crates/kvs-engine/src/hint.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use tempfile::TempDir;

    fn sample() -> Vec<Hint> {
        vec![
            Hint { key: "alpha".into(), pos: 0, len: 24 },
            Hint { key: "beta".into(), pos: 32, len: 24 },
            Hint { key: "gamma".into(), pos: 64, len: 30 },
        ]
    }

    #[test]
    fn a_hint_round_trips() {
        let hint = Hint { key: "alpha".into(), pos: 4096, len: 24 };
        let bytes = encode(&hint).expect("encode");
        assert_eq!(decode(&bytes, 0).expect("decode"), hint);
    }

    #[test]
    fn a_hint_file_round_trips_many_hints() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("0.hint");
        write_file(&path, &sample()).expect("write");
        assert_eq!(read_file(&path).expect("read"), sample());
    }

    #[test]
    fn an_empty_hint_file_yields_no_hints() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("0.hint");
        write_file(&path, &[]).expect("write");
        assert!(read_file(&path).expect("read").is_empty());
    }

    #[test]
    fn a_hint_file_is_much_smaller_than_the_values_it_indexes() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("0.hint");
        let hints: Vec<Hint> = (0..100)
            .map(|i| Hint { key: format!("key-{i}"), pos: i * 4096, len: 4096 })
            .collect();
        write_file(&path, &hints).expect("write");

        let hint_len = std::fs::metadata(&path).expect("metadata").len();
        assert!(hint_len < 100 * 4096 / 10, "hint file was {hint_len} bytes");
    }

    #[test]
    fn a_truncated_hint_file_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("0.hint");
        write_file(&path, &sample()).expect("write");

        let bytes = std::fs::read(&path).expect("read");
        std::fs::write(&path, &bytes[..bytes.len() - 3]).expect("truncate");

        assert!(matches!(read_file(&path), Err(EngineError::Corrupt { .. })));
    }

    #[test]
    fn a_tampered_hint_file_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("0.hint");
        write_file(&path, &sample()).expect("write");

        let mut bytes = std::fs::read(&path).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).expect("tamper");

        assert!(matches!(read_file(&path), Err(EngineError::Corrupt { .. })));
    }

    #[test]
    fn reading_a_hint_file_that_does_not_exist_is_an_io_error() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("nope.hint");
        assert!(matches!(read_file(&path), Err(EngineError::Io(_))));
    }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `cargo test -p kvs-engine hint::`
Expected: FAIL — `hint` is not a module.

- [ ] **Step 7: Write `hint.rs`**

Declare `mod hint;` in `lib.rs`.

```rust
use crate::{EngineError, Result, record};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// One keydir entry, without its value. `file_id` is absent on purpose: a hint
/// file describes exactly one log file, so the id comes from the filename.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Hint {
    pub(crate) key: String,
    pub(crate) pos: u64,
    pub(crate) len: u32,
}

pub(crate) fn encode(hint: &Hint) -> Result<Vec<u8>> { /* your code */ }
pub(crate) fn decode(buffer: &[u8], offset: u64) -> Result<Hint> { /* your code */ }

/// Write every hint to `path`, then `sync_data`. Overwrites what is there.
pub(crate) fn write_file(path: &Path, hints: &[Hint]) -> Result<()> { /* your code */ }

/// Read every hint from `path`. Any framing or decode failure is
/// `EngineError::Corrupt`; a missing file is `EngineError::Io`.
pub(crate) fn read_file(path: &Path) -> Result<Vec<Hint>> { /* your code */ }
```

`encode`/`decode` mirror `record::encode`/`decode` exactly — bincode then
`record::frame`, `record::unframe` then bincode.

`read_file` reads the whole file and walks it with the same header-then-payload
loop as replay, except that **a short read is a failure rather than a stopping
point**. Replay tolerates a torn tail because the log's tail is where a crash
lands; a hint file is written and synced in one shot before its log is renamed
into place, so a short hint file means something is wrong and the log should be
used instead.

- [ ] **Step 8: Run them to verify they pass**

Run: `cargo test -p kvs-engine hint::`
Expected: PASS, 7 tests.

- [ ] **Step 9: Write the failing replay-with-hints tests**

In `replay.rs`'s `mod tests`:

```rust
    use crate::hint::{self, Hint};
    use crate::layout::hint_path;

    #[test]
    fn replay_prefers_a_hint_file_over_the_log() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "first"), set("alpha", "second")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("beta", "two")]);

        // The log says alpha lives at record 1. The hint says record 0. If the
        // hint is honoured, the keydir points at the *first* record.
        let (first_pos, first_len) = locate(&cmds, 0);
        hint::write_file(
            &hint_path(dir.path(), 0),
            &[Hint { key: "alpha".into(), pos: first_pos, len: first_len }],
        )
        .expect("write hint");

        let replayed = build(dir.path()).expect("build");
        let entry = replayed.store.key_dir.get("alpha").expect("alpha");
        assert_eq!(entry.pos, first_pos, "the hint was ignored");
        assert_eq!(entry.file_id, 0, "file_id comes from the filename");
    }

    #[test]
    fn replay_falls_back_to_the_log_when_the_hint_is_missing() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("beta", "two")]);

        let replayed = build(dir.path()).expect("build");
        let (pos, _) = locate(&cmds, 0);
        assert_eq!(replayed.store.key_dir.get("alpha").expect("alpha").pos, pos);
    }

    #[test]
    fn replay_falls_back_to_the_log_when_the_hint_is_corrupt() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one"), set("gamma", "three")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("beta", "two")]);

        std::fs::write(hint_path(dir.path(), 0), b"this is not a hint file")
            .expect("write garbage");

        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.key_dir.contains_key("alpha"));
        assert!(replayed.store.key_dir.contains_key("gamma"));
        assert!(replayed.store.key_dir.contains_key("beta"));
    }

    #[test]
    fn a_hint_file_for_the_active_file_is_ignored() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "first"), set("alpha", "second")];
        let len = write_log(dir.path(), 0, &cmds);

        let (first_pos, first_len) = locate(&cmds, 0);
        hint::write_file(
            &hint_path(dir.path(), 0),
            &[Hint { key: "alpha".into(), pos: first_pos, len: first_len }],
        )
        .expect("write hint");

        // File 0 is the only file, so it is the active file.
        let replayed = build(dir.path()).expect("build");
        let (second_pos, _) = locate(&cmds, 1);
        assert_eq!(
            replayed.store.key_dir.get("alpha").expect("alpha").pos,
            second_pos,
            "the active file must be replayed from its log"
        );
        assert_eq!(replayed.write_offset, len);
    }

    #[test]
    fn a_hinted_file_still_gets_a_read_handle() {
        let dir = TempDir::new().expect("tempdir");
        let cmds = [set("alpha", "one")];
        write_log(dir.path(), 0, &cmds);
        write_log(dir.path(), 1, &[set("beta", "two")]);

        let (pos, len) = locate(&cmds, 0);
        hint::write_file(
            &hint_path(dir.path(), 0),
            &[Hint { key: "alpha".into(), pos, len }],
        )
        .expect("write hint");

        let replayed = build(dir.path()).expect("build");
        assert!(replayed.store.files.contains_key(&0));
        assert_eq!(replayed.total_bytes, {
            let a = std::fs::metadata(log_path(dir.path(), 0)).expect("m").len();
            let b = std::fs::metadata(log_path(dir.path(), 1)).expect("m").len();
            a + b
        });
    }
```

That last assertion pins something easy to get wrong: `total_bytes` must come
from the log files' sizes on disk, not from what replay happened to read. A
hinted file's log is never read, so summing consumed bytes would undercount it
and make the merge trigger fire late.

- [ ] **Step 10: Run them to verify they fail**

Run: `cargo test -p kvs-engine replay::`
Expected: FAIL — three of the five fail; the two fallback tests already pass
because no hint support exists yet.

- [ ] **Step 11: Teach replay to use hints**

`build` gains, for each immutable file id: try `hint::read_file(hint_path(dir, id))`,
and on `Ok(hints)` insert an `Entry { file_id: id, pos, len }` per hint without
opening the log. On any `Err`, `tracing::debug!` and fall through to
`replay_file`. The active file always goes through `replay_file`.

A hint file **must not be trusted to be complete relative to other files** — the
per-file ordering is unchanged, so a hinted file's entries still overwrite lower
ids and are still overwritten by higher ones.

- [ ] **Step 12: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add crates/kvs-engine/src/hint.rs crates/kvs-engine/src/record.rs \
        crates/kvs-engine/src/replay.rs crates/kvs-engine/src/lib.rs
git commit -m "feat(engine): add hint files and prefer them during replay"
```

---

## Task 6: The merge

**Files:**
- Create: `crates/kvs-engine/src/merge.rs`
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Store`, `Stats`, `layout::*`, `hint::*`, `record::{decode, HEADER_LEN}`.
- Produces:
  - `MergeOutcome { files_merged, bytes_reclaimed, duration_ms }` — public
  - `merge::run(dir, store, stats, output_id, after_snapshot) -> Result<MergeOutcome>`
  - `Engine::roll` promoted to `pub(crate)`, plus `pub(crate)` accessors
    `Engine::store()` and `Engine::stats()`

**Why the merge runs synchronously in this task.** The thread arrives in Task 8.
Calling `run` directly from tests means every assertion is deterministic and the
two concurrency rules are exercised by a hook the test controls rather than by
timing. When the thread lands, it calls exactly this function.

**Records are copied, not re-encoded.** The merge preads the record, validates it
with `record::decode`, and then writes the *original bytes*. Re-encoding would be
wasted work and would make the output's size depend on bincode's stability. A
consequence worth knowing for the tests: merging a file whose records are all
live reclaims exactly zero bytes.

**A `Command::Remove` found in the snapshot is corruption.** The keydir only ever
holds entries for `Set` records. If one decodes to a `Remove`, warn and drop it —
the same treatment as a bad checksum.

- [ ] **Step 1: Write the failing tests**

`crates/kvs-engine/src/merge.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Command, Engine, EngineConfig, EngineError, Reader,
        keydir::Entry,
        layout::{hint_path, log_ids, log_path, test_support::set},
        record,
    };
    use std::{fs::File, io::Write, path::Path, sync::Arc};
    use tempfile::TempDir;

    fn no_op() -> impl Fn() {
        || {}
    }

    /// Append `cmd` to the active log and update the keydir, exactly as
    /// `Engine::set`/`Engine::remove` would. Used from merge hooks to simulate a
    /// write landing while a merge is in flight.
    fn write_to_active(
        dir: &Path,
        store: &Arc<RwLock<Store>>,
        active_id: u32,
        cmd: &Command,
    ) {
        let path = log_path(dir, active_id);
        let bytes = record::encode(cmd).expect("encode");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .expect("open active");
        let pos = file.metadata().expect("metadata").len();
        file.write_all(&bytes).expect("append");
        file.sync_data().expect("sync");

        let mut guard = store.write().expect("lock");
        guard
            .files
            .entry(active_id)
            .or_insert_with(|| Arc::new(File::open(&path).expect("reopen")));

        match cmd {
            Command::Set { key, .. } => {
                let entry = Entry {
                    file_id: active_id,
                    pos,
                    len: (bytes.len() - record::HEADER_LEN) as u32,
                };
                guard.key_dir.insert(key.clone(), entry);
            }
            Command::Remove { key } => {
                guard.key_dir.remove(key);
            }
        }
    }

    #[test]
    fn a_merge_rewrites_only_the_live_records() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");
        engine.set("beta".into(), "three".into()).expect("set");

        let output_id = engine.roll().expect("roll");
        let outcome = run(
            dir.path(),
            engine.store(),
            engine.stats(),
            output_id,
            &no_op(),
        )
        .expect("merge");

        assert_eq!(outcome.files_merged, 1);
        assert_eq!(engine.get("alpha").expect("alpha"), "two");
        assert_eq!(engine.get("beta").expect("beta"), "three");
    }

    #[test]
    fn a_merge_drops_the_records_of_deleted_keys() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("beta".into(), "two".into()).expect("set");
        engine.remove("alpha").expect("remove");

        let output_id = engine.roll().expect("roll");
        run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
            .expect("merge");

        assert!(matches!(engine.get("alpha"), Err(EngineError::KeyNotFound)));
        assert_eq!(engine.get("beta").expect("beta"), "two");

        // The tombstone and the record it killed are both gone from disk.
        let merged = std::fs::read(log_path(dir.path(), output_id)).expect("read");
        assert!(!merged.windows(5).any(|w| w == b"alpha"));
    }

    #[test]
    fn a_merge_leaves_only_its_output_and_the_active_file() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine =
            Engine::open_with(dir.path(), EngineConfig { max_file_bytes: 8, ..Default::default() })
                .expect("open");
        for i in 0..4 {
            engine.set(format!("key-{i}"), "value".into()).expect("set");
        }
        assert_eq!(log_ids(dir.path()).expect("ids").len(), 4);

        let output_id = engine.roll().expect("roll");
        let outcome = run(
            dir.path(),
            engine.store(),
            engine.stats(),
            output_id,
            &no_op(),
        )
        .expect("merge");

        assert_eq!(outcome.files_merged, 4);
        assert_eq!(
            log_ids(dir.path()).expect("ids"),
            vec![output_id, output_id + 1]
        );
    }

    #[test]
    fn a_merge_writes_a_hint_file_for_its_output() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");

        let output_id = engine.roll().expect("roll");
        run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
            .expect("merge");

        let hints = crate::hint::read_file(&hint_path(dir.path(), output_id))
            .expect("hints must be readable");
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].key, "alpha");
    }

    #[test]
    fn a_merge_of_entirely_live_records_reclaims_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("beta".into(), "two".into()).expect("set");

        let output_id = engine.roll().expect("roll");
        let outcome = run(
            dir.path(),
            engine.store(),
            engine.stats(),
            output_id,
            &no_op(),
        )
        .expect("merge");

        assert_eq!(
            outcome.bytes_reclaimed, 0,
            "records are copied verbatim, so nothing shrinks"
        );
    }

    #[test]
    fn a_merge_reports_and_reclaims_the_space_of_dead_records() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        for _ in 0..10 {
            engine
                .set("alpha".into(), "a-value-of-some-length".into())
                .expect("set");
        }
        let before = engine.stats().total_bytes();

        let output_id = engine.roll().expect("roll");
        let outcome = run(
            dir.path(),
            engine.store(),
            engine.stats(),
            output_id,
            &no_op(),
        )
        .expect("merge");

        assert!(outcome.bytes_reclaimed > 0);
        assert_eq!(engine.stats().total_bytes(), before - outcome.bytes_reclaimed);
        assert_eq!(engine.stats().dead_bytes(), 0);
        assert_eq!(engine.get("alpha").expect("alpha"), "a-value-of-some-length");
    }

    #[test]
    fn a_value_written_during_a_merge_is_not_overwritten_by_the_patch() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "old".into()).expect("set");
        engine.set("beta".into(), "untouched".into()).expect("set");

        let output_id = engine.roll().expect("roll");
        let active_id = output_id + 1;
        let store = Arc::clone(engine.store());
        let hook_dir = dir.path().to_path_buf();
        let hook_store = Arc::clone(&store);
        let hook = move || {
            write_to_active(&hook_dir, &hook_store, active_id, &set("alpha", "new"));
        };

        run(dir.path(), &store, engine.stats(), output_id, &hook).expect("merge");

        let reader = engine.reader();
        assert_eq!(reader.get("alpha").expect("alpha"), "new");
        let entry = *store.read().expect("lock").key_dir.get("alpha").expect("alpha");
        assert_eq!(entry.file_id, active_id, "the patch must not claw it back");
        assert_eq!(reader.get("beta").expect("beta"), "untouched");
    }

    #[test]
    fn a_key_removed_during_a_merge_does_not_come_back() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("beta".into(), "two".into()).expect("set");

        let output_id = engine.roll().expect("roll");
        let active_id = output_id + 1;
        let store = Arc::clone(engine.store());
        let hook_dir = dir.path().to_path_buf();
        let hook_store = Arc::clone(&store);
        let hook = move || {
            write_to_active(
                &hook_dir,
                &hook_store,
                active_id,
                &Command::Remove { key: "alpha".into() },
            );
        };

        run(dir.path(), &store, engine.stats(), output_id, &hook).expect("merge");

        let reader = engine.reader();
        assert!(
            matches!(reader.get("alpha"), Err(EngineError::KeyNotFound)),
            "the merge inserted a key that had been removed"
        );
        assert_eq!(reader.get("beta").expect("beta"), "two");
    }

    #[test]
    fn a_merge_drops_a_record_that_fails_its_checksum() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("beta".into(), "two".into()).expect("set");

        let output_id = engine.roll().expect("roll");

        // File 0 is immutable now, so rewriting it in place is safe. Corrupt the
        // payload of whichever record holds beta.
        let entry = *engine.store().read().expect("lock").key_dir.get("beta").expect("beta");
        let path = log_path(dir.path(), entry.file_id);
        let mut bytes = std::fs::read(&path).expect("read");
        bytes[entry.pos as usize + record::HEADER_LEN] ^= 0xff;
        std::fs::write(&path, &bytes).expect("rewrite");

        run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
            .expect("merge");

        assert_eq!(engine.get("alpha").expect("alpha"), "one");
        assert!(matches!(engine.get("beta"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn a_handle_captured_before_a_merge_still_reads_after_the_unlink() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");

        let captured = Arc::clone(
            engine.store().read().expect("lock").files.get(&0).expect("file 0"),
        );
        let original_len = captured.metadata().expect("metadata").len();

        let output_id = engine.roll().expect("roll");
        run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
            .expect("merge");

        assert!(!log_path(dir.path(), 0).exists() || output_id == 0);
        assert_eq!(
            captured.metadata().expect("metadata").len(),
            original_len,
            "an open fd keeps the inode alive across unlink"
        );
    }

    #[test]
    fn a_merged_store_reopens_with_the_same_contents() {
        let dir = TempDir::new().expect("tempdir");
        {
            let mut engine = Engine::open(dir.path()).expect("open");
            for i in 0..20 {
                engine.set(format!("key-{i}"), format!("v{i}")).expect("set");
            }
            for i in 0..10 {
                engine.remove(&format!("key-{i}")).expect("remove");
            }
            for i in 0..20 {
                engine.set(format!("key-{i}"), format!("final-{i}")).expect("set");
            }

            let output_id = engine.roll().expect("roll");
            run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
                .expect("merge");
            engine.sync().expect("sync");
        }

        let engine = Engine::open(dir.path()).expect("reopen");
        for i in 0..20 {
            assert_eq!(
                engine.get(&format!("key-{i}")).expect("get"),
                format!("final-{i}")
            );
        }
    }

    #[test]
    fn a_reader_cloned_before_a_merge_sees_the_merged_addresses() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        let reader: Reader = engine.reader();

        let output_id = engine.roll().expect("roll");
        run(dir.path(), engine.store(), engine.stats(), output_id, &no_op())
            .expect("merge");

        assert_eq!(reader.get("alpha").expect("alpha"), "one");
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine merge::`
Expected: FAIL — `merge` is not a module, and `Engine::roll`/`store`/`stats`
are not reachable.

- [ ] **Step 3: Expose what the merge needs from `Engine`**

In `engine.rs`, promote `roll` to `pub(crate) fn roll(&mut self) -> Result<u32>`
and add:

```rust
    pub(crate) fn store(&self) -> &Arc<RwLock<Store>> { /* your code */ }
    pub(crate) fn stats(&self) -> &Arc<Stats> { /* your code */ }
```

`stats()` returning `&Arc<Stats>` rather than `&Stats` is what lets Task 8 clone
it for the thread. `&Arc<Stats>` coerces to `&Stats` at the `run` call site
through deref, so the tests above compile against either — but the thread needs
the `Arc`.

- [ ] **Step 4: Write `merge.rs`**

Declare `mod merge;` in `lib.rs` and re-export `MergeOutcome`.

```rust
use crate::{
    Command, Result,
    hint::{self, Hint},
    keydir::Entry,
    layout::{self, log_path},
    record::{self, HEADER_LEN},
    stats::Stats,
    store::Store,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::{Arc, RwLock},
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeOutcome {
    pub files_merged: usize,
    pub bytes_reclaimed: u64,
    pub duration_ms: u64,
}

/// Rewrite every log file with an id at or below `output_id` into a single file,
/// then swap it in. `after_snapshot` runs once, after the keydir snapshot is
/// taken and before the patch is applied; it is a no-op in production and a
/// hook for tests that need a write to land mid-merge.
///
/// The hook is a bare `&dyn Fn()` with no `Send`/`Sync` bound: `run` always
/// executes on whichever thread called it, so the bound would buy nothing and
/// would rule out the most useful kind of test hook — see Task 8.
pub(crate) fn run(
    dir: &Path,
    store: &Arc<RwLock<Store>>,
    stats: &Stats,
    output_id: u32,
    after_snapshot: &dyn Fn(),
) -> Result<MergeOutcome> { /* your code */ }
```

The body, in the order the spec fixes:

1. **Snapshot.** `layout::log_ids(dir)?`, keep those `<= output_id` — these are
   the inputs; sum their file lengths. Then, in one read-lock critical section,
   collect `Vec<(String, Entry)>` for every entry with `entry.file_id <= output_id`,
   clone each file's `Arc<File>` you will need, and read `stats.dead_bytes()`.
   Drop the guard before doing any I/O.
2. `after_snapshot()`.
3. **Rewrite.** Create `layout::tmp_log_path(dir, output_id)`, wrap it in a
   `BufWriter`. For each snapshotted entry: `read_exact_at` a
   `HEADER_LEN + entry.len` buffer, `record::decode` it, and on `Ok(Command::Set { .. })`
   write the buffer verbatim and record a `Hint { key, pos, len }` at the new
   offset. A decode error or a `Command::Remove` is a `tracing::warn!` and a skip.
4. **Durable.** `flush`, then `sync_data` on the file under the writer. Write the
   hints to `layout::tmp_hint_path(dir, output_id)` with `hint::write_file`.
5. **Swap.** Open a read handle on the tmp log. Take the write lock and, without
   releasing it: for each `(key, new_entry)` apply the table from the spec —
   absent means skip, `current.file_id > output_id` means skip, otherwise
   replace; then remove `files` entries for every input id below `output_id` and
   insert the tmp handle under `output_id`. Drop the guard.
6. **Clean up.** Unlink every input `<id>.log` and `<id>.hint`, `output_id`
   included, then fsync `dir`. Then rename tmp hint to `hint_path` and tmp log to
   `log_path`, and fsync `dir` again.
7. **Report.** `stats.reclaim(dead_at_snapshot, input_bytes - output_bytes)`,
   `tracing::info!` the outcome, and return it.

Two things to get exactly right:

**The swap's critical section must not do I/O.** Open the tmp read handle
*before* taking the write lock. Opening a file inside the lock stalls every
reader on a syscall for no reason.

**Fsyncing a directory needs a handle on the directory itself:**
`File::open(dir)?.sync_all()?`. Opening a directory as a `File` works on Unix and
is the only way to make a rename or unlink durable. Factor it into
`layout::sync_dir(dir: &Path) -> Result<()>` — Task 7's recovery needs it too.

- [ ] **Step 5: Run them to verify they pass**

Run: `cargo test -p kvs-engine merge::`
Expected: PASS, 12 tests.

If `a_value_written_during_a_merge_is_not_overwritten_by_the_patch` fails, the
patch is replacing entries unconditionally. If
`a_key_removed_during_a_merge_does_not_come_back` fails, it is inserting rather
than replacing. Those are the two halves of the rule and they fail
independently — which is the reason both tests exist.

- [ ] **Step 6: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/kvs-engine/src/merge.rs crates/kvs-engine/src/engine.rs \
        crates/kvs-engine/src/lib.rs
git commit -m "feat(engine): merge the immutable log prefix into one file"
```

---

## Task 7: Recovering an interrupted merge

**Files:**
- Modify: `crates/kvs-engine/src/layout.rs`
- Modify: `crates/kvs-engine/src/engine.rs`

**Interfaces:**
- Consumes: `layout::{log_ids, log_path, hint_path, tmp_log_path, tmp_hint_path}`.
- Produces:
  - `layout::sync_dir(dir: &Path) -> Result<()>` (extracted in Task 6)
  - `layout::tmp_log_ids(dir: &Path) -> Result<Vec<u32>>`
  - `layout::recover_interrupted_merge(dir: &Path) -> Result<()>`, called by
    `Engine::open_with` before `replay::build`

**Why this task exists.** Task 6 orders its cleanup so that a crash always lands
in a recoverable state. This task is the other half: the code that recognises
which state it woke up in. Without it, Task 6's ordering is a promise nothing
keeps.

**The state that must never be entered** is the merged output committed
*alongside* surviving input files. Replay order is `0, 1, …, output, active`, so
a key that still exists is corrected by the merged file. But a key that was
*deleted* is not: the merged file deliberately holds nothing for it, so a stale
`Set` in a surviving input has nothing to mask it and the key comes back. That is
what `a_key_deleted_before_an_interrupted_merge_does_not_come_back` defends, and
it is the single most important test in this plan.

**The decision table**, read entirely off the filesystem:

| `<id>.log.tmp` | `<id>.log` | Conclusion | Action |
|---|---|---|---|
| present | present | Cleanup never started | Delete the tmp pair; inputs are authoritative |
| present | absent | Cleanup started | The tmp is the only complete copy: unlink every surviving `<j>.log`/`<j>.hint` for `j < id`, then rename the tmp pair into place |
| absent | — | The merge committed | Delete any stray `*.hint.tmp` |

- [ ] **Step 1: Write the failing tests**

In `layout.rs`'s `mod tests`:

```rust
    use crate::{Command, hint::{self, Hint}, record};
    use test_support::{locate, set, write_log};

    /// Write `cmds` as a merge output that has been fsynced but not renamed.
    fn write_tmp_merge(dir: &Path, id: u32, cmds: &[Command]) {
        use std::io::Write;
        let mut file = std::fs::File::create(tmp_log_path(dir, id)).expect("create tmp");
        let mut hints = Vec::new();
        let mut offset = 0u64;
        for cmd in cmds {
            let bytes = record::encode(cmd).expect("encode");
            file.write_all(&bytes).expect("write");
            if let Command::Set { key, .. } = cmd {
                hints.push(Hint {
                    key: key.clone(),
                    pos: offset,
                    len: (bytes.len() - record::HEADER_LEN) as u32,
                });
            }
            offset += bytes.len() as u64;
        }
        file.flush().expect("flush");
        hint::write_file(&tmp_hint_path(dir, id), &hints).expect("write hints");
    }

    #[test]
    fn tmp_log_ids_finds_uncommitted_merge_outputs() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(tmp_log_path(dir.path(), 7), b"").expect("write");
        std::fs::write(log_path(dir.path(), 0), b"").expect("write");
        std::fs::write(tmp_hint_path(dir.path(), 7), b"").expect("write");
        assert_eq!(tmp_log_ids(dir.path()).expect("ids"), vec![7]);
    }

    #[test]
    fn recovery_is_a_no_op_on_a_directory_with_no_tmp_files() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[set("beta", "two")]);

        recover_interrupted_merge(dir.path()).expect("recover");
        assert_eq!(log_ids(dir.path()).expect("ids"), vec![0, 1]);
    }

    #[test]
    fn an_uncommitted_merge_is_discarded_while_its_output_name_is_taken() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[set("beta", "two")]);
        write_log(dir.path(), 2, &[set("gamma", "three")]);
        write_tmp_merge(dir.path(), 1, &[set("alpha", "one"), set("beta", "two")]);

        recover_interrupted_merge(dir.path()).expect("recover");

        assert_eq!(log_ids(dir.path()).expect("ids"), vec![0, 1, 2]);
        assert!(!tmp_log_path(dir.path(), 1).exists());
        assert!(!tmp_hint_path(dir.path(), 1).exists());
        assert!(!hint_path(dir.path(), 1).exists(), "the tmp hint must not be promoted");
    }

    #[test]
    fn an_interrupted_merge_is_finished_when_its_output_name_is_free() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 2, &[set("gamma", "three")]);
        write_tmp_merge(dir.path(), 1, &[set("alpha", "one"), set("beta", "two")]);

        recover_interrupted_merge(dir.path()).expect("recover");

        assert_eq!(
            log_ids(dir.path()).expect("ids"),
            vec![1, 2],
            "the surviving input must be removed and the tmp promoted"
        );
        assert!(hint_path(dir.path(), 1).exists(), "the hint is promoted too");
        assert!(!tmp_log_path(dir.path(), 1).exists());
        assert!(!tmp_hint_path(dir.path(), 1).exists());
    }

    #[test]
    fn finishing_an_interrupted_merge_removes_the_inputs_hint_files_too() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        hint::write_file(
            &hint_path(dir.path(), 0),
            &[Hint { key: "alpha".into(), pos: 0, len: 8 }],
        )
        .expect("write hint");
        write_log(dir.path(), 2, &[set("gamma", "three")]);
        write_tmp_merge(dir.path(), 1, &[set("alpha", "one")]);

        recover_interrupted_merge(dir.path()).expect("recover");

        assert!(!hint_path(dir.path(), 0).exists(), "a stale hint would outlive its log");
    }

    #[test]
    fn recovery_leaves_files_above_the_output_id_alone() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 2, &[set("gamma", "three")]);
        write_log(dir.path(), 3, &[set("delta", "four")]);
        write_tmp_merge(dir.path(), 1, &[set("alpha", "one")]);

        recover_interrupted_merge(dir.path()).expect("recover");
        assert_eq!(log_ids(dir.path()).expect("ids"), vec![1, 2, 3]);
    }

    #[test]
    fn a_stray_tmp_hint_is_removed_when_no_merge_is_pending() {
        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        hint::write_file(
            &tmp_hint_path(dir.path(), 0),
            &[Hint { key: "alpha".into(), pos: 0, len: 8 }],
        )
        .expect("write hint");

        recover_interrupted_merge(dir.path()).expect("recover");
        assert!(!tmp_hint_path(dir.path(), 0).exists());
        assert_eq!(log_ids(dir.path()).expect("ids"), vec![0]);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p kvs-engine layout::`
Expected: FAIL — `tmp_log_ids` and `recover_interrupted_merge` are not defined.

- [ ] **Step 3: Write the recovery functions**

```rust
/// Every `<id>.log.tmp` in `dir`, ascending. There should be at most one.
pub(crate) fn tmp_log_ids(dir: &Path) -> Result<Vec<u32>> { /* your code */ }

/// Bring `dir` to a state replay can trust. Safe to call on a clean directory.
pub(crate) fn recover_interrupted_merge(dir: &Path) -> Result<()> { /* your code */ }
```

Implement the decision table directly. Two details:

- **Follow the same ordering the merge uses**: unlink survivors and `sync_dir`,
  then rename hint, then rename log, then `sync_dir`. Recovery can itself be
  interrupted, and reusing the ordering makes it idempotent — running it again on
  the partial result reaches the same conclusion.
- **More than one `<id>.log.tmp` means something is wrong** (one merge at a time
  is enforced by the `merge_pending` flag in Task 8). `tracing::warn!` and process them in
  ascending order rather than erroring; a store that refuses to open is worse
  than one that cleans up conservatively.

- [ ] **Step 4: Run them to verify they pass**

Run: `cargo test -p kvs-engine layout::`
Expected: PASS, 11 tests.

- [ ] **Step 5: Write the failing end-to-end test**

In `engine.rs`'s `mod tests`:

```rust
    #[test]
    fn a_key_deleted_before_an_interrupted_merge_does_not_come_back() {
        use crate::layout::{hint_path, log_path, tmp_hint_path, tmp_log_path};
        use crate::layout::test_support::{locate, set, write_log};

        let dir = TempDir::new().expect("tempdir");

        // File 0 is a stale input holding a Set for a key that was later deleted.
        write_log(dir.path(), 0, &[set("alpha", "resurrected"), set("beta", "two")]);
        // File 2 is the active file.
        write_log(dir.path(), 2, &[set("gamma", "three")]);

        // The merge wrote its output — which, correctly, omits alpha — and then
        // crashed after unlinking 1.log but before the rename.
        let merged = [set("beta", "two")];
        write_log(dir.path(), 1, &merged);
        std::fs::rename(log_path(dir.path(), 1), tmp_log_path(dir.path(), 1))
            .expect("stage the tmp log");
        let (pos, len) = locate(&merged, 0);
        crate::hint::write_file(
            &tmp_hint_path(dir.path(), 1),
            &[crate::hint::Hint { key: "beta".into(), pos, len }],
        )
        .expect("write the tmp hint");

        let engine = Engine::open(dir.path()).expect("open");

        assert!(
            matches!(engine.get("alpha"), Err(EngineError::KeyNotFound)),
            "a deleted key came back from a stale input file"
        );
        assert_eq!(engine.get("beta").expect("beta"), "two");
        assert_eq!(engine.get("gamma").expect("gamma"), "three");
        assert!(!hint_path(dir.path(), 0).exists());
    }

    #[test]
    fn opening_a_store_with_an_uncommitted_merge_uses_the_inputs() {
        use crate::layout::{log_ids, tmp_log_path};
        use crate::layout::test_support::{set, write_log};

        let dir = TempDir::new().expect("tempdir");
        write_log(dir.path(), 0, &[set("alpha", "one")]);
        write_log(dir.path(), 1, &[set("beta", "two")]);
        std::fs::write(tmp_log_path(dir.path(), 1), b"a half-written merge")
            .expect("write tmp");

        let engine = Engine::open(dir.path()).expect("open");

        assert_eq!(engine.get("alpha").expect("alpha"), "one");
        assert_eq!(engine.get("beta").expect("beta"), "two");
        assert_eq!(log_ids(dir.path()).expect("ids"), vec![0, 1]);
    }
```

The first test's staged hint must list `beta` at its real offset, and the reason
is worth holding onto: **an empty hint file is a valid hint file.** Replay cannot
tell one apart from a complete one, so a hint that under-reports its log silently
hides records — `beta` would simply vanish. Nothing in production can produce
that state, because the merge writes the log and the hint from the same loop, but
it does mean "hints are a cache" has a precise limit: a hint is trusted to be
*complete*, and only its framing is verified. If you ever add hints for rolled
files (spec section 9), that is the invariant to be careful with.

Also note `locate` is imported from `test_support` here — the same helper the
replay tests use to compute a record's address without hardcoding byte counts.

- [ ] **Step 6: Run them to verify they fail**

Run: `cargo test -p kvs-engine engine::`
Expected: FAIL — `Engine::open_with` does not call recovery.

- [ ] **Step 7: Call recovery from `open_with`**

`layout::recover_interrupted_merge(dir)?` goes immediately after
`create_dir_all` and before `replay::build`. Nothing else changes.

- [ ] **Step 8: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/kvs-engine/src/layout.rs crates/kvs-engine/src/engine.rs
git commit -m "feat(engine): recover an interrupted merge at startup"
```

---

## Task 8: The merge thread, the trigger, and `compact()`

**Files:**
- Modify: `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/error.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `merge::{run, MergeOutcome}`, `Stats`, `Store`.
- Produces:
  - `EngineError::MergeInProgress`
  - `Engine::compact(&mut self) -> Result<MergeOutcome>`
  - a merge thread spawned by `open_with`, joined on `Drop`
  - `pub(crate) Engine::merge_pending(&self) -> bool` for tests
  - `pub(crate) Engine::open_with_hook(path, config, hook: Box<dyn Fn() + Send>)`

**The spec was wrong about how "one merge at a time" is enforced**, and this is
the task that discovers it. The spec claimed a capacity-1 channel was the entire
policy. It is not, for two reasons:

1. **The channel is empty while a merge runs.** Once the thread takes the
   request, `try_send` succeeds again, so a second merge queues behind the first.
2. **Worse, the writer would roll a file for nothing.** `output_id` is the id of
   the file the writer just rolled, so the roll must happen *before* the send. A
   store sitting above its dead-byte threshold re-evaluates the trigger on every
   single append, so a failed `try_send` after a successful roll means one new
   empty log file per write.

The fix is an explicit `merge_pending: Arc<AtomicBool>`. The writer claims it with
`swap(true)` and only then rolls and sends; the merge thread clears it when it
finishes, whether it succeeded or failed. The channel goes back to being just a
way to carry the request. This also makes `compact()`'s `409` deterministic:
the flag is set synchronously on the writer thread, so there is no window in
which a caller sees the wrong answer.

**`Drop` joins the merge thread.** Take the sender out of its `Option` so the
thread's `recv` returns `Err` and it exits, then join. The hook lives inside the
thread's closure, so it is dropped there too. Without this, process exit
can land in the middle of a merge — survivable, since Task 7's recovery handles
exactly that, but there is no reason to rely on it during an orderly shutdown.
This mirrors the `join_handle.join()` already in `main.rs`.

- [ ] **Step 1: Add the error variant**

In `error.rs`:

```rust
    #[error("a merge is already in progress")]
    MergeInProgress,
```

And a case in `errors_describe_themselves`:

```rust
        assert_eq!(
            EngineError::MergeInProgress.to_string(),
            "a merge is already in progress"
        );
```

- [ ] **Step 2: Write the failing tests**

In `engine.rs`'s `mod tests`:

```rust
    use crate::layout::{log_ids, tmp_log_ids};
    use std::sync::{Arc, mpsc};

    /// Poll `predicate` until it holds or the budget runs out. Used only for the
    /// genuinely asynchronous property — that an automatic merge eventually
    /// happens with nobody asking. The budget is generous so a slow machine does
    /// not produce a false failure.
    fn wait_until(predicate: impl Fn() -> bool) -> bool {
        for _ in 0..200 {
            if predicate() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        false
    }

    fn eager_merges() -> EngineConfig {
        EngineConfig { min_merge_bytes: 0, dead_ratio: 0.4, ..EngineConfig::default() }
    }

    #[test]
    fn compact_on_an_empty_store_merges_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");

        let outcome = engine.compact().expect("compact");
        assert_eq!(outcome.bytes_reclaimed, 0);
        assert!(tmp_log_ids(dir.path()).expect("ids").is_empty());
    }

    #[test]
    fn compact_reclaims_the_space_of_overwritten_records() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        for i in 0..20 {
            engine.set("alpha".into(), format!("value-number-{i}")).expect("set");
        }
        let before = engine.stats().total_bytes();

        let outcome = engine.compact().expect("compact");

        assert!(outcome.bytes_reclaimed > 0);
        assert!(engine.stats().total_bytes() < before);
        assert_eq!(engine.get("alpha").expect("alpha"), "value-number-19");
        assert!(tmp_log_ids(dir.path()).expect("ids").is_empty());
    }

    #[test]
    fn compact_is_safe_to_call_repeatedly() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");

        engine.compact().expect("first");
        let second = engine.compact().expect("second");
        let third = engine.compact().expect("third");

        assert_eq!(second.bytes_reclaimed, 0, "nothing dead is left to reclaim");
        assert_eq!(third.bytes_reclaimed, 0);
        assert_eq!(engine.get("alpha").expect("alpha"), "two");
    }

    #[test]
    fn compact_survives_a_reopen() {
        let dir = TempDir::new().expect("tempdir");
        {
            let mut engine = Engine::open(dir.path()).expect("open");
            for i in 0..10 {
                engine.set(format!("key-{i}"), "first".into()).expect("set");
            }
            for i in 0..10 {
                engine.set(format!("key-{i}"), format!("second-{i}")).expect("set");
            }
            engine.remove("key-0").expect("remove");
            engine.compact().expect("compact");
            engine.sync().expect("sync");
        }

        let engine = Engine::open(dir.path()).expect("reopen");
        assert!(matches!(engine.get("key-0"), Err(EngineError::KeyNotFound)));
        for i in 1..10 {
            assert_eq!(
                engine.get(&format!("key-{i}")).expect("get"),
                format!("second-{i}")
            );
        }
    }

    #[test]
    fn a_ratio_crossing_write_triggers_a_merge_with_nobody_asking() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open_with(dir.path(), eager_merges()).expect("open");

        engine.set("alpha".into(), "a-reasonably-long-value".into()).expect("set");
        let peak = engine.stats().total_bytes();
        engine.set("alpha".into(), "a-reasonably-long-value".into()).expect("overwrite");

        assert!(
            wait_until(|| engine.stats().total_bytes() < peak * 2 && !engine.merge_pending()),
            "no automatic merge happened"
        );
        assert_eq!(engine.get("alpha").expect("alpha"), "a-reasonably-long-value");
        assert!(tmp_log_ids(dir.path()).expect("ids").is_empty());
    }

    #[test]
    fn a_store_above_the_threshold_does_not_roll_a_file_per_write() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open_with(dir.path(), eager_merges()).expect("open");
        for _ in 0..30 {
            engine.set("alpha".into(), "value".into()).expect("set");
        }

        assert!(wait_until(|| !engine.merge_pending()), "merges never settled");
        let files = log_ids(dir.path()).expect("ids").len();
        assert!(files < 10, "{files} log files for 30 writes to one key");
    }

    #[test]
    fn compact_reports_merge_in_progress_while_one_is_pending() {
        let dir = TempDir::new().expect("tempdir");
        let (gate_tx, gate_rx) = mpsc::channel::<()>();
        let hook: Box<dyn Fn() + Send> = Box::new(move || {
            // Hold the merge open until the test lets it go.
            let _ = gate_rx.recv();
        });

        let mut engine =
            Engine::open_with_hook(dir.path(), eager_merges(), hook).expect("open");

        // These two writes cross the ratio, so `set` claims merge_pending
        // synchronously before returning.
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");
        assert!(engine.merge_pending(), "the trigger did not fire");

        assert!(
            matches!(engine.compact(), Err(EngineError::MergeInProgress)),
            "a second merge was allowed to start"
        );

        gate_tx.send(()).expect("release the merge");
        assert!(wait_until(|| !engine.merge_pending()), "the merge never finished");
        assert_eq!(engine.get("alpha").expect("alpha"), "two");
    }

    #[test]
    fn a_write_that_lands_during_a_merge_survives_it() {
        let dir = TempDir::new().expect("tempdir");
        let (entered_tx, entered_rx) = mpsc::channel::<()>();
        let (gate_tx, gate_rx) = mpsc::channel::<()>();
        let hook: Box<dyn Fn() + Send> = Box::new(move || {
            entered_tx.send(()).expect("announce");
            let _ = gate_rx.recv();
        });

        let mut engine =
            Engine::open_with_hook(dir.path(), eager_merges(), hook).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");

        // The merge has taken its snapshot and is parked in the hook.
        entered_rx.recv().expect("merge must reach the hook");
        engine.set("alpha".into(), "three".into()).expect("write during the merge");
        engine.set("beta".into(), "new-key".into()).expect("write during the merge");
        gate_tx.send(()).expect("release the merge");

        assert!(wait_until(|| !engine.merge_pending()), "the merge never finished");
        assert_eq!(engine.get("alpha").expect("alpha"), "three");
        assert_eq!(engine.get("beta").expect("beta"), "new-key");
    }

    #[test]
    fn a_remove_that_lands_during_a_merge_is_not_undone() {
        let dir = TempDir::new().expect("tempdir");
        let (entered_tx, entered_rx) = mpsc::channel::<()>();
        let (gate_tx, gate_rx) = mpsc::channel::<()>();
        let hook: Box<dyn Fn() + Send> = Box::new(move || {
            entered_tx.send(()).expect("announce");
            let _ = gate_rx.recv();
        });

        let mut engine =
            Engine::open_with_hook(dir.path(), eager_merges(), hook).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.set("alpha".into(), "two".into()).expect("overwrite");

        entered_rx.recv().expect("merge must reach the hook");
        engine.remove("alpha").expect("remove during the merge");
        gate_tx.send(()).expect("release the merge");

        assert!(wait_until(|| !engine.merge_pending()), "the merge never finished");
        assert!(matches!(engine.get("alpha"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn a_dropped_engine_leaves_a_directory_that_reopens_cleanly() {
        let dir = TempDir::new().expect("tempdir");
        {
            let mut engine = Engine::open_with(dir.path(), eager_merges()).expect("open");
            for i in 0..10 {
                engine.set("alpha".into(), format!("value-{i}")).expect("set");
            }
            engine.sync().expect("sync");
        }

        assert!(
            tmp_log_ids(dir.path()).expect("ids").is_empty(),
            "Drop must join the merge thread rather than abandon a tmp file"
        );
        let engine = Engine::open(dir.path()).expect("reopen");
        assert_eq!(engine.get("alpha").expect("alpha"), "value-9");
    }
```

- [ ] **Step 3: Run them to verify they fail**

Run: `cargo test -p kvs-engine engine::`
Expected: FAIL — `compact`, `merge_pending`, and `open_with_hook` are not
defined.

- [ ] **Step 4: Spawn the merge thread**

`Engine` gains four fields:

```rust
    merge_tx: Option<std::sync::mpsc::SyncSender<MergeRequest>>,
    merge_handle: Option<std::thread::JoinHandle<()>>,
    merge_pending: Arc<std::sync::atomic::AtomicBool>,
```

**The hook is `Box<dyn Fn() + Send>` and `Engine` does not store it** — it is
moved straight into the merge thread at spawn. That is forced rather than chosen.
`std::sync::mpsc::Sender` and `Receiver` are `Send` but **not `Sync`**, so a
closure capturing either is not `Sync`, and `Arc<T>` is `Send` only when
`T: Send + Sync`. Holding the hook in an `Arc` behind those bounds would
therefore reject exactly the channel-based hooks the tests below depend on. A
`Box` moved into the thread needs only `Send`, which those closures satisfy.

and the request type is private to `engine.rs`:

```rust
struct MergeRequest {
    output_id: u32,
    reply: Option<std::sync::mpsc::Sender<Result<MergeOutcome>>>,
}
```

`open_with_hook` builds everything and spawns:

```rust
    pub(crate) fn open_with_hook(
        path: impl AsRef<Path>,
        config: EngineConfig,
        hook: Box<dyn Fn() + Send>,
    ) -> Result<Engine> { /* your code */ }
```

with `open_with(path, config)` delegating to it with `Box::new(|| {})`, and
`open(path)` delegating to `open_with` with `EngineConfig::default()`.

The thread's loop: `while let Ok(request) = rx.recv()`, call
`merge::run(&dir, &store, &stats, request.output_id, &*hook)`, log an error if it
failed, send the result if `reply` is `Some`, then **clear `merge_pending`
regardless of the outcome** and loop. Clearing it in exactly one place — after
the result has been sent — is what keeps a failed merge from wedging the store.

- [ ] **Step 5: Add the trigger and `compact`**

```rust
    /// Merge now, blocking until it commits.
    pub fn compact(&mut self) -> Result<MergeOutcome> { /* your code */ }

    /// Fire a merge if the dead-byte ratio warrants one. Never blocks, never fails.
    fn maybe_merge(&mut self) { /* your code */ }

    pub(crate) fn merge_pending(&self) -> bool { /* your code */ }
```

Both `compact` and `maybe_merge` follow the same claim-then-act order, and the
order is the point:

1. `merge_pending.swap(true)`. If it was already `true`, stop — `compact` returns
   `MergeInProgress`, `maybe_merge` returns quietly.
2. `roll()`. **If it fails, clear the flag before propagating**, or the store
   never merges again.
3. `try_send`. **If it fails, clear the flag.**

`maybe_merge` is called from `append` after `maybe_roll`, guarded by
`self.stats.should_merge(self.config.min_merge_bytes, self.config.dead_ratio)`.
`compact` skips the ratio check entirely — an explicit request is not a
heuristic — and blocks on `reply_rx.recv()`, mapping a disconnect to
`EngineError::ShuttingDown`.

- [ ] **Step 6: Implement `Drop`**

```rust
impl Drop for Engine {
    fn drop(&mut self) { /* your code */ }
}
```

`self.merge_tx.take()` and let it fall out of scope, then
`self.merge_handle.take()` and join it, ignoring a panic (a panicking merge
thread has already been logged, and panicking inside `Drop` during an unwind
aborts the process).

- [ ] **Step 7: Run them to verify they pass**

Run: `cargo test -p kvs-engine engine::`
Expected: PASS.

If `compact_reports_merge_in_progress_while_one_is_pending` hangs, `compact`
is blocking on the reply before checking the flag. If
`a_store_above_the_threshold_does_not_roll_a_file_per_write` fails, the roll is
happening before the flag is claimed.

- [ ] **Step 8: Run the whole suite**

Run: `cargo test --workspace && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/kvs-engine/src/engine.rs crates/kvs-engine/src/error.rs \
        crates/kvs-engine/src/lib.rs
git commit -m "feat(engine): merge on a background thread and expose compact()"
```

---

## Task 9: The server surface

**Files:**
- Modify: `crates/kvs-server/src/config.rs`
- Modify: `crates/kvs-server/src/dto.rs`
- Modify: `crates/kvs-server/src/error.rs`
- Modify: `crates/kvs-server/src/writer.rs`
- Modify: `crates/kvs-server/src/routes.rs`
- Modify: `crates/kvs-server/src/main.rs`
- Test: `crates/kvs-server/tests/api.rs`

**Interfaces:**
- Consumes: `EngineConfig`, `MergeOutcome`, `EngineError::MergeInProgress`.
- Produces: `POST /v1/admin/compact`; `Config` fields `max_file_bytes`,
  `dead_ratio`, `min_merge_bytes`; `KvHandle::compact`.

**Why this is last and why it is small.** Everything hard is done. What remains
is the observability the Kubernetes runs asked for: the merge is the first engine
operation that can report what it actually did, and right now nothing about this
process's internals is visible from outside it.

`compact` goes through the writer channel like `set` and `remove`, because
`Engine::compact` takes `&mut self` and the writer thread is the only thing that
holds an `Engine`. It is not a `spawn_blocking` call on a `Reader` — that path is
for reads only.

- [ ] **Step 1: Write the failing config test**

In `crates/kvs-server/src/config.rs`'s `mod tests`, extend the existing
`flags_set_every_field` and add:

```rust
    #[test]
    fn the_compaction_defaults_match_the_engine() {
        let config = Config::parse_from(["kvs-server"]);
        assert_eq!(config.max_file_bytes, 64 * 1024 * 1024);
        assert_eq!(config.dead_ratio, 0.5);
        assert_eq!(config.min_merge_bytes, 1024 * 1024);
    }

    #[test]
    fn the_compaction_flags_are_settable() {
        let config = Config::parse_from([
            "kvs-server",
            "--max-file-bytes", "1024",
            "--dead-ratio", "0.25",
            "--min-merge-bytes", "512",
        ]);
        assert_eq!(config.max_file_bytes, 1024);
        assert_eq!(config.dead_ratio, 0.25);
        assert_eq!(config.min_merge_bytes, 512);
    }
```

- [ ] **Step 2: Add the flags**

```rust
    #[arg(long, env = "KVS_MAX_FILE_BYTES", default_value_t = 64 * 1024 * 1024)]
    pub max_file_bytes: u64,

    #[arg(long, env = "KVS_DEAD_RATIO", default_value_t = 0.5)]
    pub dead_ratio: f64,

    #[arg(long, env = "KVS_MIN_MERGE_BYTES", default_value_t = 1024 * 1024)]
    pub min_merge_bytes: u64,
```

Run: `cargo test -p kvs-server config::` — Expected: PASS.

- [ ] **Step 3: Write the failing API tests**

In `crates/kvs-server/tests/api.rs`, following the existing harness:

```rust
#[tokio::test]
async fn compact_reports_what_it_reclaimed() {
    let (addr, _guard) = spawn_server().await;
    let client = reqwest::Client::new();

    for i in 0..20 {
        let response = client
            .put(format!("http://{addr}/v1/kv/alpha"))
            .json(&serde_json::json!({ "value": format!("value-number-{i}") }))
            .send()
            .await
            .expect("put");
        assert_eq!(response.status(), 204);
    }

    let response = client
        .post(format!("http://{addr}/v1/admin/compact"))
        .send()
        .await
        .expect("compact");
    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.expect("json");
    assert!(body["bytes_reclaimed"].as_u64().expect("bytes_reclaimed") > 0);
    assert!(body["files_merged"].as_u64().expect("files_merged") >= 1);
    assert!(body["duration_ms"].is_number());

    // The store still serves the live value afterwards.
    let response = client
        .get(format!("http://{addr}/v1/kv/alpha"))
        .send()
        .await
        .expect("get");
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["value"], "value-number-19");
}

#[tokio::test]
async fn compact_on_a_fresh_store_reclaims_nothing() {
    let (addr, _guard) = spawn_server().await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{addr}/v1/admin/compact"))
        .send()
        .await
        .expect("compact");
    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["bytes_reclaimed"], 0);
}

#[tokio::test]
async fn compact_does_not_lose_a_deleted_key() {
    let (addr, _guard) = spawn_server().await;
    let client = reqwest::Client::new();

    for key in ["alpha", "beta"] {
        client
            .put(format!("http://{addr}/v1/kv/{key}"))
            .json(&serde_json::json!({ "value": "one" }))
            .send()
            .await
            .expect("put");
    }
    let response = client
        .delete(format!("http://{addr}/v1/kv/alpha"))
        .send()
        .await
        .expect("delete");
    assert_eq!(response.status(), 204);

    client
        .post(format!("http://{addr}/v1/admin/compact"))
        .send()
        .await
        .expect("compact");

    let response = client
        .get(format!("http://{addr}/v1/kv/alpha"))
        .send()
        .await
        .expect("get");
    assert_eq!(response.status(), 404);

    let response = client
        .get(format!("http://{addr}/v1/kv/beta"))
        .send()
        .await
        .expect("get");
    assert_eq!(response.status(), 200);
}
```

- [ ] **Step 4: Run them to verify they fail**

Run: `cargo test -p kvs-server --test api`
Expected: FAIL with `404` — the route does not exist.

- [ ] **Step 5: Add the DTO**

In `dto.rs`:

```rust
#[derive(Debug, serde::Serialize)]
pub struct CompactResponse {
    pub files_merged: usize,
    pub bytes_reclaimed: u64,
    pub duration_ms: u64,
}
```

- [ ] **Step 6: Map the new error**

In `error.rs`, add the arm to the existing exhaustive match:

```rust
            EngineError::MergeInProgress => (
                StatusCode::CONFLICT,
                "merge_in_progress",
                "a merge is already in progress".to_string(),
            ),
```

`409` and not `503`: the request was well-formed and the server is healthy. The
client's own action is simply already happening, and retrying later will work.

- [ ] **Step 7: Route the request through the writer**

In `writer.rs`, add to `Request`:

```rust
    Compact {
        reply: oneshot::Sender<Result<MergeOutcome, EngineError>>,
    },
```

and to `KvHandle`:

```rust
    pub async fn compact(&self) -> Result<MergeOutcome, EngineError> { /* your code */ }
```

`KvHandle::compact` cannot use the existing `dispatch`, which is typed to
`oneshot::Sender<Result<(), EngineError>>`. Either widen `dispatch` with a
generic reply type or write the four lines out — both are defensible; the
four lines are simpler and this is the only caller that needs a payload back.

In `spawn_with`'s loop, add:

```rust
                Request::Compact { reply } => {
                    let engine_response = engine.compact();
                    let _ = reply.send(engine_response);
                }
```

**This blocks the writer loop for the duration of the merge**, so `set` and
`remove` queue behind it in the bounded channel. That is the documented cost of
an explicitly requested compaction, and it is why the automatic trigger does not
go through this path.

- [ ] **Step 8: Add the route and handler**

In `routes.rs`:

```rust
        .route("/v1/admin/compact", post(compact))
```

The handler takes `State(handle): State<KvHandle>`, awaits `handle.compact()`,
and returns `Result<Json<CompactResponse>, AppError>`. Note that
`MAX_VALUE_BYTES`-based `DefaultBodyLimit` layer still applies — the route takes
no body, and that is fine.

- [ ] **Step 9: Build the `EngineConfig` in `main`**

```rust
    let engine_config = EngineConfig {
        fsync: config.fsync.into(),
        max_file_bytes: config.max_file_bytes,
        dead_ratio: config.dead_ratio,
        min_merge_bytes: config.min_merge_bytes,
    };
```

- [ ] **Step 10: Run everything**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add crates/kvs-server/src crates/kvs-server/tests/api.rs
git commit -m "feat(server): add POST /v1/admin/compact and compaction config"
```

---

## Done

The store reclaims its own space, on its own, and can be asked to do it on
demand. Worth doing once the suite is green, in the cluster rather than in
`cargo test`:

```bash
./deploy/reload.sh
kubectl port-forward -n kvs svc/kvs 3000:3000 &
for i in $(seq 1 500); do
  curl -s -o /dev/null -X PUT localhost:3000/v1/kv/alpha \
    -H 'content-type: application/json' -d '{"value":"overwritten"}'
done
kubectl exec -n kvs kvs-0 -- ls -la /var/lib/kvs
curl -s -X POST localhost:3000/v1/admin/compact
kubectl exec -n kvs kvs-0 -- ls -la /var/lib/kvs
```

Add what you observe to `deploy/README.md`. It is also the load generator
experiment 5 wanted — and with 500 overwrites of one key, the hint file and the
merged log are both small enough to read by hand, which makes the format
concrete in a way a unit test does not.

**What this leaves for replication.** Unchanged from the spec: per-replica
storage, headless DNS, ordered rollout, and `/ready` as the catch-up gate are all
in place from the Kubernetes project. This project adds the two things a replica's
catch-up path will want — a file-ordered replay it can resume, and the
established pattern that the `Store` behind one `RwLock` is the shared-state
boundary. The record format is untouched, so the sequence number replication
needs can be added without migrating anything written before it.
