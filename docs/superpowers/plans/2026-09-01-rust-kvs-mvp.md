# rust-kvs MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **For this project, that does not apply.** The human is writing every line of
> implementation code; this is a learning exercise. The plan supplies the
> failing test, the exact signatures the implementation must satisfy, and the
> reasoning behind each design choice. It never supplies an implementation
> body. If you are an agent reading this: do not write the implementation.

**Goal:** A single-node, log-structured key/value store with a REST API — `GET`, `PUT`, and `DELETE` over an append-only log with an in-memory index, crash recovery, and a documented consistency model.

**Architecture:** Bitcask-style storage: every mutation appends a CRC-framed record to `0.log` and updates an in-memory keydir mapping each key to a byte offset. Writes serialize through a bounded channel onto one owner thread that holds the `Engine` exclusively; reads bypass that thread entirely, taking a brief read lock on the shared keydir and then doing positioned reads on a shared file handle.

**Tech Stack:** Rust 2024 edition, `serde` + `bincode` (log payloads), `crc32fast`, `thiserror`, `tokio`, `axum`, `clap`, `tracing`. Dev: `tempfile`, `tower`, `reqwest`.

**Spec:** `docs/superpowers/specs/2026-09-01-rust-kvs-design.md` — read it alongside this plan. Every "why" below is argued from that document.

## Global Constraints

- **Rust 1.98.0**, edition **2024**, cargo workspace with two member crates.
- **`kvs-engine` must not depend on `tokio` or `axum`.** This is the replication seam and Task 1 asserts it mechanically.
- Log file: `<data-dir>/0.log`. Record framing: `[crc32: u32][payload_len: u32][payload]`, little-endian, CRC over the payload only.
- `Entry.pos` is the offset of the **record header**, not the payload. `Entry.len` is the payload length, excluding the 8-byte header.
- The keydir insert is the commit point and happens **strictly after** the bytes are durable.
- `MAX_KEY_BYTES = 1024`, `MAX_VALUE_BYTES = 1_048_576`.
- Error envelope, everywhere in the HTTP layer: `{"error": {"code": "...", "message": "..."}}`.
- Every task: write the test, watch it fail, implement, watch it pass, then `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test` before committing.
- Commit at the end of every task. Small commits are the point.

## File Structure

```
Cargo.toml                              workspace manifest, shared dep versions
crates/kvs-engine/
  Cargo.toml
  src/lib.rs                            module decls + the crate's public surface
  src/error.rs                          EngineError, Result alias
  src/command.rs                        Command enum — the unit of mutation
  src/record.rs                         framing: encode/decode/CRC          (crate-private)
  src/keydir.rs                         Entry, KeyDir                       (crate-private)
  src/engine.rs                         Engine: open, recover, set/get/remove, fsync policy
  src/reader.rs                         Reader: cloneable read handle
  tests/recovery.rs                     reopen + torn-tail integration tests
crates/kvs-server/
  Cargo.toml
  src/main.rs                           wiring: config, tracing, spawn, serve
  src/config.rs                         Config (clap Parser), FsyncArg
  src/writer.rs                         Request, KvHandle, channel, spawn
  src/error.rs                          AppError + IntoResponse
  src/dto.rs                            request/response body types
  src/routes.rs                         router + the four handlers
  tests/writer.rs                       writer-thread integration tests
  tests/concurrency.rs                  N writers + M readers
  tests/api.rs                          HTTP tests via ServiceExt::oneshot
```

`record` and `keydir` are crate-private with their unit tests in-file; nothing
outside the engine needs to know the byte layout. `Command`, `Engine`,
`Reader`, `EngineError`, and `FsyncPolicy` are the public surface.

---

# Phase 0 — Skeleton

### Task 1: Workspace and the crate boundary

**Files:**
- Create: `Cargo.toml`, `crates/kvs-engine/Cargo.toml`, `crates/kvs-engine/src/lib.rs`, `crates/kvs-server/Cargo.toml`, `crates/kvs-server/src/main.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: two compiling crates; `kvs_engine` is on `kvs-server`'s dependency list.

**Why this task exists:** the "no tokio in the engine" rule is the whole reason
for a workspace. Asserting it now, with a command, means it cannot rot later.

- [ ] **Step 1: Write the workspace manifest**

```toml
# Cargo.toml
[workspace]
resolver = "3"
members = ["crates/kvs-engine", "crates/kvs-server"]

[workspace.package]
edition = "2024"
rust-version = "1.98"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bincode = { version = "2", features = ["serde"] }
crc32fast = "1"
thiserror = "2"
anyhow = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync"] }
axum = "0.8"
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tempfile = "3"
tower = { version = "0.5", features = ["util"] }
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }
```

`resolver = "3"` is the edition-2024 default and what makes per-target feature
unification behave. Pinning versions once in `[workspace.dependencies]` means
the two crates can never drift onto different `serde` majors.

- [ ] **Step 2: Write the two crate manifests**

```toml
# crates/kvs-engine/Cargo.toml
[package]
name = "kvs-engine"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
serde = { workspace = true }
bincode = { workspace = true }
crc32fast = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
```

```toml
# crates/kvs-server/Cargo.toml
[package]
name = "kvs-server"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
kvs-engine = { path = "../kvs-engine" }
axum = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
clap = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
tower = { workspace = true }
reqwest = { workspace = true }
serde_json = { workspace = true }
```

Note what is absent from `kvs-engine`: no `tokio`, no `axum`. That absence is
load-bearing.

- [ ] **Step 3: Write the placeholder sources**

```rust
// crates/kvs-engine/src/lib.rs
#[cfg(test)]
mod tests {
    #[test]
    fn the_crate_compiles() {}
}
```

```rust
// crates/kvs-server/src/main.rs
fn main() {
    println!("kvs-server");
}
```

- [ ] **Step 4: Verify the build and the crate boundary**

Run:
```bash
cargo build --workspace
cargo test --workspace
! cargo tree -p kvs-engine --edges normal | grep -qE '^\s*[|`-]+\s+(tokio|axum)' && echo "BOUNDARY OK"
```
Expected: build and tests pass; the last command prints `BOUNDARY OK`. If it
does not, something pulled an async runtime into the engine — find it with
`cargo tree -p kvs-engine -i tokio` before continuing.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/
git commit -m "chore: scaffold kvs-engine and kvs-server workspace"
```

---

# Phase 1 — Engine, single-threaded

No threads, no async, no HTTP. The engine is built and fully tested as an
ordinary synchronous library first, because a storage bug found under
concurrency costs ten times what it costs here.

### Task 2: Command and EngineError

**Files:**
- Create: `crates/kvs-engine/src/command.rs`, `crates/kvs-engine/src/error.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `Command` (public, `Serialize + Deserialize + Debug + Clone + PartialEq`), `EngineError`, `kvs_engine::Result<T>`.

**Why this task exists:** `Command` is the seam the spec keeps open for
replication. Defining it first, before anything writes it to disk, keeps it a
description of *what happened* rather than a description of a file format.

- [ ] **Step 1: Write the failing test**

```rust
// crates/kvs-engine/src/command.rs
use serde::{Deserialize, Serialize};

// ... your Command definition goes here ...

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_compare_by_value() {
        let a = Command::Set { key: "alpha".into(), value: "one".into() };
        let b = Command::Set { key: "alpha".into(), value: "one".into() };
        let c = Command::Remove { key: "alpha".into() };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
```

```rust
// crates/kvs-engine/src/error.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_describe_themselves() {
        let err = EngineError::Corrupt { offset: 4096 };
        assert_eq!(err.to_string(), "corrupt record at offset 4096");
        assert_eq!(EngineError::KeyNotFound.to_string(), "key not found");
    }

    #[test]
    fn io_errors_convert_automatically() {
        let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "nope");
        let err: EngineError = io.into();
        assert!(matches!(err, EngineError::Io(_)));
    }
}
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine`
Expected: FAIL — `cannot find type Command`, `cannot find type EngineError`.

- [ ] **Step 3: Implement**

Write these two modules yourself. The signatures they must produce:

```rust
// command.rs
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
```
Derive `Debug, Clone, PartialEq, Serialize, Deserialize`.

```rust
// error.rs
pub enum EngineError {
    Io(std::io::Error),                 // #[from]
    Corrupt { offset: u64 },
    KeyNotFound,
    KeyTooLarge { len: usize },
    ValueTooLarge { len: usize },
    ShuttingDown,
}

pub type Result<T> = std::result::Result<T, EngineError>;
```

Use `thiserror::Error` with `#[error("...")]` on each variant; the two messages
the test pins are `"corrupt record at offset {offset}"` and `"key not found"`.
`#[from]` on the `Io` variant is what makes the second test pass and what lets
you write `?` over `std::io` calls for the rest of the project.

Delete the `the_crate_compiles` placeholder test from `lib.rs` while you are
there — it did its job in Task 1, and leaving it makes every test count below
off by one.

Deliberately **no** `anyhow` here. A library that erases its error type forces
every caller — including the HTTP layer, which must choose a status code — to
guess. Add the `Result` alias and re-export everything from `lib.rs`:

```rust
// lib.rs
mod command;
mod error;

pub use command::Command;
pub use error::{EngineError, Result};
```

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine/src
git commit -m "feat(engine): add Command and EngineError"
```

### Task 3: Record framing

**Files:**
- Create: `crates/kvs-engine/src/record.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Command`, `EngineError`, `Result`.
- Produces (all `pub(crate)`):
  - `const HEADER_LEN: usize = 8`
  - `fn encode(cmd: &Command) -> Result<Vec<u8>>` — a complete record, header first
  - `fn decode(record: &[u8], offset: u64) -> Result<Command>` — takes a complete record; `offset` is only for error reporting
  - `fn payload_len(header: &[u8; HEADER_LEN]) -> u32`

**Why this task exists:** this is the byte layer, and it is the one place where
a mistake is silent — a wrong offset or an unchecked length reads plausible
garbage rather than failing. Testing it in isolation, before any file exists,
is what makes the recovery tests in Task 7 trustworthy.

**Design note:** the CRC covers the payload only. Verifying it requires the
payload bytes *and* the checksum that precedes them, which is exactly why
`Entry.pos` points at the header — one positioned read of `HEADER_LEN + len`
bytes gets both.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kvs-engine/src/record.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, EngineError};

    fn sample() -> Command {
        Command::Set { key: "alpha".into(), value: "one".into() }
    }

    #[test]
    fn a_record_round_trips() {
        let cmd = sample();
        let bytes = encode(&cmd).expect("encode");
        assert_eq!(decode(&bytes, 0).expect("decode"), cmd);
    }

    #[test]
    fn a_remove_record_round_trips() {
        let cmd = Command::Remove { key: "alpha".into() };
        let bytes = encode(&cmd).expect("encode");
        assert_eq!(decode(&bytes, 0).expect("decode"), cmd);
    }

    #[test]
    fn the_header_reports_the_payload_length() {
        let bytes = encode(&sample()).expect("encode");
        let header: [u8; HEADER_LEN] = bytes[..HEADER_LEN].try_into().expect("header");
        assert_eq!(payload_len(&header) as usize, bytes.len() - HEADER_LEN);
    }

    #[test]
    fn a_tampered_payload_is_rejected() {
        let mut bytes = encode(&sample()).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the payload, leave the checksum intact
        let err = decode(&bytes, 4096).expect_err("a bad checksum must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 4096 }));
    }

    #[test]
    fn a_record_shorter_than_its_header_claims_is_rejected() {
        let bytes = encode(&sample()).expect("encode");
        let truncated = &bytes[..bytes.len() - 1];
        let err = decode(truncated, 0).expect_err("a short record must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 0 }));
    }

    #[test]
    fn a_buffer_smaller_than_a_header_is_rejected() {
        let err = decode(&[0u8; 3], 0).expect_err("a runt buffer must be rejected");
        assert!(matches!(err, EngineError::Corrupt { offset: 0 }));
    }
}
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine record`
Expected: FAIL — `cannot find function encode in this scope`.

- [ ] **Step 3: Implement**

Write `record.rs`. What each function owes you:

- `encode` — serialize the `Command` to a payload, compute `crc32fast::hash`
  over that payload, then build a `Vec<u8>` of `crc.to_le_bytes()`,
  `(payload.len() as u32).to_le_bytes()`, and the payload.
- `payload_len` — read `u32::from_le_bytes` from bytes `4..8`.
- `decode` — reject buffers shorter than `HEADER_LEN`; read the stored CRC and
  length; reject when `record.len() != HEADER_LEN + len`; hash the payload and
  reject on mismatch; then deserialize. Every rejection is
  `EngineError::Corrupt { offset }`.

For the payload encoding, `bincode` 2's serde bridge:
`bincode::serde::encode_to_vec(cmd, bincode::config::standard())` and
`bincode::serde::decode_from_slice::<Command, _>(payload, bincode::config::standard())`
— the decode returns `(value, bytes_read)`, so destructure it. Check the exact
names on docs.rs when you add the dependency; if the 2.x API has moved, pinning
`bincode = "1.3"` and using `bincode::serialize` / `bincode::deserialize` is a
fine fallback and changes nothing else in this plan. A `bincode` error maps to
`Corrupt { offset }`, not to `Io`.

Then wire it up in `lib.rs`:
```rust
mod record;
```
(no `pub use` — the byte layout stays inside the crate.)

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine record`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine/src
git commit -m "feat(engine): add CRC-framed log record encoding"
```

### Task 4: Engine — open, set, get

**Files:**
- Create: `crates/kvs-engine/src/keydir.rs`, `crates/kvs-engine/src/engine.rs`
- Modify: `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Command`, `record::{encode, HEADER_LEN}`, `EngineError`, `Result`.
- Produces:
  - `pub(crate) struct Entry { file_id: u32, pos: u64, len: u32, timestamp: u64 }`, deriving `Clone, Copy, Debug`
  - `pub(crate) type KeyDir = std::collections::HashMap<String, Entry>`
  - `pub struct Engine`
  - `pub fn Engine::open(dir: impl AsRef<Path>) -> Result<Engine>`
  - `pub fn Engine::set(&mut self, key: String, value: String) -> Result<()>`
  - `pub fn Engine::get(&self, key: &str) -> Result<String>`

**Why this task exists:** this is the first time the three pieces meet — a file,
a framing, and an index. Note the signatures: `set` takes `&mut self` and `get`
takes `&self`. That asymmetry is not decoration. It is the type system carrying
the concurrency design, and Task 10 collects on it.

**Design notes:**
- `open` creates the directory if missing, then opens `0.log` with
  `.create(true).read(true).append(true)`.
- `get` returns `Result<String>` with `EngineError::KeyNotFound`, not
  `Option<String>`. The HTTP layer needs one error type to map to status codes,
  and a `None` that must be converted at every call site invites divergence.
- Write order, and this is the invariant the whole design rests on: append the
  bytes, flush, fsync, **then** insert into the keydir. Never the reverse.
- `timestamp` is millis since the Unix epoch (`SystemTime::now()`). Nothing
  reads it for correctness yet; compaction will.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kvs-engine/src/engine.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use tempfile::TempDir;

    fn open_temp() -> (TempDir, Engine) {
        let dir = TempDir::new().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("open");
        (dir, engine)
    }

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
        engine.set("alpha".into(), "two".into()).expect("second set");
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
        let err = engine.get("ghost").expect_err("an unset key must not resolve");
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
        assert!(nested.join("0.log").exists(), "the log file must exist after open");
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
}
```

That last test is the one that pins the design: an append-only log never edits
in place, so overwriting a key must make the file grow. It is also the test
that will eventually motivate compaction.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine engine`
Expected: FAIL — `cannot find type Engine in this scope`.

- [ ] **Step 3: Implement**

Write `keydir.rs` (just the two type definitions, no behaviour) and
`engine.rs`. The engine holds, at minimum: the data directory path, a
`BufWriter<File>` for appends, the current write offset, a read handle, and the
`KeyDir`.

Two things worth getting right the first time:

- Track the write offset yourself in a field rather than calling
  `stream_position()` per write. A `BufWriter` buffers, so the file's real
  length lags your logical offset — and the offset you record in the `Entry`
  must be the logical one.
- Implement `Drop` (or flush at the end of every `set`) so a dropped engine
  cannot strand buffered bytes. Task 6 reopens the log after a drop and will
  catch you if you skip this.

Wire up `lib.rs`:
```rust
mod engine;
mod keydir;

pub use engine::Engine;
```

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine engine`
Expected: PASS, 7 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine/src
git commit -m "feat(engine): add Engine with open, set, and get"
```

### Task 5: Remove

**Files:**
- Modify: `crates/kvs-engine/src/engine.rs`

**Interfaces:**
- Consumes: everything from Task 4.
- Produces: `pub fn Engine::remove(&mut self, key: &str) -> Result<()>`

**Design note:** removing an absent key returns `KeyNotFound` **without**
appending anything. A tombstone for a key that was never there is pure garbage
in a log that cannot be edited — it costs disk forever and buys nothing. Check
the keydir first, append the tombstone second.

- [ ] **Step 1: Write the failing tests**

```rust
// append inside the existing `mod tests` in crates/kvs-engine/src/engine.rs

    #[test]
    fn remove_makes_a_key_unreadable() {
        let (_dir, mut engine) = open_temp();
        engine.set("alpha".into(), "one".into()).expect("set");
        engine.remove("alpha").expect("remove");
        let err = engine.get("alpha").expect_err("a removed key must not resolve");
        assert!(matches!(err, EngineError::KeyNotFound));
    }

    #[test]
    fn remove_on_an_unknown_key_reports_key_not_found() {
        let (_dir, mut engine) = open_temp();
        let err = engine.remove("ghost").expect_err("removing nothing must fail");
        assert!(matches!(err, EngineError::KeyNotFound));
    }

    #[test]
    fn a_rejected_remove_writes_nothing_to_the_log() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let log = dir.path().join("0.log");

        engine.set("alpha".into(), "one".into()).expect("set");
        let before = std::fs::metadata(&log).expect("metadata").len();
        engine.remove("ghost").expect_err("removing nothing must fail");
        let after = std::fs::metadata(&log).expect("metadata").len();

        assert_eq!(before, after, "a rejected remove must not append a tombstone");
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
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine engine`
Expected: FAIL — `no method named remove found for struct Engine`.

- [ ] **Step 3: Implement**

Add `remove`. It appends a `Command::Remove` record and erases the key from the
keydir — same order as `set`: bytes durable first, index second.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine engine`
Expected: PASS, 12 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine/src
git commit -m "feat(engine): add remove with tombstone records"
```

### Task 6: Recovery — replaying the log on open

**Files:**
- Create: `crates/kvs-engine/tests/recovery.rs`
- Modify: `crates/kvs-engine/src/engine.rs`

**Interfaces:**
- Consumes: `Engine::{open, set, get, remove}`, `EngineError`.
- Produces: no new API — `open` gains its replay behaviour.

**Why this is an integration test:** it lives in `tests/` and touches only the
public API, which means it is also a check that the public API is sufficient.
If you need a crate-private detail to write this test, the API is wrong.

**Design note:** replay applies records in file order — `Set` inserts, `Remove`
erases. Because later records overwrite earlier ones, the keydir you end up
with is exactly the state at the last complete write. That is the entire
recovery algorithm; there is no separate metadata to keep in sync, which is
precisely why this design is worth learning.

- [ ] **Step 1: Write the failing test**

```rust
// crates/kvs-engine/tests/recovery.rs
use kvs_engine::{Engine, EngineError};
use tempfile::TempDir;

#[test]
fn state_survives_a_reopen() {
    let dir = TempDir::new().expect("tempdir");

    {
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set alpha");
        engine.set("beta".into(), "two".into()).expect("set beta");
        engine.set("alpha".into(), "three".into()).expect("overwrite alpha");
        engine.set("gamma".into(), "four".into()).expect("set gamma");
        engine.remove("gamma").expect("remove gamma");
    } // dropped: the writer must have flushed

    let engine = Engine::open(dir.path()).expect("reopen");
    assert_eq!(engine.get("alpha").expect("get"), "three", "the last write wins");
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
        engine.set("beta".into(), "two".into()).expect("set after reopen");
    }

    let engine = Engine::open(dir.path()).expect("third open");
    assert_eq!(engine.get("alpha").expect("get"), "one");
    assert_eq!(engine.get("beta").expect("get"), "two");
}

#[test]
fn opening_an_empty_directory_yields_an_empty_store() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    assert!(matches!(engine.get("anything"), Err(EngineError::KeyNotFound)));
}
```

The second test is the one that catches the subtle bug: after replay, the write
offset must resume at the end of the existing log, not at zero. Get that wrong
and the second `set` overwrites the first record — silently, since `append`
mode will mask it in some configurations and not others.

- [ ] **Step 2: Run the test and watch it fail**

Run: `cargo test -p kvs-engine --test recovery`
Expected: FAIL — `state_survives_a_reopen` panics at `get`, because `open`
currently ignores the file's contents.

- [ ] **Step 3: Implement**

Add replay to `open`. Read sequentially from offset 0: read `HEADER_LEN` bytes,
take `payload_len`, read that many payload bytes, `record::decode` the whole
record, apply it to the keydir with `pos` set to the record's start offset, and
advance. Set the write offset to the end of the last good record when you are
done.

For now, a malformed record may return an error from `open`. Task 7 replaces
that with truncation — the tests there will tell you when you have it right.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine`
Expected: PASS — 21 tests in the lib target, 3 in the `recovery` target. Cargo
reports each test binary separately; there is no combined total.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine
git commit -m "feat(engine): rebuild the keydir by replaying the log on open"
```

### Task 7: Crash safety — discarding a torn tail

**Files:**
- Modify: `crates/kvs-engine/tests/recovery.rs`, `crates/kvs-engine/src/engine.rs`

**Interfaces:**
- Consumes: everything from Task 6.
- Produces: no new API — `open` gains truncation behaviour.

**Why this task exists:** this is the payoff for the CRC and the length prefix,
and it is the difference between a toy and a store you would trust. A process
killed mid-append leaves a partial record. Without this, the next `open` either
fails forever or — worse — reads the partial record as real data.

**Design note:** scanning stops at the first record that is not intact: a header
that runs past EOF, a short payload, a CRC mismatch, or a payload that will not
decode. The file is then truncated to the offset where *that record began*, and
the writer resumes there. Everything before the tear is untouched.

- [ ] **Step 1: Write the failing tests**

```rust
// append to crates/kvs-engine/tests/recovery.rs
use std::fs::OpenOptions;
use std::io::Write;

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
        let mut f = OpenOptions::new().append(true).open(&log).expect("reopen for append");
        f.write_all(&0u32.to_le_bytes()).expect("crc");
        f.write_all(&64u32.to_le_bytes()).expect("length claiming 64 payload bytes");
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
    engine.set("gamma".into(), "three".into()).expect("set after recovery");
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
    assert_eq!(engine.get("alpha").expect("get"), "one", "records before the corruption survive");
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
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine --test recovery`
Expected: FAIL — `Engine::open` returns `Err(Corrupt { .. })` where the tests
require it to recover.

- [ ] **Step 3: Implement**

Change replay so a bad record ends the scan instead of failing the open. Track
the offset at which each record starts; on the first failure, call
`set_len(start_of_bad_record)` on the file, position the writer there, and stop
scanning. A short read at EOF is the same case as a CRC failure — both mean
"the log ends here".

Log the truncation with `eprintln!` for now; Task 12 replaces that with
`tracing::warn!`. Silently discarding data is not acceptable even when it is
the right thing to do.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine`
Expected: PASS — 21 in the lib target, 5 in `recovery`.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine
git commit -m "feat(engine): truncate a torn log tail during recovery"
```

### Task 8: Size limits and the fsync policy

**Files:**
- Modify: `crates/kvs-engine/src/engine.rs`, `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `pub const MAX_KEY_BYTES: usize = 1024`
  - `pub const MAX_VALUE_BYTES: usize = 1_048_576`
  - `pub enum FsyncPolicy { Always, Never }`, deriving `Clone, Copy, Debug, Default, PartialEq, Eq` with `Always` as `#[default]`
  - `pub fn Engine::open_with(dir: impl AsRef<Path>, fsync: FsyncPolicy) -> Result<Engine>` (`open` delegates with `FsyncPolicy::default()`)

**Design note:** the HTTP layer will also enforce these limits, at the edge,
before a request ever reaches the channel. That is not redundant — the engine's
check is an invariant of the store (it holds for every caller, including a
future replication layer), and the HTTP check is an early rejection that avoids
buffering a body nobody wants. Two checks, two different jobs.

Limits are measured in **bytes**, via `key.len()`, not in `chars`. A key of 400
emoji is over a 1 KiB byte limit, and the byte count is what the log stores.

- [ ] **Step 1: Write the failing tests**

```rust
// append inside the existing `mod tests` in crates/kvs-engine/src/engine.rs

    #[test]
    fn set_rejects_an_oversized_key() {
        let (_dir, mut engine) = open_temp();
        let key = "k".repeat(MAX_KEY_BYTES + 1);
        let err = engine.set(key, "one".into()).expect_err("an oversized key must be rejected");
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
        engine.set(key.clone(), "one".into()).expect("a key at the limit is legal");
        assert_eq!(engine.get(&key).expect("get"), "one");
    }

    #[test]
    fn limits_are_measured_in_bytes_not_characters() {
        let (_dir, mut engine) = open_temp();
        // 'é' is two bytes in UTF-8, so this is over the limit despite being
        // MAX_KEY_BYTES characters long.
        let key = "é".repeat(MAX_KEY_BYTES);
        let err = engine.set(key, "one".into()).expect_err("byte length is what counts");
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
            let mut engine =
                Engine::open_with(dir.path(), FsyncPolicy::Never).expect("open");
            engine.set("alpha".into(), "one".into()).expect("set");
        }
        let engine = Engine::open(dir.path()).expect("reopen");
        assert_eq!(engine.get("alpha").expect("get"), "one");
    }

    #[test]
    fn the_default_policy_is_always() {
        assert_eq!(FsyncPolicy::default(), FsyncPolicy::Always);
    }
```

`a_rejected_write_leaves_the_log_untouched` is the important one. Validating
after the append would still return the right error to the caller while leaving
a record on disk that replay would happily apply on the next open — a bug that
only shows up after a restart.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine engine`
Expected: FAIL — `cannot find value MAX_KEY_BYTES in this scope`.

- [ ] **Step 3: Implement**

Add the constants, the `FsyncPolicy` enum, and `open_with`. Validate at the top
of `set` before touching the writer. In the write path, call `sync_data()` on
the file after flushing the `BufWriter` when the policy is `Always`, and skip it
when `Never`.

`sync_data()` rather than `sync_all()`: you need the file's contents durable,
not its metadata, and the metadata sync is the more expensive half.

Export the new names from `lib.rs`:
```rust
pub use engine::{Engine, FsyncPolicy, MAX_KEY_BYTES, MAX_VALUE_BYTES};
```

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine`
Expected: PASS — 28 in the lib target, 5 in `recovery`.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine
git commit -m "feat(engine): enforce size limits and add a configurable fsync policy"
```

**Phase 1 is complete.** You have a durable, crash-safe, fully tested key/value
store with no threads and no network in it. Run `cargo test -p kvs-engine` once
more and read the test names as a list — that list is the specification of what
the engine promises.

---

# Phase 2 — Concurrency

The engine now grows a read handle that can be cloned across threads, and moves
onto a thread of its own. Still no HTTP.

### Task 9: Reader — a cloneable read handle

**Files:**
- Create: `crates/kvs-engine/src/reader.rs`
- Modify: `crates/kvs-engine/src/engine.rs`, `crates/kvs-engine/src/keydir.rs`, `crates/kvs-engine/src/lib.rs`

**Interfaces:**
- Consumes: `Entry`, `KeyDir`, `record::{decode, HEADER_LEN}`, `EngineError`.
- Produces:
  - `pub struct Reader` — `Clone + Send + Sync`, holding `Arc<RwLock<KeyDir>>` and `Arc<File>`
  - `pub fn Reader::get(&self, key: &str) -> Result<String>`
  - `pub fn Engine::reader(&self) -> Reader`
- Changed: `Engine` now stores its keydir as `Arc<RwLock<KeyDir>>`. `Engine::get` delegates to an internal `Reader`, so its signature and behaviour are unchanged and every Phase 1 test must still pass untouched.

**Why this task exists:** this is the split that makes reads scale. Reads stop
being an engine method that needs the engine and become a self-contained handle
that needs only the index and the file.

**Design notes — the three rules that make this correct:**

1. **Copy the `Entry` out, then drop the guard, then do I/O.** `Entry` is
   `Copy` and 24 bytes; holding a read lock across a syscall would serialize
   every reader behind the slowest disk access in the process.
2. **Use positioned reads.** `FileExt::read_at(&mut buf, pos)` (from
   `std::os::unix::fs`; `seek_read` in `std::os::windows::fs`) does not touch
   the file's cursor, which is what makes a single `Arc<File>` safe to share
   across threads with no lock and no per-read `open`. A `seek` + `read` pair
   on a shared handle is a data race in disguise — two threads interleaving
   between the seek and the read read each other's offsets.
3. **Read `HEADER_LEN + entry.len` bytes at `entry.pos`.** One syscall gets the
   checksum and the payload it covers. Verify that the header's length matches
   `entry.len` before trusting anything.

`std::sync::RwLock` here, not `tokio::sync::RwLock`: this code is synchronous
and will be called from inside `spawn_blocking`. An async lock in a blocking
context is the wrong tool and will not compile into anything you want.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kvs-engine/src/reader.rs
#[cfg(test)]
mod tests {
    use crate::{Engine, EngineError};
    use tempfile::TempDir;

    #[test]
    fn a_reader_is_clone_send_and_sync() {
        fn assert_bounds<T: Clone + Send + Sync + 'static>() {}
        assert_bounds::<crate::Reader>();
    }

    #[test]
    fn a_reader_observes_writes_made_after_it_was_created() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let reader = engine.reader();

        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(reader.get("alpha").expect("get"), "one");

        engine.set("alpha".into(), "two".into()).expect("overwrite");
        assert_eq!(reader.get("alpha").expect("get"), "two");

        engine.remove("alpha").expect("remove");
        assert!(matches!(reader.get("alpha"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn a_reader_reports_key_not_found_for_unknown_keys() {
        let dir = TempDir::new().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("open");
        let reader = engine.reader();
        assert!(matches!(reader.get("ghost"), Err(EngineError::KeyNotFound)));
    }

    #[test]
    fn readers_work_from_other_threads() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        engine.set("alpha".into(), "one".into()).expect("set");

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let reader = engine.reader();
                std::thread::spawn(move || reader.get("alpha"))
            })
            .collect();

        for handle in handles {
            let value = handle.join().expect("thread must not panic").expect("get");
            assert_eq!(value, "one");
        }
    }

    #[test]
    fn a_reader_sees_a_write_that_lands_while_other_readers_are_live() {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = Engine::open(dir.path()).expect("open");
        let first = engine.reader();
        let second = first.clone();

        engine.set("alpha".into(), "one".into()).expect("set");
        assert_eq!(first.get("alpha").expect("get"), "one");
        assert_eq!(second.get("alpha").expect("get"), "one");
    }
}
```

`a_reader_is_clone_send_and_sync` is a compile-time test: it has no assertions
because the bound check *is* the assertion. If `Reader` ever gains a non-`Sync`
field, this fails to compile with a message pointing straight at the cause.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-engine reader`
Expected: FAIL — `cannot find type Reader`, `no method named reader`.

- [ ] **Step 3: Implement**

Write `reader.rs`, then refactor `Engine` to share its keydir. The refactor:

- `Engine`'s keydir field becomes `Arc<RwLock<KeyDir>>`. Replay in `open`
  builds a plain `HashMap` and wraps it once at the end — no need to lock per
  record when nobody else can see it yet.
- `set` and `remove` take the **write** lock only for the index update, after
  the bytes are durable. Do not hold it across the append.
- `Engine::get` becomes a delegation to an internal `Reader`. Keep a `Reader`
  field on the engine rather than constructing one per call.
- `Engine::reader()` clones that field.

Export it:
```rust
mod reader;
pub use reader::Reader;
```

On lock poisoning: a panic while the write lock is held poisons the `RwLock`.
For now, `.expect("keydir lock poisoned")` is acceptable and honest — a
poisoned index means a panic already corrupted your invariants, and continuing
is worse than aborting. Task 13 maps that to a 500 at the HTTP boundary.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-engine`
Expected: PASS — 33 in the lib target, 5 in `recovery`. Every Phase 1 test must
still pass **unmodified**; that is what tells you the refactor preserved
behaviour.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-engine
git commit -m "feat(engine): add a cloneable Reader over a shared keydir"
```

### Task 10: The writer thread and KvHandle

**Files:**
- Create: `crates/kvs-server/src/writer.rs`, `crates/kvs-server/tests/writer.rs`
- Modify: `crates/kvs-server/src/main.rs`

**Interfaces:**
- Consumes: `Engine`, `Reader`, `EngineError` from `kvs-engine`.
- Produces:

```rust
pub enum Request {
    Set { key: String, value: String, reply: oneshot::Sender<Result<(), EngineError>> },
    Remove { key: String, reply: oneshot::Sender<Result<(), EngineError>> },
}

pub fn channel(capacity: usize) -> (mpsc::Sender<Request>, mpsc::Receiver<Request>);

pub fn spawn_with(engine: Engine, rx: mpsc::Receiver<Request>) -> std::thread::JoinHandle<()>;

pub fn spawn(engine: Engine, capacity: usize) -> (KvHandle, std::thread::JoinHandle<()>);

#[derive(Clone)]
pub struct KvHandle { /* mpsc::Sender<Request> + Reader */ }

impl KvHandle {
    pub fn new(tx: mpsc::Sender<Request>, reader: Reader) -> Self;
    pub async fn set(&self, key: String, value: String) -> Result<(), EngineError>;
    pub async fn remove(&self, key: String) -> Result<(), EngineError>;
    pub async fn get(&self, key: String) -> Result<String, EngineError>;
}
```

**Why this lives in `kvs-server` and not `kvs-engine`:** the channel is
`tokio::sync::mpsc`, and the engine is forbidden from depending on `tokio`.
That constraint from Task 1 is doing real work here — it forced the runtime
choice out of the storage layer, which is exactly the property a future
replication layer needs.

**Why `spawn` is built from `channel` + `spawn_with` rather than being
primitive:** it lets a caller hold a `Sender` whose `Receiver` is gone, which
is the only way to test the shutdown path — and is also how a real graceful
shutdown works.

**Design notes:**
- Bounded channel, capacity 1024 in production. Bounded means a write flood
  applies backpressure to HTTP handlers instead of accumulating unbounded work
  in memory. `send().await` on a full channel is the backpressure.
- The writer thread is a plain `std::thread`, not a tokio task. It does
  blocking file I/O and owns the `Engine`; parking it on a runtime worker
  thread would starve the executor. Inside it, `rx.blocking_recv()` is the
  bridge from async senders to a blocking consumer.
- `KvHandle::get` never touches the channel. It clones the `Reader` and runs it
  inside `tokio::task::spawn_blocking`, because `Reader::get` does a synchronous
  read syscall and must not run on a runtime worker.
- Error mapping: a failed `send` (receiver dropped) and a failed `recv` on the
  oneshot (writer died before replying) both become `EngineError::ShuttingDown`.
  So does a `JoinError` from `spawn_blocking` — if the read task panicked, the
  honest answer to the client is that the server cannot serve, not a 500 that
  implies the data is bad.
- The writer loop exits when every `Sender` is dropped. Flush and fsync on the
  way out.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kvs-server/tests/writer.rs
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
    join.join().expect("the writer thread must survive rejected writes");
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
```

That last assertion is worth sitting with. Reads surviving a dead writer is not
an accident — it falls out of reads bypassing the channel, and it is a real
property: a store whose writer has failed can still serve every key it already
has.

Note that these tests import from `kvs_server::...`, which means the crate needs
a library target alongside its binary.

- [ ] **Step 2: Add a library target to kvs-server**

```rust
// crates/kvs-server/src/main.rs — replace the placeholder
fn main() {
    println!("kvs-server");
}
```

```rust
// crates/kvs-server/src/lib.rs — new
pub mod writer;
```

```toml
# append to crates/kvs-server/Cargo.toml
[lib]
name = "kvs_server"
path = "src/lib.rs"

[[bin]]
name = "kvs-server"
path = "src/main.rs"
```

Integration tests in `tests/` can only link against a library target. Splitting
`lib.rs` from `main.rs` is what makes the server's internals testable at all,
and it is the standard shape for a Rust binary of any size.

- [ ] **Step 3: Run the tests and watch them fail**

Run: `cargo test -p kvs-server --test writer`
Expected: FAIL — `unresolved import kvs_server::writer`.

- [ ] **Step 4: Implement**

Write `writer.rs`. The thread body is a loop over `rx.blocking_recv()` that
matches on the `Request`, calls the corresponding `Engine` method, and sends the
result back through the `reply` channel. Ignore a send failure on the reply —
it just means the client hung up, which is not the writer's problem.

`spawn` is: build the engine's reader, build the channel, hand the receiver to
`spawn_with`, and pair the sender with the reader in a `KvHandle`.

- [ ] **Step 5: Run the tests and watch them pass**

Run: `cargo test -p kvs-server`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server
git commit -m "feat(server): run the engine on a writer thread behind a bounded channel"
```

### Task 11: Concurrency tests

**Files:**
- Create: `crates/kvs-server/tests/concurrency.rs`

**Interfaces:**
- Consumes: `writer::spawn`, `KvHandle`.
- Produces: no new API. This task adds only evidence.

**Why this task exists:** everything so far *claims* the design is safe under
concurrency. These tests are where the claim gets tested. Nothing in this task
changes the implementation — if a test fails here, the bug is in Task 9 or 10.

**What each test proves:**
- The first proves no lost updates: every write that returned `Ok` is readable
  afterwards.
- The second proves the strong property. A **single sequential reader** issues
  reads one after another, and each read must observe a version at least as new
  as the previous one. If reads could see a stale index — or a half-applied
  write — versions would go backwards or fail to parse. Monotonicity from a
  sequential observer is what linearizability looks like from the outside.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kvs-server/tests/concurrency.rs
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
                let observed = handle
                    .get("hot".into())
                    .await
                    .expect("a read concurrent with writes must never fail");
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
                handle.set("hot".into(), format!("v-{round}")).await.expect("set");
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
                        "reads must never observe garbage, saw {observed:?}"
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
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p kvs-server --test concurrency`
Expected: PASS on the first run, if Tasks 9 and 10 are right. That is unusual
for TDD and worth naming: these tests are a *check on work already done*, not a
driver for new work.

If they fail, resist patching the test. Read the failure against the invariant
in the spec — bytes durable, then index — and find which side of it broke.

- [ ] **Step 3: Run them under repetition and thread stress**

Run:
```bash
for i in $(seq 1 20); do cargo test -p kvs-server --test concurrency || break; done
```
Expected: 20 clean runs. Concurrency bugs are probabilistic; one green run is
not evidence. If you want to go further, `cargo install cargo-nextest` and run
`cargo nextest run -p kvs-server --test concurrency --test-threads 8`.

- [ ] **Step 4: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server/tests
git commit -m "test(server): prove no lost updates and monotonic sequential reads"
```

**Phase 2 is complete.** The store is concurrent and the consistency claims in
the spec now have tests behind them rather than prose.

---

# Phase 3 — HTTP service

### Task 12: Router, config, and /health

**Files:**
- Create: `crates/kvs-server/src/config.rs`, `crates/kvs-server/src/routes.rs`, `crates/kvs-server/tests/api.rs`
- Modify: `crates/kvs-server/src/lib.rs`

**Interfaces:**
- Consumes: `KvHandle`, `FsyncPolicy`, `MAX_VALUE_BYTES`.
- Produces:
  - `pub fn routes::router(handle: KvHandle) -> axum::Router`
  - `pub struct Config { pub data_dir: PathBuf, pub addr: SocketAddr, pub fsync: FsyncArg }` deriving `clap::Parser`
  - `pub enum FsyncArg { Always, Never }` deriving `clap::ValueEnum`, with `impl From<FsyncArg> for FsyncPolicy`

**Why `router` takes a `KvHandle` and returns a `Router`:** it makes the whole
HTTP surface constructible in a test without binding a port. Every test in this
phase runs through `tower::ServiceExt::oneshot`, which drives the router
directly — no sockets, no port collisions, no sleeping to wait for a bind, no
flakes.

**Why `FsyncArg` duplicates `FsyncPolicy`:** deriving `clap::ValueEnum` on the
engine's type would put `clap` in `kvs-engine`'s dependency list. A four-line
`From` impl at the boundary is the cost of keeping the engine runtime-free and
CLI-free, and it is a cost worth paying every time.

- [ ] **Step 1: Write the failing test**

```rust
// crates/kvs-server/tests/api.rs
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
    Request::builder().uri(uri).body(Body::empty()).expect("request")
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
```

Keep `send`, `get`, `test_app`, and `TestApp` — every later task in this phase
builds on them.

- [ ] **Step 2: Run the test and watch it fail**

Run: `cargo test -p kvs-server --test api`
Expected: FAIL — `unresolved import kvs_server::routes`.

- [ ] **Step 3: Implement**

Write `config.rs` and `routes.rs`, and add both to `lib.rs`:

```rust
// crates/kvs-server/src/lib.rs
pub mod config;
pub mod routes;
pub mod writer;
```

`Config` fields and their defaults:
```rust
#[arg(long, default_value = "./data")]      pub data_dir: PathBuf,
#[arg(long, default_value = "127.0.0.1:3000")] pub addr: SocketAddr,
#[arg(long, value_enum, default_value_t = FsyncArg::Always)] pub fsync: FsyncArg,
```

`router` builds an `axum::Router` with `GET /health`, `.with_state(handle)`, and
a `DefaultBodyLimit` layer. Set that limit to `MAX_VALUE_BYTES + 8 * 1024`, not
to `MAX_VALUE_BYTES` — deliberately, and Task 14 depends on it. A body rejected
by the limit layer never reaches your handler, so it gets axum's default error
response rather than your JSON envelope. Leaving headroom means a
just-over-limit value is rejected by *your* check, with your error shape, and
the layer stays as a backstop against genuinely enormous bodies.

Handlers take `State(handle): State<KvHandle>`. Note that `axum` 0.8 spells path
parameters `{key}`, not `:key`.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-server --test api`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server
git commit -m "feat(server): add the axum router, CLI config, and a health endpoint"
```

### Task 13: AppError and GET /v1/kv/{key}

**Files:**
- Create: `crates/kvs-server/src/error.rs`, `crates/kvs-server/src/dto.rs`
- Modify: `crates/kvs-server/src/routes.rs`, `crates/kvs-server/src/lib.rs`, `crates/kvs-server/tests/api.rs`

**Interfaces:**
- Consumes: `EngineError`, `KvHandle::get`.
- Produces:
  - `pub struct AppError(pub EngineError)` with `impl IntoResponse` and `impl From<EngineError>`
  - `pub struct ValueResponse { pub key: String, pub value: String }` (`Serialize`)
  - route `GET /v1/kv/{key}`

**Why `AppError` is one type with one `IntoResponse`:** the mapping from engine
failure to status code lives in exactly one place. Scatter it across handlers
and the same failure will answer 404 on one route and 500 on another — a class
of bug that is invisible until a client depends on it.

**The mapping:**

| `EngineError` | Status | `code` |
|---|---|---|
| `KeyNotFound` | 404 | `not_found` |
| `KeyTooLarge` / `ValueTooLarge` | 413 | `payload_too_large` |
| `ShuttingDown` | 503 | `unavailable` |
| `Io` / `Corrupt` | 500 | `internal` |

`Io` and `Corrupt` carry operational detail — file paths, byte offsets — so log
them with `tracing::error!` and return a generic message. A 500 body is for the
client; the log is for you.

- [ ] **Step 1: Write the failing tests**

```rust
// append to crates/kvs-server/tests/api.rs

fn error_code(body: &Bytes) -> String {
    let json: serde_json::Value = serde_json::from_slice(body).expect("json body");
    json["error"]["code"].as_str().expect("error.code must be a string").to_string()
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
```

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-server --test api`
Expected: FAIL — `get_on_an_unknown_key_is_404_with_the_standard_envelope`
returns 404 from axum's *fallback* (no such route), whose body is empty, so
`error_code` panics parsing JSON. That failure is informative: it shows the
difference between "no route" and "no key", which is precisely what this task
makes distinguishable.

- [ ] **Step 3: Implement**

Write `error.rs` and `dto.rs`, add them to `lib.rs`, and add the `GET` route.

The `IntoResponse` impl returns `(StatusCode, Json(body))` where `body` is a
serializable struct shaped as `{"error": {"code", "message"}}`. Define that
shape as types in `dto.rs` rather than building `serde_json::json!` inline — you
will construct it from four places, and a typo in a string literal is a silent
API break.

The handler signature:
```rust
async fn get_key(
    State(handle): State<KvHandle>,
    Path(key): Path<String>,
) -> Result<Json<ValueResponse>, AppError>
```
`Result<T, AppError>` as a return type works because both arms implement
`IntoResponse`; that is what lets you write `?` in a handler. Axum's `Path`
extractor percent-decodes for you, which is what the second test pins.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-server --test api`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server
git commit -m "feat(server): add GET /v1/kv/{key} and the shared error envelope"
```

### Task 14: PUT /v1/kv/{key}

**Files:**
- Modify: `crates/kvs-server/src/routes.rs`, `crates/kvs-server/src/dto.rs`, `crates/kvs-server/tests/api.rs`

**Interfaces:**
- Consumes: `KvHandle::set`, `AppError`, `MAX_KEY_BYTES`, `MAX_VALUE_BYTES`.
- Produces: `pub struct SetRequest { pub value: String }` (`Deserialize`); route `PUT /v1/kv/{key}`.

**Design notes:**
- `PUT` returns `204 No Content` for both create and update. It does not report
  whether the key existed — that would make every write also do a read, which
  is real cost for information no client in the MVP needs.
- Reject oversized keys and values in the handler, before calling
  `handle.set`. The engine checks too (Task 8); this check exists so an
  oversized write never occupies a slot in the bounded channel.
- A malformed JSON body produces axum's `JsonRejection`. Left alone it returns
  a plain-text 400 that does not match your envelope. Capture it by taking
  `Result<Json<SetRequest>, JsonRejection>` as the extractor and mapping the
  rejection yourself.

- [ ] **Step 1: Write the failing tests**

```rust
// append to crates/kvs-server/tests/api.rs

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
    assert_eq!(status, StatusCode::NO_CONTENT, "an update is not a different status");

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
    let (status, body) = send(&app, put("/v1/kv/alpha", "not json at all")).await;

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
    let body = serde_json::to_string(&serde_json::json!({ "value": value })).expect("body");

    let (status, body) = send(&app, put("/v1/kv/alpha", &body)).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        error_code(&body),
        "payload_too_large",
        "the body limit must leave headroom so our own check produces this response"
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
```

`put_rejects_a_body_missing_the_value_field` is there because `serde` is strict
about missing fields but silent about extra ones — this test documents which
half you are relying on.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-server --test api`
Expected: FAIL — `put_then_get_round_trips` gets `405 Method Not Allowed`,
since the path exists for `GET` only.

- [ ] **Step 3: Implement**

Add `SetRequest` to `dto.rs` and the `PUT` route. Add a `bad_request` arm to
`AppError` — it is not an `EngineError`, so `AppError` needs to represent
"rejected before the engine saw it" as well. A second variant, or a small enum
with a `Engine(EngineError)` and a `BadRequest(String)` arm, is the natural
shape; keep the single `IntoResponse` impl.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-server --test api`
Expected: PASS, 12 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server
git commit -m "feat(server): add PUT /v1/kv/{key} with request validation"
```

### Task 15: DELETE /v1/kv/{key} and the 503 path

**Files:**
- Modify: `crates/kvs-server/src/routes.rs`, `crates/kvs-server/tests/api.rs`

**Interfaces:**
- Consumes: `KvHandle::remove`, `writer::channel`, `KvHandle::new`.
- Produces: route `DELETE /v1/kv/{key}`.

**Why the 503 test matters:** it is the only test that exercises what the
service does when its storage layer is gone. A server that hangs, or that
answers 500 and implies the data is corrupt, is worse than one that says
plainly "I cannot accept writes right now". This is also the test that proves
reads survive a dead writer — through the HTTP layer, not just the handle.

- [ ] **Step 1: Write the failing tests**

```rust
// append to crates/kvs-server/tests/api.rs
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
    assert_eq!(status, StatusCode::NOT_FOUND, "a read must answer, not fail");

    let (status, _) = send(&app, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
}
```

Note the last assertion: `/health` still reports OK even though writes are
failing. That is a deliberate choice for the MVP — `/health` is a liveness
check, not a readiness check. If you later want a load balancer to pull this
instance out of rotation when the writer dies, that is a *readiness* endpoint
and a separate design decision. Worth writing down rather than conflating.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `cargo test -p kvs-server --test api`
Expected: FAIL — `delete_removes_a_key` gets `405 Method Not Allowed`.

- [ ] **Step 3: Implement**

Add the `DELETE` route. If `ShuttingDown` already maps to 503 in `AppError`
from Task 13, the last test should pass with no additional work — confirm that
rather than assuming it.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `cargo test -p kvs-server --test api`
Expected: PASS, 16 tests.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/kvs-server
git commit -m "feat(server): add DELETE /v1/kv/{key} and surface writer loss as 503"
```

### Task 16: Wiring main, and one test over a real socket

**Files:**
- Modify: `crates/kvs-server/src/main.rs`
- Create: `crates/kvs-server/tests/smoke.rs`

**Interfaces:**
- Consumes: `Config`, `writer::spawn`, `routes::router`, `Engine::open_with`.
- Produces: a runnable binary.

**Why a socket test, when `oneshot` already covers the routes:** `oneshot`
drives the router but never touches `axum::serve`, a TCP listener, HTTP framing,
or a real client. Everything in `main.rs` — the part that turns a `Router` into
a server — is currently untested. This one test covers that seam. It is the only
test in the project that needs a port, which is exactly the right number.

- [ ] **Step 1: Write the failing test**

```rust
// crates/kvs-server/tests/smoke.rs
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
```

There is no `sleep` in this test, and there must not be one. The listener is
bound before `tokio::spawn` is called, so the OS is already queueing
connections by the time the client dials — the accept loop starting a moment
later is fine. A `sleep` here would be papering over a race that does not exist,
and would make the test slow and flaky at the same time.

- [ ] **Step 2: Run the test and watch it fail**

Run: `cargo test -p kvs-server --test smoke`
Expected: FAIL to compile — `reqwest` and `axum::serve` need features. Add
`tokio`'s `net` feature and `axum`'s default features if the compiler asks;
read the error rather than guessing.

- [ ] **Step 3: Write main.rs**

```rust
// crates/kvs-server/src/main.rs — the shape, not the implementation
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. tracing_subscriber init, with an EnvFilter defaulting to "info"
    // 2. Config::parse()
    // 3. Engine::open_with(&config.data_dir, config.fsync.into())
    // 4. writer::spawn(engine, 1024)
    // 5. routes::router(handle)
    // 6. TcpListener::bind(config.addr), log the bound address
    // 7. axum::serve(listener, router).await
    todo!("yours to write")
}
```

This is the one place `anyhow` earns its keep: `main` reports errors, it never
matches on them. Log the resolved config at startup — data dir, address, fsync
policy — because "which fsync mode was that benchmark run in?" is a question you
will ask.

- [ ] **Step 4: Run the test and watch it pass**

Run: `cargo test -p kvs-server --test smoke`
Expected: PASS, 1 test.

- [ ] **Step 5: Run the whole thing by hand**

```bash
cargo run -p kvs-server -- --data-dir /tmp/kvs-manual --addr 127.0.0.1:3000
```
Then, in another terminal:
```bash
curl -i -X PUT localhost:3000/v1/kv/greeting -H 'content-type: application/json' -d '{"value":"hello"}'
curl -i localhost:3000/v1/kv/greeting
curl -i localhost:3000/v1/kv/missing
curl -i -X DELETE localhost:3000/v1/kv/greeting
curl -i localhost:3000/v1/kv/greeting
```
Expected: 204, 200 with the value, 404, 204, 404. Then stop the server with
Ctrl-C, start it again with the same `--data-dir`, and confirm a key you wrote
before the restart is still there. That last step is the whole project in one
gesture.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
cargo test --workspace
git add crates/kvs-server
git commit -m "feat(server): wire up main and add an end-to-end socket test"
```

**The MVP is complete.** `cargo test --workspace` should report 33 tests in the
engine's lib target, 5 in `recovery`, and 4 + 3 + 16 + 1 across the server's
`writer`, `concurrency`, `api`, and `smoke` targets — 62 in all.

---

## What you have, and what is next

Read `docs/superpowers/specs/2026-09-01-rust-kvs-design.md` §5 again now that
the code exists. The consistency claims there — linearizable per key,
read-your-writes, total write order, no cross-key atomicity — should each map
to something concrete you wrote.

The runway from the spec, roughly in order of value:

1. **Compaction.** The log grows forever; `every_write_appends_to_the_log` from
   Task 4 is the proof. This is where `file_id` stops being always-zero, where
   `timestamp` starts mattering, and where tombstones can finally be discarded.
2. **`criterion` benchmarks**, then flip `--fsync=never` and measure. Numbers
   turn "fsync is expensive" into a figure you can defend.
3. **`proptest` against a `HashMap` oracle** — random operation sequences
   compared to a model. It finds the cases you did not think to write.
4. **Binary values.** Changes the payload, the JSON representation, and the size
   limits together.
5. **Replication.** Give each `Command` a log index, make the writer loop
   replay-driven, and the seam you have been protecting since Task 1 starts
   paying for itself.
