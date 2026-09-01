# rust-kvs: Design

**Date:** 2026-09-01
**Status:** Approved
**Scope:** MVP — a single-node, log-structured key/value store exposed over a REST API.

## 1. Purpose and constraints

`rust-kvs` is a learning project. The goal is to write idiomatic Rust across three
areas at once: a storage engine that manages its own file format and recovery, a
concurrency model with an explicit consistency story, and an async HTTP service.

Two constraints shape every decision below.

**Learning over expedience.** Where a hand-written mechanism teaches something
worth knowing (record framing, checksums, crash recovery), we write it. Where a
crate is simply the idiomatic answer (`serde`, `axum`, `thiserror`), we use it.
We do not hand-roll HTTP parsing, and we do not pull in an existing storage
engine.

**Distribution is the eventual destination.** Replication (Raft) is not in this
spec and will not be built soon, but the design keeps exactly one seam open for
it: mutations are first-class `Command` values that flow through a single
totally-ordered channel, and the storage engine has no async runtime of its own.
No other speculative structure is built.

### Out of scope for the MVP

Compaction, transactions, TTLs, range scans, authentication, binary values, a
client crate, and replication. Section 9 places each on the runway.

## 2. Workspace layout

```
rust-kvs/
├── Cargo.toml              # workspace manifest
├── crates/
│   ├── kvs-engine/         # library: storage. No tokio, no axum, no HTTP.
│   └── kvs-server/         # binary: axum service, owns the writer thread
└── docs/superpowers/specs/
```

Two crates rather than one binary, because the boundary carries weight.
`kvs-engine` does not depend on `tokio` or `axum`, and the manifest enforces
that rather than leaving it to discipline. A future replication layer must be
able to consume `Command`s and drive the engine directly; it cannot do that if
the engine has already chosen a runtime.

### Dependencies

`kvs-engine`: `serde` (derive), `bincode`, `crc32fast`, `thiserror`.
Dev: `tempfile`.

`kvs-server`: `kvs-engine`, `tokio` (rt-multi-thread, macros, sync), `axum`,
`serde`, `serde_json`, `thiserror`, `anyhow`, `tracing`, `tracing-subscriber`,
`clap` (derive). Dev: `tower`, `http-body-util`.

## 3. Data model and on-disk format

One type is the unit of mutation everywhere — the log record, the channel
message, and eventually the replication proposal:

```rust
#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
```

Keys and values are UTF-8 `String`s in the MVP. Binary values are a later
phase; they change the log payload, the JSON representation, and the size
limits together, so they are one coherent piece of work rather than a detail.

### Log record framing

Records are appended to `<data-dir>/0.log`. All integers are little-endian.

```
┌──────────────┬────────────────────┬─────────────────────────┐
│ crc32: u32   │ payload_len: u32   │ payload: bincode(Command)│
└──────────────┴────────────────────┴─────────────────────────┘
     4 bytes          4 bytes              payload_len bytes
```

The framing is hand-written; the payload encoding is not. Writing the framing
teaches length-prefixing, `to_le_bytes`, buffered writes, and torn-write
detection. Encoding the `Command` with `serde`/`bincode` costs nothing to
learn twice — `serde` is already required for the HTTP layer, and the same type
will serialize to JSON for future RPC.

The CRC covers the payload bytes only, not the header. A record is valid when
its `payload_len` fits within the remaining file and the CRC of those bytes
matches. Anything else marks the end of the usable log (see recovery).

### In-memory index (the "keydir")

```rust
#[derive(Clone, Copy, Debug)]
struct Entry {
    file_id: u32,    // always 0 in the MVP
    pos: u64,        // byte offset of the record header (start of the CRC)
    len: u32,        // payload length, excluding the 8-byte header
    timestamp: u64,  // millis since epoch, when the record was written
}

type KeyDir = HashMap<String, Entry>;
```

`pos` addresses the record header rather than the payload so that one
positioned read of `8 + len` bytes yields both the checksum and the bytes it
covers. Pointing at the payload would make every read either a second syscall
or an unverified one. `Entry` is `Copy` and small, which is what lets a reader
copy it out from under the lock and release the guard before doing any I/O.

`file_id` is present but constant. Compaction requires multiple log files, and
adding the field later would mean touching recovery, the read path, and every
test that constructs an `Entry`. One `u32` now is cheaper than that churn.
`timestamp` is written for debuggability and for the merge rule compaction will
need; nothing in the MVP reads it for correctness.

## 4. Engine internals

The engine is single-threaded by construction. It requires `&mut self` for
mutation, which means exclusive access is a type-level fact rather than a
convention. Section 5 is what supplies that exclusivity.

### Open and recovery

On open, scan the log from offset 0 and replay it into the keydir: `Set`
inserts an `Entry`, `Remove` erases the key. Because records are applied in
file order and later records overwrite earlier ones, the resulting keydir is
exactly the state at the moment of the last complete write.

Scanning stops at the first record that is not intact — a header that runs past
EOF, a short payload read, a CRC mismatch, or a payload that fails to decode.
The file is then truncated to the offset where that record began, and the
writer continues from there.

This is the crash-safety contract: a process that dies mid-append loses the
partial record and nothing else. It deserves a deliberate test that appends
garbage to a healthy log, reopens, and asserts every earlier key survived.

### Write path

1. Serialize the `Command`, compute the CRC, append header and payload.
2. Flush the buffered writer, then apply the fsync policy.
3. Update the keydir.

The order is the correctness argument, and Section 5 depends on it.

### Read path

Look up the `Entry`, then perform one positioned read of `8 + len` bytes at
`pos`: that yields the header and the payload together. Check that the header's
`payload_len` equals the `Entry`'s `len`, verify the CRC over the payload,
decode, and return the value. A CRC or length mismatch on a record the keydir
points at is `Corrupt` — not `KeyNotFound` — because it means the file changed
underneath a valid index.

### fsync policy

A runtime flag, `--fsync`:

- `always` (default) — `sync_data()` before the write is acknowledged.
- `never` — leave it to the OS page cache.

Durability is the reason the log exists, so `always` is the default. The flag
exists because the throughput gap between the two is large, and measuring it is
a worthwhile exercise once benchmarks land (Phase 4). An `interval` policy is
deliberately deferred: it needs a background flusher and a story about what an
acknowledged-but-unflushed write means, which is a design question of its own.

## 5. Concurrency architecture

```
axum handler (tokio task)
  │
  ├── WRITE ──► bounded mpsc ──► writer thread (owns Engine, owns File)
  │             (Command + oneshot reply)          │
  │                                    append → fsync → write-lock keydir → insert
  │                                                 │
  │             ◄─────────── oneshot reply ─────────┘
  │
  └── READ ───► spawn_blocking
                   │
                   ├─ read-lock keydir, copy Entry, DROP THE GUARD
                   └─ read_at on shared Arc<File> → verify CRC → decode
```

**Writes** are sent as `Command` plus a `tokio::sync::oneshot::Sender` over a
bounded `tokio::sync::mpsc` channel (capacity 1024). A single `std::thread`
owns the `Engine` and consumes the channel with `blocking_recv()`. Bounded, not
unbounded: a write flood should apply backpressure to HTTP handlers, not
silently accumulate in memory.

**Reads** never touch the writer thread. The keydir lives behind an
`Arc<RwLock<KeyDir>>` that the writer publishes into. A reader takes the read
lock, copies out the `Entry` (which is `Copy`-sized), releases the lock, and
only then performs I/O. Readers share a single `Arc<File>` and use positioned
reads (`FileExt::read_at` on Unix, `seek_read` on Windows), which do not touch
the file's cursor — so one handle is safe to share across threads, with no
per-read `open` and no cursor contention.

### Types and ownership

Three types carry this structure, and naming them removes the question of who
owns what after Phase 2:

- `Engine` — the storage engine of Section 4. Owns the write `File`, requires
  `&mut self` to mutate, and lives on the writer thread and nowhere else.
- `Reader` — a cheap, cloneable read handle holding `Arc<RwLock<KeyDir>>` and
  `Arc<File>`. It serves `get` without any reference to the `Engine`. Cloned
  per request; holds no mutable state of its own.
- `KvHandle` — what the axum router holds in its state: an
  `mpsc::Sender<Request>` for mutations plus a `Reader` for lookups. Cloneable,
  `Send + Sync`, and the only type the HTTP layer knows about.

`Engine` is constructed first; it produces the `Reader`, then moves onto the
writer thread. After that move no code outside the writer thread holds an
`Engine` — the compiler enforcing Section 5's exclusivity rather than a
convention to remember.

### The invariant

**The keydir insert is the commit point, and it happens strictly after the
bytes are durable.**

Every `Entry` a reader can observe therefore points at data already on disk. A
reader can never see an index entry for a write that is still in flight.

### Consistency properties

- **Linearizable per key.** Each operation takes effect at a single instant —
  the keydir insert for a write, the keydir lookup for a read — and a client
  receives its response only after that instant.
- **Read-your-writes.** Guaranteed for a client that waits for its `PUT`
  response before reading.
- **Total write order.** All mutations are ordered by the channel. That order
  is the log, and the log is what a replica would replay.
- **No cross-key atomicity.** Two keys written by one logical operation can be
  observed half-applied. Transactions are a separate design, not a tweak.

The property worth returning to: reads bypass the writer entirely, so a read
concurrent with a write may land on either side of the commit point. That is
still linearizable — but it is precisely where staleness becomes a real
question once replicas exist.

### Rust mechanics this exercises

Holding a `std::sync` guard across an `.await` (don't); blocking file I/O on a
runtime thread (don't); `Arc` versus borrowing for shared ownership; and why
the writer needs `&mut Engine` yet no lock — the channel grants exclusivity by
moving ownership onto one thread.

### Shutdown

Dropping every `Sender` ends the writer loop, which flushes and fsyncs before
exiting. A handler that finds the channel closed returns `503`, so a writer
thread that has panicked is visible as an error rather than a hang.

## 6. HTTP surface

| Method   | Path             | Request body        | Success                     | Errors                  |
|----------|------------------|---------------------|-----------------------------|-------------------------|
| `GET`    | `/v1/kv/{key}`   | —                   | `200` `{"key","value"}`     | `404`, `500`            |
| `PUT`    | `/v1/kv/{key}`   | `{"value": "..."}`  | `204 No Content`            | `400`, `413`, `503`     |
| `DELETE` | `/v1/kv/{key}`   | —                   | `204 No Content`            | `404`, `503`            |
| `GET`    | `/health`        | —                   | `200` `{"status":"ok"}`     | —                       |

`PUT` is idempotent and does not distinguish create from update; it returns
`204` in both cases. Returning the prior value was considered and rejected — it
would force every write to also perform a read.

Limits are enforced at the edge, before anything reaches the channel: keys up
to 1 KiB, values up to 1 MiB, with `DefaultBodyLimit` as the outer guard.

Errors use one shape throughout:

```json
{"error": {"code": "not_found", "message": "key not found"}}
```

Configuration via `clap`: `--data-dir` (default `./data`), `--addr` (default
`127.0.0.1:3000`), `--fsync` (default `always`). Structured logging via
`tracing` with `tracing-subscriber`.

## 7. Error handling

`kvs-engine` returns a `thiserror` enum. Callers match on these, so they are
part of the API:

```rust
enum EngineError {
    Io(std::io::Error),
    Corrupt { offset: u64 },
    KeyNotFound,
    KeyTooLarge { len: usize },
    ValueTooLarge { len: usize },
    ShuttingDown,
}
```

No `anyhow` in the library — erasing the error type would make the status-code
mapping guesswork.

`kvs-server` defines an `AppError` implementing `axum::response::IntoResponse`.
That impl is the single place where an engine error becomes a status code:
`KeyNotFound` → `404`, `KeyTooLarge`/`ValueTooLarge` → `413`, malformed JSON →
`400`, `ShuttingDown` or a closed channel → `503`, `Io`/`Corrupt` → `500` with
the detail logged rather than returned. `anyhow` is acceptable in the binary,
where errors are reported rather than matched on.

## 8. Testing

Test-driven throughout: write the failing test, watch it fail, then make it
pass.

**Engine unit tests** over `tempfile::TempDir` — set and get; overwrite; delete;
get on a missing key; reopen after drop and confirm recovery; and a corrupted-
tail test that appends garbage to a healthy log, reopens, and asserts every
earlier key survived while the torn record is gone.

**Concurrency tests** — N threads writing disjoint keys alongside M readers.
Assert no lost updates, and that every read returns either the prior or the new
value and never a decode error. A second variant has all threads contend on one
key and asserts the final value is one of the values actually written.

**HTTP tests** — drive the `axum::Router` through `tower::ServiceExt::oneshot`.
No sockets, no port binding, no flakiness. Cover each row of the table in
Section 6, including the `413` and `404` paths.

**Deferred to Phase 4** — `proptest` running random operation sequences against
a `HashMap` oracle, and `criterion` benchmarks. Benchmarks arrive when there is
a concurrency change to justify with numbers, not before.

## 9. Phasing

The MVP is Phases 0 through 3. Each phase ends with `cargo fmt`, `cargo clippy`
and `cargo test` clean.

**Phase 0 — Skeleton.** Workspace manifest, both crates, dependency wiring, a
trivial passing test in each. Verifies the toolchain and the crate boundary.

**Phase 1 — Engine, single-threaded.** `Command`, record framing with CRC,
`open` with replay and tail truncation, `set`/`get`/`remove`, `EngineError`,
the fsync policy. Full unit and recovery test coverage. No threads yet.

**Phase 2 — Concurrency.** Extract the keydir behind `Arc<RwLock<_>>`, move the
engine onto a writer thread behind a bounded channel, build the positioned-read
path, and add the concurrency tests. Still no HTTP.

**Phase 3 — HTTP service.** `axum` router, the four routes, `AppError` and its
`IntoResponse`, size limits, `clap` configuration, `tracing`, and the
`ServiceExt::oneshot` integration tests. **MVP complete.**

**Phase 4 and beyond — the runway.** Compaction with multiple log files; fsync
policy benchmarks under `criterion`; the `proptest` model oracle; metrics;
binary values; then replication groundwork, which begins by giving each
`Command` a log index and making the writer loop replay-driven.
