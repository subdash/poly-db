# rust-kvs Log Compaction: Design

**Date:** 2026-09-12
**Status:** Approved
**Scope:** Replace the single append-only log with a multi-file log, and add a
background merge that reclaims the space held by overwritten and deleted records.

## 1. Purpose and constraints

The store currently writes one file, `0.log`, and never reclaims anything. A key
written a thousand times costs a thousand records on disk, and a deleted key
costs more than it did before it was deleted. Startup replays every byte ever
written. Compaction is the piece that makes the write path sustainable.

Three things shape the design.

**The engine is the thing being learned here.** Unlike the Kubernetes project,
where the store was the excuse, this is a storage problem end to end: an on-disk
format, a concurrent merge, and a crash-recovery protocol.

**The writer's exclusivity must survive.** The current design is easy to reason
about because a single dedicated thread owns `Engine` through `&mut self`. A
background merge threatens exactly that property, and the design below keeps it:
the merge thread never touches `Engine`, only the `Store` that was already shared
behind an `RwLock`.

**The deployment must not regress.** The Kubernetes work established that the
startup path is the store's most fragile operational property — a replay slow
enough to outlast a liveness probe produces an unrecoverable restart loop. Hint
files are in scope specifically because compaction is the natural place to make
startup cost proportional to live *keys* rather than total *bytes written*.

### Out of scope

Replication, which gets its own spec. Also deferred, with reasoning in section 9:
hint files for rolled-but-unmerged files, value-size-aware merge scheduling,
multiple concurrent merges, and a manifest file.

### Success criteria

A store that has overwritten and deleted its way to a large log shrinks to the
size of its live data without losing a write, without stalling reads for longer
than a keydir-sized critical section, and without resurrecting a deleted key
across any crash point in the merge.

## 2. On-disk layout

Files are named `<id>.log`, where `id` is a `u32` that ascends. The highest id is
the **active** file and is the only writable one; every lower id is immutable.
Merged files carry a sidecar `<id>.hint` (section 6).

**Replay processes files in ascending id order**, so last-write-wins falls out of
the ordering and needs no timestamps in the record format. This is the reason the
record format is unchanged by this project.

Today's single `0.log` is already a valid instance of this layout. There is no
migration step and no format version to bump.

### 2.1 Rolling

When the active file passes `max_file_bytes`, the writer flushes it, `sync_data`s
it, marks it immutable, opens `id + 1`, and installs a read handle for the new
file. Rolling is what gives the merge anything to work on: without it there is
one file, and it is the one being appended to.

**The check runs after the append, not before it**, so a file may exceed
`max_file_bytes` by at most one record. `max_file_bytes` is therefore a threshold,
not a hard cap, and the alternative — refusing an append that would cross the
limit — would mean a record larger than the limit could never be written at all.

## 3. The read path

`Reader` currently holds a single `Arc<File>`. It needs a `file_id → handle`
table, and that table must not be able to drift from the keydir. If a reader
copies an entry pointing at file 3 and then resolves the handle table *after* a
merge has removed file 3, it reads the wrong bytes. So both live under one lock:

```rust
pub(crate) struct Store {
    pub(crate) key_dir: KeyDir,
    pub(crate) files: HashMap<u32, Arc<File>>,
}

#[derive(Clone)]
pub struct Reader {
    store: Arc<RwLock<Store>>,
}
```

`get` takes the read lock once, copies the `Entry` and clones the `Arc<File>`,
drops the lock, then calls `read_exact_at`. Two cheap copies, and the lock is
released before the syscall — the same property the current chained `get`
preserves.

**Unlinking an open file is safe on Unix.** The inode survives as long as any fd
references it, so a reader that cloned a handle a microsecond before the merge
deleted that file still reads correct data. The merge never waits for readers to
drain, and there is no epoch scheme or refcount to build. The `Arc<File>`
refcount *is* the scheme.

`Entry.file_id` stops being `#[allow(dead_code)]` here, after carrying the field
since Phase 1 of the MVP.

## 4. The merge protocol

### 4.1 Why this does not cost the writer its exclusivity

The merge thread needs nothing from `Engine`. The keydir was already shared
through `Arc<RwLock<_>>`; the merge thread holds a clone of that `Arc` and does
its own locking. It never touches the writer's `BufWriter`, never writes to the
active file, and never needs `&mut Engine`. The only shared mutable state is the
`Store`, which was already shared.

`Engine::open` spawns the thread and keeps a `std::sync::mpsc::SyncSender` with
**capacity 1**. A `try_send` that fails means a merge is already queued, so the
request is dropped. That single line is the entire "one merge at a time" policy —
no flag, no mutex.

### 4.2 A merge begins by rolling

The writer closes the active file as id `N` and opens `N + 1`. Two consequences
the rest of the protocol leans on:

1. The input set is always the complete immutable prefix `{0..=N}`, so the test
   "was this entry part of the merge?" degenerates from set membership to
   `entry.file_id <= N`.
2. Everything written during the merge lands in `N + 1`, so the merge never reads
   a file that is concurrently being appended to.

### 4.3 The output file takes id `N`

Not a fresh id. The merged records are all older than anything in `N + 1`, so the
merged file must sort *before* the active file for ascending-id replay to remain
last-write-wins. A fresh id above the active file would replay old values over
new ones. Reusing the highest input id gets the ordering right without inventing
a second id space.

### 4.4 Steps

On the merge thread:

1. **Snapshot.** Take the read lock; collect every `(key, entry)` with
   `entry.file_id <= N`; read `Stats::dead_bytes` in the same critical section
   (section 7); drop the lock. This is O(keys) at memory speed. It blocks the
   writer's keydir *mutations* but not its appends.
2. **Rewrite.** For each snapshotted entry, `pread` the record, decode it — CRC
   included — and append it to `N.log.tmp` through a `BufWriter`, recording the
   new offset. A record that fails to decode is logged and dropped, so a merge
   doubles as a scrub.
3. **Durable.** `flush` and `sync_data` the tmp log; write and `sync_data`
   `N.hint.tmp`.
4. **Swap.** Take the write lock and, in one critical section: open a read handle
   on `N.log.tmp`; apply the patch (section 4.5); drop the handles for `0..=N-1`
   and replace `N`'s handle with the tmp's; release. This is the only stall
   readers see, and it is proportional to live keys, not to the size of the merge.
5. **Clean up.** Unlink the inputs and rename the tmp files into place, in the
   order section 5 requires.
6. **Report.** Update `Stats` and log the outcome.

### 4.5 The patch application rule

For each `(key, new_entry)` in the patch:

| Current keydir state | Action | Why |
|---|---|---|
| key absent | **skip** | It was removed during the merge. Inserting resurrects a deleted key. |
| `current.file_id > N` | **skip** | It was overwritten during the merge and now lives in the active file. Replacing points back at the stale merged copy and loses the new value. |
| `current.file_id <= N` | replace | Still the record the merge rewrote; only its address changed. |

As an invariant: **the merge may only ever replace an entry it recognizes, never
insert one.**

Both skip branches are ordinary and will fire constantly under load. They are not
error paths, and they are invisible to any test that does not write concurrently
with a merge — which is why section 8 makes the hook that reaches them mandatory
rather than optional.

### 4.6 Tombstones disappear for free

The snapshot comes from the keydir, which already reflects every remove, so a
deleted key is simply absent from it. No tombstone is written to the merged file,
and the key's old records *and* its tombstone are both deleted with the input
files. That is how compaction reclaims what a delete costs.

It also means correctness depends on no stale `Set` record outliving the merged
file that has no tombstone to mask it — the constraint that dictates the next
section.

## 5. Crash safety

### 5.1 The one dangerous state

The merged file committed *alongside* surviving input files. Replay order is
`0, 1, …, N-1, N(merged), N+1(active)`, so for a key that still exists the merged
file wins. But for a key that was deleted, a stale `Set` in a surviving input
file gets no correction, because the merged file deliberately contains nothing
for that key. The key silently returns.

The ordering constraint that avoids it: **the inputs must be gone before the
merged file becomes visible under its final name.**

### 5.2 Ordering

```
1. write N.log.tmp + N.hint.tmp, fsync both
2. in-memory swap (section 4.4, step 4)
3. unlink 0.log … N.log and their hints, fsync the directory
4. rename N.hint.tmp → N.hint, then N.log.tmp → N.log, fsync the directory
```

`N.log` is itself an input — the file that was just rolled — so step 3 removes
it, and its absence becomes a reliable signal. Renaming the log **last** makes
that rename the commit point.

**`fsync` the directory, not only the files.** A rename and an unlink are
directory metadata operations. Syncing file contents without syncing the
directory can lose the rename while keeping the bytes, which lands precisely in
the state of section 5.1. `File::open(dir)` followed by `sync_all()` — opening a
directory as a `File` works on Unix.

### 5.3 Recovery

Run at startup, before any replay. The entire state is readable off the
filesystem; no manifest is needed.

| On disk | Conclusion | Action |
|---|---|---|
| `<id>.log.tmp` and `<id>.log` both present | Cleanup never started | Delete the tmp pair; the inputs are authoritative |
| `<id>.log.tmp` present, `<id>.log` absent | Cleanup started; inputs partly or wholly gone | The tmp is the only complete copy. Unlink any surviving `<j>.log` and `<j>.hint` for `j < id`, then rename the tmp pair into place |
| No `<id>.log.tmp` | The merge committed | Delete any stray `*.hint.tmp` |

### 5.4 Corruption during replay

The replay path now spans several files, so the existing torn-tail rule needs
splitting:

- **Active file, torn tail** → truncate, as today. It is the file that was being
  written when the process died.
- **Immutable file, corruption mid-file** → log a warning, stop reading *that
  file*, continue with the next. Truncating an immutable file would be wrong, and
  abandoning replay entirely would discard every file above it.

## 6. Hint files

A hint file lets startup read an index instead of the data. Each record uses the
log's existing `[crc: u32][len: u32][payload]` framing, so torn-tail and
corruption detection come along unchanged. The payload is:

```rust
struct Hint { key: String, pos: u64, len: u32 }
```

No `file_id` — a hint file describes exactly one log file, so the id comes from
the filename. No value; that is the entire point. A key with a 1 MiB value costs
a few dozen bytes in the hint rather than a megabyte in the log, which is what
turns startup from "read every live value" into "read every live key."

**Only merged files get hints.** The active file is still growing, and giving
rolled files hints would require scanning the keydir at every roll. Startup cost
is therefore `hints(merged) + full replay(rolled-but-unmerged) + full
replay(active)`, and the dead-byte trigger is what keeps the unmerged tail from
growing without bound.

**Hints are a cache and never authoritative.** Any problem at all — missing file,
bad CRC, torn tail, decode failure — means "fall back to replaying `<id>.log`."
One rule, no partial-hint bookkeeping, and a corrupt hint file can never cost
data.

## 7. Triggering and configuration

### 7.1 Dead-byte accounting is a heuristic

Worth stating before the mechanism, because it bounds how much machinery it
deserves: the counters drive a threshold. Drift means the next merge fires
slightly early or late. Nothing is built to keep them exact.

An `Arc<Stats>` holds two `AtomicU64`s, `total_bytes` and `dead_bytes`. The
writer adds to `dead_bytes` when:

- a `set` overwrites an existing key — the old record's full length
- a `remove` lands — the removed record's length **and** the tombstone's, since a
  tombstone is dead from the moment it is written. Counting it is what makes a
  delete-heavy workload trigger a merge rather than grow forever.

The merge thread reads `dead_bytes` inside the snapshot's read lock and subtracts
exactly that value at commit. Dead bytes accrued during the merge survive the
subtraction and correctly describe the new active file, because the roll
guarantees every earlier record lives in a file being merged.

### 7.2 The trigger

Checked by the writer after each append:

```
total_bytes >= min_merge_bytes
  && (dead_bytes as f64 / total_bytes as f64) >= dead_ratio
```

The cast matters: both counters are `u64`, and integer division would floor the
ratio to `0` for every case below 100% dead and never fire.

then `try_send`. The capacity-1 channel absorbs everything else.

### 7.3 Configuration

`Engine::open_with(path, policy)` becomes `Engine::open_with(path, EngineConfig)`
with `FsyncPolicy` folded in; the knob count has outgrown positional arguments.
`Engine::open` stays, so most existing tests are untouched.

| Field | Default | Env |
|---|---|---|
| `fsync` | `Always` | `KVS_FSYNC` |
| `max_file_bytes` | 64 MiB | `KVS_MAX_FILE_BYTES` |
| `dead_ratio` | 0.5 | `KVS_DEAD_RATIO` |
| `min_merge_bytes` | 1 MiB | `KVS_MIN_MERGE_BYTES` |

### 7.4 The manual trigger

`POST /v1/admin/compact` returns `200` with
`{"files_merged":…,"bytes_reclaimed":…,"duration_ms":…}`, or `409 Conflict` if a
merge is already running.

`Engine::compact() -> Result<MergeOutcome>` blocks until the merge commits, and
returns the counts the response body reports. That makes tests deterministic and
the demonstration watchable, at the cost of stalling writes for the duration —
acceptable for an explicitly requested operation.

When the capacity-1 channel is already occupied, `compact()` returns a new
`EngineError::MergeInProgress` rather than queueing behind the running merge.
That variant is what the handler maps to `409`; without it the endpoint has no way
to distinguish "already running" from "done", and `AppError`'s match on
`EngineError` is exhaustive, so the variant has to exist for the mapping to
compile.

It is also the first endpoint that reports something about the store's internals.
The Kubernetes verification runs found that nothing about this process is
observable from outside it; an operation that says what it reclaimed is a direct
answer to that.

**One deadlock to note and then dismiss:** `compact()` blocks the writer thread
on a channel while the merge thread needs the store's write lock. The writer is
not holding that lock while blocked, so there is no cycle. It is named here
because it is the obvious place to introduce one.

## 8. Testing

### 8.1 The merge needs a test hook, and it is not optional

The two subtlest rules in the design — the overwrite skip and the never-insert
skip — are only reachable when a write lands *during* a merge. Tested by timing,
they would be flaky, and flaky tests guarding the two rules most likely to break
silently is the worst available outcome.

So the merge is structured as a function taking an `after_snapshot: impl Fn()`
hook: a no-op in production, a barrier in tests. Both races become deterministic.

### 8.2 Coverage, by what it defends

- **Layout and rolling** — crossing `max_file_bytes` opens a second file; replay
  across several files is last-write-wins; the active file shadows an immutable
  one.
- **Merge, quiescent** — live keys survive; deleted keys stay deleted; byte count
  and file count both drop.
- **Merge, concurrent** (via the hook) — a key overwritten mid-merge keeps its new
  value; a key removed mid-merge stays removed.
- **Crash recovery** — each state in the section 5.3 table, constructed by hand on
  disk, then `Engine::open`. Including the one that matters most: tmp present,
  `N.log` gone, stale inputs surviving, holding a `Set` for a key the merge
  dropped — assert it does not come back.
- **Hints** — a hint round-trips; deleting a hint yields the same keydir via log
  replay; a truncated hint falls back rather than losing data.
- **Stats** — the accounting moves as expected, and the ratio actually fires a
  merge.

The MVP's suite is the floor; none of it should need rewriting, and `Engine::open`
staying put is what keeps that true.

## 9. Future work

**Hints for rolled files.** Writing a hint when the active file rolls would cover
everything but the active file, at the cost of a keydir scan per roll. Worth doing
once startup time is measured rather than assumed.

**Value-size-aware scheduling.** The dead-byte ratio treats a megabyte of dead
value the same as a megabyte of dead keys. A store dominated by large values
might prefer a different trigger.

**Multiple concurrent merges.** The capacity-1 channel is a deliberate ceiling.
Lifting it means partitioning the file space so two merges cannot claim the same
inputs.

**A manifest file.** The recovery rules in section 5.3 read state off the
filesystem, which works because the merge output is always a single file with a
predictable name. A manifest becomes worthwhile if merges ever produce several
files or run concurrently.

**Replication.** Unchanged by this project except favourably: the merge protocol
establishes that the `Store` behind one `RwLock` is the right shared-state
boundary, and a replica's catch-up path will want the same file-ordered replay
that section 2 defines. The record format is untouched, so the sequence number
replication needs can be added without a migration on top of this work.
