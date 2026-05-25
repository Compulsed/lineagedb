# MVCC Multi-Reader / Multi-Writer Transaction Plan

> Long-term working document. Goal: evolve the current per-row-locked store into a
> correct multi-reader / multi-writer MVCC engine with well-defined isolation
> semantics. Smallest-correct-first; get the model right before optimizing.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done

---

## 1. How the system works today

- N worker threads pull `DatabaseCommandRequest`s off flume channels, round-robin
  load-balanced (`database/src/database/database.rs:53`, `request_manager.rs:119`).
- Every request grabs a `TransactionId` from one global atomic counter **at receipt
  time** (`database.rs:76`, `persistence/transaction.rs:282`).
- Mutations → `apply_transaction` applies each statement into the `PersonTable`
  (a `SkipMap<EntityId, RwLock<PersonRow>>`), pushing a new `PersonVersion` onto that
  row's `Vec`, then ships the transaction to a single WAL thread which fsyncs and only
  then replies to the client (`database.rs:301`, `persistence/transaction.rs:84`).
- Reads → `query_transaction` reads at a snapshot id using `at_transaction_id`,
  scanning versions in reverse for `tx_id <= snapshot` (`database.rs:275`,
  `table/row.rs:257`).

The per-row `RwLock` + lock-free `SkipMap` is a reasonable foundation for "concurrent
readers/writers don't corrupt the heap." The **transaction semantics on top of it are
not yet correct.**

---

## 2. The central bug: timestamp assignment is decoupled from visibility order

The transaction id is assigned **before** the row lock is acquired (`database.rs:76`
vs. the `.write()` inside `table/table.rs:185`). So the order in which versions get
pushed onto a row is **lock-acquisition order, not transaction-id order**. Version
*numbers* stay monotonic (each push does `current_version().version.increment()`,
`table/row.rs:193`), but the `tx_id` stamped on each version can zig-zag.

This breaks the read path, which assumes versions are ordered by tx_id. Concrete
corruption:

1. `T(id=6)` acquires the lock first, reads current state, writes `v2` stamped `tx6`.
2. `T(id=5)` acquires next. `apply_update` reads `current_version()` = `v2` (tx6's
   data, `table/row.rs:120`), builds `v3` **on top of tx6's value**, stamps it `tx5`.
3. A reader at snapshot **5** calls `at_transaction_id(5)`, scans in reverse, hits
   `v3 (tx5)` → `5 <= 5` → returns it. It just observed tx6's changes even though tx6
   should be invisible to snapshot 5.

A causality / isolation violation. It is masked today because every benchmark that
mutates uses `WRITE_THREADS = 1` (`database.rs:635`) — with a single writer, id order
== lock order and everything lines up. With two concurrent writers, snapshot isolation
is wrong.

---

## 3. Correctness issues, by severity

### 3.1 No commit timestamp → no atomic visibility; reads see uncommitted/partial/rolled-back data
A version becomes visible the instant it is pushed under the row lock — before the rest
of the transaction's statements run, before the WAL fsync, and before any rollback
decision. So:
- Multi-statement transactions are observable half-applied (row A updated, row B not).
- A reader can read data that is not yet durable (`persistence/transaction.rs:144`
  documents this).
- A reader can read versions that later get rolled back.

`at_version` even carries the telling `// TODO: Filter out the versions that are not
committed?` (`table/row.rs:239`). This is **read uncommitted**, not the "read committed"
the notes claim. Proper MVCC needs **two stamps per version**: the writer's id and a
*commit* timestamp; a version is visible only once `commit_ts` is set and
`commit_ts <= reader_snapshot`.

### 3.2 Rollback can corrupt other transactions
`apply_rollback` → `rollback_version` just does `versions.pop()` (`table/row.rs:211`).
The lock is released between statements, so if another writer pushed a newer version on
that row in the meantime, the pop removes *their* version, not yours. Combined with the
out-of-order tx_ids (§2), "pop the last" does not reliably mean "undo my write."

### 3.3 No write-write conflict detection (lost updates)
Two transactions updating the same entity just stack versions; whoever locks last wins,
silently. No first-committer-wins check. This is why the uniqueness/constraint tests are
`#[ignore]`d (`database.rs:525`, `:564`, `:585`).

### 3.4 Concurrent create of the same id races
In `apply` for `Add` (`table/table.rs:156`): `get(&id)` returns `None`, then `insert(...)`.
Two threads adding the same id both see `None` and both insert — the second
`SkipMap::insert` silently overwrites the first. No error, lost write.

### 3.5 WAL order ≠ tx-id order → restore may not reproduce live state
Transactions reach the single WAL thread in `commit()`-call order (lock order), not id
order, and are appended in that order (`persistence/transaction.rs:116`). On restore we
replay in file order (`database.rs:183`). With concurrent writers the replay order can
differ from the original apply order, so restore is not guaranteed to reconstruct the
same state.

### 3.6 Durability vs. visibility hole
`T2` can read `T1`'s in-memory write and be told "committed/durable" after its own
fsync, while `T1`'s WAL write has not landed. If `T1`'s WAL write then fails (DB
crashes, `persistence/transaction.rs:150`), restore replays `T2`-without-`T1` →
inconsistent. Consequence of §3.1 + §3.5.

### 3.7 `at_version` positional indexing is fragile
`table/row.rs:247` filters to visible versions then indexes by `version_id - 1`. This
only works while nothing is pruned and tx_ids are in order. With GC, or with the
out-of-order issue, "version N" stops meaning the Nth element. `GetVersion` is really
"Nth visible version," not "version literally numbered N."

### 3.8 Off-by-one in the clock
Clock starts at 0 (`persistence/transaction.rs:269`), `fetch_add` hands out 0 first, but
`new_first_transaction()` is 1 (`consts/consts.rs:17`, with its own TODO). The first
live transaction gets an id below the declared "first." Minor, but it bites snapshot
comparisons at the boundary.

---

## 4. What's missing for a correct MVCC system

- **A transaction object** with begin-snapshot and commit-timestamp. Today a
  "transaction" is just an id passed around — no write-set, no state machine
  (active → committed/aborted), no place to detect conflicts.
- **A global "last committed" watermark** separate from the id allocator, so readers
  take a consistent snapshot (everything with `commit_ts <= watermark`) and never see
  in-flight writers.
- **Conflict detection** at commit (write-write for snapshot isolation; read-set
  validation if serializable is ever wanted).
- **Atomic commit**: stamp all of a transaction's versions with one commit_ts and
  publish them together.
- **Version GC / vacuum**: versions accumulate forever in the `Vec` until a
  snapshot+flush. Prune versions older than the oldest live reader's snapshot.
- **True long-lived read-write transactions** (BEGIN/COMMIT sessions). Point-in-time
  *reads* exist via `TransactionContext` (`commands.rs:137`), but writers always use a
  fresh id and there is no session holding a write-set.

---

## 5. Staged plan (smallest-correct-first)

Resist jumping straight to lock-free cleverness. Get the *model* right first, then
optimize.

### Stage 0 — Make the failure visible `[x]`
- [x] Write a test with 2+ writer threads hammering the same entity, plus a concurrent
      reader asserting snapshot invariants. It should fail today (exposes §2 / §3.1 / §3.3).
      → `database/tests/mvcc_concurrency.rs`. Two invariants:
      `multi_statement_transaction_is_atomically_visible` (cross-row atomicity, target:
      Stage 2) and `rolled_back_writes_are_never_visible` (no dirty reads, target:
      Stage 2/3). Both `#[ignore]`d so CI stays green; run with
      `cargo test -p database --test mvcc_concurrency -- --ignored --nocapture`.
      Confirmed failing today: ~5k torn reads / ~32k dirty reads per run.
- [ ] (Deferred to Stage 3) Re-enable the `#[ignore]`d constraint tests
      (`database.rs:525`, `:564`, `:585`). These assert a uniqueness *feature* that was
      removed when going multi-writer; conflict detection (Stage 3) is the prerequisite,
      so they stay ignored until then rather than being re-enabled now as a red test.
- This is the regression harness for everything below. As each stage lands, remove the
  `#[ignore]` from the test it satisfies.

### Stage 1 — Separate the two clocks and add a commit timestamp `[x]`
- [x] Renamed `PersonVersion.transaction_id` → `commit_ts`; rows now only ever contain
      committed versions, so a single timestamp suffices (uncommitted writes live in the
      write-set, not the row). `database/src/database/table/row.rs`.
- [x] Added two clocks in `TransactionWAL` (`persistence/transaction.rs`):
      `commit_id_sequence` (allocates commit ids, starts at 1) and `committed_watermark`
      (last published commit id, starts at 0). `SnapshotTimestamp::Latest` readers snapshot
      at the watermark via `current_snapshot_id()`.
- [x] Visibility (`at_transaction_id` / `at_version`) filters on `commit_ts <= snapshot`.
      Because commits are serialized, a row's versions are appended in ascending
      `commit_ts` order, so the reverse-scan is correct again (kills the §2 causality leak).

### Stage 2 — Atomic, ordered commit `[x]`
- [x] Added a `WriteSet` buffer (`table/write_set.rs`): a write transaction executes its
      statements against `snapshot ∪ its own buffer` without touching shared rows, so an
      error aborts with nothing to undo (fixes unsafe `pop()` rollback, §3.2, for the live
      path).
- [x] Live commit path is `Database::commit_transaction` (`database.rs`): on success a
      single `commit_lock` critical section allocates one `commit_ts`, publishes every
      buffered version, and hands off to the WAL — all in commit-id order, so commit id
      order == publish order == WAL append order (§3.5). Multi-statement transactions are
      atomically visible. The slow fsync runs off the lock.
- [x] **Watermark is advanced only after the WAL fsync** (§3.6): the WAL thread, after
      fsyncing a batch, advances `committed_watermark` to the batch's highest commit id (in
      channel order, so monotonic) *before* sending the client response. A version is
      therefore visible only once durable, and a client that receives "committed" can
      immediately read its own write. `committed_watermark` is an `Arc<LocalClock>` shared
      with the WAL thread (`persistence/transaction.rs`).
- [x] Both Stage 0 invariant tests pass with **zero** violations and are no longer
      `#[ignore]`d — they gate CI now. Added `committed_write_is_durable_and_then_visible`
      which exercises the real `File(Sync)` fsync path (`database/tests/mvcc_concurrency.rs`).
- [x] Isolation level: **snapshot isolation** (readers see a consistent committed snapshot;
      writers read at their begin snapshot). Write-write conflict detection is Stage 3.
- Note on the design fork (write-set vs. in-place invisible versions): chose the **write-set
  buffer**. It keeps shared rows containing only committed data and sets up Stage 3
  (validate before publish) and Stage 5 (a transaction handle that owns its write-set).
- Known degraded-mode edge: if an fsync *fails*, the WAL thread leaves that batch's versions
  published-but-invisible and reports "unsure if durable" (pre-existing handling). A later
  successful batch would advance the watermark past it; treating fsync failure as fatal
  (like a WAL write failure already is) would be cleaner — noted for a future hardening pass.

### Stage 3 — Conflict detection + safe rollback `[x]`
- [x] First-committer-wins (§3.3): `PersonTable::find_write_conflict` checks, under the
      commit lock, whether any written entity has a latest committed `commit_ts` greater
      than the transaction's snapshot; if so the transaction aborts with a write-conflict
      and publishes nothing (`table/table.rs`, `database.rs::commit_transaction`).
- [x] Safe rollback / abort-by-discard (§3.2): in the write-set model an aborted
      transaction never published, so abort just drops the buffer — there is nothing to
      pop and no other transaction's writes can be disturbed. (The legacy `pop()` path in
      `apply_transaction` only runs single-threaded during restore.)
- [x] Create race (§3.4): two concurrent `Add`s of the same id now resolve to exactly one
      winner — the loser either sees the row already exists at its snapshot (validation) or
      is caught by the conflict check at commit. Covered by
      `concurrent_add_of_same_id_has_exactly_one_winner`.
- [x] Tests: unit tests for `find_write_conflict` (`table.rs`), the create-race integration
      test, and the atomic-visibility test updated so contending writers retry on conflict
      (the realistic snapshot-isolation client pattern).
- **Scope correction:** the `#[ignore]`d tests at `database.rs:525/564/585` assert **email
  uniqueness**, which needs a *unique secondary index* feature — separate from MVCC
  write-write conflict detection. They stay ignored; a unique-index feature would be its
  own piece of work (candidate future stage), at which point conflict detection here is the
  concurrency-safety prerequisite it can build on.

### Stage 4 — Vacuum `[x]` (aggressive MVCC GC)
- Decision: **aggressive MVCC GC** — reclaim every version no active transaction can still
  see, keeping only each row's latest visible version. Trades away unbounded time-travel
  (GetVersion / point-in-time) below a GC horizon, Postgres-style. (The other option,
  keeping full lineage history, was considered and rejected for this stage.)
- [x] **Stop-the-world** vacuum (`Control::VacuumDatabase`, `control.rs::vacuum`), using the
      existing `DatabasePauseEvent` like snapshot/reset. Pausing guarantees no transaction is
      mid-read, so a concurrent vacuum can't reap a version an in-flight reader captured at
      an older snapshot. With nothing in flight, `oldest = current watermark`.
- [x] `PersonTable::vacuum` / `PersonRow::reap_below_floor`: per row, drop every version
      strictly older than the floor (latest version with `commit_ts <= oldest`); drop rows
      that collapse to a delete tombstone. Returns the reclaimed count.
- [x] **Snapshot-too-old**: vacuum advances `gc_horizon` (in `TransactionWAL`); the read
      path rejects point-in-time reads at a snapshot below it. `Latest` reads use the
      watermark (always `>=` the GC mark) so they're never affected.
- [x] **Prereq fix (§3.7):** `at_version` now finds a version by its id, not by Vec
      position, so `GetVersion` stays correct after reaping shifts positions (a reclaimed
      version reads back as `None`).
- [x] Tests: row-level reaping/tombstone/`at_version`-by-id unit tests (`row.rs`), and
      `vacuum_reclaims_old_versions_and_rejects_too_old_reads` end-to-end.
- **Why stop-the-world (not concurrent):** a correct concurrent GC needs every reader to
  publish its snapshot before reading (a global registry or per-thread snapshot slots), which
  would add a hot-path synchronization point and undercut the lock-free read concurrency the
  engine showcases. Maintenance-style stop-the-world keeps reads fast and is obviously safe.
- **Stage 5 hook:** `oldest = watermark` is only correct because all transactions are
  short-lived today. Once long-lived transactions exist they hold a snapshot across the
  pause, so `oldest` must become `min(watermark, oldest active transaction)` — i.e. the
  active-snapshot registry deferred from here lands in Stage 5.

### Stage 5 — Long-lived read-write transactions `[x]`
- [x] Session protocol: `DatabaseCommand::Interactive(InteractiveCommand)` with
      `Begin` / `Execute(handle, statements)` / `Commit(handle)` / `Rollback(handle)`
      (`commands.rs`), exposed via `RequestManager::send_begin_transaction` /
      `send_transaction_statements` / `send_commit_transaction` / `send_rollback_transaction`.
      `Begin` returns a `Uuid` handle.
- [x] Server-side `InteractiveTransaction` (begin-snapshot + `WriteSet` + recorded mutation
      statements for the WAL) held in `Database::interactive_transactions`
      (`SkipMap<Uuid, Mutex<..>>`), so the snapshot and write-set persist across requests and
      worker threads. `run_interactive` drives each step.
- [x] Reads inside the transaction see its begin-snapshot + its own buffered writes
      (read-your-writes); mutations are buffered and invisible to others until commit.
- [x] Commit reuses the shared `finalize_commit` (first-committer-wins conflict check +
      atomic publish + WAL), so a long-lived transaction that lost a write race aborts and
      the client retries. Rollback just discards the buffer.
- [x] **Active-snapshot registry** (`snapshot_registry.rs`, deferred from Stage 4): an
      interactive transaction registers its snapshot at `Begin` and unregisters at
      `Commit`/`Rollback`. Vacuum now reaps below `min(watermark, oldest active snapshot)`,
      so it preserves exactly the history an open transaction can still read. Only
      interactive transactions register (one-shot work is never in flight during a
      stop-the-world vacuum), so the read/write hot path is untouched.
- [x] Tests: snapshot isolation + read-your-writes, interactive write-write conflict,
      rollback discards writes, and `vacuum_respects_open_transaction_then_reclaims` (vacuum
      preserves the open transaction's snapshot, then reclaims once it closes). Plus
      `ActiveSnapshots` unit tests.
- **Known limitations / follow-ups:**
  - No idle/abandoned-transaction reaper: a client that `Begin`s and never commits/rolls
    back leaks a registered snapshot and pins the GC horizon forever. A real system needs a
    transaction timeout.
  - A failed `Execute` statement leaves the transaction open with partial buffered writes;
    the client is expected to roll back (no automatic abort-on-error yet).
  - `List` / `GetVersion` inside a write transaction read the committed snapshot only and do
    not overlay the transaction's own buffered writes (only `Get` does); see the note in
    `write_set.rs`.
- This is where the GraphQL session work in `docs/notes.md` plugs in.

---

## 6. References

- **tihku** — MVCC database in Rust (already bookmarked in `docs/notes.md`):
  https://github.com/penberg/tihku
- **CMU 15-445** MVCC lectures (two-timestamp model, visibility, GC, conflict detection).
- Wu et al., *An Empirical Evaluation of In-Memory MVCC* — survey of version storage /
  GC / conflict-detection trade-offs.
- Postgres WAL reliability (already referenced in `persistence/transaction.rs`):
  https://www.postgresql.org/docs/current/wal-reliability.html
