# MVCC Work — Handoff / Cold-Start Notes

> Read this first if you're resuming the MVCC work after a break. It's the orientation +
> the traps that aren't obvious from the code. The *what/why* is in
> [`mvcc-plan.md`](./mvcc-plan.md) (staged plan, all stages done) and
> [`mvcc-problem-and-solution.md`](./mvcc-problem-and-solution.md) (the model).

## Current state (as of 2026-05-25)

- Branch **`ai/iteration`**, 5 commits ahead of `main` (`c769800`). **Not merged, no PR yet.**
  Commits: `Phase one` (Stages 0+1), `Stage 2`, `Stage 3 first committer wins`, `GC` (Stage 4),
  `Stage 5`.
- All six stages (0–5) implemented. **46 tests pass** (37 unit + 9 integration); full
  workspace builds. Nothing flaky observed across repeated runs.
- Result: the engine moved from read-uncommitted-with-torn-reads to **snapshot isolation**
  with first-committer-wins conflict detection, durable-before-visible commits, aggressive
  stop-the-world vacuum, and long-lived (interactive) transactions.
- Run the concurrency/MVCC tests: `cargo test -p database --test mvcc_concurrency` (~2s).

## Where things live

| Concern | File |
| --- | --- |
| Live commit path (write-set, conflict check, publish) | `database/src/database/database.rs` — `commit_transaction`, `finalize_commit` |
| Interactive (long-lived) transactions | `database.rs` — `run_interactive`, `InteractiveTransaction`; `commands.rs` — `InteractiveCommand` |
| Per-transaction write buffer | `database/src/database/table/write_set.rs` |
| Row versions + visibility + vacuum reaping | `database/src/database/table/row.rs` |
| Publish / conflict detection / vacuum (table level) | `database/src/database/table/table.rs` |
| Clocks, WAL thread, watermark, gc horizon | `database/src/persistence/transaction.rs` |
| Active-snapshot registry (for vacuum) | `database/src/database/snapshot_registry.rs` |
| Vacuum control command | `database/src/database/control.rs` — `vacuum` |
| Invariant tests | `database/tests/mvcc_concurrency.rs` |

## Gotchas / traps (the non-obvious stuff)

1. **There are TWO transaction-apply paths. Don't confuse them.**
   - `commit_transaction` → `finalize_commit` is the real MVCC path (write-set buffer,
     conflict detection, atomic publish). All live writes go here.
   - `replay_transaction` is the *immediate-apply* path used **only for restore (WAL replay)
     and the `apply_transaction_at_next_timestamp` test/bench helper**. It mutates rows as it
     goes and rolls back by `pop()`, which is safe **only because restore is single-threaded**.
     It has **no conflict detection** — never route live writes through it.
   - (Cleaned up — was next-work #2) The old `apply_transaction(mode: ApplyMode)` with its
     dead `ApplyMode::Request` branches is gone, and the `ApplyMode` enum was removed.
     `replay_transaction` takes no mode; the WAL's `commit()` now takes a `resolver` directly
     (live path) and `record_restored_transaction()` handles restore size bookkeeping.

2. **Visibility is advanced by the WAL thread after fsync, not at publish.** A committed
   version is published into the row (under `commit_lock`) but stays invisible until the WAL
   thread fsyncs the batch and advances `committed_watermark` (in commit-id order). So:
   - `send_transaction` returns only after the WAL thread responds (post-fsync, post-visible).
   - Any new code path that publishes versions but doesn't go through the WAL thread must
     advance the watermark itself (this is exactly what the restore path does synchronously).

3. **The WAL channel must stay UNBOUNDED.** `commit()` enqueues inside `commit_lock`; if the
   channel were bounded, that send could block under the lock waiting on the WAL thread,
   serializing all commits behind fsync. (Documented at the call site.)

4. **`commit_lock` ordering invariant:** allocate-commit-id → publish → WAL-enqueue all
   happen under the lock so that commit-id order == publish order == WAL-append order ==
   watermark-advance order. The WAL thread relies on receiving commits in id order to advance
   the watermark monotonically and to make restore replay in the original order. Don't move
   the WAL enqueue out of the lock.

5. **Vacuum is stop-the-world AND must consult the active-snapshot registry.**
   `oldest = min(watermark, active_snapshots.oldest())`. I once forgot the registry half and
   a test caught it (vacuum reaped a version an open transaction needed). If you add another
   vacuum trigger, don't recompute `oldest` as just the watermark.

6. **Only interactive transactions register snapshots.** One-shot reads/writes don't — they
   can't be in flight during a stop-the-world vacuum, so the hot path stays lock-free of the
   registry. **If you ever make vacuum concurrent (not stop-the-world), this breaks**: every
   reader would then need to publish its snapshot (a global registry or per-thread snapshot
   slots), which reintroduces hot-path synchronization. See the plan's Stage 4 rationale.

7. **`at_version` finds versions by their version-id field, not by Vec position** (Stage 4
   fixed this). Vacuum shifts positions. If someone "optimizes" it back to positional
   indexing, `GetVersion` breaks after a vacuum.

8. **Abandoned interactive transactions pin the GC horizon forever.** A `Begin` with no
   matching `Commit`/`Rollback` keeps its snapshot registered, so vacuum can never advance
   past it. There is **no timeout/idle reaper yet** — this is a real leak and a likely first
   thing to hit when running (not just testing).

9. **"Snapshot too old":** point-in-time reads (`AtTransactionId(s)`) with `s < gc_horizon`
   are rejected. `Latest` reads use the watermark (always `>=` gc horizon) and interactive
   transactions are protected by registration, so only old pinned one-shot reads trip it.

10. **The `#[ignore]`d "constraint" tests are about email uniqueness**, which needs a unique
    *secondary index* — a separate feature that was never built. Stage 3 (write-write
    conflict detection) does **not** make them pass. Don't expect it to.

11. **Restore is never exercised by the test suite** (`restore = false` everywhere). The
    interactive-commit → WAL → restore-replay path is logically correct (mutations are logged)
    but **untested**. If you touch WAL/restore, there is no safety net — add a restore test.

12. **WriteSet dedupes to one version per entity** (final state, first-seen order). Writing
    the same entity twice in a transaction publishes a single version. The interactive
    transaction's `mutations: Vec<Statement>` keeps the *full* statement sequence (for the WAL).

13. **`List`/`GetVersion` inside a write/interactive transaction read the committed snapshot
    only** — they do NOT overlay the transaction's own buffered writes. Only `Get` does. (Noted
    in `write_set.rs`.)

14. **Clocks:** `commit_id_sequence` starts at 1, `committed_watermark` and `gc_horizon` start
    at 0. First commit gets id 1, watermark → 1. Restore seeds via `restore_clocks`.

## Next work (roughly ranked)

1. **Idle/abandoned-transaction reaper** (timeout). Gotcha #8 — most impactful for real use.
2. ~~Delete the dead `ApplyMode::Request` branches in `apply_transaction`.~~ **DONE** —
   `ApplyMode` removed; `apply_transaction` → `replay_transaction` (restore-only). See gotcha #1.
3. **Restore tests**, especially interactive-commit → WAL → replay (gotcha #11).
4. **fsync-failure handling**: today a failed fsync leaves the batch published-but-invisible
   and a later successful batch advances the watermark past it. Treating fsync failure as
   fatal (like a WAL *write* failure already is) would be cleaner.
5. **Unique secondary index** (would let the email-uniqueness tests pass; builds on the
   conflict-detection machinery).
6. **`List`/`GetVersion` buffer overlay** inside write transactions (gotcha #13).
7. If write throughput matters: revisit the single global `commit_lock` (lock-free commit
   sequencer) and/or concurrent vacuum (needs gotcha #6's per-reader snapshot publishing).

## Merge

The work is on `ai/iteration`, 5 per-stage commits, not yet on `main`. When ready, open a PR
from `ai/iteration` → `main` (the repo's other features land via PRs, e.g. `#41`).
