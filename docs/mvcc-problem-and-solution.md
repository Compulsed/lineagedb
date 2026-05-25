# MVCC: The Problem and the Solution

> Design note that explains, from first principles, the core defect in the current
> transaction model and what the correct model looks like. This is the "why" and "what";
> the staged "how/when" lives in [`mvcc-plan.md`](./mvcc-plan.md).

---

## The problem

### One-sentence version

Versions are made visible in **lock-acquisition order** and as soon as they are written,
but reads assume versions are ordered by **transaction id** and that every version they
can see is **committed**. Neither assumption holds once there is more than one writer, so
readers can observe uncommitted, partial, rolled-back, or causally-impossible data.

### What a "transaction" is today

A transaction is just a `TransactionId` (a `usize`) handed out from one global atomic
counter at the moment a request is *received* (`database/src/database/database.rs:76`,
`persistence/transaction.rs:282`). It carries no state, no write-set, and no notion of
when it actually committed. There is exactly **one timestamp** in the system, and it is
assigned too early to mean anything useful.

A write appends a `PersonVersion` to a row's `Vec` under that row's `RwLock`
(`table/table.rs:185`, `table/row.rs:184`), stamping it with that single id. A read scans
a row's versions in reverse and returns the first one whose `tx_id <= snapshot`
(`table/row.rs:257`).

### Why that is broken

Two independent orderings exist and they do not match:

1. **Id order** — assigned before the lock is taken.
2. **Visibility order** — the order writers actually acquire the row lock and push.

Because the id is assigned *before* locking, a transaction with a **lower** id can push
its version **after** one with a higher id. The row's `Vec` ends up ordered by version
number (monotonic) but with **non-monotonic tx_ids**. Every reader that assumes
"reverse-scan finds the right snapshot" is now wrong.

### Worked example (the causality leak)

```
T(id=6) locks row X first → reads current state → writes v2, stamped tx6
T(id=5) locks row X next  → reads current_version() == v2 (tx6's data!)
                          → builds v3 ON TOP OF tx6's value, stamps it tx5

Reader at snapshot 5:
  at_transaction_id(5) scans in reverse, hits v3 (tx5), 5 <= 5 → returns v3
  → it just observed tx6's changes, even though tx6 must be invisible at snapshot 5
```

The reader sees the effect of a transaction that should not exist in its snapshot. This
is a true isolation violation, not a stale read.

### Everything that follows from the same root cause

- **Dirty / partial / rolled-back reads.** A version is visible the instant it is
  pushed — before the transaction's other statements run, before the WAL fsync, and
  before any rollback decision. `at_version` even asks `// TODO: Filter out the versions
  that are not committed?` (`table/row.rs:239`). This is *read uncommitted*, not the
  "read committed" the notes claim.
- **Unsafe rollback.** Rollback is `versions.pop()` (`table/row.rs:211`); under
  concurrency it can pop a *different* transaction's version.
- **Lost updates.** No write-write conflict check — last locker silently wins.
- **Create race.** `Add` does get-then-insert (`table/table.rs:156`); two concurrent
  creates of the same id both see "absent" and the second overwrites the first.
- **Restore divergence.** The WAL is appended in lock order, not id order
  (`persistence/transaction.rs:116`), so replay may not reproduce the live state.
- **Durable-before-visible inversion.** A reader can act on data that is not yet durable
  and be told it committed (`persistence/transaction.rs:144`).

### Why it looks fine today

Every mutating benchmark uses a single writer thread (`WRITE_THREADS = 1`,
`database.rs:635`). With one writer, id order == lock order, the two orderings collapse
into one, and all of the above stays hidden. The defects appear only with ≥2 concurrent
writers.

---

## The solution

The fix is to stop treating a transaction as a single early-assigned number, and instead
give it a proper lifecycle with **two timestamps** and a **commit step that is the only
moment writes become visible**.

### 1. Two timestamps per transaction (and per version)

- **`begin_ts` (snapshot):** taken when the transaction starts. Defines what it can see.
- **`commit_ts`:** assigned atomically at commit, *after* all work succeeds. Defines when
  its writes become visible to others.

Each `PersonVersion` records the `commit_ts` of the transaction that produced it (and,
during its lifetime before commit, is tagged as belonging to an in-flight transaction).

### 2. A visibility rule, not a position scan

A version is visible to a reader iff:

```
version.commit_ts is set  AND  version.commit_ts <= reader.begin_ts
```

Reads filter by this predicate instead of assuming `Vec` order encodes tx order. This
single rule removes dirty reads, partial reads, rolled-back reads, and the causality leak
in one move — visibility no longer depends on lock-acquisition order at all.

### 3. A commit watermark separate from the id allocator

A global `last_committed_ts` advances only when a transaction finishes committing (and,
for durability, only after its WAL fsync). `SnapshotTimestamp::Latest` readers take their
`begin_ts` from this watermark, so they never see anything in flight.

### 4. Atomic commit

A writer buffers its changes (a write-set) and, at commit, stamps **all** of its versions
with one `commit_ts` and publishes them together. Multi-statement transactions become
all-or-nothing and never observable half-applied.

### 5. Conflict detection (first-committer-wins)

At commit, for each row in the write-set, check whether another transaction committed to
that row after this transaction's `begin_ts`. If so, abort with a write-conflict. This is
what makes lost updates and constraint violations impossible — and what lets the
`#[ignore]`d constraint tests pass again.

### 6. Abort by marking, not popping

An aborted transaction's versions simply never receive a `commit_ts`; they are invisible
by the rule in (2) and later reaped. Rollback no longer mutates a shared `Vec` and can
never disturb another transaction's writes.

### Worked example (after the fix)

```
T_a: begin_ts = 10
T_b: begin_ts = 10

Both write row X (each buffers into its own write-set; nothing is visible yet).

T_a commits first:
  - conflict check vs begin_ts 10: no one committed to X after 10 → OK
  - assign commit_ts = 11, publish T_a's version of X
  - advance watermark to 11 (after WAL fsync)

T_b commits:
  - conflict check: X was committed at 11, which is AFTER T_b.begin_ts (10) → ABORT
  - T_b's buffered version is discarded (never gets a commit_ts)

Reader with begin_ts = 11: sees T_a's X (commit_ts 11 <= 11), never sees T_b's.
Reader with begin_ts = 10: sees neither write — a clean, consistent snapshot.
```

No causality leak, no lost update, no dirty read — and the outcome is independent of who
grabbed the lock first.

### What this buys us

| Defect today | Resolved by |
| --- | --- |
| Causality leak / wrong snapshot (§ central bug) | commit_ts + visibility rule (1, 2) |
| Dirty / partial / rolled-back reads | atomic commit + watermark (3, 4) |
| Unsafe `pop()` rollback | abort-by-marking (6) |
| Lost updates / broken constraints | conflict detection (5) |
| Restore divergence, durable-before-visible | commit ordering + watermark after fsync (3, 4) |

The implementation order for all of this is tracked in [`mvcc-plan.md`](./mvcc-plan.md)
(Stages 0–5).
