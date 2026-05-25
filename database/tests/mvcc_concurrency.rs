//! MVCC concurrency invariants (see `docs/mvcc-plan.md`, `docs/mvcc-problem-and-solution.md`).
//!
//! These tests assert invariants that a correct multi-reader / multi-writer MVCC engine
//! with snapshot isolation MUST uphold. They began as Stage 0 "make the failure visible"
//! demonstrations (failing with thousands of violations against the original
//! apply-immediately design) and now act as regression guards: the write-set + commit
//! timestamp + watermark work (Stage 1/2) makes them pass with zero violations.
//!
//! They are real concurrency tests (multiple writer and reader threads) and run in a few
//! seconds. Each prints a short diagnostic so a regression is legible, not just a red bar.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use database::{
    consts::consts::EntityId,
    database::{
        commands::TransactionContext,
        database::Database,
        options::DatabaseOptions,
        table::row::{UpdatePersonData, UpdateStatement},
    },
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
    persistence::storage::StorageEngine,
};

const DATABASE_THREADS: usize = 4;

fn new_person(id: &str, full_name: &str) -> Person {
    Person {
        id: EntityId(id.to_string()),
        full_name: full_name.to_string(),
        email: None,
    }
}

fn set_name(name: String) -> UpdatePersonData {
    UpdatePersonData {
        full_name: UpdateStatement::Set(name),
        email: UpdateStatement::NoChanges,
    }
}

/// Pull the `full_name` out of a `Get` statement result, panicking on any other shape.
fn expect_name(result: &StatementResult) -> String {
    match result {
        StatementResult::GetSingle(Some(person)) => person.full_name.clone(),
        other => panic!("expected GetSingle(Some(..)), got {:?}", other),
    }
}

/// INVARIANT: atomic visibility of a multi-statement transaction (snapshot isolation).
///
/// A single transaction sets two rows (A and B) to the same value. Therefore, at ANY
/// snapshot, a reader that reads both rows in one transaction must see A == B: either
/// the transaction is visible (both new) or it is not (both old), never a mix.
///
/// The original apply-immediately design applied each statement under its own per-row lock
/// and made the write visible at once, so a reader could observe A updated while B still
/// held its previous value (thousands of torn reads). The write-set buffer publishes both
/// versions under one commit id and only advances the watermark afterwards, so the
/// transaction is observed all-or-nothing.
///
/// Satisfied by: Stage 1/2 (commit timestamp + watermark + write-set buffer).
#[test]
fn multi_statement_transaction_is_atomically_visible() {
    const WRITER_THREADS: u64 = 4;
    const WRITES_PER_THREAD: u64 = 10_000;
    const READER_THREADS: u64 = 4;

    let rm = Database::new(DatabaseOptions::new_benchmark().set_threads(DATABASE_THREADS)).run();

    // Seed A and B to the same starting value, in one transaction.
    rm.send_transaction(
        vec![
            Statement::Add(new_person("A", "seed")),
            Statement::Add(new_person("B", "seed")),
        ],
        TransactionContext::default(),
    )
    .expect("seed transaction should commit");

    let writers_done = Arc::new(AtomicBool::new(false));

    // Writers: each repeatedly sets A and B to the SAME fresh value in one transaction.
    let writer_handles: Vec<_> = (0..WRITER_THREADS)
        .map(|writer_id| {
            let rm = rm.clone();
            thread::spawn(move || {
                for i in 0..WRITES_PER_THREAD {
                    let value = format!("{}-{}", writer_id, i);
                    rm.send_transaction(
                        vec![
                            Statement::Update(EntityId("A".to_string()), set_name(value.clone())),
                            Statement::Update(EntityId("B".to_string()), set_name(value)),
                        ],
                        TransactionContext::default(),
                    )
                    .expect("update transaction should commit");
                }
            })
        })
        .collect();

    // Readers: read A and B in one transaction and check they always agree.
    let reader_handles: Vec<_> = (0..READER_THREADS)
        .map(|_| {
            let rm = rm.clone();
            let writers_done = writers_done.clone();
            thread::spawn(move || {
                let mut reads: u64 = 0;
                let mut violations: u64 = 0;
                let mut example: Option<(String, String)> = None;

                while !writers_done.load(Ordering::Relaxed) {
                    let results = rm
                        .send_transaction(
                            vec![
                                Statement::Get(EntityId("A".to_string())),
                                Statement::Get(EntityId("B".to_string())),
                            ],
                            TransactionContext::default(),
                        )
                        .expect("read transaction should commit");

                    let a = expect_name(&results[0]);
                    let b = expect_name(&results[1]);
                    reads += 1;

                    if a != b {
                        violations += 1;
                        if example.is_none() {
                            example = Some((a, b));
                        }
                    }
                }

                (reads, violations, example)
            })
        })
        .collect();

    for handle in writer_handles {
        handle.join().unwrap();
    }
    writers_done.store(true, Ordering::Relaxed);

    let mut total_reads = 0;
    let mut total_violations = 0;
    let mut example = None;
    for handle in reader_handles {
        let (reads, violations, ex) = handle.join().unwrap();
        total_reads += reads;
        total_violations += violations;
        example = example.or(ex);
    }

    println!(
        "atomic-visibility: {} reads, {} torn reads (A != B). example: {:?}",
        total_reads, total_violations, example
    );

    assert_eq!(
        total_violations, 0,
        "observed {} torn reads where A != B despite both being set in one transaction; \
         multi-statement transactions are not atomically visible",
        total_violations
    );
}

/// INVARIANT: once a write transaction returns "committed", its data is durable AND
/// visible to a subsequent read.
///
/// This exercises the real durability path (`File(Sync)` write mode, i.e. an actual
/// fsync). The watermark is advanced by the WAL thread only after the fsync and before the
/// client response, so a read issued after the commit response must see the write. A
/// regression that advanced the watermark after replying (or that responded before fsync)
/// would break this.
#[test]
fn committed_write_is_durable_and_then_visible() {
    let database_dir: PathBuf = ["/", "tmp", "lineagedb-test", &uuid::Uuid::new_v4().to_string()]
        .iter()
        .collect();

    // `DatabaseOptions::default()` uses File(Sync) -- a real fsync per commit batch.
    let options = DatabaseOptions::default()
        .set_storage_engine(StorageEngine::File(database_dir))
        .set_restore(false)
        .set_threads(2);

    let rm = Database::new(options).run();

    // The add returns only once the WAL thread has fsynced and advanced the watermark.
    rm.send_transaction(
        vec![Statement::Add(new_person("durable-1", "committed-value"))],
        TransactionContext::default(),
    )
    .expect("add should commit");

    // Therefore an immediate read at the latest snapshot must see it.
    let read = rm
        .send_transaction(
            vec![Statement::Get(EntityId("durable-1".to_string()))],
            TransactionContext::default(),
        )
        .expect("read should commit");

    assert_eq!(
        expect_name(&read[0]),
        "committed-value",
        "a write that returned committed must be visible to a later read"
    );
}

/// INVARIANT: a rolled-back transaction's writes are never visible to readers.
///
/// A transaction `[Update(A, marker), Update(MISSING, ..)]` must roll back entirely
/// because the second statement targets a non-existent row. Row A must therefore never
/// be observed holding `marker`.
///
/// The original design applied the first update and made it visible before the second
/// statement failed, so a concurrent reader could observe the doomed value before the
/// rollback `pop()`ed it. With the write-set buffer, a transaction's writes are never
/// published until every statement has succeeded, so an aborted transaction touches no
/// shared state at all.
///
/// Satisfied by: Stage 1/2 (writes buffered until commit).
#[test]
fn rolled_back_writes_are_never_visible() {
    const WRITER_THREADS: u64 = 4;
    const WRITES_PER_THREAD: u64 = 10_000;
    const READER_THREADS: u64 = 4;
    const MARKER: &str = "DOOMED-UNCOMMITTED-VALUE";

    let rm = Database::new(DatabaseOptions::new_benchmark().set_threads(DATABASE_THREADS)).run();

    rm.send_transaction(
        vec![Statement::Add(new_person("A", "committed"))],
        TransactionContext::default(),
    )
    .expect("seed transaction should commit");

    let writers_done = Arc::new(AtomicBool::new(false));

    // Writers: every transaction is doomed to roll back (second statement targets a
    // missing row), so `MARKER` must never become visible.
    let writer_handles: Vec<_> = (0..WRITER_THREADS)
        .map(|_| {
            let rm = rm.clone();
            thread::spawn(move || {
                for _ in 0..WRITES_PER_THREAD {
                    let _ = rm.send_transaction(
                        vec![
                            Statement::Update(EntityId("A".to_string()), set_name(MARKER.to_string())),
                            Statement::Update(EntityId("does-not-exist".to_string()), set_name("x".to_string())),
                        ],
                        TransactionContext::default(),
                    );
                    // We expect this to be Err(TransactionRollback(..)); ignore the result.
                }
            })
        })
        .collect();

    let dirty_reads = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));

    let reader_handles: Vec<_> = (0..READER_THREADS)
        .map(|_| {
            let rm = rm.clone();
            let writers_done = writers_done.clone();
            let dirty_reads = dirty_reads.clone();
            let total_reads = total_reads.clone();
            thread::spawn(move || {
                while !writers_done.load(Ordering::Relaxed) {
                    let results = rm
                        .send_transaction(
                            vec![Statement::Get(EntityId("A".to_string()))],
                            TransactionContext::default(),
                        )
                        .expect("read transaction should commit");

                    total_reads.fetch_add(1, Ordering::Relaxed);
                    if expect_name(&results[0]) == MARKER {
                        dirty_reads.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in writer_handles {
        handle.join().unwrap();
    }
    writers_done.store(true, Ordering::Relaxed);
    for handle in reader_handles {
        handle.join().unwrap();
    }

    let dirty = dirty_reads.load(Ordering::Relaxed);
    println!(
        "dirty-read: {} reads, {} observed the uncommitted/rolled-back marker",
        total_reads.load(Ordering::Relaxed),
        dirty
    );

    assert_eq!(
        dirty, 0,
        "observed {} reads of a value written by a transaction that rolled back; \
         uncommitted writes are visible to readers",
        dirty
    );
}
