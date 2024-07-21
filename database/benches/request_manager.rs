use std::sync::mpsc::channel;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use database::{
    consts::consts::EntityId,
    database::{
        commands::{ShutdownRequest, TransactionContext},
        database::{test_utils::run_action, Database, DatabaseOptions},
    },
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
};
use threadpool::ThreadPool;
use uuid::Uuid;

// does improve performance for read operations
const DATABASE_THREADS_WRITE: [usize; 4] = [1, 2, 3, 4];
const DATABASE_THREADS_READ: [usize; 4] = [1, 2, 3, 4];
const POOL_SIZE: usize = 4;

const SAMPLE_SIZE: u64 = 10_000;

const INPUT_SIZE: criterion::BatchSize = criterion::BatchSize::LargeInput;

/*
    How this bench is configured:
    1. `iter_batched` is used to avoid the database clone time / drop of the vec results from from affecting the benchmark
    2. Database is shared within the same test, this is to prevent cross state tests from affecting each other
    3. Tests turn off the sync file writes, this is to avoid the underlying file system disk flush from affecting the benchmark
    4. Each database has N + 1 threads
        - N for the database
        - 1 is used as the background file writer
    5. After each SAMPLE we reset the database, this is to avoid the database from growing too large / eating up all the memory
        - After a test with a low sample size the database cleanup can take > 30 seconds

    Possible test improvements:
    1. For read tests it could be possible that the 'run_action's single thread might not be fast enough. It could be possible
        to add a thread pool, though starting the pool in the test would affect the benchmark (starting threads is slow)
    2. Test setup requires new uuid / to string, which could be a slow heap operation
    3. Unsure if we can / what it means to shutdown the database after the test
    4. Unsure what is the right INPUT_SIZE for the benchmark

    Notes:
    1. Looks like the system monitor is accurate (max 220% CPU usage), this is expected because there are three threads
        - Database, background file writer, and the benchmark thread
    2. Impacts of database shutdown
        - Dropping *might* be done async, takes a little while for the activity monitor to report the changes in memory.
           this cleanup may might take some time in the kernel and thus affect subsequent tests
        - I suspect we are leaking the WAL thread. TODO: look into this
*/
pub fn rm_add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("rm_add_group");

    let pool = ThreadPool::new(POOL_SIZE);

    for database_write_threads in DATABASE_THREADS_WRITE.iter() {
        let options = DatabaseOptions::new_test().set_threads(*database_write_threads);

        let rm = Database::new(options).run();

        group.throughput(Throughput::Elements(SAMPLE_SIZE));

        group.bench_with_input(
            BenchmarkId::from_parameter(database_write_threads),
            database_write_threads,
            |b, &database_write_threads| {
                b.iter_batched(
                    || rm.clone(),
                    |rm| {
                        let test_rm = rm.clone();

                        let (test_tx, test_rx) = channel::<i32>();

                        for _ in 0..POOL_SIZE {
                            let local_rm = test_rm.clone();
                            let local_tx = test_tx.clone();

                            pool.execute(move || {
                                let local_actions = SAMPLE_SIZE / POOL_SIZE as u64;

                                run_action(
                                    local_rm.clone(),
                                    local_actions.try_into().unwrap(),
                                    database_write_threads.try_into().unwrap(),
                                    |_, index| {
                                        return Statement::Add(Person {
                                            id: EntityId(Uuid::new_v4().to_string()),
                                            full_name: index.to_string(),
                                            email: None,
                                        });
                                    },
                                );

                                local_tx.send(1).expect("Should not timeout");
                            });
                        }

                        test_rx
                            .iter()
                            .take(POOL_SIZE)
                            .fold(0, |a: i32, b: i32| a + b);
                    },
                    INPUT_SIZE,
                )
            },
        );

        rm.send_shutdown_request(ShutdownRequest::Coordinator)
            .expect("Should not timeout");
    }

    group.finish();
}

pub fn rm_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("rm_get_group");

    let pool = ThreadPool::new(POOL_SIZE);

    for database_read_threads in DATABASE_THREADS_READ.iter() {
        let options = DatabaseOptions::new_test().set_threads(*database_read_threads);

        let rm = Database::new(options).run();

        for i in 0..SAMPLE_SIZE {
            let person = Person {
                id: EntityId(i.to_string()),
                full_name: "Test".to_string(),
                email: None,
            };

            rm.send_single_statement(
                Statement::Add(person.clone()),
                TransactionContext::default(),
            )
            .expect("Should not timeout");
        }

        group.throughput(Throughput::Elements(SAMPLE_SIZE));

        group.bench_with_input(
            BenchmarkId::from_parameter(database_read_threads),
            database_read_threads,
            |b, &database_read_threads| {
                b.iter_batched(
                    || rm.clone(),
                    |rm| {
                        let test_rm = rm.clone();
                        let (test_tx, test_rx) = channel::<Vec<Vec<StatementResult>>>();

                        for _ in 0..POOL_SIZE {
                            let local_rm = test_rm.clone();
                            let local_tx = test_tx.clone();

                            pool.execute(move || {
                                let local_actions = SAMPLE_SIZE / POOL_SIZE as u64;

                                let stmt_results = run_action(
                                    local_rm.clone(),
                                    local_actions.try_into().unwrap(),
                                    database_read_threads.try_into().unwrap(),
                                    |_, index| Statement::Get(EntityId(index.to_string())),
                                );

                                local_tx.send(stmt_results).expect("Should not timeout");
                            });
                        }

                        // Collect all the results, and let the test handle the drop
                        return test_rx
                            .iter()
                            .take(POOL_SIZE)
                            .flatten()
                            .collect::<Vec<Vec<StatementResult>>>();
                    },
                    INPUT_SIZE,
                )
            },
        );

        rm.send_shutdown_request(ShutdownRequest::Coordinator)
            .expect("Should not timeout");
    }

    group.finish();
}

pub fn rm_hybrid_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("rm_hybrid_group");

    let pool = ThreadPool::new(POOL_SIZE);

    for database_read_threads in DATABASE_THREADS_READ.iter() {
        let options = DatabaseOptions::new_test().set_threads(*database_read_threads);

        let rm = Database::new(options).run();

        for i in 0..SAMPLE_SIZE {
            let person = Person {
                id: EntityId(i.to_string()),
                full_name: "Test".to_string(),
                email: None,
            };

            rm.send_single_statement(
                Statement::Add(person.clone()),
                TransactionContext::default(),
            )
            .expect("Should not timeout");
        }

        group.throughput(Throughput::Elements(SAMPLE_SIZE));

        group.bench_with_input(
            BenchmarkId::from_parameter(database_read_threads),
            database_read_threads,
            |b, &database_read_threads| {
                b.iter_batched(
                    || rm.clone(),
                    |rm| {
                        let test_rm = rm.clone();
                        let (test_tx, test_rx) = channel::<Vec<Vec<StatementResult>>>();

                        for _ in 0..POOL_SIZE {
                            let local_rm = test_rm.clone();
                            let local_tx = test_tx.clone();

                            pool.execute(move || {
                                let local_actions = SAMPLE_SIZE / POOL_SIZE as u64;

                                let stmt_results = run_action(
                                    local_rm.clone(),
                                    local_actions.try_into().unwrap(),
                                    database_read_threads.try_into().unwrap(),
                                    |_, index| match index % 2 {
                                        0 => Statement::Add(Person {
                                            id: EntityId(Uuid::new_v4().to_string()),
                                            full_name: index.to_string(),
                                            email: None,
                                        }),
                                        _ => Statement::Get(EntityId(index.to_string())),
                                    },
                                );

                                local_tx.send(stmt_results).expect("Should not timeout");
                            });
                        }

                        // Collect all the results, and let the test handle the drop
                        return test_rx
                            .iter()
                            .take(POOL_SIZE)
                            .flatten()
                            .collect::<Vec<Vec<StatementResult>>>();
                    },
                    INPUT_SIZE,
                )
            },
        );

        rm.send_shutdown_request(ShutdownRequest::Coordinator)
            .expect("Should not timeout");
    }

    group.finish();
}

criterion_group!(
    benches,
    rm_add_benchmark,
    rm_get_benchmark,
    rm_hybrid_benchmark
);

criterion_main!(benches);
