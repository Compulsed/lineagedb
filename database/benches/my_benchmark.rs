use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use database::{
    consts::consts::EntityId,
    database::{
        database::{test_utils::run_action, Database},
        table::{
            query::{QueryMatch, QueryPersonData},
            row::{UpdatePersonData, UpdateStatement},
        },
    },
    model::{person::Person, statement::Statement},
};
use uuid::Uuid;

// Number of threads to use for the database
// Due to the RW lock, additional write threads do not improve performance (contention will cause slow down),
// does improve performance for read operations
const DATABASE_THREADS_WRITE: u32 = 1;

// There is an upper bound limit on RW lock read concurrency, this is because as a part of the RW lock implementing
//  you must get exclusive access to the lock to increment the read count. This means that reader lock performance
//  with threads is not linear, but rather has a cap. This is okay if there is a long critical section, but if the
//  critical section is short then the overhead of the RW lock will dominate the performance.
const DATABASE_THREADS_READ: u32 = 2;

const SAMPLE_SIZE: [u64; 3] = [100, 1000, 10_000];

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

pub fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_group");

    for size in SAMPLE_SIZE.iter() {
        let rm = Database::new_test().run(DATABASE_THREADS_WRITE);

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm.clone(),
                |rm| {
                    run_action(rm, size.try_into().unwrap(), size, |_, index| {
                        return Statement::Add(Person {
                            id: EntityId(Uuid::new_v4().to_string()),
                            full_name: index.to_string(),
                            email: None,
                        });
                    });
                },
                INPUT_SIZE,
            )
        });

        rm.send_shutdown_request().expect("Should not timeout");
    }

    group.finish();
}

pub fn update_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_group");

    for size in SAMPLE_SIZE.iter() {
        let rm = Database::new_test().run(DATABASE_THREADS_WRITE);

        let person = Person {
            id: EntityId("update".to_string()),
            full_name: "Test".to_string(),
            email: None,
        };

        rm.send_single_statement(Statement::Add(person.clone()))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm.clone(),
                |rm| {
                    run_action(rm, size.try_into().unwrap(), size, |_, _| {
                        return Statement::Update(
                            EntityId("update".to_string()),
                            UpdatePersonData {
                                full_name: UpdateStatement::Set("Test".to_string()),
                                email: UpdateStatement::NoChanges,
                            },
                        );
                    });
                },
                INPUT_SIZE,
            )
        });

        rm.send_shutdown_request().expect("Should not timeout");
    }

    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_group");

    for size in SAMPLE_SIZE.iter() {
        let rm = Database::new_test().run(DATABASE_THREADS_READ);

        let person = Person {
            id: EntityId("get".to_string()),
            full_name: "Test".to_string(),
            email: None,
        };

        rm.send_single_statement(Statement::Add(person.clone()))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm.clone(),
                |rm| {
                    run_action(rm, size.try_into().unwrap(), size, |_, _| {
                        Statement::Get(EntityId("get".to_string()))
                    });
                },
                INPUT_SIZE,
            )
        });

        rm.send_shutdown_request().expect("Should not timeout");
    }

    group.finish();
}

pub fn list_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_group");

    for size in SAMPLE_SIZE.iter() {
        let rm = Database::new_test().run(DATABASE_THREADS_READ);

        rm.send_single_statement(Statement::Add(Person::new("list".to_string(), None)))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm.clone(),
                |rm_list| {
                    run_action(rm_list, size.try_into().unwrap(), size, |_, _| {
                        Statement::List(Some(QueryPersonData {
                            full_name: QueryMatch::Value("list".to_string()),
                            email: QueryMatch::Any,
                        }))
                    });
                },
                INPUT_SIZE,
            )
        });

        rm.send_shutdown_request().expect("Should not timeout");
    }

    group.finish();
}

criterion_group!(
    benches,
    add_benchmark,
    update_benchmark,
    get_benchmark,
    list_benchmark
);

criterion_main!(benches);
