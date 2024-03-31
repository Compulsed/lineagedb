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

// Number of threads to use for the database
// Due to the RW lock, additional write threads do not improve performance (contention will cause slow down),
// does improve performance for read operations
const DATABASE_THREADS_WRITE: u32 = 1;
const DATABASE_THREADS_READ: u32 = 3;

const SAMPLE_SIZE: [u64; 3] = [100, 1_000, 10_000];

/*
    How this bench is configured:
    1. `iter_batched` is used to avoid the database clone time / drop of the vec results from from affecting the benchmark
    2. Database is shared within the same test, this is to prevent cross state tests from affecting each other
    3. Tests turn off the sync file writes, this is to avoid the underlying file system disk flush from affecting the benchmark
    4. Each database has N + 1 threads
        - N for the database
        - 1 is used as the background file writer
*/

pub fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_add = Database::new_test().run(DATABASE_THREADS_WRITE);

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm_add.clone(),
                |rm_add| {
                    run_action(rm_add, size.try_into().unwrap(), size, |id, index| {
                        return Statement::Add(Person {
                            id: EntityId(format!("{}{}", id, index)),
                            full_name: index.to_string(),
                            email: None,
                        });
                    });
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

pub fn update_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_update = Database::new_test().run(DATABASE_THREADS_WRITE);

        let person = Person {
            id: EntityId("update".to_string()),
            full_name: "Test".to_string(),
            email: None,
        };

        rm_update
            .send_single_statement(Statement::Add(person.clone()))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm_update.clone(),
                |rm_add| {
                    run_action(rm_add, size.try_into().unwrap(), size, |_, _| {
                        return Statement::Update(
                            EntityId("update".to_string()),
                            UpdatePersonData {
                                full_name: UpdateStatement::Set("Test".to_string()),
                                email: UpdateStatement::NoChanges,
                            },
                        );
                    });
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_get = Database::new_test().run(DATABASE_THREADS_READ);

        let person = Person {
            id: EntityId("get".to_string()),
            full_name: "Test".to_string(),
            email: None,
        };

        rm_get
            .send_single_statement(Statement::Add(person.clone()))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm_get.clone(),
                |rm_get| {
                    run_action(rm_get, size.try_into().unwrap(), size, |_, _| {
                        Statement::Get(EntityId("get".to_string()))
                    });
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

pub fn list_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_list = Database::new_test().run(DATABASE_THREADS_READ);

        rm_list
            .send_single_statement(Statement::Add(Person::new("list".to_string(), None)))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || rm_list.clone(),
                |rm_list| {
                    run_action(rm_list, size.try_into().unwrap(), size, |_, _| {
                        Statement::List(Some(QueryPersonData {
                            full_name: QueryMatch::Value("list".to_string()),
                            email: QueryMatch::Any,
                        }))
                    });
                },
                criterion::BatchSize::SmallInput,
            )
        });
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
