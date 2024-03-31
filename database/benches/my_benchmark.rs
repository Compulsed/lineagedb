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
const DATABASE_THREADS: u32 = 1;

const SAMPLE_SIZE: [u64; 3] = [100, 1_000, 10_000];

pub fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_add = Database::new_test().run(DATABASE_THREADS);

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                run_action(rm_add.clone(), size.try_into().unwrap(), |index| {
                    return Statement::Add(Person {
                        id: EntityId::new(),
                        full_name: index.to_string(),
                        email: None,
                    });
                });
            });
        });
    }

    group.finish();
}

pub fn update_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_update = Database::new_test().run(DATABASE_THREADS);

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
            b.iter(|| {
                run_action(rm_update.clone(), size.try_into().unwrap(), |index| {
                    return Statement::Update(
                        EntityId("update".to_string()),
                        UpdatePersonData {
                            full_name: UpdateStatement::Set(index.to_string()),
                            email: UpdateStatement::NoChanges,
                        },
                    );
                });
            });
        });
    }

    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_get = Database::new_test().run(DATABASE_THREADS);

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
            b.iter(|| {
                run_action(rm_get.clone(), size.try_into().unwrap(), |_| {
                    Statement::Get(EntityId("get".to_string()))
                });
            });
        });
    }

    group.finish();
}

pub fn list_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_group");

    for size in SAMPLE_SIZE.iter() {
        let rm_list = Database::new_test().run(DATABASE_THREADS);

        rm_list
            .send_single_statement(Statement::Add(Person::new("list".to_string(), None)))
            .expect("Should not timeout");

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                run_action(rm_list.clone(), size.try_into().unwrap(), |_| {
                    Statement::List(Some(QueryPersonData {
                        full_name: QueryMatch::Value("list".to_string()),
                        email: QueryMatch::Any,
                    }))
                });
            });
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
