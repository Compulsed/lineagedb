use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use database::{
    consts::consts::{EntityId, TransactionId},
    database::database::{test_utils::apply_transaction_at_next_timestamp, Database},
    model::{person::Person, statement::Statement},
};
use std::sync::{mpsc::channel, Arc};
use threadpool::ThreadPool;
use uuid::Uuid;

const SAMPLE_SIZE: u64 = 10_000;

const POOL_SIZE: [usize; 4] = [1, 2, 3, 4];

pub fn database_add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_add");

    let mut pool = ThreadPool::new(1);

    for size in POOL_SIZE.iter() {
        pool.set_num_threads(*size);

        let database = Arc::new(Database::new_test());

        group.throughput(Throughput::Elements(SAMPLE_SIZE));

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &thread_count| {
                b.iter_with_large_drop(|| {
                    let (test_tx, test_rx) = channel::<i32>();

                    for _ in 0..thread_count {
                        let test_tx = test_tx.clone();
                        let database = database.clone();

                        pool.execute(move || {
                            for _ in 0..SAMPLE_SIZE / thread_count as u64 {
                                let person = Person {
                                    id: EntityId(Uuid::new_v4().to_string()),
                                    full_name: "Test".to_string(),
                                    email: None,
                                };

                                let statements = vec![Statement::Add(person.clone())];

                                let _ = apply_transaction_at_next_timestamp(&database, statements);
                            }

                            test_tx.send(1).expect("Should not timeout");
                        });
                    }

                    test_rx
                        .iter()
                        .take(thread_count)
                        .fold(0, |a: i32, b: i32| a + b);
                })
            },
        );
    }

    group.finish();
}

pub fn database_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_get");
    let mut pool = ThreadPool::new(1);

    for size in POOL_SIZE.iter() {
        let database = Arc::new(Database::new_test());
        pool.set_num_threads(*size);

        for i in 0..SAMPLE_SIZE {
            let person = Person {
                id: EntityId(i.to_string()),
                full_name: "Test".to_string(),
                email: None,
            };

            let statements = vec![Statement::Add(person.clone())];

            let _ = apply_transaction_at_next_timestamp(&database, statements);
        }

        group.throughput(Throughput::Elements(SAMPLE_SIZE));

        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &thread_count| {
                b.iter_with_large_drop(|| {
                    let (test_tx, test_rx) = channel::<i32>();

                    for _ in 0..thread_count {
                        let test_tx = test_tx.clone();
                        let database = database.clone();

                        pool.execute(move || {
                            for i in 0..SAMPLE_SIZE / thread_count as u64 {
                                let statements = vec![Statement::Get(EntityId(i.to_string()))];

                                let _ = database
                                    .query_transaction(&TransactionId(100_0000), statements);
                            }

                            test_tx.send(1).expect("Should not timeout");
                        });
                    }

                    test_rx
                        .iter()
                        .take(thread_count)
                        .fold(0, |a: i32, b: i32| a + b);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, database_add_benchmark, database_get_benchmark);

criterion_main!(benches);
