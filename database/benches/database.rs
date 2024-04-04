use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use database::{
    consts::consts::{EntityId, TransactionId},
    database::database::{ApplyMode, Database},
    model::{person::Person, statement::Statement},
};
use std::sync::Arc;
use uuid::Uuid;

const SAMPLE_SIZE: [u64; 1] = [1_000];

pub fn database_add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_add");

    for size in SAMPLE_SIZE.iter() {
        let database = Arc::new(Database::new_test());

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_with_large_drop(|| {
                for _ in 0..size {
                    let person = Person {
                        id: EntityId(Uuid::new_v4().to_string()),
                        full_name: "Test".to_string(),
                        email: None,
                    };

                    let statements = vec![Statement::Add(person.clone())];

                    database.apply_transaction(statements, ApplyMode::Restore);
                }
            })
        });
    }

    group.finish();
}

pub fn database_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_get");

    for size in SAMPLE_SIZE.iter() {
        let database = Arc::new(Database::new_test());

        for i in 0..*size {
            let person = Person {
                id: EntityId(i.to_string()),
                full_name: "Test".to_string(),
                email: None,
            };

            let statements = vec![Statement::Add(person.clone())];

            let _ = database.apply_transaction(statements, ApplyMode::Restore);
        }

        group.throughput(Throughput::Elements(*size));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_with_large_drop(|| {
                for i in 0..size {
                    let statements = vec![Statement::Get(EntityId(i.to_string()))];

                    let _ = database.query_transaction(&TransactionId(100_0000), statements);
                }
            })
        });
    }

    group.finish();
}

criterion_group!(benches, database_add_benchmark, database_get_benchmark);

criterion_main!(benches);
