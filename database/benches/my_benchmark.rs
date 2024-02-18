use criterion::{criterion_group, criterion_main, Criterion};
use database::{
    consts::consts::EntityId,
    database::{
        database::test_utils::database_test,
        table::{
            query::{QueryMatch, QueryPersonData},
            row::{UpdatePersonData, UpdateStatement},
        },
    },
    model::{person::Person, statement::Statement},
};
use uuid::Uuid;

const WORKER_THREADS: u32 = 1;
const DESIRED_ACTIONS: u32 = 100;
const ACTIONS_PER_THREAD: u32 = DESIRED_ACTIONS / WORKER_THREADS;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("add 100", |b| {
        b.iter(|| {
            let action_generator = |_, _| {
                Statement::Add(Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            database_test(WORKER_THREADS, ACTIONS_PER_THREAD, action_generator);
        })
    });

    c.bench_function("update 100", |b| {
        b.iter(|| {
            let action_generator = |thread: u32, index: u32| {
                let id = EntityId(thread.to_string());
                let full_name = format!("Full Name {}-{}", thread, index);
                let email = format!("Email {}-{}", thread, index);

                if index == 0 {
                    return Statement::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Statement::Update(
                    id,
                    UpdatePersonData {
                        full_name: UpdateStatement::Set(full_name),
                        email: UpdateStatement::Set(email),
                    },
                );
            };

            database_test(WORKER_THREADS, ACTIONS_PER_THREAD, action_generator);
        })
    });

    c.bench_function("get 100", |b| {
        b.iter(|| {
            let action_generator = |thread_id: u32, index: u32| {
                let id = EntityId(thread_id.to_string());
                let full_name = format!("Full Name {}-{}", thread_id, index);
                let email = format!("Email {}-{}", thread_id, index);

                if index == 0 {
                    return Statement::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Statement::Get(id);
            };

            database_test(WORKER_THREADS, ACTIONS_PER_THREAD, action_generator);
        })
    });

    c.bench_function("non-indexed list 100", |b| {
        b.iter(|| {
            let action_generator = |thread_id: u32, index: u32| {
                let full_name = format!("Full Name {}-{}", thread_id, index);
                let email = format!("Email {}-{}", thread_id, index);

                if index < ACTIONS_PER_THREAD {
                    return Statement::Add(Person::new(full_name, Some(email)));
                } else {
                    // Email is not indexed
                    return Statement::List(Some(QueryPersonData {
                        full_name: QueryMatch::Any,
                        email: QueryMatch::Value("Will never match".to_string()),
                    }));
                }
            };

            database_test(WORKER_THREADS, ACTIONS_PER_THREAD * 2, action_generator);
        })
    });

    c.bench_function("indexed list 100", |b| {
        b.iter(|| {
            let action_generator = |thread_id: u32, index: u32| {
                let full_name = format!("Full Name {}-{}", thread_id, index);
                let email = format!("Email {}-{}", thread_id, index);

                if index < ACTIONS_PER_THREAD {
                    return Statement::Add(Person::new(full_name, Some(email)));
                } else {
                    // Full name is index, which means it will return 'NoResults' and this is a
                    //  must faster path
                    return Statement::List(Some(QueryPersonData {
                        full_name: QueryMatch::Value("Will never match".to_string()),
                        email: QueryMatch::Any,
                    }));
                }
            };

            database_test(WORKER_THREADS, ACTIONS_PER_THREAD * 2, action_generator);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
