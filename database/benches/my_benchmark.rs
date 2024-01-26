use criterion::{criterion_group, criterion_main, Criterion};
use database::{
    consts::consts::EntityId,
    database::{
        database::test_utils::database_test,
        table::row::{UpdateAction, UpdatePersonData},
    },
    model::{action::Action, person::Person},
};
use uuid::Uuid;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("add 1000", |b| {
        b.iter(|| {
            // 65k tps on M1 MBA
            let action_generator = |_, _| {
                Action::Add(Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            database_test(1, 100, action_generator);
        })
    });

    c.bench_function("update 1000", |b| {
        b.iter(|| {
            let action_generator = |thread: i32, index: u32| {
                let id = EntityId(thread.to_string());
                let full_name = format!("Full Name {}-{}", thread, index);
                let email = format!("Email {}-{}", thread, index);

                if index == 0 {
                    return Action::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Action::Update(
                    id,
                    UpdatePersonData {
                        full_name: UpdateAction::Set(full_name),
                        email: UpdateAction::Set(email),
                    },
                );
            };

            database_test(1, 100, action_generator);
        })
    });

    c.bench_function("list 1000", |b| {
        b.iter(|| {
            // 100k tps on M1 MBA
            let action_generator = |thread_id: i32, index: u32| {
                let id = EntityId(thread_id.to_string());
                let full_name = format!("Full Name {}-{}", thread_id, index);
                let email = format!("Email {}-{}", thread_id, index);

                if index == 0 {
                    return Action::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Action::Get(id);
            };

            database_test(1, 100, action_generator);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
