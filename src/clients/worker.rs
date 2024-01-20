use core::time;
use std::{sync::mpsc::Sender, thread};

use crate::{
    consts::consts::EntityId,
    database::{
        request_manager::{DatabaseRequest, RequestManager},
        table::row::{UpdateAction, UpdatePersonData},
    },
    model::{action::Action, person::Person},
};

pub fn spawn_workers(threads: i32, database_sender: Sender<DatabaseRequest>) {
    for thread_id in 0..threads {
        let request_manager = RequestManager::new(database_sender.clone());

        thread::spawn(move || {
            let record_id = format!("[Thread {}]", thread_id.to_string());

            let add_transaction = Action::Add(Person {
                id: EntityId(record_id.clone()),
                full_name: format!("[Count 0] Dale Salter"),
                email: Some(format!("dalejsalter-{}@outlook.com", thread_id)),
            });

            let response = request_manager
                .send_request(add_transaction)
                .expect("Should not timeout");

            println!("{:#?}", response);

            let mut counter = 0;

            loop {
                counter = counter + 1;

                // UPDATE
                let update_transaction = Action::Update(
                    EntityId(record_id.clone()),
                    UpdatePersonData {
                        full_name: UpdateAction::Set(format!("[Count {}] Dale Salter", counter)),
                        email: UpdateAction::NoChanges,
                    },
                );

                let update_response = request_manager
                    .send_request(update_transaction)
                    .expect("Should not timeout");

                println!("{:#?}", update_response);

                // GET
                let get_action = Action::Get(EntityId(record_id.clone()));

                let get_response = request_manager
                    .send_request(get_action)
                    .expect("Should not timeout");

                println!("{:#?}", get_response);

                thread::sleep(time::Duration::from_millis(5000));
            }
        });
    }
}
