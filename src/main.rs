use database::request_manager::{DatabaseRequest, RequestManager};
use model::action::Action;
use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread::{self},
    time::{self},
};

use crate::{
    database::{
        database::Database,
        table::row::{UpdateAction, UpdatePersonData},
    },
    model::person::Person,
};

mod consts;
mod database;
mod model;

fn spawn_workers(threads: i32, database_sender: Sender<DatabaseRequest>) {
    for thread_id in 0..threads {
        let request_manager = RequestManager::new(database_sender.clone());

        thread::spawn(move || {
            let record_id = format!("[Thread {}]", thread_id.to_string());

            let add_transaction = Action::Add(Person {
                id: record_id.clone(),
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
                    record_id.clone(),
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
                let get_action = Action::Get(record_id.clone());

                let get_response = request_manager
                    .send_request(get_action)
                    .expect("Should not timeout");

                println!("{:#?}", get_response);

                thread::sleep(time::Duration::from_millis(5000));
            }
        });
    }
}

fn main() {
    static NTHREADS: i32 = 3;

    let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
        mpsc::channel();

    // Spawns threads which generate work for the database layer
    spawn_workers(NTHREADS, database_sender);

    let mut database = Database::new(database_receiver);

    database.run();
}
