use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread::{self},
    time::{self, Duration},
};

use consts::consts::ErrorString;
use model::action::{Action, ActionResult};
use row::row::UpdateAction;
use table::table::PersonTable;
use transaction::transaction::TransactionLog;

use crate::{model::person::Person, row::row::UpdatePersonData};

mod consts;
mod model;
mod row;
mod table;
mod transaction;

fn process_action(
    person_table: &mut PersonTable,
    transaction_log: &mut TransactionLog,
    user_action: Action,
    restore: bool,
) -> Result<ActionResult, ErrorString> {
    let mut transaction_id = transaction_log.get_current_transaction_id();

    let is_mutation = user_action.is_mutation();

    if is_mutation {
        transaction_id = transaction_log.add_applying(user_action.clone());
    }

    let action_result = person_table.apply(user_action, transaction_id);

    if is_mutation {
        match action_result {
            Ok(_) => transaction_log.update_committed(restore),
            Err(_) => transaction_log.update_failed(),
        }
    }

    return action_result;
}

struct Request {
    response_sender: oneshot::Sender<ActionResult>,
    action: Action,
}

struct RequestManager {
    database_sender: Sender<Request>,
}

impl RequestManager {
    pub fn new(database_sender: Sender<Request>) -> Self {
        Self { database_sender }
    }

    pub fn send_request(&self, action: Action) -> Result<ActionResult, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<ActionResult>();

        let request = Request {
            response_sender,
            action,
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        match response_receiver.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => Ok(result),
            Err(oneshot::RecvTimeoutError::Timeout) => Err("Processor was too slow".to_string()),
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }
}

fn spawn_workers(threads: i32, database_sender: Sender<Request>) {
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

fn run_database(database_receiver: Receiver<Request>) {
    let mut person_table = PersonTable::new();
    let mut transaction_log = TransactionLog::new();

    for action in TransactionLog::restore() {
        process_action(&mut person_table, &mut transaction_log, action, true)
            .expect("Should not error when replaying valid transactions");
    }

    loop {
        let Request {
            action,
            response_sender,
        } = database_receiver.recv().unwrap();

        let action_response = process_action(
            &mut person_table,
            &mut transaction_log,
            action.clone(),
            false,
        );

        let _ = match action_response {
            Ok(action_response) => response_sender.send(action_response),
            Err(err) => response_sender.send(ActionResult::Status(format!("ERROR: {}", err))),
        };
    }
}

fn main() {
    static NTHREADS: i32 = 3;

    let (database_sender, database_receiver): (Sender<Request>, Receiver<Request>) =
        mpsc::channel();

    // Spawns threads which generate work for the database layer
    spawn_workers(NTHREADS, database_sender);

    run_database(database_receiver);
}
