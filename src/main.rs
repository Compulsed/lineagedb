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

fn main() {
    static NTHREADS: i32 = 3;

    let (tx, rx): (
        Sender<(Action, oneshot::Sender<ActionResult>)>,
        Receiver<(Action, oneshot::Sender<ActionResult>)>,
    ) = mpsc::channel();

    for thread_id in 0..NTHREADS {
        let thread_tx = tx.clone();

        thread::spawn(move || {
            let record_id = format!("[Thread {}]", thread_id.to_string());

            let add_transaction = Action::Add(Person {
                id: record_id.clone(),
                full_name: format!("[Count 0] Dale Salter"),
                email: Some(format!("dalejsalter-{}@outlook.com", thread_id)),
            });

            let (response_sender, response_receiver) = oneshot::channel::<ActionResult>();

            let request = (add_transaction, response_sender);

            thread_tx.send(request).unwrap();

            let mut counter = 0;

            loop {
                counter = counter + 1;

                let transaction = Action::ListLatestVersions(1000);

                let (response_sender, response_receiver) = oneshot::channel::<ActionResult>();

                let request = (transaction, response_sender);

                thread_tx.send(request).unwrap();

                match response_receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(result) => println!("🎉 {:#?}", result),
                    Err(oneshot::RecvTimeoutError::Timeout) => eprintln!("Processor was too slow"),
                    Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
                }

                thread::sleep(time::Duration::from_millis(5000));
            }
        });
    }

    let mut person_table = PersonTable::new();
    let mut transaction_log = TransactionLog::new();

    for action in TransactionLog::restore() {
        process_action(&mut person_table, &mut transaction_log, action, true)
            .expect("Should not error when replaying valid transactions");
    }

    loop {
        let (action, response) = rx.recv().unwrap();

        let action_response = process_action(
            &mut person_table,
            &mut transaction_log,
            action.clone(),
            false,
        );

        match action_response {
            Ok(action_response) => response.send(action_response),
            Err(err) => response.send(ActionResult::Status(format!("ERROR: {}", err))),
        };

        // if let Err(err) = action_response {}

        // let list_response = process_action(
        //     &mut person_table,
        //     &mut transaction_log,
        //     Action::ListLatestVersions(1000),
        //     false,
        // )
        // .expect("List calls should never fail");

        // println!("{:#?}", list_response);
    }
}
