use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread::{self},
    time,
};

use consts::consts::ErrorString;
use model::action::Action;
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
) -> Result<(), ErrorString> {
    let mut transaction_id = transaction_log.get_current_transaction_id();

    let is_mutation = user_action.is_mutation();

    if is_mutation {
        transaction_id = transaction_log.add_applying(user_action.clone());
    }

    let apply_result = person_table.apply(user_action, transaction_id);

    if is_mutation {
        match apply_result {
            Ok(_) => transaction_log.update_committed(restore),
            Err(_) => transaction_log.update_failed(),
        }
    }

    // If there was an error processing the action, return it to the caller
    apply_result?;

    Ok(())
}

fn main() {
    static NTHREADS: i32 = 3;

    let (tx, rx): (Sender<Action>, Receiver<Action>) = mpsc::channel();

    for thread_id in 0..NTHREADS {
        let thread_tx = tx.clone();

        thread::spawn(move || {
            let record_id = format!("[Thread {}]", thread_id.to_string());

            let add_transaction = Action::Add(Person {
                id: record_id.clone(),
                full_name: format!("[Count 0] Dale Salter"),
                email: Some(format!("dalejsalter-{}@outlook.com", thread_id)),
            });

            thread_tx.send(add_transaction).unwrap();

            let mut counter = 0;

            loop {
                counter = counter + 1;

                let transaction = Action::Update(
                    record_id.clone(),
                    UpdatePersonData {
                        full_name: UpdateAction::Set(format!("[Count {}] Dale Salter", counter)),
                        email: UpdateAction::NoChanges,
                    },
                );

                thread_tx.send(transaction).unwrap();

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
        let action = rx.recv().unwrap();
        let response = process_action(
            &mut person_table,
            &mut transaction_log,
            action.clone(),
            false,
        );

        if let Err(err) = response {
            println!("Error applying transaction: {}", err);
            println!("Action: {:?}", action)
        }

        process_action(
            &mut person_table,
            &mut transaction_log,
            Action::ListLatestVersions(1000),
            false,
        )
        .unwrap();
    }
}
