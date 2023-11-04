use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread::{self},
    time,
};

use consts::consts::{ErrorString, START_AT_INDEX};
use model::action::Action;
use row::row::UpdateAction;
use table::table::PersonTable;

use crate::{model::person::Person, row::row::UpdatePersonData};

mod consts;
mod model;
mod row;
mod table;

#[derive(Debug)]
struct Transaction {
    transaction_id: usize,
    action: Action,
}

fn process_action(
    person_table: &mut PersonTable,
    processed_transactions: &mut Vec<Transaction>,
    user_action: Action,
) -> Result<(), ErrorString> {
    let mut transaction_id = processed_transactions
        .last()
        .and_then(|t| Some(t.transaction_id))
        .unwrap_or(START_AT_INDEX);

    if user_action.is_mutation() {
        transaction_id = transaction_id + 1;

        let new_transaction = Transaction {
            transaction_id: transaction_id,
            action: user_action.clone(),
        };

        processed_transactions.push(new_transaction)
    }

    person_table.apply(user_action, transaction_id)?;

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
    let mut processed_transactions: Vec<Transaction> = vec![];

    loop {
        let action = rx.recv().unwrap();
        let response = process_action(
            &mut person_table,
            &mut processed_transactions,
            action.clone(),
        );

        if let Err(err) = response {
            println!("Error applying transaction: {}", err);
            println!("Action: {:?}", action)
        }

        process_action(
            &mut person_table,
            &mut processed_transactions,
            Action::List(1000),
        )
        .unwrap();
    }
}
