use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;

use crate::consts::consts::TRANSACTION_LOG_LOCATION;
use crate::model::action::Action;

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionStatus {
    Applying,
    Committed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    id: usize,
    action: Action,
    status: TransactionStatus,
}

#[derive(Debug)]
pub struct TransactionLog {
    transactions: Vec<Transaction>,
    log_file: File,
}

impl TransactionLog {
    pub fn new() -> Self {
        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(TRANSACTION_LOG_LOCATION)
            .expect("Cannot open file");

        Self {
            transactions: vec![],
            log_file,
        }
    }

    pub fn get_current_transaction_id(&self) -> usize {
        self.transactions.len()
    }

    pub fn add_applying(&mut self, action: Action) -> usize {
        let new_transaction_id = self.get_current_transaction_id() + 1;

        self.transactions.push(Transaction {
            id: new_transaction_id,
            action: action,
            status: TransactionStatus::Applying,
        });

        new_transaction_id
    }

    pub fn update_committed(&mut self, restore: bool) {
        let last_transaction = self
            .transactions
            .last_mut()
            .expect("should exist as all mutations are written to the log");

        last_transaction.status = TransactionStatus::Committed;

        if !restore {
            let transaction_json_line =
                format!("{}\n", serde_json::to_string(last_transaction).unwrap());

            let _ = &self
                .log_file
                .write_all(transaction_json_line.as_bytes())
                .unwrap();
        }
    }

    pub fn update_failed(&mut self) {
        self.transactions.pop();
    }

    pub fn restore() -> Vec<Action> {
        let mut file = match File::open(TRANSACTION_LOG_LOCATION) {
            Ok(file) => file,
            Err(_) => return vec![],
        };

        let mut contents = String::new();

        file.read_to_string(&mut contents).unwrap();

        let mut actions: Vec<Action> = vec![];

        for transaction_string in contents.split("\n") {
            if transaction_string.is_empty() {
                continue;
            }

            let transaction: Transaction = serde_json::from_str(transaction_string).unwrap();
            actions.push(transaction.action);
        }

        actions
    }
}
