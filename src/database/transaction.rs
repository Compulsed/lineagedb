use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;

use crate::consts::consts::TransactionId;
use crate::model::action::Action;

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionStatus {
    Applying,
    Committed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    id: TransactionId,
    action: Action,
    status: TransactionStatus,
}

#[derive(Debug)]
pub struct TransactionLog {
    transactions: Vec<Transaction>,
    log_file: File,
}

fn get_transaction_log_location(data_directory: String) -> String {
    format!("{data_directory}/transaction_log.json")
}

impl TransactionLog {
    pub fn new(data_directory: String) -> Self {
        fs::create_dir_all(&data_directory)
            .expect("Should always be able to create a path at data/");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_transaction_log_location(data_directory))
            .expect("Cannot open file");

        Self {
            transactions: vec![],
            log_file,
        }
    }

    pub fn get_current_transaction_id(&self) -> TransactionId {
        TransactionId(self.transactions.len())
    }

    pub fn add_applying(&mut self, action: Action) -> TransactionId {
        let new_transaction_id = self.get_current_transaction_id().increment();

        self.transactions.push(Transaction {
            id: new_transaction_id.clone(),
            action,
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

    pub fn restore(data_directory: String) -> Vec<Action> {
        let mut file = match File::open(get_transaction_log_location(data_directory)) {
            Ok(file) => file,
            Err(_) => return vec![],
        };

        let mut contents = String::new();

        file.read_to_string(&mut contents).unwrap();

        let mut actions: Vec<Action> = vec![];

        for transaction_string in contents.split('\n') {
            if transaction_string.is_empty() {
                continue;
            }

            let transaction: Transaction = serde_json::from_str(transaction_string).unwrap();
            actions.push(transaction.action);
        }

        actions
    }
}
