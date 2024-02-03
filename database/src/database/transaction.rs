use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;

use crate::consts::consts::TransactionId;
use crate::model::action::Action;

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionStatus {
    Applying,
    Committed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub actions: Vec<Action>,
    pub status: TransactionStatus,
}

#[derive(Debug)]
pub struct TransactionLog {
    pub transactions: Vec<Transaction>,
    log_file: File,
    data_directory: PathBuf,
    current_transaction_id: TransactionId,
}

fn get_transaction_log_location(data_directory: PathBuf) -> PathBuf {
    // Defaults to $CWD/data/transaction_log.json, but $CWD/data can be overridden via the CLI
    data_directory.join("transaction_log.json")
}

impl TransactionLog {
    pub fn new(data_directory: PathBuf) -> Self {
        fs::create_dir_all(&data_directory)
            .expect("Should always be able to create a path at data/");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_transaction_log_location(data_directory.clone()))
            .expect("Cannot open file");

        Self {
            transactions: vec![],
            log_file,
            data_directory: data_directory,
            current_transaction_id: TransactionId::new_first_transaction(),
        }
    }

    pub fn flush_transactions(&mut self) {
        self.transactions = vec![];

        let path = get_transaction_log_location(self.data_directory.clone());

        fs::remove_file(&path).expect("Unable to remove file");

        self.log_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .expect("Cannot open file");
    }

    pub fn get_current_transaction_id(&self) -> &TransactionId {
        &self.current_transaction_id
    }

    pub fn add_applying(&mut self, actions: Vec<Action>) -> TransactionId {
        let new_transaction_id = self.get_current_transaction_id().increment();

        self.transactions.push(Transaction {
            id: new_transaction_id.clone(),
            actions,
            status: TransactionStatus::Applying,
        });

        new_transaction_id
    }

    pub fn update_committed(&mut self, applied_transaction_id: TransactionId, restore: bool) {
        let last_transaction = self
            .transactions
            .last_mut()
            .expect("should exist as all mutations are written to the log");

        last_transaction.status = TransactionStatus::Committed;

        self.current_transaction_id = applied_transaction_id;

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

    pub fn restore(data_directory: PathBuf) -> Vec<Transaction> {
        let mut file = match File::open(get_transaction_log_location(data_directory)) {
            Ok(file) => file,
            Err(_) => return vec![],
        };

        let mut contents = String::new();

        file.read_to_string(&mut contents).unwrap();

        let mut transactions: Vec<Transaction> = vec![];

        for transaction_string in contents.split('\n') {
            if transaction_string.is_empty() {
                continue;
            }

            let transaction: Transaction = serde_json::from_str(transaction_string).unwrap();

            transactions.push(transaction);
        }

        transactions
    }

    pub fn set_current_transaction_id(&mut self, transaction_id: TransactionId) {
        self.current_transaction_id = transaction_id;
    }
}
