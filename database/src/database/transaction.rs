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

    ///
    /// # WAL NOTES
    /// This is the point where we could do a WAL, the issue is, why would this make sense?
    ///
    /// If we want durability the write at the end of the transaction is enough.
    ///
    /// I could maybe see a use case for the WAL in the case of performance, i.e. we know the transaction will
    /// be committed (& we respond success) so we can write it to the WAL and then apply it to the database in the background.
    ///
    /// The issue is to know that the transaction will be committed we need to do a all of the work to validate the constraints (no Logical OR Internal Errors).
    ///
    /// Also the way we are doing this today (just storing the transaction in a vector or transactions) is not useful. The only
    ///     step that makes sense is that we perform the database apply then we write the transaction to disk (transaction.update_committed()).
    ///
    /// WAL notes off of wikipedia:
    /// 1. After a certain amount of operations the WAL should be written to the database & deleted (check point)
    /// 2. WAL allows updates of a database to be done in-place
    /// 3. In a system using WAL, all modifications are written to a log before they are applied. Usually both redo and undo information is stored in the log.
    ///    - This is confusing, how do we respond OK to the client if we haven't actually applied the transaction to the database ('tell the outside world it is committed when it is not')?
    ///     https://www.youtube.com/watch?v=5blTGTwKZPI. is this just consistency levels? Oh, maybe it's to do with the fact a 'transaction' is eventual.    
    ///
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
