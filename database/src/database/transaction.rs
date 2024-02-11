use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;

use crate::consts::consts::TransactionId;
use crate::model::action::Action;

// Todo: use this status to denote if we have done an fsync on the transaction log
//  once fsync is done, THEN we can consider the transaction committed / durable
//  then we can send the message to the caller that we have committed the transaction
#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionStatus {
    Committed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub actions: Vec<Action>,
    pub status: TransactionStatus,
}

#[derive(Debug)]
pub struct TransactionWAL {
    log_file: File,
    data_directory: PathBuf,
    current_transaction_id: TransactionId,
}

fn get_transaction_log_location(data_directory: PathBuf) -> PathBuf {
    // Defaults to $CWD/data/transaction_log.json, but $CWD/data can be overridden via the CLI
    data_directory.join("transaction_log.json")
}

impl TransactionWAL {
    pub fn new(data_directory: PathBuf) -> Self {
        fs::create_dir_all(&data_directory)
            .expect("Should always be able to create a path at data/");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_transaction_log_location(data_directory.clone()))
            .expect("Cannot open file");

        Self {
            log_file,
            data_directory: data_directory,
            current_transaction_id: TransactionId::new_first_transaction(),
        }
    }

    pub fn flush_transactions(&mut self) {
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

    pub fn commit(
        &mut self,
        applied_transaction_id: TransactionId,
        actions: Vec<Action>,
        restore: bool,
    ) {
        // We do not need to write back to the WAL if we restoring the database
        if !restore {
            // TODO: We should add a transaction lifetime, though it messes with the deserialize trait
            let transaction_json_line = format!(
                "{}\n",
                serde_json::to_string(&Transaction {
                    id: applied_transaction_id.clone(),
                    actions: actions.clone(),
                    status: TransactionStatus::Committed,
                })
                .unwrap()
            );

            let _ = &self
                .log_file
                .write_all(transaction_json_line.as_bytes())
                .unwrap();

            // Performs an fsync on the transaction log, ensuring that the transaction is durable
            // https://www.postgresql.org/docs/current/wal-reliability.html
            //
            // Note: This is a slow operation and if possible we should allow multiple transactions to be committed at once
            //   e.g. every 5ms, we flush the log and send back to the caller we have committed.
            //
            // Note: The observed speed of fsync is ~3ms on my machine. This is a _very_ slow operation.
            //
            // I'm not sure the best way to do this, i.e. another thread that flushes the log / calls commit every 5ms?
            //   we use the DB thread to do this, wake up at least every 5ms, and if there are transactions to commit, we do so
            // let _ = &self.log_file.sync_all().unwrap();
        }

        self.current_transaction_id = applied_transaction_id;
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
