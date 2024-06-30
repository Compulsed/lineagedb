use oneshot::Sender;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::consts::consts::TransactionId;
use crate::model::statement::Statement;

use super::commands::DatabaseCommandResponse;
use super::database::{ApplyMode, DatabaseOptions};
use super::orchestrator::DatabasePauseEvent;

// Todo: use this status to denote if we have done an fsync on the transaction log
//  once fsync is done, THEN we can consider the transaction committed / durable
//  then we can send the message to the caller that we have committed the transaction
#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionStatus {
    Committed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionFileWriteMode {
    /// Writes the file to disk and performs a batched fsync
    Sync,
    /// Writes the file to disk, lets the OS buffer the writes
    OSBuffered,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionWriteMode {
    /// Writes the WAL to disk
    File(TransactionFileWriteMode),
    /// Used for testing purposes. Skips writing the file to disk
    Off,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub id: TransactionId,
    pub statements: Vec<Statement>,
    pub status: TransactionStatus,
}

#[derive(Debug)]
pub struct TransactionWAL {
    log_file: Arc<Mutex<File>>,
    database_options: DatabaseOptions,
    current_transaction_id: LocalClock,
    size: AtomicUsize,
    commit_sender: mpsc::Sender<TransactionCommitData>,
}

struct TransactionCommitData {
    applied_transaction_id: TransactionId,
    statements: Vec<Statement>,
    response: DatabaseCommandResponse,
    resolver: oneshot::Sender<DatabaseCommandResponse>,
}

fn get_transaction_log_location(data_directory: PathBuf) -> PathBuf {
    // Defaults to $CWD/data/transaction_log.json, but $CWD/data can be overridden via the CLI
    data_directory.join("transaction_log.json")
}

impl TransactionWAL {
    pub fn new(database_options: DatabaseOptions) -> Self {
        fs::create_dir_all(&database_options.data_directory)
            .expect("Should always be able to create a path at data/");

        let raw_log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_transaction_log_location(
                database_options.data_directory.clone(),
            ))
            .expect("Cannot open file");

        let log_file = Arc::new(Mutex::new(raw_log_file));

        let (sender, receiver) = mpsc::channel::<TransactionCommitData>();

        let thread_log_file = log_file.clone();
        let sync_file_write = database_options.write_mode.clone();

        // TODO: Name the thread
        thread::spawn(move || {
            let worker_log_file = thread_log_file;

            loop {
                let mut batch: Vec<(Sender<DatabaseCommandResponse>, DatabaseCommandResponse)> =
                    vec![];

                for transaction_data in receiver.try_iter() {
                    let mut file = worker_log_file.lock().unwrap();

                    let TransactionCommitData {
                        applied_transaction_id,
                        statements,
                        response,
                        resolver,
                    } = transaction_data;

                    if matches!(sync_file_write, TransactionWriteMode::File(_)) {
                        let transaction_json_line = format!(
                            "{}\n",
                            serde_json::to_string(&Transaction {
                                id: applied_transaction_id,
                                statements: statements,
                                status: TransactionStatus::Committed,
                            })
                            .unwrap()
                        );

                        // Buffered OS write, is not 'durable' without the fsync
                        file.write_all(transaction_json_line.as_bytes()).unwrap();
                    }

                    batch.push((resolver, response));
                }

                // Performs an fsync on the transaction log, ensuring that the transaction is durable
                // https://www.postgresql.org/docs/current/wal-reliability.html
                //
                // Note: This is a slow operation and if possible we should allow multiple transactions to be committed at once
                //   e.g. every 5ms, we flush the log and send back to the caller we have committed.
                //
                // Note: The observed speed of fsync is ~3ms on my machine. This is a _very_ slow operation.
                if let TransactionWriteMode::File(m) = &sync_file_write {
                    if m == &TransactionFileWriteMode::Sync {
                        let file = worker_log_file.lock().unwrap();

                        file.sync_all().unwrap();
                    }
                }

                for (resolver, response) in batch {
                    let _ = resolver.send(response);
                }
            }
        });

        Self {
            log_file,
            database_options: database_options,
            commit_sender: sender,
            current_transaction_id: LocalClock::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn flush_transactions(&self, _: &DatabasePauseEvent) -> usize {
        let path = get_transaction_log_location(self.database_options.data_directory.clone());
        let flushed_size = self.size.load(Ordering::SeqCst);

        self.size.store(0, Ordering::SeqCst);

        // When we flush we need to reset the file handle for the WAL
        let mut state = self.log_file.lock().unwrap();

        // TODO: When we are doing a dual reset, this could fail. Add
        //  the unwrap back and think this through
        let _ = fs::remove_file(&path);

        let raw_log_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .expect("Cannot open file");

        // Swap the old file handle (with transactions) with the new one (empty file handle)
        *state = raw_log_file;

        flushed_size
    }

    pub fn get_increment_current_transaction_id(&self) -> TransactionId {
        self.current_transaction_id.get_timestamp()
    }

    pub fn commit(
        &self,
        applied_transaction_id: TransactionId,
        statements: Vec<Statement>,
        response: DatabaseCommandResponse,
        mode: ApplyMode,
    ) {
        if let ApplyMode::Request(resolver) = mode {
            let commit_data = TransactionCommitData {
                applied_transaction_id: applied_transaction_id.clone(),
                statements,
                response,
                resolver,
            };

            self.commit_sender.send(commit_data).unwrap();
        }

        // We have committed a transaction, add it to our counter
        self.size.fetch_add(1, Ordering::SeqCst);
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

    pub fn set_current_transaction_id(&self, transaction_id: TransactionId) {
        self.current_transaction_id.set(transaction_id.0)
    }
}

// TODO: Usize seems odd, but that's what transaction id uses. Should change to u64
#[derive(Debug, Default)]
pub struct LocalClock {
    ts_sequence: AtomicUsize,
}

impl LocalClock {
    pub fn new() -> Self {
        Self {
            ts_sequence: AtomicUsize::new(0),
        }
    }
}

impl LocalClock {
    // It is unlikely we need `SeqCst` Acq / Rel should be sufficient
    fn get_timestamp(&self) -> TransactionId {
        TransactionId(self.ts_sequence.fetch_add(1, Ordering::SeqCst))
    }

    fn reset(&self) {
        self.ts_sequence.store(0, Ordering::SeqCst);
    }

    fn set(&self, value: usize) {
        self.ts_sequence.store(value, Ordering::SeqCst);
    }
}
