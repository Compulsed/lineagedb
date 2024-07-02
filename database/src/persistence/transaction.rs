use oneshot::Sender;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::consts::consts::TransactionId;
use crate::database::commands::DatabaseCommandResponse;
use crate::database::database::{ApplyMode, DatabaseOptions};
use crate::database::orchestrator::DatabasePauseEvent;
use crate::model::statement::Statement;

use super::storage::Storage;

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

struct TransactionCommitData {
    applied_transaction_id: TransactionId,
    statements: Vec<Statement>,
    response: DatabaseCommandResponse,
    resolver: oneshot::Sender<DatabaseCommandResponse>,
}

pub struct TransactionWAL {
    database_options: DatabaseOptions,
    current_transaction_id: LocalClock,
    size: AtomicUsize,
    commit_sender: mpsc::Sender<TransactionCommitData>,
    storage: Arc<Mutex<dyn Storage + Sync + Send>>,
}

impl TransactionWAL {
    pub fn new(
        database_options: DatabaseOptions,
        storage: Arc<Mutex<dyn Storage + Sync + Send>>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<TransactionCommitData>();

        let thread_storage = storage.clone();

        let sync_file_write = database_options.write_mode.clone();

        // TODO: Should we move this into its own method?
        let _ = thread::Builder::new()
            .name("Transaction Manager".to_string())
            .spawn(move || {
                let worker_storage = thread_storage;

                loop {
                    let mut batch: Vec<(Sender<DatabaseCommandResponse>, DatabaseCommandResponse)> =
                        vec![];

                    for transaction_data in receiver.try_iter() {
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

                            worker_storage
                                .lock()
                                .unwrap()
                                .transaction_write(transaction_json_line.as_bytes())
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
                            worker_storage.lock().unwrap().transaction_sync();
                        }
                    }

                    for (resolver, response) in batch {
                        let _ = resolver.send(response);
                    }
                }
            });

        Self {
            database_options: database_options,
            commit_sender: sender,
            current_transaction_id: LocalClock::new(),
            size: AtomicUsize::new(0),
            storage,
        }
    }

    // We have persisted the current state, we can delete the transaction log
    pub fn flush_transactions(&self, _: &DatabasePauseEvent) -> usize {
        let flushed_size = self.size.load(Ordering::SeqCst);

        self.size.store(0, Ordering::SeqCst);

        self.storage.lock().unwrap().transaction_flush();

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

    pub fn restore(&self) -> Vec<Transaction> {
        let mut transactions: Vec<Transaction> = vec![];

        let transactions_data = self.storage.lock().unwrap().transaction_load();

        for transaction_string in transactions_data.split('\n') {
            if transaction_string.is_empty() {
                continue;
            }

            transactions.push(serde_json::from_str(transaction_string).unwrap());
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
