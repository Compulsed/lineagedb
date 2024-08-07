use oneshot::Sender;
use serde::{Deserialize, Serialize};
use tracing::{event, field, span, Level};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::consts::consts::TransactionId;
use crate::database::commands::DatabaseCommandResponse;
use crate::database::database::ApplyMode;
use crate::database::options::DatabaseOptions;
use crate::database::orchestrator::DatabasePauseEvent;
use crate::database::utils::crash::{crash_database, DatabaseCrash};
use crate::model::statement::Statement;

use super::storage::{Storage, StorageResult};

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

pub struct TransactionCommitData {
    applied_transaction_id: TransactionId,
    statements: Vec<Statement>,
    response: DatabaseCommandResponse,
    resolver: oneshot::Sender<DatabaseCommandResponse>,
}

pub enum TransactionWalStatus {
    Ready(flume::Sender<TransactionCommitData>),
    Uninitialized,
}

// By decoupling init from thread start we are able to initialize anything (files, directories, etc). that is needed for the WAL to start
//  without immediately starting it.
pub struct TransactionWAL {
    current_transaction_id: LocalClock,
    database_options: DatabaseOptions,
    size: AtomicUsize,
    commit_sender: TransactionWalStatus,
    storage: Arc<Mutex<dyn Storage + Sync + Send>>,
}

impl TransactionWAL {
    pub fn new(
        database_options: DatabaseOptions,
        storage: Arc<Mutex<dyn Storage + Sync + Send>>,
    ) -> Self {
        Self {
            current_transaction_id: LocalClock::new(),
            size: AtomicUsize::new(0),
            database_options,
            commit_sender: TransactionWalStatus::Uninitialized,
            storage,
        }
    }

    pub fn init(&mut self) {
        let sync_file_write = self.database_options.write_mode.clone();
        let storage_thread = self.storage.clone();

        let (sender, receiver) = flume::unbounded::<TransactionCommitData>();

        // Mark the WAL as ready to accept transactions
        self.commit_sender = TransactionWalStatus::Ready(sender);

        let _ = thread::Builder::new()
            .name("Transaction Manager".to_string())
            .spawn(move || {
                let worker_storage = storage_thread;

                loop {
                    let storage_batch = span!(Level::INFO, "root:storage-batch", batch_size = field::Empty);

                    let _enter = storage_batch.enter();

                    let mut batch: Vec<(Sender<DatabaseCommandResponse>, DatabaseCommandResponse)> =
                        vec![];

                    // Receiver.recv() gives us a nice blocking call
                    let Ok(blocking_data) = receiver.recv() else {
                        // Error will be because the sender has been dropped, we can safely exit the thread
                        return
                    };

                    event!(Level::INFO, "data_from_receiver");

                    // once the thread is token up we use `try_iter` to attempt to take a decent batch
                    let batched_data = vec![blocking_data].into_iter()
                        .chain(receiver.try_iter().take(50).collect::<Vec<TransactionCommitData>>())
                        .collect::<Vec<TransactionCommitData>>();

                    // Then we can persist the transactions to disk
                    for transaction_data in batched_data.into_iter() {
                        let TransactionCommitData {
                            applied_transaction_id,
                            statements,
                            response,
                            resolver,
                        } = transaction_data;

                        if matches!(sync_file_write, TransactionWriteMode::File(_)) {
                            let transaction_json_line = format!(
                                "{}",
                                serde_json::to_string(&Transaction {
                                    id: applied_transaction_id,
                                    statements: statements,
                                    status: TransactionStatus::Committed,
                                })
                                .unwrap()
                            );

                            // - NOTE: For disk, this is fast (because it is technically async, the OS will buffer the writes)
                            //  though for S3 it is very slow, is there any way we can buffer this?
                            let result = worker_storage
                                .lock()
                                .unwrap()
                                .transaction_write(transaction_json_line.as_bytes());

                            // There are a few problems here:
                            // 1. We are 'committing' to world state, and other writes can read that commit BEFORE it is durable to disk.
                            //      the only benefit to this approach is that at least we are not responding committed to the CLIENT until it is durable.
                            // 2. The above is not great -- though this type of error is especially bad, this is because once we get to this point
                            //      of not being able to commit the transaction to disk, the world state is now invalid and non-recoverable w/o
                            //      restoring from the existing WAL / snapshot. Crash, and let the caller restart the DB process.
                            if let Err(e) = result {
                                let _ =
                                    resolver.send(DatabaseCommandResponse::transaction_rollback(
                                        "Transaction aborted. Critical error writing to WAL, world state is invalid. Database crash",
                                    ));

                                crash_database(DatabaseCrash::InconsistentUncommittedInMemoryWorldStateFromWALWrite(e));
                            }
                        }

                        batch.push((resolver, response));
                    }

                    storage_batch.record("batch_size", batch.len() as u64);

                    // Performs an fsync on the transaction log, ensuring that the transaction is durable
                    // https://www.postgresql.org/docs/current/wal-reliability.html
                    //
                    // Note: This is a slow operation and if possible we should allow multiple transactions to be committed at once
                    //   e.g. every 5ms, we flush the log and send back to the caller we have committed.
                    //
                    // Note: The observed speed of fsync is ~3ms on my machine. This is a _very_ slow operation.
                    if batch.len() > 0 {
                        if let TransactionWriteMode::File(m) = &sync_file_write {
                            if m == &TransactionFileWriteMode::Sync {
                                let transaction_sync_error_result = worker_storage.lock().unwrap().transaction_sync();
    
                                if let Err(e) = transaction_sync_error_result {
                                    log::error!("Unable to fsync transaction to disk: {}", e);
    
                                    for (resolver, _) in batch {
                                        let _ = resolver.send(DatabaseCommandResponse::transaction_status(
                                            "Unable to flush transaction to disk, unsure if transaction is durable",
                                        ));
                                    }
    
                                    continue;
                                }
    
                            }
                        }
                    }

                    for (resolver, response) in batch {
                        let _ = resolver.send(response);
                    }
                }
            });
    }

    // We have persisted the current state, we can delete the transaction log
    pub fn flush_transactions(&self, _: &DatabasePauseEvent) -> StorageResult<usize> {
        let flushed_size = self.size.load(Ordering::SeqCst);

        self.size.store(0, Ordering::SeqCst);

        self.storage.lock().unwrap().transaction_flush()?;

        Ok(flushed_size)
    }

    pub fn get_wal_size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    pub fn get_increment_current_transaction_id(&self) -> TransactionId {
        self.current_transaction_id.get_timestamp()
    }

    #[tracing::instrument(skip(self))]
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

            match self.commit_sender {
                TransactionWalStatus::Ready(ref sender) => {
                    sender.send(commit_data).unwrap();
                }
                TransactionWalStatus::Uninitialized => {
                    panic!(
                        r#"The WAL must be initialized before we can perform a commit. This is a programmer error because WAL initialization
                        is not dynamic and should be performed as a part of the database initialization"#
                    );
                }
            }
        }

        // We have committed a transaction, add it to our counter
        self.size.fetch_add(1, Ordering::SeqCst);
    }

    pub fn restore(&self) -> StorageResult<Vec<Transaction>> {
        let mut transactions: Vec<Transaction> = vec![];

        let transactions_data = self.storage.lock().unwrap().transaction_load()?;

        for transaction_string in transactions_data {
            transactions.push(serde_json::from_str(&transaction_string).unwrap());
        }

        Ok(transactions)
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

    #[allow(dead_code)]
    fn reset(&self) {
        self.ts_sequence.store(0, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    fn set(&self, value: usize) {
        self.ts_sequence.store(value, Ordering::SeqCst);
    }
}
