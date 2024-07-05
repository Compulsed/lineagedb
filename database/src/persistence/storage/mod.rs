use crate::database::database::DatabaseOptions;
use std::{
    io,
    sync::{Arc, Mutex},
};

// use dynamodb::DynamoDBStorage;
use file::FileStorage;
use postgres::PgStorage;
use s3::S3Storage;
use thiserror::Error;

pub mod file;
pub mod network;
pub mod s3;
// pub mod dynamodb;
pub mod postgres;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unhandled")]
    Unhandled,

    // Control plane
    #[error("Unable to initialize the storage engine")]
    UnableToInitializePersistence(anyhow::Error),

    #[error("Unable to reset the storage engine")]
    UnableToResetPersistence(anyhow::Error),

    // Snapshot
    #[error("Unable write blob to storage")]
    UnableToWriteBlob(anyhow::Error),

    #[error("No pervious save state found")]
    UnableToReadBlob(anyhow::Error),

    // Transactions
    #[error("Unable to delete transaction log")]
    UnableToDeleteTransactionLog(anyhow::Error),

    #[error("Unable to create new transaction log")]
    UnableToCreateNewTransactionLog(anyhow::Error),

    #[error("Unable to create new transaction log")]
    UnableToSyncTransactionBufferToPersistentStorage(anyhow::Error),

    #[error("Unable write transaction to log")]
    UnableToWriteTransaction(anyhow::Error),

    #[error("Unable load previous transactions")]
    UnableToLoadPreviousTransactions(anyhow::Error),
}

// Unable to easily convert io::Error to anyhow::Error
pub fn io_to_generic_error(error: io::Error) -> anyhow::Error {
    anyhow::Error::new(error)
}

pub type StorageResult<T> = Result<T, StorageError>;

pub enum ReadBlobState {
    Found(Vec<u8>),
    /// If not found, this is an okay state, it may mean this is the first time the database has been initialized
    /// or the file has been trimmed. Caller should resort back to a default value for this file
    NotFound,
}

pub trait Storage {
    // Control plane
    fn init(&self) -> StorageResult<()>;
    fn reset_database(&self) -> StorageResult<()>;

    // Snapshot (world state, meta data, etc.)
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()>;
    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState>;

    // Transactions
    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()>;
    fn transaction_sync(&self) -> StorageResult<()>;
    fn transaction_flush(&mut self) -> StorageResult<()>;
    fn transaction_load(&mut self) -> StorageResult<Vec<String>>;
}

#[derive(Debug, Clone)]
pub enum StorageEngine {
    File,
    S3(String),
    // DynamoDB(String),
    Postgres(String),
}

impl StorageEngine {
    pub fn get_engine(options: DatabaseOptions) -> Arc<Mutex<dyn Storage + Sync + Send>> {
        match options.storage_engine {
            StorageEngine::File => {
                Arc::new(Mutex::new(FileStorage::new(options.data_directory.clone())))
            }
            StorageEngine::S3(bucket) => Arc::new(Mutex::new(S3Storage::new(
                bucket.clone(),
                options.data_directory.clone(),
            ))),
            // StorageEngine::DynamoDB(table) => Arc::new(Mutex::new(DynamoDBStorage::new(
            //     table.clone(),
            //     options.data_directory.clone(),
            // ))),
            StorageEngine::Postgres(database) => Arc::new(Mutex::new(PgStorage::new(
                database.clone(),
                options.data_directory.clone(),
            ))),
        }
    }
}
