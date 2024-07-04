use std::{
    any, io,
    sync::{Arc, Mutex},
};

// use dynamodb::DynamoDBStorage;
use file::FileStorage;
// use postgres::PgStorage;
// use s3::S3Storage;
use thiserror::Error;

use crate::database::database::DatabaseOptions;

pub mod file;
// pub mod dynamodb;
// pub mod network;
// pub mod postgres;
// pub mod s3;

/*
Questions:
- How do we handle the errors from different types of storage engines? does each engine implement their own error type?
    though this is kind of odd because they are the same type of error, just different implementations.
- Maybe we need to abstract the error type, as in, these errors are more behavioral rather than 'what'.

Options:
1. Each storage engine implements their own error type
    1. Files is:                        io:Error
    2. Cloud / Postgres is different    credentials, network, etc.
2. Unable to is now just a result of the storage specific implementation errors (generic...? dyn?)
3. I guess this might be where refying an error if we have to?...
*/
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Unhandled")]
    Unhandled,

    // Control plane
    #[error("Unable to initialize the storage engine")]
    UnableToInitializePersistence(anyhow::Error),

    #[error("Unable to reset the storage engine")]
    UnableToResetPersistence(io::Error),

    // Snapshot
    #[error("Unable write blob to storage")]
    UnableToWriteBlob(io::Error),

    #[error("No pervious save state found")]
    UnableToReadBlob(io::Error),

    // Transactions
    #[error("Unable to delete transaction log")]
    UnableToDeleteTransactionLog(io::Error),

    #[error("Unable to create new transaction log")]
    UnableToCreateNewTransactionLog(io::Error),

    #[error("Unable to create new transaction log")]
    UnableToSyncTransactionBufferToPersistentStorage(io::Error),

    #[error("Unable write transaction to log")]
    UnableToWriteTransaction(io::Error),

    #[error("Unable load previous transactions")]
    UnableToLoadPreviousTransactions(io::Error),
}

#[derive(Error, Debug)]

pub enum UnableToInitializePersistenceStruct {
    Io { source: io::Error },
}

pub fn from_io_error(err: io::Error) -> StorageError {
    StorageError::UnableToInitializePersistence(anyhow::Error::new(err))
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
    fn transaction_load(&mut self) -> StorageResult<String>;
}

#[derive(Debug, Clone)]
pub enum StorageEngine {
    File,
    // S3(String),
    // DynamoDB(String),
    // Postgres(String),
}

impl StorageEngine {
    pub fn get_engine(options: DatabaseOptions) -> Arc<Mutex<dyn Storage + Sync + Send>> {
        match options.storage_engine {
            StorageEngine::File => {
                Arc::new(Mutex::new(FileStorage::new(options.data_directory.clone())))
            } // StorageEngine::S3(bucket) => Arc::new(Mutex::new(S3Storage::new(
              //     bucket.clone(),
              //     options.data_directory.clone(),
              // ))),
              // StorageEngine::DynamoDB(table) => Arc::new(Mutex::new(DynamoDBStorage::new(
              //     table.clone(),
              //     options.data_directory.clone(),
              // ))),
              // StorageEngine::Postgres(database) => Arc::new(Mutex::new(PgStorage::new(
              //     database.clone(),
              //     options.data_directory.clone(),
              // ))),
        }
    }
}
