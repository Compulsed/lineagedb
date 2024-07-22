use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use dynamodb::{DynamoDBStorage, DynamoOptions};
use file::FileStorage;
use postgres::{PgStorage, PostgresOptions};
use s3::{S3Options, S3Storage};
use thiserror::Error;

use crate::database::options::DatabaseOptions;

pub mod dynamodb;
pub mod file;
pub mod network;
pub mod postgres;
pub mod s3;

// Our use of anyhow is because each storage provider will return a different error type
//  this means we cannot just standardize on something like say IO Error.
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

// TODO: Our interface is a little wonky, sometimes we're passing bytes, sometimes it's a string.
//   the idea of bytes is that it can be any format, not just JSON. Though, at the moment both layers
//   strictly use JSON. If we use JSON we should just double down on the Value type
// TODO: As PgStorage is a transactional data store it can be handled differently compared to other types
//  1. Because it's transactional, we are able to safely reject ALL writes w/o crashing the database
//      we could implement this in other stores, though, for non-atomic writes we would need to build
//      our own system. For file storage, reset is not atomic as we need to delete two files. S3 and DynamoDB
//      are definitely not atomic because we need to do a of network calls.
//  2. Note: for SQL we do need the TX rollback message, we cannot just assume if there was a failure it was rolled back.
//       This is because it could commmit & the client might never get it.
//  3. At the moment it does not make sense to implement this level of rollback see `InconsistentUncommittedInMemoryWorldStateFromWALWrite`
//      as to why
pub trait Storage {
    // Control plane
    fn init(&mut self) -> StorageResult<()>;
    fn reset_database(&mut self) -> StorageResult<()>;

    // Snapshot (world state, meta data, etc.)
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()>;
    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState>;

    // Transactions
    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()>;
    fn transaction_sync(&self) -> StorageResult<()>;
    fn transaction_flush(&mut self) -> StorageResult<()>;
    fn transaction_load(&mut self) -> StorageResult<Vec<String>>;
}

#[derive(Debug, Clone, strum_macros::Display)]
pub enum StorageEngine {
    File(PathBuf),
    S3(S3Options),
    DynamoDB(DynamoOptions),
    Postgres(PostgresOptions),
}

impl StorageEngine {
    pub fn get_engine(options: DatabaseOptions) -> Arc<Mutex<dyn Storage + Sync + Send>> {
        match options.storage_engine {
            StorageEngine::File(base_dir) => Arc::new(Mutex::new(FileStorage::new(base_dir))),
            StorageEngine::S3(options) => Arc::new(Mutex::new(S3Storage::new(options.clone()))),
            StorageEngine::DynamoDB(options) => {
                Arc::new(Mutex::new(DynamoDBStorage::new(options.clone())))
            }
            StorageEngine::Postgres(options) => {
                Arc::new(Mutex::new(PgStorage::new(options.clone())))
            }
        }
    }

    pub fn get_engine_info_stats(&self) -> Vec<(String, String)> {
        let storage_engine = ("StorageEngine".to_string(), format!("{}", self));

        fn prefix(info_type: &str) -> String {
            format!("- {}", info_type)
        }

        let storage_engine_config_info: (String, String) = match self {
            StorageEngine::File(base_dir) => (
                prefix("BaseDir"),
                format!("{}", fs::canonicalize(base_dir).unwrap().display()),
            ),
            StorageEngine::S3(options) => (prefix("S3 Bucket"), format!("{}", options.bucket)),
            StorageEngine::DynamoDB(options) => (prefix("DDB Table"), format!("{}", options.table)),
            StorageEngine::Postgres(options) => {
                (prefix("SQL Database"), format!("{}", options.database))
            }
        };

        return vec![storage_engine, storage_engine_config_info];
    }
}
