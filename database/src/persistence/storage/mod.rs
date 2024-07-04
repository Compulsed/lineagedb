use std::sync::{Arc, Mutex};

use dynamodb::DynamoDBStorage;
use file::FileStorage;
use postgres::PgStorage;
use s3::S3Storage;

use crate::database::database::DatabaseOptions;

pub mod dynamodb;
pub mod file;
pub mod network;
pub mod postgres;
pub mod s3;

pub trait Storage {
    // Control plane
    fn init(&self);
    fn reset_database(&self);

    // Snapshot (world state, meta data, etc.)
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> ();
    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()>;

    // Transactions
    fn transaction_write(&mut self, transaction: &[u8]) -> ();
    fn transaction_sync(&self) -> ();
    fn transaction_flush(&mut self) -> ();
    fn transaction_load(&mut self) -> String;
}

#[derive(Debug, Clone)]
pub enum StorageEngine {
    File,
    S3(String),
    DynamoDB(String),
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
            StorageEngine::DynamoDB(table) => Arc::new(Mutex::new(DynamoDBStorage::new(
                table.clone(),
                options.data_directory.clone(),
            ))),
            StorageEngine::Postgres(database) => Arc::new(Mutex::new(PgStorage::new(
                database.clone(),
                options.data_directory.clone(),
            ))),
        }
    }
}
