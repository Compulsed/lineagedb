use std::sync::{Arc, Mutex};

use crate::database::database::{DatabaseOptions, StorageEngine};

use super::{
    snapshot::SnapshotManager,
    storage::{dynamodb::DynamoDBStorage, file::FileStorage, s3::S3Storage, Storage},
    transaction::TransactionWAL,
};

// TODO: Do not expose the underlying WAL / Snapshot manager
pub struct Persistence {
    pub transaction_wal: TransactionWAL,
    pub snapshot_manager: SnapshotManager,
    storage: Arc<Mutex<dyn Storage + Sync + Send>>,
}

impl Persistence {
    pub fn new(options: DatabaseOptions) -> Self {
        let storage: Arc<Mutex<dyn Storage + Sync + Send>> = match &options.storage_engine {
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
            _ => unimplemented!(),
        };

        // Profiles the environment to ensure we are ready to write
        storage.lock().unwrap().init();

        Self {
            transaction_wal: TransactionWAL::new(options.clone(), storage.clone()),
            snapshot_manager: SnapshotManager::new(storage.clone()),
            storage,
        }
    }

    pub fn reset(&self) {
        self.storage.lock().unwrap().reset_database();
    }
}
