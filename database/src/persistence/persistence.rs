use std::sync::{Arc, Mutex};

use crate::database::database::DatabaseOptions;

use super::{
    snapshot::SnapshotManager,
    storage::{Storage, StorageEngine, StorageResult},
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
        let storage: Arc<Mutex<dyn Storage + Sync + Send>> =
            StorageEngine::get_engine(options.clone());

        let mut transaction_wal = TransactionWAL::new(options.clone(), storage.clone());

        transaction_wal.init();

        Self {
            transaction_wal: transaction_wal,
            snapshot_manager: SnapshotManager::new(storage.clone()),
            storage,
        }
    }

    pub fn init(&self) -> StorageResult<()> {
        return self.storage.lock().unwrap().init();
    }

    pub fn reset(&self) -> StorageResult<()> {
        self.storage.lock().unwrap().reset_database()
    }
}
