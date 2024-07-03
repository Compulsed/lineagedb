use std::sync::{Arc, Mutex};

use crate::database::database::DatabaseOptions;

use super::{
    snapshot::SnapshotManager,
    storage::{Storage, StorageEngine},
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
