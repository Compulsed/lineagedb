use std::path::PathBuf;

use uuid::Uuid;

use crate::persistence::{
    storage::StorageEngine,
    transaction::{TransactionFileWriteMode, TransactionWriteMode},
};

#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    pub restore: bool,
    pub write_mode: TransactionWriteMode,
    pub storage_engine: StorageEngine,
    pub threads: usize,
}

// Implements: https://rust-unofficial.github.io/patterns/patterns/creational/builder.html
impl DatabaseOptions {
    /// Defines whether we should attempt to restore the database from a snapshot and transaction log
    /// on startup
    pub fn set_restore(mut self, restore: bool) -> Self {
        self.restore = restore;
        self
    }

    /// Defines whether we should sync the file write to disk before marking the
    /// transaction as committed. This is useful for durability but can be slow ~3ms per sync
    pub fn set_sync_file_write(mut self, write_mode: TransactionWriteMode) -> Self {
        self.write_mode = write_mode;
        self
    }

    pub fn set_storage_engine(mut self, storage_engine: StorageEngine) -> Self {
        self.storage_engine = storage_engine;
        self
    }

    pub fn set_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            write_mode: TransactionWriteMode::File(TransactionFileWriteMode::Sync),
            storage_engine: StorageEngine::File(PathBuf::from("data")),
            restore: true,
            threads: 2,
        }
    }
}

#[cfg(test)]
impl DatabaseOptions {
    pub fn new_test() -> Self {
        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default()
            .set_storage_engine(StorageEngine::File(database_dir))
            .set_restore(false)
            .set_threads(2)
            .set_sync_file_write(TransactionWriteMode::Off);

        return options;
    }
}

impl DatabaseOptions {
    pub fn new_benchmark() -> Self {
        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default()
            .set_storage_engine(StorageEngine::File(database_dir))
            .set_restore(false)
            .set_threads(2)
            .set_sync_file_write(TransactionWriteMode::Off);

        return options;
    }
}
