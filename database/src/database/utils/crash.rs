use std::process;

use thiserror::Error;

use crate::persistence::storage::StorageError;

#[derive(Error, Debug)]
pub enum DatabaseCrash {
    #[error("Inconsistent, uncommitted world state due to storage error: {0}")]
    InconsistentUncommittedInMemoryWorldStateFromWALWrite(StorageError),

    // We snapshot the world state and trim the wal, either could be inconsistent
    #[error("Inconsistent snapshot disk state due to storage error: {0}")]
    InconsistentStorageFromSnapshot(StorageError),

    /// We reset both the WAL / Snapshots, either could be inconsistent
    #[error("Inconsistent storage from restarting database: {0}")]
    InconsistentStorageFromReset(StorageError),
}

pub fn crash_database(reason: DatabaseCrash) -> ! {
    log::error!("Database crash: {}", reason);

    // This is a serious unrecoverable crash. Database must be restarted
    process::exit(0x0100);
}
