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

    #[error("Unhandled crash")]
    Unhandled,
}

// TODO: Determine if we are able to print out the stack from the `DatabaseCrash` error.
//  - Also we may want to print out the our the stack of what method called the crash_database method.
//      this is a little bit harder though, because how do we panic then do a process exit? Maybe
//      we need to do the process exit in a different thread.
pub fn crash_database(reason: DatabaseCrash) -> ! {
    log::error!("Database crash: {}", reason);

    // This is a serious unrecoverable crash. Database must be restarted
    process::exit(0x0100);
}
