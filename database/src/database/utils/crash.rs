use std::process;

use thiserror::Error;

use crate::persistence::storage::StorageError;

#[derive(Error, Debug)]
pub enum DatabaseCrash {
    /// We commit to the in memory structure before writing to the WAL, if the WAL write fails
    /// we have an inconsistent state
    #[error("Inconsistent, uncommitted world state due to storage error: {0}")]
    InconsistentUncommittedInMemoryWorldStateFromWALWrite(StorageError),

    /// World state creation / WAL not currently implemented as an atomic operation
    /// this error is what happens when there are inconsistencies due to a storage error
    #[error("Inconsistent snapshot disk state due to storage error: {0}")]
    InconsistentStorageFromSnapshot(StorageError),

    /// Cleaning up table state / the WAL is not currently implemented as an atomic operation
    /// this error is what happens when there are inconsistencies due to a storage error
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
