use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use super::{io_to_generic_error, ReadBlobState, Storage, StorageError, StorageResult};

pub struct FileStorage {
    base_path: PathBuf,
    log_file: File,
    transaction_file_path: PathBuf,
}

impl FileStorage {
    pub fn new(base_path: PathBuf) -> Self {
        let transaction_file_path = base_path.join("transaction_log.json");

        // TODO: This is duplicated from the init function
        //  should this be refactored into a common function?
        std::fs::create_dir_all(&base_path).expect("Cannot create directory");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(transaction_file_path.clone())
            .expect("Cannot open file");

        Self {
            base_path,
            log_file,
            transaction_file_path,
        }
    }

    fn get_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path)
    }
}

impl Storage for FileStorage {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.get_path(&path))
            .map_err(|e| StorageError::UnableToWriteBlob(io_to_generic_error(e)))?;

        file.write_all(&bytes)
            .map_err(|e| StorageError::UnableToWriteBlob(io_to_generic_error(e)))
    }

    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState> {
        let mut file = match File::open(self.get_path(&path)) {
            Ok(file) => file,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => return Ok(ReadBlobState::NotFound),
                _ => return Err(StorageError::UnableToReadBlob(io_to_generic_error(err))),
            },
        };

        let mut buf = Vec::new();

        let _ = file.read_to_end(&mut buf);

        return Ok(ReadBlobState::Found(buf));
    }

    // Called on DB Start-up, should be idempotent
    fn init(&self) -> StorageResult<()> {
        std::fs::create_dir_all(&self.base_path)
            .map_err(|e| StorageError::UnableToInitializePersistence(io_to_generic_error(e)))?;

        Ok(())
    }

    // Called when the database gets cleared (via user)
    fn reset_database(&self) -> StorageResult<()> {
        fs::remove_dir_all(&self.base_path)
            .map_err(|e| StorageError::UnableToInitializePersistence(io_to_generic_error(e)))?;

        self.init()
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()> {
        // Buffered OS write, is not 'durable' without the fsync
        self.log_file
            .write_all(transaction)
            .map_err(|e| StorageError::UnableToWriteTransaction(io_to_generic_error(e)))
    }

    fn transaction_sync(&self) -> StorageResult<()> {
        self.log_file.sync_all().map_err(|e| {
            StorageError::UnableToSyncTransactionBufferToPersistentStorage(io_to_generic_error(e))
        })?;

        Ok(())
    }

    fn transaction_flush(&mut self) -> StorageResult<()> {
        // TODO: When we are doing a dual reset, this could fail. Add
        //  the unwrap back and think this through
        let _ = fs::remove_file(self.transaction_file_path.clone())
            .map_err(|e| StorageError::UnableToDeleteTransactionLog(io_to_generic_error(e)));

        self.log_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&self.transaction_file_path)
            .map_err(|e| StorageError::UnableToCreateNewTransactionLog(io_to_generic_error(e)))?;

        Ok(())
    }

    // File may or may not exist
    fn transaction_load(&mut self) -> StorageResult<String> {
        let mut contents = String::new();

        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.transaction_file_path)
            .map_err(|e| StorageError::UnableToLoadPreviousTransactions(io_to_generic_error(e)))?;

        file.read_to_string(&mut contents)
            .map_err(|e| StorageError::UnableToLoadPreviousTransactions(io_to_generic_error(e)))?;

        Ok(contents)
    }
}
