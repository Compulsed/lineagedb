use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use super::Storage;

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
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.get_path(&path))
            .expect("Cannot open file");

        file.write_all(&bytes).expect("Cannot write to file");
    }

    // TODO: Should have a specific type file not found, let caller handle it
    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        let mut file = match File::open(self.get_path(&path)) {
            Ok(file) => file,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => return Err(()),
                _ => panic!("Error reading file: {:?}", err),
            },
        };

        let mut buf = Vec::new();

        let _ = file.read_to_end(&mut buf);

        return Ok(buf);
    }

    // Called on DB Start-up
    fn init(&self) {
        std::fs::create_dir_all(&self.base_path).expect("Cannot create directory");
    }

    // Called when the database gets cleared (via user)
    fn reset_database(&self) {
        fs::remove_dir_all(&self.base_path)
            .expect("Should always exist, folder is created on init");

        self.init();
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> () {
        // Buffered OS write, is not 'durable' without the fsync
        self.log_file.write_all(transaction).unwrap();
    }

    fn transaction_sync(&self) -> () {
        self.log_file.sync_all().unwrap();
    }

    fn transaction_flush(&mut self) -> () {
        // TODO: When we are doing a dual reset, this could fail. Add
        //  the unwrap back and think this through
        let _ = fs::remove_file(self.transaction_file_path.clone());

        self.log_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&self.transaction_file_path)
            .expect("Cannot open file");
    }

    // File may or may not exist
    fn transaction_load(&mut self) -> String {
        let mut contents = String::new();

        OpenOptions::new()
            .read(true)
            .open(&self.transaction_file_path)
            .expect("Cannot open file")
            .read_to_string(&mut contents)
            .unwrap();

        contents
    }
}
