use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use super::Persistence;

pub struct FilePersistence {
    base_path: PathBuf,
}

impl FilePersistence {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    fn get_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path)
    }
}

impl Persistence for FilePersistence {
    fn write_blob(&self, path: &str, bytes: &[u8]) -> () {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.get_path(path))
            .expect("Cannot open file");

        file.write_all(bytes).expect("Cannot write to file");
    }

    // TODO: Should have a specific type file not found, let caller handle it
    fn read_blob(&self, path: &str) -> Result<Vec<u8>, ()> {
        let mut file = match File::open(self.get_path(path)) {
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

    fn init(&self) {
        std::fs::create_dir_all(&self.base_path).expect("Cannot create directory");
    }

    fn reset(&self) {
        fs::remove_dir_all(&self.base_path)
            .expect("Should always exist, folder is created on init");

        self.init();
    }
}
