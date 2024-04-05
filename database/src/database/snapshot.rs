use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, TransactionId},
    model::statement::Statement,
};

use super::{
    database::DatabaseOptions,
    orchestrator::DatabasePauseEvent,
    table::{row::PersonVersion, table::PersonTable},
};

enum FileType {
    Metadata,
    Snapshot,
    SecondaryIndexFullName,
    SecondaryIndexUniqueEmail,
}

impl FileType {
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Metadata => "metadata.json",
            FileType::Snapshot => "snapshot.json",
            FileType::SecondaryIndexFullName => "fullname-index.json",
            FileType::SecondaryIndexUniqueEmail => "unique-email-index.json",
        }
    }
}

struct PersistanceManager<T: Default + DeserializeOwned + Serialize> {
    phantom: std::marker::PhantomData<T>, // TODO: Do we actually need this?
    file: PathBuf,
}

// Note: This generic approach well because there is no special logic for each file type, once
//  we start doing something special we may need to create custom methods for each file type
impl<T: Default + DeserializeOwned + Serialize> PersistanceManager<T> {
    fn new(data_directory: &PathBuf, file_path: FileType) -> Self {
        Self {
            file: data_directory.join(file_path.as_str()),
            phantom: std::marker::PhantomData,
        }
    }

    fn read(&self) -> T {
        let mut file = match File::open(&self.file) {
            Ok(file) => file,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => return T::default(),
                _ => panic!("Error reading file: {:?}", err),
            },
        };

        let mut file_string = String::new();

        file.read_to_string(&mut file_string)
            .expect("should be able to read file into string");

        let data: T = serde_json::from_str(&file_string).unwrap();

        data
    }

    fn write(&self, data: &T) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.file)
            .expect("Cannot open file");

        let serialized_data = serde_json::to_string::<T>(data).unwrap();

        let serialized_bytes = serialized_data.as_str().as_bytes();

        file.write_all(serialized_bytes)
            .expect("Cannot write to file");
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub current_transaction_id: TransactionId,
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            current_transaction_id: TransactionId::new_first_transaction(),
        }
    }
}

pub struct SnapshotManager {
    database_options: DatabaseOptions,
    snapshot_file: PersistanceManager<Vec<PersonVersion>>,
    metadata_file: PersistanceManager<Metadata>,
    // full_name_index_file: PersistanceManager<FullNameIndex>,
    unique_email_index_file: PersistanceManager<HashMap<String, EntityId>>,
}

impl SnapshotManager {
    pub fn new(database_options: DatabaseOptions) -> Self {
        Self {
            snapshot_file: PersistanceManager::new(
                &database_options.data_directory,
                FileType::Snapshot,
            ),
            metadata_file: PersistanceManager::new(
                &database_options.data_directory,
                FileType::Metadata,
            ),
            // full_name_index_file: PersistanceManager::new(
            //     &database_options.data_directory,
            //     FileType::SecondaryIndexFullName,
            // ),
            unique_email_index_file: PersistanceManager::new(
                &database_options.data_directory,
                FileType::SecondaryIndexUniqueEmail,
            ),
            database_options,
        }
    }

    pub fn restore_snapshot(&self, table: &PersonTable) -> (usize, Metadata) {
        // -- Table
        let version_snapshots: Vec<PersonVersion> = self.snapshot_file.read();

        let snapeshot_count = version_snapshots.len();

        // -- Indexes
        // let full_name_index: FullNameIndex = self.full_name_index_file.read();

        let unique_email_index: HashMap<String, EntityId> = self.unique_email_index_file.read();

        // -- Perform the restore
        // table.from_restore(version_snapshots, unique_email_index, full_name_index);

        let metadata_data: Metadata = self.metadata_file.read();

        return (snapeshot_count, metadata_data);
    }

    pub fn create_snapshot(&self, table: &PersonTable, transaction_id: TransactionId) {
        fs::create_dir_all(&self.database_options.data_directory)
            .expect("Should always be able to create a path at data/");

        // -- Table
        let result = table
            .query_statement(Statement::ListLatestVersions, &transaction_id.clone())
            .expect("Should always be able to list latest versions")
            .list_version();

        self.snapshot_file.write(&result);

        // -- Metadata
        self.metadata_file.write(&Metadata {
            current_transaction_id: transaction_id,
        });

        // -- Indexes
        // self.full_name_index_file.write(&table.full_name_index);

        // self.unique_email_index_file
        //     .write(&table.unique_email_index);
    }

    pub fn delete_snapshot(&self, _: &DatabasePauseEvent) {
        fs::remove_dir_all(&self.database_options.data_directory)
            .expect("Should always exist, folder is created on init");

        fs::create_dir_all(&self.database_options.data_directory)
            .expect("Should always be able to create a path at data/");
    }
}
