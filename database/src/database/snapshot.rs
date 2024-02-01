use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Error, Read, Write},
    path::PathBuf,
    vec,
};

use serde::{Deserialize, Serialize};

use crate::{
    consts::consts::{EntityId, TransactionId},
    model::action::Action,
};

use super::table::{
    index::FullNameIndex,
    row::PersonVersion,
    table::{ApplyErrors, PersonTable},
};

pub struct SnapshotManager {
    data_directory: PathBuf,
}

enum FileNames {
    Metadata,
    Snapshot,
    SecondaryIndexFullName,
    SecondaryIndexUniqueEmail,
}

impl FileNames {
    fn as_str(&self) -> &'static str {
        match self {
            FileNames::Metadata => "metadata.json",
            FileNames::Snapshot => "snapshot.json",
            FileNames::SecondaryIndexFullName => "fullname-index.json",
            FileNames::SecondaryIndexUniqueEmail => "unique-email-index.json",
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub current_transaction_id: TransactionId,
}

struct FileNotFound;

// TODO: Can we a PK, file offset index?
//  - Any time we do a update against an existing ID, we need to know the version of the last update of the current item
//     (might be able to store the version w/ the index)
//  - How do we create a new snapshot without loading the entire table into memory?
//  - How do we know the file offset? I guess we search the string then set the range
// { "PK": "1", "offset": 0, "length": 100 }
impl SnapshotManager {
    pub fn new(data_directory: PathBuf) -> Self {
        fs::create_dir_all(&data_directory)
            .expect("Should always be able to create a path at data/");

        Self { data_directory }
    }

    pub fn restore_snapshot(&self, table: &mut PersonTable) -> Metadata {
        // -- Table
        let version_snapshots: Vec<PersonVersion> = match self.read_file(FileNames::Snapshot) {
            Ok(full_name_data) => serde_json::from_str(full_name_data.as_str()).unwrap(),
            Err(_file_not_found) => vec![],
        };

        // -- Indexes
        let full_name_index: FullNameIndex = match self.read_file(FileNames::SecondaryIndexFullName)
        {
            Ok(full_name_data) => serde_json::from_str(full_name_data.as_str()).unwrap(),
            Err(_file_not_found) => FullNameIndex::new(),
        };

        let unique_email_index: HashMap<String, EntityId> =
            match self.read_file(FileNames::SecondaryIndexUniqueEmail) {
                Ok(unique_email_index_data) => {
                    serde_json::from_str(unique_email_index_data.as_str()).unwrap()
                }
                Err(_file_not_found) => HashMap::new(),
            };

        // -- Perform the restore
        table.from_restore(version_snapshots, unique_email_index, full_name_index);

        let metadata_data = match self.read_file(FileNames::Metadata) {
            Ok(metadata_data) => serde_json::from_str(metadata_data.as_str()).unwrap(),
            Err(_file_not_found) => Metadata {
                current_transaction_id: TransactionId::new_first_transaction(),
            },
        };

        return metadata_data;
    }

    // TODO:
    //  - Create snapshot error types
    //  - There is a lot of boilerplate here, can we reduce it?
    pub fn create_snapshot(
        &self,
        table: &mut PersonTable,
        transaction_id: TransactionId,
    ) -> Result<(), ApplyErrors> {
        // -- Table
        let result = table
            .apply(Action::ListLatestVersions, transaction_id.clone())?
            .list_version();

        self.write_file(
            FileNames::Snapshot,
            serde_json::to_string(&result).unwrap().as_str(),
        );

        // -- Metadata
        self.write_file(
            FileNames::Metadata,
            serde_json::to_string(&Metadata {
                current_transaction_id: transaction_id,
            })
            .unwrap()
            .as_str(),
        );

        // -- Indexes
        self.write_file(
            FileNames::SecondaryIndexFullName,
            serde_json::to_string(&table.full_name_index)
                .unwrap()
                .as_str(),
        );

        self.write_file(
            FileNames::SecondaryIndexUniqueEmail,
            serde_json::to_string(&table.unique_email_index)
                .unwrap()
                .as_str(),
        );

        Ok(())
    }

    fn write_file(&self, file_name: FileNames, data: &str) {
        let file_path = self.data_directory.join(file_name.as_str());

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .expect("Cannot open file");

        file.write_all(data.as_bytes())
            .expect("Cannot write to file");
    }

    fn read_file(&self, file_name: FileNames) -> Result<String, FileNotFound> {
        let file_path = self.data_directory.join(file_name.as_str());

        // If file does not exist, this is what happens
        let mut file = match File::open(file_path) {
            Ok(file) => file,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => return Err(FileNotFound {}),
                _ => panic!("Error reading file: {:?}", err),
            },
        };

        let mut file_string = String::new();

        file.read_to_string(&mut file_string)
            .expect("Cannot read file");

        Ok(file_string)
    }
}
