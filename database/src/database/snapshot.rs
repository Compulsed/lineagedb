use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    vec,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

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

enum ReadFileResult<T: DeserializeOwned> {
    Success(T),
    Default,
}

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

    pub fn restore_snapshot(&self, table: &mut PersonTable) -> (usize, Metadata) {
        // -- Table
        let version_snapshots: Vec<PersonVersion> = match self.read_file(FileNames::Snapshot) {
            ReadFileResult::Success(version_snapshots) => version_snapshots,
            ReadFileResult::Default => vec![],
        };

        let snapeshot_count = version_snapshots.len();

        // -- Indexes
        let full_name_index: FullNameIndex = match self.read_file(FileNames::SecondaryIndexFullName)
        {
            ReadFileResult::Success(version_snapshots) => version_snapshots,
            ReadFileResult::Default => FullNameIndex::new(),
        };

        let unique_email_index: HashMap<String, EntityId> =
            match self.read_file(FileNames::SecondaryIndexUniqueEmail) {
                ReadFileResult::Success(version_snapshots) => version_snapshots,
                ReadFileResult::Default => HashMap::new(),
            };

        // -- Perform the restore
        table.from_restore(version_snapshots, unique_email_index, full_name_index);

        let metadata_data: Metadata = match self.read_file(FileNames::Metadata) {
            ReadFileResult::Success(version_snapshots) => version_snapshots,
            ReadFileResult::Default => Metadata {
                current_transaction_id: TransactionId::new_first_transaction(),
            },
        };

        return (snapeshot_count, metadata_data);
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

        self.write_file(FileNames::Snapshot, &result);

        // -- Metadata
        self.write_file(
            FileNames::Metadata,
            &Metadata {
                current_transaction_id: transaction_id,
            },
        );

        // -- Indexes
        self.write_file(FileNames::SecondaryIndexFullName, &table.full_name_index);

        self.write_file(
            FileNames::SecondaryIndexUniqueEmail,
            &table.unique_email_index,
        );

        Ok(())
    }

    fn read_file<T: DeserializeOwned>(&self, file: FileNames) -> ReadFileResult<T> {
        let file_path = self.data_directory.join(file.as_str());

        let mut file = match File::open(file_path) {
            Ok(file) => file,
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => return ReadFileResult::Default,
                _ => panic!("Error reading file: {:?}", err),
            },
        };

        let mut file_string = String::new();

        file.read_to_string(&mut file_string)
            .expect("should be able to read file into string");

        let data: T = serde_json::from_str(&file_string).unwrap();

        ReadFileResult::Success(data)
    }

    fn write_file<T: Serialize>(&self, file_name: FileNames, data: T) {
        let file_path = self.data_directory.join(file_name.as_str());

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .expect("Cannot open file");

        let serialized_data = serde_json::to_string::<T>(&data).unwrap();

        let serialized_bytes = serialized_data.as_str().as_bytes();

        file.write_all(serialized_bytes)
            .expect("Cannot write to file");
    }
}
