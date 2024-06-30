use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    consts::consts::TransactionId,
    model::statement::Statement,
    persistence::{file::FilePersistence, s3::S3Persistence, Persistence},
};

use super::{
    database::DatabaseOptions,
    orchestrator::DatabasePauseEvent,
    table::{row::PersonVersion, table::PersonTable},
};

enum FileType {
    Metadata,
    Snapshot,
}

impl FileType {
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Metadata => "metadata.json",
            FileType::Snapshot => "snapshot.json",
        }
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
    file_persistence: Box<dyn Persistence + Sync + Send>,
}

impl SnapshotManager {
    pub fn new(database_options: DatabaseOptions) -> Self {
        // let persistence: Box<dyn Persistence + Sync + Send> =
        //     Box::new(FilePersistence::new(database_options.data_directory));

        let persistence: Box<dyn Persistence + Sync + Send> =
            Box::new(S3Persistence::new(database_options.data_directory));

        persistence.init();

        Self {
            file_persistence: persistence,
        }
    }

    pub fn restore_snapshot(&self, table: &PersonTable) -> (usize, Metadata) {
        // -- Table
        let version_snapshots: Vec<PersonVersion> = self.read_file(FileType::Snapshot);

        let snapshot_count = version_snapshots.len();

        table.restore_table(version_snapshots);

        let metadata_data: Metadata = self.read_file(FileType::Metadata);

        return (snapshot_count, metadata_data);
    }

    pub fn create_snapshot(
        &self,
        _: &DatabasePauseEvent,
        table: &PersonTable,
        transaction_id: TransactionId,
    ) {
        // -- Table
        let result = table
            .query_statement(Statement::ListLatestVersions, &transaction_id.clone())
            .expect("Should always be able to list latest versions")
            .list_version();

        self.write_file(FileType::Snapshot, result);

        self.write_file(
            FileType::Metadata,
            &Metadata {
                current_transaction_id: transaction_id,
            },
        );
    }

    pub fn delete_snapshot(&self, _: &DatabasePauseEvent) {
        self.file_persistence.reset()
    }

    fn read_file<T: DeserializeOwned + Default>(&self, file_path: FileType) -> T {
        let result = self
            .file_persistence
            .read_blob(file_path.as_str().to_string());

        if let Ok(file_contents) = result {
            let data: T = serde_json::from_slice(&file_contents).unwrap();
            return data;
        } else {
            return T::default();
        }
    }

    fn write_file<T: Serialize>(&self, file_path: FileType, data: T) -> () {
        let serialized_data = serde_json::to_string::<T>(&data).unwrap();

        let serialized_bytes = serialized_data.as_str().as_bytes();

        self.file_persistence
            .write_blob(file_path.as_str().to_string(), serialized_bytes.to_vec());
    }
}
