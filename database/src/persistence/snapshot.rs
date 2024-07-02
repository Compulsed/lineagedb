use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    consts::consts::TransactionId,
    database::{
        orchestrator::DatabasePauseEvent,
        table::{row::PersonVersion, table::PersonTable},
    },
    model::statement::Statement,
};

use super::storage::Storage;

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
    storage: Arc<Mutex<dyn Storage + Sync + Send>>,
}

impl SnapshotManager {
    pub fn new(storage: Arc<Mutex<dyn Storage + Sync + Send>>) -> Self {
        Self { storage }
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

    fn read_file<T: DeserializeOwned + Default>(&self, file_path: FileType) -> T {
        let result = self
            .storage
            .lock()
            .unwrap()
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

        self.storage
            .lock()
            .unwrap()
            .write_blob(file_path.as_str().to_string(), serialized_bytes.to_vec());
    }
}
