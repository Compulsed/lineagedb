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

use super::storage::{ReadBlobState, Storage, StorageResult};

#[derive(Debug)]
enum FileType {
    Metadata,
    Snapshot,
}

impl FileType {
    fn as_str(&self) -> &'static str {
        match self {
            FileType::Metadata => "metadata",
            FileType::Snapshot => "snapshot",
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

    #[tracing::instrument(skip(self, table))]
    pub fn restore_snapshot(&self, table: &PersonTable) -> StorageResult<(usize, Metadata)> {
        // -- Table
        let version_snapshots: Vec<PersonVersion> = self.read_file(FileType::Snapshot)?;

        let snapshot_count = version_snapshots.len();

        table.restore_table(version_snapshots);

        let metadata_data: Metadata = self.read_file(FileType::Metadata)?;

        return Ok((snapshot_count, metadata_data));
    }

    #[tracing::instrument(skip(self, table))]
    pub fn create_snapshot(
        &self,
        _: &DatabasePauseEvent,
        table: &PersonTable,
        transaction_id: TransactionId,
    ) -> StorageResult<()> {
        // -- Table
        let result = table
            .query_statement(Statement::ListLatestVersions, &transaction_id.clone())
            .expect("Should always be able to list latest versions")
            .list_version();

        self.write_file(FileType::Snapshot, result)?;

        self.write_file(
            FileType::Metadata,
            &Metadata {
                current_transaction_id: transaction_id,
            },
        )?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn read_file<T: DeserializeOwned + Default>(&self, file_path: FileType) -> StorageResult<T> {
        let result = self
            .storage
            .lock()
            .unwrap()
            .read_blob(file_path.as_str().to_string());

        match result {
            Ok(ReadBlobState::Found(file_contents)) => {
                let data: T = serde_json::from_slice(&file_contents).unwrap();
                return Ok(data);
            }
            Ok(ReadBlobState::NotFound) => {
                return Ok(T::default());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    #[tracing::instrument(skip(self, data))]
    fn write_file<T: Serialize>(&self, file_path: FileType, data: T) -> StorageResult<()> {
        let serialized_data = serde_json::to_string::<T>(&data).unwrap();

        let serialized_bytes = serialized_data.as_str().as_bytes();

        self.storage
            .lock()
            .unwrap()
            .write_blob(file_path.as_str().to_string(), serialized_bytes.to_vec())
    }
}
