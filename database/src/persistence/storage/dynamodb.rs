use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::anyhow;
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, TableStatus,
    },
    Client, Error,
};
use chrono::Utc;
use tokio::sync::mpsc::{self};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    ReadBlobState, Storage, StorageError, StorageResult,
};

const HASH_KEY: &str = "Hash";
const SORT_KEY: &str = "Sort";
const BLOB_PARTITION: &str = "Blob";
const DATA_KEY: &str = "Data";

const TRANSACTION_LOG_PATH: &str = "transaction_log";

/// Limitations / issues:
/// 1. World state is limited to 400kb (unless we split)
/// 2. Unsure if we can write an item w/ just a PK
pub struct DynamoDBStorage {
    network_storage: NetworkStorage,
}

impl DynamoDBStorage {
    pub fn new(options: DynamoOptions) -> Self {
        let (action_sender, action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        start_runtime(action_receiver, options, task_fn, client_fn);

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct DynamoOptions {
    pub table: String,
    base_path: PathBuf,
}

impl DynamoOptions {
    pub fn new(table: String) -> Self {
        Self {
            base_path: PathBuf::from("data"),
            table,
        }
    }

    pub fn new_test() -> Self {
        Self {
            base_path: PathBuf::from("data"),
            table: "lineagedb-ddb".to_string(),
        }
    }
}

fn client_fn(_options: DynamoOptions) -> Pin<Box<dyn Future<Output = Client> + Send + 'static>> {
    Box::pin(async {
        let sdk = aws_config::load_from_env().await;

        Client::new(&sdk)
    })
}

// Is there a way to avoid this duplication?
impl Storage for DynamoDBStorage {
    fn init(&mut self) -> StorageResult<()> {
        self.network_storage.init()
    }

    fn reset_database(&mut self) -> StorageResult<()> {
        self.network_storage.reset_database()
    }

    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()> {
        self.network_storage.write_blob(path, bytes)
    }

    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState> {
        self.network_storage.read_blob(path)
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()> {
        self.network_storage.transaction_write(transaction)
    }

    fn transaction_sync(&self) -> StorageResult<()> {
        self.network_storage.transaction_sync()
    }

    fn transaction_flush(&mut self) -> StorageResult<()> {
        self.network_storage.transaction_flush()
    }

    fn transaction_load(&mut self) -> StorageResult<Vec<String>> {
        self.network_storage.transaction_load()
    }
}

fn task_fn(
    data: DynamoOptions,
    client: Arc<Client>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        let table_str = &data.table;
        let base_path = &data.base_path;

        match action {
            NetworkStorageAction::Init(r) => {
                let attribute_definitions_pk = AttributeDefinition::builder()
                    .attribute_name(HASH_KEY)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("Should not error value is statically defined in code");

                let attribute_definitions_sk = AttributeDefinition::builder()
                    .attribute_name(SORT_KEY)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("Should not error value is statically defined in code");

                let key_schema_pk = KeySchemaElement::builder()
                    .attribute_name(HASH_KEY)
                    .key_type(KeyType::Hash)
                    .build()
                    .expect("Should not error value is statically defined in code");

                let key_schema_sk = KeySchemaElement::builder()
                    .attribute_name(SORT_KEY)
                    .key_type(KeyType::Range)
                    .build()
                    .expect("Should not error value is statically defined in code");

                let result = client
                    .create_table()
                    .table_name(table_str)
                    .billing_mode(BillingMode::PayPerRequest)
                    .attribute_definitions(attribute_definitions_pk)
                    .attribute_definitions(attribute_definitions_sk)
                    .key_schema(key_schema_pk)
                    .key_schema(key_schema_sk)
                    .send()
                    .await;

                let mut response = match result {
                    Ok(_) => Ok(()),
                    Err(e) => match Error::from(e) {
                        aws_sdk_dynamodb::Error::TableAlreadyExistsException(_) => Ok(()),
                        aws_sdk_dynamodb::Error::ResourceInUseException(_) => Ok(()),
                        e => Err(StorageError::UnableToInitializePersistence(anyhow!(e))),
                    },
                };

                if let Ok(()) = response {
                    response = wait_to_completion(table_str, &client).await;
                }

                let _ = r.send(response).unwrap();
            }
            NetworkStorageAction::Reset(r) => {
                let result = reset_table(&client, table_str).await;

                let _ = r.sender.send(result).unwrap();
            }
            NetworkStorageAction::WriteBlob(file_request) => {
                let file_path = base_path.join(file_request.file_path);

                let req = client
                    .put_item()
                    .table_name(table_str)
                    .item(HASH_KEY, AttributeValue::S(BLOB_PARTITION.to_string()))
                    .item(
                        SORT_KEY,
                        AttributeValue::S(file_path.to_str().unwrap().to_string()),
                    )
                    .item(
                        DATA_KEY,
                        AttributeValue::S(String::from_utf8(file_request.bytes).unwrap()),
                    );

                let result = req
                    .send()
                    .await
                    .map(|_| {})
                    .map_err(|e| StorageError::UnableToWriteBlob(anyhow!(e)));

                let _ = file_request.sender.send(result).unwrap();
            }
            NetworkStorageAction::ReadBlob(file_request) => {
                let file_path = base_path.join(file_request.file_path);

                let request = client
                    .get_item()
                    .table_name(table_str)
                    .key(HASH_KEY, AttributeValue::S(BLOB_PARTITION.to_string()))
                    .key(
                        SORT_KEY,
                        AttributeValue::S(file_path.to_str().unwrap().to_string()),
                    )
                    .send()
                    .await;

                let response = match request {
                    Ok(output) => match output.item {
                        Some(item) => Ok(ReadBlobState::Found(
                            item.get(DATA_KEY)
                                .unwrap()
                                .as_s()
                                .unwrap()
                                .bytes()
                                .collect::<Vec<u8>>(),
                        )),
                        None => Ok(ReadBlobState::NotFound),
                    },
                    Err(e) => Err(StorageError::UnableToReadBlob(anyhow!(e))),
                };

                let _ = file_request.sender.send(response).unwrap();
            }
            NetworkStorageAction::TransactionWrite(request) => {
                let req = client
                    .put_item()
                    .table_name(table_str)
                    .item(
                        HASH_KEY,
                        AttributeValue::S(TRANSACTION_LOG_PATH.to_string()),
                    )
                    .item(SORT_KEY, AttributeValue::S(Utc::now().to_rfc3339()))
                    .item(
                        DATA_KEY,
                        AttributeValue::S(String::from_utf8(request.bytes).unwrap()),
                    );

                let response = req
                    .send()
                    .await
                    .map(|_| {})
                    .map_err(|e| StorageError::UnableToWriteTransaction(anyhow!(e)));

                let _ = request.sender.send(response).unwrap();
            }
            NetworkStorageAction::TransactionFlush(r) => {
                let response =
                    delete_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH)
                        .await;

                let _ = r.send(response).unwrap();
            }
            NetworkStorageAction::TransactionLoad(request) => {
                let contents =
                    get_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH).await;

                let _ = request.send(contents).unwrap();
            }
        }
    })
}

async fn reset_table(client: &Client, table_name: &str) -> StorageResult<()> {
    let mut response = client.scan().table_name(table_name).into_paginator().send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for item in output.items() {
                    client
                        .delete_item()
                        .table_name(table_name)
                        .key(HASH_KEY.to_string(), item.get(HASH_KEY).unwrap().clone())
                        .key(SORT_KEY.to_string(), item.get(SORT_KEY).unwrap().clone())
                        .send()
                        .await
                        .map_err(|e| StorageError::UnableToDeleteTransactionLog(anyhow!(e)))?;
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(())
}

async fn get_transactions_at_partition(
    client: &Client,
    table: &str,
    partition: &str,
) -> StorageResult<Vec<String>> {
    let mut response = client
        .query()
        .table_name(table)
        .key_condition_expression("#hash = :hash")
        .expression_attribute_names("#hash", HASH_KEY)
        .expression_attribute_values(":hash", AttributeValue::S(partition.to_string()))
        .into_paginator()
        .send();

    let mut contents: Vec<String> = Vec::new();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for item in output.items() {
                    // Assumes transaction per DynamoDB item
                    let data = item.get(DATA_KEY).unwrap().as_s().unwrap();

                    contents.push(data.to_string());
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(contents)
}

async fn delete_transactions_at_partition(
    client: &Client,
    table: &str,
    partition: &str,
) -> StorageResult<()> {
    let mut response = client
        .query()
        .table_name(table)
        .key_condition_expression("#hash = :hash")
        .expression_attribute_names("#hash", HASH_KEY)
        .expression_attribute_values(":hash", AttributeValue::S(partition.to_string()))
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for item in output.items() {
                    client
                        .delete_item()
                        .table_name(table)
                        .key(HASH_KEY.to_string(), item.get(HASH_KEY).unwrap().clone())
                        .key(SORT_KEY.to_string(), item.get(SORT_KEY).unwrap().clone())
                        .send()
                        .await
                        .map_err(|e| StorageError::UnableToDeleteTransactionLog(anyhow!(e)))?;
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(())
}

pub async fn ddb_table_status(
    table_name: &str,
    client: &Client,
) -> Result<TableStatus, StorageError> {
    client
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .map_err(|e| StorageError::UnableToInitializePersistence(anyhow!(e)))?
        .table
        .ok_or_else(|| StorageError::UnableToInitializePersistence(anyhow!("Table not found")))?
        .table_status
        .ok_or_else(|| {
            StorageError::UnableToInitializePersistence(anyhow!("Table status not found"))
        })
}

/// Waits for the DynamoDB table to be active, typically takes ~7 seconds
pub async fn wait_to_completion(table_name: &str, client: &Client) -> Result<(), StorageError> {
    loop {
        let status = ddb_table_status(table_name, client).await?;

        match &status {
            TableStatus::Active => return Ok(()),
            TableStatus::Creating | TableStatus::Updating => {}
            _ => {
                return Err(StorageError::UnableToInitializePersistence(anyhow!(
                    "Unexpected table status: {:?}",
                    status
                )));
            }
        };

        log::info!(
            "Waiting for table to be active current status: {:?}",
            status
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
