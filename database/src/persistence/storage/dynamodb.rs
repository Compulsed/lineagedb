use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use chrono::Utc;
use tokio::sync::mpsc::{self};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    Storage,
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
    pub fn new(table: String, base_path: PathBuf) -> Self {
        let (action_sender, action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        let data: DynamoDBEnv = DynamoDBEnv {
            table: table,
            base_path: base_path,
        };

        start_runtime(action_receiver, data, task_fn, client_fn);

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

#[derive(Clone)]
struct DynamoDBEnv {
    table: String,
    base_path: PathBuf,
}

fn client_fn() -> Pin<Box<dyn Future<Output = Client> + Send + 'static>> {
    Box::pin(async {
        let sdk = aws_config::load_from_env().await;

        Client::new(&sdk)
    })
}

// Is there a way to avoid this duplication?
impl Storage for DynamoDBStorage {
    fn init(&self) {
        self.network_storage.init();
    }

    fn reset_database(&self) {
        self.network_storage.reset_database();
    }

    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        self.network_storage.write_blob(path, bytes);
    }

    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        self.network_storage.read_blob(path)
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> () {
        self.network_storage.transaction_write(transaction);
    }

    fn transaction_sync(&self) -> () {
        self.network_storage.transaction_sync();
    }

    fn transaction_flush(&mut self) -> () {
        self.network_storage.transaction_flush();
    }

    fn transaction_load(&mut self) -> String {
        self.network_storage.transaction_load()
    }
}

fn task_fn(
    data: DynamoDBEnv,
    client: Arc<Client>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        let table_str = &data.table;
        let base_path = &data.base_path;

        match action {
            NetworkStorageAction::Reset(r) => {
                reset_table(&client, table_str).await;

                let _ = r.sender.send(()).unwrap();
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

                let _ = req.send().await.unwrap();

                let _ = file_request.sender.send(()).unwrap();
            }
            NetworkStorageAction::ReadBlob(file_request) => {
                let file_path = base_path.join(file_request.file_path);

                let response = client
                    .get_item()
                    .table_name(table_str)
                    .key(HASH_KEY, AttributeValue::S(BLOB_PARTITION.to_string()))
                    .key(
                        SORT_KEY,
                        AttributeValue::S(file_path.to_str().unwrap().to_string()),
                    );

                let response = if let Some(item) = response.send().await.unwrap().item {
                    Ok(item
                        .get(DATA_KEY)
                        .unwrap()
                        .as_s()
                        .unwrap()
                        .bytes()
                        .collect::<Vec<u8>>())
                } else {
                    Err(())
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

                let _ = req.send().await.unwrap();

                let _ = request.sender.send(()).unwrap();
            }
            NetworkStorageAction::TransactionFlush(r) => {
                delete_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH).await;

                let _ = r.send(()).unwrap();
            }
            NetworkStorageAction::TransactionLoad(request) => {
                let contents =
                    get_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH).await;

                let _ = request.send(contents).unwrap();
            }
        }
    })
}

async fn reset_table(client: &Client, table_name: &str) {
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
                        .unwrap();
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }
}

async fn get_transactions_at_partition(client: &Client, table: &str, partition: &str) -> String {
    let mut response = client
        .query()
        .table_name(table)
        .key_condition_expression("#hash = :hash")
        .expression_attribute_names("#hash", HASH_KEY)
        .expression_attribute_values(":hash", AttributeValue::S(partition.to_string()))
        .into_paginator()
        .send();

    let mut contents: String = String::new();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for item in output.items() {
                    // Assumes transaction per DynamoDB item
                    let data = item.get(DATA_KEY).unwrap().as_s().unwrap();

                    contents.push_str(data);
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    contents
}

async fn delete_transactions_at_partition(client: &Client, table: &str, partition: &str) {
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
                        .unwrap();
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }
}
