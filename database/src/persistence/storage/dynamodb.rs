use std::{path::PathBuf, sync::Arc, thread};

use aws_sdk_dynamodb::{types::AttributeValue, Client};
use chrono::Utc;
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, Sender},
};

use super::Storage;

const HASH_KEY: &str = "Hash";
const SORT_KEY: &str = "Sort";
const BLOB_PARTITION: &str = "Blob";
const DATA_KEY: &str = "Data";

struct WriteFileRequest {
    bytes: Vec<u8>,
    file_path: String,
    sender: oneshot::Sender<()>,
}

struct ResetFileRequest {
    sender: oneshot::Sender<()>,
}

struct ReadFileRequest {
    file_path: String,
    sender: oneshot::Sender<Result<Vec<u8>, ()>>,
}

struct TransactionWriteRequest {
    bytes: Vec<u8>,
    sender: oneshot::Sender<()>,
}

enum DynamoDBAction {
    WriteBlob(WriteFileRequest),
    ReadBlob(ReadFileRequest),
    Reset(ResetFileRequest),
    TransactionWrite(TransactionWriteRequest),
    TransactionFlush(oneshot::Sender<()>),
    TransactionLoad(oneshot::Sender<String>),
}

const TRANSACTION_LOG_PATH: &str = "transaction_log";

/// Limitations / issues:
/// 1. World state is limited to 400kb (unless we split)
/// 2. Unsure if we can write an item w/ just a PK
pub struct DynamoDBStorage {
    ddb_action_sender: Sender<DynamoDBAction>,
}

impl DynamoDBStorage {
    pub fn new(table: String, base_path: PathBuf) -> Self {
        let (dynamo_action_sender, mut dynamo_action_receiver) =
            mpsc::channel::<DynamoDBAction>(16);

        let _ = thread::Builder::new()
            .name("AWS SDK Tokio".to_string())
            .spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();

                rt.block_on(async move {
                    let client = Arc::new(Client::new(&aws_config::load_from_env().await));

                    while let Some(request) = dynamo_action_receiver.recv().await {
                        tokio::spawn(handle_task(
                            table.clone(),
                            base_path.clone(),
                            client.clone(),
                            request,
                        ));
                    }
                });
            });

        Self {
            ddb_action_sender: dynamo_action_sender,
        }
    }
}

impl Storage for DynamoDBStorage {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        let write_file_request = DynamoDBAction::WriteBlob(WriteFileRequest {
            file_path: path,
            bytes: bytes,
            sender: sender,
        });

        self.ddb_action_sender
            .blocking_send(write_file_request)
            .unwrap();

        let _ = receiver.recv();

        return ();
    }

    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        let (sender, receiver) = oneshot::channel::<Result<Vec<u8>, ()>>();

        // Is the problem that this is happening within the main thread?
        self.ddb_action_sender
            .blocking_send(DynamoDBAction::ReadBlob(ReadFileRequest {
                file_path: path,
                sender: sender,
            }))
            .unwrap();

        receiver.recv().unwrap()
    }

    fn init(&self) {
        // This method is not needed, ddb does not have folders
    }

    fn reset_database(&self) {
        let (sender, receiver) = oneshot::channel::<()>();

        self.ddb_action_sender
            .blocking_send(DynamoDBAction::Reset(ResetFileRequest { sender: sender }))
            .unwrap();

        let _ = receiver.recv();
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        // TODO: Externalize transaction log
        self.ddb_action_sender
            .blocking_send(DynamoDBAction::TransactionWrite(TransactionWriteRequest {
                bytes: transaction.to_vec(),
                sender: sender,
            }))
            .unwrap();

        let _ = receiver.recv();
    }

    fn transaction_load(&mut self) -> String {
        let (sender, receiver) = oneshot::channel::<String>();

        self.ddb_action_sender
            .blocking_send(DynamoDBAction::TransactionLoad(sender))
            .unwrap();

        receiver.recv().unwrap()
    }

    fn transaction_flush(&mut self) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        self.ddb_action_sender
            .blocking_send(DynamoDBAction::TransactionFlush(sender))
            .unwrap();

        receiver.recv().unwrap()
    }

    fn transaction_sync(&self) -> () {
        // For ddb we do not need a disk sync
    }
}

async fn handle_task(
    bucket: String,
    base_path: PathBuf,
    client: Arc<Client>,
    ddb_action: DynamoDBAction,
) {
    let table_str = &bucket;

    match ddb_action {
        DynamoDBAction::Reset(r) => {
            reset_table(&client, table_str).await;

            let _ = r.sender.send(()).unwrap();
        }
        DynamoDBAction::WriteBlob(file_request) => {
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
        DynamoDBAction::ReadBlob(file_request) => {
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
        DynamoDBAction::TransactionWrite(request) => {
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
        DynamoDBAction::TransactionFlush(r) => {
            delete_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH).await;

            let _ = r.send(()).unwrap();
        }
        DynamoDBAction::TransactionLoad(request) => {
            let contents =
                get_transactions_at_partition(&client, table_str, TRANSACTION_LOG_PATH).await;

            let _ = request.send(contents).unwrap();
        }
    }
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
