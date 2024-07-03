use std::{path::PathBuf, sync::Arc, thread};

use aws_sdk_s3::{primitives::ByteStream, Client};
use chrono::Utc;
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, Sender},
};

use super::Storage;

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
    sender: oneshot::Sender<Vec<u8>>,
}

struct TransactionWriteRequest {
    bytes: Vec<u8>,
    sender: oneshot::Sender<()>,
}

pub enum NetworkStorageAction {
    WriteBlob(WriteFileRequest),
    ReadBlob(ReadFileRequest),
    Reset(ResetFileRequest),
    TransactionWrite(TransactionWriteRequest),
    TransactionFlush(oneshot::Sender<()>),
    TransactionLoad(oneshot::Sender<String>),
}

const TRANSACTION_LOG_PATH: &str = "transaction_log";

struct S3NetworkStorage {
    network_storage: NetworkStorage,
}

impl S3NetworkStorage {
    pub fn new(bucket: String, base_path: PathBuf) -> Self {
        let (action_sender, mut s3_action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        let _ = thread::Builder::new()
            .name("AWS SDK Tokio".to_string())
            .spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();

                rt.block_on(async move {
                    let client = Arc::new(Client::new(&aws_config::load_from_env().await));

                    while let Some(request) = s3_action_receiver.recv().await {
                        tokio::spawn(handle_task(
                            bucket.clone(),
                            base_path.clone(),
                            client.clone(),
                            request,
                        ));
                    }
                });
            });

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

// Is there a way to avoid this duplication?
impl Storage for S3NetworkStorage {
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

/// --- Base Network Struct

struct NetworkStorage {
    action_sender: Sender<NetworkStorageAction>,
}

impl Storage for NetworkStorage {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        let write_file_request = NetworkStorageAction::WriteBlob(WriteFileRequest {
            file_path: path,
            bytes: bytes,
            sender: sender,
        });

        self.action_sender
            .blocking_send(write_file_request)
            .unwrap();

        let _ = receiver.recv();

        return ();
    }

    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        let (sender, receiver) = oneshot::channel::<Vec<u8>>();

        // Is the problem that this is happening within the main thread?
        self.action_sender
            .blocking_send(NetworkStorageAction::ReadBlob(ReadFileRequest {
                file_path: path,
                sender: sender,
            }))
            .unwrap();

        let response = receiver.recv().unwrap();

        return Ok(response);
    }

    fn init(&self) {
        // This method is not needed, s3 does not have folders
    }

    fn reset_database(&self) {
        let (sender, receiver) = oneshot::channel::<()>();

        self.action_sender
            .blocking_send(NetworkStorageAction::Reset(ResetFileRequest {
                sender: sender,
            }))
            .unwrap();

        let _ = receiver.recv();
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        // TODO: Externalize transaction log
        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionWrite(
                TransactionWriteRequest {
                    bytes: transaction.to_vec(),
                    sender: sender,
                },
            ))
            .unwrap();

        let _ = receiver.recv();
    }

    fn transaction_load(&mut self) -> String {
        let (sender, receiver) = oneshot::channel::<String>();

        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionLoad(sender))
            .unwrap();

        receiver.recv().unwrap()
    }

    fn transaction_flush(&mut self) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionFlush(sender))
            .unwrap();

        receiver.recv().unwrap()
    }

    fn transaction_sync(&self) -> () {
        // For s3 we do not need a disk sync
    }
}

async fn handle_task(
    bucket: String,
    base_path: PathBuf,
    client: Arc<Client>,
    s3_action: NetworkStorageAction,
) {
    let bucket_str = &bucket;

    match s3_action {
        NetworkStorageAction::Reset(r) => {
            delete_files_at_path(&client, bucket_str, base_path).await;

            let _ = r.sender.send(()).unwrap();
        }
        NetworkStorageAction::WriteBlob(file_request) => {
            // TODO: Should we normalize the path before getting to this point? Will make system more dry
            let file_path = base_path.join(file_request.file_path);

            let req = client
                .put_object()
                .bucket(bucket_str)
                .key(file_path.to_str().unwrap())
                .body(ByteStream::from(file_request.bytes));

            let _ = req.send().await.unwrap();

            let _ = file_request.sender.send(()).unwrap();
        }
        NetworkStorageAction::ReadBlob(file_request) => {
            let file_path = base_path.join(file_request.file_path);

            let response = client
                .get_object()
                .bucket(bucket)
                .key(file_path.to_str().unwrap())
                .send()
                .await
                .unwrap();

            let bytes = response.body.collect().await.unwrap().into_bytes().to_vec();

            let _ = file_request.sender.send(bytes).unwrap();
        }
        NetworkStorageAction::TransactionWrite(request) => {
            let file_path = base_path
                .join(TRANSACTION_LOG_PATH)
                .join(Utc::now().to_rfc3339());

            let req = client
                .put_object()
                .bucket(bucket)
                .key(file_path.to_str().unwrap())
                .body(ByteStream::from(request.bytes));

            let _ = req.send().await.unwrap();

            let _ = request.sender.send(()).unwrap();
        }
        NetworkStorageAction::TransactionFlush(r) => {
            let transactions_folder = base_path.join(TRANSACTION_LOG_PATH);

            delete_files_at_path(&client, bucket_str, transactions_folder).await;

            let _ = r.send(()).unwrap();
        }
        NetworkStorageAction::TransactionLoad(request) => {
            let transactions_folder = base_path.join(TRANSACTION_LOG_PATH);

            let contents =
                get_file_contents_at_path(&client, bucket_str, transactions_folder).await;

            let _ = request.send(contents).unwrap();
        }
    }
}

async fn delete_files_at_path(client: &Client, bucket: &str, path: PathBuf) {
    let mut response = client
        .list_objects_v2()
        .prefix(path.to_str().unwrap())
        .bucket(bucket)
        .max_keys(10)
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    client
                        .delete_object()
                        .bucket(bucket)
                        .key(object.key().unwrap())
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

async fn get_file_contents_at_path(client: &Client, bucket: &str, path: PathBuf) -> String {
    let mut response = client
        .list_objects_v2()
        .prefix(path.to_str().unwrap())
        .bucket(bucket)
        .max_keys(10)
        .into_paginator()
        .send();

    let mut contents: String = String::new();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    let result = client
                        .get_object()
                        .bucket(bucket)
                        .key(object.key().unwrap())
                        .send()
                        .await
                        .unwrap();

                    let result_bytes = result.body.collect().await.unwrap().into_bytes();

                    contents.push_str(std::str::from_utf8(&result_bytes).unwrap());
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    contents
}
