use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use anyhow::anyhow;
use aws_sdk_s3::{primitives::ByteStream, Client, Error as S3Error};
use chrono::Utc;
use tokio::sync::mpsc::{self};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    ReadBlobState, Storage, StorageError, StorageResult,
};

const TRANSACTION_LOG_PATH: &str = "transaction_log";

pub struct S3Storage {
    network_storage: NetworkStorage,
}

impl S3Storage {
    pub fn new(options: S3Options) -> Self {
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
pub struct S3Options {
    bucket: String,
    base_path: PathBuf,
}

impl S3Options {
    pub fn new(bucket: String) -> Self {
        Self {
            base_path: PathBuf::from("data"),
            bucket,
        }
    }

    pub fn new_local() -> Self {
        Self {
            base_path: PathBuf::from("data"),
            bucket: "dalesalter-test-bucket".to_string(),
        }
    }
}

fn client_fn(_: S3Options) -> Pin<Box<dyn Future<Output = Client> + Send + 'static>> {
    Box::pin(async {
        let sdk = aws_config::load_from_env().await;

        Client::new(&sdk)
    })
}

// Is there a way to avoid this duplication?
impl Storage for S3Storage {
    fn init(&self) -> StorageResult<()> {
        self.network_storage.init()
    }

    fn reset_database(&self) -> StorageResult<()> {
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

// TODO: How do we handle the async nature of aws sso? Looks like it may only resolve on the first SDK request
// TODO: Understand how to intercept errors / the paginators (there appears to be no way to intercept)
// TODO: Should we surface / handle other SDK error types? E.g. Timeout, Dispatch, Response, Service, etc.
//  perhaps there is a way to configure the SDK to make these errors less likely
fn task_fn(
    data: S3Options,
    client: Arc<Client>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        let bucket = &data.bucket;
        let base_path = data.base_path;

        match action {
            NetworkStorageAction::Init(r) => {
                let result = client.create_bucket().bucket(bucket).send().await;

                let response = match result {
                    Ok(_) => Ok(()),
                    Err(e) => match S3Error::from(e) {
                        S3Error::BucketAlreadyExists(_) => Ok(()),
                        e => Err(StorageError::UnableToInitializePersistence(anyhow!(e))),
                    },
                };

                let _ = r.send(response).unwrap();
            }
            NetworkStorageAction::Reset(r) => {
                let result = delete_files_at_path(&client, &bucket, base_path).await;

                let _ = r.sender.send(result).unwrap();
            }
            NetworkStorageAction::WriteBlob(file_request) => {
                // TODO: Should we normalize the path before getting to this point? Will make system more dry
                let file_path = base_path.join(file_request.file_path);

                let req = client
                    .put_object()
                    .bucket(bucket)
                    .key(file_path.to_str().unwrap())
                    .body(ByteStream::from(file_request.bytes));

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
                    .get_object()
                    .bucket(bucket)
                    .key(file_path.to_str().unwrap())
                    .send()
                    .await;

                let response = match request {
                    Ok(o) => {
                        let response = o.body.collect().await.unwrap().into_bytes().to_vec();

                        Ok(ReadBlobState::Found(response))
                    }
                    Err(e) => match S3Error::from(e) {
                        S3Error::NoSuchKey(_) => Ok(ReadBlobState::NotFound),
                        e => Err(StorageError::UnableToReadBlob(anyhow!(e))),
                    },
                };

                let _ = file_request.sender.send(response).unwrap();
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

                // Why do we past the tests if we fail to write the transaction?
                let result = req
                    .send()
                    .await
                    .map(|_| {})
                    .map_err(|e| StorageError::UnableToWriteTransaction(anyhow!(e)));

                let _ = request.sender.send(result).unwrap();
            }
            NetworkStorageAction::TransactionFlush(r) => {
                let transactions_folder = base_path.join(TRANSACTION_LOG_PATH);

                let result = delete_files_at_path(&client, &bucket, transactions_folder).await;

                let _ = r.send(result).unwrap();
            }
            NetworkStorageAction::TransactionLoad(request) => {
                let transactions_folder = base_path.join(TRANSACTION_LOG_PATH);

                let contents =
                    get_file_contents_at_path(&client, &bucket, transactions_folder).await;

                let _ = request.send(contents).unwrap();
            }
        }
    })
}

async fn delete_files_at_path(client: &Client, bucket: &str, path: PathBuf) -> StorageResult<()> {
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

async fn get_file_contents_at_path(
    client: &Client,
    bucket: &str,
    path: PathBuf,
) -> StorageResult<Vec<String>> {
    let mut response = client
        .list_objects_v2()
        .prefix(path.to_str().unwrap())
        .bucket(bucket)
        .max_keys(10)
        .into_paginator()
        .send();

    let mut contents: Vec<String> = Vec::new();

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
                        .map_err(|e| StorageError::UnableToLoadPreviousTransactions(anyhow!(e)))?;

                    let result_bytes = result.body.collect().await.unwrap().into_bytes();

                    contents.push(std::str::from_utf8(&result_bytes).unwrap().to_string());
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(contents)
}
