use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use aws_sdk_s3::{primitives::ByteStream, Client};
use chrono::Utc;
use tokio::sync::mpsc::{self};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    Storage,
};

const TRANSACTION_LOG_PATH: &str = "transaction_log";

pub struct S3Storage {
    network_storage: NetworkStorage,
}

impl S3Storage {
    pub fn new(bucket: String, base_path: PathBuf) -> Self {
        let (action_sender, action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        let data = S3Env { bucket, base_path };

        start_runtime(action_receiver, data, task_fn, client_fn);

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

#[derive(Clone)]
struct S3Env {
    bucket: String,
    base_path: PathBuf,
}

fn client_fn() -> Pin<Box<dyn Future<Output = Client> + Send + 'static>> {
    Box::pin(async {
        let sdk = aws_config::load_from_env().await;

        Client::new(&sdk)
    })
}

// Is there a way to avoid this duplication?
impl Storage for S3Storage {
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
    data: S3Env,
    client: Arc<Client>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        let bucket = &data.bucket;
        let base_path = data.base_path;

        match action {
            NetworkStorageAction::Reset(r) => {
                delete_files_at_path(&client, &bucket, base_path).await;

                let _ = r.sender.send(()).unwrap();
            }
            NetworkStorageAction::WriteBlob(file_request) => {
                // TODO: Should we normalize the path before getting to this point? Will make system more dry
                let file_path = base_path.join(file_request.file_path);

                let req = client
                    .put_object()
                    .bucket(bucket)
                    .key(file_path.to_str().unwrap())
                    .body(ByteStream::from(file_request.bytes));

                let _ = req.send().await.unwrap();

                let _ = file_request.sender.send(()).unwrap();
            }
            NetworkStorageAction::ReadBlob(file_request) => {
                let file_path = base_path.join(file_request.file_path);

                let request = client
                    .get_object()
                    .bucket(bucket)
                    .key(file_path.to_str().unwrap())
                    .send()
                    .await;

                let response = if let Ok(o) = request {
                    Ok(o.body.collect().await.unwrap().into_bytes().to_vec())
                } else {
                    Err(())
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

                panic!("AWS SSO Error");

                // Problems:
                // 1. file transaction write panics if there is an issue, network does not
                // 2. there is no result type from the persistence layer
                // 3. DatabaseCommandResponse -> DatabaseCommandTransactionResponse -> Rollback is the only type of error
                //  which is likely okay, because we do not apply if we timeout
                // 4. Is there even a way to roll back if we fail to write to the WAL? -- looks like the response
                //  is only a success... Would then need to hold onto the transaction until we can write it (no longer an async pipeline.
                //  may need to look at the paper for the various phases of the TX. It is kind of bad that we have applied
                //  the transaction to the database, but not to the WAL. We should probably hold onto the transaction until we can write it.
                //
                // Solutions:
                // 1. Implement an error class / result (1/2/3)
                // 2. Solve the transaction phase issues (4) -- 4 is unlikely, though increases
                //  in the case we are using network storage. In a way a panic / restart here is okay
                //  and because the transaction was not durably written to the WAL, a restart would be a stop-gap.

                // Why do we past the tests if we fail to write the transaction?
                let _ = req.send().await.unwrap();

                let _ = request.sender.send(()).unwrap();
            }
            NetworkStorageAction::TransactionFlush(r) => {
                let transactions_folder = base_path.join(TRANSACTION_LOG_PATH);

                delete_files_at_path(&client, &bucket, transactions_folder).await;

                let _ = r.send(()).unwrap();
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
