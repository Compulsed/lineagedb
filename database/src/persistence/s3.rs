use std::{fmt::Write, path::PathBuf};

use aws_sdk_s3::{primitives::ByteStream, Client};
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, Sender},
};

use super::Persistence;

struct WriteFileRequest {
    bytes: Vec<u8>,
    file_path: String,
}

pub struct S3Persistence {
    sender: Sender<S3Action>,
}

enum S3Action {
    WriteBlob(WriteFileRequest),
    ReadBlob,
    Reset,
}

// TODO:
//  - Add error handling
//  - Does not want a base path, maybe we convert it to a string before passing it into handle task
//  - One shot the response
async fn handle_task(base_path: PathBuf, s3_action: S3Action) {
    let shared_config = aws_config::load_from_env().await;

    let client = Client::new(&shared_config);

    let bucket = "dalesalter-test-bucket";

    match s3_action {
        S3Action::Reset => {
            let mut response = client
                .list_objects_v2()
                .prefix(base_path.to_str().unwrap())
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
        S3Action::WriteBlob(file_request) => {
            // TODO: Should we normalize the path before getting to this point? Will make system more dry
            let file_path = base_path.join(file_request.file_path);

            // TODO: Make bucket configurable
            let req = client
                .put_object()
                .bucket(bucket)
                .key(file_path.to_str().unwrap())
                .body(ByteStream::from(file_request.bytes));

            let _ = req.send().await.unwrap();
        }
        S3Action::ReadBlob => todo!(),
    }
}

impl S3Persistence {
    pub fn new(base_path: PathBuf) -> Self {
        let (send, mut recv) = mpsc::channel::<S3Action>(16);

        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        std::thread::spawn(move || {
            rt.block_on(async move {
                while let Some(request) = recv.recv().await {
                    tokio::spawn(handle_task(base_path.clone(), request));
                }
            });
        });

        Self { sender: send }
    }
}

impl Persistence for S3Persistence {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        let write_file_request = S3Action::WriteBlob(WriteFileRequest {
            file_path: path,
            bytes: bytes,
        });

        self.sender.blocking_send(write_file_request).unwrap();

        // TODO: Add one shot response?
        ()
    }

    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        Err(())
    }

    fn init(&self) {}

    fn reset(&self) {
        self.sender.blocking_send(S3Action::Reset).unwrap();
    }
}
