use std::{
    path::PathBuf,
    sync::mpsc::{self, Sender},
};

use aws_sdk_s3::{primitives::ByteStream, Client};
use tokio::runtime::Builder;

use super::Persistence;

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

enum S3Action {
    WriteBlob(WriteFileRequest),
    ReadBlob(ReadFileRequest),
    Reset(ResetFileRequest),
}

// TODO:
//  - Add error handling (_ + unwrap)
//  - Does not want a base path, maybe we convert it to a string before passing it into handle task
//  - One shot the response
async fn handle_task(base_path: PathBuf, s3_action: S3Action) {
    let shared_config = aws_config::load_from_env().await;

    let client = Client::new(&shared_config);

    // TODO: Make bucket configurable
    let bucket = "dalesalter-test-bucket";

    match s3_action {
        S3Action::Reset(r) => {
            // Delete all files within a particular folder / prefix (data/)
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

            let _ = r.sender.send(()).unwrap();
        }
        S3Action::WriteBlob(file_request) => {
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
        S3Action::ReadBlob(file_request) => {
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
    }
}

pub struct S3Persistence {
    sender: Sender<S3Action>,
}

impl S3Persistence {
    pub fn new(base_path: PathBuf) -> Self {
        let (send, recv) = mpsc::channel::<S3Action>();

        std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            rt.block_on(async move {
                loop {
                    let request = recv.recv().unwrap();

                    tokio::spawn(handle_task(base_path.clone(), request));
                }

                // while let Some(request) = recv.recv() {
                //     tokio::spawn(handle_task(base_path.clone(), request));
                // }
            });
        });

        Self { sender: send }
    }
}

impl Persistence for S3Persistence {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> () {
        let (sender, receiver) = oneshot::channel::<()>();

        let write_file_request = S3Action::WriteBlob(WriteFileRequest {
            file_path: path,
            bytes: bytes,
            sender: sender,
        });

        self.sender.send(write_file_request).unwrap();

        let _ = receiver.recv();

        return ();
    }

    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()> {
        let (sender, receiver) = oneshot::channel::<Vec<u8>>();

        // Cannot block the current thread from within a runtime. This happens because a function
        //  attempted to block the current thread while the thread is being used to drive asynchronous tasks.
        self.sender
            .send(S3Action::ReadBlob(ReadFileRequest {
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

    fn reset(&self) {
        let (sender, receiver) = oneshot::channel::<()>();

        self.sender
            .send(S3Action::Reset(ResetFileRequest { sender: sender }))
            .unwrap();

        let _ = receiver.recv();
    }
}
