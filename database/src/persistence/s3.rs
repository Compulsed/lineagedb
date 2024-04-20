use std::path::PathBuf;

use aws_sdk_s3::{primitives::ByteStream, Client};
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, Sender},
};

use super::Persistence;

pub struct S3Persistence {
    base_path: PathBuf,
    sender: Sender<Vec<u8>>,
}

async fn handle_task(task: Vec<u8>) {
    let shared_config = aws_config::load_from_env().await;

    let client = Client::new(&shared_config);

    let req = client
        .put_object()
        .bucket("dalesalter_test_bucket")
        .key("test.json")
        .body(ByteStream::from(task));

    let _ = req.send().await.unwrap();
}

impl S3Persistence {
    pub fn new(base_path: PathBuf) -> Self {
        let (send, mut recv) = mpsc::channel::<Vec<u8>>(16);

        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        std::thread::spawn(move || {
            rt.block_on(async move {
                while let Some(task) = recv.recv().await {
                    tokio::spawn(handle_task(task));
                }

                // Once all senders have gone out of scope,
                // the `.recv()` call returns None and it will
                // exit from the while loop and shut down the
                // thread.
            });
        });

        Self {
            sender: send,
            base_path,
        }
    }

    fn get_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path)
    }
}

impl Persistence for S3Persistence {
    fn write_blob(&self, path: &str, bytes: &[u8]) -> () {
        self.sender.blocking_send(bytes.to_vec()).unwrap();
        ()
    }

    fn read_blob(&self, path: &str) -> Result<Vec<u8>, ()> {
        Err(())
    }

    fn init(&self) {}

    fn reset(&self) {}
}
