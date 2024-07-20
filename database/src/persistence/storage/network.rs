use std::{future::Future, pin::Pin, sync::Arc, thread};

use tokio::{
    runtime::Builder,
    sync::mpsc::{Receiver, Sender},
};

use super::{ReadBlobState, Storage, StorageResult};

pub struct WriteFileRequest {
    pub bytes: Vec<u8>,
    pub file_path: String,
    pub sender: oneshot::Sender<StorageResult<()>>,
}

pub struct ResetFileRequest {
    pub sender: oneshot::Sender<StorageResult<()>>,
}

pub struct ReadFileRequest {
    pub file_path: String,
    pub sender: oneshot::Sender<StorageResult<ReadBlobState>>,
}

pub struct TransactionWriteRequest {
    pub bytes: Vec<u8>,
    pub sender: oneshot::Sender<StorageResult<()>>,
}

pub enum NetworkStorageAction {
    Init(oneshot::Sender<StorageResult<()>>),
    WriteBlob(WriteFileRequest),
    ReadBlob(ReadFileRequest),
    Reset(ResetFileRequest),
    TransactionWrite(TransactionWriteRequest),
    TransactionFlush(oneshot::Sender<StorageResult<()>>),
    TransactionLoad(oneshot::Sender<StorageResult<Vec<String>>>),
}

pub struct NetworkStorage {
    pub action_sender: Sender<NetworkStorageAction>,
}

const RECEIVER_EXPECTED_TO_WORK: &str = "should not have issues with the receiver";

impl Storage for NetworkStorage {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()> {
        let (sender, receiver) = oneshot::channel::<StorageResult<()>>();

        let write_file_request = NetworkStorageAction::WriteBlob(WriteFileRequest {
            file_path: path,
            bytes: bytes,
            sender: sender,
        });

        self.action_sender
            .blocking_send(write_file_request)
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState> {
        let (sender, receiver) = oneshot::channel::<StorageResult<ReadBlobState>>();

        // Is the problem that this is happening within the main thread?
        self.action_sender
            .blocking_send(NetworkStorageAction::ReadBlob(ReadFileRequest {
                file_path: path,
                sender: sender,
            }))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn init(&self) -> StorageResult<()> {
        let (sender, receiver) = oneshot::channel::<StorageResult<()>>();

        self.action_sender
            .blocking_send(NetworkStorageAction::Init(sender))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn reset_database(&self) -> StorageResult<()> {
        let (sender, receiver) = oneshot::channel::<StorageResult<()>>();

        self.action_sender
            .blocking_send(NetworkStorageAction::Reset(ResetFileRequest {
                sender: sender,
            }))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()> {
        let (sender, receiver) = oneshot::channel::<StorageResult<()>>();

        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionWrite(
                TransactionWriteRequest {
                    bytes: transaction.to_vec(),
                    sender: sender,
                },
            ))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn transaction_load(&mut self) -> StorageResult<Vec<String>> {
        let (sender, receiver) = oneshot::channel::<StorageResult<Vec<String>>>();

        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionLoad(sender))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn transaction_flush(&mut self) -> StorageResult<()> {
        let (sender, receiver) = oneshot::channel::<StorageResult<()>>();

        self.action_sender
            .blocking_send(NetworkStorageAction::TransactionFlush(sender))
            .unwrap();

        receiver.recv().expect(RECEIVER_EXPECTED_TO_WORK)
    }

    fn transaction_sync(&self) -> StorageResult<()> {
        // For network we do not need a disk sync
        Ok(())
    }
}

/// Context, provided during initial set-up and is passed to both the client and task functions
/// Client function, run once and is used to pass the client to the task function
/// Task function, called for each incoming action
pub fn start_runtime<T: Clone + Send + 'static, C: Clone + Send + 'static>(
    mut action_receiver: Receiver<NetworkStorageAction>,
    context: T,
    task: fn(T, Arc<C>, NetworkStorageAction) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    client: fn(T) -> Pin<Box<dyn Future<Output = C> + Send>>,
) {
    let _ = thread::Builder::new()
        .name("AWS SDK Tokio".to_string())
        .spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            rt.block_on(async move {
                let client = Arc::new(client(context.clone()).await);

                while let Some(request) = action_receiver.recv().await {
                    tokio::spawn(task(context.clone(), client.clone(), request));
                }
            });
        });
}
