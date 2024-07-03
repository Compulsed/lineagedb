use tokio::sync::mpsc::Sender;

use super::Storage;

pub struct WriteFileRequest {
    pub bytes: Vec<u8>,
    pub file_path: String,
    pub sender: oneshot::Sender<()>,
}

pub struct ResetFileRequest {
    pub sender: oneshot::Sender<()>,
}

pub struct ReadFileRequest {
    pub file_path: String,
    pub sender: oneshot::Sender<Result<Vec<u8>, ()>>,
}

pub struct TransactionWriteRequest {
    pub bytes: Vec<u8>,
    pub sender: oneshot::Sender<()>,
}

pub enum NetworkStorageAction {
    WriteBlob(WriteFileRequest),
    ReadBlob(ReadFileRequest),
    Reset(ResetFileRequest),
    TransactionWrite(TransactionWriteRequest),
    TransactionFlush(oneshot::Sender<()>),
    TransactionLoad(oneshot::Sender<String>),
}

pub struct NetworkStorage {
    pub action_sender: Sender<NetworkStorageAction>,
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
        let (sender, receiver) = oneshot::channel::<Result<Vec<u8>, ()>>();

        // Is the problem that this is happening within the main thread?
        self.action_sender
            .blocking_send(NetworkStorageAction::ReadBlob(ReadFileRequest {
                file_path: path,
                sender: sender,
            }))
            .unwrap();

        receiver.recv().unwrap()
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
