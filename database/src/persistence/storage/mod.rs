pub mod dynamodb;
pub mod file;
pub mod s3;

pub trait Storage {
    // Control plane
    fn init(&self);
    fn reset_database(&self);

    // Snapshot (world state, meta data, etc.)
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> ();
    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()>;

    // Transactions
    fn transaction_write(&mut self, transaction: &[u8]) -> ();
    fn transaction_sync(&self) -> ();
    fn transaction_flush(&mut self) -> ();
    fn transaction_load(&mut self) -> String;
}
