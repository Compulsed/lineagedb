pub mod file;
pub mod s3;

// TODO:
//  - Implement result class
//  - Append some bytes to an existing path (WAL)
pub trait Storage {
    // Snapshot
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> ();
    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()>;
    fn init(&self);
    fn reset_database(&self);

    // Transactions
    fn transaction_write(&mut self, transaction: &[u8]) -> ();
    fn transaction_sync(&self) -> ();
    fn transaction_flush(&mut self) -> ();
    fn transaction_load(&mut self) -> String;
}
