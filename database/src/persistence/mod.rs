pub mod file;
pub mod s3;

// TODO:
//  - Implement result class
//  - Append some bytes to an existing path (WAL)
pub trait Persistence {
    fn write_blob(&self, path: &str, bytes: &[u8]) -> ();
    fn read_blob(&self, path: &str) -> Result<Vec<u8>, ()>;
    fn init(&self);
    fn reset(&self);
}
