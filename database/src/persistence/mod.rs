pub mod file;
pub mod s3;

// TODO:
//  - Implement result class
//  - Append some bytes to an existing path (WAL)
pub trait Persistence {
    fn write_blob(&self, path: String, bytes: Vec<u8>) -> ();
    fn read_blob(&self, path: String) -> Result<Vec<u8>, ()>;
    fn init(&self);
    fn reset(&self);
}
