use flume::Sender;

use super::request_manager::RequestManager;

/// Represents a pause event for the database
///
/// Rust's type system works really well when you are have to have mutable exclusive reference.
/// This is because the borrow checker will not allow you to have multiple mutable references. This
/// means that it is not possible for multiple threads to have mutable access to the same data.
///
/// Lockless data structures get around this problem through interior mutability. This is where you
/// can pass around a reference and then anything can mutate the data inside. This is what happens
/// with our database threads & skiplists. Because of this, it means that we now need to manage our
/// own locking. This is where the `DatabasePauseEvent` comes in.
///
/// The DatabasePauseEvent acts like a guard and requires that the database threads are paused. In
/// order to do this, we send a message to each database thread telling them to pause. Once they are
/// paused we get this guard. This guard can then be used to ensure that the database threads are
/// paused. Various methods which need to ensure there is no concurrent access can be marked up
/// as requiring this guard. This will ensure that the reader knows that there is no concurrent access
pub struct DatabasePauseEvent {
    resume_txs: Vec<Sender<()>>,
}

impl DatabasePauseEvent {
    pub fn new(database_request_managers: &Vec<RequestManager>) -> Self {
        let mut resume_txs = vec![];

        // Send request to every DB thread, telling them to shutdown / stop working
        for rm in database_request_managers {
            // This makes more sense at a 1-shot, but a 1 shot does not work in a drop,
            //  this is because drop cannot take ownership of the channel
            //
            // Also I wonder if this is possible bug, can drop be called multiple times?
            let (resume_tx, resume_rx) = flume::unbounded::<()>();

            resume_txs.push(resume_tx);

            let _ = rm
                .send_pause_request(resume_rx)
                .expect("Should respond to pause request");
        }

        Self { resume_txs }
    }
}

// TODO: We should turn this into a guard
impl Drop for DatabasePauseEvent {
    fn drop(&mut self) {
        let resume_txs = &self.resume_txs;

        // Start the other database threads back up
        for resume_tx in resume_txs {
            let _ = resume_tx.send(());
        }
    }
}

impl Fake for DatabasePauseEvent {
    fn stub() -> DatabasePauseEvent {
        DatabasePauseEvent { resume_txs: vec![] }
    }
}

pub trait Fake {
    fn stub() -> DatabasePauseEvent;
}
