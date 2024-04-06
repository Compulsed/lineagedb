use flume::Sender;

use super::request_manager::RequestManager;

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
