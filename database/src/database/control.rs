use oneshot::Sender;

use crate::consts::consts::TransactionId;

use super::{
    commands::{Control, DatabaseCommandResponse, ShutdownRequest},
    database::Database,
    orchestrator::DatabasePauseEvent,
    request_manager::RequestManager,
    utils::crash::{crash_database, DatabaseCrash},
};
use std::{thread, time::Duration};

pub enum DatabaseControlAction {
    Continue,
    Exit,
}

pub struct ControlContext<'a> {
    pub resolver: Sender<DatabaseCommandResponse>,
    pub thread_id: usize,
    pub database: &'a Database,
    pub database_request_managers: &'a Vec<RequestManager>,
    pub transaction_timestamp: &'a TransactionId,
}

impl<'a> ControlContext<'a> {
    pub fn run(self, control: Control) -> DatabaseControlAction {
        match control {
            Control::Sleep(d) => self.sleep(d),
            Control::DatabaseStats => self.database_stats(),
            Control::Shutdown(r) => self.shutdown(r),
            Control::PauseDatabase(r) => self.pause(r),
            Control::ResetDatabase => self.reset(),
            Control::SnapshotDatabase => self.snapshot(),
        }
    }

    fn send_response(self, response: DatabaseCommandResponse) {
        let _ = self
            .resolver
            .send(response)
            .expect("Requester should not be dropped");
    }

    pub fn sleep(self, duration: Duration) -> DatabaseControlAction {
        thread::sleep(duration);

        let response = DatabaseCommandResponse::control_success(&format!(
            "[Thread - {}] Successfully slept thread for {} seconds",
            self.thread_id,
            duration.as_secs()
        ));

        self.send_response(response);

        DatabaseControlAction::Continue
    }

    pub fn database_stats(self) -> DatabaseControlAction {
        let current_transaction_id = (
            "CurrentTransactionID".to_string(),
            self.transaction_timestamp.to_string(),
        );

        let wal_size = (
            "WALSize".to_string(),
            self.database
                .persistence
                .transaction_wal
                .get_wal_size()
                .to_string(),
        );

        let row_count = (
            "RowCount".to_string(),
            self.database.person_table.person_rows.len().to_string(),
        );

        let database_threads = (
            "DatabaseThreads".to_string(),
            self.database.database_options.threads.to_string(),
        );

        let database_thread_index = (
            "DatabaseThreadIndex".to_string(),
            self.thread_id.to_string(),
        );

        let engine = self
            .database
            .database_options
            .storage_engine
            .get_engine_info_stats();

        let info = vec![
            row_count,
            wal_size,
            current_transaction_id,
            database_threads,
            database_thread_index,
        ]
        .into_iter()
        .chain(engine.into_iter())
        .collect::<Vec<(String, String)>>();

        self.send_response(DatabaseCommandResponse::control_info(info));

        DatabaseControlAction::Continue
    }

    pub fn shutdown(self, request: ShutdownRequest) -> DatabaseControlAction {
        // The DB thread that received the shutdown request is responsible for ensuring all the other threads shutdown.
        let response = match request {
            ShutdownRequest::Coordinator => {
                // Send request to every DB thread, telling them to shutdown / stop working,
                //  'send_shutdown_request' is a blocking call, so we will wait for all threads to shutdown
                for rm in self.database_request_managers {
                    let _ = rm
                        .send_shutdown_request(ShutdownRequest::Worker)
                        .expect("Should respond to shutdown request");
                }

                // Once we have successfully shutdown all threads, report success to the caller
                DatabaseCommandResponse::control_success(&format!(
                    "[Thread: {}] Successfully shutdown database",
                    self.thread_id
                ))
            }
            ShutdownRequest::Worker => DatabaseCommandResponse::control_success(&format!(
                "[Thread: {}] Successfully shut down worker thread",
                self.thread_id
            )),
        };

        self.send_response(response);

        // As we are shutting down, we can now exit the control loop
        DatabaseControlAction::Exit
    }

    pub fn pause(self, resume: flume::Receiver<()>) -> DatabaseControlAction {
        let thread_id = self.thread_id;

        let response = DatabaseCommandResponse::control_success(&format!(
            "[Thread - {}] Successfully paused thread",
            thread_id
        ));

        self.send_response(response);

        // Blocking wait for `DatabasePauseEvent` to be dropped
        let _ = resume.recv();

        log::info!("[Thread - {}] Successfully resumed thread", thread_id);

        DatabaseControlAction::Continue
    }

    pub fn reset(self) -> DatabaseControlAction {
        // Note, because we have paused the database we should not get ANY deadlocks
        //  concurrency issues
        let database_reset_guard = &DatabasePauseEvent::new(self.database_request_managers);

        let dropped_row_count = self.database.reset_database_state(database_reset_guard);

        let response = DatabaseCommandResponse::control_success(&format!(
            "Successfully reset database, dropped: {} rows",
            dropped_row_count
        ));

        self.send_response(response);

        DatabaseControlAction::Continue
    }

    pub fn snapshot(self) -> DatabaseControlAction {
        // Note, because we have paused the database we should not get ANY deadlocks
        //  concurrency issues
        let database_reset_guard = &DatabasePauseEvent::new(&self.database_request_managers);

        let table = &self.database.person_table;

        // Persist current state to disk
        let snapshot_request = self.database.persistence.snapshot_manager.create_snapshot(
            database_reset_guard,
            table,
            self.transaction_timestamp.clone(),
        );

        if let Err(e) = snapshot_request {
            let _ = self
                .resolver
                .send(DatabaseCommandResponse::control_error(&format!(
                    "Failed to create snapshot database is now inconsistent: {}",
                    e
                )));

            crash_database(DatabaseCrash::InconsistentStorageFromSnapshot(e));
        }

        let flush_transactions = self
            .database
            .persistence
            .transaction_wal
            .flush_transactions(database_reset_guard);

        let flush_transactions_count = match flush_transactions {
            Ok(t) => t,
            Err(e) => {
                let _ = self
                    .resolver
                    .send(DatabaseCommandResponse::control_error(&format!(
                        "Failed to create snapshot database is now inconsistent: {}",
                        e
                    )));

                crash_database(DatabaseCrash::InconsistentStorageFromSnapshot(e));
            }
        };

        let response = DatabaseCommandResponse::control_success(&format!(
            "Successfully created snapshot: compressed {} txs",
            flush_transactions_count
        ));

        self.send_response(response);

        DatabaseControlAction::Continue
    }
}
