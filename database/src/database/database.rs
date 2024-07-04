use std::{path::PathBuf, sync::Arc, thread, time::Instant};

use num_format::{Locale, ToFormattedString};
use uuid::Uuid;

use crate::{
    consts::consts::TransactionId,
    database::{
        commands::{Control, DatabaseCommand, DatabaseCommandResponse, ShutdownRequest},
        orchestrator::DatabasePauseEvent,
        utils::crash::{crash_database, DatabaseCrash},
    },
    model::statement::{Statement, StatementResult},
    persistence::{
        persistence::Persistence,
        storage::StorageEngine,
        transaction::{TransactionFileWriteMode, TransactionWriteMode},
    },
};

use super::{
    commands::{DatabaseCommandRequest, DatabaseCommandTransactionResponse},
    request_manager::RequestManager,
    table::table::PersonTable,
};

#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    pub data_directory: PathBuf,
    pub restore: bool,
    pub write_mode: TransactionWriteMode,
    pub storage_engine: StorageEngine,
}

// Implements: https://rust-unofficial.github.io/patterns/patterns/creational/builder.html
impl DatabaseOptions {
    /// Defines the directory where the database will store its data
    pub fn set_data_directory(mut self, data_directory: PathBuf) -> Self {
        self.data_directory = data_directory;
        self
    }

    /// Defines whether we should attempt to restore the database from a snapshot and transaction log
    /// on startup
    pub fn set_restore(mut self, restore: bool) -> Self {
        self.restore = restore;
        self
    }

    /// Defines whether we should sync the file write to disk before marking the
    /// transaction as committed. This is useful for durability but can be slow ~3ms per sync
    pub fn set_sync_file_write(mut self, write_mode: TransactionWriteMode) -> Self {
        self.write_mode = write_mode;
        self
    }

    pub fn set_storage_engine(mut self, storage_engine: StorageEngine) -> Self {
        self.storage_engine = storage_engine;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        // Defaults to $CDW/data
        Self {
            data_directory: PathBuf::from("data"),
            write_mode: TransactionWriteMode::File(TransactionFileWriteMode::Sync),
            storage_engine: StorageEngine::File,
            restore: true,
        }
    }
}

// TODO: This is a part of the transaction_wal, should be moved there
enum CommitStatus {
    Commit,
    Rollback(String),
}

/// Transactions can be created from a client submitting a request or from a restore operation
pub enum ApplyMode {
    /// Return the result of the transaction to the client
    Request(oneshot::Sender<DatabaseCommandResponse>),
    /// Do not return the result of the transaction to the client
    Restore,
}

pub struct Database {
    person_table: PersonTable,
    database_options: DatabaseOptions,
    persistence: Persistence,
}

impl Database {
    pub fn new(options: DatabaseOptions) -> Self {
        Self {
            person_table: PersonTable::new(),
            persistence: Persistence::new(options.clone()),
            database_options: options,
        }
    }

    pub fn new_test() -> Self {
        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default()
            .set_storage_engine(StorageEngine::File)
            .set_data_directory(database_dir)
            .set_restore(false)
            .set_sync_file_write(TransactionWriteMode::Off);

        Self {
            person_table: PersonTable::new(),
            persistence: Persistence::new(options.clone()),
            database_options: options,
        }
    }

    pub fn new_test_other_storage() -> Self {
        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default()
            // .set_storage_engine(StorageEngine::S3("dalesalter-test-bucket".to_string()))
            .set_data_directory(database_dir)
            .set_restore(false)
            .set_sync_file_write(TransactionWriteMode::File(TransactionFileWriteMode::Sync));

        Self {
            person_table: PersonTable::new(),
            persistence: Persistence::new(options.clone()),
            database_options: options,
        }
    }

    /// Resets the filesystem and any in-memory state.
    ///
    /// ⚠️ The caller is responsible for stopping the database or else
    /// it may end up in an inconsistent state. If a reset happens
    /// at the same time as a write it is possible that a part of the write is erased
    pub fn reset_database_state(&self, database_pause: &DatabasePauseEvent) -> usize {
        let row_count = self.person_table.person_rows.len();

        // Resets tx id, scrubs wal
        // NOTE: This is kind of odd, why do we need to delete the WAL off of disk if the
        //  crash database will handle this? -- We still have to call this method to delete
        //  transaction state out of memory
        let flush_transactions_from_disk_result = self
            .persistence
            .transaction_wal
            .flush_transactions(database_pause);

        if let Err(e) = flush_transactions_from_disk_result {
            crash_database(DatabaseCrash::InconsistentStorageFromReset(e));
        }

        // Reset the transaction id counter
        // TODO: We should make flushing and deleting separate operations
        //  This was a legacy of us just newing up new WAL when we needed one.
        self.persistence
            .transaction_wal
            .set_current_transaction_id(TransactionId::new_first_transaction());

        // Clean out snapshot and transaction log
        let result = self.persistence.reset();

        if let Err(e) = result {
            crash_database(DatabaseCrash::InconsistentStorageFromReset(e));
        }

        // Resets the in-memory persons table
        self.person_table.reset(database_pause);

        row_count
    }

    fn start_thread(
        thread_id: usize,
        receiver: flume::Receiver<DatabaseCommandRequest>,
        database_request_managers: Vec<RequestManager>,
        database: Arc<Self>,
    ) {
        let database_request_managers = &database_request_managers;

        loop {
            let DatabaseCommandRequest { command, resolver } = match receiver.recv() {
                Ok(request) => request,
                Err(e) => {
                    log::error!("Failed to receive data from channel {}", e);
                    continue;
                }
            };

            log::info!(
                "[Thread: {}] Received request: {}",
                thread_id,
                command.log_format()
            );

            // TODO: We assume that the send() commands are successful. This is likely okay? because if
            //   if sender is disconnected that should not impact the database
            let transaction_statements = match command {
                DatabaseCommand::Transaction(statements) => statements,
                DatabaseCommand::Control(control) => {
                    match control {
                        Control::Shutdown(request) => {
                            // The DB thread that received the shutdown request is responsible for ensuring all the other threads shutdown.
                            match request {
                                ShutdownRequest::Coordinator => {
                                    // Send request to every DB thread, telling them to shutdown / stop working
                                    for rm in database_request_managers {
                                        let _ = rm
                                            .send_shutdown_request(ShutdownRequest::Worker)
                                            .expect("Should respond to shutdown request");
                                    }

                                    // Once we have successfully shutdown all threads, report success to the caller
                                    let _ = resolver.send(
                                        DatabaseCommandResponse::control_success(&format!(
                                            "[Thread: {}] Successfully shutdown database",
                                            thread_id
                                        )),
                                    );
                                }
                                ShutdownRequest::Worker => {
                                    let _ = resolver.send(
                                        DatabaseCommandResponse::control_success(&format!(
                                            "[Thread: {}] Successfully shut down",
                                            thread_id
                                        )),
                                    );
                                }
                            }

                            return;
                        }
                        Control::PauseDatabase(resume) => {
                            let _ = resolver.send(DatabaseCommandResponse::control_success(
                                &format!("[Thread - {}] Successfully paused thread", thread_id),
                            ));

                            // No timeout, should just wait until the caller tells us we are ready to resume processing
                            let _ = resume.recv();

                            log::info!("[Thread - {}] Successfully resumed thread", thread_id);

                            continue;
                        }
                        Control::ResetDatabase => {
                            // Note, because we have paused the database we should not get ANY deadlocks
                            //  concurrency issues
                            let database_reset_guard =
                                &DatabasePauseEvent::new(&database_request_managers);

                            let dropped_row_count =
                                database.reset_database_state(database_reset_guard);

                            let _ =
                                resolver.send(DatabaseCommandResponse::control_success(&format!(
                                    "Successfully reset database, dropped: {} rows",
                                    dropped_row_count
                                )));

                            continue;
                        }
                        Control::SnapshotDatabase => {
                            // Note, because we have paused the database we should not get ANY deadlocks
                            //  concurrency issues
                            let database_reset_guard =
                                &DatabasePauseEvent::new(&database_request_managers);

                            let transaction_id = database
                                .persistence
                                .transaction_wal
                                .get_increment_current_transaction_id()
                                .clone();

                            let table = &database.person_table;

                            // Persist current state to disk
                            let snapshot_request = database
                                .persistence
                                .snapshot_manager
                                .create_snapshot(database_reset_guard, table, transaction_id);

                            if let Err(e) = snapshot_request {
                                let _ = resolver.send(DatabaseCommandResponse::control_error(
                                    &format!(
                                    "Failed to create snapshot database is now inconsistent: {}",
                                    e
                                ),
                                ));

                                crash_database(DatabaseCrash::InconsistentStorageFromSnapshot(e));
                            }

                            let flush_transactions = database
                                .persistence
                                .transaction_wal
                                .flush_transactions(database_reset_guard);

                            let flush_transactions_count = match flush_transactions {
                                Ok(t) => t,
                                Err(e) => {
                                    let _ = resolver.send(DatabaseCommandResponse::control_error(
                                        &format!("Failed to create snapshot database is now inconsistent: {}", e),
                                    ));

                                    crash_database(DatabaseCrash::InconsistentStorageFromSnapshot(
                                        e,
                                    ));
                                }
                            };

                            let _ =
                                resolver.send(DatabaseCommandResponse::control_success(&format!(
                                    "Successfully created snapshot: compressed {} txs",
                                    flush_transactions_count
                                )));

                            continue;
                        }
                    }
                }
            };

            // If all statements are read, only use the reader lock
            let contains_mutation = transaction_statements
                .iter()
                .any(|statement| statement.is_mutation());

            match contains_mutation {
                true => {
                    // Runs in 'async' mode, once the transaction is committed to the WAL the response database response is sent
                    let _ = database
                        .apply_transaction(transaction_statements, ApplyMode::Request(resolver));
                }
                false => {
                    // TODO: Change this, doing this to remove lock contention on the transaction WAL
                    let query_transaction_id = database
                        .persistence
                        .transaction_wal
                        .get_increment_current_transaction_id()
                        .clone();

                    let response =
                        database.query_transaction(&query_transaction_id, transaction_statements);

                    let _ = resolver.send(
                        DatabaseCommandResponse::DatabaseCommandTransactionResponse(response),
                    );
                }
            };
        }
    }

    /// Starts the database and returns a request manager that can be used to send requests to the database
    ///
    /// Note: Because this method is being called in the main thread, it is sufficient to just panic and the process
    ///     will exist
    pub fn run(self, threads: usize) -> RequestManager {
        self.persistence.init().expect(
            r#"Should always be able to initialize persistence, e.g. setting up files, database connections, etc.
            if we are unable to it means we cannot durably write and thus, need to panic"#,
        );

        if self.database_options.restore {
            let transaction_log_location = self.database_options.data_directory.clone();

            log::info!(
                "Transaction Log Location: [{}]",
                transaction_log_location.display()
            );

            let now = Instant::now();

            // Call chain -> snapshot_manager -> person_table
            let (snapshot_count, metadata) = self
                .persistence
                .snapshot_manager
                .restore_snapshot(&self.person_table)
                .expect(
                    r#"Once persistence has been initialized there should be no issues restoring state from storage"#,
                );

            // If there was a snapshot to restore from we update the transaction log
            self.persistence
                .transaction_wal
                .set_current_transaction_id(metadata.current_transaction_id.clone());

            let restored_transactions = self.persistence.transaction_wal.restore()
                .expect(r#"Once persistence has been initialized there should be no issues restoring state from storage"#);

            let restored_transaction_count = restored_transactions.len();

            // Then add states from the transaction log
            for transaction in restored_transactions {
                let apply_transaction_result =
                    self.apply_transaction(transaction.statements, ApplyMode::Restore);

                if let DatabaseCommandTransactionResponse::Rollback(rollback_message) =
                    apply_transaction_result
                {
                    panic!(
                        "All committed transactions should be replayable on startup: {}",
                        rollback_message
                    );
                }
            }

            log::info!(
                "✅ Successful Restore [Duration: {}ms]",
                now.elapsed().as_millis(),
            );

            log::info!(
                "📀 Data               [RowsFromSnapshot: {}, TransactionsAppliedToSnapshot: {}, CurrentTxId: {}]",
                snapshot_count,
                restored_transaction_count,
                self.persistence.transaction_wal
                    .get_increment_current_transaction_id()
                    .to_number()
                    .to_formatted_string(&Locale::en)
            );
        }

        /*
           Channel strategy:
           - We create a channel per database thread, this acts as sort of thread work queue
           - The request manager will get _all_ of the database channels and will load balance requests across them
           - Database get their own channels AND the channels of all other threads. This will be used for cross
               thread communication (servicing, stop the world, etc)
        */
        let mut tx_channels = vec![];
        let mut rx_channels = vec![];

        for _ in 0..threads {
            let (tx, rx) = flume::unbounded::<DatabaseCommandRequest>();

            tx_channels.push(tx);
            rx_channels.push(rx);
        }

        let database_arc = Arc::new(self);

        for (thread_index, database_rx_channel) in rx_channels.into_iter().enumerate() {
            let database_arc = database_arc.clone();

            // TODO: We do this per thread, likely could do this once and then clone for each thread
            let mut request_managers = tx_channels
                .clone()
                .into_iter()
                .map(|tx| RequestManager::new(vec![tx]))
                .collect::<Vec<RequestManager>>();

            // Remove our own request manager, as we will not need to call ourselves
            request_managers.remove(thread_index);

            // Spawn a new thread for each request
            thread::spawn(move || {
                Database::start_thread(
                    thread_index,
                    database_rx_channel,
                    request_managers,
                    database_arc,
                );
            });
        }

        return RequestManager::new(tx_channels);
    }

    pub fn query_transaction(
        &self,
        query_latest_transaction_id: &TransactionId,
        statements: Vec<Statement>,
    ) -> DatabaseCommandTransactionResponse {
        let mut statement_results: Vec<StatementResult> = Vec::new();

        for statement in statements {
            let statement_result = self
                .person_table
                .query_statement(statement, query_latest_transaction_id);

            match statement_result {
                Ok(statement_result) => statement_results.push(statement_result),
                Err(err) => {
                    return DatabaseCommandTransactionResponse::Rollback(format!("{}", err))
                }
            }
        }

        DatabaseCommandTransactionResponse::Commit(statement_results)
    }

    pub fn apply_transaction(
        &self,
        statements: Vec<Statement>,
        mode: ApplyMode,
    ) -> DatabaseCommandTransactionResponse {
        // Getting also increments -- I think this might be the place where we should be incrementing from
        let applying_transaction_id = self
            .persistence
            .transaction_wal
            .get_increment_current_transaction_id();

        let mut status = CommitStatus::Commit;

        struct StatementAndResult {
            statement: Statement,
            result: StatementResult,
        }

        let mut statement_stack: Vec<StatementAndResult> = Vec::new();

        for statement in statements.clone() {
            let apply_result = self
                .person_table
                .apply(statement.clone(), applying_transaction_id.clone());

            match apply_result {
                Ok(statement_result) => {
                    statement_stack.push(StatementAndResult {
                        statement,
                        result: statement_result,
                    });
                }
                Err(err_string) => {
                    status = CommitStatus::Rollback(format!("{}", err_string));
                }
            }
        }

        match status {
            CommitStatus::Commit => {
                if let ApplyMode::Request(_) = &mode {
                    log::info!("✅ Committed: [TX: {}]", &applying_transaction_id);
                }

                let action_result_stack: Vec<StatementResult> = statement_stack
                    .into_iter()
                    .map(|action_and_result| action_and_result.result)
                    .collect();

                let response = DatabaseCommandTransactionResponse::Commit(action_result_stack);

                // Send the TX off, and increment the transaction id -- Refactor this out
                self.persistence.transaction_wal.commit(
                    applying_transaction_id,
                    statements,
                    DatabaseCommandResponse::DatabaseCommandTransactionResponse(response.clone()),
                    mode,
                );

                return response;
            }
            CommitStatus::Rollback(error_status) => {
                if let ApplyMode::Request(_) = &mode {
                    log::info!("⚠️  Rolled back: [TX: {}]", &applying_transaction_id);
                }

                // TODO: Write a test to ensure that we rollback in the correct order
                for StatementAndResult {
                    statement,
                    result: _,
                } in statement_stack.into_iter().rev()
                {
                    self.person_table.apply_rollback(statement)
                }

                // Rollbacks are not committed to the WAL so we can just return the response
                if let ApplyMode::Request(resolver) = mode {
                    let _ =
                        resolver.send(DatabaseCommandResponse::DatabaseCommandTransactionResponse(
                            DatabaseCommandTransactionResponse::Rollback(error_status.clone()),
                        ));
                }

                DatabaseCommandTransactionResponse::Rollback(error_status)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        consts::consts::EntityId,
        database::table::row::{UpdatePersonData, UpdateStatement},
        model::{
            person::{self, Person},
            statement::Statement,
        },
    };

    use super::test_utils::database_test_task;
    use crate::database::commands::DatabaseCommandTransactionResponse;
    use crate::database::database::Database;
    use crate::model::statement::StatementResult;

    mod add {

        use crate::database::database::ApplyMode;

        use super::*;

        #[test]
        fn add_happy_path() {
            let database = Database::new_test();

            let person = Person::new_test();

            let transcation_result = database
                .apply_transaction(vec![Statement::Add(person.clone())], ApplyMode::Restore);

            assert_eq!(
                transcation_result,
                DatabaseCommandTransactionResponse::new_committed_single_result(
                    StatementResult::Single(person)
                )
            );
        }

        #[test]
        fn add_multiple_separate() {
            let database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));

            let transcation_result_one = database
                .apply_transaction(vec![Statement::Add(person_one.clone())], ApplyMode::Restore);

            assert_eq!(
                transcation_result_one,
                DatabaseCommandTransactionResponse::new_committed_single_result(
                    StatementResult::Single(person_one.clone())
                ),
                "Person should be returned as a single statement result"
            );

            let person_two: Person =
                Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let transcation_result_two = database
                .apply_transaction(vec![Statement::Add(person_two.clone())], ApplyMode::Restore);

            assert_eq!(
                transcation_result_two,
                DatabaseCommandTransactionResponse::new_committed_single_result(
                    StatementResult::Single(person_two.clone())
                ),
                "Person should be returned as a single statement result"
            );
        }

        #[test]
        fn add_multiple_transaction() {
            let database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));
            let person_two = Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let action_results = database.apply_transaction(
                vec![
                    Statement::Add(person_one.clone()),
                    Statement::Add(person_two.clone()),
                ],
                ApplyMode::Restore,
            );

            assert_eq!(
                action_results,
                DatabaseCommandTransactionResponse::new_committed_multiple(vec![
                    StatementResult::Single(person_one),
                    StatementResult::Single(person_two)
                ])
            );
        }

        #[test]
        #[ignore = "with multiple writers we no longer support database constraints"]
        fn add_multiple_transaction_rollback() {
            let database = Database::new_test();

            let person_one = Person::new(
                "Person One".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            let person_two = Person::new(
                "Person Two".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            let process_action_result = database.apply_transaction(
                vec![
                    Statement::Add(person_one.clone()),
                    Statement::Add(person_two.clone()),
                ],
                ApplyMode::Restore,
            );

            let action_error = process_action_result;

            assert_eq!(
                action_error,
                DatabaseCommandTransactionResponse::Rollback(
                    "Cannot add row as a person already exists with this email: OverlappingEmail"
                        .to_string()
                ),
                "When one statement fails, all actions should be rolled back"
            );
        }
    }

    mod transaction_rollback {
        use crate::{consts::consts::TransactionId, database::database::ApplyMode};

        use super::*;

        #[test]
        #[ignore = "with multiple writers we no longer support database constraints"]
        fn rollback_response() {
            // Given an empty database
            let database = Database::new_test();

            // When a rollback happens
            let rollback_actions = create_rollback_statements();

            let error_message = database.apply_transaction(rollback_actions, ApplyMode::Restore);

            // The transaction log will be empty
            assert_eq!(
                error_message,
                DatabaseCommandTransactionResponse::Rollback(
                    "Cannot add row as a person already exists with this email: OverlappingEmail"
                        .to_string()
                )
            );
        }

        #[test]
        fn transaction_log_is_empty() {
            // Given an empty database
            let database = Database::new_test();

            let rollback_actions = create_rollback_statements();

            // When a rollback happens
            database.apply_transaction(rollback_actions, ApplyMode::Restore);

            // Then there should be no items in the transaction log
            assert_eq!(
                database
                    .persistence
                    .transaction_wal
                    .get_increment_current_transaction_id(),
                TransactionId::new_first_transaction(),
                "Transaction log should be empty"
            );
        }

        #[test]
        #[ignore = "with multiple writers we no longer support database constraints"]
        fn row_table_is_empty() {
            // Given an empty database
            let database = Database::new_test();

            // When a rollback happens
            let rollback_actions = create_rollback_statements();

            let _ = database.apply_transaction(rollback_actions, ApplyMode::Restore);

            // The row that was created for the item is removed
            assert_eq!(
                database.person_table.person_rows.len(),
                0,
                "Person rows should be empty"
            );
        }

        fn create_rollback_statements() -> Vec<Statement> {
            let person_one = Person::new(
                "Person One".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            let person_two = Person::new(
                "Person Two".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            vec![
                Statement::Add(person_one.clone()),
                Statement::Add(person_two.clone()),
            ]
        }
    }

    /// Running these tests: cargo test --package database "database::database::tests::bulk" -- --nocapture --ignored --test-threads=1
    mod bulk {
        use super::*;
        use crate::database::table::query::{QueryMatch, QueryPersonData};

        const CLIENT_THREADS: u32 = 2;

        // Seems that three threads is the sweet spot for the M1 MBA
        const READ_THREADS: usize = 4;
        const READ_SAMPLE_SIZE: u32 = 3_000_000;

        // Due to the RW Lock, writes are constant regardless of the number of threads (the lower the thread count the better)
        // As a general note -- 75k TPS is really good, this is a 'some-what' durable commit as we're writing the WAL to disk
        //  We are not technically flushing the WAL to disk, hence why
        const WRITE_THREADS: usize = 1;
        const WRITE_SAMPLE_SIZE: u32 = 500_000;

        #[test]
        #[ignore]
        fn update() {
            let setup_generator = |thread_id: u32| {
                Statement::Add(person::Person {
                    id: EntityId(thread_id.to_string()),
                    full_name: "Test".to_string(),
                    email: Some(format!("Email-{}", thread_id)),
                })
            };

            let action_generator = |thread: u32, index: u32| {
                return Statement::Update(
                    EntityId(thread.to_string()),
                    UpdatePersonData {
                        full_name: UpdateStatement::Set(index.to_string()),
                        email: UpdateStatement::Set(format!("Email-{}{}", thread, index)),
                    },
                );
            };

            let metrics = database_test_task(
                CLIENT_THREADS,
                WRITE_THREADS,
                WRITE_SAMPLE_SIZE,
                action_generator,
                Some(setup_generator),
            );

            // ~150k s/ps on M1 MBA
            println!("[ASYNC] Metrics: {:#?}", metrics);
        }

        #[test]
        #[ignore]
        fn add() {
            // Due to the RW Lock, writes are constant regardless of the number of threads (the lower the thread count the better)
            // As a general note -- 75k TPS is really good, this is a 'some-what' durable commit as we're writing the WAL to disk
            //  We are not technically flushing the WAL to disk, hence why
            let action_generator = |_, _| {
                Statement::Add(person::Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            let metrics = database_test_task(
                CLIENT_THREADS,
                WRITE_THREADS,
                WRITE_SAMPLE_SIZE,
                action_generator,
                None,
            );

            // ~150k s/ps on M1 MBA
            println!("[ASYNC] Metrics: {:#?}", metrics);
        }

        #[test]
        #[ignore]
        fn get() {
            // Due to the RW Lock allows scaling, scaling reads scales logarithmically with the number of threads
            let setup_generator = |thread_id: u32| {
                Statement::Add(person::Person {
                    id: EntityId(thread_id.to_string()),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            let action_generator = |thread_id: u32, _: u32| {
                return Statement::Get(EntityId(thread_id.to_string()));
            };

            let metrics = database_test_task(
                CLIENT_THREADS,
                READ_THREADS,
                READ_SAMPLE_SIZE,
                action_generator,
                Some(setup_generator),
            );

            // ~600k-~900k s/ps on M1 MBA
            println!("[ASYNC] Metrics: {:#?}", metrics);
        }

        #[test]
        #[ignore]
        fn list() {
            // Due to the RW Lock allows scaling, scaling reads scales logarithmically with the number of threads
            let setup_generator = |thread_id: u32| {
                Statement::Add(person::Person {
                    id: EntityId(thread_id.to_string()),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            let action_generator = |_: u32, _: u32| {
                return Statement::List(Some(QueryPersonData {
                    full_name: QueryMatch::Any,
                    email: QueryMatch::Any,
                }));
            };

            let metrics = database_test_task(
                CLIENT_THREADS,
                READ_THREADS,
                READ_SAMPLE_SIZE,
                action_generator,
                Some(setup_generator),
            );

            // ~600k-~900k s/ps on M1 MBA
            println!("[ASYNC] Metrics: {:#?}", metrics);
        }
    }
}

pub mod test_utils {

    use num_format::ToFormattedString;

    use crate::{
        database::{
            database::Database,
            request_manager::{RequestManager, TaskStatementResponse},
        },
        model::statement::{Statement, StatementResult},
    };
    use std::{
        fmt::Debug,
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    };

    #[derive(Debug)]
    pub enum Mode {
        Single,
        Task,
    }

    pub struct TestMetrics {
        pub test_duration: Duration,
        pub statements: u32,
        pub mode: Mode,
    }

    impl TestMetrics {
        pub fn new(mode: Mode, test_duration: Duration, statements: u32) -> Self {
            Self {
                mode,
                test_duration,
                statements,
            }
        }
    }

    impl Debug for TestMetrics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let duration_ms = format!(
                "{}ms",
                self.test_duration
                    .as_millis()
                    .to_formatted_string(&num_format::Locale::en)
            );

            let statements_per_second =
                ((self.statements as f64 / self.test_duration.as_secs_f64()) as u32)
                    .to_formatted_string(&num_format::Locale::en);

            f.debug_struct("TestMetrics")
                .field("mode", &self.mode)
                .field("test_duration", &duration_ms)
                .field(
                    "actions",
                    &self.statements.to_formatted_string(&num_format::Locale::en),
                )
                .field("statements/s", &statements_per_second)
                .finish()
        }
    }

    pub fn run_action(
        rm: RequestManager,
        actions: usize,
        test_identifier: u64,
        action_generator: fn(u64, usize) -> Statement,
    ) -> Vec<Vec<StatementResult>> {
        let mut task_statement_response: Vec<TaskStatementResponse> = Vec::with_capacity(actions);

        for index in 0..actions {
            let statement = action_generator(test_identifier, index);

            task_statement_response.push(rm.send_transaction_task(vec![statement]));
        }

        let mut statement_result: Vec<Vec<StatementResult>> = Vec::with_capacity(actions);

        for statement_response in task_statement_response {
            statement_result.push(statement_response.get().expect("Should not timeout"));
        }

        // Return the statement results so the drop can be calculated outside of the test function
        return statement_result;
    }

    /// Sends N items into the channel and then awaits them all at the end. In theory this test
    /// should be faster because it avoids all the 'ping-pong' of sending and receiving
    ///
    /// Note: At the moment we do not validate the results of the actions, but because we use
    ///     get() we are validating that the transaction did commit
    pub fn database_test_task(
        worker_threads: u32,
        database_threads: usize,
        actions: u32,
        action_generator: fn(u32, u32) -> Statement,
        setup_generator: Option<fn(u32) -> Statement>,
    ) -> TestMetrics {
        let rm = Database::new_test().run(database_threads);

        let mut sender_threads: Vec<JoinHandle<()>> = vec![];

        // All setup is performed in the same thread, is synchronous, and is not included in the timer
        if let Some(setup) = setup_generator {
            for thread_id in 0..worker_threads {
                let rm = rm.clone();

                let statement = setup(thread_id);

                let action_result = rm
                    .send_single_statement(statement)
                    .expect("Should not timeout");

                match action_result {
                    StatementResult::Single(_)
                    | StatementResult::GetSingle(_)
                    | StatementResult::List(_) => {}
                    _ => panic!("Unexpected response type"),
                }
            }
        }

        let now = Instant::now();

        for thread_id in 0..worker_threads {
            let rm = rm.clone();

            let sender_thread = thread::spawn(move || {
                let mut task_statement_response: Vec<TaskStatementResponse> = vec![];

                // Use the task based API, this prevents the need to sync wait for a response before sending another request
                for index in 0..(actions / worker_threads) {
                    let statement = action_generator(thread_id, index);

                    let action_result = rm.send_transaction_task(vec![statement]);

                    task_statement_response.push(action_result);
                }

                for statement_response in task_statement_response {
                    statement_response.get().expect("Should not timeout");
                }
            });

            sender_threads.push(sender_thread);
        }

        for thread in sender_threads {
            thread.join().unwrap();
        }

        let metrics = TestMetrics::new(Mode::Task, now.elapsed(), actions);

        // Allows database thread to successfully exit
        // let shutdown_response = rm
        //     .clone()
        //     .send_shutdown_request()
        //     .expect("Should not timeout");

        // assert_eq!(
        //     shutdown_response,
        //     "Successfully shutdown database".to_string()
        // );

        metrics
    }
}
