use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    thread,
    time::Instant,
};

use num_format::{Locale, ToFormattedString};
use uuid::Uuid;

use crate::{
    database::commands::{Control, DatabaseCommand, DatabaseCommandResponse},
    model::statement::{Statement, StatementResult},
};

use super::{
    commands::{DatabaseCommandRequest, DatabaseCommandTransactionResponse},
    request_manager::RequestManager,
    snapshot::SnapshotManager,
    table::table::PersonTable,
    transaction::TransactionWAL,
};

pub struct DatabaseOptions {
    data_directory: PathBuf,
    restore: bool,
}

// Implements: https://rust-unofficial.github.io/patterns/patterns/creational/builder.html
impl DatabaseOptions {
    pub fn set_data_directory(mut self, data_directory: PathBuf) -> Self {
        self.data_directory = data_directory;
        self
    }

    pub fn set_restore(mut self, restore: bool) -> Self {
        self.restore = restore;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        // Defaults to $CDW/data
        Self {
            data_directory: PathBuf::from("data"),
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
    transaction_wal: TransactionWAL,
    pub database_options: DatabaseOptions,
    snapshot_manager: SnapshotManager,
}

impl Database {
    pub fn new(options: DatabaseOptions) -> Self {
        Self {
            person_table: PersonTable::new(),
            transaction_wal: TransactionWAL::new(options.data_directory.clone()),
            snapshot_manager: SnapshotManager::new(options.data_directory.clone()),
            database_options: options,
        }
    }

    pub fn new_test() -> Self {
        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default()
            .set_data_directory(database_dir)
            .set_restore(false);

        Self {
            person_table: PersonTable::new(),
            transaction_wal: TransactionWAL::new(options.data_directory.clone()),
            snapshot_manager: SnapshotManager::new(options.data_directory.clone()),
            database_options: options,
        }
    }

    pub fn reset_database_state(&mut self) -> usize {
        let row_count = self.person_table.person_rows.len();

        // Clean out snapshot and transaction log
        self.snapshot_manager.delete_snapshot();

        // Reset the database to a clean state
        self.person_table = PersonTable::new();
        self.transaction_wal = TransactionWAL::new(self.database_options.data_directory.clone());
        self.snapshot_manager = SnapshotManager::new(self.database_options.data_directory.clone());

        row_count
    }

    fn start_thread(
        receiver: flume::Receiver<DatabaseCommandRequest>,
        database_rw: Arc<RwLock<Self>>,
    ) {
        loop {
            let DatabaseCommandRequest { command, resolver } = match receiver.recv() {
                Ok(request) => request,
                Err(e) => {
                    log::error!("Failed to receive data from channel {}", e);
                    continue;
                }
            };

            log::info!("Received request: {}", command.log_format());

            // TODO: We assume that the send() commands are successful. This is likely okay? because if
            //   if sender is disconnected that should not impact the database
            let transaction_statements = match command {
                DatabaseCommand::Transaction(statements) => statements,
                DatabaseCommand::Control(control) => {
                    match control {
                        Control::Shutdown => {
                            let _ = resolver.send(DatabaseCommandResponse::control_success(
                                "Successfully shutdown database",
                            ));

                            return;
                        }
                        Control::ResetDatabase => {
                            let dropped_row_count =
                                database_rw.write().unwrap().reset_database_state();

                            let _ =
                                resolver.send(DatabaseCommandResponse::control_success(&format!(
                                    "Successfully reset database, dropped: {} rows",
                                    dropped_row_count
                                )));

                            continue;
                        }
                        Control::SnapshotDatabase => {
                            let mut database = database_rw.write().unwrap();

                            let transaction_id = database
                                .transaction_wal
                                .get_current_transaction_id()
                                .clone();

                            let table = &database.person_table;

                            // Persist current state to disk
                            database
                                .snapshot_manager
                                .create_snapshot(table, transaction_id);

                            let flush_transactions = database.transaction_wal.flush_transactions();

                            let _ =
                                resolver.send(DatabaseCommandResponse::control_success(&format!(
                                    "Successfully created snapshot: compressed {} txs",
                                    flush_transactions
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
                    let _ = database_rw
                        .write()
                        .unwrap()
                        .apply_transaction(transaction_statements, ApplyMode::Request(resolver));
                }
                false => {
                    // As there is no WAL, we can just read from the database and send the response
                    let response = database_rw
                        .read()
                        .unwrap()
                        .query_transaction(transaction_statements);

                    let _ = resolver.send(
                        DatabaseCommandResponse::DatabaseCommandTransactionResponse(response),
                    );
                }
            };
        }
    }

    pub fn run(mut self, threads: u32) -> RequestManager {
        let transaction_log_location = self.database_options.data_directory.clone();

        log::info!(
            "Transaction Log Location: [{}]",
            transaction_log_location.display()
        );

        if self.database_options.restore {
            let now = Instant::now();

            // Call chain -> snapshot_manager -> person_table
            let (snapshot_count, metadata) = self
                .snapshot_manager
                .restore_snapshot(&mut self.person_table);

            // If there was a snapshot to restore from we update the transaction log
            self.transaction_wal
                .set_current_transaction_id(metadata.current_transaction_id.clone());

            let restored_transactions = TransactionWAL::restore(transaction_log_location);
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
                self.transaction_wal
                    .get_current_transaction_id()
                    .to_number()
                    .to_formatted_string(&Locale::en)
            );
        }

        let (tx, rx) = flume::unbounded::<DatabaseCommandRequest>();

        let database_mutex = Arc::new(RwLock::new(self));

        for _ in 0..threads {
            let thread_rx = rx.clone();
            let database_rw = database_mutex.clone();

            // Spawn a new thread for each request
            thread::spawn(move || {
                Database::start_thread(thread_rx, database_rw);
            });
        }

        return RequestManager::new(tx);
    }

    pub fn query_transaction(
        &self,
        statements: Vec<Statement>,
    ) -> DatabaseCommandTransactionResponse {
        let query_latest_transaction_id = self.transaction_wal.get_current_transaction_id();

        let mut statement_results: Vec<StatementResult> = Vec::new();

        for statement in statements {
            let statement_result = self
                .person_table
                .query_statement(statement.clone(), query_latest_transaction_id);

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
        &mut self,
        statements: Vec<Statement>,
        mode: ApplyMode,
    ) -> DatabaseCommandTransactionResponse {
        let applying_transaction_id = self
            .transaction_wal
            .get_current_transaction_id()
            .increment();

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

                self.transaction_wal.commit(
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

    use super::test_utils::{database_test, database_test_task};
    use crate::database::commands::DatabaseCommandTransactionResponse;
    use crate::database::database::Database;
    use crate::model::statement::StatementResult;

    mod add {

        use crate::database::database::ApplyMode;

        use super::*;

        #[test]
        fn add_happy_path() {
            let mut database = Database::new_test();

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
            let mut database = Database::new_test();

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
            let mut database = Database::new_test();

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
        fn add_multiple_transaction_rollback() {
            let mut database = Database::new_test();

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
        fn rollback_response() {
            // Given an empty database
            let mut database = Database::new_test();

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
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_statements();

            // When a rollback happens
            database.apply_transaction(rollback_actions, ApplyMode::Restore);

            // Then there should be no items in the transaction log
            assert_eq!(
                database.transaction_wal.get_current_transaction_id(),
                &TransactionId::new_first_transaction(),
                "Transaction log should be empty"
            );
        }

        #[test]
        fn indexes_are_empty() {
            // Given an empty database
            let mut database = Database::new_test();

            // When a rollback happens
            let rollback_actions = create_rollback_statements();

            let _ = database.apply_transaction(rollback_actions, ApplyMode::Restore);

            // Then the items at the start of the transaction, should be emptied from the index
            assert_eq!(
                database.person_table.unique_email_index.len(),
                0,
                "Unique email index should be empty"
            );
        }

        #[test]
        fn row_table_is_empty() {
            // Given an empty database
            let mut database = Database::new_test();

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

    mod test_utils {
        use uuid::Uuid;

        use crate::{
            consts::consts::EntityId,
            database::database::test_utils::DatabaseTest,
            model::{person::Person, statement::Statement},
        };

        #[test]
        fn decouple_set_up_from_run() {
            let action_generator = |_, _| {
                Statement::Add(Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            let db_test = DatabaseTest::new(5, 2, 100, action_generator, None);

            db_test.start();
        }
    }

    mod bulk {
        use crate::database::table::query::{QueryMatch, QueryPersonData};

        use super::*;

        // The sync tests work by sending a single statement, waiting for a response and THEN
        //  sending the next statement. Due to the fact we batch process file system writes, this
        //  is not a good test for performance. Due to this batch processing the minimum time for
        //  a single statement is 1-3ms, so the minimum time for 100 statements is 100ms.
        //
        // This is not an issue with async tests because we submit all the statements at once and
        //  then wait for all of them to respond.
        const SYNC_WRITE_ENABLED: bool = false;

        #[test]
        #[ignore]
        fn update() {
            // Due to the RW Lock, writes are constant regardless of the number of threads (the lower the thread count the better)
            // As a general note -- 75k TPS is really good, this is a 'some-what' durable commit as we're writing the WAL to disk
            //  We are not technically flushing the WAL to disk, hence why
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

            // ~82k s/ps on M1 MBA
            if SYNC_WRITE_ENABLED {
                let metrics = database_test(5, 1, 500_000, action_generator, Some(setup_generator));
                println!("[SYNC] Metrics: {:#?}", metrics);
            }

            let metrics =
                database_test_task(4, 1, 500_000, action_generator, Some(setup_generator));

            // ~150k s/ps on M1 MBA
            println!("[ASYNC] Metrics: {:#?}", metrics);
        }

        #[test]
        #[ignore]
        fn add() {
            // Due to the RW Lock, writes are constant regardless of the number of threads (the lower the thread count the better)
            // As a general note -- 75k TPS is really good, this is a 'some-what' durable commit as we're writing the WAL to disk
            //  We are not technically flushing the WAL to disk, hence why
            let action_generator = |thread, i| {
                Statement::Add(person::Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(format!("Email-{}{}", thread, i)),
                })
            };

            // ~75k s/ps on M1 MBA
            if SYNC_WRITE_ENABLED {
                let metrics = database_test(5, 1, 1_000_000, action_generator, None);
                println!("[SYNC] Metrics: {:#?}", metrics);
            }

            let metrics = database_test_task(4, 1, 1_000_000, action_generator, None);

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

            // ~300k-400k s/ps on M1 MBA
            let metrics = database_test(5, 3, 3_000_000, action_generator, Some(setup_generator));
            println!("[SYNC] Metrics: {:#?}", metrics);

            let metrics =
                database_test_task(4, 4, 3_000_000, action_generator, Some(setup_generator));

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

            let metrics = database_test(5, 3, 3_000_000, action_generator, Some(setup_generator));

            // ~300k-400k s/ps on M1 MBA
            println!("[SYNC] Metrics: {:#?}", metrics);

            let metrics =
                database_test_task(3, 3, 3_000_000, action_generator, Some(setup_generator));

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
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
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

    pub struct DatabaseTest {
        request_manager: RequestManager,
        worker_threads: u32,
    }

    impl DatabaseTest {
        pub fn new(worker_threads: u32, database_threads: u32) -> Self {
            let rm = Database::new_test().run(database_threads);

            Self {
                request_manager: rm,
                worker_threads,
            }
        }

        pub fn set_up_thread_pool(
            &self,
            actions: u32,
            action_generator: fn(u32, u32) -> Statement,
        ) {
            for thread_id in 0..self.worker_threads {
                let rm = self.request_manager.clone();

                thread::spawn(move || loop {
                    for index in 0..actions {
                        let statement = action_generator(thread_id, index);

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
                });
            }
        }

        pub fn set_up_data(&self, setup_generator: fn(u32) -> Statement) {
            // We require worker threads to be specified upfront, this helps
            //  to partition data into worker specific identifiers (e.g. worker-<id>-<iteration>@gmail.com)
            for thread_id in 0..self.worker_threads {
                let rm = self.request_manager.clone();

                let statement = setup_generator(thread_id);

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
    }

    pub fn database_test(
        worker_threads: u32,
        database_threads: u32,
        actions: u32,
        action_generator: fn(u32, u32) -> Statement,
        setup_generator: Option<fn(u32) -> Statement>,
    ) -> TestMetrics {
        let rm = Database::new_test().run(database_threads);

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

        let mut sender_threads: Vec<JoinHandle<()>> = vec![];

        let now = Instant::now();

        for thread_id in 0..worker_threads {
            let rm = rm.clone();

            let sender_thread = thread::spawn(move || {
                for index in 0..(actions / worker_threads) {
                    let statement = action_generator(thread_id, index);

                    let action_result = rm
                        .send_single_statement(statement)
                        .expect("Should not timeout");

                    // Single will panic if this fails
                    match action_result {
                        StatementResult::Single(_)
                        | StatementResult::GetSingle(_)
                        | StatementResult::List(_) => {}
                        _ => panic!("Unexpected response type"),
                    }
                }
            });

            sender_threads.push(sender_thread);
        }

        for thread in sender_threads {
            thread.join().unwrap();
        }

        let metrics = TestMetrics::new(Mode::Single, now.elapsed(), actions);

        // Allows database thread to successfully exit
        let shutdown_response = rm
            .clone()
            .send_shutdown_request()
            .expect("Should not timeout");

        assert_eq!(
            shutdown_response,
            "Successfully shutdown database".to_string()
        );

        metrics
    }

    /// Sends N items into the channel and then awaits them all at the end. In theory this test
    /// should be faster because it avoids all the 'ping-pong' of sending and receiving
    ///
    /// Note: At the moment we do not validate the results of the actions, but because we use
    ///     get() we are validating that the transaction did commit
    pub fn database_test_task(
        worker_threads: u32,
        database_threads: u32,
        actions: u32,
        action_generator: fn(u32, u32) -> Statement,
        setup_generator: Option<fn(u32) -> Statement>,
    ) -> TestMetrics {
        let database = Database::new_test();

        println!(
            "\nDatabase output location: {}",
            database.database_options.data_directory.display()
        );

        let rm = database.run(database_threads);

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
        let shutdown_response = rm
            .clone()
            .send_shutdown_request()
            .expect("Should not timeout");

        assert_eq!(
            shutdown_response,
            "Successfully shutdown database".to_string()
        );

        metrics
    }
}
