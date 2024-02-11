use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use num_format::{Locale, ToFormattedString};
use uuid::Uuid;

use crate::{
    database::request_manager::{DatabaseRequestStatement, DatabaseResponseStatement},
    model::action::{Statement, StatementResult},
};

use super::{
    request_manager::DatabaseRequest, snapshot::SnapshotManager, table::table::PersonTable,
    transaction::TransactionWAL,
};

pub struct DatabaseOptions {
    data_directory: PathBuf,
}

// Implements: https://rust-unofficial.github.io/patterns/patterns/creational/builder.html
impl DatabaseOptions {
    pub fn set_data_directory(mut self, data_directory: PathBuf) -> Self {
        self.data_directory = data_directory;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        // Defaults to $CDW/data
        Self {
            data_directory: PathBuf::from("data"),
        }
    }
}

enum CommitStatus {
    Commit,
    Rollback(String),
}

pub struct Database {
    person_table: PersonTable,
    transaction_wal: TransactionWAL,
    database_receiver: Receiver<DatabaseRequest>,
    database_options: DatabaseOptions,
    snapshot_manager: SnapshotManager,
}

impl Database {
    pub fn new(database_receiver: Receiver<DatabaseRequest>, options: DatabaseOptions) -> Self {
        Self {
            person_table: PersonTable::new(),
            transaction_wal: TransactionWAL::new(options.data_directory.clone()),
            snapshot_manager: SnapshotManager::new(options.data_directory.clone()),
            database_receiver,
            database_options: options,
        }
    }

    pub fn new_test() -> Self {
        let (_, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
            mpsc::channel();

        let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
            .iter()
            .collect();

        let options = DatabaseOptions::default().set_data_directory(database_dir);

        Self {
            person_table: PersonTable::new(),
            transaction_wal: TransactionWAL::new(options.data_directory.clone()),
            snapshot_manager: SnapshotManager::new(options.data_directory.clone()),
            database_receiver: database_receiver,
            database_options: options,
        }
    }

    pub fn reset_database_state(&mut self) {
        // Clean out snapshot and transaction log
        self.snapshot_manager.delete_snapshot();

        // Reset the database to a clean state
        self.person_table = PersonTable::new();
        self.transaction_wal = TransactionWAL::new(self.database_options.data_directory.clone());
        self.snapshot_manager = SnapshotManager::new(self.database_options.data_directory.clone());
    }

    pub fn run(&mut self) {
        let transaction_log_location = self.database_options.data_directory.clone();

        log::info!(
            "Transaction Log Location: [{}]",
            transaction_log_location.display()
        );

        let now = Instant::now();

        // Restore from snapshots
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
            if let DatabaseResponseStatement::TransactionRollback(rollback_message) =
                self.apply_transaction(transaction.statements, true)
            {
                panic!(
                    "Should not be able to rollback a transaction on startup: {}",
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

        // Process incoming requests from the channel
        loop {
            let DatabaseRequest {
                statement,
                response_sender,
            } = self.database_receiver.recv().unwrap();
            log::info!("Received request: {}", statement.log_format());

            let process_statement = match statement {
                DatabaseRequestStatement::Request(statement) => statement,
                DatabaseRequestStatement::Shutdown => {
                    let _ = response_sender.send(DatabaseResponseStatement::new_single_response(
                        StatementResult::SuccessStatus(
                            "Successfully shutdown database".to_string(),
                        ),
                    ));

                    return;
                }
                DatabaseRequestStatement::DropDatabase => {
                    self.reset_database_state();

                    let statement_response = DatabaseResponseStatement::new_single_response(
                        StatementResult::SuccessStatus(
                            "Successfully dropped and shutdown the database".to_string(),
                        ),
                    );

                    response_sender
                        .send(statement_response)
                        .expect("Should always be able to send a response back to the caller");

                    continue;
                }
                DatabaseRequestStatement::SnapshotDatabase => {
                    // Persist current state to disk
                    let result = self.snapshot_manager.create_snapshot(
                        &mut self.person_table,
                        self.transaction_wal.get_current_transaction_id().clone(),
                    );

                    self.transaction_wal.flush_transactions();

                    let action_response: DatabaseResponseStatement = match result {
                        Ok(_) => DatabaseResponseStatement::new_single_response(
                            StatementResult::SuccessStatus(
                                "Successfully snap shotted database".to_string(),
                            ),
                        ),
                        Err(err) => DatabaseResponseStatement::CommandError(format!("{}", err)),
                    };

                    response_sender
                        .send(action_response)
                        .expect("Should always be able to send a response back to the caller");

                    continue;
                }
            };

            let statement_response = self.apply_transaction(process_statement, false);

            // Sends the response data back to the caller of the request (i.e.), the entity on the other end of the channel
            response_sender
                .send(statement_response)
                .expect("Should always be able to send a response back to the caller")
        }
    }

    pub fn apply_statement(
        &mut self,
        statement: Statement,
        restore: bool,
    ) -> DatabaseResponseStatement {
        let results = self.apply_transaction(vec![statement], restore);

        if let DatabaseResponseStatement::Response(mut results) = results {
            return DatabaseResponseStatement::new_single_response(
                results
                    .pop()
                    .expect("should exist due to process_actions returning the same length"),
            );
        }

        // Transaction rollback
        return results;
    }

    pub fn apply_transaction(
        &mut self,
        statements: Vec<Statement>,
        restore: bool,
    ) -> DatabaseResponseStatement {
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
                if !restore {
                    log::info!("✅ Committed: [TX: {}]", &applying_transaction_id);
                }

                self.transaction_wal
                    .commit(applying_transaction_id, statements, restore);

                let action_result_stack: Vec<StatementResult> = statement_stack
                    .into_iter()
                    .map(|action_and_result| action_and_result.result)
                    .collect();

                DatabaseResponseStatement::Response(action_result_stack)
            }
            CommitStatus::Rollback(error_status) => {
                if !restore {
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

                DatabaseResponseStatement::TransactionRollback(error_status)
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
            action::Statement,
            person::{self, Person},
        },
    };

    use super::test_utils::database_test;
    use crate::database::database::Database;
    use crate::model::action::StatementResult;

    mod add {
        use crate::database::request_manager::DatabaseResponseStatement;

        use super::*;

        #[test]
        fn add_happy_path() {
            let mut database = Database::new_test();

            let person = Person::new_test();

            let statement_result = database.apply_statement(Statement::Add(person.clone()), false);

            assert_eq!(
                statement_result,
                DatabaseResponseStatement::new_single_response(StatementResult::Single(person))
            );
        }

        #[test]
        fn add_multiple_separate() {
            let mut database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));

            let statement_result_one =
                database.apply_statement(Statement::Add(person_one.clone()), false);

            assert_eq!(
                statement_result_one,
                DatabaseResponseStatement::new_single_response(StatementResult::Single(
                    person_one.clone()
                )),
                "Person should be returned as a single action result"
            );

            let person_two: Person =
                Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let action_result_two =
                database.apply_statement(Statement::Add(person_two.clone()), false);

            assert_eq!(
                action_result_two,
                DatabaseResponseStatement::new_single_response(StatementResult::Single(
                    person_two.clone()
                )),
                "Person should be returned as a single action result"
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
                false,
            );

            assert_eq!(
                action_results,
                DatabaseResponseStatement::new_multiple_response(vec![
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
                false,
            );

            let action_error = process_action_result;

            assert_eq!(
                action_error,
                DatabaseResponseStatement::TransactionRollback(
                    "Cannot add row as a person already exists with this email: OverlappingEmail"
                        .to_string()
                ),
                "When one action fails, all actions should be rolled back"
            );
        }
    }

    mod transaction_rollback {
        use crate::{
            consts::consts::TransactionId, database::request_manager::DatabaseResponseStatement,
        };

        use super::*;

        #[test]
        fn rollback_response() {
            // Given an empty database
            let mut database = Database::new_test();

            // When a rollback happens
            let rollback_actions = create_rollback_actions();

            let error_message = database.apply_transaction(rollback_actions, false);

            // The transaction log will be empty
            assert_eq!(
                error_message,
                DatabaseResponseStatement::TransactionRollback(
                    "Cannot add row as a person already exists with this email: OverlappingEmail"
                        .to_string()
                )
            );
        }

        #[test]
        fn transaction_log_is_empty() {
            // Given an empty database
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_actions();

            // When a rollback happens
            database.apply_transaction(rollback_actions, false);

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
            let rollback_actions = create_rollback_actions();

            let _ = database.apply_transaction(rollback_actions, false);

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
            let rollback_actions = create_rollback_actions();

            let _ = database.apply_transaction(rollback_actions, false);

            // The row that was created for the item is removed
            assert_eq!(
                database.person_table.person_rows.len(),
                0,
                "Person rows should be empty"
            );
        }

        fn create_rollback_actions() -> Vec<Statement> {
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

    mod bulk {
        use super::*;

        #[test]
        fn update() {
            // 65k tps on M1 MBA
            let action_generator = |thread: i32, index: u32| {
                let id = EntityId(thread.to_string());
                let full_name = format!("Full Name {}-{}", thread, index);
                let email = format!("Email {}-{}", thread, index);

                if index == 0 {
                    return Statement::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Statement::Update(
                    id,
                    UpdatePersonData {
                        full_name: UpdateStatement::Set(full_name),
                        email: UpdateStatement::Set(email),
                    },
                );
            };

            database_test(1, 5, action_generator);
        }

        #[test]
        fn add() {
            let action_generator = |_, _| {
                Statement::Add(person::Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
            };

            database_test(1, 5, action_generator);
        }

        #[test]
        fn get() {
            let action_generator = |thread_id: i32, index: u32| {
                let id = EntityId(thread_id.to_string());
                let full_name = format!("Full Name {}-{}", thread_id, index);
                let email = format!("Email {}-{}", thread_id, index);

                if index == 0 {
                    return Statement::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Statement::Get(id);
            };

            database_test(1, 5, action_generator);
        }
    }
}

pub mod test_utils {
    use uuid::Uuid;

    use crate::{
        database::{
            database::{Database, DatabaseOptions},
            request_manager::{DatabaseRequest, RequestManager},
        },
        model::action::{Statement, StatementResult},
    };
    use std::{
        path::PathBuf,
        sync::mpsc::{self, Receiver, Sender},
        thread::{self, JoinHandle},
    };

    pub fn database_test(
        worker_threads: i32,
        actions: u32,
        action_generator: fn(i32, u32) -> Statement,
    ) {
        let (database_sender, database_receiver): (
            Sender<DatabaseRequest>,
            Receiver<DatabaseRequest>,
        ) = mpsc::channel();

        thread::spawn(move || {
            let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
                .iter()
                .collect();

            log::info!("Database directory: {}", database_dir.display());

            let options = DatabaseOptions::default().set_data_directory(database_dir);

            Database::new(database_receiver, options).run();
        });

        let mut sender_threads: Vec<JoinHandle<()>> = vec![];

        for thread_id in 0..worker_threads {
            let rm = RequestManager::new(database_sender.clone());

            let sender_thread = thread::spawn(move || {
                for index in 0..actions {
                    let action = action_generator(thread_id, index);

                    let action_result = rm
                        .send_single_statement(action)
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

        // Allows database thread to successfully exit
        let shutdown_response = RequestManager::new(database_sender.clone())
            .send_shutdown_request()
            .expect("Should not timeout");

        assert_eq!(
            shutdown_response,
            "Successfully shutdown database".to_string()
        );
    }
}
