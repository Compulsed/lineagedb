use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use num_format::{Locale, ToFormattedString};
use uuid::Uuid;

use crate::{
    database::request_manager::{DatabaseRequestAction, DatabaseResponseAction},
    model::action::{Action, ActionResult},
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
            if let DatabaseResponseAction::TransactionRollback(rollback_message) =
                self.process_actions(transaction.actions, true)
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
                action,
                response_sender,
            } = self.database_receiver.recv().unwrap();
            log::info!("Received request: {}", action.log_format());

            let process_action = match action {
                DatabaseRequestAction::Request(action) => action,
                DatabaseRequestAction::Shutdown => {
                    let _ = response_sender.send(DatabaseResponseAction::new_single_response(
                        ActionResult::SuccessStatus("Successfully shutdown database".to_string()),
                    ));

                    return;
                }
                DatabaseRequestAction::SaveSnapshot => {
                    // Persist current state to disk
                    let result = self.snapshot_manager.create_snapshot(
                        &mut self.person_table,
                        self.transaction_wal.get_current_transaction_id().clone(),
                    );

                    // As we have persisted the snapshot, we can now trim the transaction log
                    // TODO: This does not work
                    self.transaction_wal.flush_transactions();

                    let action_response: DatabaseResponseAction = match result {
                        Ok(_) => DatabaseResponseAction::new_single_response(
                            ActionResult::SuccessStatus(
                                "Successfully snap shotted database".to_string(),
                            ),
                        ),
                        Err(err) => DatabaseResponseAction::CommandError(format!("{}", err)),
                    };

                    response_sender
                        .send(action_response)
                        .expect("Should always be able to send a response back to the caller");

                    continue;
                }
            };

            let action_response = self.process_actions(process_action, false);

            // Sends the response data back to the caller of the request (i.e.), the entity on the other end of the channel
            response_sender
                .send(action_response)
                .expect("Should always be able to send a response back to the caller")
        }
    }

    pub fn process_action(&mut self, user_action: Action, restore: bool) -> DatabaseResponseAction {
        let results = self.process_actions(vec![user_action], restore);

        if let DatabaseResponseAction::Response(mut results) = results {
            return DatabaseResponseAction::new_single_response(
                results
                    .pop()
                    .expect("should exist due to process_actions returning the same length"),
            );
        }

        // Transaction rollback
        return results;
    }

    pub fn process_actions(
        &mut self,
        user_actions: Vec<Action>,
        restore: bool,
    ) -> DatabaseResponseAction {
        let applying_transaction_id = self
            .transaction_wal
            .get_current_transaction_id()
            .increment();

        let mut status = CommitStatus::Commit;

        struct ActionAndResult {
            action: Action,
            result: ActionResult,
        }

        let mut action_stack: Vec<ActionAndResult> = Vec::new();

        for action in user_actions.clone() {
            let apply_result = self
                .person_table
                .apply(action.clone(), applying_transaction_id.clone());

            match apply_result {
                Ok(action_result) => {
                    action_stack.push(ActionAndResult {
                        action,
                        result: action_result,
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
                    .commit(applying_transaction_id, user_actions, restore);

                let action_result_stack: Vec<ActionResult> = action_stack
                    .into_iter()
                    .map(|action_and_result| action_and_result.result)
                    .collect();

                DatabaseResponseAction::Response(action_result_stack)
            }
            CommitStatus::Rollback(error_status) => {
                if !restore {
                    log::info!("⚠️  Rolled back: [TX: {}]", &applying_transaction_id);
                }

                // TODO: Write a test to ensure that we rollback in the correct order
                for ActionAndResult { action, result: _ } in action_stack.into_iter().rev() {
                    self.person_table.apply_rollback(action)
                }

                DatabaseResponseAction::TransactionRollback(error_status)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        consts::consts::EntityId,
        database::table::row::{UpdateAction, UpdatePersonData},
        model::{
            action::Action,
            person::{self, Person},
        },
    };

    use super::test_utils::database_test;
    use crate::database::database::Database;
    use crate::model::action::ActionResult;

    mod add {
        use crate::database::request_manager::DatabaseResponseAction;

        use super::*;

        #[test]
        fn add_happy_path() {
            let mut database = Database::new_test();

            let person = Person::new_test();

            let action_result = database.process_action(Action::Add(person.clone()), false);

            assert_eq!(
                action_result,
                DatabaseResponseAction::new_single_response(ActionResult::Single(person))
            );
        }

        #[test]
        fn add_multiple_separate() {
            let mut database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));

            let action_result_one = database.process_action(Action::Add(person_one.clone()), false);

            assert_eq!(
                action_result_one,
                DatabaseResponseAction::new_single_response(ActionResult::Single(
                    person_one.clone()
                )),
                "Person should be returned as a single action result"
            );

            let person_two: Person =
                Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let action_result_two = database.process_action(Action::Add(person_two.clone()), false);

            assert_eq!(
                action_result_two,
                DatabaseResponseAction::new_single_response(ActionResult::Single(
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

            let action_results = database.process_actions(
                vec![
                    Action::Add(person_one.clone()),
                    Action::Add(person_two.clone()),
                ],
                false,
            );

            assert_eq!(
                action_results,
                DatabaseResponseAction::new_multiple_response(vec![
                    ActionResult::Single(person_one),
                    ActionResult::Single(person_two)
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

            let process_action_result = database.process_actions(
                vec![
                    Action::Add(person_one.clone()),
                    Action::Add(person_two.clone()),
                ],
                false,
            );

            let action_error = process_action_result;

            assert_eq!(
                action_error,
                DatabaseResponseAction::TransactionRollback(
                    "Cannot add row as a person already exists with this email: OverlappingEmail"
                        .to_string()
                ),
                "When one action fails, all actions should be rolled back"
            );
        }
    }

    mod transaction_rollback {
        use crate::{
            consts::consts::TransactionId, database::request_manager::DatabaseResponseAction,
        };

        use super::*;

        #[test]
        fn rollback_response() {
            // Given an empty database
            let mut database = Database::new_test();

            // When a rollback happens
            let rollback_actions = create_rollback_actions();

            let error_message = database.process_actions(rollback_actions, false);

            // The transaction log will be empty
            assert_eq!(
                error_message,
                DatabaseResponseAction::TransactionRollback(
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
            database.process_actions(rollback_actions, false);

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

            let _ = database.process_actions(rollback_actions, false);

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

            let _ = database.process_actions(rollback_actions, false);

            // The row that was created for the item is removed
            assert_eq!(
                database.person_table.person_rows.len(),
                0,
                "Person rows should be empty"
            );
        }

        fn create_rollback_actions() -> Vec<Action> {
            let person_one = Person::new(
                "Person One".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            let person_two = Person::new(
                "Person Two".to_string(),
                Some("OverlappingEmail".to_string()),
            );

            vec![
                Action::Add(person_one.clone()),
                Action::Add(person_two.clone()),
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
                    return Action::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Action::Update(
                    id,
                    UpdatePersonData {
                        full_name: UpdateAction::Set(full_name),
                        email: UpdateAction::Set(email),
                    },
                );
            };

            database_test(1, 5, action_generator);
        }

        #[test]
        fn add() {
            let action_generator = |_, _| {
                Action::Add(person::Person {
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
                    return Action::Add(Person {
                        id,
                        full_name,
                        email: Some(email),
                    });
                }

                return Action::Get(id);
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
        model::action::{Action, ActionResult},
    };
    use std::{
        path::PathBuf,
        sync::mpsc::{self, Receiver, Sender},
        thread::{self, JoinHandle},
    };

    pub fn database_test(
        worker_threads: i32,
        actions: u32,
        action_generator: fn(i32, u32) -> Action,
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

                    let action_result = rm.send_single_action(action).expect("Should not timeout");

                    // Single will panic if this fails
                    match action_result {
                        ActionResult::Single(_)
                        | ActionResult::GetSingle(_)
                        | ActionResult::List(_) => {}
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
