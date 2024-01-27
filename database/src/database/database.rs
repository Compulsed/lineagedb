use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use num_format::{Locale, ToFormattedString};
use uuid::Uuid;

use crate::{
    consts::consts::ErrorString,
    database::request_manager::DatabaseRequestAction,
    model::action::{Action, ActionResult},
};

use super::{
    request_manager::DatabaseRequest, table::table::PersonTable, transaction::TransactionLog,
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
    transaction_log: TransactionLog,
    database_receiver: Receiver<DatabaseRequest>,
    database_options: DatabaseOptions,
}

impl Database {
    pub fn new(database_receiver: Receiver<DatabaseRequest>, options: DatabaseOptions) -> Self {
        Self {
            person_table: PersonTable::new(),
            transaction_log: TransactionLog::new(options.data_directory.clone()),
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
            transaction_log: TransactionLog::new(options.data_directory.clone()),
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

        // On spin-up restore database from disk
        for transaction in TransactionLog::restore(transaction_log_location) {
            self.process_actions(transaction.actions, true)
                .expect("Should not error when replaying valid transactions");
        }

        log::info!(
            "Restored [Duration {}ms, Tx Count {}]",
            now.elapsed().as_millis(),
            self.transaction_log
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

            let process_action = match action {
                DatabaseRequestAction::Request(action) => action,
                DatabaseRequestAction::Shutdown => {
                    let _ = response_sender.send(ActionResult::SuccessStatus(
                        "Successfully shutdown database".to_string(),
                    ));
                    return;
                }
            };

            // TODO:
            //  - Consider how we handle single actions -- perhaps we create a special method for them
            //  - It's likely we have an additional clone in the action response
            let action_response = self.process_actions(vec![process_action], false);

            let _ = match action_response {
                Ok(action_response) => response_sender.send(
                    action_response
                        .first()
                        .expect("Should exist as we only send one action result")
                        .clone(),
                ),
                Err(err) => {
                    response_sender.send(ActionResult::ErrorStatus(format!("ERROR: {}", err)))
                }
            };
        }
    }

    pub fn process_action(
        &mut self,
        user_action: Action,
        restore: bool,
    ) -> Result<ActionResult, ErrorString> {
        let results = self.process_actions(vec![user_action], restore);

        match results {
            Ok(results) => Ok(results
                .into_iter()
                .nth(0)
                .expect("should exist due to process_actions returning the same length")),
            Err(err) => Err(err),
        }
    }

    pub fn process_actions(
        &mut self,
        user_actions: Vec<Action>,
        restore: bool,
    ) -> Result<Vec<ActionResult>, ErrorString> {
        // TODO: Consider filtering out transactions that just have query actions
        let transaction_id = self.transaction_log.add_applying(user_actions.clone());

        let mut status = CommitStatus::Commit;

        struct ActionAndResult {
            action: Action,
            result: ActionResult,
        }

        let mut action_stack: Vec<ActionAndResult> = Vec::new();

        for action in user_actions {
            let apply_result = self
                .person_table
                .apply(action.clone(), transaction_id.clone());

            match apply_result {
                Ok(action_result) => {
                    action_stack.push(ActionAndResult {
                        action,
                        result: action_result,
                    });
                }
                Err(err_string) => {
                    status = CommitStatus::Rollback(err_string);
                }
            }
        }

        match status {
            CommitStatus::Commit => {
                self.transaction_log.update_committed(restore);

                let action_result_stack: Vec<ActionResult> = action_stack
                    .into_iter()
                    .map(|action_and_result| action_and_result.result)
                    .collect();

                Ok(action_result_stack)
            }
            CommitStatus::Rollback(error_status) => {
                // TODO: Write a test to ensure that we rollback in the correct order
                for ActionAndResult { action, result: _ } in action_stack.into_iter().rev() {
                    self.person_table.apply_rollback(action)
                }

                self.transaction_log.update_failed();

                Err(error_status)
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
        use super::*;

        #[test]
        fn add_happy_path() {
            let mut database = Database::new_test();

            let person = Person::new_test();

            let process_action_result = database.process_action(Action::Add(person.clone()), false);

            let action_result = process_action_result.expect("Should not error");

            assert_eq!(action_result, ActionResult::Single(person.clone()));
        }

        #[test]
        fn add_multiple_separate() {
            let mut database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));

            let process_action_result_one =
                database.process_action(Action::Add(person_one.clone()), false);

            let action_result_one = process_action_result_one.expect("Should not error");

            assert_eq!(
                action_result_one,
                ActionResult::Single(person_one.clone()),
                "Person should be returned as a single action result"
            );

            let person_two: Person =
                Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let process_action_result_two =
                database.process_action(Action::Add(person_two.clone()), false);

            let action_result_two = process_action_result_two.expect("Should not error");

            assert_eq!(
                action_result_two,
                ActionResult::Single(person_two),
                "Person should be returned as a single action result"
            );
        }

        #[test]
        fn add_multiple_transaction() {
            let mut database = Database::new_test();

            let person_one = Person::new("Person One".to_string(), Some("Email One".to_string()));
            let person_two = Person::new("Person Two".to_string(), Some("Email Two".to_string()));

            let process_action_result = database.process_actions(
                vec![
                    Action::Add(person_one.clone()),
                    Action::Add(person_two.clone()),
                ],
                false,
            );

            let action_result = process_action_result.expect("Should not error");

            assert_eq!(
                action_result,
                vec![
                    ActionResult::Single(person_one),
                    ActionResult::Single(person_two)
                ]
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

            let action_error = process_action_result.err().expect("Should error");

            assert_eq!(
                action_error.to_string(),
                "Cannot add row as a person already exists with this email: OverlappingEmail"
                    .to_string(),
                "When one action fails, all actions should be rolled back"
            );
        }
    }

    mod transaction_rollback {
        use super::*;

        #[test]
        fn error_response() {
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_actions();

            let error_message = database
                .process_actions(rollback_actions.clone(), false)
                .expect_err("Should error");

            assert_eq!(
                error_message,
                "Cannot add row as a person already exists with this email: OverlappingEmail"
                    .to_string()
            );
        }

        #[test]
        fn transaction_log_is_empty() {
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_actions();

            let _ = database
                .process_actions(rollback_actions.clone(), false)
                .expect_err("Should error");

            assert_eq!(
                database.transaction_log.transactions.len(),
                0,
                "Transaction log should be empty"
            );
        }

        #[test]
        fn indexes_are_empty() {
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_actions();

            let _ = database
                .process_actions(rollback_actions.clone(), false)
                .expect_err("Should error");

            assert_eq!(
                database.person_table.unique_email_index.len(),
                0,
                "Unique email index should be empty"
            );
        }

        #[test]
        fn row_table_is_empty() {
            let mut database = Database::new_test();

            let rollback_actions = create_rollback_actions();

            let _ = database
                .process_actions(rollback_actions.clone(), false)
                .expect_err("Should error");

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

                    let db_response = rm.send_request(action).expect("Should not timeout");

                    // Single will panic if this fails
                    match db_response {
                        ActionResult::Single(_) | ActionResult::GetSingle(_) => {}
                        _ => panic!("Unexpected response"),
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
            .send_shutdown()
            .expect("Should not timeout")
            .success_status();

        assert!(shutdown_response == "Successfully shutdown database".to_string());
    }
}
