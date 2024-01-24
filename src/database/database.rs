use std::{sync::mpsc::Receiver, time::Instant};

use num_format::{Locale, ToFormattedString};

use crate::{
    consts::consts::ErrorString,
    database::request_manager::DatabaseRequestAction,
    model::action::{Action, ActionResult},
};

use super::{
    request_manager::DatabaseRequest, table::table::PersonTable, transaction::TransactionLog,
};

pub struct DatabaseOptions {
    data_directory: String,
}

impl DatabaseOptions {
    pub fn set_data_directory(mut self, data_directory: String) -> Self {
        self.data_directory = data_directory;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            data_directory: String::from("data"),
        }
    }
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

    pub fn run(&mut self) {
        // println!("Restoring database from disk");

        let now = Instant::now();

        // On spin-up restore database from disk
        for action in TransactionLog::restore(self.database_options.data_directory.clone()) {
            self.process_action(action, true)
                .expect("Should not error when replaying valid transactions");
        }

        // println!(
        //     "Restored database from transaction log. [Duration {}ms, Tx Count {}]",
        //     now.elapsed().as_millis(),
        //     self.transaction_log
        //         .get_current_transaction_id()
        //         .to_number()
        //         .to_formatted_string(&Locale::en)
        // );

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

            let action_response = self.process_action(process_action, false);

            let _ = match action_response {
                Ok(action_response) => response_sender.send(action_response),
                Err(err) => {
                    response_sender.send(ActionResult::ErrorStatus(format!("ERROR: {}", err)))
                }
            };
        }
    }

    fn process_action(
        &mut self,
        user_action: Action,
        restore: bool,
    ) -> Result<ActionResult, ErrorString> {
        let mut transaction_id = self.transaction_log.get_current_transaction_id();

        let is_mutation = user_action.is_mutation();

        if is_mutation {
            transaction_id = self.transaction_log.add_applying(user_action.clone());
        }

        let action_result = self.person_table.apply(user_action, transaction_id);

        if is_mutation {
            match action_result {
                Ok(_) => self.transaction_log.update_committed(restore),
                Err(_) => self.transaction_log.update_failed(),
            }
        }

        action_result
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
            let database_dir = format!("/tmp/lineagedb/{}/", Uuid::new_v4().to_string());

            // println!("Database directory: {}", database_dir);

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
