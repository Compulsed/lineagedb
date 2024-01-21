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
        println!("Restoring database from disk");

        let now = Instant::now();

        // On spin-up restore database from disk
        for action in TransactionLog::restore(self.database_options.data_directory.clone()) {
            self.process_action(action, true)
                .expect("Should not error when replaying valid transactions");
        }

        println!(
            "Restored database from transaction log. [Duration {}ms, Tx Count {}]",
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
mod test {
    use std::{
        sync::mpsc::{self, Receiver, Sender},
        thread,
    };

    use uuid::Uuid;

    use crate::{
        consts::consts::EntityId,
        database::{
            database::{Database, DatabaseOptions},
            request_manager::{DatabaseRequest, RequestManager},
            table::row::{UpdateAction, UpdatePersonData},
        },
        model::{
            action::Action,
            person::{self, Person},
        },
    };

    fn run_performance_test(actions: u32, action_generator: impl Fn(u32) -> Action) {
        let (database_sender, database_receiver): (
            Sender<DatabaseRequest>,
            Receiver<DatabaseRequest>,
        ) = mpsc::channel();

        thread::spawn(move || {
            let database_dir = format!("/tmp/lineagedb/{}/", Uuid::new_v4().to_string());

            println!("Database directory: {}", database_dir);

            let options = DatabaseOptions::default().set_data_directory(database_dir);

            Database::new(database_receiver, options).run();
        });

        let rm = RequestManager::new(database_sender.clone());

        for index in 0..actions {
            let action = action_generator(index);

            let db_response = rm.send_request(action).expect("Should not timeout");

            // Single will panic if this fails
            db_response.single();
        }

        // Allows database thread to successfully exit
        let shutdown_response = rm
            .send_shutdown()
            .expect("Should not timeout")
            .success_status();

        assert!(shutdown_response == "Successfully shutdown database".to_string());
    }

    #[test]
    fn performance_test_update_long() {
        let id = EntityId::new();

        // 65k tps on M1 MBA
        let action_generator = |index| {
            let full_name = format!("Full Name {}", index);
            let email = format!("Email {}", index);

            if index == 0 {
                return Action::Add(Person {
                    id: id.clone(),
                    full_name,
                    email: Some(email),
                });
            }

            return Action::Update(
                id.clone(),
                UpdatePersonData {
                    full_name: UpdateAction::Set(full_name),
                    email: UpdateAction::Set(email),
                },
            );
        };

        run_performance_test(1_000_000, action_generator);
    }

    #[test]
    fn performance_test_add_long() {
        // 65k tps on M1 MBA
        let action_generator = |_| {
            Action::Add(person::Person {
                id: EntityId::new(),
                full_name: "Test".to_string(),
                email: Some(Uuid::new_v4().to_string()),
            })
        };

        run_performance_test(1_000_000, action_generator);
    }

    #[test]
    fn performance_test_add_short() {
        let action_generator = |_| {
            Action::Add(person::Person {
                id: EntityId::new(),
                full_name: "Test".to_string(),
                email: Some(Uuid::new_v4().to_string()),
            })
        };

        run_performance_test(100, action_generator);
    }
}
