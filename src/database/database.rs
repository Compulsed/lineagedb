use std::sync::mpsc::Receiver;

use crate::{
    consts::consts::ErrorString,
    model::action::{Action, ActionResult},
};

use super::{
    request_manager::DatabaseRequest, table::table::PersonTable, transaction::TransactionLog,
};

pub struct Database {
    person_table: PersonTable,
    transaction_log: TransactionLog,
    database_receiver: Receiver<DatabaseRequest>,
}

impl Database {
    pub fn new(database_receiver: Receiver<DatabaseRequest>) -> Self {
        Self {
            person_table: PersonTable::new(),
            transaction_log: TransactionLog::new(),
            database_receiver,
        }
    }

    pub fn run(&mut self) {
        // On spin-up restore database from disk
        for action in TransactionLog::restore() {
            self.process_action(action, true)
                .expect("Should not error when replaying valid transactions");
        }

        // Process incoming requests from the channel
        loop {
            let DatabaseRequest {
                action,
                response_sender,
            } = self.database_receiver.recv().unwrap();

            let action_response = self.process_action(action.clone(), false);

            let _ = match action_response {
                Ok(action_response) => response_sender.send(action_response),
                Err(err) => response_sender.send(ActionResult::Status(format!("ERROR: {}", err))),
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

        return action_result;
    }
}
