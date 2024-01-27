use core::panic;
use std::{sync::mpsc::Sender, time::Duration};

use crate::{
    consts::consts::ErrorString,
    model::action::{Action, ActionResult},
};

pub enum DatabaseRequestAction {
    Request(Vec<Action>),
    Shutdown,
}

// TODO: How do we make it more ergonomic to:
//  1. Send a single action to the database and get a single action result back (without having to assert on the response)
//      - Add an additional enum that returns a single ActionResult?
//      - This could work IF we make the send_request method accept specific action types, and thus, return their corresponding response.
//      - Then we could make the internals perform the assertion
//  2. How do we make it more ergonomic to send an action say get, and know that we will get a single item
#[derive(Debug, PartialEq)]
pub enum DatabaseResponseAction {
    Response(Vec<ActionResult>),
    TransactionRollback(String),
}

impl DatabaseResponseAction {
    pub fn single_action_result(self) -> Result<ActionResult, ErrorString> {
        match self {
            DatabaseResponseAction::Response(action_results) => {
                match action_results.into_iter().next() {
                    Some(action_result) => Ok(action_result),
                    None => panic!("Expected single action result"),
                }
            }
            DatabaseResponseAction::TransactionRollback(s) => {
                // TODO: Test if this maps correctly to the GraphQL error string
                Err(format!("Transaction rollback: {}", s))
            }
        }
    }

    pub fn multiple_action_result(self) -> Result<Vec<ActionResult>, ErrorString> {
        match self {
            DatabaseResponseAction::Response(action_results) => Ok(action_results),
            DatabaseResponseAction::TransactionRollback(s) => {
                // TODO: Test if this maps correctly to the GraphQL error string
                Err(format!("Transaction rollback: {}", s))
            }
        }
    }

    pub fn new_single_response(action_result: ActionResult) -> Self {
        DatabaseResponseAction::Response(vec![action_result])
    }

    pub fn new_multiple_response(action_results: Vec<ActionResult>) -> Self {
        DatabaseResponseAction::Response(action_results)
    }
}

pub struct DatabaseRequest {
    pub response_sender: oneshot::Sender<DatabaseResponseAction>,
    pub action: DatabaseRequestAction,
}

pub struct RequestManager {
    database_sender: Sender<DatabaseRequest>,
}

impl RequestManager {
    pub fn new(database_sender: Sender<DatabaseRequest>) -> Self {
        Self { database_sender }
    }

    /// Series a series of actions (committed a transaction) to the database, the response list of responses from the database
    ///
    /// State:
    ///    - OK: All actions were successful OR the transaction was rolled back
    ///    - ERR: Database timeout
    pub fn send_request(&self, action: Vec<Action>) -> Result<DatabaseResponseAction, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<DatabaseResponseAction>();

        let request = DatabaseRequest {
            response_sender,
            action: DatabaseRequestAction::Request(action),
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        // TODO: Consider making the timeout more ergonomic. I.e. there is an error enum type for it
        //  also a lot of the calling code is just panicking on timeout, which is not ideal
        match response_receiver.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => Ok(result),
            Err(oneshot::RecvTimeoutError::Timeout) => Err("Processor was too slow".to_string()),
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }

    /// Sends a shutdown request to the database, a static shu
    pub fn send_shutdown(&self) -> Result<String, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<DatabaseResponseAction>();

        let request = DatabaseRequest {
            response_sender,
            action: DatabaseRequestAction::Shutdown,
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        match response_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => {
                if let DatabaseResponseAction::Response(mut action_results) = result {
                    if let ActionResult::SuccessStatus(s) =
                        action_results.pop().expect("single response should exist")
                    {
                        return Ok(s);
                    }
                }

                panic!("Unexpected response from database")
            }
            Err(oneshot::RecvTimeoutError::Timeout) => {
                Err("Too slow to process shutdown".to_string())
            }
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }
}
