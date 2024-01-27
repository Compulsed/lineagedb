use std::{sync::mpsc::Sender, time::Duration};

use crate::{
    consts::consts::ErrorString,
    model::action::{Action, ActionResult},
};

pub enum DatabaseRequestAction {
    Request(Vec<Action>),
    Shutdown,
}

pub struct DatabaseRequest {
    pub response_sender: oneshot::Sender<Vec<ActionResult>>,
    pub action: DatabaseRequestAction,
}

pub struct RequestManager {
    database_sender: Sender<DatabaseRequest>,
}

impl RequestManager {
    pub fn new(database_sender: Sender<DatabaseRequest>) -> Self {
        Self { database_sender }
    }

    pub fn send_request(&self, action: Vec<Action>) -> Result<Vec<ActionResult>, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<Vec<ActionResult>>();

        let request = DatabaseRequest {
            response_sender,
            action: DatabaseRequestAction::Request(action),
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        match response_receiver.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => Ok(result),
            Err(oneshot::RecvTimeoutError::Timeout) => Err("Processor was too slow".to_string()),
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }

    pub fn send_shutdown(&self) -> Result<Vec<ActionResult>, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<Vec<ActionResult>>();

        let request = DatabaseRequest {
            response_sender,
            action: DatabaseRequestAction::Shutdown,
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        match response_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => Ok(result),
            Err(oneshot::RecvTimeoutError::Timeout) => {
                Err("Too slow to process shutdown".to_string())
            }
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }
}
