use std::{sync::mpsc::Sender, time::Duration};

use crate::{
    consts::consts::ErrorString,
    model::action::{Action, ActionResult},
};

pub struct DatabaseRequest {
    pub response_sender: oneshot::Sender<ActionResult>,
    pub action: Action,
}

pub struct RequestManager {
    database_sender: Sender<DatabaseRequest>,
}

impl RequestManager {
    pub fn new(database_sender: Sender<DatabaseRequest>) -> Self {
        Self { database_sender }
    }

    pub fn send_request(&self, action: Action) -> Result<ActionResult, ErrorString> {
        let (response_sender, response_receiver) = oneshot::channel::<ActionResult>();

        let request = DatabaseRequest {
            response_sender,
            action,
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
}
