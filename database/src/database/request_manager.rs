use core::panic;
use std::{sync::mpsc::Sender, time::Duration};
use thiserror::Error;

use crate::{
    consts::consts::{EntityId, VersionId},
    model::{
        action::{Action, ActionResult},
        person::Person,
    },
};

use super::table::{query::QueryPersonData, row::UpdatePersonData};

#[derive(Debug)]
pub enum DatabaseRequestAction {
    Request(Vec<Action>),
    Shutdown,
    SaveSnapshot,
}

impl DatabaseRequestAction {
    /// Prints complex logs in a more readable format
    pub fn log_format(&self) -> String {
        match self {
            DatabaseRequestAction::Request(actions) => {
                if actions.len() > 1 {
                    format!("{:#?}", self)
                } else {
                    format!("{:?}", self)
                }
            }
            _ => format!("{:?}", self),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DatabaseResponseAction {
    Response(Vec<ActionResult>),
    TransactionRollback(String),
    CommandError(String),
}

impl DatabaseResponseAction {
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

#[derive(Error, Debug)]
pub enum RequestManagerError {
    #[error("Database too too long to response to request")]
    DatabaseTimeout,
    #[error("Rolled back transaction: {0}")]
    TransactionRollback(String),
    #[error("Database Error Status: {0}")]
    DatbaseErrorStatus(String),
}

/// Goal of the request manager is to provide a simple interface for interacting with the database
///
/// The request manager providers the following APIs. These are sorted by the easiest to use to the most complex
/// 1. CRUD operations on a single person -- these are completely type safe
/// 2. Generic Action based API -- not type safe because you need to know what Action maps ActionResult (e.g. Action::Add maps -> ActionResult::Single)
/// 3. Transaction based API -- similar to the generic action based API, but allows you to send multiple actions to the database at once
///
/// For 2/ Can we improve the type safety of the generic action based API?
/// - Action knows what ActionResult it maps to
/// - ActionResult knows what Action it maps to
/// - Generics...?
///
/// For 3/ Can we improve the type safety of the transaction based API?
/// Might be hard because the results are a vector of ActionResult, which is a enum of all possible results (meaning we have to match on all of them)
impl RequestManager {
    pub fn new(database_sender: Sender<DatabaseRequest>) -> Self {
        Self { database_sender }
    }

    pub fn send_add(&self, person: Person) -> Result<Person, RequestManagerError> {
        let action_result = self.send_single_action(Action::Add(person))?;
        return Ok(action_result.single());
    }

    pub fn send_update(
        &self,
        id: EntityId,
        person_update: UpdatePersonData,
    ) -> Result<Person, RequestManagerError> {
        let action_result = self.send_single_action(Action::Update(id, person_update))?;
        return Ok(action_result.single());
    }

    pub fn send_get(&self, id: EntityId) -> Result<Option<Person>, RequestManagerError> {
        let action_result = self.send_single_action(Action::Get(id))?;
        return Ok(action_result.get_single());
    }

    pub fn send_get_version(
        &self,
        id: EntityId,
        version_id: VersionId,
    ) -> Result<Option<Person>, RequestManagerError> {
        let action_result = self.send_single_action(Action::GetVersion(id, version_id))?;
        return Ok(action_result.get_single());
    }

    pub fn send_list(
        &self,
        query: Option<QueryPersonData>,
    ) -> Result<Vec<Person>, RequestManagerError> {
        let action_result = self.send_single_action(Action::List(query))?;
        return Ok(action_result.list());
    }

    /// Sends a shutdown request to the database and returns the database's response
    pub fn send_shutdown_request(&self) -> Result<String, RequestManagerError> {
        let single_action_result = self
            .send_database_request(DatabaseRequestAction::Shutdown)?
            .pop()
            .expect("single a action should generate single response");

        return Ok(single_action_result.success_status());
    }

    /// Sends a single action to the database and returns a single action result
    pub fn send_single_action(&self, action: Action) -> Result<ActionResult, RequestManagerError> {
        let single_action_result = self
            .send_database_request(DatabaseRequestAction::Request(vec![action]))?
            .pop()
            .expect("single a action should generate single response");

        return Ok(single_action_result);
    }

    /// Used to create a transaction
    pub fn send_transaction(
        &self,
        actions: Vec<Action>,
    ) -> Result<Vec<ActionResult>, RequestManagerError> {
        let action_results = self.send_database_request(DatabaseRequestAction::Request(actions))?;

        return Ok(action_results);
    }

    pub fn send_database_request(
        &self,
        database_request: DatabaseRequestAction,
    ) -> Result<Vec<ActionResult>, RequestManagerError> {
        let (response_sender, response_receiver) = oneshot::channel::<DatabaseResponseAction>();

        let request = DatabaseRequest {
            response_sender,
            action: database_request,
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        // TOOD: If we panic the
        self.database_sender.send(request).unwrap();

        match response_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(DatabaseResponseAction::Response(action_response)) => Ok(action_response),
            Ok(DatabaseResponseAction::TransactionRollback(s)) => {
                Err(RequestManagerError::TransactionRollback(s))
            }
            Ok(DatabaseResponseAction::CommandError(s)) => {
                Err(RequestManagerError::DatbaseErrorStatus(s))
            }
            Err(oneshot::RecvTimeoutError::Timeout) => Err(RequestManagerError::DatabaseTimeout),
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }
}
