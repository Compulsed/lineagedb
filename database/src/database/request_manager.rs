use core::panic;
use std::time::Duration;
use thiserror::Error;

use crate::{
    consts::consts::{EntityId, VersionId},
    model::{
        person::Person,
        statement::{Statement, StatementResult},
    },
};

use super::{
    commands::{
        Control, DatabaseCommand, DatabaseCommandControlResponse, DatabaseCommandRequest,
        DatabaseCommandResponse, DatabaseCommandTransactionResponse,
    },
    table::{query::QueryPersonData, row::UpdatePersonData},
};

pub struct RequestManager {
    database_sender: flume::Sender<DatabaseCommandRequest>,
}

/// Converts the database command hierarchy into a simple string, this is an easy interface to work with
#[derive(Error, Debug)]
pub enum RequestManagerError {
    /// From issues dealing with the channel
    #[error("Database too too long to response to request")]
    DatabaseTimeout,

    /// From transaction rollbacks
    #[error("Rolled back transaction: {0}")]
    TransactionRollback(String),

    /// From control commands
    #[error("Database Error Status: {0}")]
    DatabaseErrorStatus(String),
}

/// Goal of the request manager is to provide a simple interface for interacting with the database
impl RequestManager {
    pub fn new(database_sender: flume::Sender<DatabaseCommandRequest>) -> Self {
        Self { database_sender }
    }

    // -- Transaction Methods --

    pub fn send_add(&self, person: Person) -> Result<Person, RequestManagerError> {
        let action_result = self.send_single_statement(Statement::Add(person))?;
        return Ok(action_result.single());
    }

    pub fn send_update(
        &self,
        id: EntityId,
        person_update: UpdatePersonData,
    ) -> Result<Person, RequestManagerError> {
        let action_result = self.send_single_statement(Statement::Update(id, person_update))?;
        return Ok(action_result.single());
    }

    pub fn send_get(&self, id: EntityId) -> Result<Option<Person>, RequestManagerError> {
        let action_result = self.send_single_statement(Statement::Get(id))?;
        return Ok(action_result.get_single());
    }

    pub fn send_get_version(
        &self,
        id: EntityId,
        version_id: VersionId,
    ) -> Result<Option<Person>, RequestManagerError> {
        let action_result = self.send_single_statement(Statement::GetVersion(id, version_id))?;
        return Ok(action_result.get_single());
    }

    pub fn send_list(
        &self,
        query: Option<QueryPersonData>,
    ) -> Result<Vec<Person>, RequestManagerError> {
        let action_result = self.send_single_statement(Statement::List(query))?;
        return Ok(action_result.list());
    }

    /// Convenience method to send a single statement to the database and returns the response
    ///
    /// The reason this method exists is because it's a common pattern to send a single statement to the database and get a single response back
    pub fn send_single_statement(
        &self,
        statement: Statement,
    ) -> Result<StatementResult, RequestManagerError> {
        let single_statement_result = self
            .send_transaction(vec![statement])?
            .pop()
            .expect("single a statement should generate single response");

        return Ok(single_statement_result);
    }

    /// Sends a set of statements to the database and returns the response
    pub fn send_transaction(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<StatementResult>, RequestManagerError> {
        let command_result =
            self.send_database_command(DatabaseCommand::Transaction(statements))?;

        match command_result {
            DatabaseCommandResponse::DatabaseCommandTransactionResponse(
                DatabaseCommandTransactionResponse::Commit(action_results),
            ) => Ok(action_results),
            _ => panic!("Transaction commands should always return a commit or rollback"),
        }
    }

    // -- Control Methods --

    /// Sends a shutdown request to the database and returns the database's response
    pub fn send_shutdown_request(&self) -> Result<String, RequestManagerError> {
        return self.send_control(Control::Shutdown);
    }

    pub fn send_reset_request(&self) -> Result<String, RequestManagerError> {
        return self.send_control(Control::ResetDatabase);
    }

    pub fn send_snapshot_request(&self) -> Result<String, RequestManagerError> {
        return self.send_control(Control::SnapshotDatabase);
    }

    // -- Internal methods --

    fn send_control(&self, control: Control) -> Result<String, RequestManagerError> {
        let command_result = self.send_database_command(DatabaseCommand::Control(control))?;

        match command_result {
            DatabaseCommandResponse::DatabaseCommandControlResponse(
                DatabaseCommandControlResponse::Success(s),
            ) => Ok(s),
            _ => panic!("Controls should always return a success or error status"),
        }
    }

    /// Generic method to send a transaction or control command to the database and returns the response. For type safety this method should not be used
    /// use the more specific methods instead. These specific methods ensure that the correct response is returned
    fn send_database_command(
        &self,
        database_request: DatabaseCommand,
    ) -> Result<DatabaseCommandResponse, RequestManagerError> {
        let (response_sender, response_receiver) = oneshot::channel::<DatabaseCommandResponse>();

        let request = DatabaseCommandRequest {
            resolver: response_sender,
            command: database_request,
        };

        // Sends the request to the database worker, database will response
        //  on the response_receiver once it's finished processing it's request
        self.database_sender.send(request).unwrap();

        let response = response_receiver.recv_timeout(Duration::from_secs(5));

        match response {
            // Transaction commands
            Ok(DatabaseCommandResponse::DatabaseCommandTransactionResponse(
                transaction_response,
            )) => match transaction_response {
                DatabaseCommandTransactionResponse::Commit(statement_result) => {
                    Ok(DatabaseCommandResponse::DatabaseCommandTransactionResponse(
                        DatabaseCommandTransactionResponse::Commit(statement_result),
                    ))
                }
                DatabaseCommandTransactionResponse::Rollback(s) => {
                    Err(RequestManagerError::TransactionRollback(s))
                }
            },
            // Control commands
            Ok(DatabaseCommandResponse::DatabaseCommandControlResponse(control_response)) => {
                match control_response {
                    DatabaseCommandControlResponse::Success(s) => {
                        Ok(DatabaseCommandResponse::DatabaseCommandControlResponse(
                            DatabaseCommandControlResponse::Success(s),
                        ))
                    }
                    DatabaseCommandControlResponse::Error(s) => {
                        Err(RequestManagerError::DatabaseErrorStatus(s))
                    }
                }
            }
            // Issues with the channel
            Err(oneshot::RecvTimeoutError::Timeout) => Err(RequestManagerError::DatabaseTimeout),
            Err(oneshot::RecvTimeoutError::Disconnected) => panic!("Processor exited"),
        }
    }
}
