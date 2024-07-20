use core::panic;
use rand::{seq::SliceRandom, thread_rng};
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
        DatabaseCommandResponse, DatabaseCommandTransactionResponse, ShutdownRequest,
    },
    table::{query::QueryPersonData, row::UpdatePersonData},
};

/// Converts the database command hierarchy into a simple string, this is an easy interface to work with
#[derive(Error, Debug)]
pub enum RequestManagerError {
    /// From issues dealing with the channel
    #[error("Database too too long to response to request")]
    DatabaseTimeout,

    /// From transaction rollbacks
    #[error("Rolled back transaction: {0}")]
    TransactionRollback(String),

    /// From transaction rollbacks
    #[error("Transaction status: {0}")]
    TransactionStatus(String),

    /// From control commands
    #[error("Database Error Status: {0}")]
    DatabaseErrorStatus(String),
}

#[derive(Clone)]
pub struct RequestManager {
    database_sender: Vec<flume::Sender<DatabaseCommandRequest>>,
}

/// Goal of the request manager is to provide a simple interface for interacting with the database
///
/// The request manager has two categories of methods:
/// 1. The level of abstraction (highest to lowest): Entity, Statement or Command
/// 2. The level of synchronicity: Async (Task) or Sync
///
/// Sync:
/// - Sync methods are the simplest to use, they send to the request and immediately put the thread to sleep until the response is received
/// - Async methods are more complex to use, they send the request and return a future that can be awaited on
///
/// Abstraction level:
/// - Entity, Provides Add, Update, Get, GetVersion, List methods for interacting with the database
/// - Statement, allows sending a single statement or a transaction to the database
/// - Command, allows sending control commands to the database, e.g. shutdown, reset, snapshot
impl RequestManager {
    pub fn new(database_sender: Vec<flume::Sender<DatabaseCommandRequest>>) -> Self {
        Self { database_sender }
    }

    fn get_sender(&self) -> &flume::Sender<DatabaseCommandRequest> {
        let mut rng = thread_rng();

        // return &self.database_sender[0];

        self.database_sender
            .choose(&mut rng)
            .expect("Should have at least one sender")
    }

    // -- Entity Methods: Async Task --
    pub fn send_add_task(&self, person: Person) -> TaskAddResponse {
        TaskAddResponse::send(self, person)
    }

    pub fn send_update_task(
        &self,
        id: EntityId,
        person_update: UpdatePersonData,
    ) -> TaskUpdateResponse {
        TaskUpdateResponse::send(self, id, person_update)
    }

    pub fn send_get_task(&self, id: EntityId) -> TaskGetResponse {
        TaskGetResponse::send(self, id)
    }

    pub fn send_get_version_task(
        &self,
        id: EntityId,
        version_id: VersionId,
    ) -> TaskGetVersionResponse {
        TaskGetVersionResponse::send(self, id, version_id)
    }

    pub fn send_list_task(&self, query: Option<QueryPersonData>) -> TaskListResponse {
        TaskListResponse::send(self, query)
    }

    // -- Entity Methods: Sync --
    pub fn send_add(&self, person: Person) -> Result<Person, RequestManagerError> {
        self.send_add_task(person).get()
    }

    pub fn send_update(
        &self,
        id: EntityId,
        person_update: UpdatePersonData,
    ) -> Result<Person, RequestManagerError> {
        self.send_update_task(id, person_update).get()
    }

    pub fn send_get(&self, id: EntityId) -> Result<Option<Person>, RequestManagerError> {
        self.send_get_task(id).get()
    }

    pub fn send_get_version(
        &self,
        id: EntityId,
        version_id: VersionId,
    ) -> Result<Option<Person>, RequestManagerError> {
        self.send_get_version_task(id, version_id).get()
    }

    pub fn send_list(
        &self,
        query: Option<QueryPersonData>,
    ) -> Result<Vec<Person>, RequestManagerError> {
        self.send_list_task(query).get()
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
    pub fn send_transaction_task(&self, statements: Vec<Statement>) -> TaskStatementResponse {
        TaskStatementResponse::send(self, statements)
    }

    pub fn send_transaction(
        &self,
        statements: Vec<Statement>,
    ) -> Result<Vec<StatementResult>, RequestManagerError> {
        self.send_transaction_task(statements).get()
    }

    // -- Control Methods --

    /// Sends a shutdown request to the database and returns the database's response
    pub fn send_shutdown_request(
        &self,
        request: ShutdownRequest,
    ) -> Result<String, RequestManagerError> {
        return self.send_control(Control::Shutdown(request));
    }

    pub fn send_pause_request(
        &self,
        resume: flume::Receiver<()>,
    ) -> Result<String, RequestManagerError> {
        return self.send_control(Control::PauseDatabase(resume));
    }

    /// Resets the database to a clean state
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
        let send_result = self.get_sender().send(request);

        if let Err(e) = send_result {
            log::error!("{}", e);

            // The likely result of this error is that the database has shut down, which will
            //  result in the database sender channel being closed. The other possible error is that
            //  the channel has been overloaded, though we do not bound
            return Err(RequestManagerError::DatabaseErrorStatus(
                "Request failed, this is likely due to the database being shutdown".to_string(),
            ));
        }

        // If the database is large it can take > 30 seconds to reset
        let response = response_receiver.recv_timeout(Duration::from_secs(60));

        map_response(response)
    }

    #[allow(dead_code)]
    fn send_database_command_task(&self, database_request: DatabaseCommand) -> TaskCommandResponse {
        let (response_sender, response_receiver) = oneshot::channel::<DatabaseCommandResponse>();

        let request = DatabaseCommandRequest {
            resolver: response_sender,
            command: database_request,
        };

        self.get_sender().send(request).unwrap();

        TaskCommandResponse::send(response_receiver)
    }
}

fn map_response(
    response: Result<DatabaseCommandResponse, oneshot::RecvTimeoutError>,
) -> Result<DatabaseCommandResponse, RequestManagerError> {
    match response {
        // Transaction commands
        Ok(DatabaseCommandResponse::DatabaseCommandTransactionResponse(transaction_response)) => {
            match transaction_response {
                DatabaseCommandTransactionResponse::Commit(statement_result) => {
                    Ok(DatabaseCommandResponse::DatabaseCommandTransactionResponse(
                        DatabaseCommandTransactionResponse::Commit(statement_result),
                    ))
                }
                DatabaseCommandTransactionResponse::Rollback(s) => {
                    Err(RequestManagerError::TransactionRollback(s))
                }
                DatabaseCommandTransactionResponse::Status(s) => {
                    Err(RequestManagerError::TransactionStatus(s))
                }
            }
        }
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

fn send_request(
    request_manager: &RequestManager,
    statement: Vec<Statement>,
) -> oneshot::Receiver<DatabaseCommandResponse> {
    let (response_sender, response_receiver) = oneshot::channel::<DatabaseCommandResponse>();

    let request = DatabaseCommandRequest {
        resolver: response_sender,
        command: DatabaseCommand::Transaction(statement),
    };

    request_manager.get_sender().send(request).unwrap();

    response_receiver
}

fn get_statement(
    response: &oneshot::Receiver<DatabaseCommandResponse>,
) -> Result<Vec<StatementResult>, RequestManagerError> {
    let response = response.recv_timeout(Duration::from_secs(30));

    let command_result = map_response(response)?;

    match command_result {
        DatabaseCommandResponse::DatabaseCommandTransactionResponse(
            DatabaseCommandTransactionResponse::Commit(action_results),
        ) => Ok(action_results),
        _ => panic!("Transaction commands should always return a commit or rollback"),
    }
}

pub trait Wait {
    fn wait(&self);
}

pub struct TaskCommandResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskCommandResponse {
    pub fn send(response: oneshot::Receiver<DatabaseCommandResponse>) -> Self {
        Self { response }
    }

    pub fn get(&self) -> Result<DatabaseCommandResponse, RequestManagerError> {
        let response = self.response.recv_timeout(Duration::from_secs(30));

        map_response(response)
    }
}

impl Wait for TaskCommandResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

pub struct TaskStatementResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskStatementResponse {
    pub fn send(request_manager: &RequestManager, statement: Vec<Statement>) -> Self {
        Self {
            response: send_request(request_manager, statement),
        }
    }

    pub fn get(&self) -> Result<Vec<StatementResult>, RequestManagerError> {
        get_statement(&self.response)
    }
}

impl Wait for TaskStatementResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

pub struct TaskAddResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskAddResponse {
    pub fn send(request_manager: &RequestManager, person: Person) -> Self {
        Self {
            response: send_request(request_manager, vec![Statement::Add(person)]),
        }
    }

    pub fn get(&self) -> Result<Person, RequestManagerError> {
        get_statement(&self.response).and_then(|mut action_result| {
            Ok(action_result
                .pop()
                .expect("single a statement should generate single response")
                .single())
        })
    }
}

impl Wait for TaskAddResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

pub struct TaskUpdateResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskUpdateResponse {
    fn send(
        request_manager: &RequestManager,
        id: EntityId,
        person_update: UpdatePersonData,
    ) -> Self {
        Self {
            response: send_request(request_manager, vec![Statement::Update(id, person_update)]),
        }
    }

    pub fn get(&self) -> Result<Person, RequestManagerError> {
        get_statement(&self.response).and_then(|mut action_result| {
            Ok(action_result
                .pop()
                .expect("single a statement should generate single response")
                .single())
        })
    }
}

impl Wait for TaskUpdateResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

pub struct TaskGetResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskGetResponse {
    pub fn send(request_manager: &RequestManager, id: EntityId) -> Self {
        Self {
            response: send_request(request_manager, vec![Statement::Get(id)]),
        }
    }

    pub fn get(&self) -> Result<Option<Person>, RequestManagerError> {
        get_statement(&self.response).and_then(|mut action_result| {
            Ok(action_result
                .pop()
                .expect("single a statement should generate single response")
                .get_single())
        })
    }
}

pub struct TaskGetVersionResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskGetVersionResponse {
    pub fn send(request_manager: &RequestManager, id: EntityId, version_id: VersionId) -> Self {
        Self {
            response: send_request(request_manager, vec![Statement::GetVersion(id, version_id)]),
        }
    }

    pub fn get(&self) -> Result<Option<Person>, RequestManagerError> {
        get_statement(&self.response).and_then(|mut action_result| {
            Ok(action_result
                .pop()
                .expect("single a statement should generate single response")
                .get_single())
        })
    }
}

impl Wait for TaskGetVersionResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

pub struct TaskListResponse {
    response: oneshot::Receiver<DatabaseCommandResponse>,
}

impl TaskListResponse {
    pub fn send(request_manager: &RequestManager, query: Option<QueryPersonData>) -> Self {
        Self {
            response: send_request(request_manager, vec![Statement::List(query)]),
        }
    }

    pub fn get(&self) -> Result<Vec<Person>, RequestManagerError> {
        get_statement(&self.response).and_then(|mut action_result| {
            Ok(action_result
                .pop()
                .expect("single a statement should generate single response")
                .list())
        })
    }
}

impl Wait for TaskListResponse {
    fn wait(&self) {
        self.get().expect("Should not timeout");
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        consts::consts::EntityId,
        database::{
            commands::{DatabaseCommand, DatabaseCommandResponse},
            database::Database,
        },
        model::{
            person::Person,
            statement::{Statement, StatementResult},
        },
    };

    #[test]
    fn sync() {
        let request_manager = Database::new_test().run(1);

        let action_result = request_manager
            .send_single_statement(Statement::Add(Person {
                id: EntityId::new(),
                full_name: "Test".to_string(),
                email: Some(Uuid::new_v4().to_string()),
            }))
            .expect("Should not timeout");

        assert_eq!(action_result.single().full_name, "Test");
    }

    #[test]
    fn task_command() {
        let request_manager = Database::new_test().run(1);

        let person = Person {
            id: EntityId::new(),
            full_name: "Test".to_string(),
            email: Some(Uuid::new_v4().to_string()),
        };

        let task = request_manager.send_database_command_task(DatabaseCommand::Transaction(vec![
            Statement::Add(person.clone()),
        ]));

        let action_result = task.get().expect("Should not timeout");

        assert_eq!(
            action_result,
            DatabaseCommandResponse::transaction_commit(vec![StatementResult::Single(person)])
        );
    }

    #[test]
    fn task_statement() {
        let request_manager = Database::new_test().run(1);

        let task = request_manager.send_transaction_task(vec![Statement::Add(Person {
            id: EntityId::new(),
            full_name: "Test".to_string(),
            email: Some(Uuid::new_v4().to_string()),
        })]);

        let action_result = task.get().expect("Should not timeout");

        assert_eq!(action_result.len(), 1);
    }

    #[test]
    fn task_add() {
        let request_manager = Database::new_test().run(1);

        let person = Person {
            id: EntityId::new(),
            full_name: "Test".to_string(),
            email: Some(Uuid::new_v4().to_string()),
        };

        let added_person = request_manager
            .send_add_task(person.clone())
            .get()
            .expect("should not timeout");

        assert_eq!(added_person, person);
    }

    mod with_storage {
        use std::path::PathBuf;

        use crate::{
            database::{commands::ShutdownRequest, database::DatabaseOptions},
            persistence::{
                storage::{
                    dynamodb::DynamoOptions, postgres::PostgresOptions, s3::S3Options,
                    StorageEngine,
                },
                transaction::{TransactionFileWriteMode, TransactionWriteMode},
            },
        };

        use super::*;

        #[test_log::test]
        fn with_storage_file() {
            let database_dir: PathBuf = ["/", "tmp", "lineagedb", &Uuid::new_v4().to_string()]
                .iter()
                .collect();

            test_restore_with_engine(StorageEngine::File(database_dir));
        }

        #[test]
        #[ignore = "CI will not be set up for running Postgres"]
        fn with_storage_pg() {
            test_restore_with_engine(StorageEngine::Postgres(PostgresOptions::new_test()));
        }

        #[test]
        #[ignore = "CI will not be set up for running S3"]
        fn with_storage_s3() {
            test_restore_with_engine(StorageEngine::S3(S3Options::new_test()));
        }

        #[test]
        #[ignore = "CI will not be set up for running DynamoDB"]
        fn with_storage_ddb() {
            test_restore_with_engine(StorageEngine::DynamoDB(DynamoOptions::new_test()));
        }

        fn test_restore_with_engine(engine: StorageEngine) {
            let options_initial = DatabaseOptions::default()
                .set_storage_engine(engine.clone())
                .set_restore(false)
                .set_sync_file_write(TransactionWriteMode::File(TransactionFileWriteMode::Sync));

            let request_manager_initial = Database::new(options_initial).run(1);

            let expected_person = Person {
                id: EntityId::new(),
                full_name: "Test".to_string(),
                email: Some(Uuid::new_v4().to_string()),
            };

            // Write #1
            let actual_person = request_manager_initial
                .send_add_task(expected_person.clone())
                .get()
                .expect("should not timeout");

            // Write #2 -- just used to ensure we are correctly batching multiple
            //  writes together in the WAL. I suspect this would be better as a more
            //  isolated test
            let _ = request_manager_initial
                .send_add_task(Person {
                    id: EntityId::new(),
                    full_name: "Test".to_string(),
                    email: Some(Uuid::new_v4().to_string()),
                })
                .get()
                .expect("should not timeout");

            assert_eq!(actual_person, expected_person);

            let _ = request_manager_initial
                .send_shutdown_request(ShutdownRequest::Coordinator)
                .unwrap();

            // // -- Restore from disk

            let options_restore = DatabaseOptions::default()
                .set_storage_engine(engine)
                .set_restore(true)
                .set_sync_file_write(TransactionWriteMode::File(TransactionFileWriteMode::Sync));

            let request_manager_restored = Database::new(options_restore).run(1);

            let actual_person_restored = request_manager_restored
                .send_get_task(expected_person.clone().id)
                .get()
                .expect("should not timeout");

            assert_eq!(actual_person_restored, Some(expected_person));

            // // Gracefully shutdown
            let _ = request_manager_restored
                .send_shutdown_request(ShutdownRequest::Coordinator)
                .unwrap();
        }
    }
}
