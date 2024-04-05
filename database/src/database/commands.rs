use crate::model::statement::{Statement, StatementResult};

/// Database commands are how we interact with the database, they are how we ask the database to run a transaction, shutdown, etc
///
/// The majority of interactions happen via statements (e.g. add, update, remove, etc), but there are also commands that are used
/// to control the database (e.g. shutdown, snapshot, etc).
#[derive(Debug)]
pub enum DatabaseCommand {
    /// Sends a set of statements to the database and returns the results
    Transaction(Vec<Statement>),

    /// Commands that control the database
    Control(Control),
}

impl DatabaseCommand {
    /// Prints complex logs in a more readable format
    pub fn log_format(&self) -> String {
        match self {
            DatabaseCommand::Transaction(statements) => {
                if statements.len() > 1 {
                    format!("{:#?}", self)
                } else {
                    format!("{:?}", self)
                }
            }
            _ => format!("{:?}", self),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DatabaseCommandTransactionResponse {
    /// Transaction has successfully committed, returns a list of statement results
    Commit(Vec<StatementResult>),
    /// Transaction has been rolled back, returns a message for why it was rolled back
    Rollback(String),
}

impl DatabaseCommandTransactionResponse {
    /// Used to help with testing, creates a new committed result
    pub fn new_committed_single_result(result: StatementResult) -> Self {
        DatabaseCommandTransactionResponse::Commit(vec![result])
    }

    /// Used to help with testing, creates a new committed result
    pub fn new_committed_multiple(result: Vec<StatementResult>) -> Self {
        DatabaseCommandTransactionResponse::Commit(result)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DatabaseCommandControlResponse {
    /// Successfully performed the control
    Success(String),
    /// Command has failed, returns a message for why it failed
    Error(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DatabaseCommandResponse {
    DatabaseCommandTransactionResponse(DatabaseCommandTransactionResponse),
    DatabaseCommandControlResponse(DatabaseCommandControlResponse),
}

impl DatabaseCommandResponse {
    pub fn control_success(message: &str) -> Self {
        DatabaseCommandResponse::DatabaseCommandControlResponse(
            DatabaseCommandControlResponse::Success(message.to_string()),
        )
    }

    pub fn control_error(message: &str) -> Self {
        DatabaseCommandResponse::DatabaseCommandControlResponse(
            DatabaseCommandControlResponse::Error(message.to_string()),
        )
    }

    pub fn transaction_commit(results: Vec<StatementResult>) -> Self {
        DatabaseCommandResponse::DatabaseCommandTransactionResponse(
            DatabaseCommandTransactionResponse::Commit(results),
        )
    }

    pub fn transaction_rollback(message: &str) -> Self {
        DatabaseCommandResponse::DatabaseCommandTransactionResponse(
            DatabaseCommandTransactionResponse::Rollback(message.to_string()),
        )
    }
}

#[derive(Debug, PartialEq)]
pub enum ShutdownRequest {
    // Single thread that is responsible for checking that other threads shut down
    Coordinator,
    // Thread that shuts down
    Worker,
}

#[derive(Debug)]
pub enum Control {
    /// Performs a safe shutdown of the database, requests before the shutdown will be run / committed, requests after the shutdown will be ignored
    Shutdown(ShutdownRequest),
    /// Writes the current state of the database to disk, removes the need for a WAL replay on next startup
    SnapshotDatabase,
    /// Resets the database to the initial state, removes all data from the database, resets transaction ids, etc
    ResetDatabase,
    /// Pauses the database so that we can perform certain operations
    PauseDatabase(oneshot::Receiver<()>),
}

pub struct DatabaseCommandRequest {
    pub resolver: oneshot::Sender<DatabaseCommandResponse>,
    pub command: DatabaseCommand,
}
