//! Lineagedb PostgreSQL-wire server.
//!
//! Lets a standard Postgres client (e.g. `psql`) talk to the lineagedb engine. The wire
//! protocol is handled by `pgwire`; SQL is parsed by `sqlparser` and mapped onto the engine's
//! `Statement`s, run via the embedded `RequestManager`.
//!
//! Supported so far:
//!   - `SELECT * FROM person`           (M1) -> Statement::List
//!   - `INSERT INTO person ...`         (M2, next)
//!
//! Verify:
//!   cargo run -p postgres-server -- --port 5433
//!   psql "host=127.0.0.1 port=5433 user=postgres dbname=lineagedb" -c "SELECT * FROM person"

use std::sync::Arc;

mod catalog;

use async_trait::async_trait;
use clap::Parser as ClapParser;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use pgwire::tokio::process_socket;

use uuid::Uuid;

use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser as SqlParser;

use database::consts::consts::EntityId;
use database::database::commands::TransactionContext;
use database::database::database::Database;
use database::database::options::DatabaseOptions;
use database::database::request_manager::{RequestManager, RequestManagerError};
use database::model::person::Person;
use database::model::statement::{Statement, StatementResult};
use database::persistence::storage::StorageEngine;

#[derive(ClapParser, Debug)]
struct Cli {
    /// Directory the database reads/writes. Note: does not expand shell paths like `~`.
    #[clap(short, long, default_value = "data")]
    data: std::path::PathBuf,

    /// Port to listen on (Postgres default is 5432; we use 5433 to avoid colliding).
    #[clap(short, long, default_value = "5433")]
    port: u16,

    /// Address to bind.
    #[clap(short, long, default_value = "127.0.0.1")]
    address: String,
}

// ---------------------------------------------------------------------------------------
// SQL -> engine translation
// ---------------------------------------------------------------------------------------

/// A SQL statement translated into something the engine can run.
enum Planned {
    /// `SELECT * FROM person`
    ListPerson,
    /// `INSERT INTO person (...) VALUES (...)`
    AddPerson(Person),
}

fn feature_not_supported(message: impl Into<String>) -> ErrorInfo {
    // SQLSTATE 0A000 = feature_not_supported
    ErrorInfo::new("ERROR".to_string(), "0A000".to_string(), message.into())
}

fn undefined_table(name: &str) -> ErrorInfo {
    // SQLSTATE 42P01 = undefined_table
    ErrorInfo::new(
        "ERROR".to_string(),
        "42P01".to_string(),
        format!("relation \"{}\" does not exist", name),
    )
}

fn syntax_error(message: impl Into<String>) -> ErrorInfo {
    // SQLSTATE 42601 = syntax_error
    ErrorInfo::new("ERROR".to_string(), "42601".to_string(), message.into())
}

fn internal_error(message: impl Into<String>) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_string(), "XX000".to_string(), message.into())
}

fn not_null_violation(column: &str) -> ErrorInfo {
    // SQLSTATE 23502 = not_null_violation
    ErrorInfo::new(
        "ERROR".to_string(),
        "23502".to_string(),
        format!("null value in column \"{}\" violates not-null constraint", column),
    )
}

fn undefined_column(column: &str) -> ErrorInfo {
    // SQLSTATE 42703 = undefined_column
    ErrorInfo::new(
        "ERROR".to_string(),
        "42703".to_string(),
        format!("column \"{}\" of relation \"person\" does not exist", column),
    )
}

/// Maps an engine error (channel/timeout, or a logical rollback like "already exists") to a
/// Postgres error. Logical conflicts map to unique_violation; everything else is internal.
fn engine_error(error: RequestManagerError) -> ErrorInfo {
    let message = error.to_string();
    let code = if message.contains("already exists") {
        "23505" // unique_violation
    } else {
        "XX000"
    };
    ErrorInfo::new("ERROR".to_string(), code.to_string(), message)
}

/// The (only) table this server exposes mirrors the engine's single `Person` entity.
fn person_schema() -> Arc<Vec<FieldInfo>> {
    Arc::new(vec![
        FieldInfo::new("id".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("full_name".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("email".into(), None, None, Type::TEXT, FieldFormat::Text),
    ])
}

/// Extracts the (last) identifier of a table reference, e.g. `person` or `schema.person`.
fn table_name(relation: &ast::TableFactor) -> Result<String, ErrorInfo> {
    match relation {
        ast::TableFactor::Table { name, .. } => name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.clone())
            .ok_or_else(|| feature_not_supported("unsupported table reference")),
        _ => Err(feature_not_supported(
            "only a plain table reference is supported in FROM",
        )),
    }
}

fn plan(statement: ast::Statement) -> Result<Planned, ErrorInfo> {
    match statement {
        ast::Statement::Query(query) => plan_select(*query),
        ast::Statement::Insert(insert) => plan_insert(insert),
        _ => Err(feature_not_supported(
            "only SELECT * FROM person and INSERT INTO person are supported",
        )),
    }
}

/// Extracts the (last) identifier of a column reference, e.g. `email` or `person.email`.
fn column_name(name: &ast::ObjectName) -> Result<String, ErrorInfo> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .ok_or_else(|| syntax_error("invalid column reference"))
}

/// A single VALUES literal -> `Some(text)` or `None` for SQL NULL. Numbers are accepted and
/// stored as text (the person columns are all text).
fn literal_value(expr: &ast::Expr) -> Result<Option<String>, ErrorInfo> {
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::SingleQuotedString(s) => Ok(Some(s.clone())),
            ast::Value::Number(n, _) => Ok(Some(n.to_string())),
            ast::Value::Null => Ok(None),
            other => Err(feature_not_supported(format!(
                "unsupported value literal: {other:?}"
            ))),
        },
        _ => Err(feature_not_supported(
            "only literal values are supported in VALUES",
        )),
    }
}

fn plan_insert(insert: ast::Insert) -> Result<Planned, ErrorInfo> {
    let name = match &insert.table {
        ast::TableObject::TableName(object_name) => object_name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.clone())
            .ok_or_else(|| feature_not_supported("unsupported INSERT target"))?,
        _ => return Err(feature_not_supported("unsupported INSERT target")),
    };
    if name.to_lowercase() != "person" {
        return Err(undefined_table(&name));
    }

    // Column list defaults to the full person schema, in order, when omitted.
    let columns: Vec<String> = if insert.columns.is_empty() {
        vec![
            "id".to_string(),
            "full_name".to_string(),
            "email".to_string(),
        ]
    } else {
        insert
            .columns
            .iter()
            .map(column_name)
            .collect::<Result<Vec<_>, _>>()?
    };

    let source = insert
        .source
        .ok_or_else(|| feature_not_supported("INSERT requires a VALUES clause"))?;
    let values = match *source.body {
        ast::SetExpr::Values(values) => values,
        _ => {
            return Err(feature_not_supported(
                "only INSERT ... VALUES is supported (no INSERT ... SELECT)",
            ))
        }
    };

    if values.rows.len() != 1 {
        return Err(feature_not_supported("only single-row INSERT is supported"));
    }
    let row = &values.rows[0].content;
    if row.len() != columns.len() {
        return Err(syntax_error(format!(
            "INSERT has {} target columns but {} values were supplied",
            columns.len(),
            row.len()
        )));
    }

    let mut id: Option<String> = None;
    let mut full_name: Option<String> = None;
    let mut email: Option<String> = None;

    for (column, expr) in columns.iter().zip(row.iter()) {
        let value = literal_value(expr)?;
        match column.to_lowercase().as_str() {
            "id" => id = value,
            "full_name" => full_name = value,
            "email" => email = value,
            other => return Err(undefined_column(other)),
        }
    }

    // full_name is NOT NULL; id is generated when absent/NULL; email is nullable.
    let full_name = full_name.ok_or_else(|| not_null_violation("full_name"))?;
    let id = id.map(EntityId).unwrap_or_else(EntityId::new);

    Ok(Planned::AddPerson(Person {
        id,
        full_name,
        email,
    }))
}

fn plan_select(query: ast::Query) -> Result<Planned, ErrorInfo> {
    let select = match *query.body {
        ast::SetExpr::Select(select) => select,
        _ => return Err(feature_not_supported("only simple SELECT is supported")),
    };

    // Projection must be exactly `*`.
    let is_wildcard = select.projection.len() == 1
        && matches!(select.projection[0], ast::SelectItem::Wildcard(_));
    if !is_wildcard {
        return Err(feature_not_supported(
            "only SELECT * is supported (column lists are not implemented yet)",
        ));
    }

    // Exactly one table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(feature_not_supported(
            "only a single table with no joins is supported",
        ));
    }

    if select.selection.is_some() {
        return Err(feature_not_supported("WHERE is not supported yet"));
    }

    let name = table_name(&select.from[0].relation)?;
    if name.to_lowercase() != "person" {
        return Err(undefined_table(&name));
    }

    Ok(Planned::ListPerson)
}

fn in_failed_transaction() -> ErrorInfo {
    // SQLSTATE 25P02 = in_failed_sql_transaction
    ErrorInfo::new(
        "ERROR".to_string(),
        "25P02".to_string(),
        "current transaction is aborted, commands ignored until end of transaction block"
            .to_string(),
    )
}

/// Turns a single engine statement result into a wire response.
fn response_from_results(results: Vec<StatementResult>) -> Response {
    match results.into_iter().next() {
        Some(StatementResult::List(people)) => Response::Query(person_query_response(people)),
        Some(StatementResult::Single(_)) => {
            // Postgres reports inserts as `INSERT <oid> <rows>`; oid is 0 for our table.
            Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1))
        }
        other => Response::Error(Box::new(internal_error(format!(
            "unexpected engine result: {:?}",
            other
        )))),
    }
}

// ---------------------------------------------------------------------------------------
// Wire handler
// ---------------------------------------------------------------------------------------

/// Per-connection key under which the open interactive-transaction handle is stashed in the
/// pgwire client's metadata. Present iff this connection is inside a `BEGIN ... COMMIT` block.
const TX_KEY: &str = "lineagedb.transaction_handle";

/// Speaks SQL over the Postgres wire protocol, backed by the lineagedb engine.
struct LineageHandler {
    request_manager: RequestManager,
}

impl LineageHandler {
    // --- engine transaction lifecycle (each wraps the blocking RequestManager) ---

    async fn begin_engine_tx(&self) -> Result<Uuid, ErrorInfo> {
        let request_manager = self.request_manager.clone();
        tokio::task::spawn_blocking(move || request_manager.send_begin_transaction())
            .await
            .map_err(|_| internal_error("begin task panicked"))?
            .map_err(engine_error)
    }

    async fn commit_engine_tx(&self, handle: Uuid) -> Result<(), ErrorInfo> {
        let request_manager = self.request_manager.clone();
        tokio::task::spawn_blocking(move || request_manager.send_commit_transaction(handle))
            .await
            .map_err(|_| internal_error("commit task panicked"))?
            .map_err(engine_error)
    }

    async fn rollback_engine_tx(&self, handle: Uuid) {
        let request_manager = self.request_manager.clone();
        // Best effort: a rollback discards buffered writes, so failures aren't fatal.
        let _ =
            tokio::task::spawn_blocking(move || request_manager.send_rollback_transaction(handle))
                .await;
    }

    /// Runs a data statement (SELECT/INSERT) either within the open transaction (`handle`) or
    /// as a one-shot autocommit transaction. The engine API is blocking, so it runs on a
    /// blocking thread.
    async fn run_data_statement(&self, planned: Planned, handle: Option<Uuid>) -> Response {
        let engine_statement = match planned {
            Planned::ListPerson => Statement::List(None),
            Planned::AddPerson(person) => Statement::Add(person),
        };

        let request_manager = self.request_manager.clone();
        let join = tokio::task::spawn_blocking(move || match handle {
            Some(handle) => request_manager.send_transaction_statements(handle, vec![engine_statement]),
            None => {
                request_manager.send_transaction(vec![engine_statement], TransactionContext::default())
            }
        })
        .await;

        match join {
            Ok(Ok(results)) => response_from_results(results),
            Ok(Err(e)) => Response::Error(Box::new(engine_error(e))),
            Err(_) => Response::Error(Box::new(internal_error("statement task panicked"))),
        }
    }

    /// Processes one parsed statement, threading the per-connection transaction state:
    /// `handle` is the open interactive transaction (if any) and `failed` whether the current
    /// transaction block is aborted.
    async fn run_statement(
        &self,
        statement: ast::Statement,
        handle: &mut Option<Uuid>,
        failed: &mut bool,
    ) -> Response {
        match &statement {
            ast::Statement::StartTransaction { .. } => {
                if handle.is_some() {
                    log::warn!("BEGIN issued inside a transaction; ignoring");
                    return Response::TransactionStart(Tag::new("BEGIN"));
                }
                return match self.begin_engine_tx().await {
                    Ok(new_handle) => {
                        *handle = Some(new_handle);
                        Response::TransactionStart(Tag::new("BEGIN"))
                    }
                    Err(info) => Response::Error(Box::new(info)),
                };
            }
            ast::Statement::Commit { .. } => {
                let Some(open) = handle.take() else {
                    log::warn!("COMMIT with no transaction in progress");
                    return Response::TransactionEnd(Tag::new("COMMIT"));
                };
                if *failed {
                    // Committing an aborted transaction rolls it back (matches Postgres).
                    self.rollback_engine_tx(open).await;
                    *failed = false;
                    return Response::TransactionEnd(Tag::new("ROLLBACK"));
                }
                return match self.commit_engine_tx(open).await {
                    Ok(()) => Response::TransactionEnd(Tag::new("COMMIT")),
                    Err(info) => Response::Error(Box::new(info)),
                };
            }
            ast::Statement::Rollback { .. } => {
                if let Some(open) = handle.take() {
                    self.rollback_engine_tx(open).await;
                }
                *failed = false;
                return Response::TransactionEnd(Tag::new("ROLLBACK"));
            }
            _ => {}
        }

        // Inside an aborted transaction block, reject everything until COMMIT/ROLLBACK.
        if *failed {
            return Response::Error(Box::new(in_failed_transaction()));
        }

        let planned = match plan(statement) {
            Ok(planned) => planned,
            Err(info) => {
                if handle.is_some() {
                    *failed = true;
                }
                return Response::Error(Box::new(info));
            }
        };

        let response = self.run_data_statement(planned, *handle).await;
        if handle.is_some() && matches!(response, Response::Error(_)) {
            *failed = true;
        }
        response
    }
}

fn person_query_response(people: Vec<Person>) -> QueryResponse {
    let schema = person_schema();
    let mut encoder = DataRowEncoder::new(schema.clone());

    let rows = stream::iter(people).map(move |person| {
        encoder.encode_field(&person.id.to_string())?;
        encoder.encode_field(&person.full_name)?;
        encoder.encode_field(&person.email)?;
        Ok(encoder.take_row())
    });

    QueryResponse::new(schema, rows)
}

// No authentication: accept any user, no password (uses the default no-op `post_startup`).
impl NoopStartupHandler for LineageHandler {}

#[async_trait]
impl SimpleQueryHandler for LineageHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        log::info!("query: {}", query);

        // System-catalog / introspection queries (from GUI clients) are answered with canned
        // responses so the `person` table shows up in their schema browser.
        if let Some(responses) = catalog::intercept(query) {
            return Ok(responses);
        }

        let statements = match SqlParser::parse_sql(&PostgreSqlDialect {}, query) {
            Ok(statements) => statements,
            Err(e) => return Ok(vec![Response::Error(Box::new(syntax_error(e.to_string())))]),
        };

        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Per-connection transaction state. The open engine handle is stashed in the client's
        // metadata so it survives across messages (BEGIN and COMMIT arrive separately); the
        // aborted-block flag is read from pgwire's transaction status (set on the prior reply).
        let mut handle: Option<Uuid> = client
            .metadata()
            .get(TX_KEY)
            .and_then(|raw| Uuid::parse_str(raw).ok());
        let mut failed = matches!(client.transaction_status(), TransactionStatus::Error);

        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            let response = self.run_statement(statement, &mut handle, &mut failed).await;
            let is_error = matches!(response, Response::Error(_));
            responses.push(response);
            if is_error {
                // A simple-query message aborts at the first error (Postgres semantics).
                break;
            }
        }

        // Persist the (possibly changed) transaction handle back onto the connection.
        match &handle {
            Some(open) => {
                client
                    .metadata_mut()
                    .insert(TX_KEY.to_string(), open.to_string());
            }
            None => {
                client.metadata_mut().remove(TX_KEY);
            }
        }

        Ok(responses)
    }
}

struct LineageHandlerFactory {
    handler: Arc<LineageHandler>,
}

impl PgWireServerHandlers for LineageHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }
    // extended_query_handler / copy_handler default to pgwire's no-op handlers.
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Cli::parse();

    // Fresh, non-restoring database for now (configurable persistence is a follow-up).
    let options = DatabaseOptions::default()
        .set_storage_engine(StorageEngine::File(args.data.clone()))
        .set_restore(false);
    let request_manager = Database::new(options).run();

    let factory = Arc::new(LineageHandlerFactory {
        handler: Arc::new(LineageHandler { request_manager }),
    });

    let bind = format!("{}:{}", args.address, args.port);
    let listener = TcpListener::bind(&bind).await.expect("should bind");
    log::info!("Postgres wire server listening on {}", bind);

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                log::info!("connection from {}", peer);
                let factory = factory.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, factory).await {
                        log::error!("connection error: {}", e);
                    }
                });
            }
            Err(e) => log::error!("accept error: {}", e),
        }
    }
}
