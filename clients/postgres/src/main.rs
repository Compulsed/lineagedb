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
use pgwire::tokio::process_socket;

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

// ---------------------------------------------------------------------------------------
// Wire handler
// ---------------------------------------------------------------------------------------

/// Speaks SQL over the Postgres wire protocol, backed by the lineagedb engine.
struct LineageHandler {
    request_manager: RequestManager,
}

impl LineageHandler {
    /// Runs an already-validated planned statement against the engine and produces a wire
    /// response. The engine's `RequestManager` is blocking, so it runs on a blocking thread.
    async fn execute(&self, statement: ast::Statement) -> Response {
        let planned = match plan(statement) {
            Ok(planned) => planned,
            Err(info) => return Response::Error(Box::new(info)),
        };

        match planned {
            Planned::ListPerson => match self.list_people().await {
                Ok(people) => Response::Query(person_query_response(people)),
                Err(info) => Response::Error(Box::new(info)),
            },
            Planned::AddPerson(person) => match self.add_person(person).await {
                // Postgres reports inserts as `INSERT <oid> <rows>`; oid is 0 for our table.
                Ok(()) => Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1)),
                Err(info) => Response::Error(Box::new(info)),
            },
        }
    }

    async fn list_people(&self) -> Result<Vec<Person>, ErrorInfo> {
        let request_manager = self.request_manager.clone();

        // The engine API is synchronous (blocks on a channel); keep it off the async runtime.
        let result = tokio::task::spawn_blocking(move || {
            request_manager
                .send_transaction(vec![Statement::List(None)], TransactionContext::default())
        })
        .await
        .map_err(|_| internal_error("query task panicked"))?;

        let statement_results = result.map_err(engine_error)?;

        match statement_results.into_iter().next() {
            Some(StatementResult::List(people)) => Ok(people),
            _ => Err(internal_error("unexpected engine result for SELECT")),
        }
    }

    async fn add_person(&self, person: Person) -> Result<(), ErrorInfo> {
        let request_manager = self.request_manager.clone();

        let result = tokio::task::spawn_blocking(move || {
            request_manager
                .send_transaction(vec![Statement::Add(person)], TransactionContext::default())
        })
        .await
        .map_err(|_| internal_error("insert task panicked"))?;

        result.map(|_| ()).map_err(engine_error)
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
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        log::info!("query: {}", query);

        let statements = match SqlParser::parse_sql(&PostgreSqlDialect {}, query) {
            Ok(statements) => statements,
            Err(e) => return Ok(vec![Response::Error(Box::new(syntax_error(e.to_string())))]),
        };

        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            responses.push(self.execute(statement).await);
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
