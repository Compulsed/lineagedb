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
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response,
};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireResult};
use pgwire::tokio::process_socket;

use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser as SqlParser;

use database::database::commands::TransactionContext;
use database::database::database::Database;
use database::database::options::DatabaseOptions;
use database::database::request_manager::RequestManager;
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
        _ => Err(feature_not_supported(
            "only SELECT * FROM person and INSERT INTO person are supported",
        )),
    }
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

        let statement_results = result.map_err(|e| internal_error(e.to_string()))?;

        match statement_results.into_iter().next() {
            Some(StatementResult::List(people)) => Ok(people),
            _ => Err(internal_error("unexpected engine result for SELECT")),
        }
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
