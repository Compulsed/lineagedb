//! Lineagedb PostgreSQL-wire server.
//!
//! Lets a standard Postgres client (e.g. `psql`) talk to the lineagedb engine. The wire
//! protocol is handled by `pgwire`; SQL is parsed by `sqlparser` (added in M1) and mapped to
//! the engine's `Statement`s, run via the embedded `RequestManager`.
//!
//! M0 (this file): scaffold + handshake only. A stub query handler echoes any query back as
//! a single-row result, which is enough to prove the connection/handshake and the simple
//! query path end to end. SQL parsing (M1) and INSERT (M2) build on top.
//!
//! Verify:
//!   cargo run -p postgres-server -- --port 5433
//!   psql "host=127.0.0.1 port=5433 user=postgres dbname=lineagedb" -c "SELECT 1"

use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

use database::database::database::Database;
use database::database::options::DatabaseOptions;
use database::database::request_manager::RequestManager;
use database::persistence::storage::StorageEngine;

#[derive(Parser, Debug)]
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

/// Speaks SQL over the Postgres wire protocol, backed by the lineagedb engine.
struct LineageHandler {
    // Wired in at M1, when SELECT/INSERT start hitting the engine.
    #[allow(dead_code)]
    request_manager: RequestManager,
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

        // M0 stub: echo the query back as one row. Proves the handshake + simple-query path
        // before any SQL parsing or engine wiring (those land in M1/M2).
        let schema = Arc::new(vec![FieldInfo::new(
            "message".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )]);

        let mut encoder = DataRowEncoder::new(schema.clone());
        let rows = vec![format!("lineagedb received: {}", query)];
        let row_stream = stream::iter(rows).map(move |message| {
            encoder.encode_field(&Some(message))?;
            Ok(encoder.take_row())
        });

        Ok(vec![Response::Query(QueryResponse::new(schema, row_stream))])
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
