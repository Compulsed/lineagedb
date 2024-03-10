use actix_cors::Cors;
use actix_web::{
    get,
    middleware::{self, Condition},
    route,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use actix_web_lab::respond::Html;
use clap::Parser;
use database::database::{
    database::{Database, DatabaseOptions},
    request_manager::RequestManager,
};
use juniper::http::{graphiql::graphiql_source, GraphQLRequest};
use std::sync::Mutex;
use std::{io, sync::Arc};

use crate::schema::{create_schema, GraphQLContext, Schema};

mod schema;

/// GraphiQL playground UI
#[get("/graphiql")]
async fn graphql_playground() -> impl Responder {
    Html(graphiql_source("/graphql", None))
}

/// GraphQL endpoint -- triggered once per request
#[route("/graphql", method = "GET", method = "POST")]
async fn graphql(
    schema: web::Data<Schema>,
    request_manager_ref: web::Data<RequestManager>,
    data: web::Json<GraphQLRequest>,
) -> impl Responder {
    let request_manager = request_manager_ref.as_ref();

    let graphql_context = GraphQLContext {
        request_manager: Mutex::new(request_manager.clone()),
    };

    let user = data.execute(&schema, &graphql_context).await;

    HttpResponse::Ok().json(user)
}

/// 📀 Lineagedb GraphQL Server, provides a simple GraphQL interface for interacting with the database
#[derive(Parser, Debug)]
struct Cli {
    /// Location of the database. Reads / writes to this directory. Note: Does not support shell paths, e.g. ~
    #[clap(short, long, default_value = "data")]
    data: std::path::PathBuf,

    /// Port the graphql server will run on
    #[clap(short, long, default_value = "9000")]
    port: u16,

    /// Address the graphql server will run on
    #[clap(short, long, default_value = "0.0.0.0")]
    address: String,

    /// Address the graphql server will run on
    #[clap(long)]
    log_http: bool,

    #[clap(long, default_value_t = 2)]
    http_workers: usize,
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Cli::parse();

    let database_options = DatabaseOptions::default().set_data_directory(args.data);

    let request_manager = Database::new(database_options).run(5);

    // Set up Ctrl-C handler
    let set_handler_database_sender_clone = request_manager.clone();

    ctrlc::set_handler(move || {
        let shutdown_response = set_handler_database_sender_clone
            .clone()
            .send_shutdown_request()
            .expect("Should not timeout");

        log::info!("Shutting down server: {}", shutdown_response);
    })
    .expect("Error setting Ctrl-C handler");

    // Create Juniper schema
    let schema = Arc::new(create_schema());

    log::info!("starting HTTP server on port {}.", args.port);

    log::info!(
        "GraphiQL playground: http://{}:{}/graphiql",
        args.address,
        args.port
    );

    // Start HTTP server
    HttpServer::new(move || {
        let app = App::new()
            .app_data(Data::from(schema.clone()))
            .app_data(web::Data::new(request_manager.clone()))
            .service(graphql)
            .service(graphql_playground)
            .wrap(Cors::permissive())
            .wrap(Condition::new(args.log_http, middleware::Logger::default()));

        app
    })
    .workers(args.http_workers)
    .bind((args.address, args.port))?
    .run()
    .await
}
