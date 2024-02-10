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
    request_manager::{DatabaseRequest, RequestManager},
};
use juniper::http::{graphiql::graphiql_source, GraphQLRequest};
use std::{backtrace::Backtrace, io, panic, sync::Arc};
use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
    thread,
};

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
    database_sender: web::Data<Sender<DatabaseRequest>>,
    data: web::Json<GraphQLRequest>,
) -> impl Responder {
    let sender = database_sender.as_ref();

    let graphql_context = GraphQLContext {
        request_manager: Mutex::new(RequestManager::new(sender.clone())),
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

    panic::set_hook(Box::new(|panic_info| {
        log::error!("Panic occurred - {:?}", panic_info);
    }));

    let args = Cli::parse();

    let database_options = DatabaseOptions::default().set_data_directory(args.data);

    let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
        mpsc::channel();

    // Setup database thread
    thread::spawn(move || {
        // TOOD: We should improve how we handle panics, problems are as follows:
        //  1. The database shuts down and all the requests are lost (perhaps we should reset or process should exit?)
        //  2. The result of a panic is hitting in STD out (colorful info logs mask the plain panic message)
        let mut database = Database::new(database_receiver, database_options);

        panic!("test panic");

        database.run();
    });

    // Set up Ctrl-C handler
    let set_handler_database_sender_clone = database_sender.clone();

    ctrlc::set_handler(move || {
        let clean_up_hander_clone = set_handler_database_sender_clone.clone();

        let shutdown_response = RequestManager::new(clean_up_hander_clone)
            .send_shutdown_request()
            .expect("Should not timeout");

        log::info!("Shutting down server: {}", shutdown_response);
    })
    .expect("Error setting Ctrl-C handler");

    // Create Juniper schema
    let schema = Arc::new(create_schema());

    log::info!("Starting HTTP server on port {}.", args.port);

    log::info!(
        "GraphiQL playground: http://{}:{}/graphiql",
        args.address,
        args.port
    );

    // Start HTTP server
    HttpServer::new(move || {
        let app = App::new()
            .app_data(Data::from(schema.clone()))
            .app_data(web::Data::new(database_sender.clone()))
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
