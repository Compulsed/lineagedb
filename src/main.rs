use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
    thread,
};

use crate::{
    clients::{server::Server, worker::spawn_workers},
    database::{database::Database, request_manager::RequestManager},
    schema::RequestManagerContext,
};
use database::request_manager::DatabaseRequest;

mod clients;
mod consts;
mod database;
mod model;

use std::{io, sync::Arc};

use actix_cors::Cors;
use actix_web::{
    get, middleware, route,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use actix_web_lab::respond::Html;
use juniper::http::{graphiql::graphiql_source, GraphQLRequest};

mod schema;

use crate::schema::{create_schema, Schema};

/// GraphiQL playground UI
#[get("/graphiql")]
async fn graphql_playground() -> impl Responder {
    Html(graphiql_source("/graphql", None))
}

/// GraphQL endpoint
#[route("/graphql", method = "GET", method = "POST")]
async fn graphql(schema: web::Data<Schema>, data: web::Json<GraphQLRequest>) -> impl Responder {
    let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
        mpsc::channel();

    let request_manager =
        RequestManagerContext(Mutex::new(RequestManager::new(database_sender.clone())));

    thread::spawn(move || {
        let mut database = Database::new(database_receiver);

        database.run();
    });

    let user = data.execute(&schema, &request_manager).await;

    HttpResponse::Ok().json(user)
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Create Juniper schema
    let schema = Arc::new(create_schema());

    log::info!("starting HTTP server on port 9000");
    log::info!("GraphiQL playground: http://localhost:9000/graphiql");

    // Start HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(Data::from(schema.clone()))
            .service(graphql)
            .service(graphql_playground)
            // the graphiql UI requires CORS to be enabled
            .wrap(Cors::permissive())
            .wrap(middleware::Logger::default())
    })
    .workers(2)
    .bind(("127.0.0.1", 9000))?
    .run()
    .await
}
