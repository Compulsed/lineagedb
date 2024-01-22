// https://doc.rust-lang.org/nightly/unstable-book/library-features/test.html
#![feature(test)]
extern crate test;

use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
    thread,
};

use crate::{
    database::{
        database::{Database, DatabaseOptions},
        request_manager::RequestManager,
    },
    schema::GraphQLContext,
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

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Create Juniper schema
    let schema = Arc::new(create_schema());

    log::info!("starting HTTP server on port 9000.");
    log::info!("GraphiQL playground: http://localhost:9000/graphiql");

    let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
        mpsc::channel();

    thread::spawn(move || {
        let mut database = Database::new(database_receiver, DatabaseOptions::default());

        database.run();
    });

    // Start HTTP server
    HttpServer::new(move || {
        // Triggered based for N workers
        App::new()
            .app_data(Data::from(schema.clone()))
            .app_data(web::Data::new(database_sender.clone()))
            .service(graphql)
            .service(graphql_playground)
            // the graphiql UI requires CORS to be enabled
            .wrap(Cors::permissive())
            .wrap(middleware::Logger::default())
    })
    .workers(8)
    .bind(("0.0.0.0", 9000))?
    .run()
    .await
}
