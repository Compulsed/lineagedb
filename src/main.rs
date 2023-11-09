// use std::{
//     sync::mpsc::{self, Receiver, Sender},
//     thread,
// };

// use crate::{
//     clients::{server::Server, worker::spawn_workers},
//     database::database::Database,
// };
// use database::request_manager::DatabaseRequest;

// mod clients;
// mod consts;
// mod database;
// mod model;

// fn main() {
//     let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
//         mpsc::channel();

//     // static NTHREADS: i32 = 3;
//     // Spawns threads which generate work for the database layer
//     // spawn_workers(NTHREADS, database_sender);

//     thread::spawn(move || {
//         let server = Server::new("127.0.0.1:8080".to_string());

//         server.run(database_sender);
//     });

//     let mut database = Database::new(database_receiver);

//     database.run();
// }

//! Actix Web juniper example
//!
//! A simple example integrating juniper in Actix Web

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
async fn graphql(st: web::Data<Schema>, data: web::Json<GraphQLRequest>) -> impl Responder {
    let user = data.execute(&st, &()).await;
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
