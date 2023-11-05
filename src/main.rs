use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread,
};

use crate::{
    clients::{server::Server, worker::spawn_workers},
    database::database::Database,
};
use database::request_manager::DatabaseRequest;

mod clients;
mod consts;
mod database;
mod model;

fn main() {
    let (database_sender, database_receiver): (Sender<DatabaseRequest>, Receiver<DatabaseRequest>) =
        mpsc::channel();

    // static NTHREADS: i32 = 3;
    // Spawns threads which generate work for the database layer
    // spawn_workers(NTHREADS, database_sender);

    thread::spawn(move || {
        let server = Server::new("127.0.0.1:8080".to_string());

        server.run(database_sender);
    });

    let mut database = Database::new(database_receiver);

    database.run();
}
