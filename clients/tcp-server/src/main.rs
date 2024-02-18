use std::io::{Read, Write};
use std::net::TcpListener;
use std::str::from_utf8;
use std::thread;

use clap::Parser;
use database::consts::consts::EntityId;
use database::database::database::{Database, DatabaseOptions};
use database::database::request_manager::RequestManager;
use database::database::table::row::{UpdatePersonData, UpdateStatement};
use database::model::person::Person;
use database::model::statement::Statement; // TCP Stream defines implementation

/// 📀 Lineagedb TCP Server, provides a simple tcp interface for interacting with the database
///
/// Can connect via netcat `echo "l" | netcat 127.0.0.1 9000`
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
}

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Cli::parse();

    log::info!("TCP Server running on {}:{}", args.address, args.port);

    let database_options = DatabaseOptions::default().set_data_directory(args.data);

    // Setup database
    let database_sender = Database::new(database_options).run(5);

    let listener = TcpListener::bind(format!("{}:{}", args.address, args.port)).unwrap();

    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let request_manager = RequestManager::new(database_sender.clone());

                thread::spawn(move || {
                    println!("Connected stream");

                    // Must initalize memory to 0s usage
                    let mut buffer = [0; 1024];

                    match stream.read(&mut buffer) {
                        Ok(_) => {
                            let (request, _) =
                                from_utf8(&buffer[..]).unwrap().split_once('\n').unwrap();

                            log::info!("Request: {}", request);

                            let statement = match request {
                                "l" => Some(Statement::List(None)),
                                "a" => Some(Statement::Add(Person {
                                    id: EntityId("test".to_string()),
                                    full_name: format!("[Count 0] Dale Salter"),
                                    email: Some(format!("dalejsalter-{}@outlook.com", "test")),
                                })),
                                "u" => Some(Statement::Update(
                                    EntityId("test".to_string()),
                                    UpdatePersonData {
                                        full_name: UpdateStatement::Set(format!(
                                            "[Count TEST] Dale Salter"
                                        )),
                                        email: UpdateStatement::NoChanges,
                                    },
                                )),
                                "d" => Some(Statement::Remove(EntityId("test".to_string()))),
                                _ => None,
                            };

                            if let Some(statement) = statement {
                                let response = request_manager
                                    .send_single_statement(statement)
                                    .expect("Should not timeout");

                                writeln!(stream, "{:#?}", response).unwrap();
                            } else {
                                writeln!(stream, "Unknown Command").unwrap();
                            }
                        }
                        Err(e) => log::info!("Failed to read connection: {}", e),
                    }
                });
            }
            Err(e) => {
                log::info!("Failed to establish connection: {}", e)
            }
        }
    }
}
