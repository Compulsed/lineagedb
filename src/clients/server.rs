use std::io::{Read, Write};
use std::net::TcpListener;
use std::str::from_utf8;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use crate::consts::consts::EntityId;
use crate::database::request_manager::{DatabaseRequest, RequestManager};
use crate::database::table::row::{UpdateAction, UpdatePersonData};
use crate::model::action::Action;
use crate::model::person::Person; // TCP Stream defines implementation

pub struct Server {
    addr: String,
}

impl Server {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub fn run(self, database_sender: Sender<DatabaseRequest>) {
        println!("Listening on port {}", self.addr);

        let listener = TcpListener::bind(&self.addr).unwrap();

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

                                println!("Request: {}", request);

                                let action = match request {
                                    "l" => Some(Action::List),
                                    "a" => Some(Action::Add(Person {
                                        id: EntityId("test".to_string()),
                                        full_name: format!("[Count 0] Dale Salter"),
                                        email: Some(format!("dalejsalter-{}@outlook.com", "test")),
                                    })),
                                    "u" => Some(Action::Update(
                                        EntityId("test".to_string()),
                                        UpdatePersonData {
                                            full_name: UpdateAction::Set(format!(
                                                "[Count TEST] Dale Salter"
                                            )),
                                            email: UpdateAction::NoChanges,
                                        },
                                    )),
                                    "d" => Some(Action::Remove(EntityId("test".to_string()))),
                                    _ => None,
                                };

                                if let Some(action) = action {
                                    let response = request_manager
                                        .send_request(action)
                                        .expect("Should not timeout");

                                    writeln!(stream, "{:#?}", response).unwrap();
                                } else {
                                    writeln!(stream, "Unknown Command").unwrap();
                                }
                            }
                            Err(e) => println!("Failed to read connection: {}", e),
                        }
                    });
                }
                Err(e) => {
                    println!("Failed to establish connection: {}", e)
                }
            }
        }
    }
}
