use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};
use tokio::sync::mpsc::{self};
use tokio_postgres::{Client, NoTls};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    ReadBlobState, Storage, StorageResult,
};

pub struct PgStorage {
    network_storage: NetworkStorage,
}

impl PgStorage {
    pub fn new(database: String, base_path: PathBuf) -> Self {
        let (action_sender, action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        let data = PgEnv {
            database,
            base_path,
        };

        start_runtime(action_receiver, data, task_fn, client_fn);

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

#[derive(Clone)]
struct PgEnv {
    database: String,
    base_path: PathBuf,
}

fn client_fn() -> Pin<Box<dyn Future<Output = Arc<Client>> + Send + 'static>> {
    Box::pin(async {
        // TODO: Make this generic
        let (client, connection) = tokio_postgres::connect(
            r#"
            host=localhost
            user=dalesalter
            password=mysecretpassword"#,
            NoTls,
        )
        .await
        .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // DO baseline creates
        let data_table = r#"
            CREATE TABLE IF NOT EXISTS "public"."data" (
                "id" text NOT NULL,
                "data" jsonb,
                PRIMARY KEY ("id")
            );
        "#;

        client.execute(data_table, &[]).await.unwrap();

        let tx_sequence = r#"
            CREATE SEQUENCE IF NOT EXISTS transaction_id_seq;
        "#;

        client.execute(tx_sequence, &[]).await.unwrap();

        let transaction_table = r#"
            CREATE TABLE IF NOT EXISTS "public"."transaction" (
                "id" int4 NOT NULL DEFAULT nextval('tranaction_id_seq'::regclass),
                "data" jsonb,
                PRIMARY KEY ("id")
            );
        "#;

        client.execute(transaction_table, &[]).await.unwrap();

        Arc::new(client)
    })
}

// Is there a way to avoid this duplication?
impl Storage for PgStorage {
    fn init(&self) -> StorageResult<()> {
        self.network_storage.init()
    }

    fn reset_database(&self) -> StorageResult<()> {
        self.network_storage.reset_database()
    }

    fn write_blob(&self, path: String, bytes: Vec<u8>) -> StorageResult<()> {
        self.network_storage.write_blob(path, bytes)
    }

    fn read_blob(&self, path: String) -> StorageResult<ReadBlobState> {
        self.network_storage.read_blob(path)
    }

    fn transaction_write(&mut self, transaction: &[u8]) -> StorageResult<()> {
        self.network_storage.transaction_write(transaction)
    }

    fn transaction_sync(&self) -> StorageResult<()> {
        self.network_storage.transaction_sync()
    }

    fn transaction_flush(&mut self) -> StorageResult<()> {
        self.network_storage.transaction_flush()
    }

    fn transaction_load(&mut self) -> StorageResult<Vec<String>> {
        self.network_storage.transaction_load()
    }
}

// This Arc<Arc<>> Is wonky, it's only because postgres is not cloneable (unlike the others)
//  should we just wrap everything in an arc?
fn task_fn(
    data: PgEnv,
    client: Arc<Arc<Client>>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        // TODO: Move the database into the set-up
        let bucket = &data.database;
        let base_path = data.base_path;

        match action {
            NetworkStorageAction::Reset(r) => {
                r.sender.send(Ok(())).unwrap();
            }
            NetworkStorageAction::WriteBlob(file_request) => {
                let write_blob = r#"
                    INSERT INTO "public"."data" ("id", "data") VALUES ($1, $2);
                "#;

                let json = serde_json::to_value(file_request.bytes).unwrap();

                // TODO: Confirm if this was successful or not, perhaps look at the insert count
                client
                    .execute(write_blob, &[&file_request.file_path, &json])
                    .await
                    .unwrap();

                // TOOD: Handle unwraps
                let _ = file_request.sender.send(Ok(())).unwrap();
            }
            NetworkStorageAction::ReadBlob(file_request) => {
                let read_blob = r#"
                    SELECT * FROM "public"."data" WHERE id = $1;
                "#;

                let result = client
                    .query(read_blob, &[&file_request.file_path])
                    .await
                    .unwrap();

                let response = match result.first() {
                    Some(row) => {
                        let data: serde_json::Value = row.get("data");
                        Ok(ReadBlobState::Found(
                            data.as_str().unwrap().as_bytes().to_vec(),
                        ))
                    }
                    None => Ok(ReadBlobState::NotFound),
                };

                let _ = file_request.sender.send(response).unwrap();
            }
            NetworkStorageAction::TransactionWrite(request) => {
                let transaction_insert = r#"
                    INSERT INTO "public"."transaction" ("data") VALUES ($1);
                "#;

                // Should we take in a Value type and then convert to disk? This does lock us into serde json.
                let json = serde_json::to_value(request.bytes).unwrap();

                // TODO: Confirm if this was successful or not, perhaps look at the insert count
                client.execute(transaction_insert, &[&json]).await.unwrap();

                request.sender.send(Ok(())).unwrap();
            }
            NetworkStorageAction::TransactionFlush(r) => {
                r.send(Ok(())).unwrap();
            }
            NetworkStorageAction::TransactionLoad(request) => {
                let transaction_select = r#"
                    SELECT * FROM "public"."transaction";
                "#;

                let result = client.query(transaction_select, &[]).await.unwrap();

                let mut contents: Vec<String> = vec![];

                for row in result {
                    let data: serde_json::Value = row.get("data");

                    contents.push(data.to_string());
                }

                request.send(Ok(contents)).unwrap();
            }
        }
    })
}
