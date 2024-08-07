use anyhow::anyhow;
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::mpsc::{self};
use tokio_postgres::{Client, NoTls};

use super::{
    network::{start_runtime, NetworkStorage, NetworkStorageAction},
    ReadBlobState, Storage, StorageError, StorageResult,
};

pub struct PgStorage {
    network_storage: NetworkStorage,
}

impl PgStorage {
    pub fn new(options: PostgresOptions) -> Self {
        let (action_sender, action_receiver) = mpsc::channel::<NetworkStorageAction>(16);

        start_runtime(action_receiver, options, task_fn, client_fn);

        Self {
            network_storage: NetworkStorage {
                action_sender: action_sender,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostgresOptions {
    pub database: String,
    pub host: String,
    pub user: String,
    pub password: String,
}

impl PostgresOptions {
    pub fn new(user: String, database: String, host: String, password: String) -> Self {
        Self {
            user,
            database,
            host,
            password,
        }
    }

    pub fn new_test() -> Self {
        Self {
            user: "dalesalter".to_string(),
            database: "dalesalter1".to_string(),
            host: "localhost".to_string(),
            password: "mysecretpassword".to_string(),
        }
    }
}

pub fn format_connection_string(options: &PostgresOptions, database_name: &str) -> String {
    format!(
        r#"
            host={host}
            user={user}
            password={password}
            dbname={dbname}
        "#,
        dbname = database_name,
        host = options.host,
        password = options.password,
        user = options.user,
    )
}

fn client_fn(
    options: PostgresOptions,
) -> Pin<Box<dyn Future<Output = Arc<Client>> + Send + 'static>> {
    Box::pin(async move {
        // Database creation must be done via the servicing / admin user 'postgres'
        let (admin_client, admin_connection) =
            tokio_postgres::connect(&format_connection_string(&options, "postgres"), NoTls)
                .await
                .unwrap();

        tokio::spawn(async move {
            if let Err(e) = admin_connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let create_database = format!(r#"CREATE DATABASE {};"#, &options.database);

        // There is no IF NOT EXISTs, attempt to create the database and if it fails that's okay
        //  the DB already exists
        let _ = admin_client.execute(&create_database, &[]).await;

        // Return a connection string with our upserted database
        let (client, connection) = tokio_postgres::connect(
            &format_connection_string(&options, &options.database),
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
                "id" int4 NOT NULL DEFAULT nextval('transaction_id_seq'::regclass),
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
    fn init(&mut self) -> StorageResult<()> {
        self.network_storage.init()
    }

    fn reset_database(&mut self) -> StorageResult<()> {
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

// This Arc<Arc<>> Is wonky, it's only because postgres is not clonable (unlike the others)
//  should we just wrap everything in an arc?
// Note: We are not able to use postgres transactions, this is because we require exclusive access to the client.
//  unfortunately it is not as simple as just wrapping the client in a mutex, because
//  we cannot have a mutex guard cross await points (it's not Send). A way around this MIGHT
//  be to find an API that allows us to create a transaction w/o multiple await points, another
//  way might be to inject 'static clients each take task_fn is invoked. This could be done via
//  a connection pool. Though again, this is a little odd because we mutex the persistence client (meaning)
//  there already is exclusive access. Wild.
fn task_fn(
    _data: PostgresOptions,
    client: Arc<Arc<Client>>,
    action: NetworkStorageAction,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        match action {
            NetworkStorageAction::Init(r) => {
                let _ = r.send(Ok(())).unwrap();
            }
            NetworkStorageAction::Reset(r) => {
                let delete_transactions = r#"
                    DELETE FROM "public"."transaction";
                "#;

                if let Err(e) = client.execute(delete_transactions, &[]).await {
                    r.sender
                        .send(Err(StorageError::UnableToResetPersistence(anyhow!(e))))
                        .unwrap();

                    return;
                }

                let delete_data = r#"
                    DELETE FROM "public"."data";
                "#;

                if let Err(e) = client.execute(delete_data, &[]).await {
                    r.sender
                        .send(Err(StorageError::UnableToResetPersistence(anyhow!(e))))
                        .unwrap();

                    return;
                }

                r.sender.send(Ok(())).unwrap();
            }
            NetworkStorageAction::WriteBlob(file_request) => {
                let write_blob = r#"
                    INSERT INTO "public"."data" ("id", "data") VALUES ($1, $2);
                "#;

                let json: Value = byte_array_to_value(&file_request.bytes);

                let result = client
                    .execute(write_blob, &[&file_request.file_path, &json])
                    .await;

                let response = match result {
                    Ok(1) => Ok(()),
                    Ok(insert_count) => Err(StorageError::UnableToWriteBlob(anyhow!(
                        "Expected 1 row to be inserted, got {}",
                        insert_count
                    ))),
                    Err(e) => Err(StorageError::UnableToWriteBlob(anyhow!(e))),
                };

                let _ = file_request.sender.send(response).unwrap();
            }
            NetworkStorageAction::ReadBlob(file_request) => {
                let read_blob = r#"
                    SELECT * FROM "public"."data" WHERE id = $1;
                "#;

                let result = client.query(read_blob, &[&file_request.file_path]).await;

                let response = match result {
                    Ok(rows) => match rows.first() {
                        Some(row) => {
                            let data: serde_json::Value = row.get("data");

                            let json_string =
                                serde_json::to_string(&data).unwrap().as_bytes().to_vec();

                            Ok(ReadBlobState::Found(json_string))
                        }
                        None => Ok(ReadBlobState::NotFound),
                    },
                    Err(e) => Err(StorageError::UnableToReadBlob(anyhow!(e))),
                };

                let _ = file_request.sender.send(response).unwrap();
            }
            NetworkStorageAction::TransactionWrite(request) => {
                let transaction_insert = r#"
                    INSERT INTO "public"."transaction" ("data") VALUES ($1);
                "#;

                let json: Value = byte_array_to_value(&request.bytes);

                let response = match client.execute(transaction_insert, &[&json]).await {
                    Ok(1) => Ok(()),
                    Ok(insert_count) => Err(StorageError::UnableToWriteTransaction(anyhow!(
                        "Expected 1 row to be inserted, got {}",
                        insert_count
                    ))),
                    Err(e) => Err(StorageError::UnableToWriteTransaction(anyhow!(e))),
                };

                request.sender.send(response).unwrap();
            }
            NetworkStorageAction::TransactionFlush(request) => {
                let reset_sql = r#"
                    DELETE FROM "public"."transaction";
                "#;

                let delete_transaction_response = match client.execute(reset_sql, &[]).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(StorageError::UnableToDeleteTransactionLog(anyhow!(e))),
                };

                request.send(delete_transaction_response).unwrap();
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

// So that we store the jsonb value (rather than the byte array,
//  we must first convert the bytes back to a string, then, from there a Value
fn byte_array_to_value(bytes: &Vec<u8>) -> Value {
    let json_string = std::str::from_utf8(&bytes).unwrap();
    serde_json::from_str(json_string).unwrap()
}
