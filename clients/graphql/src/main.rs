use actix_cors::Cors;
use actix_web::{
    get,
    middleware::{self, Condition},
    route,
    rt::task::spawn_blocking,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
use actix_web_lab::respond::Html;
use clap::Parser;
use database::{
    database::{
        commands::ShutdownRequest, database::Database, options::DatabaseOptions,
        request_manager::RequestManager,
    },
    persistence::storage::{
        dynamodb::DynamoOptions, postgres::PostgresOptions, s3::S3Options, StorageEngine,
    },
};
use juniper::http::{graphiql::graphiql_source, GraphQLRequest};
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        Aggregation, Instrument, MeterProviderBuilder, PeriodicReader, SdkMeterProvider, Stream,
    },
    runtime,
    trace::{BatchConfig, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use std::{io, sync::Arc};
use tracing::Level;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Registry};

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
        request_manager: request_manager.clone(),
    };

    let user = data.execute(&schema, &graphql_context).await;

    HttpResponse::Ok().json(user)
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum StorageEngineFlag {
    File,
    Dynamo,
    Postgres,
    S3,
}

fn to_storage_engine(args: &Cli) -> StorageEngine {
    match args.storage {
        StorageEngineFlag::File => StorageEngine::File(args.data.clone()),
        StorageEngineFlag::Dynamo => {
            StorageEngine::DynamoDB(DynamoOptions::new(args.table.clone()))
        }
        StorageEngineFlag::Postgres => StorageEngine::Postgres(PostgresOptions::new(
            args.database_user.clone(),
            args.database_database.clone(),
            args.database_host.clone(),
            args.database_password.clone(),
        )),
        StorageEngineFlag::S3 => StorageEngine::S3(S3Options::new(args.bucket.clone())),
    }
}

/// 📀 Lineagedb GraphQL Server, provides a simple GraphQL interface for interacting with the database
#[derive(Parser, Debug)]
struct Cli {
    /// Port the graphql server will run on
    #[clap(short, long, default_value = "9000")]
    port: u16,

    /// Address the graphql server will run on
    #[clap(short, long, default_value = "0.0.0.0")]
    address: String,

    /// Whether to log out GraphQL HTTP requests
    #[clap(long, default_value = "false")]
    log_http: bool,

    #[clap(long, default_value_t = 2)]
    http_workers: usize,

    #[clap(long)]
    #[clap(help = "Which storage mechanism to use")]
    #[clap(value_enum, default_value_t=StorageEngineFlag::File)]
    storage: StorageEngineFlag,

    /// When using file storage, location of the database. Reads / writes to this directory. Note: Does not support shell paths, e.g. ~
    #[clap(long, default_value = "data")]
    data: std::path::PathBuf,

    /// When using DynamoDB the table name
    #[clap(long, default_value = "lineagedb-ddb")]
    table: String,

    /// When using S3 the bucket name
    #[clap(long, default_value = "dalesalter-test-bucket")]
    bucket: String,

    /// When using Postgres the database information
    #[clap(long, default_value = "dalesalter")]
    database_user: String,

    #[clap(long, default_value = "dalesalter1")]
    database_database: String,

    #[clap(long, default_value = "localhost")]
    database_host: String,

    #[clap(long, default_value = "mysecretpassword")]
    database_password: String,
}

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(DEPLOYMENT_ENVIRONMENT, "develop"),
        ],
        SCHEMA_URL,
    )
}

// Construct MeterProvider for MetricsLayer
// fn init_meter_provider() -> SdkMeterProvider {
//     let exporter = opentelemetry_otlp::new_exporter()
//         .tonic()
//         .build_metrics_exporter(
//             Box::new(DefaultAggregationSelector::new()),
//             Box::new(DefaultTemporalitySelector::new()),
//         )
//         .unwrap();

//     let reader = PeriodicReader::builder(exporter, runtime::Tokio)
//         .with_interval(std::time::Duration::from_secs(30))
//         .build();

//     // For debugging in development
//     let stdout_reader = PeriodicReader::builder(
//         opentelemetry_stdout::MetricsExporter::default(),
//         runtime::Tokio,
//     )
//     .build();

//     // Set Custom histogram boundaries for baz metrics
//     let view_baz = |instrument: &Instrument| -> Option<Stream> {
//         if instrument.name == "baz" {
//             Some(
//                 Stream::new()
//                     .name("baz")
//                     .aggregation(Aggregation::ExplicitBucketHistogram {
//                         boundaries: vec![0.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0],
//                         record_min_max: true,
//                     }),
//             )
//         } else {
//             None
//         }
//     };

//     let meter_provider = MeterProviderBuilder::default()
//         .with_resource(resource())
//         .with_reader(reader)
//         .with_reader(stdout_reader)
//         .with_view(view_baz)
//         .build();

//     global::set_meter_provider(meter_provider.clone());

//     meter_provider
// }

// Construct Tracer for OpenTelemetryLayer
fn init_tracer() -> Tracer {
    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    1.0,
                ))))
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(resource()),
        )
        .with_batch_config(BatchConfig::default())
        // .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"),
        )
        .install_batch(runtime::Tokio)
        .unwrap();

    global::set_tracer_provider(provider.clone());
    provider.tracer("tracing-otel-subscriber")
}

// Initialize tracing-subscriber and return OtelGuard for opentelemetry-related termination processing
fn init_tracing_subscriber() -> OtelGuard {
    // let meter_provider = init_meter_provider();
    let tracer = init_tracer();

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::INFO,
        ))
        .with(tracing_subscriber::fmt::layer())
        // .with(MetricsLayer::new(meter_provider.clone()))
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    OtelGuard { /* meter_provider */ }
}

struct OtelGuard {
    // meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // if let Err(err) = self.meter_provider.shutdown() {
        //     eprintln!("{err:?}");
        // }
        opentelemetry::global::shutdown_tracer_provider();
    }
}

// #[tokio::main]
// async fn main() {
//     let _guard = init_tracing_subscriber();

//     foo().await;
// }

#[actix_web::main]
#[tracing::instrument(name = "database-startup")]
async fn main() -> io::Result<()> {
    // let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();

    // let tracer = opentelemetry_otlp::new_pipeline()
    //     .tracing()
    //     .with_exporter(otlp_exporter)
    //     .install_simple()
    //     .unwrap()
    //     .tracer("trace_demo");

    // let otel_layer = OpenTelemetryLayer::new(tracer);

    // let subscriber = Registry::default().with(otel_layer);

    let _guard = init_tracing_subscriber();

    // env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let args = Cli::parse();

    let database_options = DatabaseOptions::default().set_storage_engine(to_storage_engine(&args));

    // For S3 (an optional backing storage engine), we must use tokio. This would be fine
    //  but the database uses sync apis (blocking_send). blocking_send CANNOT be called with any call-stack
    //  that has tokio or actix. This is fine for the standard database requests as they have their own sync
    //  thread. This becomes an issue when we want to call blocking_send when spinning up a new database (like below)
    //  the way we can get around this issue is just by transferring into a sync context.
    //
    // Error Message: Cannot block the current thread from within a runtime. This \
    // happens because a function attempted to block the current \
    // thread while the thread is being used to drive asynchronous \
    // tasks.
    //
    // Context reference: Actix (Async) -> Database (Sync) -> Tokio S3 (Async)
    let request_manager: RequestManager = spawn_blocking(|| Database::new(database_options).run())
        .await
        .unwrap();

    // Set up Ctrl-C handler
    let set_handler_database_sender_clone = request_manager.clone();

    ctrlc::set_handler(move || {
        let shutdown_response = set_handler_database_sender_clone
            .clone()
            .send_shutdown_request(ShutdownRequest::Coordinator)
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
