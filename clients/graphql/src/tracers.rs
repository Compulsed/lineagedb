use std::{collections::HashMap, env};

use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    runtime,
    trace::{BatchConfig, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tracing::Level;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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

// TODO: Configure GraphQL client to use the opentelemetry tracing layer
fn init_tracer() -> Option<Tracer> {
    let honeycomb_dataset = env::var("HONEYCOMB_DATA_SET");
    let honeycomb_api_key = env::var("HONEYCOMB_API_KEY");
    let otel_exporter = env::var("OTEL_EXPORTER");

    let trace_config = opentelemetry_sdk::trace::Config::default()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource());

    // We are unable to share common new_pipeline methods because `with_exporter` is a generic
    //  and creates a unique type per exporter.
    let optional_provider: Option<_> = match (honeycomb_dataset, honeycomb_api_key, otel_exporter) {
        (Ok(honeycomb_dataset), Ok(honeycomb_api_key), _) => {
            let hny_exporter = opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint("https://api.honeycomb.io/v1/traces")
                .with_http_client(reqwest::Client::default())
                .with_headers(HashMap::from([
                    ("x-honeycomb-dataset".into(), honeycomb_dataset),
                    ("x-honeycomb-team".into(), honeycomb_api_key),
                ]))
                .with_timeout(std::time::Duration::from_secs(2));

            Some(
                opentelemetry_otlp::new_pipeline()
                    .tracing()
                    .with_trace_config(trace_config)
                    .with_batch_config(BatchConfig::default())
                    .with_exporter(hny_exporter)
                    .install_batch(runtime::Tokio)
                    .unwrap(),
            )
        }
        (_, _, Ok(exporter)) => Some(
            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_trace_config(trace_config)
                .with_batch_config(BatchConfig::default())
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(exporter),
                )
                .install_batch(runtime::Tokio)
                .unwrap(),
        ),
        _ => None,
    };

    if let Some(provider) = optional_provider {
        global::set_tracer_provider(provider.clone());

        // Unsure if this tracer name does anything
        return Some(provider.tracer("tracing-otel-subscriber"));
    }

    None
}

// TODO:
//  - Implement the metrics provider, it is available just not configured
//  - Document the .env file
pub fn init_tracing_subscriber() {
    let tracer = init_tracer();

    let tracing_subscriber_info = tracing_subscriber::filter::LevelFilter::from_level(Level::INFO);

    // We must duplicate these methods as with `OpenTelemetryLayer` is a generic and creates
    //  a unique type per layer.
    if let Some(t) = tracer {
        tracing_subscriber::registry()
            .with(tracing_subscriber_info)
            .with(OpenTelemetryLayer::new(t))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber_info)
            .init();
    }
}
