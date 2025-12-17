#![allow(dead_code)]
mod delta;
mod head;
mod index;
mod minitsdb;
mod model;
mod promql;
mod query;
mod serde;
mod storage;
#[cfg(test)]
mod test_utils;
mod tsdb;
mod util;

use std::sync::Arc;

use clap::Parser;
use opendata_common::storage::in_memory::InMemoryStorage;

use promql::config::{CliArgs, PrometheusConfig, load_config};
use promql::server::{PromqlServer, ServerConfig};
use storage::merge_operator::OpenTsdbMergeOperator;
use tsdb::Tsdb;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Load Prometheus configuration if provided
    let prometheus_config = if let Some(config_path) = &args.config {
        match load_config(config_path) {
            Ok(config) => {
                tracing::info!("Loaded configuration from {}", config_path);
                config
            }
            Err(e) => {
                tracing::error!("Failed to load configuration: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        tracing::info!("No configuration file provided, using defaults");
        PrometheusConfig::default()
    };

    // Create in-memory storage with merge operator
    let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
        OpenTsdbMergeOperator,
    )));

    // Create Tsdb
    let tsdb = Arc::new(Tsdb::new(storage));

    // Create server configuration
    let config = ServerConfig {
        port: args.port,
        prometheus_config,
    };

    // Create and run server
    let server = PromqlServer::new(tsdb, config);

    tracing::info!(
        "Starting open-tsdb Prometheus-compatible server on port {}...",
        args.port
    );
    server.run().await;
}
