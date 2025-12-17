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

use opendata_common::storage::in_memory::InMemoryStorage;

use promql::server::{PromqlServer, ServerConfig};
use storage::merge_operator::OpenTsdbMergeOperator;
use tsdb::Tsdb;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create in-memory storage with merge operator
    let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
        OpenTsdbMergeOperator,
    )));

    // Create Tsdb
    let tsdb = Arc::new(Tsdb::new(storage));

    // Create and run server on default port (9090)
    let config = ServerConfig::default();
    let server = PromqlServer::new(tsdb, config);

    println!("Starting open-tsdb Prometheus-compatible server on port 9090...");
    server.run().await;
}
