use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::time::interval;

use super::config::PrometheusConfig;
use super::request::{
    LabelValuesParams, LabelsParams, LabelsRequest, QueryParams, QueryRangeParams,
    QueryRangeRequest, QueryRequest, SeriesParams, SeriesRequest,
};
use super::response::{
    LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
};
use super::router::PromqlRouter;
use super::scraper::Scraper;
use crate::tsdb::Tsdb;
use crate::util::OpenTsdbError;

/// Server configuration
pub(crate) struct ServerConfig {
    pub(crate) port: u16,
    pub(crate) prometheus_config: PrometheusConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            prometheus_config: PrometheusConfig::default(),
        }
    }
}

/// Prometheus-compatible HTTP server
pub(crate) struct PromqlServer {
    tsdb: Arc<Tsdb>,
    config: ServerConfig,
}

impl PromqlServer {
    pub(crate) fn new(tsdb: Arc<Tsdb>, config: ServerConfig) -> Self {
        Self { tsdb, config }
    }

    /// Run the HTTP server
    pub(crate) async fn run(self) {
        let tsdb = self.tsdb.clone();

        // Start the scraper if there are scrape configs
        if !self.config.prometheus_config.scrape_configs.is_empty() {
            let scraper = Arc::new(Scraper::new(
                self.tsdb.clone(),
                self.config.prometheus_config.clone(),
            ));
            scraper.run();
            tracing::info!(
                "Started scraper with {} job(s)",
                self.config.prometheus_config.scrape_configs.len()
            );
        } else {
            tracing::info!("No scrape configs found, scraper not started");
        }

        // Start the flush timer (flushes TSDB every 30 seconds)
        let tsdb_for_flush = self.tsdb.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                if let Err(e) = tsdb_for_flush.flush().await {
                    tracing::error!("Failed to flush TSDB: {}", e);
                } else {
                    tracing::debug!("Flushed TSDB");
                }
            }
        });

        // Build router
        let app = Router::new()
            .route("/api/v1/query", get(handle_query).post(handle_query))
            .route(
                "/api/v1/query_range",
                get(handle_query_range).post(handle_query_range),
            )
            .route("/api/v1/series", get(handle_series).post(handle_series))
            .route("/api/v1/labels", get(handle_labels))
            .route("/api/v1/label/{name}/values", get(handle_label_values))
            .with_state(tsdb);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Prometheus-compatible server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

/// Error response wrapper for converting OpenTsdbError to HTTP responses
struct ApiError(OpenTsdbError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            OpenTsdbError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            OpenTsdbError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Encoding(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = serde_json::json!({
            "status": "error",
            "errorType": error_type,
            "error": self.0.to_string()
        });

        (status, Json(body)).into_response()
    }
}

impl From<OpenTsdbError> for ApiError {
    fn from(err: OpenTsdbError) -> Self {
        ApiError(err)
    }
}

/// Handle /api/v1/query
async fn handle_query(
    State(tsdb): State<Arc<Tsdb>>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, ApiError> {
    let request: QueryRequest = params.try_into()?;
    Ok(Json(tsdb.query(request).await))
}

/// Handle /api/v1/query_range
async fn handle_query_range(
    State(tsdb): State<Arc<Tsdb>>,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<QueryRangeResponse>, ApiError> {
    let request: QueryRangeRequest = params.try_into()?;
    Ok(Json(tsdb.query_range(request).await))
}

/// Handle /api/v1/series
async fn handle_series(
    State(tsdb): State<Arc<Tsdb>>,
    Query(params): Query<SeriesParams>,
) -> Result<Json<SeriesResponse>, ApiError> {
    let request: SeriesRequest = params.try_into()?;
    Ok(Json(tsdb.series(request).await))
}

/// Handle /api/v1/labels
async fn handle_labels(
    State(tsdb): State<Arc<Tsdb>>,
    Query(params): Query<LabelsParams>,
) -> Result<Json<LabelsResponse>, ApiError> {
    let request: LabelsRequest = params.try_into()?;
    Ok(Json(tsdb.labels(request).await))
}

/// Handle /api/v1/label/{name}/values
async fn handle_label_values(
    State(tsdb): State<Arc<Tsdb>>,
    Path(name): Path<String>,
    Query(params): Query<LabelValuesParams>,
) -> Result<Json<LabelValuesResponse>, ApiError> {
    let request = params.into_request(name)?;
    Ok(Json(tsdb.label_values(request).await))
}
