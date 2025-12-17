use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::time::interval;

use super::request::{
    LabelValuesParams, LabelsParams, LabelsRequest, QueryParams, QueryRangeParams,
    QueryRangeRequest, QueryRequest, SeriesParams, SeriesRequest,
};
use super::response::{
    LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
};
use super::router::PromqlRouter;
use crate::model::{Attribute, MetricType, Sample, SampleWithAttributes, TimeBucket};
use crate::tsdb::Tsdb;
use crate::util::OpenTsdbError;

/// Server configuration
pub(crate) struct ServerConfig {
    pub(crate) port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { port: 9090 }
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

        // Spawn background task to ingest 'up' metric every 15 seconds
        let tsdb_for_ingest = self.tsdb.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(15));
            loop {
                ticker.tick().await;
                if let Err(e) = ingest_up_metric(&tsdb_for_ingest).await {
                    tracing::error!("Failed to ingest up metric: {}", e);
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

/// Ingest a sample for the 'up' metric
async fn ingest_up_metric(tsdb: &Tsdb) -> crate::util::Result<()> {
    let now = SystemTime::now();
    let bucket = TimeBucket::round_to_hour(now)?;

    let timestamp_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

    let sample = SampleWithAttributes {
        attributes: vec![
            Attribute {
                key: "__name__".to_string(),
                value: "up".to_string(),
            },
            Attribute {
                key: "job".to_string(),
                value: "prometheus".to_string(),
            },
            Attribute {
                key: "instance".to_string(),
                value: "localhost:9090".to_string(),
            },
        ],
        metric_unit: None,
        metric_type: MetricType::Gauge,
        sample: Sample {
            timestamp: timestamp_ms,
            value: 1.0,
        },
    };

    let mini = tsdb.get_or_create_for_ingest(bucket).await?;
    mini.ingest(vec![sample]).await?;
    tsdb.flush().await?;

    tracing::debug!("Ingested up metric at timestamp {}", timestamp_ms);
    Ok(())
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
