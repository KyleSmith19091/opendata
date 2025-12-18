//! Prometheus metrics for the open-tsdb server.

use std::sync::atomic::AtomicI64;

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

/// Labels for target health metrics (up gauge).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TargetLabels {
    pub job: String,
    pub instance: String,
}

/// Labels for scrape metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ScrapeLabels {
    pub job: String,
    pub instance: String,
}

/// Labels for HTTP request metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpLabelsWithStatus {
    pub method: HttpMethod,
    pub endpoint: String,
    pub status: u16,
}

/// HTTP method label value.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
    Other,
}

impl From<&Method> for HttpMethod {
    fn from(method: &Method) -> Self {
        match *method {
            Method::GET => HttpMethod::Get,
            Method::POST => HttpMethod::Post,
            Method::PUT => HttpMethod::Put,
            Method::DELETE => HttpMethod::Delete,
            Method::PATCH => HttpMethod::Patch,
            Method::HEAD => HttpMethod::Head,
            Method::OPTIONS => HttpMethod::Options,
            _ => HttpMethod::Other,
        }
    }
}

/// Container for all Prometheus metrics.
pub struct Metrics {
    registry: Registry,

    /// Target health gauge (1 = up, 0 = down).
    pub up: Family<TargetLabels, Gauge<i64, AtomicI64>>,

    /// Counter of samples successfully scraped.
    pub scrape_samples_scraped: Family<ScrapeLabels, Counter>,

    /// Counter of failed samples.
    pub scrape_samples_failed: Family<ScrapeLabels, Counter>,

    /// Counter of HTTP requests.
    pub http_requests_total: Family<HttpLabelsWithStatus, Counter>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics registry with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // Target health gauge
        let up = Family::<TargetLabels, Gauge<i64, AtomicI64>>::default();
        registry.register("up", "Whether the target is up (1) or down (0)", up.clone());

        // Scrape samples scraped counter
        let scrape_samples_scraped = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_scraped",
            "Number of samples scraped per target",
            scrape_samples_scraped.clone(),
        );

        // Scrape samples failed counter
        let scrape_samples_failed = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_failed",
            "Number of failed samples per target",
            scrape_samples_failed.clone(),
        );

        // HTTP requests total counter
        let http_requests_total = Family::<HttpLabelsWithStatus, Counter>::default();
        registry.register(
            "http_requests_total",
            "Total number of HTTP requests",
            http_requests_total.clone(),
        );

        Self {
            registry,
            up,
            scrape_samples_scraped,
            scrape_samples_failed,
            http_requests_total,
        }
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &self.registry)
            .expect("encoding metrics should not fail");
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_metrics() {
        // given/when
        let metrics = Metrics::new();

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("# HELP up"));
        assert!(encoded.contains("# HELP scrape_samples_scraped"));
        assert!(encoded.contains("# HELP http_requests_total"));
    }

    #[test]
    fn should_record_up_gauge() {
        // given
        let metrics = Metrics::new();
        let labels = TargetLabels {
            job: "test_job".to_string(),
            instance: "localhost:9090".to_string(),
        };

        // when
        metrics.up.get_or_create(&labels).set(1);

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("up{job=\"test_job\",instance=\"localhost:9090\"} 1"));
    }

    #[test]
    fn should_convert_http_method_to_label() {
        // given
        let method = Method::GET;

        // when
        let label = HttpMethod::from(&method);

        // then
        assert!(matches!(label, HttpMethod::Get));
    }
}
