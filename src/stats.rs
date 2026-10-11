use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;

/// Client-facing counters: one entry per request to /v1/responses, regardless of failover.
pub struct RelayStats {
    started: Instant,
    inflight: AtomicUsize,
    requests: AtomicU64,
    success: AtomicU64,
    client_errors: AtomicU64,
    rate_limited: AtomicU64,
    server_errors: AtomicU64,
    saturated: AtomicU64,
    quota_limited: AtomicU64,
    tokens: AtomicU64,
    latency_ewma_ms: AtomicU64,
}

/// Response extension marking a 429 the relay produced itself rather than passed through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// Every upstream was busy or cooling down.
    Saturated,
    /// The calling client exceeded its own rpm / tpm quota.
    Quota,
}

/// Response extension carrying the tokens the upstream reported for the request.
#[derive(Debug, Clone, Copy)]
pub struct Usage(pub u64);

pub struct InflightGuard<'a>(&'a RelayStats);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for RelayStats {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            inflight: AtomicUsize::new(0),
            requests: AtomicU64::new(0),
            success: AtomicU64::new(0),
            client_errors: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            server_errors: AtomicU64::new(0),
            saturated: AtomicU64::new(0),
            quota_limited: AtomicU64::new(0),
            tokens: AtomicU64::new(0),
            latency_ewma_ms: AtomicU64::new(0),
        }
    }
}

impl RelayStats {
    pub fn begin(&self) -> InflightGuard<'_> {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        InflightGuard(self)
    }

    /// Buckets are disjoint: relay-made rejections are not also counted as rate limited.
    pub fn finish(&self, response: &Response, latency: Duration) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let bucket = match (response.extensions().get::<Rejection>(), response.status()) {
            (Some(Rejection::Saturated), _) => &self.saturated,
            (Some(Rejection::Quota), _) => &self.quota_limited,
            (None, status) if status.is_success() => &self.success,
            (None, StatusCode::TOO_MANY_REQUESTS) => &self.rate_limited,
            (None, status) if status.is_client_error() => &self.client_errors,
            _ => &self.server_errors,
        };
        bucket.fetch_add(1, Ordering::Relaxed);
        if let Some(Usage(tokens)) = response.extensions().get::<Usage>() {
            self.tokens.fetch_add(*tokens, Ordering::Relaxed);
        }
        observe_ewma(&self.latency_ewma_ms, latency);
    }

    pub fn status(&self) -> RelayStatus {
        RelayStatus {
            uptime_secs: self.started.elapsed().as_secs(),
            inflight: self.inflight.load(Ordering::Acquire),
            requests: self.requests.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            client_errors: self.client_errors.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            server_errors: self.server_errors.load(Ordering::Relaxed),
            saturated: self.saturated.load(Ordering::Relaxed),
            quota_limited: self.quota_limited.load(Ordering::Relaxed),
            tokens: self.tokens.load(Ordering::Relaxed),
            latency_ewma_ms: self.latency_ewma_ms.load(Ordering::Relaxed),
        }
    }
}

/// Folds a latency sample into a millisecond EWMA (weight 1/5); 0 means "no sample yet".
pub(crate) fn observe_ewma(cell: &AtomicU64, latency: Duration) {
    let sample = latency.as_millis().min(u64::MAX as u128) as u64;
    cell.update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
        if prev == 0 {
            sample
        } else {
            prev.saturating_mul(4).saturating_add(sample) / 5
        }
    });
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayStatus {
    pub uptime_secs: u64,
    pub inflight: usize,
    pub requests: u64,
    pub success: u64,
    pub client_errors: u64,
    pub rate_limited: u64,
    pub server_errors: u64,
    pub saturated: u64,
    pub quota_limited: u64,
    pub tokens: u64,
    pub latency_ewma_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    fn response(code: u16, rejection: Option<Rejection>, usage: Option<u64>) -> Response {
        let mut response = StatusCode::from_u16(code).unwrap().into_response();
        if let Some(rejection) = rejection {
            response.extensions_mut().insert(rejection);
        }
        if let Some(tokens) = usage {
            response.extensions_mut().insert(Usage(tokens));
        }
        response
    }

    #[test]
    fn buckets_statuses_and_tracks_inflight() {
        let stats = RelayStats::default();
        let guard = stats.begin();
        assert_eq!(stats.status().inflight, 1);
        let latency = Duration::from_millis(100);
        for code in [200, 201, 400, 401, 429, 500, 502] {
            stats.finish(&response(code, None, Some(10)), latency);
        }
        stats.finish(&response(429, Some(Rejection::Saturated), None), latency);
        stats.finish(&response(429, Some(Rejection::Quota), None), latency);
        drop(guard);
        let status = stats.status();
        assert_eq!(status.inflight, 0);
        assert_eq!(status.requests, 9);
        assert_eq!(status.success, 2);
        assert_eq!(status.client_errors, 2);
        assert_eq!(status.rate_limited, 1);
        assert_eq!(status.server_errors, 2);
        assert_eq!(status.saturated, 1);
        assert_eq!(status.quota_limited, 1);
        assert_eq!(status.tokens, 70);
        assert_eq!(status.latency_ewma_ms, 100);
    }
}
