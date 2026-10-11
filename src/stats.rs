use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
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
    latency_ewma_ms: AtomicU64,
}

/// Response extension marking a 429 the relay produced itself because every upstream was busy.
#[derive(Clone, Copy)]
pub struct Saturated;

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
            latency_ewma_ms: AtomicU64::new(0),
        }
    }
}

impl RelayStats {
    pub fn begin(&self) -> InflightGuard<'_> {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        InflightGuard(self)
    }

    /// Buckets are disjoint: saturated replies are not also counted as rate limited.
    pub fn finish(&self, status: StatusCode, saturated: bool, latency: Duration) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let bucket = match status.as_u16() {
            _ if saturated => &self.saturated,
            200..=299 => &self.success,
            429 => &self.rate_limited,
            400..=499 => &self.client_errors,
            _ => &self.server_errors,
        };
        bucket.fetch_add(1, Ordering::Relaxed);
        let sample = latency.as_millis().min(u64::MAX as u128) as u64;
        self.latency_ewma_ms
            .update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                if prev == 0 {
                    sample
                } else {
                    (prev * 4 + sample) / 5
                }
            });
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
            latency_ewma_ms: self.latency_ewma_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RelayStatus {
    pub uptime_secs: u64,
    pub inflight: usize,
    pub requests: u64,
    pub success: u64,
    pub client_errors: u64,
    pub rate_limited: u64,
    pub server_errors: u64,
    pub saturated: u64,
    pub latency_ewma_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_statuses_and_tracks_inflight() {
        let stats = RelayStats::default();
        let guard = stats.begin();
        assert_eq!(stats.status().inflight, 1);
        for code in [200, 201, 400, 401, 429, 500, 502] {
            stats.finish(
                StatusCode::from_u16(code).unwrap(),
                false,
                Duration::from_millis(100),
            );
        }
        stats.finish(
            StatusCode::TOO_MANY_REQUESTS,
            true,
            Duration::from_millis(100),
        );
        drop(guard);
        let status = stats.status();
        assert_eq!(status.inflight, 0);
        assert_eq!(status.requests, 8);
        assert_eq!(status.success, 2);
        assert_eq!(status.client_errors, 2);
        assert_eq!(status.rate_limited, 1);
        assert_eq!(status.server_errors, 2);
        assert_eq!(status.saturated, 1);
        assert_eq!(status.latency_ewma_ms, 100);
    }
}
