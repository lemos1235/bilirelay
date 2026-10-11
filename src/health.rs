//! Active health checks: upstreams with `health_check_secs` set are probed with a tiny real
//! request while they are unhealthy, so they recover without waiting for client traffic to
//! trip over them. Healthy upstreams are never probed and cost nothing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use crate::AppState;
use crate::classify::{Failure, Verdict};
use crate::config::{UpstreamApi, UpstreamConfig};
use crate::convert;
use crate::pool::Lease;
use crate::proxy::{Attempt, assess, rewrite, send};
use crate::tokens::{self, RequestSize};
use crate::upstream::Spend;

const TICK: Duration = Duration::from_secs(1);
const TIMEOUT_CAP: Duration = Duration::from_secs(30);
/// Many providers reject smaller output limits.
const MAX_OUTPUT_TOKENS: u64 = 16;
const SIZE: RequestSize = RequestSize {
    input: 1,
    max_output: Some(MAX_OUTPUT_TOKENS),
};

/// Runs for the life of the process, following config reloads.
pub fn spawn(app: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: HashMap<String, Instant> = HashMap::new();
        let mut ticker = tokio::time::interval(TICK);
        loop {
            ticker.tick().await;
            let live = app.live();
            let now = Instant::now();
            for (index, upstream) in live.pool.upstreams().iter().enumerate() {
                let cfg = upstream.cfg();
                let Some(every) = cfg.health_check().filter(|_| cfg.enabled) else {
                    continue;
                };
                if last.get(&cfg.name).is_some_and(|at| now < *at + every) {
                    continue;
                }
                // Only unhealthy upstreams outside a cooldown hand out a check lease.
                let Some(lease) = live.pool.lease_check(index, &SIZE) else {
                    continue;
                };
                last.insert(cfg.name.clone(), now);
                tokio::spawn(check(app.clone(), lease));
            }
        }
    })
}

async fn check(app: Arc<AppState>, lease: Lease) {
    let cfg = lease.upstream.cfg();
    let payload = match payload(&cfg) {
        Ok(payload) => payload,
        Err(message) => {
            tracing::error!(upstream = %cfg.name, error = %message, "cannot build health check");
            return;
        }
    };
    let started = Instant::now();
    let attempt = send(
        &app.client,
        &cfg,
        rewrite(&payload, &cfg),
        cfg.timeout().min(TIMEOUT_CAP),
    )
    .await;
    let latency = started.elapsed();
    let (verdict, spend, remaining, outcome) = match attempt {
        Attempt::Reply(reply) => {
            let (reply, verdict, spend) = assess(&cfg, reply);
            let remaining = tokens::remaining_tokens(&reply.headers);
            (
                verdict,
                spend,
                remaining,
                format!("HTTP {}", reply.status.as_u16()),
            )
        }
        Attempt::NetworkError(message) => (
            Verdict::Failover(Failure::Transient),
            Spend::Estimate,
            None,
            message,
        ),
    };
    let penalty = lease
        .upstream
        .record(verdict, latency, &lease.ticket, spend, remaining);
    match verdict {
        Verdict::Success | Verdict::ClientError => tracing::info!(
            upstream = %cfg.name,
            outcome = %outcome,
            latency_ms = latency.as_millis() as u64,
            "health check reached upstream; marked healthy"
        ),
        Verdict::Failover(_) => tracing::warn!(
            upstream = %cfg.name,
            outcome = %outcome,
            penalty = ?penalty,
            "health check failed"
        ),
    }
}

/// The smallest useful request, shaped for the upstream's API and passed through the same
/// `drop_fields` / `override` rewriting as client traffic.
fn payload(cfg: &UpstreamConfig) -> Result<Map<String, Value>, String> {
    let Value::Object(request) = json!({
        "model": cfg.model,
        "input": "ping",
        "max_output_tokens": MAX_OUTPUT_TOKENS,
    }) else {
        unreachable!("a JSON object literal");
    };
    match cfg.api {
        UpstreamApi::Responses => Ok(request),
        UpstreamApi::ChatCompletions => convert::request_to_chat(&request),
    }
}
