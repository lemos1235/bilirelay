pub mod classify;
pub mod clients;
pub mod config;
pub mod convert;
pub mod error;
pub mod health;
pub mod metrics;
pub mod monitor;
pub mod pool;
pub mod proxy;
pub mod reload;
pub mod stats;
pub mod tokens;
pub mod upstream;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::clients::Clients;
use crate::config::Config;
use crate::error::{api_error, bearer_matches};
use crate::pool::Pool;
use crate::stats::RelayStats;

/// Everything derived from one version of the config. Each request works against the snapshot
/// it started with; a reload swaps in a new one that shares upstream and client state.
pub struct Live {
    pub config: Config,
    pub pool: Pool,
    pub clients: Clients,
}

pub struct AppState {
    live: RwLock<Arc<Live>>,
    pub client: reqwest::Client,
    pub stats: RelayStats,
    pub reloads: AtomicU64,
    pub reload_failures: AtomicU64,
}

impl AppState {
    pub fn new(config: Config) -> anyhow::Result<Arc<Self>> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Arc::new(Self {
            live: RwLock::new(Arc::new(Live {
                pool: Pool::new(&config, None),
                clients: Clients::new(&config.clients, None),
                config,
            })),
            client,
            stats: RelayStats::default(),
            reloads: AtomicU64::new(0),
            reload_failures: AtomicU64::new(0),
        }))
    }

    pub fn live(&self) -> Arc<Live> {
        self.live.read().unwrap().clone()
    }

    /// Swaps in a new configuration. Settings that are fixed at startup are reported back so
    /// the caller can warn that they still need a restart.
    pub fn apply(&self, config: Config) -> Vec<&'static str> {
        let mut live = self.live.write().unwrap();
        let old = &live.config.server;
        let mut needs_restart = Vec::new();
        if old.bind != config.server.bind {
            needs_restart.push("server.bind");
        }
        if old.max_body_bytes != config.server.max_body_bytes {
            needs_restart.push("server.max_body_bytes");
        }
        *live = Arc::new(Live {
            pool: Pool::new(&config, Some(&live.pool)),
            clients: Clients::new(&config.clients, Some(&live.clients)),
            config,
        });
        self.reloads.fetch_add(1, Ordering::Relaxed);
        needs_restart
    }
}

pub fn build_app(config: Config) -> anyhow::Result<Router> {
    Ok(router(AppState::new(config)?))
}

pub fn router(state: Arc<AppState>) -> Router {
    let body_limit = state.live().config.server.max_body_bytes;
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/responses", post(proxy::responses))
        .route("/admin/upstreams", get(admin_upstreams))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

/// Admin endpoints are off without an admin token and require it otherwise; returns the
/// rejection, if any.
fn reject_admin(live: &Live, headers: &HeaderMap) -> Option<Response> {
    let Some(token) = live.config.server.admin_token.as_deref() else {
        return Some(StatusCode::NOT_FOUND.into_response());
    };
    (!bearer_matches(headers, token)).then(|| {
        api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_admin_token",
            "admin token is invalid or missing",
        )
    })
}

async fn admin_upstreams(State(app): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let live = app.live();
    if let Some(response) = reject_admin(&live, &headers) {
        return response;
    }
    let groups: Vec<_> = live
        .config
        .groups
        .iter()
        .map(|g| json!({ "name": g.name, "strategy": g.strategy, "max_attempts": g.max_attempts, "upstreams": g.upstreams }))
        .collect();
    Json(json!({
        "relay": app.stats.status(),
        "clients": live.clients.status(),
        "groups": groups,
        "upstreams": live.pool.upstream_status(),
    }))
    .into_response()
}

async fn metrics(State(app): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let live = app.live();
    if let Some(response) = reject_admin(&live, &headers) {
        return response;
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render(&app, &live),
    )
        .into_response()
}
