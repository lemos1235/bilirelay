pub mod classify;
pub mod config;
pub mod convert;
pub mod error;
pub mod pool;
pub mod proxy;
pub mod tokens;
pub mod upstream;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::config::Config;
use crate::error::{api_error, bearer_matches};
use crate::pool::Pool;

pub struct AppState {
    pub config: Config,
    pub pool: Pool,
    pub client: reqwest::Client,
}

pub fn build_app(config: Config) -> anyhow::Result<Router> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .context("failed to build HTTP client")?;
    let body_limit = config.server.max_body_bytes;
    let state = Arc::new(AppState {
        pool: Pool::new(&config),
        config,
        client,
    });
    Ok(Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/responses", post(proxy::responses))
        .route("/admin/upstreams", get(admin_upstreams))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state))
}

async fn admin_upstreams(State(app): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(token) = app.config.server.admin_token.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !bearer_matches(&headers, token) {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_admin_token",
            "admin token is invalid or missing",
        );
    }
    let groups: Vec<_> = app
        .config
        .groups
        .iter()
        .map(|g| json!({ "name": g.name, "strategy": g.strategy, "max_attempts": g.max_attempts, "upstreams": g.upstreams }))
        .collect();
    Json(json!({ "groups": groups, "upstreams": app.pool.upstream_status() })).into_response()
}
