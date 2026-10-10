use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value};

use crate::AppState;
use crate::classify::{Failure, Verdict, classify_status};
use crate::config::{UpstreamApi, UpstreamConfig};
use crate::convert;
use crate::error::{api_error, bearer_matches};
use crate::tokens;
use crate::upstream::Spend;

const MODEL_TIER_HEADER: &str = "x-model-tier";

const PASSTHROUGH_HEADERS: [HeaderName; 3] = [
    header::CONTENT_TYPE,
    header::RETRY_AFTER,
    HeaderName::from_static("x-request-id"),
];

pub async fn responses(
    State(app): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&app, &headers, &body).await
}

struct UpstreamReply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

enum Attempt {
    Reply(UpstreamReply),
    NetworkError(String),
}

async fn forward(app: &AppState, headers: &HeaderMap, body: &Bytes) -> Response {
    let server = &app.config.server;
    if !server.client_tokens.is_empty()
        && !server
            .client_tokens
            .iter()
            .any(|t| bearer_matches(headers, t))
    {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "invalid or missing relay token",
        );
    }

    let payload = match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "request body must be a JSON object",
            );
        }
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("request body is not valid JSON: {error}"),
            );
        }
    };
    let Some(group_name) = payload
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "request body is missing the model field",
        );
    };
    let min_tier = match headers.get(MODEL_TIER_HEADER) {
        None => 0,
        Some(value) => match value
            .to_str()
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
        {
            Some(tier) => tier,
            None => {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    format!("{MODEL_TIER_HEADER} must be a non-negative integer"),
                );
            }
        },
    };
    let Some(group) = app.pool.group(&group_name, min_tier) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "model_not_found",
            format!(
                "unknown model group '{group_name}'; available groups: {}",
                app.pool.group_names().join(", ")
            ),
        );
    };
    if group.members == 0 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "model_not_found",
            format!("model group '{group_name}' has no upstreams with tier >= {min_tier}"),
        );
    }
    if payload.get("stream").and_then(Value::as_bool) == Some(true) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "unsupported",
            "relay does not support streaming responses (stream=true)",
        );
    }

    let chat_payload = if app.config.group_uses_chat(&group_name) {
        match convert::request_to_chat(&payload) {
            Ok(chat) => Some(chat),
            Err(message) => return api_error(StatusCode::BAD_REQUEST, "unsupported", message),
        }
    } else {
        None
    };

    let size = tokens::measure(&payload);
    let started = Instant::now();
    let deadline = started + server.request_deadline();
    let attempt_budget = group.max_attempts.min(group.members);
    let mut tried: Vec<usize> = Vec::new();
    let mut tried_names: Vec<String> = Vec::new();
    let mut last: Option<Attempt> = None;
    let mut saturated_retry_after: Option<Duration> = None;

    while tried.len() < attempt_budget {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let wait_until = (now + server.queue_wait()).min(deadline);
        let lease = match app
            .pool
            .acquire(&group_name, &tried, min_tier, &size, wait_until)
            .await
        {
            Ok(lease) => lease,
            Err(saturated) => {
                saturated_retry_after = saturated.retry_after;
                break;
            }
        };
        tried.push(lease.index);
        let upstream = &lease.upstream.cfg;
        tried_names.push(upstream.name.clone());

        let base = match upstream.api {
            UpstreamApi::Responses => &payload,
            UpstreamApi::ChatCompletions => chat_payload.as_ref().expect(
                "payload has already been converted when the group contains a chat upstream",
            ),
        };
        let request_body = rewrite(base, upstream);
        let url = format!(
            "{}/{}",
            upstream.base_url.trim_end_matches('/'),
            upstream.api.path()
        );
        let timeout = upstream
            .timeout()
            .min(deadline.saturating_duration_since(Instant::now()));
        let attempt_started = Instant::now();
        let attempt = send(&app.client, &url, &upstream.api_key, request_body, timeout).await;
        let latency = attempt_started.elapsed();

        match attempt {
            Attempt::Reply(mut reply) => {
                let mut verdict = classify_status(reply.status.as_u16(), &reply.headers);
                let mut malformed = false;
                if verdict == Verdict::Success && upstream.api == UpstreamApi::ChatCompletions {
                    match convert::response_from_chat(&reply.body) {
                        Some(body) => {
                            reply.body = body.into();
                            reply.headers.insert(
                                header::CONTENT_TYPE,
                                HeaderValue::from_static("application/json"),
                            );
                        }
                        None => {
                            verdict = Verdict::Failover(Failure::Transient);
                            malformed = true;
                            reply = malformed_reply(&upstream.name);
                        }
                    }
                }
                let spend = match verdict {
                    _ if malformed => Spend::Estimate,
                    Verdict::Success => {
                        tokens::usage_tokens(&reply.body).map_or(Spend::Estimate, Spend::Actual)
                    }
                    Verdict::ClientError => {
                        tokens::usage_tokens(&reply.body).map_or(Spend::Refund, Spend::Actual)
                    }
                    Verdict::Failover(Failure::RateLimited { .. }) => Spend::Exhausted,
                    Verdict::Failover(_) => Spend::Refund,
                };
                let remaining = tokens::remaining_tokens(&reply.headers);
                let penalty =
                    lease
                        .upstream
                        .record(verdict, latency, &lease.ticket, spend, remaining);
                match verdict {
                    Verdict::Success | Verdict::ClientError => {
                        tracing::info!(
                            group = %group_name,
                            upstream = %upstream.name,
                            status = reply.status.as_u16(),
                            attempts = tried.len(),
                            latency_ms = latency.as_millis() as u64,
                            total_ms = started.elapsed().as_millis() as u64,
                            "forwarded"
                        );
                        return reply_to_client(reply, &tried_names);
                    }
                    Verdict::Failover(failure) => {
                        log_failover(
                            &group_name,
                            &upstream.name,
                            failure,
                            &penalty,
                            Some(reply.status),
                        );
                        last = Some(Attempt::Reply(reply));
                    }
                }
            }
            Attempt::NetworkError(message) => {
                let penalty = lease.upstream.record(
                    Verdict::Failover(Failure::Transient),
                    latency,
                    &lease.ticket,
                    Spend::Estimate,
                    None,
                );
                tracing::warn!(
                    group = %group_name,
                    upstream = %upstream.name,
                    penalty = ?penalty,
                    error = %message,
                    "upstream request failed, failing over"
                );
                last = Some(Attempt::NetworkError(message));
            }
        }
    }

    match last {
        Some(Attempt::Reply(reply)) => reply_to_client(reply, &tried_names),
        Some(Attempt::NetworkError(message)) => {
            let mut response = api_error(
                StatusCode::from_u16(502).expect("valid HTTP status"),
                "upstream_unreachable",
                format!("all upstreams failed; last error: {message}"),
            );
            set_attempts_header(&mut response, &tried_names);
            response
        }
        None => {
            tracing::warn!(group = %group_name, "all upstreams saturated");
            let mut response = api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "relay_saturated",
                format!(
                    "all upstreams in model group '{group_name}' are busy or cooling down; please retry later"
                ),
            );
            let secs = saturated_retry_after.map_or(1, |d| d.as_secs_f64().ceil().max(1.0) as u64);
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(secs));
            response
        }
    }
}

fn malformed_reply(upstream: &str) -> UpstreamReply {
    let body = serde_json::json!({
        "error": {
            "message": format!("upstream '{upstream}' returned an unrecognized Chat Completions response"),
            "type": "api_error",
            "code": "upstream_bad_response",
        }
    });
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    UpstreamReply {
        status: StatusCode::from_u16(502).expect("valid HTTP status"),
        headers,
        body: body.to_string().into(),
    }
}

fn log_failover(
    group: &str,
    upstream: &str,
    failure: Failure,
    penalty: &Option<crate::classify::Penalty>,
    status: Option<StatusCode>,
) {
    let level_error = failure == Failure::Misconfigured;
    if level_error {
        tracing::error!(group, upstream, status = ?status, penalty = ?penalty, "upstream rejected credentials or model; disabled");
    } else {
        tracing::warn!(group, upstream, status = ?status, penalty = ?penalty, "upstream failed, failing over");
    }
}

async fn send(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    body: Vec<u8>,
    timeout: Duration,
) -> Attempt {
    let response = match client
        .post(url)
        .bearer_auth(api_key)
        .header(header::CONTENT_TYPE, "application/json")
        .timeout(timeout)
        .body(body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => return Attempt::NetworkError(error.to_string()),
    };
    let status = response.status();
    let headers = response.headers().clone();
    match response.bytes().await {
        Ok(body) => Attempt::Reply(UpstreamReply {
            status,
            headers,
            body,
        }),
        Err(error) => Attempt::NetworkError(format!(
            "failed to read response body (HTTP {status}): {error}"
        )),
    }
}

pub fn rewrite(payload: &Map<String, Value>, upstream: &UpstreamConfig) -> Vec<u8> {
    let mut body = payload.clone();
    body.insert("model".to_string(), Value::String(upstream.model.clone()));
    for field in &upstream.drop_fields {
        body.remove(field);
    }
    merge(&mut body, &upstream.overrides);
    serde_json::to_vec(&body).expect("serializing a Value cannot fail")
}

fn merge(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, value) in patch {
        match (target.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(patch)) => merge(existing, patch),
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn reply_to_client(reply: UpstreamReply, tried_names: &[String]) -> Response {
    let mut response = (reply.status, reply.body).into_response();
    for name in PASSTHROUGH_HEADERS {
        if let Some(value) = reply.headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    if let Some(Ok(upstream)) = tried_names.last().map(|n| HeaderValue::from_str(n)) {
        response.headers_mut().insert("x-relay-upstream", upstream);
    }
    set_attempts_header(&mut response, tried_names);
    response
}

fn set_attempts_header(response: &mut Response, tried_names: &[String]) {
    if let Ok(value) = HeaderValue::from_str(&tried_names.join(",")) {
        response.headers_mut().insert("x-relay-attempts", value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rewrite_swaps_model_drops_and_deep_merges() {
        let upstream: UpstreamConfig = toml::from_str(
            r#"
            name = "a"
            base_url = "https://x"
            api_key = "k"
            model = "real-model"
            drop_fields = ["store"]
            override = { reasoning = { summary = "none" }, temperature = 0.3 }
            "#,
        )
        .unwrap();
        let payload = json!({
            "model": "translate",
            "store": false,
            "temperature": 0.0,
            "reasoning": { "effort": "low" },
            "input": [{ "role": "user", "content": "hi" }]
        });
        let out: Value =
            serde_json::from_slice(&rewrite(payload.as_object().unwrap(), &upstream)).unwrap();
        assert_eq!(
            out,
            json!({
                "model": "real-model",
                "temperature": 0.3,
                "reasoning": { "effort": "low", "summary": "none" },
                "input": [{ "role": "user", "content": "hi" }]
            })
        );
    }
}
