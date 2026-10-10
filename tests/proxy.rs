use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bilirelay::config::Config;
use serde_json::{Value, json};

type Behavior = Box<dyn Fn(usize) -> (u16, Vec<(&'static str, &'static str)>) + Send + Sync>;

struct Mock {
    behavior: Behavior,
    delay: Duration,
    calls: Mutex<Vec<(String, Value)>>,
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl Mock {
    fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().unwrap().clone()
    }
}

async fn mock_handler(
    State(mock): State<Arc<Mock>>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> Response {
    let now = mock.current.fetch_add(1, Ordering::SeqCst) + 1;
    mock.peak.fetch_max(now, Ordering::SeqCst);
    let index = {
        let mut calls = mock.calls.lock().unwrap();
        let auth = headers
            .get("authorization")
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        calls.push((auth, serde_json::from_str(&body).unwrap()));
        calls.len() - 1
    };
    tokio::time::sleep(mock.delay).await;
    mock.current.fetch_sub(1, Ordering::SeqCst);
    let (status, extra) = (mock.behavior)(index);
    let body = if uri.path().ends_with("/chat/completions") {
        json!({
            "id": format!("call{index}"),
            "object": "chat.completion",
            "model": "chat-model",
            "choices": [{ "index": 0, "finish_reason": "stop",
                "message": { "role": "assistant", "content": format!("served by call {index}") } }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
        })
    } else {
        json!({
            "status": "completed",
            "served_by_call": index,
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        })
    };
    let mut response = (StatusCode::from_u16(status).unwrap(), body.to_string()).into_response();
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    for (k, v) in extra {
        response.headers_mut().insert(k, v.parse().unwrap());
    }
    response
}

async fn mock_upstream(
    delay: Duration,
    behavior: impl Fn(usize) -> (u16, Vec<(&'static str, &'static str)>) + Send + Sync + 'static,
) -> (String, Arc<Mock>) {
    let mock = Arc::new(Mock {
        behavior: Box::new(behavior),
        delay,
        calls: Mutex::new(Vec::new()),
        current: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/v1/responses", post(mock_handler))
        .route("/v1/chat/completions", post(mock_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/v1"), mock)
}

fn ok() -> impl Fn(usize) -> (u16, Vec<(&'static str, &'static str)>) {
    |_| (200, vec![])
}

fn status(code: u16) -> impl Fn(usize) -> (u16, Vec<(&'static str, &'static str)>) {
    move |_| (code, vec![])
}

async fn relay(server_extra: &str, upstreams: &[(&str, &str, &str)]) -> String {
    let names: Vec<String> = upstreams
        .iter()
        .map(|(n, _, _)| format!("\"{n}\""))
        .collect();
    let mut text = format!(
        r#"
        [server]
        client_tokens = ["client-token"]
        admin_token = "admin-token"
        {server_extra}

        [[groups]]
        name = "translate"
        max_attempts = 3
        upstreams = [{}]
        "#,
        names.join(", ")
    );
    for (name, base_url, extra) in upstreams {
        text.push_str(&format!(
            r#"
            [[upstreams]]
            name = "{name}"
            base_url = "{base_url}"
            api_key = "key-{name}"
            model = "model-{name}"
            {extra}
            "#
        ));
    }
    let config = Config::parse(&text).unwrap();
    let app = bilirelay::build_app(config).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

struct Reply {
    status: u16,
    headers: HeaderMap,
    body: Value,
}

impl Reply {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap())
            .unwrap_or("")
    }
}

async fn call(relay: &str, token: &str, body: Value) -> Reply {
    call_with_headers(relay, token, &[], body).await
}

async fn call_with_headers(
    relay: &str,
    token: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> Reply {
    let mut builder = reqwest::Client::new()
        .post(format!("{relay}/v1/responses"))
        .bearer_auth(token);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = builder
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap_or(Value::Null);
    Reply {
        status,
        headers,
        body,
    }
}

fn request() -> Value {
    json!({
        "model": "translate",
        "temperature": 0.0,
        "instructions": "translate",
        "input": [{ "type": "message", "role": "user", "content": "hello" }],
        "reasoning": { "effort": "low" },
        "max_output_tokens": 8192,
        "store": false
    })
}

#[tokio::test]
async fn forwards_with_upstream_model_and_key_and_keeps_other_fields() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, "drop_fields = [\"store\"]")]).await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["status"], "completed");
    assert_eq!(reply.header("x-relay-upstream"), "a");
    assert_eq!(reply.header("content-type"), "application/json");

    let calls = a.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "Bearer key-a");
    let mut expected = request();
    expected["model"] = json!("model-a");
    expected.as_object_mut().unwrap().remove("store");
    assert_eq!(calls[0].1, expected);
}

#[tokio::test]
async fn rejects_bad_token_unknown_group_and_streaming() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, "")]).await;

    assert_eq!(call(&relay_url, "wrong", request()).await.status, 401);
    let mut unknown = request();
    unknown["model"] = json!("gpt-4");
    let reply = call(&relay_url, "client-token", unknown).await;
    assert_eq!(reply.status, 400);
    assert_eq!(reply.body["error"]["code"], "model_not_found");
    let mut streaming = request();
    streaming["stream"] = json!(true);
    assert_eq!(
        call(&relay_url, "client-token", streaming).await.status,
        400
    );
    assert!(a.calls().is_empty());
}

#[tokio::test]
async fn model_tier_header_only_routes_to_upstreams_at_or_above_tier() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, status(500)).await;
    let (c_url, c) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, ""),
            ("b", &b_url, "tier = 1"),
            ("c", &c_url, "tier = 2"),
        ],
    )
    .await;
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    let reply = call_with_headers(
        &relay_url,
        "client-token",
        &[("x-model-tier", "1")],
        request(),
    )
    .await;
    assert_eq!(
        (reply.status, reply.header("x-relay-attempts")),
        (200, "b,c")
    );
    let reply = call_with_headers(
        &relay_url,
        "client-token",
        &[("x-model-tier", "2")],
        request(),
    )
    .await;
    assert_eq!((reply.status, reply.header("x-relay-attempts")), (200, "c"));
    let reply = call_with_headers(
        &relay_url,
        "client-token",
        &[("x-model-tier", "3")],
        request(),
    )
    .await;
    assert_eq!(reply.status, 400);
    assert_eq!(reply.body["error"]["code"], "model_not_found");
    assert_eq!(
        call_with_headers(
            &relay_url,
            "client-token",
            &[("x-model-tier", "high")],
            request()
        )
        .await
        .status,
        400
    );

    assert_eq!(a.calls().len(), 1);
    assert_eq!(b.calls().len(), 1);
    assert_eq!(c.calls().len(), 2);
}

#[tokio::test]
async fn rate_limited_upstream_fails_over_then_cools_down() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |_| (429, vec![("retry-after", "30")])).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    let first = call(&relay_url, "client-token", request()).await;
    assert_eq!(first.status, 200);
    assert_eq!(first.header("x-relay-attempts"), "a,b");

    for _ in 0..3 {
        let reply = call(&relay_url, "client-token", request()).await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.header("x-relay-attempts"), "b");
    }
    assert_eq!(a.calls().len(), 1);
    assert_eq!(b.calls().len(), 4);

    let bytes = reqwest::Client::new()
        .get(format!("{relay_url}/admin/upstreams"))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let status: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status["upstreams"][0]["state"], "cooldown");
    assert_eq!(status["upstreams"][0]["rate_limited"], 1);
    assert!(
        status["upstreams"][0]["cooldown_remaining_ms"]
            .as_u64()
            .unwrap()
            > 25_000
    );
}

#[tokio::test]
async fn client_errors_are_returned_without_failover() {
    let (a_url, a) = mock_upstream(Duration::ZERO, status(400)).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 400);
    assert_eq!(reply.header("x-relay-attempts"), "a");
    assert_eq!(a.calls().len(), 1);
    assert!(b.calls().is_empty());
}

#[tokio::test]
async fn unauthorized_upstream_is_disabled() {
    let (a_url, a) = mock_upstream(Duration::ZERO, status(401)).await;
    let (b_url, _b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    for _ in 0..3 {
        assert_eq!(
            call(&relay_url, "client-token", request()).await.status,
            200
        );
    }
    assert_eq!(a.calls().len(), 1);
}

#[tokio::test]
async fn unreachable_upstream_fails_over() {
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}/v1", listener.local_addr().unwrap())
    };
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("dead", &dead, ""), ("b", &b_url, "")]).await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-relay-attempts"), "dead,b");
    assert_eq!(b.calls().len(), 1);
}

#[tokio::test]
async fn all_upstreams_failing_returns_last_upstream_error() {
    let (a_url, _a) = mock_upstream(Duration::ZERO, status(503)).await;
    let (b_url, _b) = mock_upstream(Duration::ZERO, |_| (429, vec![("retry-after", "7")])).await;
    let relay_url = relay(
        "queue_wait_secs = 1",
        &[("a", &a_url, ""), ("b", &b_url, "")],
    )
    .await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 429);
    assert_eq!(reply.header("retry-after"), "7");
    assert_eq!(reply.header("x-relay-attempts"), "a,b");

    let saturated = call(&relay_url, "client-token", request()).await;
    assert_eq!(saturated.status, 429);
    assert_eq!(saturated.body["error"]["code"], "relay_saturated");
    let wait: u64 = saturated.header("retry-after").parse().unwrap();
    assert!((1..=7).contains(&wait), "{wait}");
}

#[tokio::test]
async fn queues_until_a_slot_frees_up() {
    let (a_url, a) = mock_upstream(Duration::from_millis(300), ok()).await;
    let relay_url = relay(
        "queue_wait_secs = 5",
        &[("a", &a_url, "max_concurrency = 1")],
    )
    .await;

    let (r1, r2, r3) = tokio::join!(
        call(&relay_url, "client-token", request()),
        call(&relay_url, "client-token", request()),
        call(&relay_url, "client-token", request()),
    );
    assert_eq!([r1.status, r2.status, r3.status], [200, 200, 200]);
    assert_eq!(a.peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gives_up_when_queue_wait_expires() {
    let (a_url, _a) = mock_upstream(Duration::from_millis(2500), ok()).await;
    let relay_url = relay(
        "queue_wait_secs = 1",
        &[("a", &a_url, "max_concurrency = 1")],
    )
    .await;

    let (r1, r2) = tokio::join!(call(&relay_url, "client-token", request()), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        call(&relay_url, "client-token", request()).await
    });
    assert_eq!(r1.status, 200);
    assert_eq!(r2.status, 429);
    assert_eq!(r2.body["error"]["code"], "relay_saturated");
    assert_eq!(r2.header("retry-after"), "1");
}

#[tokio::test]
async fn priority_respects_per_upstream_concurrency_under_load() {
    let (a_url, a) = mock_upstream(Duration::from_millis(100), ok()).await;
    let (b_url, b) = mock_upstream(Duration::from_millis(100), ok()).await;
    let relay_url = relay(
        "queue_wait_secs = 10",
        &[
            ("a", &a_url, "max_concurrency = 3"),
            ("b", &b_url, "max_concurrency = 6"),
        ],
    )
    .await;

    let tasks: Vec<_> = (0..45)
        .map(|_| {
            let relay_url = relay_url.clone();
            tokio::spawn(async move { call(&relay_url, "client-token", request()).await.status })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), 200);
    }
    assert!(a.peak.load(Ordering::SeqCst) <= 3);
    assert!(b.peak.load(Ordering::SeqCst) <= 6);
    let (na, nb) = (a.calls().len(), b.calls().len());
    assert_eq!(na + nb, 45);
    assert!(na > 0 && nb > 0, "a={na} b={nb}");
}

#[tokio::test]
async fn priority_first_retry_goes_to_second_then_third() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |_| (429, vec![("retry-after", "30")])).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, status(503)).await;
    let (c_url, c) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[("a", &a_url, ""), ("b", &b_url, ""), ("c", &c_url, "")],
    )
    .await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-relay-attempts"), "a,b,c");
    assert_eq!(
        (a.calls().len(), b.calls().len(), c.calls().len()),
        (1, 1, 1)
    );
}

#[tokio::test]
async fn priority_prefers_first_and_falls_back_to_second_while_first_cools() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |i| {
        if i == 1 {
            (429, vec![("retry-after", "30")])
        } else {
            (200, vec![])
        }
    })
    .await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let (c_url, c) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[("a", &a_url, ""), ("b", &b_url, ""), ("c", &c_url, "")],
    )
    .await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a,b"
    );
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "b"
    );
    assert_eq!(
        (a.calls().len(), b.calls().len(), c.calls().len()),
        (2, 2, 0)
    );
}

#[tokio::test]
async fn priority_spills_over_when_first_is_at_concurrency_limit() {
    let (a_url, a) = mock_upstream(Duration::from_millis(300), ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let (c_url, c) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, "max_concurrency = 2"),
            ("b", &b_url, ""),
            ("c", &c_url, ""),
        ],
    )
    .await;

    let tasks: Vec<_> = (0..5)
        .map(|_| {
            let relay_url = relay_url.clone();
            tokio::spawn(async move { call(&relay_url, "client-token", request()).await.status })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), 200);
    }
    assert_eq!(
        (a.calls().len(), b.calls().len(), c.calls().len()),
        (2, 3, 0)
    );
}

#[tokio::test]
async fn priority_waits_for_first_slot_before_falling_back() {
    let (a_url, a) = mock_upstream(Duration::from_millis(400), ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, "max_concurrency = 1\nwait_for_slot_secs = 3"),
            ("b", &b_url, ""),
        ],
    )
    .await;

    let (r1, r2, r3) = tokio::join!(
        call(&relay_url, "client-token", request()),
        call(&relay_url, "client-token", request()),
        call(&relay_url, "client-token", request()),
    );
    for reply in [&r1, &r2, &r3] {
        assert_eq!(reply.header("x-relay-attempts"), "a");
    }
    assert_eq!((a.calls().len(), b.calls().len()), (3, 0));
    assert_eq!(a.peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn priority_falls_back_once_slot_wait_expires() {
    let (a_url, _a) = mock_upstream(Duration::from_millis(2000), ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, "max_concurrency = 1\nwait_for_slot_secs = 0.5"),
            ("b", &b_url, ""),
        ],
    )
    .await;

    let (r1, (r2, waited)) = tokio::join!(call(&relay_url, "client-token", request()), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = std::time::Instant::now();
        let reply = call(&relay_url, "client-token", request()).await;
        (reply, started.elapsed())
    });
    assert_eq!(r1.header("x-relay-attempts"), "a");
    assert_eq!(r2.header("x-relay-attempts"), "b");
    assert!(
        waited >= Duration::from_millis(450) && waited < Duration::from_millis(1500),
        "{waited:?}"
    );
    assert_eq!(b.calls().len(), 1);
}

#[tokio::test]
async fn priority_does_not_wait_for_a_cooling_upstream() {
    let (a_url, _a) = mock_upstream(Duration::ZERO, |_| (429, vec![("retry-after", "30")])).await;
    let (b_url, _b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[("a", &a_url, "wait_for_slot_secs = 5"), ("b", &b_url, "")],
    )
    .await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a,b"
    );
    let started = std::time::Instant::now();
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "b"
    );
    assert!(started.elapsed() < Duration::from_millis(300));
}

async fn admin_status(relay: &str) -> Value {
    let bytes = reqwest::Client::new()
        .get(format!("{relay}/admin/upstreams"))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn half_open_sends_one_probe_then_recovers() {
    let (a_url, a) = mock_upstream(Duration::from_millis(300), |i| {
        if i == 0 {
            (429, vec![("retry-after", "1")])
        } else {
            (200, vec![])
        }
    })
    .await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a,b"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        admin_status(&relay_url).await["upstreams"][0]["state"],
        "half_open"
    );

    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let relay_url = relay_url.clone();
            tokio::spawn(async move {
                call(&relay_url, "client-token", request())
                    .await
                    .header("x-relay-attempts")
                    .to_string()
            })
        })
        .collect();
    let mut served = Vec::new();
    for task in tasks {
        served.push(task.await.unwrap());
    }
    served.sort();
    assert_eq!(served, vec!["a", "b", "b", "b"]);
    assert_eq!(a.calls().len(), 2);

    assert_eq!(
        admin_status(&relay_url).await["upstreams"][0]["state"],
        "healthy"
    );
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    assert_eq!(b.calls().len(), 4);
}

#[tokio::test]
async fn failed_probe_goes_back_to_cooldown() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |_| (429, vec![("retry-after", "1")])).await;
    let (b_url, _b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a,b"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a,b"
    );
    let status = admin_status(&relay_url).await;
    assert_eq!(status["upstreams"][0]["state"], "cooldown");
    assert_eq!(status["upstreams"][0]["consecutive_failures"], 2);
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "b"
    );
    assert_eq!(a.calls().len(), 2);
}

fn tpm_available(status: &Value, upstream: usize) -> i64 {
    status["upstreams"][upstream]["tpm_available"]
        .as_i64()
        .unwrap()
}

#[tokio::test]
async fn tpm_budget_is_settled_with_actual_usage() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, "tpm = 60"), ("b", &b_url, "")]).await;

    for _ in 0..5 {
        assert_eq!(
            call(&relay_url, "client-token", request())
                .await
                .header("x-relay-attempts"),
            "a"
        );
    }
    assert_eq!((a.calls().len(), b.calls().len()), (5, 0));
    assert!(tpm_available(&admin_status(&relay_url).await, 0) >= 50);
}

#[tokio::test]
async fn tpm_budget_exhausted_falls_back_without_hitting_upstream() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |_| {
        (200, vec![("x-ratelimit-remaining-tokens", "0")])
    })
    .await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, "tpm = 60"), ("b", &b_url, "")]).await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    for _ in 0..3 {
        assert_eq!(
            call(&relay_url, "client-token", request())
                .await
                .header("x-relay-attempts"),
            "b"
        );
    }
    assert_eq!((a.calls().len(), b.calls().len()), (1, 3));
    assert_eq!(
        admin_status(&relay_url).await["upstreams"][0]["rate_limited"],
        0
    );
}

#[tokio::test]
async fn priority_waits_for_tpm_budget_when_it_refills_within_the_window() {
    let (a_url, a) = mock_upstream(Duration::ZERO, |i| {
        if i == 0 {
            (200, vec![("x-ratelimit-remaining-tokens", "0")])
        } else {
            (200, vec![])
        }
    })
    .await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, "tpm = 6000\nwait_for_slot_secs = 3"),
            ("b", &b_url, ""),
        ],
    )
    .await;

    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    let started = std::time::Instant::now();
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "a"
    );
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(100) && waited < Duration::from_secs(2),
        "{waited:?}"
    );
    assert_eq!((a.calls().len(), b.calls().len()), (2, 0));
}

#[tokio::test]
async fn priority_skips_wait_when_tpm_cannot_refill_within_the_window() {
    let (a_url, _a) = mock_upstream(Duration::ZERO, |_| {
        (200, vec![("x-ratelimit-remaining-tokens", "0")])
    })
    .await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, "tpm = 60\nwait_for_slot_secs = 3"),
            ("b", &b_url, ""),
        ],
    )
    .await;

    call(&relay_url, "client-token", request()).await;
    let started = std::time::Instant::now();
    assert_eq!(
        call(&relay_url, "client-token", request())
            .await
            .header("x-relay-attempts"),
        "b"
    );
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(b.calls().len(), 1);
}

#[tokio::test]
async fn burst_of_rate_limits_cools_down_once() {
    let (a_url, _a) = mock_upstream(Duration::from_millis(200), |_| (429, vec![])).await;
    let (b_url, _b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, ""), ("b", &b_url, "")]).await;

    let tasks: Vec<_> = (0..5)
        .map(|_| {
            let relay_url = relay_url.clone();
            tokio::spawn(async move { call(&relay_url, "client-token", request()).await.status })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), 200);
    }
    let status = admin_status(&relay_url).await;
    assert_eq!(status["upstreams"][0]["rate_limited"], 5);
    assert_eq!(status["upstreams"][0]["consecutive_failures"], 1);
    assert!(
        status["upstreams"][0]["cooldown_remaining_ms"]
            .as_u64()
            .unwrap()
            <= 5_000
    );
}

#[tokio::test]
async fn chat_completions_route_is_not_exposed() {
    let (a_url, _a) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay("", &[("a", &a_url, "")]).await;
    let response = reqwest::Client::new()
        .post(format!("{relay_url}/v1/chat/completions"))
        .bearer_auth("client-token")
        .body(request().to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn chat_upstream_gets_converted_request_and_returns_responses_shape() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[(
            "a",
            &a_url,
            "api = \"chat_completions\"\ndrop_fields = [\"reasoning_effort\"]\ntpm = 60",
        )],
    )
    .await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("content-type"), "application/json");
    assert_eq!(reply.body["object"], "response");
    assert_eq!(reply.body["status"], "completed");
    assert_eq!(reply.body["output"][0]["type"], "message");
    assert_eq!(
        reply.body["output"][0]["content"][0]["text"],
        "served by call 0"
    );
    assert_eq!(reply.body["usage"]["total_tokens"], 2);

    let calls = a.calls();
    assert_eq!(calls[0].0, "Bearer key-a");
    assert_eq!(
        calls[0].1,
        json!({
            "model": "model-a",
            "messages": [
                { "role": "system", "content": "translate" },
                { "role": "user", "content": "hello" }
            ],
            "temperature": 0.0,
            "max_tokens": 8192,
            "stream": false
        })
    );
    assert_eq!(tpm_available(&admin_status(&relay_url).await, 0), 60 - 2);
}

#[tokio::test]
async fn priority_falls_back_from_responses_upstream_to_chat_upstream() {
    let (a_url, a) = mock_upstream(Duration::ZERO, status(429)).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, ""),
            ("b", &b_url, "api = \"chat_completions\""),
        ],
    )
    .await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-relay-attempts"), "a,b");
    assert_eq!(
        reply.body["output"][0]["content"][0]["text"],
        "served by call 0"
    );
    assert_eq!(
        a.calls()[0].1["input"],
        request()["input"],
        "the responses upstream should receive the original request"
    );
    assert!(
        b.calls()[0].1.get("messages").is_some(),
        "the chat upstream should receive the converted request"
    );
}

#[tokio::test]
async fn unconvertible_request_is_rejected_before_any_upstream_is_used() {
    let (a_url, a) = mock_upstream(Duration::ZERO, ok()).await;
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let relay_url = relay(
        "",
        &[
            ("a", &a_url, ""),
            ("b", &b_url, "api = \"chat_completions\""),
        ],
    )
    .await;
    let mut with_tools = request();
    with_tools["tools"] = json!([{ "type": "function", "name": "f" }]);
    let reply = call(&relay_url, "client-token", with_tools).await;
    assert_eq!(reply.status, 400);
    assert_eq!(reply.body["error"]["code"], "unsupported");
    assert!(a.calls().is_empty() && b.calls().is_empty());
}

#[tokio::test]
async fn cline_wrapped_chat_response_is_converted_and_usage_is_settled() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            axum::Json(json!({
                "success": true,
                "data": {
                    "id": "cline-call",
                    "model": "minimax/minimax-m2.5",
                    "choices": [{ "index": 0, "finish_reason": "stop",
                        "message": { "role": "assistant", "content": "Hello" } }],
                    "usage": { "prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7 }
                }
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("http://{addr}/v1");
    let relay_url = relay(
        "",
        &[("cline", &url, "api = \"chat_completions\"\ntpm = 60")],
    )
    .await;

    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-relay-attempts"), "cline");
    assert_eq!(reply.body["object"], "response");
    assert_eq!(reply.body["id"], "resp_cline-call");
    assert_eq!(reply.body["model"], "minimax/minimax-m2.5");
    assert_eq!(reply.body["output"][0]["content"][0]["text"], "Hello");
    assert_eq!(reply.body["usage"]["total_tokens"], 7);
    assert_eq!(tpm_available(&admin_status(&relay_url).await, 0), 60 - 7);
}

#[tokio::test]
async fn unreadable_chat_response_fails_over() {
    let (b_url, b) = mock_upstream(Duration::ZERO, ok()).await;
    let bad_url = garbage_chat_upstream().await;
    let relay_url = relay(
        "",
        &[
            ("bad", &bad_url, "api = \"chat_completions\""),
            ("b", &b_url, ""),
        ],
    )
    .await;
    let reply = call(&relay_url, "client-token", request()).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("x-relay-attempts"), "bad,b");
    assert_eq!(b.calls().len(), 1);
}

async fn garbage_chat_upstream() -> String {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async { r#"{"unexpected":true}"# }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/v1")
}
