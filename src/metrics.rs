//! Prometheus text exposition for GET /metrics.

use std::fmt::Write;
use std::sync::atomic::Ordering;

use crate::clients::ClientStatus;
use crate::stats::RelayStatus;
use crate::upstream::UpstreamStatus;
use crate::{AppState, Live};

const UPSTREAM_STATES: [&str; 5] = ["healthy", "half_open", "cooldown", "disabled", "off"];

type Labels<'a> = Vec<(&'static str, &'a str)>;

#[derive(Default)]
struct Exposition(String);

impl Exposition {
    /// Writes one metric family: its HELP / TYPE header followed by every sample.
    fn family<'a>(
        &mut self,
        name: &str,
        kind: &str,
        help: &str,
        samples: impl IntoIterator<Item = (Labels<'a>, f64)>,
    ) {
        let mut samples = samples.into_iter().peekable();
        if samples.peek().is_none() {
            return;
        }
        let out = &mut self.0;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for (labels, value) in samples {
            out.push_str(name);
            if !labels.is_empty() {
                out.push('{');
                for (i, (key, value)) in labels.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = write!(out, "{key}=\"{}\"", escape(value));
                }
                out.push('}');
            }
            let _ = writeln!(out, " {value}");
        }
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The client-facing request families, once for the relay as a whole and once per client.
fn request_families<'a>(
    out: &mut Exposition,
    prefix: &str,
    scope: &str,
    rows: &[(Labels<'a>, &RelayStatus)],
) {
    let with = |labels: &Labels<'a>, extra: (&'static str, &'a str)| {
        let mut labels = labels.clone();
        labels.push(extra);
        labels
    };
    out.family(
        &format!("{prefix}_requests_total"),
        "counter",
        &format!("Client requests to /v1/responses {scope}, by outcome."),
        rows.iter().flat_map(|(labels, s)| {
            [
                ("success", s.success),
                ("client_error", s.client_errors),
                ("rate_limited", s.rate_limited),
                ("server_error", s.server_errors),
                ("saturated", s.saturated),
                ("quota_limited", s.quota_limited),
            ]
            .map(|(outcome, n)| (with(labels, ("outcome", outcome)), n as f64))
        }),
    );
    out.family(
        &format!("{prefix}_inflight"),
        "gauge",
        &format!("Client requests in progress {scope}."),
        rows.iter().map(|(l, s)| (l.clone(), s.inflight as f64)),
    );
    out.family(
        &format!("{prefix}_tokens_total"),
        "counter",
        &format!("Tokens reported by upstreams for client requests {scope}."),
        rows.iter().map(|(l, s)| (l.clone(), s.tokens as f64)),
    );
    out.family(
        &format!("{prefix}_latency_ewma_seconds"),
        "gauge",
        &format!("Moving average of end-to-end request latency {scope}."),
        rows.iter()
            .map(|(l, s)| (l.clone(), s.latency_ewma_ms as f64 / 1000.0)),
    );
}

pub fn render(app: &AppState, live: &Live) -> String {
    let mut out = Exposition::default();
    let relay = app.stats.status();
    out.family(
        "bilirelay_uptime_seconds",
        "gauge",
        "Seconds since the relay started.",
        [(vec![], relay.uptime_secs as f64)],
    );
    out.family(
        "bilirelay_config_reloads_total",
        "counter",
        "Config reloads, by result.",
        [
            ("success", app.reloads.load(Ordering::Relaxed)),
            ("failure", app.reload_failures.load(Ordering::Relaxed)),
        ]
        .map(|(result, n)| (vec![("result", result)], n as f64)),
    );
    request_families(
        &mut out,
        "bilirelay",
        "across all clients",
        &[(vec![], &relay)],
    );

    let clients = live.clients.status();
    let rows: Vec<_> = clients
        .iter()
        .map(|c| (vec![("client", c.name.as_str())], &c.stats))
        .collect();
    request_families(&mut out, "bilirelay_client", "per client", &rows);
    type Pick = fn(&ClientStatus) -> Option<f64>;
    let budgets: [(&str, &str, Pick); 4] = [
        (
            "bilirelay_client_tpm_limit",
            "Configured tokens per minute for the client.",
            |c| c.tpm_limit.map(|v| v as f64),
        ),
        (
            "bilirelay_client_tpm_available",
            "Tokens the client may still use in the current minute.",
            |c| c.tpm_available.map(|v| v as f64),
        ),
        (
            "bilirelay_client_rpm_limit",
            "Configured requests per minute for the client.",
            |c| c.rpm_limit.map(|v| v as f64),
        ),
        (
            "bilirelay_client_rpm_available",
            "Requests the client may still make in the current minute.",
            |c| c.rpm_available.map(|v| v as f64),
        ),
    ];
    for (name, help, pick) in budgets {
        out.family(
            name,
            "gauge",
            help,
            clients
                .iter()
                .filter_map(|c| Some((vec![("client", c.name.as_str())], pick(c)?))),
        );
    }

    upstream_families(&mut out, &live.pool.upstream_status());
    out.0
}

fn upstream_families(out: &mut Exposition, upstreams: &[UpstreamStatus]) {
    out.family(
        "bilirelay_upstream_state",
        "gauge",
        "1 for the upstream's current health state, 0 for the others.",
        upstreams.iter().flat_map(|u| {
            UPSTREAM_STATES.map(|state| {
                (
                    vec![("upstream", u.name.as_str()), ("state", state)],
                    f64::from(u8::from(u.state == state)),
                )
            })
        }),
    );
    out.family(
        "bilirelay_upstream_responses_total",
        "counter",
        "Attempts sent to the upstream, by outcome.",
        upstreams.iter().flat_map(|u| {
            [
                ("success", u.success),
                ("rate_limited", u.rate_limited),
                ("error", u.errors),
            ]
            .map(|(outcome, n)| {
                (
                    vec![("upstream", u.name.as_str()), ("outcome", outcome)],
                    n as f64,
                )
            })
        }),
    );
    type Pick = fn(&UpstreamStatus) -> Option<f64>;
    let gauges: [(&str, &str, &str, Pick); 10] = [
        (
            "bilirelay_upstream_inflight",
            "gauge",
            "Attempts in progress.",
            |u| Some(u.inflight as f64),
        ),
        (
            "bilirelay_upstream_max_concurrency",
            "gauge",
            "Configured concurrency limit.",
            |u| Some(u.max_concurrency as f64),
        ),
        (
            "bilirelay_upstream_tokens_total",
            "counter",
            "Tokens the upstream reported using.",
            |u| Some(u.tokens as f64),
        ),
        (
            "bilirelay_upstream_latency_ewma_seconds",
            "gauge",
            "Moving average of attempt latency.",
            |u| Some(u.latency_ewma_ms as f64 / 1000.0),
        ),
        (
            "bilirelay_upstream_consecutive_failures",
            "gauge",
            "Failures since the last success.",
            |u| Some(u.consecutive_failures as f64),
        ),
        (
            "bilirelay_upstream_cooldown_remaining_seconds",
            "gauge",
            "Time left before a cooling upstream is tried again.",
            |u| Some(u.cooldown_remaining_ms as f64 / 1000.0),
        ),
        (
            "bilirelay_upstream_tpm_limit",
            "gauge",
            "Configured tokens per minute.",
            |u| u.tpm_limit.map(|v| v as f64),
        ),
        (
            "bilirelay_upstream_tpm_available",
            "gauge",
            "Tokens left in the current minute.",
            |u| u.tpm_available.map(|v| v as f64),
        ),
        (
            "bilirelay_upstream_rpm_limit",
            "gauge",
            "Configured requests per minute.",
            |u| u.rpm_limit.map(|v| v as f64),
        ),
        (
            "bilirelay_upstream_rpm_available",
            "gauge",
            "Requests left in the current minute.",
            |u| u.rpm_available.map(|v| v as f64),
        ),
    ];
    for (name, kind, help, pick) in gauges {
        out.family(
            name,
            kind,
            help,
            upstreams
                .iter()
                .filter_map(|u| Some((vec![("upstream", u.name.as_str())], pick(u)?))),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_families_with_escaped_labels_and_skips_empty_ones() {
        let mut out = Exposition::default();
        out.family(
            "x_total",
            "counter",
            "Help.",
            [(vec![("name", "a\"b\\c")], 3.0), (vec![], 0.5)],
        );
        out.family("empty", "gauge", "Nothing.", Vec::<(Labels, f64)>::new());
        assert_eq!(
            out.0,
            "# HELP x_total Help.\n# TYPE x_total counter\nx_total{name=\"a\\\"b\\\\c\"} 3\nx_total 0.5\n"
        );
    }
}
