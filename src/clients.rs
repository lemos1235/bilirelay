use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use axum::response::Response;
use serde::Serialize;

use crate::config::ClientConfig;
use crate::error::bearer_matches;
use crate::stats::{RelayStats, RelayStatus, Usage};
use crate::tokens::{Bucket, RequestSize};
use crate::upstream::resize;

/// Name reported for callers when no clients are configured and the relay is open.
pub const ANONYMOUS: &str = "anonymous";

pub struct Client {
    pub name: String,
    pub stats: RelayStats,
    limits: Mutex<Limits>,
}

struct Limits {
    tpm: Option<Bucket>,
    rpm: Option<Bucket>,
}

/// Quota held by an admitted request until its outcome is known.
pub struct Reservation {
    tokens: u64,
}

impl Client {
    fn new(name: String) -> Self {
        Self {
            name,
            stats: RelayStats::default(),
            limits: Mutex::new(Limits {
                tpm: None,
                rpm: None,
            }),
        }
    }

    fn set_limits(&self, tpm: Option<u64>, rpm: Option<u64>) {
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap();
        resize(&mut limits.tpm, tpm, now);
        resize(&mut limits.rpm, rpm, now);
    }

    /// Takes one request and the estimated tokens from the client's quotas, or says how long
    /// until they would allow it.
    pub fn admit(&self, size: &RequestSize) -> Result<Reservation, Duration> {
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap();
        let tokens = if limits.tpm.is_some() {
            size.cost(false)
        } else {
            0
        };
        let tpm = limits.tpm.as_mut().and_then(|b| b.ready_at(tokens, now));
        let rpm = limits.rpm.as_mut().and_then(|b| b.ready_at(1, now));
        if let Some(at) = tpm.max(rpm) {
            return Err(at - now);
        }
        if let Some(bucket) = limits.tpm.as_mut() {
            bucket.take(tokens);
        }
        if let Some(bucket) = limits.rpm.as_mut() {
            bucket.take(1);
        }
        Ok(Reservation { tokens })
    }

    /// Settles the token estimate against what the upstream reported; failures that consumed
    /// nothing are refunded. The request itself stays counted against rpm.
    pub fn settle(&self, reservation: Reservation, response: &Response) {
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap();
        let Some(bucket) = limits.tpm.as_mut() else {
            return;
        };
        let reserved = reservation.tokens as f64;
        match response.extensions().get::<Usage>() {
            Some(Usage(used)) => bucket.adjust(reserved - *used as f64, now),
            None if response.status().is_success() => {}
            None => bucket.adjust(reserved, now),
        }
    }

    pub fn status(&self) -> ClientStatus {
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap();
        let budget = |bucket: &mut Option<Bucket>| {
            bucket
                .as_mut()
                .map(|b| (b.capacity(), b.available(now)))
                .unzip()
        };
        let (tpm_limit, tpm_available) = budget(&mut limits.tpm);
        let (rpm_limit, rpm_available) = budget(&mut limits.rpm);
        ClientStatus {
            name: self.name.clone(),
            tpm_limit,
            tpm_available,
            rpm_limit,
            rpm_available,
            stats: self.stats.status(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ClientStatus {
    pub name: String,
    pub tpm_limit: Option<u64>,
    pub tpm_available: Option<i64>,
    pub rpm_limit: Option<u64>,
    pub rpm_available: Option<i64>,
    #[serde(flatten)]
    pub stats: RelayStatus,
}

pub struct Clients {
    /// `(token, client)`; empty when the relay is open.
    entries: Vec<(String, Arc<Client>)>,
    anonymous: Option<Arc<Client>>,
}

impl Clients {
    /// Clients that already exist in `previous` (by name) keep their counters and spent quota.
    pub fn new(configs: &[ClientConfig], previous: Option<&Clients>) -> Self {
        let reuse = |name: &str| {
            let existing = previous.and_then(|p| p.all().find(|c| c.name == name).cloned());
            existing.unwrap_or_else(|| Arc::new(Client::new(name.to_string())))
        };
        let entries = configs
            .iter()
            .map(|cfg| {
                let client = reuse(&cfg.name);
                client.set_limits(cfg.tpm, cfg.rpm);
                (cfg.token.clone(), client)
            })
            .collect::<Vec<_>>();
        let anonymous = entries.is_empty().then(|| reuse(ANONYMOUS));
        Self { entries, anonymous }
    }

    /// The client presenting a valid bearer token, or the anonymous client on an open relay.
    pub fn authenticate(&self, headers: &HeaderMap) -> Option<Arc<Client>> {
        if let Some(anonymous) = &self.anonymous {
            return Some(anonymous.clone());
        }
        self.entries
            .iter()
            .find(|(token, _)| bearer_matches(headers, token))
            .map(|(_, client)| client.clone())
    }

    pub fn all(&self) -> impl Iterator<Item = &Arc<Client>> {
        self.entries.iter().map(|(_, c)| c).chain(&self.anonymous)
    }

    pub fn status(&self) -> Vec<ClientStatus> {
        self.all().map(|c| c.status()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;

    fn config(name: &str, token: &str, tpm: Option<u64>, rpm: Option<u64>) -> ClientConfig {
        ClientConfig {
            name: name.into(),
            token: token.into(),
            tpm,
            rpm,
        }
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    const SIZE: RequestSize = RequestSize {
        input: 100,
        max_output: None,
    };

    #[test]
    fn authenticates_by_token_or_falls_back_to_anonymous() {
        let clients = Clients::new(&[config("a", "ta", None, None)], None);
        assert_eq!(clients.authenticate(&bearer("ta")).unwrap().name, "a");
        assert!(clients.authenticate(&bearer("nope")).is_none());
        assert!(clients.authenticate(&HeaderMap::new()).is_none());

        let open = Clients::new(&[], None);
        assert_eq!(
            open.authenticate(&HeaderMap::new()).unwrap().name,
            ANONYMOUS
        );
    }

    #[test]
    fn rpm_quota_rejects_with_a_wait() {
        let clients = Clients::new(&[config("a", "ta", None, Some(2))], None);
        let client = clients.authenticate(&bearer("ta")).unwrap();
        client.admit(&SIZE).unwrap();
        client.admit(&SIZE).unwrap();
        let wait = client.admit(&SIZE).err().unwrap();
        assert!(wait > Duration::from_secs(25) && wait <= Duration::from_secs(30));
    }

    #[test]
    fn tpm_quota_settles_with_reported_usage_and_refunds_failures() {
        let clients = Clients::new(&[config("a", "ta", Some(1000), None)], None);
        let client = clients.authenticate(&bearer("ta")).unwrap();
        let tpm = |c: &Client| c.status().tpm_available.unwrap();

        let r = client.admit(&SIZE).unwrap();
        assert_eq!(tpm(&client), 800);
        let mut ok = StatusCode::OK.into_response();
        ok.extensions_mut().insert(Usage(50));
        client.settle(r, &ok);
        assert_eq!(tpm(&client), 950);

        let r = client.admit(&SIZE).unwrap();
        client.settle(r, &StatusCode::from_u16(502).unwrap().into_response());
        assert_eq!(tpm(&client), 950);
    }

    #[test]
    fn reload_keeps_counters_and_spent_quota_by_name() {
        let first = Clients::new(&[config("a", "old", None, Some(10))], None);
        let a = first.authenticate(&bearer("old")).unwrap();
        a.admit(&SIZE).unwrap();
        a.stats
            .finish(&StatusCode::OK.into_response(), Duration::ZERO);

        let second = Clients::new(&[config("a", "new", None, Some(5))], Some(&first));
        assert!(second.authenticate(&bearer("old")).is_none());
        let status = second.authenticate(&bearer("new")).unwrap().status();
        assert_eq!(status.stats.requests, 1);
        assert_eq!((status.rpm_limit, status.rpm_available), (Some(5), Some(4)));
    }
}
