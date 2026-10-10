use std::time::Duration;

use axum::http::HeaderMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    RateLimited { retry_after: Option<Duration> },
    Transient,
    Misconfigured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Success,
    Failover(Failure),
    ClientError,
}

pub fn classify_status(status: u16, headers: &HeaderMap) -> Verdict {
    match status {
        200..=299 => Verdict::Success,
        429 => Verdict::Failover(Failure::RateLimited {
            retry_after: retry_after(headers),
        }),
        408 | 425 | 500..=599 => Verdict::Failover(Failure::Transient),
        401 | 403 | 404 => Verdict::Failover(Failure::Misconfigured),
        _ => Verdict::ClientError,
    }
}

pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Penalty {
    Cooldown(Duration),
    Disable(Duration),
}

const RATE_LIMIT_BASE: Duration = Duration::from_secs(5);
const TRANSIENT_BASE: Duration = Duration::from_secs(3);
const COOLDOWN_CAP: Duration = Duration::from_secs(60);
const DISABLE_FOR: Duration = Duration::from_secs(600);

pub fn penalty(failure: Failure, consecutive: u32) -> Penalty {
    let backoff = |base: Duration| {
        let shift = consecutive.saturating_sub(1).min(16);
        base.saturating_mul(1 << shift).min(COOLDOWN_CAP)
    };
    match failure {
        Failure::RateLimited {
            retry_after: Some(wait),
        } => Penalty::Cooldown(wait.min(COOLDOWN_CAP)),
        Failure::RateLimited { retry_after: None } => Penalty::Cooldown(backoff(RATE_LIMIT_BASE)),
        Failure::Transient => Penalty::Cooldown(backoff(TRANSIENT_BASE)),
        Failure::Misconfigured => Penalty::Disable(DISABLE_FOR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(*k, v.parse().unwrap());
        }
        map
    }

    #[test]
    fn status_classes() {
        let none = HeaderMap::new();
        for status in [200, 201] {
            assert_eq!(classify_status(status, &none), Verdict::Success);
        }
        for status in [408, 425, 500, 502, 503, 504] {
            assert_eq!(
                classify_status(status, &none),
                Verdict::Failover(Failure::Transient),
                "{status}"
            );
        }
        for status in [401, 403, 404] {
            assert_eq!(
                classify_status(status, &none),
                Verdict::Failover(Failure::Misconfigured),
                "{status}"
            );
        }
        for status in [400, 413, 422] {
            assert_eq!(
                classify_status(status, &none),
                Verdict::ClientError,
                "{status}"
            );
        }
    }

    #[test]
    fn rate_limit_reads_retry_after() {
        assert_eq!(
            classify_status(429, &headers(&[("retry-after", " 7 ")])),
            Verdict::Failover(Failure::RateLimited {
                retry_after: Some(Duration::from_secs(7))
            })
        );
        assert_eq!(
            classify_status(
                429,
                &headers(&[("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")])
            ),
            Verdict::Failover(Failure::RateLimited { retry_after: None })
        );
    }

    #[test]
    fn penalties_back_off_exponentially_and_cap() {
        let secs = |p| match p {
            Penalty::Cooldown(d) => d.as_secs(),
            Penalty::Disable(_) => unreachable!(),
        };
        let limited = Failure::RateLimited { retry_after: None };
        assert_eq!(secs(penalty(limited, 1)), 5);
        assert_eq!(secs(penalty(limited, 2)), 10);
        assert_eq!(secs(penalty(limited, 10)), 60);
        assert_eq!(secs(penalty(Failure::Transient, 1)), 3);
        assert_eq!(secs(penalty(Failure::Transient, 3)), 12);
        assert_eq!(secs(penalty(Failure::Transient, u32::MAX)), 60);
        assert_eq!(
            secs(penalty(
                Failure::RateLimited {
                    retry_after: Some(Duration::from_secs(2))
                },
                9
            )),
            2
        );
        assert_eq!(
            penalty(Failure::Misconfigured, 1),
            Penalty::Disable(Duration::from_secs(600))
        );
    }
}
