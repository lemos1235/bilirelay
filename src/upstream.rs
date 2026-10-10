use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::classify::{Failure, Penalty, Verdict, penalty};
use crate::config::UpstreamConfig;
use crate::tokens::{Bucket, RequestSize};

pub struct Upstream {
    pub cfg: UpstreamConfig,
    inflight: AtomicUsize,
    health: Mutex<Health>,
    requests: AtomicU64,
    success: AtomicU64,
    rate_limited: AtomicU64,
    errors: AtomicU64,
    latency_ewma_ms: AtomicU64,
}

struct Health {
    cooldown_until: Option<Instant>,
    disabled_until: Option<Instant>,
    consecutive_failures: u32,
    probing: bool,
    failure_epoch: u64,
    tpm: Option<Bucket>,
    rpm: Option<Bucket>,
}

impl Health {
    fn blocked_until(&self, now: Instant) -> Option<Instant> {
        [self.cooldown_until, self.disabled_until]
            .into_iter()
            .flatten()
            .filter(|until| *until > now)
            .max()
    }

    fn half_open(&self, now: Instant) -> bool {
        self.consecutive_failures > 0 && self.blocked_until(now).is_none()
    }

    fn budget_ready_at(&mut self, tokens: u64, now: Instant) -> Option<Instant> {
        let tpm = self.tpm.as_mut().and_then(|b| b.ready_at(tokens, now));
        let rpm = self.rpm.as_mut().and_then(|b| b.ready_at(1, now));
        tpm.max(rpm)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Ready,
    Blocked(Instant),
    ProbeBusy,
    Full,
    Budget(Instant),
}

#[derive(Debug, Clone, Copy)]
pub struct Ticket {
    pub probe: bool,
    epoch: u64,
    reserved_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spend {
    Actual(u64),
    Estimate,
    Refund,
    Exhausted,
}

impl Upstream {
    pub fn new(cfg: UpstreamConfig) -> Self {
        let now = Instant::now();
        let health = Health {
            cooldown_until: None,
            disabled_until: None,
            consecutive_failures: 0,
            probing: false,
            failure_epoch: 0,
            tpm: cfg.tpm.map(|limit| Bucket::per_minute(limit, now)),
            rpm: cfg.rpm.map(|limit| Bucket::per_minute(limit, now)),
        };
        Self {
            cfg,
            inflight: AtomicUsize::new(0),
            health: Mutex::new(health),
            requests: AtomicU64::new(0),
            success: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_ewma_ms: AtomicU64::new(0),
        }
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }

    fn token_cost(&self, size: &RequestSize) -> u64 {
        if self.cfg.tpm.is_some() {
            size.cost(self.cfg.tpm_count_max_output)
        } else {
            0
        }
    }

    pub fn admission(&self, size: &RequestSize, now: Instant) -> Admission {
        let mut health = self.health.lock().unwrap();
        if let Some(until) = health.blocked_until(now) {
            return Admission::Blocked(until);
        }
        if health.half_open(now) && health.probing {
            return Admission::ProbeBusy;
        }
        if self.inflight() >= self.cfg.max_concurrency {
            return Admission::Full;
        }
        match health.budget_ready_at(self.token_cost(size), now) {
            Some(at) => Admission::Budget(at),
            None => Admission::Ready,
        }
    }

    pub fn try_reserve(&self, size: &RequestSize, now: Instant) -> Option<Ticket> {
        let mut health = self.health.lock().unwrap();
        if health.blocked_until(now).is_some() {
            return None;
        }
        let probe = health.half_open(now);
        if probe && health.probing {
            return None;
        }
        let tokens = self.token_cost(size);
        if health.budget_ready_at(tokens, now).is_some() {
            return None;
        }
        let max = self.cfg.max_concurrency;
        self.inflight
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()?;
        if let Some(bucket) = health.tpm.as_mut() {
            bucket.take(tokens);
        }
        if let Some(bucket) = health.rpm.as_mut() {
            bucket.take(1);
        }
        if probe {
            health.probing = true;
            tracing::info!(upstream = %self.cfg.name, failures = health.consecutive_failures, "half-open: sending probe");
        }
        Some(Ticket {
            probe,
            epoch: health.failure_epoch,
            reserved_tokens: tokens,
        })
    }

    pub fn release(&self, ticket: &Ticket) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        if ticket.probe {
            self.health.lock().unwrap().probing = false;
        }
    }

    pub fn record(
        &self,
        verdict: Verdict,
        latency: Duration,
        ticket: &Ticket,
        spend: Spend,
        remaining_tokens: Option<u64>,
    ) -> Option<Penalty> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut health = self.health.lock().unwrap();
        if let Some(bucket) = health.tpm.as_mut() {
            match spend {
                Spend::Actual(used) => {
                    bucket.adjust(ticket.reserved_tokens as f64 - used as f64, now)
                }
                Spend::Estimate => {}
                Spend::Refund => bucket.adjust(ticket.reserved_tokens as f64, now),
                Spend::Exhausted => bucket.drain(now),
            }
            if let Some(remaining) = remaining_tokens {
                bucket.cap(remaining, now);
            }
        }

        let fresh = ticket.epoch == health.failure_epoch;
        match verdict {
            Verdict::Success | Verdict::ClientError => {
                if verdict == Verdict::Success {
                    self.success.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                }
                self.observe_latency(latency);
                if fresh {
                    if health.consecutive_failures > 0 {
                        tracing::info!(upstream = %self.cfg.name, failures = health.consecutive_failures, "upstream recovered");
                    }
                    health.consecutive_failures = 0;
                    health.cooldown_until = None;
                }
                None
            }
            Verdict::Failover(failure) => {
                if matches!(failure, Failure::RateLimited { .. }) {
                    self.rate_limited.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                }
                if fresh {
                    health.consecutive_failures = health.consecutive_failures.saturating_add(1);
                    health.failure_epoch += 1;
                }
                let applied = penalty(failure, health.consecutive_failures.max(1));
                let extend = |slot: &mut Option<Instant>, d: Duration| {
                    let until = now + d;
                    if fresh || slot.is_none_or(|current| current < until) {
                        *slot = Some(until);
                    }
                };
                match applied {
                    Penalty::Cooldown(d) => extend(&mut health.cooldown_until, d),
                    Penalty::Disable(d) => extend(&mut health.disabled_until, d),
                }
                Some(applied)
            }
        }
    }

    fn observe_latency(&self, latency: Duration) {
        let sample = latency.as_millis().min(u64::MAX as u128) as u64;
        let prev = self.latency_ewma_ms.load(Ordering::Relaxed);
        let next = if prev == 0 {
            sample
        } else {
            (prev * 4 + sample) / 5
        };
        self.latency_ewma_ms.store(next, Ordering::Relaxed);
    }

    pub fn status(&self, now: Instant) -> UpstreamStatus {
        let mut health = self.health.lock().unwrap();
        let tpm_available = health.tpm.as_mut().map(|b| b.available(now));
        let rpm_available = health.rpm.as_mut().map(|b| b.available(now));
        let remaining = |until: Option<Instant>| {
            until
                .filter(|u| *u > now)
                .map(|u| (u - now).as_millis() as u64)
                .unwrap_or(0)
        };
        let disabled_ms = remaining(health.disabled_until);
        let cooldown_ms = remaining(health.cooldown_until);
        let state = if !self.cfg.enabled {
            "off"
        } else if disabled_ms > 0 {
            "disabled"
        } else if cooldown_ms > 0 {
            "cooldown"
        } else if health.consecutive_failures > 0 {
            "half_open"
        } else {
            "healthy"
        };
        UpstreamStatus {
            name: self.cfg.name.clone(),
            model: self.cfg.model.clone(),
            state,
            inflight: self.inflight(),
            max_concurrency: self.cfg.max_concurrency,
            cooldown_remaining_ms: cooldown_ms,
            disabled_remaining_ms: disabled_ms,
            consecutive_failures: health.consecutive_failures,
            requests: self.requests.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            latency_ewma_ms: self.latency_ewma_ms.load(Ordering::Relaxed),
            tpm_limit: self.cfg.tpm,
            tpm_available,
            rpm_limit: self.cfg.rpm,
            rpm_available,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UpstreamStatus {
    pub name: String,
    pub model: String,
    pub state: &'static str,
    pub inflight: usize,
    pub max_concurrency: usize,
    pub cooldown_remaining_ms: u64,
    pub disabled_remaining_ms: u64,
    pub consecutive_failures: u32,
    pub requests: u64,
    pub success: u64,
    pub rate_limited: u64,
    pub errors: u64,
    pub latency_ewma_ms: u64,
    pub tpm_limit: Option<u64>,
    pub tpm_available: Option<i64>,
    pub rpm_limit: Option<u64>,
    pub rpm_available: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(extra: &str) -> Upstream {
        Upstream::new(
            toml::from_str(&format!(
                r#"
                name = "a"
                base_url = "https://x"
                api_key = "k"
                model = "m"
                max_concurrency = 5
                {extra}
                "#
            ))
            .unwrap(),
        )
    }

    const SIZE: RequestSize = RequestSize {
        input: 100,
        max_output: Some(1000),
    };

    const EXPIRED_COOLDOWN: Verdict = Verdict::Failover(Failure::RateLimited {
        retry_after: Some(Duration::ZERO),
    });

    fn fail(u: &Upstream, verdict: Verdict) -> Option<Penalty> {
        let ticket = u
            .try_reserve(&SIZE, Instant::now())
            .expect("reservation should succeed");
        let applied = u.record(verdict, Duration::ZERO, &ticket, Spend::Refund, None);
        u.release(&ticket);
        applied
    }

    #[test]
    fn half_open_admits_a_single_probe() {
        let u = upstream("");
        fail(&u, EXPIRED_COOLDOWN);

        let now = Instant::now();
        assert_eq!(u.admission(&SIZE, now), Admission::Ready);
        assert!(
            u.try_reserve(&SIZE, now).unwrap().probe,
            "the first request should be a probe"
        );
        assert_eq!(u.admission(&SIZE, now), Admission::ProbeBusy);
        assert!(
            u.try_reserve(&SIZE, now).is_none(),
            "no second request should be allowed while a probe is in flight"
        );
        assert_eq!(u.status(now).state, "half_open");
    }

    #[test]
    fn successful_probe_closes_the_circuit() {
        let u = upstream("");
        fail(&u, EXPIRED_COOLDOWN);
        let now = Instant::now();
        let probe = u.try_reserve(&SIZE, now).unwrap();
        u.record(
            Verdict::Success,
            Duration::ZERO,
            &probe,
            Spend::Estimate,
            None,
        );
        u.release(&probe);
        assert_eq!(u.status(now).state, "healthy");
        for _ in 0..5 {
            assert!(!u.try_reserve(&SIZE, now).unwrap().probe);
        }
    }

    #[test]
    fn failed_probe_cools_down_again_with_a_longer_penalty() {
        let u = upstream("");
        fail(&u, EXPIRED_COOLDOWN);
        assert_eq!(
            fail(&u, Verdict::Failover(Failure::Transient)),
            Some(Penalty::Cooldown(Duration::from_secs(6)))
        );
        assert!(matches!(
            u.admission(&SIZE, Instant::now()),
            Admission::Blocked(_)
        ));
    }

    #[test]
    fn abandoned_probe_does_not_wedge_the_upstream() {
        let u = upstream("");
        fail(&u, EXPIRED_COOLDOWN);
        let now = Instant::now();
        let probe = u.try_reserve(&SIZE, now).unwrap();
        u.release(&probe);
        assert!(
            u.try_reserve(&SIZE, now).unwrap().probe,
            "another probe should be allowed"
        );
    }

    #[test]
    fn a_burst_of_in_flight_failures_counts_once() {
        let u = upstream("");
        let now = Instant::now();
        let tickets: Vec<Ticket> = (0..5).map(|_| u.try_reserve(&SIZE, now).unwrap()).collect();
        let limited = Verdict::Failover(Failure::RateLimited { retry_after: None });
        let penalties: Vec<_> = tickets
            .iter()
            .map(|t| u.record(limited, Duration::ZERO, t, Spend::Exhausted, None))
            .collect();
        assert!(
            penalties
                .iter()
                .all(|p| *p == Some(Penalty::Cooldown(Duration::from_secs(5))))
        );
        assert_eq!(u.status(now).consecutive_failures, 1);
    }

    #[test]
    fn stale_success_does_not_lift_cooldown() {
        let u = upstream("");
        let now = Instant::now();
        let early = u.try_reserve(&SIZE, now).unwrap();
        let failing = u.try_reserve(&SIZE, now).unwrap();
        let limited = Verdict::Failover(Failure::RateLimited { retry_after: None });
        u.record(limited, Duration::ZERO, &failing, Spend::Exhausted, None);
        u.record(
            Verdict::Success,
            Duration::ZERO,
            &early,
            Spend::Estimate,
            None,
        );
        assert_eq!(u.status(Instant::now()).state, "cooldown");
    }

    #[test]
    fn tpm_budget_gates_and_settles_with_actual_usage() {
        let u = upstream("tpm = 1000");
        let now = Instant::now();
        let tickets: Vec<Ticket> = (0..5).map(|_| u.try_reserve(&SIZE, now).unwrap()).collect();
        assert_eq!(u.status(now).tpm_available, Some(0));
        assert!(
            matches!(u.admission(&SIZE, now), Admission::Full),
            "the concurrency limit has also been reached"
        );
        u.release(&tickets[0]);
        assert!(
            matches!(u.admission(&SIZE, now), Admission::Budget(_)),
            "a concurrency slot is available but the rate limit has been reached"
        );

        u.record(
            Verdict::Success,
            Duration::ZERO,
            &tickets[1],
            Spend::Actual(50),
            None,
        );
        assert_eq!(u.status(now).tpm_available, Some(150));
        assert!(matches!(u.admission(&SIZE, now), Admission::Budget(_)));
        u.record(
            Verdict::ClientError,
            Duration::ZERO,
            &tickets[4],
            Spend::Refund,
            None,
        );
        assert_eq!(u.status(now).tpm_available, Some(350));
        assert_eq!(u.admission(&SIZE, now), Admission::Ready);
        u.record(
            Verdict::Success,
            Duration::ZERO,
            &tickets[2],
            Spend::Estimate,
            Some(100),
        );
        assert_eq!(u.status(now).tpm_available, Some(100));
        u.record(
            Verdict::Failover(Failure::RateLimited { retry_after: None }),
            Duration::ZERO,
            &tickets[3],
            Spend::Exhausted,
            None,
        );
        assert!(u.status(now).tpm_available.unwrap() <= 0);
    }

    #[test]
    fn tpm_cost_follows_max_output_accounting() {
        let u = upstream("tpm = 5000\ntpm_count_max_output = true");
        let now = Instant::now();
        for _ in 0..4 {
            u.try_reserve(&SIZE, now).unwrap();
        }
        assert_eq!(u.status(now).tpm_available, Some(600));
        assert!(matches!(u.admission(&SIZE, now), Admission::Budget(_)));
    }

    #[test]
    fn rpm_budget_counts_requests() {
        let u = upstream("rpm = 2");
        let now = Instant::now();
        u.try_reserve(&SIZE, now).unwrap();
        u.try_reserve(&SIZE, now).unwrap();
        assert!(matches!(u.admission(&SIZE, now), Admission::Budget(_)));
        assert!(u.try_reserve(&SIZE, now).is_none());
    }
}
