use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::{Config, GroupConfig};
use crate::tokens::RequestSize;
use crate::upstream::{Admission, Ticket, Upstream, UpstreamStatus};

pub struct Pool {
    upstreams: Vec<Arc<Upstream>>,
    groups: HashMap<String, GroupRuntime>,
    released: Arc<Notify>,
}

struct GroupRuntime {
    cfg: GroupConfig,
    members: Vec<usize>,
}

pub struct Lease {
    pub upstream: Arc<Upstream>,
    pub index: usize,
    pub ticket: Ticket,
    released: Arc<Notify>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.upstream.release(&self.ticket);
        self.released.notify_waiters();
    }
}

#[derive(Debug)]
pub struct Saturated {
    pub retry_after: Option<Duration>,
}

#[derive(Debug, Clone, Copy)]
pub struct GroupInfo {
    pub max_attempts: usize,
    pub members: usize,
}

impl Pool {
    pub fn new(config: &Config) -> Self {
        let upstreams: Vec<Arc<Upstream>> = config
            .upstreams
            .iter()
            .cloned()
            .map(|cfg| Arc::new(Upstream::new(cfg)))
            .collect();
        let groups = config
            .groups
            .iter()
            .map(|group| {
                let members: Vec<usize> = group
                    .upstreams
                    .iter()
                    .map(|name| {
                        upstreams
                            .iter()
                            .position(|u| &u.cfg.name == name)
                            .expect("configuration validation ensures that group members exist")
                    })
                    .collect();
                let runtime = GroupRuntime {
                    cfg: group.clone(),
                    members,
                };
                (group.name.clone(), runtime)
            })
            .collect();
        Self {
            upstreams,
            groups,
            released: Arc::new(Notify::new()),
        }
    }

    pub fn group(&self, name: &str, min_tier: u32) -> Option<GroupInfo> {
        let group = self.groups.get(name)?;
        Some(GroupInfo {
            max_attempts: group.cfg.max_attempts,
            members: group
                .members
                .iter()
                .filter(|i| self.upstreams[**i].cfg.tier >= min_tier)
                .count(),
        })
    }

    pub fn group_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.groups.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn upstream_status(&self) -> Vec<UpstreamStatus> {
        let now = Instant::now();
        self.upstreams.iter().map(|u| u.status(now)).collect()
    }

    pub async fn acquire(
        &self,
        group: &str,
        tried: &[usize],
        min_tier: u32,
        size: &RequestSize,
        wait_until: Instant,
    ) -> Result<Lease, Saturated> {
        let runtime = self
            .groups
            .get(group)
            .expect("the caller has already verified that the group exists");
        let mut excluded = tried.to_vec();
        excluded
            .extend((0..self.upstreams.len()).filter(|&i| self.upstreams[i].cfg.tier < min_tier));
        let excluded = excluded.as_slice();
        let mut holds: HashMap<usize, Instant> = HashMap::new();
        loop {
            let notified = self.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let now = Instant::now();
            let (earliest_unblock, hold_until) = {
                let hold_until =
                    match self.try_pick(runtime, excluded, size, now, &mut holds, wait_until) {
                        Pick::Leased(index, ticket) => {
                            return Ok(Lease {
                                upstream: self.upstreams[index].clone(),
                                index,
                                ticket,
                                released: self.released.clone(),
                            });
                        }
                        Pick::Hold(until) => Some(until),
                        Pick::NoneAvailable => None,
                    };
                let earliest_unblock = runtime
                    .members
                    .iter()
                    .filter(|i| !excluded.contains(i) && self.upstreams[**i].cfg.enabled)
                    .filter_map(|i| match self.upstreams[*i].admission(size, now) {
                        Admission::Blocked(at) | Admission::Budget(at) => Some(at),
                        _ => None,
                    })
                    .min();
                (earliest_unblock, hold_until)
            };

            if now >= wait_until {
                return Err(Saturated {
                    retry_after: earliest_unblock.map(|t| t - now),
                });
            }
            let wake_at = [earliest_unblock, hold_until]
                .into_iter()
                .flatten()
                .fold(wait_until, Instant::min);
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(wake_at.into()) => {}
            }
        }
    }

    fn try_pick(
        &self,
        runtime: &GroupRuntime,
        excluded: &[usize],
        size: &RequestSize,
        now: Instant,
        holds: &mut HashMap<usize, Instant>,
        wait_until: Instant,
    ) -> Pick {
        if let Some(until) = self.priority_hold(runtime, excluded, size, now, holds, wait_until) {
            return Pick::Hold(until);
        }
        for index in runtime
            .members
            .iter()
            .copied()
            .filter(|i| !excluded.contains(i))
        {
            let upstream = &self.upstreams[index];
            if upstream.cfg.enabled
                && let Some(ticket) = upstream.try_reserve(size, now)
            {
                return Pick::Leased(index, ticket);
            }
        }
        Pick::NoneAvailable
    }

    fn priority_hold(
        &self,
        runtime: &GroupRuntime,
        excluded: &[usize],
        size: &RequestSize,
        now: Instant,
        holds: &mut HashMap<usize, Instant>,
        wait_until: Instant,
    ) -> Option<Instant> {
        for index in runtime
            .members
            .iter()
            .copied()
            .filter(|i| !excluded.contains(i))
        {
            let upstream = &self.upstreams[index];
            if !upstream.cfg.enabled {
                continue;
            }
            let budget_ready = match upstream.admission(size, now) {
                Admission::Ready => return None,
                Admission::Blocked(_) | Admission::ProbeBusy => continue,
                Admission::Full => None,
                Admission::Budget(at) => Some(at),
            };
            let wait = upstream.cfg.wait_for_slot();
            if wait.is_zero() {
                continue;
            }
            let until = (*holds.entry(index).or_insert(now) + wait).min(wait_until);
            match budget_ready {
                Some(at) if at <= until => return Some(at),
                Some(_) => continue,
                None if now < until => return Some(until),
                None => continue,
            }
        }
        None
    }
}

enum Pick {
    Leased(usize, Ticket),
    Hold(Instant),
    NoneAvailable,
}
