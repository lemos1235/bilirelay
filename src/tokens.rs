use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use serde_json::{Map, Value};

const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestSize {
    pub input: u64,
    pub max_output: Option<u64>,
}

impl RequestSize {
    /// Estimated TPM reservation: input plus max output tokens, or input x 2 by default.
    pub fn cost(&self, count_max_output: bool) -> u64 {
        if count_max_output {
            self.input + self.max_output.unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS)
        } else {
            self.input * 2
        }
    }
}

pub fn measure(payload: &Map<String, Value>) -> RequestSize {
    let mut counter = TextCounter::default();
    for (key, value) in payload {
        if key != "model" {
            counter.visit(value);
        }
    }
    let max_output = ["max_output_tokens", "max_completion_tokens", "max_tokens"]
        .iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_u64));
    RequestSize {
        input: counter.tokens(),
        max_output,
    }
}

#[derive(Default)]
struct TextCounter {
    cjk: u64,
    other: u64,
    strings: u64,
}

impl TextCounter {
    fn visit(&mut self, value: &Value) {
        match value {
            Value::String(text) => {
                self.strings += 1;
                for ch in text.chars() {
                    if is_cjk(ch) {
                        self.cjk += 1;
                    } else {
                        self.other += 1;
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|v| self.visit(v)),
            Value::Object(map) => map.values().for_each(|v| self.visit(v)),
            _ => {}
        }
    }

    fn tokens(&self) -> u64 {
        self.cjk + self.other.div_ceil(4) + self.strings
    }
}

fn is_cjk(ch: char) -> bool {
    matches!(ch,
        '\u{3040}'..='\u{30ff}'
        | '\u{3400}'..='\u{4dbf}'
        | '\u{4e00}'..='\u{9fff}'
        | '\u{ac00}'..='\u{d7af}'
        | '\u{f900}'..='\u{faff}'
        | '\u{ff00}'..='\u{ffef}'
        | '\u{3000}'..='\u{303f}'
    )
}

pub fn usage_tokens(body: &[u8]) -> Option<u64> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let usage = value.get("usage")?;
    if let Some(total) = usage.get("total_tokens").and_then(Value::as_u64) {
        return Some(total);
    }
    let pair = |a: &str, b: &str| Some(usage.get(a)?.as_u64()? + usage.get(b)?.as_u64()?);
    pair("input_tokens", "output_tokens").or_else(|| pair("prompt_tokens", "completion_tokens"))
}

pub fn remaining_tokens(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-ratelimit-remaining-tokens")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[derive(Debug, Clone)]
pub struct Bucket {
    capacity: f64,
    per_sec: f64,
    available: f64,
    updated: Instant,
}

impl Bucket {
    pub fn per_minute(limit: u64, now: Instant) -> Self {
        let capacity = limit as f64;
        Self {
            capacity,
            per_sec: capacity / 60.0,
            available: capacity,
            updated: now,
        }
    }

    /// Applies a new per-minute limit; what was spent in the current window still counts.
    pub fn resize(&mut self, limit: u64, now: Instant) {
        self.refill(now);
        let spent = self.capacity - self.available;
        self.capacity = limit as f64;
        self.per_sec = self.capacity / 60.0;
        self.available = self.capacity - spent;
    }

    pub fn capacity(&self) -> u64 {
        self.capacity as u64
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.available = (self.available + elapsed * self.per_sec).min(self.capacity);
        self.updated = self.updated.max(now);
    }

    pub fn ready_at(&mut self, cost: u64, now: Instant) -> Option<Instant> {
        self.refill(now);
        let need = (cost as f64).min(self.capacity);
        if self.available >= need {
            None
        } else {
            Some(now + Duration::from_secs_f64((need - self.available) / self.per_sec))
        }
    }

    pub fn take(&mut self, cost: u64) {
        self.available -= cost as f64;
    }

    pub fn adjust(&mut self, delta: f64, now: Instant) {
        self.refill(now);
        self.available = (self.available + delta).min(self.capacity);
    }

    pub fn drain(&mut self, now: Instant) {
        self.refill(now);
        self.available = self.available.min(0.0);
    }

    pub fn cap(&mut self, remaining: u64, now: Instant) {
        self.refill(now);
        self.available = self.available.min(remaining as f64);
    }

    pub fn available(&mut self, now: Instant) -> i64 {
        self.refill(now);
        self.available.floor() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn measures_cjk_and_latin_text_and_reads_output_limit() {
        let payload = json!({
            "model": "this-is-not-counted",
            "instructions": "\u{4f60}\u{597d}\u{4e16}\u{754c}",          // 4
            "input": [{ "role": "user", "content": "abcdefgh" }], // "user" 1 + "abcdefgh" 2
            "max_output_tokens": 8192
        });
        let size = measure(payload.as_object().unwrap());
        assert_eq!(size.input, 10);
        assert_eq!(size.max_output, Some(8192));
        assert_eq!(size.cost(true), 10 + 8192);
        assert_eq!(size.cost(false), 20);
        let no_limit = RequestSize {
            input: 10,
            max_output: None,
        };
        assert_eq!(no_limit.cost(true), 10 + 4096);
    }

    #[test]
    fn reads_usage_from_both_protocols() {
        assert_eq!(
            usage_tokens(br#"{"usage":{"input_tokens":30,"output_tokens":12}}"#),
            Some(42)
        );
        assert_eq!(
            usage_tokens(br#"{"usage":{"prompt_tokens":30,"completion_tokens":2}}"#),
            Some(32)
        );
        assert_eq!(
            usage_tokens(br#"{"usage":{"total_tokens":7,"input_tokens":1,"output_tokens":1}}"#),
            Some(7)
        );
        assert_eq!(usage_tokens(br#"{"output":[]}"#), None);
        assert_eq!(usage_tokens(b"not json"), None);
    }

    #[test]
    fn bucket_refills_linearly_and_reports_when_ready() {
        let t0 = Instant::now();
        let mut bucket = Bucket::per_minute(600, t0);
        assert_eq!(bucket.ready_at(600, t0), None);
        bucket.take(600);
        let ready = bucket.ready_at(100, t0).unwrap();
        assert_eq!((ready - t0).as_secs_f64().round(), 10.0);
        assert_eq!(bucket.ready_at(100, t0 + Duration::from_secs(10)), None);
        assert_eq!(
            bucket.available(t0 + Duration::from_secs(120)),
            600,
            "refilling should not exceed capacity"
        );
    }

    #[test]
    fn oversized_request_waits_for_a_full_bucket() {
        let t0 = Instant::now();
        let mut bucket = Bucket::per_minute(60, t0);
        assert_eq!(bucket.ready_at(1000, t0), None);
        bucket.take(1000);
        assert_eq!(bucket.available(t0), -940);
    }

    #[test]
    fn settle_drain_and_cap() {
        let t0 = Instant::now();
        let mut bucket = Bucket::per_minute(1000, t0);
        bucket.take(400);
        bucket.adjust(150.0, t0);
        assert_eq!(bucket.available(t0), 750);
        bucket.cap(100, t0);
        assert_eq!(bucket.available(t0), 100);
        bucket.cap(900, t0);
        assert_eq!(
            bucket.available(t0),
            100,
            "the cap should only tighten, never loosen"
        );
        bucket.drain(t0);
        assert_eq!(bucket.available(t0), 0);
    }

    #[test]
    fn resize_keeps_spent_budget() {
        let t0 = Instant::now();
        let mut bucket = Bucket::per_minute(600, t0);
        bucket.take(500);
        bucket.resize(6000, t0);
        assert_eq!(bucket.available(t0), 5500);
        bucket.resize(100, t0);
        assert_eq!(bucket.available(t0), -400);
        assert_eq!(bucket.capacity(), 100);
    }
}
