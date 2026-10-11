use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{OnceLock, mpsc};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::symbols::{Marker, line};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Bar, BarChart, BarGroup, Block, BorderType, Borders, Cell, Chart, Clear, Dataset,
    GraphType, LegendPosition, LineGauge, Padding, Paragraph, Row, Sparkline, Table, TableState,
};
use ratatui::{DefaultTerminal, Frame};
use serde::Deserialize;
use tokio::sync::watch;

use crate::config::Config;

/// Samples older than this are dropped; it bounds the largest selectable window.
const HISTORY_SECS: f64 = 600.0;
/// Readouts (tables, labels, bars) average over this span; charts plot raw samples.
const SMOOTH_SECS: f64 = 3.0;
const WINDOWS: [f64; 5] = [30.0, 60.0, 120.0, 300.0, 600.0];
const INTERVALS: [f64; 6] = [0.25, 0.5, 1.0, 2.0, 5.0, 10.0];
const MARKERS: [(Marker, &str); 5] = [
    (Marker::Braille, "braille"),
    (Marker::HalfBlock, "half-block"),
    (Marker::Octant, "octant"),
    (Marker::Dot, "dot"),
    (Marker::Block, "block"),
];
const GRAPHS: [(GraphType, &str); 3] = [
    (GraphType::Line, "line"),
    (GraphType::Scatter, "scatter"),
    (GraphType::Bar, "bar"),
];
/// Per-upstream trace colors, assigned in order of first appearance. The first five are spread
/// around the hue wheel (cyan, orange, violet, lime, pink) so any pair stays distinguishable,
/// including under red-green colour blindness; the tail only matters past five upstreams.
const PALETTE: [u32; 8] = [
    0x7dcfff, 0xff8c42, 0x9d7cff, 0xa3e635, 0xff6ac1, 0xffd75f, 0x5eead4, 0x6c8cff,
];

/// A Tokyo Night–style palette; falls back to the nearest xterm-256 colour without truecolor.
mod theme {
    use super::{Color, OnceLock};

    pub fn hex(rgb: u32) -> Color {
        static TRUECOLOR: OnceLock<bool> = OnceLock::new();
        let truecolor = *TRUECOLOR.get_or_init(|| {
            std::env::var("COLORTERM").is_ok_and(|v| v.contains("truecolor") || v.contains("24bit"))
        });
        let [_, r, g, b] = rgb.to_be_bytes();
        if truecolor {
            Color::Rgb(r, g, b)
        } else {
            Color::Indexed(xterm256(r, g, b))
        }
    }

    fn xterm256(r: u8, g: u8, b: u8) -> u8 {
        const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
        let nearest = |c: u8| {
            (0..6)
                .min_by_key(|&i| (LEVELS[i] as i32 - c as i32).abs())
                .unwrap_or(0)
        };
        let (ri, gi, bi) = (nearest(r), nearest(g), nearest(b));
        let cube = (LEVELS[ri], LEVELS[gi], LEVELS[bi]);
        let gray_step = ((r as u32 + g as u32 + b as u32) / 3).saturating_sub(3) / 10;
        let gray_step = gray_step.min(23) as u8;
        let gray = 8 + gray_step * 10;
        let dist = |(x, y, z): (u8, u8, u8)| {
            [(x, r), (y, g), (z, b)]
                .iter()
                .map(|&(a, b)| (a as i32 - b as i32).pow(2))
                .sum::<i32>()
        };
        if dist((gray, gray, gray)) < dist(cube) {
            232 + gray_step
        } else {
            16 + 36 * ri as u8 + 6 * gi as u8 + bi as u8
        }
    }

    pub fn text() -> Color {
        hex(0xc0caf5)
    }
    pub fn muted() -> Color {
        hex(0x737aa2)
    }
    pub fn border() -> Color {
        hex(0x3b4261)
    }
    pub fn surface() -> Color {
        hex(0x1f2335)
    }
    pub fn raised() -> Color {
        hex(0x2f354f)
    }
    pub fn selection() -> Color {
        hex(0x283457)
    }
    pub fn ink() -> Color {
        hex(0x16161e)
    }
    pub fn accent() -> Color {
        hex(0x7aa2f7)
    }
    pub fn cyan() -> Color {
        hex(0x7dcfff)
    }
    pub fn green() -> Color {
        hex(0x9ece6a)
    }
    pub fn yellow() -> Color {
        hex(0xe0af68)
    }
    pub fn orange() -> Color {
        hex(0xff9e64)
    }
    pub fn red() -> Color {
        hex(0xf7768e)
    }
    pub fn magenta() -> Color {
        hex(0xbb9af7)
    }
    pub fn blue() -> Color {
        hex(0x2ac3de)
    }
}

pub const USAGE: &str = "Usage: bilirelay monitor [-c|--config <PATH>] [--url <URL>] [--token <TOKEN>] [--interval <SECS>]

Live terminal dashboard backed by GET /admin/upstreams.
The URL and admin token default to server.bind and server.admin_token in the config.
Press ? inside the dashboard for key bindings.";

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub url: String,
    pub token: Option<String>,
    pub interval: Duration,
}

pub fn parse_args<I, T>(args: I) -> anyhow::Result<Options>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let mut iter = args
        .into_iter()
        .map(|a| a.into().to_string_lossy().into_owned());
    let mut config_path = None;
    let mut url = None;
    let mut token = None;
    let mut interval = Duration::from_secs(1);

    while let Some(arg) = iter.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => {
                (flag.to_string(), Some(value.to_string()))
            }
            _ => (arg.clone(), None),
        };
        let mut value = |name: &str| {
            inline
                .clone()
                .or_else(|| iter.next())
                .with_context(|| format!("missing value for {name}"))
        };
        match flag.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "-c" | "--config" => config_path = Some(PathBuf::from(value("--config")?)),
            "--url" => url = Some(value("--url")?),
            "--token" => token = Some(value("--token")?),
            "--interval" => {
                let secs: f64 = value("--interval")?
                    .parse()
                    .context("--interval must be a number of seconds")?;
                if !(secs >= 0.1 && secs.is_finite()) {
                    bail!("--interval must be at least 0.1 seconds");
                }
                interval = Duration::from_secs_f64(secs);
            }
            other if !other.starts_with('-') && config_path.is_none() => {
                config_path = Some(PathBuf::from(other));
            }
            other => bail!("unknown argument '{other}'\n\n{USAGE}"),
        }
    }

    if url.is_none() || token.is_none() {
        let path = config_path.unwrap_or_else(|| PathBuf::from("config.toml"));
        match Config::load(&path) {
            Ok(config) => {
                url = url.or_else(|| Some(url_from_bind(&config.server.bind)));
                token = token.or(config.server.admin_token);
            }
            Err(error) if url.is_none() => {
                return Err(error.context("pass --url, or point --config at the relay config"));
            }
            Err(_) => {}
        }
    }

    Ok(Options {
        url: url
            .expect("url is resolved above")
            .trim_end_matches('/')
            .to_string(),
        token,
        interval,
    })
}

fn url_from_bind(bind: &str) -> String {
    match bind.parse::<SocketAddr>() {
        Ok(mut addr) => {
            if addr.ip().is_unspecified() {
                addr.set_ip(if addr.is_ipv4() {
                    [127, 0, 0, 1].into()
                } else {
                    std::net::Ipv6Addr::LOCALHOST.into()
                });
            }
            format!("http://{addr}")
        }
        Err(_) => format!("http://{bind}"),
    }
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    /// Absent on relays that predate client-side stats.
    #[serde(default)]
    relay: Option<RelayView>,
    groups: Vec<GroupView>,
    upstreams: Vec<UpstreamView>,
}

#[derive(Debug, Deserialize)]
struct RelayView {
    uptime_secs: u64,
    inflight: usize,
    requests: u64,
    success: u64,
    client_errors: u64,
    rate_limited: u64,
    server_errors: u64,
    saturated: u64,
    latency_ewma_ms: u64,
}

#[derive(Debug, Deserialize)]
struct GroupView {
    name: String,
    upstreams: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UpstreamView {
    name: String,
    model: String,
    state: String,
    inflight: usize,
    max_concurrency: usize,
    cooldown_remaining_ms: u64,
    disabled_remaining_ms: u64,
    consecutive_failures: u32,
    requests: u64,
    success: u64,
    rate_limited: u64,
    errors: u64,
    #[serde(default)]
    tokens: u64,
    latency_ewma_ms: u64,
    tpm_limit: Option<u64>,
    tpm_available: Option<i64>,
    rpm_limit: Option<u64>,
    rpm_available: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Metric {
    Rps,
    Ok,
    Limited,
    Errors,
    Tokens,
    Saturated,
    Latency,
    Inflight,
    SuccessRate,
    Tpm,
    Rpm,
}

/// Metrics `0..RATE_COUNT` are derived from counter deltas; the rest are point-in-time gauges.
const RATE_COUNT: usize = 6;
const METRIC_COUNT: usize = 11;

/// What the global view's main chart plots; `m` cycles through these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    /// One metric, one line per upstream.
    Metric(Metric),
    /// ok / 429 / errors summed over all upstreams.
    Outcomes,
}

const FOCUSES: [Focus; 11] = [
    Focus::Metric(Metric::Rps),
    Focus::Outcomes,
    Focus::Metric(Metric::Ok),
    Focus::Metric(Metric::Limited),
    Focus::Metric(Metric::Errors),
    Focus::Metric(Metric::Tokens),
    Focus::Metric(Metric::Latency),
    Focus::Metric(Metric::Inflight),
    Focus::Metric(Metric::SuccessRate),
    Focus::Metric(Metric::Tpm),
    Focus::Metric(Metric::Rpm),
];

impl Focus {
    fn label(self) -> &'static str {
        match self {
            Self::Metric(metric) => metric.label(),
            Self::Outcomes => "Outcomes/s",
        }
    }
}

impl Metric {
    fn label(self) -> &'static str {
        match self {
            Self::Rps => "Requests/s",
            Self::Ok => "OK/s",
            Self::Limited => "429/s",
            Self::Errors => "Errors/s",
            Self::Tokens => "Tokens/s",
            Self::Saturated => "Saturated/s",
            Self::Latency => "Latency (EWMA)",
            Self::Inflight => "Inflight",
            Self::SuccessRate => "Success rate",
            Self::Tpm => "TPM available",
            Self::Rpm => "RPM available",
        }
    }

    fn format(self, v: f64) -> String {
        if !v.is_finite() {
            return "-".to_string();
        }
        match self {
            Self::Rps | Self::Ok | Self::Limited | Self::Errors | Self::Saturated => {
                format!("{v:.2}/s")
            }
            Self::Tokens => format!("{}/s", compact(v.round() as i64)),
            Self::Latency => format!("{v:.0}ms"),
            Self::Inflight => format!("{v:.0}"),
            Self::SuccessRate => format!("{v:.1}%"),
            Self::Tpm | Self::Rpm => compact(v.round() as i64),
        }
    }

    fn axis_label(self, v: f64, top: f64) -> String {
        match self {
            Self::Latency => format!("{v:.0}ms"),
            Self::SuccessRate => format!("{v:.0}%"),
            Self::Tokens | Self::Tpm | Self::Rpm => compact(v.round() as i64),
            _ if top >= 10.0 => format!("{v:.0}"),
            _ => format!("{v:.1}"),
        }
    }

    fn ceiling(self, max: f64) -> f64 {
        match self {
            Self::SuccessRate => 100.0,
            _ => nice_ceiling(max),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Global,
    Upstream,
    Relay,
}

const VIEWS: [View; 3] = [View::Global, View::Upstream, View::Relay];

impl View {
    fn title(self) -> &'static str {
        match self {
            Self::Global => "1 Global",
            Self::Upstream => "2 Upstream",
            Self::Relay => "3 Relay",
        }
    }
}

/// How the global view draws its charts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChartStyle {
    Bars,
    Wave,
}

const STYLES: [ChartStyle; 2] = [ChartStyle::Bars, ChartStyle::Wave];

/// Waveform strips per row in the global view.
const COLUMNS: [usize; 3] = [1, 2, 3];

impl ChartStyle {
    fn title(self) -> &'static str {
        match self {
            Self::Wave => "Wave",
            Self::Bars => "Bars",
        }
    }
}

type Fetched = (Instant, Duration, Result<Snapshot, String>);

pub async fn run(options: Options) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;
    let (tx, rx) = mpsc::channel();
    let (interval_tx, interval_rx) = watch::channel(options.interval);
    let poller = tokio::spawn(poll(
        client,
        format!("{}/admin/upstreams", options.url),
        options.token.clone(),
        interval_rx,
        tx,
    ));
    let result = tokio::task::spawn_blocking(move || {
        let mut terminal = ratatui::init();
        let result = App::new(options.url, interval_tx).run(&mut terminal, rx);
        ratatui::restore();
        result
    })
    .await
    .context("monitor UI thread panicked")?;
    poller.abort();
    result
}

async fn poll(
    client: reqwest::Client,
    endpoint: String,
    token: Option<String>,
    mut interval: watch::Receiver<Duration>,
    tx: mpsc::Sender<Fetched>,
) {
    loop {
        let begun = Instant::now();
        let result = fetch(&client, &endpoint, token.as_deref()).await;
        if tx.send((Instant::now(), begun.elapsed(), result)).is_err() {
            return;
        }
        let wait = interval.borrow_and_update().saturating_sub(begun.elapsed());
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            changed = interval.changed() => if changed.is_err() { return },
        }
    }
}

async fn fetch(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Snapshot, String> {
    let mut request = client.get(endpoint);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("cannot reach {endpoint}: {e}"))?;
    match response.status().as_u16() {
        200..=299 => {}
        404 => {
            return Err(
                "admin endpoint disabled: set server.admin_token in the relay config".into(),
            );
        }
        401 => {
            return Err(
                "admin token rejected: pass --token or set admin_token in the config".into(),
            );
        }
        status => return Err(format!("unexpected HTTP {status} from {endpoint}")),
    }
    let body = response
        .bytes()
        .await
        .map_err(|e| format!("failed to read response: {e}"))?;
    serde_json::from_slice(&body).map_err(|e| format!("unrecognized response: {e}"))
}

struct Sample {
    counters: [Option<u64>; RATE_COUNT],
    gauges: [f64; METRIC_COUNT - RATE_COUNT],
}

impl Sample {
    fn upstream(u: &UpstreamView) -> Self {
        let budget = |v: Option<i64>| v.map_or(f64::NAN, |v| v as f64);
        Self {
            counters: [
                Some(u.requests),
                Some(u.success),
                Some(u.rate_limited),
                Some(u.errors),
                Some(u.tokens),
                None,
            ],
            gauges: [
                u.latency_ewma_ms as f64,
                u.inflight as f64,
                ratio_pct(u.success, u.requests),
                budget(u.tpm_available),
                budget(u.rpm_available),
            ],
        }
    }

    /// All upstreams summed, for the global outcomes chart.
    fn totals(upstreams: &[UpstreamView]) -> Self {
        let sum = |f: fn(&UpstreamView) -> u64| Some(upstreams.iter().map(f).sum());
        Self {
            counters: [
                sum(|u| u.requests),
                sum(|u| u.success),
                sum(|u| u.rate_limited),
                sum(|u| u.errors),
                sum(|u| u.tokens),
                None,
            ],
            gauges: [f64::NAN; METRIC_COUNT - RATE_COUNT],
        }
    }

    fn relay(r: &RelayView, tokens: u64) -> Self {
        Self {
            counters: [
                Some(r.requests),
                Some(r.success),
                Some(r.rate_limited),
                Some(r.client_errors + r.server_errors),
                Some(tokens),
                Some(r.saturated),
            ],
            gauges: [
                r.latency_ewma_ms as f64,
                r.inflight as f64,
                ratio_pct(r.success, r.requests),
                f64::NAN,
                f64::NAN,
            ],
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Point {
    t: f64,
    /// Seconds since the previous sample, so rates can be integrated back into counts.
    dt: f64,
    v: [f64; METRIC_COUNT],
}

#[derive(Default)]
struct Series {
    prev: Option<(Instant, [Option<u64>; RATE_COUNT])>,
    points: VecDeque<Point>,
}

impl Series {
    fn push(&mut self, at: Instant, t: f64, sample: Sample) {
        if let Some((prev_at, prev)) = self.prev {
            let dt = (at - prev_at).as_secs_f64().max(1e-3);
            let mut v = [f64::NAN; METRIC_COUNT];
            for (slot, (now, before)) in v.iter_mut().zip(sample.counters.iter().zip(prev)) {
                if let (Some(now), Some(before)) = (now, before) {
                    // Counters going backwards means the relay restarted.
                    *slot = now.saturating_sub(before) as f64 / dt;
                }
            }
            v[RATE_COUNT..].copy_from_slice(&sample.gauges);
            self.points.push_back(Point { t, dt, v });
        }
        self.prev = Some((at, sample.counters));
        while self.points.front().is_some_and(|p| p.t < t - HISTORY_SECS) {
            self.points.pop_front();
        }
    }

    #[cfg(test)]
    fn last(&self, metric: Metric) -> f64 {
        self.points
            .back()
            .map_or(f64::NAN, |p| p.v[metric as usize])
    }

    /// Mean over the last few seconds, so fast sampling doesn't make readouts flicker.
    fn recent(&self, metric: Metric) -> f64 {
        self.points
            .back()
            .map_or(f64::NAN, |p| self.mean(metric, p.t - SMOOTH_SECS))
    }

    fn values(&self, metric: Metric, since: f64) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.points
            .iter()
            .filter(move |p| p.t >= since)
            .map(move |p| (p.t, p.v[metric as usize]))
            .filter(|(_, v)| v.is_finite())
    }

    fn peak(&self, metric: Metric, since: f64) -> f64 {
        self.values(metric, since)
            .map(|(_, v)| v)
            .fold(0.0, f64::max)
    }

    /// Events counted since `since`, for counter-derived metrics.
    fn total(&self, metric: Metric, since: f64) -> f64 {
        self.points
            .iter()
            .filter(|p| p.t >= since)
            .map(|p| p.v[metric as usize] * p.dt)
            .filter(|v| v.is_finite())
            .sum()
    }

    fn mean(&self, metric: Metric, since: f64) -> f64 {
        let (sum, n) = self
            .values(metric, since)
            .fold((0.0, 0usize), |(s, n), (_, v)| (s + v, n + 1));
        if n == 0 { f64::NAN } else { sum / n as f64 }
    }

    /// Buckets samples into `cols` columns (max per column), holding the last value across gaps.
    fn resample(&self, metric: Metric, t0: f64, t1: f64, cols: usize) -> Vec<Option<f64>> {
        let mut out = vec![None; cols];
        if cols == 0 || t1 <= t0 {
            return out;
        }
        let step = (t1 - t0) / cols as f64;
        for (t, v) in self.values(metric, t0) {
            let col = (((t - t0) / step).ceil() as usize)
                .saturating_sub(1)
                .min(cols - 1);
            out[col] = Some(out[col].map_or(v, |o: f64| o.max(v)));
        }
        let mut last = None;
        for slot in &mut out {
            match slot {
                Some(v) => last = Some(*v),
                None => *slot = last,
            }
        }
        out
    }
}

/// One plotted line: a series, the metric drawn from it, and how it is labelled.
struct Trace<'a> {
    name: String,
    color: Color,
    series: Option<&'a Series>,
    metric: Metric,
}

/// One headline number in the KPI strip.
struct Card<'a> {
    title: &'static str,
    value: String,
    color: Color,
    trend: Option<(&'a Series, Metric)>,
    footer: Vec<Span<'static>>,
}

struct App {
    url: String,
    started: Instant,
    now_t: f64,
    snapshot: Option<Snapshot>,
    updated: Option<Instant>,
    fetch_latency: Duration,
    error: Option<String>,
    series: HashMap<String, Series>,
    relay: Series,
    totals: Series,
    colors: HashMap<String, Color>,
    table: TableState,
    view: usize,
    focus: usize,
    window: usize,
    marker: usize,
    graph: usize,
    interval: watch::Sender<Duration>,
    style: usize,
    columns: usize,
    paused: bool,
    help: bool,
}

impl App {
    fn new(url: String, interval: watch::Sender<Duration>) -> Self {
        Self {
            url,
            started: Instant::now(),
            now_t: 0.0,
            snapshot: None,
            updated: None,
            fetch_latency: Duration::ZERO,
            error: None,
            series: HashMap::new(),
            relay: Series::default(),
            totals: Series::default(),
            colors: HashMap::new(),
            table: TableState::default().with_selected(0),
            view: 0,
            focus: 0,
            window: 2,
            marker: 0,
            graph: 0,
            interval,
            style: 0,
            columns: 0,
            paused: false,
            help: false,
        }
    }

    fn run(
        mut self,
        terminal: &mut DefaultTerminal,
        rx: mpsc::Receiver<Fetched>,
    ) -> anyhow::Result<()> {
        loop {
            loop {
                match rx.try_recv() {
                    Ok(fetched) if !self.paused => self.ingest(fetched),
                    Ok(_) => {}
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => bail!("metrics poller stopped"),
                }
            }
            terminal.draw(|frame| self.draw(frame))?;
            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
                && self.handle_key(key)
            {
                return Ok(());
            }
        }
    }

    /// Returns true when the monitor should exit.
    fn handle_key(&mut self, key: KeyEvent) -> bool {
        let cycle = |i: usize, len: usize, back: bool| {
            if back {
                (i + len - 1) % len
            } else {
                (i + 1) % len
            }
        };
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
            KeyCode::Esc if self.help => self.help = false,
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('?') => self.help = !self.help,
            // Only the upstream view is about a single upstream; elsewhere there is nothing to select.
            _ if VIEWS[self.view] != View::Upstream
                && matches!(
                    key.code,
                    KeyCode::Up
                        | KeyCode::Down
                        | KeyCode::Left
                        | KeyCode::Right
                        | KeyCode::Char('h' | 'j' | 'k' | 'l')
                ) => {}
            KeyCode::Tab => self.view = cycle(self.view, VIEWS.len(), false),
            KeyCode::BackTab => self.view = cycle(self.view, VIEWS.len(), true),
            KeyCode::Char(c @ '1'..='3') => self.view = c as usize - '1' as usize,
            // `m`, `v` and `c` only shape the global view's chart; `c` only its waves.
            KeyCode::Char('m' | 'M' | 'v' | 'V' | 'c' | 'C')
                if VIEWS[self.view] != View::Global => {}
            KeyCode::Char('c' | 'C') if STYLES[self.style] != ChartStyle::Wave => {}
            // `g` and `b` only style line charts.
            KeyCode::Char('g' | 'G' | 'b' | 'B') if !self.has_lines() => {}
            KeyCode::Char('m') => self.focus = cycle(self.focus, FOCUSES.len(), false),
            KeyCode::Char('M') => self.focus = cycle(self.focus, FOCUSES.len(), true),
            KeyCode::Char('w') => self.window = cycle(self.window, WINDOWS.len(), false),
            KeyCode::Char('W') => self.window = cycle(self.window, WINDOWS.len(), true),
            KeyCode::Char('b') => self.marker = cycle(self.marker, MARKERS.len(), false),
            KeyCode::Char('B') => self.marker = cycle(self.marker, MARKERS.len(), true),
            KeyCode::Char('g') => self.graph = cycle(self.graph, GRAPHS.len(), false),
            KeyCode::Char('G') => self.graph = cycle(self.graph, GRAPHS.len(), true),
            KeyCode::Char('v') => self.style = cycle(self.style, STYLES.len(), false),
            KeyCode::Char('V') => self.style = cycle(self.style, STYLES.len(), true),
            KeyCode::Char('c') => self.columns = cycle(self.columns, COLUMNS.len(), false),
            KeyCode::Char('C') => self.columns = cycle(self.columns, COLUMNS.len(), true),
            KeyCode::Char(' ') | KeyCode::Char('p') => self.paused = !self.paused,
            KeyCode::Char('r') => {
                self.series.clear();
                self.relay = Series::default();
                self.totals = Series::default();
            }
            KeyCode::Char('+') | KeyCode::Char('=') => self.step_interval(1),
            KeyCode::Char('-') | KeyCode::Char('_') => self.step_interval(-1),
            KeyCode::Down | KeyCode::Right | KeyCode::Char('j' | 'l') => self.select(1),
            KeyCode::Up | KeyCode::Left | KeyCode::Char('k' | 'h') => self.select(-1),
            _ => {}
        }
        false
    }

    fn step_interval(&self, step: isize) {
        let current = self.interval.borrow().as_secs_f64();
        let index = INTERVALS
            .iter()
            .position(|v| *v >= current - 1e-9)
            .unwrap_or(INTERVALS.len() - 1) as isize;
        let next = (index + step).clamp(0, INTERVALS.len() as isize - 1) as usize;
        self.interval
            .send_replace(Duration::from_secs_f64(INTERVALS[next]));
    }

    fn select(&mut self, step: isize) {
        let len = self.upstreams().len();
        if len == 0 {
            return;
        }
        let current = self.table.selected().unwrap_or(0) as isize;
        self.table
            .select(Some((current + step).rem_euclid(len as isize) as usize));
    }

    fn ingest(&mut self, (at, latency, result): Fetched) {
        self.fetch_latency = latency;
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        self.error = None;
        self.updated = Some(at);
        self.now_t = (at - self.started).as_secs_f64();
        for upstream in &snapshot.upstreams {
            let next = self.colors.len();
            self.colors
                .entry(upstream.name.clone())
                .or_insert_with(|| theme::hex(PALETTE[next % PALETTE.len()]));
            self.series.entry(upstream.name.clone()).or_default().push(
                at,
                self.now_t,
                Sample::upstream(upstream),
            );
        }
        self.totals
            .push(at, self.now_t, Sample::totals(&snapshot.upstreams));
        if let Some(relay) = &snapshot.relay {
            let tokens = snapshot.upstreams.iter().map(|u| u.tokens).sum();
            self.relay
                .push(at, self.now_t, Sample::relay(relay, tokens));
        }
        if self
            .table
            .selected()
            .is_none_or(|i| i >= snapshot.upstreams.len())
        {
            self.table.select(Some(0));
        }
        self.snapshot = Some(snapshot);
    }

    fn focus(&self) -> Focus {
        FOCUSES[self.focus]
    }

    fn window(&self) -> f64 {
        WINDOWS[self.window]
    }

    fn since(&self) -> f64 {
        self.now_t - self.window()
    }

    fn color(&self, name: &str) -> Color {
        self.colors.get(name).copied().unwrap_or_else(theme::text)
    }

    /// Whether the current view draws a line chart; the global view never does.
    fn has_lines(&self) -> bool {
        VIEWS[self.view] != View::Global || STYLES[self.style] == ChartStyle::Wave
    }

    fn upstreams(&self) -> &[UpstreamView] {
        self.snapshot.as_ref().map_or(&[], |s| &s.upstreams)
    }

    fn selected(&self) -> Option<&UpstreamView> {
        self.upstreams().get(self.table.selected().unwrap_or(0))
    }

    fn traces(&self, metric: Metric) -> Vec<Trace<'_>> {
        self.upstreams()
            .iter()
            .map(|u| Trace {
                name: u.name.clone(),
                color: self.color(&u.name),
                series: self.series.get(&u.name),
                metric,
            })
            .collect()
    }

    fn draw(&mut self, frame: &mut Frame) {
        let [sidebar, main] = Layout::horizontal([
            Constraint::Length(self.sidebar_width()),
            Constraint::Fill(1),
        ])
        .areas(frame.area());
        let [top, cards, groups, body, status] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(main);
        self.draw_sidebar(frame, sidebar);
        self.draw_topbar(frame, top);
        self.draw_cards(frame, cards);
        self.draw_groups(frame, groups);
        match VIEWS[self.view] {
            View::Global => self.draw_global(frame, body),
            View::Upstream => self.draw_upstream(frame, body),
            View::Relay => self.draw_relay(frame, body),
        }
        self.draw_status(frame, status);
        if self.help {
            draw_help(frame);
        }
    }

    fn sidebar_width(&self) -> u16 {
        let longest = self
            .upstreams()
            .iter()
            .map(|u| u.name.chars().count())
            .max()
            .unwrap_or(0) as u16;
        // "▌ ● name" plus padding and the right border, within sensible bounds.
        (longest + 7).clamp(16, 24)
    }

    fn draw_sidebar(&self, frame: &mut Frame, area: Rect) {
        let width = area.width.saturating_sub(1) as usize;
        let section = |title: &str| {
            Line::from(Span::styled(
                format!(" {title}"),
                Style::new().fg(theme::muted()).bold(),
            ))
        };
        let item = |spans: Vec<Span<'static>>, selected: bool| {
            let mut all = vec![if selected {
                Span::styled("▌", Style::new().fg(theme::accent()))
            } else {
                Span::raw(" ")
            }];
            all.extend(spans);
            let used: usize = all.iter().map(|s| s.width()).sum();
            all.push(Span::raw(" ".repeat(width.saturating_sub(used))));
            let line = Line::from(all);
            if selected {
                line.style(Style::new().bg(theme::selection()).bold())
            } else {
                line
            }
        };

        let mut lines = vec![Line::default(), section("VIEW")];
        for (i, view) in VIEWS.iter().enumerate() {
            let selected = i == self.view;
            let (key, name) = view.title().split_at(1);
            lines.push(item(
                vec![
                    Span::styled(format!(" {key} "), Style::new().fg(theme::muted())),
                    Span::styled(
                        name.trim().to_string(),
                        Style::new().fg(if selected {
                            theme::accent()
                        } else {
                            theme::text()
                        }),
                    ),
                ],
                selected,
            ));
        }

        // The upstream list is only a selector, so it only shows where selection means something.
        if VIEWS[self.view] == View::Upstream {
            lines.push(Line::default());
            lines.push(section("UPSTREAMS"));
            let selected = self.table.selected();
            for (i, u) in self.upstreams().iter().enumerate() {
                lines.push(item(
                    vec![
                        Span::styled(" ● ", state_style(&u.state)),
                        Span::styled(u.name.clone(), Style::new().fg(self.color(&u.name))),
                    ],
                    selected == Some(i),
                ));
            }
        }

        if VIEWS[self.view] == View::Global {
            lines.push(Line::default());
            lines.push(section("CHARTS"));
            for (i, style) in STYLES.iter().enumerate() {
                let selected = i == self.style;
                lines.push(item(
                    vec![Span::styled(
                        format!(" {}", style.title()),
                        Style::new().fg(if selected {
                            theme::accent()
                        } else {
                            theme::text()
                        }),
                    )],
                    selected,
                ));
            }
        }

        frame.render_widget(
            Paragraph::new(lines).block(
                Block::new()
                    .borders(Borders::RIGHT)
                    .border_style(Style::new().fg(theme::border())),
            ),
            area,
        );
    }

    fn draw_topbar(&self, frame: &mut Frame, area: Rect) {
        let mut left = vec![
            Span::styled(
                " ◆ bilirelay ",
                Style::new().fg(theme::ink()).bg(theme::accent()).bold(),
            ),
            Span::styled(
                format!(" {} ", self.url),
                Style::new().fg(theme::text()).bg(theme::raised()),
            ),
        ];
        if self.paused {
            left.push(Span::raw(" "));
            left.push(Span::styled(
                " ⏸ PAUSED ",
                Style::new().fg(theme::ink()).bg(theme::yellow()).bold(),
            ));
        }
        let (dot, text, color) = match (&self.error, self.updated) {
            (Some(error), _) => ("✖", error.clone(), theme::red()),
            (None, Some(at)) => (
                "●",
                format!(
                    "live · {:.1}s ago · rtt {}ms",
                    at.elapsed().as_secs_f64(),
                    self.fetch_latency.as_millis()
                ),
                theme::green(),
            ),
            (None, None) => ("◌", "connecting…".to_string(), theme::yellow()),
        };
        let right = Line::from(vec![
            Span::styled(format!("{dot} "), Style::new().fg(color)),
            Span::styled(format!("{text} "), Style::new().fg(theme::muted())),
        ]);
        let left = Line::from(left);
        let [l, r] =
            Layout::horizontal([Constraint::Length(left.width() as u16), Constraint::Fill(1)])
                .spacing(1)
                .areas(area);
        frame.render_widget(left, l);
        frame.render_widget(right.right_aligned(), r);
    }

    /// The KPI strip: one card per headline number, each with a short trend line.
    fn draw_cards(&self, frame: &mut Frame, area: Rect) {
        let ups = self.upstreams();
        let relay = self.snapshot.as_ref().and_then(|s| s.relay.as_ref());
        let since = self.since();
        let upstream_sum = |m: Metric| -> f64 {
            ups.iter()
                .filter_map(|u| self.series.get(&u.name))
                .map(|s| s.recent(m))
                .filter(|v| v.is_finite())
                .sum()
        };
        let requests: u64 = ups.iter().map(|u| u.requests).sum();
        let success: u64 = ups.iter().map(|u| u.success).sum();
        let limited: u64 = ups.iter().map(|u| u.rate_limited).sum();
        let errors: u64 = ups.iter().map(|u| u.errors).sum();
        let tokens: u64 = ups.iter().map(|u| u.tokens).sum();
        let inflight: usize = ups.iter().map(|u| u.inflight).sum();
        let capacity: usize = ups.iter().map(|u| u.max_concurrency).sum();
        let healthy = ups.iter().filter(|u| u.state == "healthy").count();
        let trend = relay.map(|_| &self.relay);

        let (rps, total, ok_pct) = match relay {
            Some(r) => (
                self.relay.recent(Metric::Rps),
                r.requests,
                ratio_pct(r.success, r.requests),
            ),
            None => (
                upstream_sum(Metric::Rps),
                requests,
                ratio_pct(success, requests),
            ),
        };
        let latency = match relay {
            Some(r) => r.latency_ewma_ms as f64,
            None => {
                ups.iter().map(|u| u.latency_ewma_ms as f64).sum::<f64>() / ups.len().max(1) as f64
            }
        };
        let slots = inflight as f64 / capacity.max(1) as f64;
        let peak = |m: Metric| trend.map_or(f64::NAN, |s| s.peak(m, since));

        let cards = [
            Card {
                title: "Requests",
                value: Metric::Rps.format(rps),
                color: theme::cyan(),
                trend: trend.map(|s| (s, Metric::Rps)),
                footer: vec![muted(format!(
                    "peak {} · {} total",
                    Metric::Rps.format(peak(Metric::Rps)),
                    compact(total as i64)
                ))],
            },
            Card {
                title: "Success",
                value: Metric::SuccessRate.format(ok_pct),
                color: success_color(ok_pct),
                trend: trend.map(|s| (s, Metric::SuccessRate)),
                footer: vec![
                    Span::styled("429 ", Style::new().fg(theme::yellow())),
                    muted(compact(limited as i64)),
                    Span::styled("  err ", Style::new().fg(theme::red())),
                    muted(compact(errors as i64)),
                ],
            },
            Card {
                title: if relay.is_some() {
                    "Latency · e2e"
                } else {
                    "Latency · avg"
                },
                value: Metric::Latency.format(latency),
                color: theme::magenta(),
                trend: trend.map(|s| (s, Metric::Latency)),
                footer: vec![muted(format!(
                    "peak {}",
                    Metric::Latency.format(peak(Metric::Latency))
                ))],
            },
            Card {
                title: "Slots",
                value: format!("{inflight}/{capacity}"),
                color: heat(slots),
                trend: trend.map(|s| (s, Metric::Inflight)),
                footer: {
                    let mut bar = meter(slots, 10, heat(slots));
                    bar.push(muted(format!(" {:.0}%", slots * 100.0)));
                    bar
                },
            },
            Card {
                title: "Tokens",
                value: Metric::Tokens.format(match relay {
                    Some(_) => self.relay.recent(Metric::Tokens),
                    None => upstream_sum(Metric::Tokens),
                }),
                color: theme::orange(),
                trend: trend.map(|s| (s, Metric::Tokens)),
                footer: vec![muted(format!("{} total", compact(tokens as i64)))],
            },
            Card {
                title: "Healthy",
                value: format!("{healthy}/{}", ups.len()),
                color: if healthy == ups.len() {
                    theme::green()
                } else if healthy == 0 {
                    theme::red()
                } else {
                    theme::yellow()
                },
                trend: None,
                footer: match relay {
                    Some(r) => vec![muted(format!("up {}", uptime(r.uptime_secs)))],
                    None => vec![],
                },
            },
        ];
        let count = (area.width / 18).clamp(1, cards.len() as u16) as usize;
        let slots = Layout::horizontal(vec![Constraint::Fill(1); count]).split(area);
        for (card, slot) in cards.iter().zip(slots.iter()) {
            self.draw_card(frame, *slot, card, ups);
        }
    }

    fn draw_card(&self, frame: &mut Frame, area: Rect, card: &Card, ups: &[UpstreamView]) {
        let block = panel(card.title);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let [value, spark, footer] = Layout::vertical([Constraint::Length(1); 3]).areas(inner);
        frame.render_widget(
            Line::from(Span::styled(
                card.value.clone(),
                Style::new().fg(card.color).bold(),
            )),
            value,
        );
        match card.trend {
            Some((series, metric)) => {
                let since = self.since();
                let top = metric.ceiling(series.peak(metric, since));
                let data: Vec<Option<u64>> = series
                    .resample(metric, since, self.now_t, spark.width as usize)
                    .into_iter()
                    .map(|v| v.map(|v| scale(v, top)))
                    .collect();
                frame.render_widget(
                    Sparkline::default()
                        .data(&data)
                        .max(scale(top, top))
                        .absent_value_symbol("·")
                        .absent_value_style(Style::new().fg(theme::border()))
                        .style(Style::new().fg(card.color)),
                    spark,
                );
            }
            // No trend to plot: show one dot per upstream instead.
            None => frame.render_widget(
                Line::from(
                    ups.iter()
                        .map(|u| Span::styled("● ", state_style(&u.state)))
                        .collect::<Vec<_>>(),
                ),
                spark,
            ),
        }
        frame.render_widget(Line::from(card.footer.clone()), footer);
    }

    fn draw_groups(&self, frame: &mut Frame, area: Rect) {
        let ups = self.upstreams();
        let mut spans = vec![Span::styled(
            " groups ",
            Style::new().fg(theme::muted()).bold(),
        )];
        for group in self.snapshot.iter().flat_map(|s| &s.groups) {
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                format!(" {} ", group.name),
                Style::new().fg(theme::text()).bg(theme::raised()).bold(),
            ));
            spans.push(Span::raw(" "));
            for (i, name) in group.upstreams.iter().enumerate() {
                if i > 0 {
                    spans.push(Span::styled(" → ", Style::new().fg(theme::border())));
                }
                let state = ups
                    .iter()
                    .find(|u| &u.name == name)
                    .map_or("", |u| u.state.as_str());
                spans.push(Span::styled("●", state_style(state)));
                spans.push(Span::styled(
                    format!(" {name}"),
                    Style::new().fg(self.color(name)),
                ));
            }
            spans.push(Span::raw("  "));
        }
        frame.render_widget(Line::from(spans), area);
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let key = |k: &str| {
            Span::styled(
                format!(" {k} "),
                Style::new().fg(theme::accent()).bg(theme::raised()).bold(),
            )
        };
        let val = |v: &str| Span::styled(format!(" {v}  "), Style::new().fg(theme::muted()));
        let mut spans = vec![key("?"), val("help")];
        if VIEWS[self.view] == View::Upstream {
            spans.extend([key("←→"), val("upstream")]);
        }
        if VIEWS[self.view] == View::Global {
            spans.extend([key("m"), val(self.focus().label())]);
        }
        spans.extend([
            key("w"),
            val(&format!("{:.0}s", self.window())),
            key("+-"),
            val(&format!("{}s", self.interval.borrow().as_secs_f64())),
        ]);
        if VIEWS[self.view] == View::Global {
            spans.extend([key("v"), val(STYLES[self.style].title())]);
            if STYLES[self.style] == ChartStyle::Wave {
                spans.extend([key("c"), val(&format!("{}/row", COLUMNS[self.columns]))]);
            }
        }
        if self.has_lines() {
            spans.extend([
                key("g"),
                val(GRAPHS[self.graph].1),
                key("b"),
                val(MARKERS[self.marker].1),
            ]);
        }
        frame.render_widget(
            Paragraph::new(Line::from(spans)).style(Style::new().bg(theme::surface())),
            area,
        );
    }

    fn draw_global(&self, frame: &mut Frame, area: Rect) {
        let rows = self.upstreams().len().max(1) as u16;
        let [table, charts] =
            Layout::vertical([Constraint::Length(rows + 3), Constraint::Min(6)]).areas(area);
        self.draw_table(frame, table);

        let style = STYLES[self.style];
        match self.focus() {
            Focus::Outcomes => {
                let outcomes = outcome_traces(Some(&self.totals), false);
                match style {
                    ChartStyle::Wave => self.draw_waveform(frame, charts, "Outcomes/s", outcomes),
                    ChartStyle::Bars => self.draw_bar_grid(frame, charts, Metric::Rps),
                }
            }
            Focus::Metric(metric) => match style {
                ChartStyle::Wave => {
                    self.draw_waveform(frame, charts, metric.label(), self.traces(metric));
                }
                ChartStyle::Bars => self.draw_bar_grid(frame, charts, metric),
            },
        }
    }

    fn draw_table(&self, frame: &mut Frame, area: Rect) {
        let name_width = self
            .upstreams()
            .iter()
            .map(|u| u.name.chars().count())
            .max()
            .unwrap_or(8)
            .clamp(8, 24) as u16;
        // (header, width, priority): higher priorities are dropped first on narrow terminals.
        let columns: [(&str, u16, u8); 12] = [
            ("Upstream", name_width, 0),
            ("State", 11, 0),
            ("Slots", 10, 1),
            ("Req/s", 6, 0),
            ("Total", 7, 0),
            ("Share", 6, 0),
            ("OK%", 6, 1),
            ("429", 5, 2),
            ("Err", 5, 2),
            ("TPM", 11, 3),
            ("RPM", 11, 4),
            ("Note", 13, 2),
        ];
        let keep = fit_columns(&columns, area.width.saturating_sub(4));
        let pick = |cells: Vec<Cell<'static>>| -> Vec<Cell<'static>> {
            cells
                .into_iter()
                .zip(keep)
                .filter_map(|(c, k)| k.then_some(c))
                .collect()
        };
        let header = Row::new(pick(
            columns
                .iter()
                .map(|c| Cell::from(right(c.0, NUMERIC.contains(&c.0))))
                .collect(),
        ))
        .style(Style::new().fg(theme::muted()).bold())
        .bottom_margin(0);
        let since = self.since();
        let window_total = |name: &str| {
            self.series
                .get(name)
                .map_or(0.0, |s| s.total(Metric::Rps, since))
        };
        let sum: f64 = self.upstreams().iter().map(|u| window_total(&u.name)).sum();
        let rows: Vec<Row> = self
            .upstreams()
            .iter()
            .map(|u| {
                let total = window_total(&u.name);
                let share = if sum >= 0.5 {
                    format!("{:.1}%", total / sum * 100.0)
                } else {
                    "-".to_string()
                };
                let rps = self
                    .series
                    .get(&u.name)
                    .map_or(f64::NAN, |s| s.recent(Metric::Rps));
                let load = u.inflight as f64 / u.max_concurrency.max(1) as f64;
                let mut slots = meter(load, 4, heat(load));
                slots.push(Span::raw(format!(" {}/{}", u.inflight, u.max_concurrency)));
                let ok_color = success_color(ratio_pct(u.success, u.requests));
                let count = |n: u64, color: Color| {
                    Cell::from(right(n.to_string(), true)).style(Style::new().fg(if n == 0 {
                        theme::border()
                    } else {
                        color
                    }))
                };
                Row::new(pick(vec![
                    Cell::from(u.name.clone()).style(Style::new().fg(self.color(&u.name)).bold()),
                    Cell::from(format!("● {}", u.state)).style(state_style(&u.state)),
                    Cell::from(Line::from(slots)),
                    Cell::from(right(
                        Metric::Rps.format(rps).trim_end_matches("/s").to_string(),
                        true,
                    )),
                    Cell::from(right(total.round().to_string(), true)).bold(),
                    Cell::from(right(share, true))
                        .style(Style::new().fg(self.color(&u.name)).bold()),
                    Cell::from(right(percent(u.success, u.requests), true))
                        .style(Style::new().fg(ok_color)),
                    count(u.rate_limited, theme::yellow()),
                    count(u.errors, theme::red()),
                    Cell::from(right(budget(u.tpm_available, u.tpm_limit), true)),
                    Cell::from(right(budget(u.rpm_available, u.rpm_limit), true)),
                    Cell::from(note(u)).style(Style::new().fg(theme::yellow())),
                ]))
                .style(Style::new().fg(theme::text()))
            })
            .collect();
        let widths: Vec<Constraint> = columns
            .iter()
            .zip(keep)
            .filter(|(_, k)| *k)
            .map(|(c, _)| Constraint::Length(c.1))
            .collect();
        let table = Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .block(
                panel("Upstreams").title_top(
                    hint(&format!(
                        "total & share · last {:.0}s · {} requests",
                        self.window(),
                        sum.round()
                    ))
                    .right_aligned(),
                ),
            );
        frame.render_widget(table, area);
    }

    fn draw_upstream(&self, frame: &mut Frame, area: Rect) {
        let Some(u) = self.selected() else {
            frame.render_widget(
                Paragraph::new("No upstreams reported yet.").block(Block::bordered()),
                area,
            );
            return;
        };
        let series = self.series.get(&u.name);
        let color = self.color(&u.name);
        let index = self.table.selected().unwrap_or(0);
        let wide = area.width >= 100;
        let [info, charts] =
            Layout::vertical([Constraint::Length(6), Constraint::Min(6)]).areas(area);

        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(color))
            .padding(Padding::horizontal(1))
            .title(Line::from(vec![
                Span::styled(" ◀ ", Style::new().fg(theme::muted())),
                Span::styled(
                    format!(" {} ", u.name),
                    Style::new().fg(theme::ink()).bg(color).bold(),
                ),
                Span::styled(format!(" {} ", u.model), Style::new().fg(theme::text())),
                Span::styled(" ▶ ", Style::new().fg(theme::muted())),
            ]))
            .title_top(hint(&format!("{}/{}", index + 1, self.upstreams().len())).right_aligned());
        let inner = block.inner(info);
        frame.render_widget(block, info);
        let [text, gauges] = Layout::horizontal([
            Constraint::Fill(1),
            if wide {
                Constraint::Fill(1)
            } else {
                Constraint::Length(32)
            },
        ])
        .spacing(1)
        .areas(inner);
        let since = self.since();
        let metric_now = |m: Metric| series.map_or(f64::NAN, |s| s.recent(m));
        let metric_peak = |m: Metric| series.map_or(f64::NAN, |s| s.peak(m, since));
        let kv = |k: &str, v: Vec<Span<'static>>| {
            let mut spans = vec![Span::styled(
                format!("{k:<9}"),
                Style::new().fg(theme::muted()),
            )];
            spans.extend(v);
            Line::from(spans)
        };
        let note = note(u);
        let state = if note.starts_with(&u.state) {
            vec![Span::styled(note, state_style(&u.state))]
        } else if note.is_empty() {
            vec![Span::styled(u.state.clone(), state_style(&u.state))]
        } else {
            vec![
                Span::styled(u.state.clone(), state_style(&u.state)),
                Span::styled(format!(" · {note}"), Style::new().fg(theme::yellow())),
            ]
        };
        frame.render_widget(
            Paragraph::new(vec![
                kv("health", state),
                kv(
                    "traffic",
                    vec![Span::raw(format!(
                        "{} · ok {} of {} · 429 {} · err {}",
                        Metric::Rps.format(metric_now(Metric::Rps)),
                        percent(u.success, u.requests),
                        u.requests,
                        u.rate_limited,
                        u.errors
                    ))],
                ),
                kv(
                    "latency",
                    vec![Span::raw(format!(
                        "{}ms now · peak {}",
                        u.latency_ewma_ms,
                        Metric::Latency.format(metric_peak(Metric::Latency))
                    ))],
                ),
                kv(
                    "tokens",
                    vec![Span::raw(format!(
                        "{} total · {}",
                        compact(u.tokens as i64),
                        Metric::Tokens.format(metric_now(Metric::Tokens))
                    ))],
                ),
            ]),
            text,
        );
        self.draw_gauges(frame, gauges, u);

        let trace = |metric| Trace {
            name: u.name.clone(),
            color,
            series,
            metric,
        };
        let [top, bottom] = Layout::vertical([Constraint::Fill(1); 2]).areas(charts);
        self.draw_waveform(frame, top, "Requests/s", vec![trace(Metric::Rps)]);
        self.draw_lines(
            frame,
            bottom,
            "Outcomes/s",
            outcome_traces(series, false),
            Metric::Rps,
        );
    }

    fn draw_gauges(&self, frame: &mut Frame, area: Rect, u: &UpstreamView) {
        let rows = Layout::vertical([Constraint::Length(1); 4]).split(area);
        let ratio = |used: f64, limit: f64| (used / limit.max(1.0)).clamp(0.0, 1.0);
        let gauge = |label: String, ratio: f64, color: Color| {
            LineGauge::default()
                .ratio(ratio)
                .label(label)
                .filled_symbol(line::THICK_HORIZONTAL)
                .unfilled_symbol(line::THICK_HORIZONTAL)
                .filled_style(Style::new().fg(color))
                .unfilled_style(Style::new().fg(theme::border()))
                .style(Style::new().fg(theme::text()))
        };
        let slots = ratio(u.inflight as f64, u.max_concurrency as f64);
        frame.render_widget(
            gauge(
                format!(
                    "{:<6}{:>12}",
                    "slots",
                    format!("{}/{}", u.inflight, u.max_concurrency)
                ),
                slots,
                heat(slots),
            ),
            rows[0],
        );
        for (row, name, available, limit) in [
            (rows[1], "tpm", u.tpm_available, u.tpm_limit),
            (rows[2], "rpm", u.rpm_available, u.rpm_limit),
        ] {
            let widget = match (available, limit) {
                (Some(available), Some(limit)) => {
                    let left = ratio(available.max(0) as f64, limit as f64);
                    gauge(
                        format!("{name:<6}{:>12}", budget(Some(available), Some(limit))),
                        left,
                        heat(1.0 - left),
                    )
                }
                _ => gauge(format!("{name:<6}{:>12}", "unlimited"), 0.0, theme::green()),
            };
            frame.render_widget(widget, row);
        }
        let total: u64 = self.upstreams().iter().map(|x| x.requests).sum();
        let share = ratio(u.requests as f64, total as f64);
        frame.render_widget(
            gauge(
                format!("{:<6}{:>12}", "share", format!("{:.0}%", share * 100.0)),
                share,
                self.color(&u.name),
            ),
            rows[3],
        );
    }

    /// Stacked filled waveforms, one strip per trace, sharing a scale and scrolling right to left.
    fn draw_waveform(&self, frame: &mut Frame, area: Rect, title: &str, traces: Vec<Trace>) {
        let block =
            panel(title).title_top(hint(&format!("last {:.0}s", self.window())).right_aligned());
        let inner = block.inner(area);
        frame.render_widget(block, area);
        if traces.is_empty() || inner.width == 0 {
            return;
        }
        let since = self.since();
        let peak = traces
            .iter()
            .filter_map(|t| t.series.map(|s| s.peak(t.metric, since)))
            .fold(0.0, f64::max);
        let top = traces[0].metric.ceiling(peak);
        let (marker, _) = MARKERS[self.marker];
        let (graph, _) = GRAPHS[self.graph];
        // Strips per row as chosen, fewer when they would get too narrow for their label. A short
        // last row keeps the column width so all time axes stay aligned.
        let mut columns = COLUMNS[self.columns].min(traces.len());
        while columns > 1 && (inner.width as usize) < columns * 40 {
            columns -= 1;
        }
        let rows = traces.len().div_ceil(columns);
        let strips: Vec<Rect> = Layout::vertical(vec![Constraint::Fill(1); rows])
            .split(inner)
            .iter()
            .flat_map(|row| {
                Layout::horizontal(vec![Constraint::Fill(1); columns])
                    .spacing(2)
                    .split(*row)
                    .to_vec()
            })
            .collect();
        for (trace, strip) in traces.iter().zip(strips.iter()) {
            let metric = trace.metric;
            let [label, wave] = if strip.height >= 2 {
                Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(*strip)
            } else {
                [Rect::default(), *strip]
            };
            let (now, peak, mean, total) =
                trace.series.map_or((f64::NAN, 0.0, f64::NAN, 0.0), |s| {
                    (
                        s.recent(metric),
                        s.peak(metric, since),
                        s.mean(metric, since),
                        s.total(metric, since),
                    )
                });
            // Counter-derived metrics also get the window total, so no separate cumulative chart is needed.
            let total = if (metric as usize) < RATE_COUNT {
                vec![
                    muted("  total "),
                    Span::styled(
                        total.round().to_string(),
                        Style::new().fg(theme::text()).bold(),
                    ),
                ]
            } else {
                vec![]
            };
            frame.render_widget(
                Paragraph::new(Line::from(
                    vec![
                        Span::styled(trace.name.clone(), Style::new().fg(trace.color).bold()),
                        Span::styled(
                            format!("  {}", metric.format(now)),
                            Style::new().fg(theme::text()).bold(),
                        ),
                        muted(format!(
                            "  avg {}  peak {}",
                            metric.format(mean),
                            metric.format(peak),
                        )),
                    ]
                    .into_iter()
                    .chain(total)
                    .collect::<Vec<_>>(),
                )),
                label,
            );
            // Same y range in every strip so heights compare across upstreams; `g` / `b` pick
            // the graph type and marker as in the other charts.
            let points: Vec<(f64, f64)> = trace
                .series
                .map_or_else(Vec::new, |s| s.values(metric, since).collect());
            frame.render_widget(
                Chart::new(vec![
                    Dataset::default()
                        .marker(marker)
                        .graph_type(graph)
                        .style(Style::new().fg(trace.color))
                        .data(&points),
                ])
                .x_axis(Axis::default().bounds([since, self.now_t]))
                .y_axis(Axis::default().bounds([0.0, top])),
                wave,
            );
        }
    }

    fn draw_lines(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        mut traces: Vec<Trace>,
        axis: Metric,
    ) {
        let since = self.since();
        let data: Vec<Vec<(f64, f64)>> = traces
            .iter()
            .map(|t| {
                t.series
                    .map_or_else(Vec::new, |s| s.values(t.metric, since).collect())
            })
            .collect();
        // Counter-derived lines carry their window total in the legend instead of a separate chart.
        for trace in &mut traces {
            if (trace.metric as usize) < RATE_COUNT {
                let total = trace.series.map_or(0.0, |s| s.total(trace.metric, since));
                trace.name = format!("{} {}", trace.name, total.round());
            }
        }
        self.plot(frame, area, title, &traces, &data, axis);
    }

    fn plot(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        traces: &[Trace],
        data: &[Vec<(f64, f64)>],
        axis: Metric,
    ) {
        let since = self.since();
        let max = data.iter().flatten().map(|(_, y)| *y).fold(0.0, f64::max);
        let top = axis.ceiling(max);
        let (marker, _) = MARKERS[self.marker];
        let (graph, _) = GRAPHS[self.graph];
        let datasets: Vec<Dataset> = traces
            .iter()
            .zip(data)
            .map(|(trace, points)| {
                Dataset::default()
                    .name(trace.name.clone())
                    .marker(marker)
                    .graph_type(graph)
                    .style(Style::new().fg(trace.color))
                    .data(points)
            })
            .collect();
        let window = self.window();
        let chart = Chart::new(datasets)
            .block(panel(title).title_top(hint(&format!("last {window:.0}s")).right_aligned()))
            .legend_position(Some(LegendPosition::TopLeft))
            .hidden_legend_constraints((Constraint::Percentage(50), Constraint::Percentage(90)))
            .x_axis(
                Axis::default()
                    .style(Style::new().fg(theme::muted()))
                    .bounds([since, self.now_t])
                    .labels([
                        format!("-{window:.0}s"),
                        format!("-{:.0}s", window / 2.0),
                        "now".to_string(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::new().fg(theme::muted()))
                    .bounds([0.0, top])
                    .labels([
                        axis.axis_label(0.0, top),
                        axis.axis_label(top / 2.0, top),
                        axis.axis_label(top, top),
                    ]),
            );
        frame.render_widget(chart, area);
    }

    /// `metric` now and requests all time on top, outcomes now and all time below.
    fn draw_bar_grid(&self, frame: &mut Frame, area: Rect, metric: Metric) {
        let [top, bottom] = Layout::vertical([Constraint::Fill(1); 2]).areas(area);
        let [now, total] = Layout::horizontal([Constraint::Fill(1); 2]).areas(top);
        self.draw_current_bars(frame, now, metric);
        self.draw_request_totals(frame, total);
        let [now, total] = Layout::horizontal([Constraint::Fill(1); 2]).areas(bottom);
        self.draw_outcome_rates(frame, now);
        self.draw_outcome_totals(frame, total);
    }

    fn draw_current_bars(&self, frame: &mut Frame, area: Rect, metric: Metric) {
        let upstreams = self.upstreams();
        let values: Vec<f64> = upstreams
            .iter()
            .map(|u| {
                self.series
                    .get(&u.name)
                    .map_or(f64::NAN, |s| s.recent(metric))
            })
            .collect();
        let top = metric.ceiling(
            values
                .iter()
                .copied()
                .filter(|v| v.is_finite())
                .fold(0.0, f64::max),
        );
        let bars: Vec<Bar> = upstreams
            .iter()
            .zip(&values)
            .map(|(u, v)| {
                let color = self.color(&u.name);
                Bar::default()
                    .label(Line::from(u.name.clone()))
                    .value(if v.is_finite() { scale(*v, top) } else { 0 })
                    .text_value(metric.format(*v))
                    .style(Style::new().fg(color))
                    .value_style(Style::new().fg(theme::ink()).bg(color).bold())
            })
            .collect();
        frame.render_widget(
            BarChart::default()
                .block(
                    panel(metric.label())
                        .title_top(hint(&format!("last {SMOOTH_SECS:.0}s")).right_aligned()),
                )
                .direction(Direction::Horizontal)
                .bar_width(1)
                .bar_gap(1)
                .max(scale(top, top))
                .data(BarGroup::default().bars(&bars)),
            area,
        );
    }

    /// ok / 429 / errors per upstream over the last few seconds.
    fn draw_outcome_rates(&self, frame: &mut Frame, area: Rect) {
        let rows = self
            .upstreams()
            .iter()
            .map(|u| {
                let series = self.series.get(&u.name);
                let rate = |m: Metric| series.map_or(0.0, |s| s.recent(m)).max(0.0);
                let values = [
                    rate(Metric::Ok),
                    rate(Metric::Limited),
                    rate(Metric::Errors),
                ];
                StackRow {
                    name: u.name.clone(),
                    color: self.color(&u.name),
                    segments: outcome_segments(values, |v| format!("{v:.2}")),
                    total: Metric::Ok.format(values.iter().sum()),
                }
            })
            .collect();
        let block = panel("Outcomes/s")
            .title_top(hint(&format!("last {SMOOTH_SECS:.0}s")).right_aligned())
            .title_bottom(legend().right_aligned());
        draw_stacked(frame, area, block, rows, false);
    }

    /// Requests per upstream since the relay started, with each one's share of the total.
    fn draw_request_totals(&self, frame: &mut Frame, area: Rect) {
        let upstreams = self.upstreams();
        let sum: u64 = upstreams.iter().map(|u| u.requests).sum();
        let rows = upstreams
            .iter()
            .map(|u| {
                let color = self.color(&u.name);
                StackRow {
                    name: u.name.clone(),
                    color,
                    segments: vec![(u.requests as f64, compact(u.requests as i64), color)],
                    total: percent(u.requests, sum),
                }
            })
            .collect();
        let block = panel("Cumulative requests")
            .title_top(hint("all time · share").right_aligned())
            .title_bottom(hint(&format!("{} total", compact(sum as i64))).right_aligned());
        draw_stacked(frame, area, block, rows, false);
    }

    /// ok / 429 / errors per upstream since the relay started, as a share of its requests.
    fn draw_outcome_totals(&self, frame: &mut Frame, area: Rect) {
        let rows = self
            .upstreams()
            .iter()
            .map(|u| {
                let values = [u.success, u.rate_limited, u.errors].map(|n| n as f64);
                StackRow {
                    name: u.name.clone(),
                    color: self.color(&u.name),
                    segments: outcome_segments(values, |v| compact(v as i64)),
                    total: format!(
                        "{} ok",
                        percent(u.success, u.success + u.rate_limited + u.errors)
                    ),
                }
            })
            .collect();
        let block = panel("Cumulative outcomes")
            .title_top(hint("all time · 100%").right_aligned())
            .title_bottom(legend().right_aligned());
        draw_stacked(frame, area, block, rows, true);
    }

    fn draw_relay(&self, frame: &mut Frame, area: Rect) {
        let Some(relay) = self.snapshot.as_ref().and_then(|s| s.relay.as_ref()) else {
            frame.render_widget(
                Paragraph::new(
                    "This relay does not report client-side stats; upgrade it to see this view.",
                )
                .style(Style::new().fg(theme::muted()))
                .block(panel("Relay")),
                area,
            );
            return;
        };
        let [top, bottom] = Layout::vertical([Constraint::Fill(1); 2]).areas(area);
        let [outcomes, totals] =
            Layout::horizontal([Constraint::Percentage(70), Constraint::Percentage(30)])
                .areas(bottom);
        let trace = |name: &str, color, metric| Trace {
            name: name.to_string(),
            color,
            series: Some(&self.relay),
            metric,
        };
        self.draw_waveform(
            frame,
            top,
            "Client requests/s",
            vec![trace("requests", theme::cyan(), Metric::Rps)],
        );
        self.draw_lines(
            frame,
            outcomes,
            "Client outcomes/s",
            outcome_traces(Some(&self.relay), true),
            Metric::Rps,
        );

        let bar = |label: &str, value: u64, color: Color| {
            Bar::default()
                .label(Line::from(label.to_string()))
                .value(value)
                .text_value(compact(value as i64))
                .style(Style::new().fg(color))
                .value_style(Style::new().fg(theme::ink()).bg(color))
        };
        frame.render_widget(
            BarChart::default()
                .block(
                    panel("Responses").title_top(
                        hint(&format!(
                            "{} total · {} inflight",
                            relay.requests, relay.inflight
                        ))
                        .right_aligned(),
                    ),
                )
                .bar_width(6)
                .bar_gap(2)
                .data(BarGroup::default().bars(&[
                    bar("2xx", relay.success, theme::green()),
                    bar("4xx", relay.client_errors, theme::blue()),
                    bar("429", relay.rate_limited, theme::yellow()),
                    bar("5xx", relay.server_errors, theme::red()),
                    bar("satur.", relay.saturated, theme::magenta()),
                ])),
            totals,
        );
    }
}

fn outcome_traces(series: Option<&Series>, with_saturated: bool) -> Vec<Trace<'_>> {
    let mut fields = vec![
        ("ok", Metric::Ok, theme::green()),
        ("429", Metric::Limited, theme::yellow()),
        ("errors", Metric::Errors, theme::red()),
    ];
    if with_saturated {
        fields.push(("saturated", Metric::Saturated, theme::magenta()));
    }
    fields
        .into_iter()
        .map(|(name, metric, color)| Trace {
            name: name.to_string(),
            color,
            series,
            metric,
        })
        .collect()
}

fn draw_help(frame: &mut Frame) {
    let keys = [
        ("Tab / Shift-Tab, 1-3", "switch view"),
        ("← → / ↑ ↓ / h l j k", "select upstream"),
        ("m / M", "next / previous metric"),
        ("w / W", "next / previous time window"),
        ("+ / -", "slower / faster sampling"),
        ("g / G", "graph type: line, scatter, bar (charts and waves)"),
        ("b / B", "plot marker: braille, half-block, …"),
        ("v / V", "global charts: bars, wave"),
        ("c / C", "global waves per row: 1, 2, 3"),
        ("space / p", "pause updates"),
        ("r", "clear history"),
        ("?", "toggle this help"),
        ("q / Esc", "quit"),
    ];
    let mut lines: Vec<Line> = keys
        .iter()
        .map(|(k, v)| {
            Line::from(vec![
                Span::styled(
                    format!("{k:>22}  "),
                    Style::new().fg(theme::accent()).bold(),
                ),
                Span::styled(*v, Style::new().fg(theme::text())),
            ])
        })
        .collect();
    lines.push(Line::default());
    lines.push(Line::from(muted(
        "  Views: Global (all upstreams) · Upstream (one) · Relay (clients)",
    )));
    let height = lines.len() as u16 + 4;
    let [area] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(frame.area());
    let [area] = Layout::horizontal([Constraint::Length(74)])
        .flex(Flex::Center)
        .areas(area);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(Style::new().fg(theme::accent()))
                .padding(Padding::uniform(1))
                .title(Span::styled(
                    " ⌨  Key bindings ",
                    Style::new().fg(theme::ink()).bg(theme::accent()).bold(),
                ))
                .title_bottom(hint("Esc to close").right_aligned()),
        ),
        area,
    );
}

/// Chooses which columns fit in `width`, dropping the highest-priority numbers first.
fn fit_columns<const N: usize>(columns: &[(&str, u16, u8); N], width: u16) -> [bool; N] {
    let mut keep = [true; N];
    let used = |keep: &[bool; N]| -> u16 {
        columns
            .iter()
            .zip(keep)
            .filter(|(_, k)| **k)
            .map(|(c, _)| c.1 + 1)
            .sum()
    };
    let max_priority = columns.iter().map(|c| c.2).max().unwrap_or(0);
    for priority in (1..=max_priority).rev() {
        for i in (0..N).rev() {
            if used(&keep) <= width {
                return keep;
            }
            if columns[i].2 == priority {
                keep[i] = false;
            }
        }
    }
    keep
}

fn note(u: &UpstreamView) -> String {
    if u.disabled_remaining_ms > 0 {
        format!("disabled {}s", u.disabled_remaining_ms.div_ceil(1000))
    } else if u.cooldown_remaining_ms > 0 {
        format!("cooldown {}s", u.cooldown_remaining_ms.div_ceil(1000))
    } else if u.consecutive_failures > 0 {
        format!("{} fails", u.consecutive_failures)
    } else {
        String::new()
    }
}

fn state_style(state: &str) -> Style {
    let color = match state {
        "healthy" => theme::green(),
        "half_open" => theme::cyan(),
        "cooldown" => theme::yellow(),
        "disabled" => theme::red(),
        _ => theme::muted(),
    };
    Style::new().fg(color).bold()
}

fn success_color(pct: f64) -> Color {
    match pct {
        v if !v.is_finite() => theme::muted(),
        v if v >= 99.0 => theme::green(),
        v if v >= 95.0 => theme::yellow(),
        _ => theme::red(),
    }
}

/// Green when idle, yellow when busy, red when nearly exhausted.
fn heat(load: f64) -> Color {
    if load >= 0.9 {
        theme::red()
    } else if load >= 0.6 {
        theme::yellow()
    } else {
        theme::green()
    }
}

/// Columns right-aligned in the upstream table.
const NUMERIC: [&str; 8] = ["Req/s", "Total", "Share", "OK%", "429", "Err", "TPM", "RPM"];

/// A rounded panel with a quiet border and a bold title.
fn panel(title: &str) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(theme::border()))
        .title(Span::styled(
            format!(" {title} "),
            Style::new().fg(theme::text()).bold(),
        ))
}

/// Secondary text tucked into a panel border.
fn hint(text: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {text} "),
        Style::new().fg(theme::muted()),
    ))
}

fn muted(text: impl Into<String>) -> Span<'static> {
    Span::styled(text.into(), Style::new().fg(theme::muted()))
}

/// One row of `draw_stacked`: coloured segments of `(value, text, colour)` and a right-hand total.
struct StackRow {
    name: String,
    color: Color,
    segments: Vec<(f64, String, Color)>,
    total: String,
}

fn outcome_segments(values: [f64; 3], text: impl Fn(f64) -> String) -> Vec<(f64, String, Color)> {
    let colors = [theme::green(), theme::yellow(), theme::red()];
    values
        .into_iter()
        .zip(colors)
        .map(|(v, c)| (v, text(v), c))
        .collect()
}

/// One stacked horizontal bar per upstream, spaced like `draw_current_bars` so panels placed side
/// by side line up row for row. Bars scale to the largest row, or with `fill` each row spans the
/// full width so the segments read as proportions.
fn draw_stacked(
    frame: &mut Frame,
    area: Rect,
    block: Block<'static>,
    rows: Vec<StackRow>,
    fill: bool,
) {
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let sums: Vec<f64> = rows
        .iter()
        .map(|r| r.segments.iter().map(|(v, ..)| v).sum())
        .collect();
    let label_w = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0)
        + 1;
    let total_w = rows.iter().map(|r| r.total.len()).max().unwrap_or(0) + 1;
    let bar_w = (inner.width as usize).saturating_sub(label_w + total_w);
    let peak = sums.iter().copied().fold(0.0, f64::max);

    let mut lines = Vec::new();
    for (row, &sum) in rows.iter().zip(&sums) {
        let scale = if fill { sum } else { peak };
        // Every non-zero segment keeps at least one cell so small counts stay visible.
        let mut widths: Vec<usize> = row
            .segments
            .iter()
            .map(|&(v, ..)| {
                if v > 0.0 && scale > 0.0 {
                    ((v / scale * bar_w as f64).round() as usize).max(1)
                } else {
                    0
                }
            })
            .collect();
        let widest = |w: &[usize]| (0..w.len()).max_by_key(|&i| w[i]).unwrap_or(0);
        while widths.iter().sum::<usize>() > bar_w {
            let i = widest(&widths);
            if widths[i] <= 1 {
                break;
            }
            widths[i] -= 1;
        }
        if fill && sum > 0.0 {
            while widths.iter().sum::<usize>() < bar_w {
                let i = widest(&widths);
                widths[i] += 1;
            }
        }

        let mut spans = vec![Span::styled(
            format!("{:<label_w$}", row.name),
            Style::new().fg(row.color),
        )];
        for ((_, text, color), &w) in row.segments.iter().zip(&widths) {
            if w == 0 {
                continue;
            }
            let text = if text.chars().count() <= w {
                format!("{text:<w$}")
            } else {
                " ".repeat(w)
            };
            spans.push(Span::styled(
                text,
                Style::new().fg(theme::ink()).bg(*color).bold(),
            ));
        }
        let used: usize = widths.iter().sum();
        let track = if sum > 0.0 { " " } else { "·" };
        spans.push(Span::styled(
            track.repeat(bar_w.saturating_sub(used)),
            Style::new().fg(theme::border()),
        ));
        spans.push(Span::styled(
            format!("{:>total_w$}", row.total),
            Style::new().fg(theme::text()),
        ));
        lines.push(Line::from(spans));
        lines.push(Line::default());
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

fn legend() -> Line<'static> {
    let dot = |c: Color| Span::styled("■ ", Style::new().fg(c));
    Line::from(vec![
        Span::raw(" "),
        dot(theme::green()),
        muted("ok  "),
        dot(theme::yellow()),
        muted("429  "),
        dot(theme::red()),
        muted("errors "),
    ])
}

fn right(text: impl Into<String>, numeric: bool) -> Line<'static> {
    let line = Line::from(text.into());
    if numeric { line.right_aligned() } else { line }
}

/// A compact horizontal bar, `width` cells wide.
fn meter(ratio: f64, width: usize, color: Color) -> Vec<Span<'static>> {
    let filled = ((ratio.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    vec![
        Span::styled("━".repeat(filled), Style::new().fg(color)),
        Span::styled("━".repeat(width - filled), Style::new().fg(theme::border())),
    ]
}

/// Maps a value onto integer bar heights, keeping fractional rates visible.
fn scale(v: f64, top: f64) -> u64 {
    (v.max(0.0) / top.max(f64::MIN_POSITIVE) * 10_000.0).round() as u64
}

fn ratio_pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        f64::NAN
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn percent(part: u64, total: u64) -> String {
    Metric::SuccessRate.format(ratio_pct(part, total))
}

fn budget(available: Option<i64>, limit: Option<u64>) -> String {
    match (available, limit) {
        (Some(available), Some(limit)) => {
            format!("{}/{}", compact(available), compact(limit as i64))
        }
        _ => "-".to_string(),
    }
}

fn uptime(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{}s", secs / 60, secs % 60),
        3600..86400 => format!("{}h{}m", secs / 3600, secs % 3600 / 60),
        _ => format!("{}d{}h", secs / 86400, secs % 86400 / 3600),
    }
}

fn compact(n: i64) -> String {
    let abs = n.unsigned_abs() as f64;
    let sign = if n < 0 { "-" } else { "" };
    if abs >= 1e6 {
        format!("{sign}{:.1}M", abs / 1e6)
    } else if abs >= 1e3 {
        format!("{sign}{:.1}k", abs / 1e3)
    } else {
        n.to_string()
    }
}

fn nice_ceiling(max: f64) -> f64 {
    if max <= 0.0 || !max.is_finite() {
        return 1.0;
    }
    let magnitude = 10f64.powf((max * 1.1).log10().floor());
    [1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|step| step * magnitude)
        .find(|v| *v >= max * 1.1)
        .unwrap_or(10.0 * magnitude)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_url_and_token_skip_the_config() {
        let options = parse_args([
            "--url",
            "http://relay:1/",
            "--token=t",
            "--interval",
            "0.5",
            "missing.toml",
        ])
        .unwrap();
        assert_eq!(
            options,
            Options {
                url: "http://relay:1".into(),
                token: Some("t".into()),
                interval: Duration::from_millis(500),
            }
        );
    }

    #[test]
    fn missing_config_without_url_is_an_error() {
        assert!(parse_args(["-c", "definitely-missing.toml"]).is_err());
    }

    #[test]
    fn unspecified_bind_maps_to_loopback() {
        assert_eq!(url_from_bind("0.0.0.0:8080"), "http://127.0.0.1:8080");
        assert_eq!(url_from_bind("[::]:8080"), "http://[::1]:8080");
        assert_eq!(url_from_bind("10.0.0.2:80"), "http://10.0.0.2:80");
    }

    #[test]
    fn chart_ceiling_rounds_up_with_headroom() {
        assert_eq!(nice_ceiling(0.0), 1.0);
        assert_eq!(nice_ceiling(0.9), 1.0);
        assert_eq!(nice_ceiling(3.0), 5.0);
        assert_eq!(nice_ceiling(95.0), 200.0);
    }

    #[test]
    fn compact_formats_budgets() {
        assert_eq!(compact(950), "950");
        assert_eq!(compact(60_000), "60.0k");
        assert_eq!(compact(-1_500), "-1.5k");
        assert_eq!(compact(5_000_000), "5.0M");
    }

    fn sample(requests: u64, latency: f64) -> Sample {
        Sample {
            counters: [Some(requests), None, None, None, None, None],
            gauges: [latency, 0.0, f64::NAN, f64::NAN, f64::NAN],
        }
    }

    #[test]
    fn series_derives_rates_and_survives_counter_resets() {
        let mut series = Series::default();
        let t0 = Instant::now();
        series.push(t0, 0.0, sample(10, 5.0));
        assert!(series.points.is_empty(), "rates need two samples");
        series.push(t0 + Duration::from_secs(2), 2.0, sample(30, 7.0));
        assert_eq!(series.last(Metric::Rps), 10.0);
        assert_eq!(series.last(Metric::Latency), 7.0);
        assert!(series.last(Metric::Ok).is_nan());
        series.push(t0 + Duration::from_secs(3), 3.0, sample(2, 7.0));
        assert_eq!(series.last(Metric::Rps), 0.0);
        assert_eq!(series.recent(Metric::Rps), 5.0);
    }

    #[test]
    fn total_recovers_counts_within_the_window() {
        let mut series = Series::default();
        let t0 = Instant::now();
        for (i, requests) in [0u64, 3, 7, 7, 20].into_iter().enumerate() {
            series.push(
                t0 + Duration::from_secs(2 * i as u64),
                2.0 * i as f64,
                sample(requests, 0.0),
            );
        }
        assert_eq!(series.total(Metric::Rps, 0.0), 20.0);
        // Only the samples at t=6 and t=8 fall inside: 0 + 13 requests.
        assert_eq!(series.total(Metric::Rps, 5.0), 13.0);
    }

    #[test]
    fn resample_takes_bucket_maxima_and_holds_across_gaps() {
        let mut series = Series::default();
        let t0 = Instant::now();
        for (i, requests) in [0u64, 1, 4, 4, 10].into_iter().enumerate() {
            series.push(
                t0 + Duration::from_secs(i as u64),
                i as f64,
                sample(requests, 0.0),
            );
        }
        // Rates at t=1..4 are 1, 3, 0, 6.
        assert_eq!(
            series.resample(Metric::Rps, 0.0, 4.0, 2),
            vec![Some(3.0), Some(6.0)]
        );
        assert_eq!(
            series.resample(Metric::Rps, -4.0, 4.0, 4),
            vec![None, None, Some(3.0), Some(6.0)]
        );
        assert_eq!(
            series.resample(Metric::Rps, 0.0, 4.0, 8),
            vec![
                None,
                Some(1.0),
                Some(1.0),
                Some(3.0),
                Some(3.0),
                Some(0.0),
                Some(0.0),
                Some(6.0)
            ]
        );
    }

    fn snapshot(tick: u64) -> Snapshot {
        let upstream = |name: &str, state: &str, scale: u64| UpstreamView {
            name: name.into(),
            model: "deepseek-v3".into(),
            state: state.into(),
            inflight: (tick % 4) as usize,
            max_concurrency: 4,
            cooldown_remaining_ms: if state == "cooldown" { 12_000 } else { 0 },
            disabled_remaining_ms: 0,
            consecutive_failures: 0,
            requests: tick * scale,
            success: tick * scale - tick / 3,
            rate_limited: tick / 5,
            errors: tick / 7,
            tokens: tick * scale * 900,
            latency_ewma_ms: 400 + tick * 13 % 300,
            tpm_limit: Some(60_000),
            tpm_available: Some(42_000),
            rpm_limit: None,
            rpm_available: None,
        };
        Snapshot {
            relay: Some(RelayView {
                uptime_secs: 3_725 + tick,
                inflight: 3,
                requests: tick * 9,
                success: tick * 9 - tick / 2,
                client_errors: tick / 9,
                rate_limited: tick / 5,
                server_errors: tick / 11,
                saturated: 0,
                latency_ewma_ms: 620,
            }),
            groups: vec![GroupView {
                name: "default".into(),
                upstreams: vec!["volc".into(), "siliconflow".into(), "local".into()],
            }],
            upstreams: vec![
                upstream("volc", "healthy", 5),
                upstream("siliconflow", "cooldown", 3),
                upstream("local", "healthy", 1),
            ],
        }
    }

    /// Renders every view; set MONITOR_PREVIEW=1 with --nocapture to eyeball the layout.
    #[test]
    fn every_view_renders() {
        use ratatui::backend::TestBackend;
        let (interval, _rx) = watch::channel(Duration::from_secs(1));
        let mut app = App::new("http://127.0.0.1:8080".into(), interval);
        let t0 = Instant::now();
        for tick in 1..40 {
            let at = t0 + Duration::from_secs(tick);
            app.ingest((
                at,
                Duration::from_millis(3),
                Ok(snapshot(tick * tick % 17 + tick)),
            ));
        }
        for (width, height) in [(150, 42), (80, 24)] {
            for view in 0..VIEWS.len() {
                for (style, focus) in (0..STYLES.len()).flat_map(|s| [(s, 0), (s, 1)]) {
                    app.view = view;
                    app.style = style;
                    app.focus = focus;
                    let mut terminal =
                        ratatui::Terminal::new(TestBackend::new(width, height)).unwrap();
                    terminal.draw(|frame| app.draw(frame)).unwrap();
                    if std::env::var_os("MONITOR_PREVIEW").is_some()
                        && width > 100
                        && focus == 0
                        && (style == 0 || view == 0)
                    {
                        let buffer = terminal.backend().buffer();
                        for y in 0..height {
                            let row: String = (0..width).map(|x| buffer[(x, y)].symbol()).collect();
                            println!("{row}");
                        }
                    }
                }
            }
        }
        app.help = true;
        let mut terminal = ratatui::Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
    }
}
