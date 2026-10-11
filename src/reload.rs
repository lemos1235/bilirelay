//! Config hot reload: the file is re-read when it changes on disk or on SIGHUP. A config that
//! fails to load or validate is logged and ignored, leaving the running one in place.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use crate::AppState;
use crate::config::Config;

const POLL: Duration = Duration::from_secs(2);

pub fn spawn(app: Arc<AppState>, path: PathBuf) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seen = fingerprint(&path);
        let mut hangup = hangup();
        let mut ticker = tokio::time::interval(POLL);
        loop {
            let reason = tokio::select! {
                _ = ticker.tick() => {
                    let current = fingerprint(&path);
                    if current == seen {
                        continue;
                    }
                    seen = current;
                    "file changed"
                }
                _ = hangup.recv() => "SIGHUP",
            };
            reload(&app, &path, reason);
        }
    })
}

pub fn reload(app: &AppState, path: &Path, reason: &str) {
    match Config::load(path) {
        Ok(config) => {
            let groups = config.groups.len();
            let upstreams = config.upstreams.len();
            let clients = config.clients.len();
            for setting in app.apply(config) {
                tracing::warn!(setting, "changed setting only takes effect after a restart");
            }
            tracing::info!(
                config = %path.display(),
                reason,
                groups,
                upstreams,
                clients,
                "reloaded config"
            );
        }
        Err(error) => {
            app.reload_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                config = %path.display(),
                reason,
                error = format!("{error:#}"),
                "config reload failed; keeping the running config"
            );
        }
    }
}

fn fingerprint(path: &Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

#[cfg(unix)]
struct Hangup(Option<tokio::signal::unix::Signal>);

#[cfg(unix)]
fn hangup() -> Hangup {
    use tokio::signal::unix::{SignalKind, signal};
    Hangup(signal(SignalKind::hangup()).ok())
}

#[cfg(unix)]
impl Hangup {
    async fn recv(&mut self) {
        match &mut self.0 {
            Some(signal) => {
                signal.recv().await;
            }
            None => std::future::pending().await,
        }
    }
}

#[cfg(not(unix))]
struct Hangup;

#[cfg(not(unix))]
fn hangup() -> Hangup {
    Hangup
}

#[cfg(not(unix))]
impl Hangup {
    async fn recv(&mut self) {
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
        [[groups]]
        name = "g"
        upstreams = ["a"]

        [[upstreams]]
        name = "a"
        base_url = "https://x"
        api_key = "k"
        model = "m"
    "#;

    #[test]
    fn bad_config_is_counted_and_ignored() {
        let path =
            std::env::temp_dir().join(format!("bilirelay-reload-{}.toml", std::process::id()));
        std::fs::write(&path, CONFIG).unwrap();
        let app = AppState::new(Config::load(&path).unwrap()).unwrap();

        std::fs::write(&path, CONFIG.replace("model = \"m\"", "model = \"\"")).unwrap();
        reload(&app, &path, "test");
        assert_eq!(app.reload_failures.load(Ordering::Relaxed), 1);
        assert_eq!(app.live().config.upstreams[0].model, "m");

        std::fs::write(&path, CONFIG.replace("model = \"m\"", "model = \"n\"")).unwrap();
        reload(&app, &path, "test");
        assert_eq!(app.reloads.load(Ordering::Relaxed), 1);
        assert_eq!(app.live().pool.upstreams()[0].cfg().model, "n");
        let _ = std::fs::remove_file(&path);
    }
}
