use std::path::PathBuf;

use anyhow::Context;
use bilirelay::config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1).peekable();
    if args.peek().is_some_and(|a| a == "monitor") {
        let options = bilirelay::monitor::parse_args(args.skip(1))?;
        return bilirelay::monitor::run(options).await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let path = resolve_config_path()?;
    let config = Config::load(&path)?;
    let bind = config.server.bind.clone();
    tracing::info!(
        config = %path.display(),
        groups = config.groups.len(),
        upstreams = config.upstreams.len(),
        clients = config.clients.len(),
        "loaded config"
    );

    let state = bilirelay::AppState::new(config)?;
    bilirelay::reload::spawn(state.clone(), path.clone());
    bilirelay::health::spawn(state.clone());
    let app = bilirelay::router(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("failed to bind to {bind}"))?;
    tracing::info!("bilirelay listening on http://{bind}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}

fn resolve_config_path() -> anyhow::Result<PathBuf> {
    resolve_config_path_from_args(std::env::args_os().skip(1))
}

fn resolve_config_path_from_args<I, T>(args: I) -> anyhow::Result<PathBuf>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let mut iter = args.into_iter();
    let mut positional = None;

    while let Some(arg) = iter.next() {
        let os_str = arg.into();
        let lossy = os_str.to_string_lossy();
        if lossy == "-h" || lossy == "--help" {
            println!(
                "Usage: bilirelay [-c|--config <PATH>] [PATH]\n       bilirelay monitor [--help]\n\nDefault configuration: config.toml in the current working directory"
            );
            std::process::exit(0);
        } else if lossy == "-c" || lossy == "--config" {
            let next = iter.next().context("missing value for --config")?;
            return Ok(PathBuf::from(next.into()));
        } else if let Some(val) = lossy.strip_prefix("--config=") {
            return Ok(PathBuf::from(val));
        } else if !lossy.starts_with('-') && positional.is_none() {
            positional = Some(PathBuf::from(os_str));
        }
    }

    Ok(positional.unwrap_or_else(|| PathBuf::from("config.toml")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_config_toml_in_current_dir() {
        let path = resolve_config_path_from_args(Vec::<&str>::new()).unwrap();
        assert_eq!(path, PathBuf::from("config.toml"));
    }

    #[test]
    fn supports_positional_path() {
        let path = resolve_config_path_from_args(vec!["custom.toml"]).unwrap();
        assert_eq!(path, PathBuf::from("custom.toml"));
    }

    #[test]
    fn supports_short_flag() {
        let path = resolve_config_path_from_args(vec!["-c", "other/cfg.toml"]).unwrap();
        assert_eq!(path, PathBuf::from("other/cfg.toml"));
    }

    #[test]
    fn supports_long_flag() {
        let path = resolve_config_path_from_args(vec!["--config", "my.toml"]).unwrap();
        assert_eq!(path, PathBuf::from("my.toml"));
    }

    #[test]
    fn supports_equals_flag() {
        let path = resolve_config_path_from_args(vec!["--config=app.toml"]).unwrap();
        assert_eq!(path, PathBuf::from("app.toml"));
    }
}
