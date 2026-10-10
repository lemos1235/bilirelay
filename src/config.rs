use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub groups: Vec<GroupConfig>,
    pub upstreams: Vec<UpstreamConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub client_tokens: Vec<String>,
    #[serde(default)]
    pub admin_token: Option<String>,
    #[serde(default = "default_request_deadline_secs")]
    pub request_deadline_secs: u64,
    #[serde(default = "default_queue_wait_secs")]
    pub queue_wait_secs: u64,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            client_tokens: Vec::new(),
            admin_token: None,
            request_deadline_secs: default_request_deadline_secs(),
            queue_wait_secs: default_queue_wait_secs(),
            max_body_bytes: default_max_body_bytes(),
        }
    }
}

impl ServerConfig {
    pub fn request_deadline(&self) -> Duration {
        Duration::from_secs(self.request_deadline_secs)
    }

    pub fn queue_wait(&self) -> Duration {
        Duration::from_secs(self.queue_wait_secs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    Priority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamApi {
    #[default]
    Responses,
    ChatCompletions,
}

impl UpstreamApi {
    pub fn path(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::ChatCompletions => "chat/completions",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConfig {
    pub name: String,
    #[serde(default)]
    pub strategy: Strategy,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: usize,
    pub upstreams: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    #[serde(default)]
    pub api: UpstreamApi,
    #[serde(default)]
    pub tier: u32,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub wait_for_slot_secs: f64,
    #[serde(default)]
    pub tpm: Option<u64>,
    #[serde(default)]
    pub rpm: Option<u64>,
    #[serde(default)]
    pub tpm_count_max_output: bool,
    #[serde(default)]
    pub drop_fields: Vec<String>,
    #[serde(default, rename = "override")]
    pub overrides: serde_json::Map<String, serde_json::Value>,
}

impl UpstreamConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    pub fn wait_for_slot(&self) -> Duration {
        Duration::from_secs_f64(self.wait_for_slot_secs)
    }
}

fn default_bind() -> String {
    "127.0.0.1:40100".to_string()
}
fn default_request_deadline_secs() -> u64 {
    115
}
fn default_queue_wait_secs() -> u64 {
    20
}
fn default_max_body_bytes() -> usize {
    4 * 1024 * 1024
}
fn default_max_attempts() -> usize {
    3
}
fn default_max_concurrency() -> usize {
    16
}
fn default_timeout_secs() -> u64 {
    100
}
fn default_true() -> bool {
    true
}

impl Config {
    pub fn group_uses_chat(&self, group: &str) -> bool {
        self.groups
            .iter()
            .filter(|g| g.name == group)
            .flat_map(|g| &g.upstreams)
            .filter_map(|member| self.upstreams.iter().find(|u| &u.name == member))
            .any(|u| u.api == UpstreamApi::ChatCompletions)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let mut config: Config =
            toml::from_str(text).context("failed to parse configuration file")?;
        config.server.client_tokens.retain(|t| !t.is_empty());
        if config.server.admin_token.as_deref() == Some("") {
            config.server.admin_token = None;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        let bind: SocketAddr = self.server.bind.parse().with_context(|| {
            format!(
                "server.bind '{}' is not a valid IP:port address",
                self.server.bind
            )
        })?;
        if self.server.client_tokens.is_empty() && !bind.ip().is_loopback() {
            bail!(
                "when server.client_tokens is empty, binding is only allowed on a loopback address to avoid exposing upstream API keys through a public proxy"
            );
        }
        if self.server.request_deadline_secs == 0 {
            bail!("server.request_deadline_secs must be greater than 0");
        }

        let mut upstream_names = HashSet::new();
        for upstream in &self.upstreams {
            let name = &upstream.name;
            if !upstream_names.insert(name.as_str()) {
                bail!("duplicate upstream name: '{name}'");
            }
            if !(upstream.base_url.starts_with("http://")
                || upstream.base_url.starts_with("https://"))
            {
                bail!("upstream '{name}'.base_url must start with http:// or https://");
            }
            if upstream.model.trim().is_empty() {
                bail!("upstream '{name}'.model must not be empty");
            }
            if upstream.max_concurrency == 0 {
                bail!("upstream '{name}'.max_concurrency must be greater than 0");
            }
            if !(upstream.wait_for_slot_secs.is_finite() && upstream.wait_for_slot_secs >= 0.0) {
                bail!("upstream '{name}'.wait_for_slot_secs must be non-negative");
            }
            if upstream.tpm == Some(0) || upstream.rpm == Some(0) {
                bail!(
                    "upstream '{name}' tpm / rpm must be greater than 0 (omit them for unlimited rates)"
                );
            }
            if upstream.timeout_secs == 0 {
                bail!("upstream '{name}'.timeout_secs must be greater than 0");
            }
        }

        if self.groups.is_empty() {
            bail!("at least one [[groups]] entry must be configured");
        }
        let mut group_names = HashSet::new();
        for group in &self.groups {
            let name = &group.name;
            if !group_names.insert(name.as_str()) {
                bail!("duplicate model group name: '{name}'");
            }
            if group.max_attempts == 0 {
                bail!("group '{name}'.max_attempts must be greater than 0");
            }
            if group.upstreams.is_empty() {
                bail!("group '{name}' has no upstreams");
            }
            let mut seen = HashSet::new();
            for member in &group.upstreams {
                if !upstream_names.contains(member.as_str()) {
                    bail!("group '{name}' references an unknown upstream '{member}'");
                }
                if !seen.insert(member.as_str()) {
                    bail!("group '{name}' lists upstream '{member}' more than once");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [server]
        client_tokens = ["t1"]

        [[groups]]
        name = "translate"
        upstreams = ["a"]

        [[upstreams]]
        name = "a"
        base_url = "https://api.example.com/v1"
        api_key = "sk-abc-x"
        model = "m"
    "#;

    #[test]
    fn parses_and_fills_defaults() {
        let config = Config::parse(MINIMAL).unwrap();
        assert_eq!(config.server.client_tokens, vec!["t1"]);
        assert_eq!(config.upstreams[0].api_key, "sk-abc-x");
        assert_eq!(config.groups[0].strategy, Strategy::Priority);
        assert_eq!(config.groups[0].max_attempts, 3);
        assert_eq!(config.upstreams[0].max_concurrency, 16);
        assert_eq!(config.server.bind, "127.0.0.1:40100");
    }

    #[test]
    fn rejects_unsupported_strategy() {
        let text = MINIMAL.replace(
            "upstreams = [\"a\"]",
            "upstreams = [\"a\"]\nstrategy = \"unsupported\"",
        );
        assert!(Config::parse(&text).is_err());
    }

    #[test]
    fn rejects_obsolete_weight_field() {
        let text = MINIMAL.replace("model = \"m\"", "model = \"m\"\nweight = 1");
        assert!(Config::parse(&text).is_err());
    }

    #[test]
    fn parses_example_config() {
        let config = Config::parse(include_str!("../config.example.toml")).unwrap();
        assert!(
            config
                .groups
                .iter()
                .all(|g| g.strategy == Strategy::Priority)
        );
    }

    #[test]
    fn rejects_open_proxy_on_public_bind() {
        let text = MINIMAL
            .replace("[server]", "[server]\nbind = \"0.0.0.0:40100\"")
            .replace("client_tokens = [\"t1\"]", "client_tokens = []");
        let err = Config::parse(&text).unwrap_err();
        assert!(format!("{err:#}").contains("loopback"), "{err:#}");

        let text_with_token = MINIMAL.replace("[server]", "[server]\nbind = \"0.0.0.0:40100\"");
        assert!(Config::parse(&text_with_token).is_ok());
    }

    #[test]
    fn rejects_unknown_upstream_reference() {
        let text = MINIMAL.replace("upstreams = [\"a\"]", "upstreams = [\"a\", \"b\"]");
        let err = Config::parse(&text).unwrap_err();
        assert!(format!("{err:#}").contains("'b'"), "{err:#}");
    }

    #[test]
    fn parses_strategy_drop_fields_and_override() {
        let text = MINIMAL
            .replace("upstreams = [\"a\"]", "upstreams = [\"a\"]\nstrategy = \"priority\"")
            .replace(
                "model = \"m\"",
                "model = \"m\"\ndrop_fields = [\"store\"]\noverride = { reasoning = { effort = \"low\" } }",
            );
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.groups[0].strategy, Strategy::Priority);
        assert_eq!(config.upstreams[0].drop_fields, vec!["store"]);
        assert_eq!(config.upstreams[0].overrides["reasoning"]["effort"], "low");
        assert_eq!(config.upstreams[0].api, UpstreamApi::Responses);
        assert!(!config.group_uses_chat("translate"));

        let chat = text.replace("model = \"m\"", "model = \"m\"\napi = \"chat_completions\"");
        let config = Config::parse(&chat).unwrap();
        assert_eq!(config.upstreams[0].api, UpstreamApi::ChatCompletions);
        assert!(config.group_uses_chat("translate"));
    }
}
