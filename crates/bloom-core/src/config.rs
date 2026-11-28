use crate::error::{BloomError, Result};
use crate::id::NodeId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BloomConfig {
    pub daemon: DaemonConfig,
    pub api: ApiConfig,
    pub telemetry: TelemetryConfig,
    pub ollama: OllamaConfig,
    pub policies: PolicyConfig,
    pub swarm: SwarmConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SwarmConfig {
    pub discovery: bool,
    pub advertise: bool,
    pub advertise_https_port: Option<u16>,
    pub ca_pem: Option<PathBuf>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub node_id: NodeId,
    pub url: String,
    pub token_env: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub name: String,
    pub data_dir: PathBuf,
    pub availability: AvailabilitySetting,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilitySetting {
    #[default]
    Continuous,
    Restricted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub bind: String,
    pub token: Option<String>,
    pub token_env: Option<String>,
    pub peer_token_env: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OllamaConfig {
    pub enabled: bool,
    pub mode: HarnessMode,
    pub base_url: String,
    pub binary: String,
    pub keep_alive: String,
    pub spawn_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HarnessMode {
    #[default]
    Attached,
    Managed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicyConfig {
    pub max_active_jobs: u32,
    pub drain_on_start: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            data_dir: PathBuf::from("~/.local/share/bloom"),
            availability: AvailabilitySetting::Continuous,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7420".to_string(),
            token: None,
            token_env: None,
            peer_token_env: None,
            tls_cert: None,
            tls_key: None,
        }
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self { interval_ms: 1000 }
    }
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: HarnessMode::Attached,
            base_url: "http://127.0.0.1:11434".to_string(),
            binary: "ollama".to_string(),
            keep_alive: "5m".to_string(),
            spawn_timeout_ms: 15000,
        }
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            max_active_jobs: 4,
            drain_on_start: false,
        }
    }
}

impl BloomConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| BloomError::Config(format!("cannot read {}: {e}", path.display())))?;
        let config: Self = toml::from_str(&raw)
            .map_err(|e| BloomError::Config(format!("cannot parse {}: {e}", path.display())))?;
        Ok(config)
    }

    pub fn data_dir(&self) -> PathBuf {
        expand_home(&self.daemon.data_dir)
    }

    pub fn database_path(&self) -> PathBuf {
        self.data_dir().join("bloom.db")
    }
}

impl ApiConfig {
    pub fn resolve_token(&self) -> Result<Option<String>> {
        let token = match (&self.token, &self.token_env) {
            (Some(_), Some(_)) => {
                return Err(BloomError::Config(
                    "set only one of api.token and api.token_env".into(),
                ));
            }
            (_, Some(name)) => Some(std::env::var(name).map_err(|_| {
                BloomError::Config(format!("api.token_env variable {name} is unavailable"))
            })?),
            (token, None) => token.clone(),
        };
        validate_token(token.as_deref())?;
        Ok(token)
    }

    pub fn resolve_peer_token(&self, operator_token: Option<&str>) -> Result<Option<String>> {
        let Some(name) = &self.peer_token_env else {
            return Ok(None);
        };
        let token = std::env::var(name).map_err(|_| {
            BloomError::Config(format!("api.peer_token_env variable {name} is unavailable"))
        })?;
        validate_token(Some(&token))?;
        if operator_token.is_none_or(|operator| operator == token) {
            return Err(BloomError::Config(
                "api.peer_token_env requires a separate operator token".into(),
            ));
        }
        Ok(Some(token))
    }
}

fn validate_token(token: Option<&str>) -> Result<()> {
    if let Some(token) = token
        && (token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()))
    {
        return Err(BloomError::Config(
            "API bearer token must contain visible ASCII characters without spaces".into(),
        ));
    }
    Ok(())
}

pub fn expand_home(path: &Path) -> PathBuf {
    let as_str = path.to_string_lossy();
    if as_str == "~"
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home);
    }
    if let Some(rest) = as_str.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse_from_empty_table() {
        let config: BloomConfig = toml::from_str("").unwrap();
        assert_eq!(config.api.bind, "127.0.0.1:7420");
        assert_eq!(config.telemetry.interval_ms, 1000);
        assert_eq!(config.ollama.mode, HarnessMode::Attached);
        assert_eq!(config.policies.max_active_jobs, 4);
    }

    #[test]
    fn parses_full_config() {
        let raw = r#"
[daemon]
name = "sal-workstation"
data_dir = "~/.local/share/bloom"

[api]
bind = "0.0.0.0:7420"
token = "pairing-token"

[telemetry]
interval_ms = 500

[ollama]
mode = "managed"
base_url = "http://127.0.0.1:11434"
keep_alive = "10m"

[policies]
max_active_jobs = 8
drain_on_start = true

[swarm]
discovery = true

[[swarm.peers]]
node_id = "lan-node"
url = "https://peer.example"
token_env = "BLOOM_PEER_TOKEN"
"#;
        let config: BloomConfig = toml::from_str(raw).unwrap();
        assert_eq!(config.daemon.name, "sal-workstation");
        assert_eq!(config.api.token.as_deref(), Some("pairing-token"));
        assert_eq!(config.telemetry.interval_ms, 500);
        assert_eq!(config.ollama.mode, HarnessMode::Managed);
        assert_eq!(config.policies.max_active_jobs, 8);
        assert!(config.policies.drain_on_start);
        assert!(config.swarm.discovery);
        assert_eq!(config.swarm.peers[0].node_id.as_str(), "lan-node");
    }

    #[test]
    fn rejects_empty_and_ambiguous_api_tokens() {
        let mut api = ApiConfig {
            token: Some(String::new()),
            ..ApiConfig::default()
        };
        assert!(api.resolve_token().is_err());
        api.token = Some("example-token".into());
        assert_eq!(api.resolve_token().unwrap(), api.token);
        api.token_env = Some("BLOOM_API_TOKEN".into());
        assert!(api.resolve_token().is_err());
    }

    #[test]
    fn expands_home_prefix() {
        let expanded = expand_home(Path::new("~/.local/share/bloom"));
        let expected = std::env::var_os("HOME").map_or_else(
            || PathBuf::from("~/.local/share/bloom"),
            |home| PathBuf::from(home).join(".local/share/bloom"),
        );
        assert_eq!(expanded, expected);
    }
}
