use crate::proto::geyser::CommitmentLevel;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};
use url::Url;

const YELLOWSTONE_URL_ERROR: &str =
    "yellowstone url must use http://, https://, or unix:///absolute/path.sock";
const YELLOWSTONE_UNIX_PATH_ERROR: &str =
    "yellowstone unix url must include an absolute socket path";

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigToml {
    pub config: Config,
    pub endpoint: Vec<Endpoint>,
    #[serde(default)]
    pub backend: BackendSettings,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub transactions: i32,
    pub account: Vec<String>,
    pub commitment: ArgsCommitment,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_token: Option<String>,
    pub kind: EndpointKind,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct BackendSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EndpointKind {
    Yellowstone,
    Arpc,
    Thor,
    Shredstream,
    Shreder,
    Jetstream,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YellowstoneEndpointUrl {
    Http,
    Https,
    Unix(PathBuf),
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

impl ArgsCommitment {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArgsCommitment::Processed => "processed",
            ArgsCommitment::Confirmed => "confirmed",
            ArgsCommitment::Finalized => "finalized",
        }
    }
}

impl EndpointKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EndpointKind::Yellowstone => "yellowstone",
            EndpointKind::Arpc => "arpc",
            EndpointKind::Thor => "thor",
            EndpointKind::Shredstream => "shredstream",
            EndpointKind::Shreder => "shreder",
            EndpointKind::Jetstream => "jetstream",
        }
    }
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config: Self = toml::from_str(&content).map_err(|err| anyhow!(err))?;
        config.validate()?;
        Ok(config)
    }

    pub fn create_default(path: &str) -> Result<Self> {
        let default_config = ConfigToml {
            config: Config {
                transactions: 1000,
                account: vec!["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()],
                commitment: ArgsCommitment::Processed,
            },
            endpoint: vec![
                Endpoint {
                    name: "grpc".to_string(),
                    url: "http://fra.corvus-labs.io:10101".to_string(),
                    x_token: None,
                    kind: EndpointKind::Yellowstone,
                },
                Endpoint {
                    name: "arpc".to_string(),
                    url: "http://fra.corvus-labs.io:20202".to_string(),
                    x_token: None,
                    kind: EndpointKind::Arpc,
                },
            ],
            backend: BackendSettings::default(),
        };

        let toml_string = toml::to_string_pretty(&default_config)
            .context("Failed to serialize default config")?;
        fs::write(path, toml_string)
            .with_context(|| format!("Failed to write default config {}", path))?;

        Ok(default_config)
    }

    pub fn load_or_create(path: &str) -> Result<Self> {
        if Path::new(path).exists() {
            Self::load(path)
        } else {
            Self::create_default(path)
        }
    }

    fn validate(&self) -> Result<()> {
        for endpoint in &self.endpoint {
            if endpoint.kind == EndpointKind::Yellowstone {
                parse_yellowstone_endpoint_url(&endpoint.url).map_err(|err| {
                    anyhow!("invalid yellowstone url for '{}': {err}", endpoint.name)
                })?;
            }
        }
        Ok(())
    }
}

pub fn parse_yellowstone_endpoint_url(raw: &str) -> Result<YellowstoneEndpointUrl> {
    if let Some(path) = raw.strip_prefix("unix://") {
        if path.is_empty() || !path.starts_with('/') {
            bail!(YELLOWSTONE_UNIX_PATH_ERROR);
        }
        return Ok(YellowstoneEndpointUrl::Unix(PathBuf::from(path)));
    }

    let parsed = Url::parse(raw).map_err(|_| anyhow!(YELLOWSTONE_URL_ERROR))?;
    if parsed.host_str().is_none() {
        bail!(YELLOWSTONE_URL_ERROR);
    }

    match parsed.scheme() {
        "http" => Ok(YellowstoneEndpointUrl::Http),
        "https" => Ok(YellowstoneEndpointUrl::Https),
        _ => bail!(YELLOWSTONE_URL_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArgsCommitment, ConfigToml, YellowstoneEndpointUrl, parse_yellowstone_endpoint_url,
    };
    use crate::providers::common::WatchedAccounts;
    use std::{
        env, fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_config_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        env::temp_dir().join(format!("geyserbench-{prefix}-{unique}.toml"))
    }

    fn write_temp_config(prefix: &str, raw: &str) -> PathBuf {
        let path = temp_config_path(prefix);
        fs::write(&path, raw).expect("temporary config should be written");
        path
    }

    #[test]
    fn deserializes_account_array() {
        let raw = r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]
commitment = "confirmed"

[[endpoint]]
name = "grpc"
url = "http://localhost:10000"
kind = "yellowstone"
"#;

        let parsed: ConfigToml = toml::from_str(raw).expect("config should deserialize");

        assert_eq!(
            parsed.config.account,
            vec![
                "11111111111111111111111111111111".to_string(),
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string()
            ]
        );
        assert!(matches!(
            parsed.config.commitment,
            ArgsCommitment::Confirmed
        ));
    }

    #[test]
    fn create_default_writes_account_array_syntax() {
        let path = temp_config_path("default");

        let created = ConfigToml::create_default(path.to_str().expect("utf-8 temp path"))
            .expect("default config should be created");
        let written = fs::read_to_string(&path).expect("default config should be readable");

        assert_eq!(created.config.account.len(), 1);
        assert!(written.contains("account = ["));
        assert!(!written.contains("account = \""));

        let parsed = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("written config should parse");
        assert_eq!(parsed.config.account, created.config.account);

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn invalid_pubkey_values_fail_with_clear_error() {
        let raw = r#"
[config]
transactions = 1000
account = ["not-a-pubkey"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://localhost:10000"
kind = "yellowstone"
"#;

        let parsed: ConfigToml = toml::from_str(raw).expect("config should deserialize");
        let err = WatchedAccounts::new(&parsed.config.account)
            .expect_err("invalid account should fail validation");

        assert!(err.to_string().contains("config.account[0]"));
        assert!(err.to_string().contains("not-a-pubkey"));
    }

    #[test]
    fn accepts_yellowstone_http_url() {
        let path = write_temp_config(
            "yellowstone-http",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("http yellowstone url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "http://127.0.0.1:10000");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn accepts_yellowstone_https_url() {
        let path = write_temp_config(
            "yellowstone-https",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "https://example.com:443"
kind = "yellowstone"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("https yellowstone url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "https://example.com:443");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn accepts_yellowstone_unix_url() {
        let path = write_temp_config(
            "yellowstone-unix",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "unix:///tmp/geyser.sock"
kind = "yellowstone"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("unix yellowstone url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "unix:///tmp/geyser.sock");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_yellowstone_bare_socket_path() {
        let path = write_temp_config(
            "yellowstone-bare-socket",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "/tmp/geyser.sock"
kind = "yellowstone"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("bare socket path should be rejected");
        assert!(
            err.to_string().contains(
                "yellowstone url must use http://, https://, or unix:///absolute/path.sock"
            )
        );

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_yellowstone_url_with_unsupported_scheme() {
        let path = write_temp_config(
            "yellowstone-unsupported-scheme",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "grpc://127.0.0.1:10000"
kind = "yellowstone"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("unsupported scheme should be rejected");
        assert!(
            err.to_string().contains(
                "yellowstone url must use http://, https://, or unix:///absolute/path.sock"
            )
        );

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_yellowstone_unix_url_without_path() {
        let path = write_temp_config(
            "yellowstone-empty-unix",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "unix://"
kind = "yellowstone"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("unix url without path should be rejected");
        assert!(
            err.to_string()
                .contains("yellowstone unix url must include an absolute socket path")
        );

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn parses_yellowstone_http_url() {
        let parsed = parse_yellowstone_endpoint_url("http://127.0.0.1:10000")
            .expect("http url should parse");
        assert_eq!(parsed, YellowstoneEndpointUrl::Http);
    }

    #[test]
    fn parses_yellowstone_https_url() {
        let parsed = parse_yellowstone_endpoint_url("https://example.com:443")
            .expect("https url should parse");
        assert_eq!(parsed, YellowstoneEndpointUrl::Https);
    }

    #[test]
    fn parses_yellowstone_unix_url() {
        let parsed = parse_yellowstone_endpoint_url("unix:///tmp/geyser.sock")
            .expect("unix url should parse");
        assert_eq!(
            parsed,
            YellowstoneEndpointUrl::Unix(PathBuf::from("/tmp/geyser.sock"))
        );
    }
}
