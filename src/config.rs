use crate::proto::geyser::CommitmentLevel;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};
use url::Url;

const YELLOWSTONE_URL_ERROR: &str =
    "yellowstone url must use http://, https://, or unix:///absolute/path.sock";
const YELLOWSTONE_UNIX_PATH_ERROR: &str =
    "yellowstone unix url must include an absolute socket path";
pub const HELIUS_PRECONF_HOST: &str = "beta.helius-rpc.com";

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub region_include: Vec<HeliusPreconfRegion>,
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
    #[serde(rename = "yellowstone_deshred")]
    YellowstoneDeshred,
    Arpc,
    Thor,
    Shredstream,
    Shreder,
    Jetstream,
    #[serde(rename = "helius_preconf")]
    HeliusPreconf,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HeliusPreconfRegion {
    Slc,
    Fra,
    Lon,
    Pit,
    Sgp,
    Ewr,
    Tyo,
    Ams,
    Dal,
    Dub,
    Mia,
    Lax,
    Iad,
    Sea,
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
            EndpointKind::YellowstoneDeshred => "yellowstone_deshred",
            EndpointKind::Arpc => "arpc",
            EndpointKind::Thor => "thor",
            EndpointKind::Shredstream => "shredstream",
            EndpointKind::Shreder => "shreder",
            EndpointKind::Jetstream => "jetstream",
            EndpointKind::HeliusPreconf => "helius_preconf",
        }
    }

    pub fn is_yellowstone_family(&self) -> bool {
        matches!(
            self,
            EndpointKind::Yellowstone | EndpointKind::YellowstoneDeshred
        )
    }

    pub fn is_preconf(&self) -> bool {
        matches!(self, EndpointKind::HeliusPreconf)
    }
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config: Self = toml::from_str(&content).map_err(|mut err| {
            err.set_input(None);
            anyhow!(err)
        })?;
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
                    region_include: Vec::new(),
                    kind: EndpointKind::Yellowstone,
                },
                Endpoint {
                    name: "arpc".to_string(),
                    url: "http://fra.corvus-labs.io:20202".to_string(),
                    x_token: None,
                    region_include: Vec::new(),
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
        let mut endpoint_names = HashSet::new();
        for endpoint in &self.endpoint {
            if !endpoint_names.insert(endpoint.name.as_str()) {
                bail!("duplicate endpoint name '{}'", endpoint.name);
            }
        }

        let preconf_endpoints = self
            .endpoint
            .iter()
            .filter(|endpoint| endpoint.kind.is_preconf())
            .collect::<Vec<_>>();
        if preconf_endpoints.len() > 1 {
            bail!("at most one kind='helius_preconf' endpoint is supported");
        }
        if !preconf_endpoints.is_empty()
            && !self
                .endpoint
                .iter()
                .any(|endpoint| !endpoint.kind.is_preconf())
        {
            bail!("at least one non-preconf [[endpoint]] entry is required");
        }

        for endpoint in &self.endpoint {
            if endpoint.kind.is_yellowstone_family() {
                parse_yellowstone_endpoint_url(&endpoint.url).map_err(|err| {
                    anyhow!("invalid yellowstone url for '{}': {err}", endpoint.name)
                })?;
            }
            if !endpoint.kind.is_preconf() && !endpoint.region_include.is_empty() {
                bail!(
                    "endpoint '{}' may only set region_include when kind='helius_preconf'",
                    endpoint.name
                );
            }
        }

        if let Some(endpoint) = preconf_endpoints.first() {
            if self.config.account.is_empty() {
                bail!(
                    "Helius preconfSubscribe requires at least one config.account entry to avoid an unfiltered stream"
                );
            }
            if self.config.account.len() > 500 {
                bail!("Helius preconfSubscribe supports at most 500 config.account entries");
            }
            for (index, account) in self.config.account.iter().enumerate() {
                account.parse::<solana_pubkey::Pubkey>().with_context(|| {
                    format!("invalid pubkey in config.account[{index}]: {account}")
                })?;
            }

            let url = Url::parse(&endpoint.url).with_context(|| {
                format!(
                    "invalid URL for Helius preconf endpoint '{}'",
                    endpoint.name
                )
            })?;
            if url.scheme() != "wss" {
                bail!(
                    "Helius preconf endpoint '{}' must use wss://",
                    endpoint.name
                );
            }
            if url.host_str() != Some(HELIUS_PRECONF_HOST)
                || url.port().is_some_and(|port| port != 443)
            {
                bail!(
                    "Helius preconf endpoint '{}' must use the official wss://{HELIUS_PRECONF_HOST}/ endpoint",
                    endpoint.name
                );
            }

            let has_query_api_key = url
                .query_pairs()
                .any(|(key, value)| key == "api-key" && !value.trim().is_empty());
            let has_token_api_key = endpoint
                .x_token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty());
            if !has_query_api_key && !has_token_api_key {
                bail!(
                    "Helius preconf endpoint '{}' requires an API key in x_token or the api-key URL query parameter",
                    endpoint.name
                );
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
        ArgsCommitment, ConfigToml, EndpointKind, HeliusPreconfRegion, YellowstoneEndpointUrl,
        parse_yellowstone_endpoint_url,
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
    fn accepts_yellowstone_deshred_kind() {
        let raw = r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone_deshred"
"#;

        let parsed: ConfigToml = toml::from_str(raw).expect("config should deserialize");

        assert!(matches!(
            parsed.endpoint[0].kind,
            EndpointKind::YellowstoneDeshred
        ));
    }

    #[test]
    fn accepts_authenticated_helius_preconf_with_regions() {
        let path = write_temp_config(
            "helius-preconf",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone"

[[endpoint]]
name = "preconf"
url = "wss://beta.helius-rpc.com/"
x_token = "test-api-key"
region_include = ["sgp", "tyo"]
kind = "helius_preconf"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("Helius preconf config should be accepted");
        assert_eq!(loaded.endpoint[1].kind, EndpointKind::HeliusPreconf);
        assert_eq!(
            loaded.endpoint[1].region_include,
            vec![HeliusPreconfRegion::Sgp, HeliusPreconfRegion::Tyo]
        );

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_helius_preconf_without_api_key() {
        let path = write_temp_config(
            "helius-preconf-no-key",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone"

[[endpoint]]
name = "preconf"
url = "wss://beta.helius-rpc.com/"
kind = "helius_preconf"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("missing API key should be rejected");
        assert!(err.to_string().contains("requires an API key"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_unfiltered_helius_preconf_subscription() {
        let path = write_temp_config(
            "helius-preconf-empty-accounts",
            r#"
[config]
transactions = 1000
account = []
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone"

[[endpoint]]
name = "preconf"
url = "wss://beta.helius-rpc.com/"
x_token = "test-api-key"
kind = "helius_preconf"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("empty account filter should be rejected");
        assert!(err.to_string().contains("at least one config.account"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_invalid_helius_preconf_account_before_connecting() {
        let path = write_temp_config(
            "helius-preconf-invalid-account",
            r#"
[config]
transactions = 1000
account = ["not-a-pubkey"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone"

[[endpoint]]
name = "preconf"
url = "wss://beta.helius-rpc.com/"
x_token = "test-api-key"
kind = "helius_preconf"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("invalid account should be rejected before subscribing");
        assert!(err.to_string().contains("config.account[0]"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_preconf_region_on_non_preconf_endpoint() {
        let path = write_temp_config(
            "non-preconf-region",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
region_include = ["sgp"]
kind = "yellowstone"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("region filter on a non-preconf endpoint should be rejected");
        assert!(err.to_string().contains("may only set region_include"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn config_parse_errors_do_not_echo_api_keys() {
        let path = write_temp_config(
            "secret-redaction",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "preconf"
url = "wss://beta.helius-rpc.com/"
x_token = "sentinel-secret-must-not-leak"
region_include = ["not-a-region"]
kind = "helius_preconf"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("invalid region should fail parsing")
            .to_string();
        assert!(!err.contains("sentinel-secret-must-not-leak"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_duplicate_endpoint_names() {
        let path = write_temp_config(
            "duplicate-endpoint-name",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "duplicate"
url = "http://127.0.0.1:10000"
kind = "yellowstone"

[[endpoint]]
name = "duplicate"
url = "http://127.0.0.1:20000"
kind = "arpc"
"#,
        );

        let err = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect_err("duplicate endpoint names should fail validation");
        assert!(err.to_string().contains("duplicate endpoint name"));

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn accepts_yellowstone_deshred_http_url() {
        let path = write_temp_config(
            "yellowstone-deshred-http",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "http://127.0.0.1:10000"
kind = "yellowstone_deshred"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("http yellowstone deshred url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "http://127.0.0.1:10000");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn accepts_yellowstone_deshred_https_url() {
        let path = write_temp_config(
            "yellowstone-deshred-https",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "https://example.com:443"
kind = "yellowstone_deshred"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("https yellowstone deshred url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "https://example.com:443");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn accepts_yellowstone_deshred_unix_url() {
        let path = write_temp_config(
            "yellowstone-deshred-unix",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "unix:///tmp/geyser.sock"
kind = "yellowstone_deshred"
"#,
        );

        let loaded = ConfigToml::load(path.to_str().expect("utf-8 temp path"))
            .expect("unix yellowstone deshred url should be accepted");
        assert_eq!(loaded.endpoint[0].url, "unix:///tmp/geyser.sock");

        fs::remove_file(path).expect("temporary config should be removed");
    }

    #[test]
    fn rejects_yellowstone_deshred_bare_socket_path() {
        let path = write_temp_config(
            "yellowstone-deshred-bare-socket",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "/tmp/geyser.sock"
kind = "yellowstone_deshred"
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
    fn rejects_yellowstone_deshred_url_with_unsupported_scheme() {
        let path = write_temp_config(
            "yellowstone-deshred-unsupported-scheme",
            r#"
[config]
transactions = 1000
account = ["11111111111111111111111111111111"]
commitment = "processed"

[[endpoint]]
name = "grpc"
url = "grpc://127.0.0.1:10000"
kind = "yellowstone_deshred"
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
    fn endpoint_kind_reports_yellowstone_family_members() {
        assert_eq!(EndpointKind::Yellowstone.as_str(), "yellowstone");
        assert_eq!(
            EndpointKind::YellowstoneDeshred.as_str(),
            "yellowstone_deshred"
        );
        assert_eq!(EndpointKind::HeliusPreconf.as_str(), "helius_preconf");
        assert!(EndpointKind::Yellowstone.is_yellowstone_family());
        assert!(EndpointKind::YellowstoneDeshred.is_yellowstone_family());
        assert!(!EndpointKind::Arpc.is_yellowstone_family());
        assert!(EndpointKind::HeliusPreconf.is_preconf());
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
