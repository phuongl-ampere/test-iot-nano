use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use iot_core::{DatabaseStorage, StorageConfiguration};
use thiserror::Error;

pub const RETIRED_ENVIRONMENT_NAMES: &[&str] = &[
    "IOT_API_ADDRESS",
    "IOT_DATABASE_STORAGE",
    "IOT_MQTTD_API_BASE_URL",
    "IOT_MQTTD_CONFIG",
    "IOT_MQTTD_DEVICE_BACKEND_ADDRESS",
    "IOT_MQTTD_DEVICE_V5_BACKEND_ADDRESS",
    "IOT_MQTTD_INTERNAL_V311_ADDRESS",
    "IOT_MQTTD_INTERNAL_V5_ADDRESS",
    "IOT_MQTTD_MANAGEMENT_ADDRESS",
    "IOT_MQTTD_MAX_CONNECTIONS",
    "IOT_MQTTD_PLAIN_ADDRESS",
    "IOT_MQTTD_TLS_ADDRESS",
    "IOT_MQTTD_TLS_CERT_PATH",
    "IOT_MQTTD_TLS_KEY_PATH",
    "IOT_MQTTD_TRANSPORT_INTERNAL_ADDRESS",
    "IOT_NANO_API_CORE_SECRET",
    "IOT_NANO_API_MQTTD_SECRET",
    "IOT_NANO_API_SQLITE_PATH",
    "IOT_NANO_CORE_HEALTH_ADDRESS",
    "IOT_NANO_CORE_MQTTD_SECRET",
    "IOT_NANO_CORE_SQLITE_PATH",
    "IOT_NANO_CORE_STREAM_SECRET",
    "IOT_NANO_CORE_URL",
    "IOT_NANO_MQTTD_API_SECRET",
    "IOT_NANO_MQTTD_INTERNAL_URL",
    "IOT_NANO_MQTTD_STREAM_SECRET",
    "IOT_NANO_STREAM_ADDRESS",
    "IOT_NANO_STREAM_DIR",
    "IOT_NANO_STREAM_MAX_RECORD_BYTES",
    "IOT_NANO_STREAM_PARTITIONS",
    "IOT_NANO_STREAM_RETENTION_BYTES",
    "IOT_NANO_STREAM_RETENTION_INTERVAL_SECONDS",
    "IOT_NANO_STREAM_RETENTION_SECONDS",
    "IOT_NANO_STREAM_SECRET",
    "IOT_NANO_STREAM_SEGMENT_BYTES",
    "IOT_NANO_STREAM_URL",
    "IOT_SQLITE_BUSY_TIMEOUT_MS",
    "IOT_SQLITE_PATH",
    "USE_DATABASE_STORAGE",
];

const DEFAULT_PUBLIC_HTTP_ADDRESS: &str = "0.0.0.0:8080";
const DEFAULT_MANAGEMENT_HTTP_ADDRESS: &str = "127.0.0.1:8081";
const DEFAULT_MQTT_TCP_ADDRESS: &str = "0.0.0.0:1883";
const DEFAULT_MQTT_TLS_ADDRESS: &str = "0.0.0.0:8883";
const DEFAULT_BUSY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_SHUTDOWN_DEADLINE_SECONDS: u64 = 30;
const MAX_BUSY_TIMEOUT_MS: u64 = 60_000;
const MAX_SHUTDOWN_DEADLINE_SECONDS: u64 = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformStorage {
    Sqlite { path: PathBuf, busy_timeout_ms: u64 },
    Timescale { database_url: String },
}

impl PlatformStorage {
    pub const fn is_sqlite(&self) -> bool {
        matches!(self, Self::Sqlite { .. })
    }

    pub const fn is_timescale(&self) -> bool {
        matches!(self, Self::Timescale { .. })
    }

    pub fn as_legacy_storage_configuration(&self) -> StorageConfiguration {
        match self {
            Self::Sqlite {
                path,
                busy_timeout_ms,
            } => StorageConfiguration {
                storage: DatabaseStorage::Sqlite,
                database_url: None,
                sqlite_path: Some(path.clone()),
                sqlite_busy_timeout_ms: *busy_timeout_ms,
            },
            Self::Timescale { database_url } => StorageConfiguration {
                storage: DatabaseStorage::Timescale,
                database_url: Some(database_url.clone()),
                sqlite_path: None,
                sqlite_busy_timeout_ms: DEFAULT_BUSY_TIMEOUT_MS,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonolithConfig {
    pub storage: PlatformStorage,
    pub internal_dir: PathBuf,
    pub public_http: SocketAddr,
    pub management_http: SocketAddr,
    pub mqtt_tcp: SocketAddr,
    pub mqtt_tls: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub shutdown_deadline: Duration,
}

impl MonolithConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_values(std::env::vars().collect())
    }

    pub fn from_values(values: BTreeMap<String, String>) -> Result<Self, ConfigError> {
        validate_retired_environment(&values)?;

        let storage = parse_storage(&values)?;
        let internal_dir = absolute_path(&values, "IOT_NANO_INTERNAL_DIR")?;
        let tls_cert_path = absolute_path(&values, "IOT_NANO_TLS_CERT_PATH")?;
        let tls_key_path = absolute_path(&values, "IOT_NANO_TLS_KEY_PATH")?;
        let public_http = socket_address(
            &values,
            "IOT_NANO_PUBLIC_HTTP_ADDRESS",
            DEFAULT_PUBLIC_HTTP_ADDRESS,
        )?;
        let management_http = socket_address(
            &values,
            "IOT_NANO_MANAGEMENT_ADDRESS",
            DEFAULT_MANAGEMENT_HTTP_ADDRESS,
        )?;
        let mqtt_tcp = socket_address(
            &values,
            "IOT_NANO_MQTT_TCP_ADDRESS",
            DEFAULT_MQTT_TCP_ADDRESS,
        )?;
        let mqtt_tls = socket_address(
            &values,
            "IOT_NANO_MQTT_TLS_ADDRESS",
            DEFAULT_MQTT_TLS_ADDRESS,
        )?;
        validate_unique_addresses([public_http, management_http, mqtt_tcp, mqtt_tls])?;
        let shutdown_deadline = Duration::from_secs(bounded_u64(
            &values,
            "IOT_NANO_SHUTDOWN_DEADLINE_SECONDS",
            DEFAULT_SHUTDOWN_DEADLINE_SECONDS,
            MAX_SHUTDOWN_DEADLINE_SECONDS,
        )?);

        Ok(Self {
            storage,
            internal_dir,
            public_http,
            management_http,
            mqtt_tcp,
            mqtt_tls,
            tls_cert_path,
            tls_key_path,
            shutdown_deadline,
        })
    }
}

pub fn validate_retired_environment(values: &BTreeMap<String, String>) -> Result<(), ConfigError> {
    RETIRED_ENVIRONMENT_NAMES
        .iter()
        .find(|name| values.contains_key(**name))
        .map(|name| Err(ConfigError::RetiredEnvironment((*name).to_owned())))
        .unwrap_or(Ok(()))
}

fn parse_storage(values: &BTreeMap<String, String>) -> Result<PlatformStorage, ConfigError> {
    let storage = values
        .get("IOT_NANO_STORAGE")
        .filter(|value| !value.trim().is_empty())
        .ok_or(ConfigError::MissingStorage)?;

    match storage.as_str() {
        "sqlite" => {
            if has_value(values, "DATABASE_URL") {
                return Err(ConfigError::ContradictoryStorage(
                    "DATABASE_URL is not valid with IOT_NANO_STORAGE=sqlite".to_owned(),
                ));
            }
            let path = absolute_path(values, "IOT_NANO_SQLITE_PATH")?;
            let busy_timeout_ms = bounded_u64(
                values,
                "IOT_NANO_SQLITE_BUSY_TIMEOUT_MS",
                DEFAULT_BUSY_TIMEOUT_MS,
                MAX_BUSY_TIMEOUT_MS,
            )?;
            Ok(PlatformStorage::Sqlite {
                path,
                busy_timeout_ms,
            })
        }
        "timescale" => {
            if has_value(values, "IOT_NANO_SQLITE_PATH") {
                return Err(ConfigError::ContradictoryStorage(
                    "IOT_NANO_SQLITE_PATH is not valid with IOT_NANO_STORAGE=timescale".to_owned(),
                ));
            }
            let database_url = values
                .get("DATABASE_URL")
                .filter(|value| !value.trim().is_empty())
                .cloned()
                .ok_or(ConfigError::MissingDatabaseUrl)?;
            Ok(PlatformStorage::Timescale { database_url })
        }
        value => Err(ConfigError::UnsupportedStorage(value.to_owned())),
    }
}

fn has_value(values: &BTreeMap<String, String>, name: &str) -> bool {
    values
        .get(name)
        .is_some_and(|value| !value.trim().is_empty())
}

fn absolute_path(
    values: &BTreeMap<String, String>,
    name: &'static str,
) -> Result<PathBuf, ConfigError> {
    let value = values
        .get(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            if matches!(name, "IOT_NANO_TLS_CERT_PATH" | "IOT_NANO_TLS_KEY_PATH") {
                ConfigError::IncompleteTls
            } else {
                ConfigError::MissingPath(name)
            }
        })?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(ConfigError::InvalidAbsolutePath {
            name,
            value: value.to_owned(),
        });
    }
    Ok(path)
}

fn socket_address(
    values: &BTreeMap<String, String>,
    name: &'static str,
    default: &str,
) -> Result<SocketAddr, ConfigError> {
    values
        .get(name)
        .map(String::as_str)
        .unwrap_or(default)
        .parse()
        .map_err(|_| ConfigError::InvalidSocketAddress {
            name,
            value: values
                .get(name)
                .cloned()
                .unwrap_or_else(|| default.to_owned()),
        })
}

fn validate_unique_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
) -> Result<(), ConfigError> {
    let mut seen = BTreeSet::new();
    for address in addresses {
        if !seen.insert(address) {
            return Err(ConfigError::DuplicateListenerAddress(address));
        }
    }
    Ok(())
}

fn bounded_u64(
    values: &BTreeMap<String, String>,
    name: &'static str,
    default: u64,
    maximum: u64,
) -> Result<u64, ConfigError> {
    let value = values
        .get(name)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidUnsignedInteger {
                    name,
                    value: value.to_owned(),
                })
        })
        .transpose()?
        .unwrap_or(default);
    if value == 0 || value > maximum {
        return Err(ConfigError::InvalidUnsignedInteger {
            name,
            value: value.to_string(),
        });
    }
    Ok(value)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("retired monolith environment variable {0} is not supported")]
    RetiredEnvironment(String),
    #[error("IOT_NANO_STORAGE is required")]
    MissingStorage,
    #[error("IOT_NANO_STORAGE must be `sqlite` or `timescale`, got {0:?}")]
    UnsupportedStorage(String),
    #[error("contradictory platform storage configuration: {0}")]
    ContradictoryStorage(String),
    #[error("DATABASE_URL is required when IOT_NANO_STORAGE=timescale")]
    MissingDatabaseUrl,
    #[error("{0} is required")]
    MissingPath(&'static str),
    #[error("TLS certificate and key paths must both be configured")]
    IncompleteTls,
    #[error("{name} must be an absolute path, got {value:?}")]
    InvalidAbsolutePath { name: &'static str, value: String },
    #[error("{name} must be a socket address, got {value:?}")]
    InvalidSocketAddress { name: &'static str, value: String },
    #[error("listener address {0} is configured more than once")]
    DuplicateListenerAddress(SocketAddr),
    #[error("{name} must be a positive integer within its supported range, got {value:?}")]
    InvalidUnsignedInteger { name: &'static str, value: String },
}
