use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize, de::Error as DeError};
use thiserror::Error;

use crate::ListenerConfiguration;
use rumqttd::{BridgeConfig as CoreBridgeConfig, ConnectionSettings, Transport};

pub const CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFileConfig {
    pub version: u32,
    #[serde(default)]
    pub broker: BrokerLimits,
    #[serde(default)]
    pub listeners: ListenersConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub bridges: Vec<BridgeConfig>,
    #[serde(default)]
    pub static_acl: Option<StaticAclConfig>,
    #[serde(default)]
    pub rules: Vec<RuleConfig>,
    #[serde(default)]
    pub management: ManagementConfig,
    #[serde(default)]
    pub device_transport: DeviceTransportConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLimits {
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_max_payload_size")]
    pub max_payload_size: usize,
    #[serde(default = "default_max_inflight_count")]
    pub max_inflight_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenersConfig {
    #[serde(default)]
    pub tcp: TcpListenerConfig,
    #[serde(default)]
    pub tls: TlsListenerConfig,
    #[serde(default)]
    pub websocket: WebSocketListenerConfig,
    #[serde(default)]
    pub quic: QuicListenerConfig,
    #[serde(default = "default_v311_backend_address")]
    pub v311_backend_address: SocketAddr,
    #[serde(default = "default_v5_backend_address")]
    pub v5_backend_address: SocketAddr,
    #[serde(default = "default_device_backend_address")]
    pub device_backend_address: SocketAddr,
    #[serde(default = "default_device_v5_backend_address")]
    pub device_v5_backend_address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpListenerConfig {
    #[serde(default = "default_plaintext_address")]
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsListenerConfig {
    #[serde(default = "default_tls_address")]
    pub address: SocketAddr,
    #[serde(default = "default_certificate_path")]
    pub certificate_path: PathBuf,
    #[serde(default = "default_key_path")]
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketListenerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_websocket_address")]
    pub address: SocketAddr,
    #[serde(default)]
    pub tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuicListenerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_quic_address")]
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Serialize)]
pub enum StorageConfig {
    Memory,
    Sqlite {
        path: PathBuf,
        prune_interval_ms: u64,
    },
}

impl<'de> Deserialize<'de> for StorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct StorageFile {
            kind: StorageKind,
            #[serde(default)]
            path: Option<PathBuf>,
            #[serde(default = "default_storage_prune_interval_ms")]
            prune_interval_ms: u64,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum StorageKind {
            Memory,
            Sqlite,
        }

        match StorageFile::deserialize(deserializer)? {
            StorageFile {
                kind: StorageKind::Memory,
                path: None,
                ..
            } => Ok(Self::Memory),
            StorageFile {
                kind: StorageKind::Memory,
                path: Some(_),
                ..
            } => Err(D::Error::custom("memory storage does not accept path")),
            StorageFile {
                kind: StorageKind::Sqlite,
                path: Some(path),
                prune_interval_ms,
            } => Ok(Self::Sqlite {
                path,
                prune_interval_ms,
            }),
            StorageFile {
                kind: StorageKind::Sqlite,
                path: None,
                ..
            } => Err(D::Error::custom("sqlite storage requires path")),
        }
    }
}

impl StorageConfig {
    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory)
    }

    pub fn retention_policy(&self) -> rumqttd::RetentionPolicy {
        let mut policy = rumqttd::RetentionPolicy::default();
        if let Self::Sqlite {
            prune_interval_ms, ..
        } = self
        {
            policy.prune_interval_ms = *prune_interval_ms;
        }
        policy
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    pub ca_certificate_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticAclConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_policy_deny_action")]
    pub deny_action: String,
    #[serde(default)]
    pub users: Vec<StaticUser>,
    #[serde(default)]
    pub rules: Vec<AclRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AclRule {
    pub identity: String,
    pub topic: String,
    #[serde(default)]
    pub publish: bool,
    #[serde(default)]
    pub subscribe: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub expression: String,
    #[serde(default)]
    pub source_topic: String,
    #[serde(default)]
    pub target_topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementConfig {
    #[serde(default = "default_management_address")]
    pub address: SocketAddr,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NativeDeviceProtocol {
    Mqtt311,
    Mqtt5,
    #[default]
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceTransportConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub protocol: NativeDeviceProtocol,
}

impl Default for DeviceTransportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            protocol: NativeDeviceProtocol::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    MqttTcp,
    MqttTls,
    Mqtt311NativeDeviceTransport,
    #[serde(rename = "websocket")]
    WebSocket,
    Wss,
    Quic,
    SqlitePersistence,
    Mqtt5TopicAlias,
    Bridge,
    StaticAcl,
    Rules,
    Mqtt5NativeDeviceTransport,
}

impl std::fmt::Display for Capability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MqttTcp => "mqtt TCP",
            Self::MqttTls => "mqtt TLS",
            Self::Mqtt5TopicAlias => "MQTT 5 Topic Alias",
            Self::Mqtt311NativeDeviceTransport => "MQTT 3.1.1 native device transport",
            Self::WebSocket => "websocket",
            Self::Wss => "wss",
            Self::Quic => "quic",
            Self::SqlitePersistence => "sqlite broker persistence",
            Self::Bridge => "mqtt bridge",
            Self::StaticAcl => "static ACL",
            Self::Rules => "rules",
            Self::Mqtt5NativeDeviceTransport => "MQTT 5 native device transport",
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unsupported configuration schema version {0}; expected {CONFIG_SCHEMA_VERSION}")]
    UnsupportedSchemaVersion(u32),
    #[error("invalid value for {field}: {message}")]
    InvalidValue {
        field: &'static str,
        message: &'static str,
    },
    #[error("unsupported capability enabled: {0}")]
    UnsupportedCapability(Capability),
    #[error("configuration file error")]
    Io(#[from] std::io::Error),
    #[error("TOML configuration parse error")]
    Parse(#[from] toml::de::Error),
}

impl Default for BrokerFileConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_SCHEMA_VERSION,
            broker: BrokerLimits::default(),
            listeners: ListenersConfig::default(),
            storage: StorageConfig::Memory,
            bridges: vec![],
            static_acl: None,
            rules: vec![],
            management: ManagementConfig::default(),
            device_transport: DeviceTransportConfig::default(),
        }
    }
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_payload_size: default_max_payload_size(),
            max_inflight_count: default_max_inflight_count(),
        }
    }
}

impl Default for ListenersConfig {
    fn default() -> Self {
        Self {
            tcp: TcpListenerConfig::default(),
            tls: TlsListenerConfig::default(),
            websocket: WebSocketListenerConfig::default(),
            quic: QuicListenerConfig::default(),
            v311_backend_address: default_v311_backend_address(),
            v5_backend_address: default_v5_backend_address(),
            device_backend_address: default_device_backend_address(),
            device_v5_backend_address: default_device_v5_backend_address(),
        }
    }
}

impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            address: default_plaintext_address(),
        }
    }
}

impl Default for TlsListenerConfig {
    fn default() -> Self {
        Self {
            address: default_tls_address(),
            certificate_path: default_certificate_path(),
            key_path: default_key_path(),
        }
    }
}

impl Default for WebSocketListenerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            address: default_websocket_address(),
            tls: false,
        }
    }
}

impl Default for QuicListenerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            address: default_quic_address(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Memory
    }
}

impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            address: default_management_address(),
            username: None,
            password: None,
        }
    }
}

impl BrokerFileConfig {
    pub fn from_toml(source: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(source)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::from_toml(&std::fs::read_to_string(path)?)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::UnsupportedSchemaVersion(self.version));
        }
        if self.broker.max_connections == 0 {
            return Err(ConfigError::InvalidValue {
                field: "broker.max_connections",
                message: "must be greater than zero",
            });
        }
        if self.broker.max_payload_size == 0 {
            return Err(ConfigError::InvalidValue {
                field: "broker.max_payload_size",
                message: "must be greater than zero",
            });
        }
        if self.broker.max_inflight_count == 0 {
            return Err(ConfigError::InvalidValue {
                field: "broker.max_inflight_count",
                message: "must be greater than zero",
            });
        }
        if self.management.username.is_some() != self.management.password.is_some()
            || self
                .management
                .username
                .as_deref()
                .is_some_and(|username| username.trim().is_empty())
            || self
                .management
                .password
                .as_deref()
                .is_some_and(str::is_empty)
        {
            return Err(ConfigError::InvalidValue {
                field: "management.credentials",
                message: "username and password must be non-empty and supplied together",
            });
        }
        if self.listeners.quic.enabled {
            return Err(ConfigError::UnsupportedCapability(Capability::Quic));
        }
        if matches!(
            self.storage,
            StorageConfig::Sqlite {
                prune_interval_ms: 0,
                ..
            }
        ) {
            return Err(ConfigError::InvalidValue {
                field: "storage.prune_interval_ms",
                message: "must be greater than zero",
            });
        }
        let enabled_bridges = self
            .bridges
            .iter()
            .filter(|bridge| bridge.enabled)
            .collect::<Vec<_>>();
        if enabled_bridges.len() > 1 {
            return Err(ConfigError::InvalidValue {
                field: "bridges",
                message: "only one bridge is supported by the controlled broker lifecycle",
            });
        }
        if let Some(bridge) = enabled_bridges.first() {
            if bridge.name.trim().is_empty() || bridge.address.trim().is_empty() {
                return Err(ConfigError::InvalidValue {
                    field: "bridges",
                    message: "enabled bridge requires a name and address",
                });
            }
            if bridge.tls
                && bridge
                    .ca_certificate_path
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().is_empty())
            {
                return Err(ConfigError::InvalidValue {
                    field: "bridges.ca_certificate_path",
                    message: "TLS bridge requires a CA certificate path",
                });
            }
        }
        let static_acl_enabled = self.static_acl.as_ref().is_some_and(|acl| acl.enabled);
        if static_acl_enabled {
            let acl = self.static_acl.as_ref().expect("enabled static ACL exists");
            if acl.users.is_empty() {
                return Err(ConfigError::InvalidValue {
                    field: "static_acl.users",
                    message: "at least one user is required when static ACL is enabled",
                });
            }
            let mut usernames = HashSet::with_capacity(acl.users.len());
            if acl.users.iter().any(|user| {
                user.username.trim().is_empty()
                    || user.password.is_empty()
                    || !usernames.insert(user.username.as_str())
            }) {
                return Err(ConfigError::InvalidValue {
                    field: "static_acl.users",
                    message: "usernames and passwords must not be empty or usernames duplicated",
                });
            }
            if acl.rules.iter().any(|rule| {
                rule.identity.is_empty() || !rumqttd::protocol::valid_filter(&rule.topic)
            }) {
                return Err(ConfigError::InvalidValue {
                    field: "static_acl.rules",
                    message: "rules require an identity and valid MQTT topic filter",
                });
            }
        }
        if self
            .static_acl
            .as_ref()
            .is_some_and(|acl| acl.deny_action != "disconnect")
        {
            return Err(ConfigError::InvalidValue {
                field: "static_acl.deny_action",
                message: "only disconnect is supported",
            });
        }
        if self.rules.iter().any(|rule| {
            rule.enabled
                && (rule.name.trim().is_empty()
                    || !rumqttd::protocol::valid_filter(&rule.source_topic)
                    || !rumqttd::protocol::valid_topic(&rule.target_topic)
                    || rumqttd::protocol::matches(&rule.target_topic, &rule.source_topic))
        }) {
            return Err(ConfigError::InvalidValue {
                field: "rules",
                message: "enabled republish rules require a name, valid source filter, concrete target topic and no self-loop",
            });
        }
        Ok(())
    }

    pub fn to_listener_configuration(&self) -> Result<ListenerConfiguration, ConfigError> {
        self.validate()?;
        let bridge = self
            .bridges
            .iter()
            .find(|bridge| bridge.enabled)
            .map(|bridge| CoreBridgeConfig {
                name: bridge.name.clone(),
                addr: bridge.address.clone(),
                qos: 1,
                sub_path: "#".to_owned(),
                reconnection_delay: 5,
                ping_delay: 30,
                connections: ConnectionSettings {
                    connection_timeout_ms: 60_000,
                    max_payload_size: self.broker.max_payload_size,
                    max_inflight_count: self.broker.max_inflight_count,
                    auth: None,
                    external_auth: None,
                    authorization_handler: None,
                    dynamic_filters: true,
                },
                transport: if bridge.tls {
                    Transport::Tls {
                        ca: bridge
                            .ca_certificate_path
                            .clone()
                            .expect("validated TLS bridge CA path"),
                        client_auth: None,
                    }
                } else {
                    Transport::Tcp
                },
            });
        Ok(ListenerConfiguration {
            plaintext_address: self.listeners.tcp.address,
            tls_address: self.listeners.tls.address,
            v311_backend_address: self.listeners.v311_backend_address,
            v5_backend_address: self.listeners.v5_backend_address,
            tls_cert_path: self.listeners.tls.certificate_path.clone(),
            tls_key_path: self.listeners.tls.key_path.clone(),
            websocket_address: self
                .listeners
                .websocket
                .enabled
                .then_some(self.listeners.websocket.address),
            websocket_tls: self.listeners.websocket.enabled && self.listeners.websocket.tls,
            bridge,
            max_connections: self.broker.max_connections,
            max_payload_size: self.broker.max_payload_size,
            max_inflight_count: self.broker.max_inflight_count,
            auth_handler: None,
            authorization_handler: None,
        })
    }

    pub fn redacted_json(&self) -> serde_json::Value {
        let mut value = serde_json::to_value(self).expect("configuration is serializable");
        redact_secrets(&mut value);
        value
    }

    pub fn supported_capabilities() -> Vec<Capability> {
        vec![
            Capability::MqttTcp,
            Capability::MqttTls,
            Capability::Mqtt5TopicAlias,
            Capability::SqlitePersistence,
            Capability::Mqtt311NativeDeviceTransport,
            Capability::Mqtt5NativeDeviceTransport,
        ]
    }
}

fn redact_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, child) in object.iter_mut() {
                if key.contains("password") || key.contains("secret") {
                    *child = serde_json::Value::String("[REDACTED]".into());
                } else {
                    redact_secrets(child);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(redact_secrets),
        _ => {}
    }
}

fn default_max_connections() -> usize {
    10_000
}
fn default_storage_prune_interval_ms() -> u64 {
    60_000
}
fn default_max_payload_size() -> usize {
    2 * 1024 * 1024
}
fn default_max_inflight_count() -> usize {
    100
}
fn default_plaintext_address() -> SocketAddr {
    "0.0.0.0:1883".parse().expect("valid default")
}
fn default_tls_address() -> SocketAddr {
    "0.0.0.0:8883".parse().expect("valid default")
}
fn default_websocket_address() -> SocketAddr {
    "0.0.0.0:8083".parse().expect("valid default")
}
fn default_quic_address() -> SocketAddr {
    "0.0.0.0:14567".parse().expect("valid default")
}
fn default_management_address() -> SocketAddr {
    "127.0.0.1:8082".parse().expect("valid default")
}
fn default_v311_backend_address() -> SocketAddr {
    "127.0.0.1:18831".parse().expect("valid default")
}
fn default_v5_backend_address() -> SocketAddr {
    "127.0.0.1:18832".parse().expect("valid default")
}
fn default_device_backend_address() -> SocketAddr {
    "127.0.0.1:18833".parse().expect("valid default")
}
fn default_device_v5_backend_address() -> SocketAddr {
    "127.0.0.1:18834".parse().expect("valid default")
}
fn default_certificate_path() -> PathBuf {
    PathBuf::from("server.crt")
}
fn default_key_path() -> PathBuf {
    PathBuf::from("server.key")
}
fn default_policy_deny_action() -> String {
    "disconnect".into()
}
