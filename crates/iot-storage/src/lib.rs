#![forbid(unsafe_code)]

use std::{fs, future::Future, path::PathBuf, pin::Pin, time::Duration};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, NaiveDateTime, TimeZone, Timelike, Utc};
use iot_core::{
    DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent, device_token_prefix,
    verify_device_token,
};
use sqlx::{
    Executor, PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction,
    error::DatabaseError,
    postgres::{PgPoolOptions, PgRow},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
    types::Json,
};
use thiserror::Error;

const PLATFORM_POSTGRES_SCHEMA: &str = include_str!("../migrations/0001_platform.sql");

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    display_name TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT,
    asset_id TEXT,
    device_profile_id TEXT,
    deleted_at TEXT,
    is_gateway INTEGER NOT NULL DEFAULT 0,
    gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    claimed_at TEXT,
    CHECK (
        (is_gateway = 1 AND gateway_device_id IS NULL)
        OR (is_gateway = 0 AND gateway_device_id IS NOT device_id)
    )
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    boot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    UNIQUE (event_at, device_id, boot_id, sequence)
);
CREATE INDEX IF NOT EXISTS telemetry_device_event_at_index
    ON telemetry (device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_gateway_device_event_at_index
    ON telemetry (gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (gateway_device_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS telemetry_rollups_5m (
    bucket_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_at, device_id)
);
CREATE TABLE IF NOT EXISTS telemetry_rollups_1h (
    bucket_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_at, device_id)
);

CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, app_key)
);
CREATE TABLE IF NOT EXISTS applications (
    app_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('frontend', 'full_stack')),
    launch_url TEXT NOT NULL,
    client_id TEXT NOT NULL UNIQUE,
    allowed_scopes_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1))
);
CREATE TABLE IF NOT EXISTS application_redirect_uris (
    app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
    redirect_uri TEXT NOT NULL,
    PRIMARY KEY (app_id, redirect_uri)
);
CREATE INDEX IF NOT EXISTS application_redirect_uris_lookup_index
    ON application_redirect_uris (app_id, redirect_uri);

CREATE TABLE IF NOT EXISTS asset_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    fields TEXT NOT NULL DEFAULT '{}',
    dashboard_defaults TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    telemetry_schema TEXT NOT NULL DEFAULT '{}',
    metric_mapping TEXT NOT NULL DEFAULT '{}',
    reporting_settings TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    asset_profile_id TEXT REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id TEXT REFERENCES assets(id) ON DELETE SET NULL,
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (parent_asset_id, name)
);
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
    ON devices (owner_user_id)
    WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
    ON assets (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resource_shares (
    id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
    ),
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    responded_at TEXT,
    expires_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
    ON resource_shares (resource_type, resource_id, target_user_id)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
    ON resource_shares (target_user_id, state, created_at DESC);
CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
    ON resource_shares (resource_type, resource_id, state);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    actor_account_class TEXT NOT NULL
        CHECK (actor_account_class IN ('system', 'admin', 'user')),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_value TEXT,
    after_value TEXT,
    request_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS audit_events_resource_index
    ON audit_events (resource_type, resource_id, created_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_actor_index
    ON audit_events (actor_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS device_claim_codes (
    device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    issued_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_at TEXT
);
CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index
    ON device_claim_codes (expires_at)
    WHERE used_at IS NULL;

CREATE TABLE IF NOT EXISTS device_tokens (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    token_prefix TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL,
    token_ciphertext TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_one_active_per_device
    ON device_tokens (device_id)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_tokens_active_prefix_index
    ON device_tokens (token_prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    device_id TEXT,
    metric_key TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    comparison TEXT NOT NULL,
    threshold REAL NOT NULL,
    window_seconds INTEGER,
    for_seconds INTEGER NOT NULL DEFAULT 300,
    resolve_after_seconds INTEGER NOT NULL DEFAULT 300,
    reopen_grace_seconds INTEGER NOT NULL DEFAULT 3600,
    hysteresis REAL,
    severity TEXT NOT NULL DEFAULT 'warning',
    reminder_interval_seconds INTEGER NOT NULL DEFAULT 86400,
    archived_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (rule_type, device_id)
    WHERE enabled = 1;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_at TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    PRIMARY KEY (rule_id, event_at, device_id, boot_id, sequence)
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    condition_started_at TEXT NOT NULL,
    recovery_started_at TEXT,
    opened_at TEXT,
    resolved_at TEXT,
    acknowledged_at TEXT,
    acknowledged_by TEXT,
    last_value REAL,
    last_notified_at TEXT,
    last_reminder_at TEXT,
    state_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (rule_id, device_id)
    WHERE status IN ('pending', 'open');

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES alert_incidents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    method TEXT NOT NULL CHECK (trim(method) <> ''),
    params TEXT NOT NULL DEFAULT '{}',
    mode TEXT NOT NULL DEFAULT 'one_way'
        CHECK (mode IN ('one_way', 'two_way')),
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
    expires_at TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    published_at TEXT,
    response TEXT,
    responded_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (state, next_attempt_at)
    WHERE state = 'queued';
"#;

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    path: PathBuf,
}

#[derive(Clone)]
pub enum PlatformStore {
    Sqlite(SqliteStore),
    Timescale(PgPool),
}

#[derive(Debug, Error)]
pub enum PlatformStoreError {
    #[error("platform storage configuration is incomplete")]
    InvalidConfiguration,
    #[error(transparent)]
    Sqlite(#[from] SqliteStoreError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("command ID must be a UUID, got {0:?}")]
    InvalidCommandId(String),
    #[error("command params must be valid JSON")]
    InvalidCommandParams,
    #[error("command payload conflicts with existing command ID: {0:?}")]
    CommandConflict(String),
    #[error("notification ID is not a UUID: {0:?}")]
    InvalidNotificationId(String),
    #[error("invalid notification outbox kind: {0:?}")]
    InvalidNotificationKind(String),
    #[error("invalid notification outbox state: {0:?}")]
    InvalidNotificationState(String),
    #[error("invalid notification outbox {column} timestamp: {value}")]
    InvalidNotificationTimestamp {
        column: &'static str,
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("device is not registered: {0:?}")]
    UnknownDevice(String),
    #[error("device token authentication denied")]
    DeviceTokenDenied,
    #[error("telemetry sequence does not fit PostgreSQL BIGINT")]
    TelemetrySequenceOverflow,
    #[error("filesystem backups are available only for SQLite platform storage")]
    BackupUnsupported,
    #[error("invalid application ID: {0:?}")]
    InvalidApplicationId(String),
    #[error("invalid application kind: {0:?}")]
    InvalidApplicationKind(String),
    #[error("application launch URL must not be empty")]
    EmptyApplicationLaunchUrl,
    #[error("application client ID must not be empty")]
    EmptyApplicationClientId,
    #[error("application redirect URI must not be empty")]
    EmptyApplicationRedirectUri,
    #[error("application redirect URI is duplicated: {0:?}")]
    DuplicateApplicationRedirectUri(String),
    #[error("application scope must not be empty")]
    EmptyApplicationScope,
    #[error("application scopes are invalid")]
    InvalidApplicationScopes,
    #[error("application is disabled: {0}")]
    ApplicationDisabled(ApplicationId),
    #[error("application client ID is already registered: {0:?}")]
    ApplicationClientIdConflict(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ApplicationId(String);

impl ApplicationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ApplicationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for ApplicationId {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.len() > 64 {
            return Err(PlatformStoreError::InvalidApplicationId(value.to_owned()));
        }
        let valid = value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase()
            } else {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            }
        });
        if !valid {
            return Err(PlatformStoreError::InvalidApplicationId(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationKind {
    Frontend,
    FullStack,
}

impl ApplicationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::FullStack => "full_stack",
        }
    }

    fn parse(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "frontend" => Ok(Self::Frontend),
            "full_stack" => Ok(Self::FullStack),
            _ => Err(PlatformStoreError::InvalidApplicationKind(value.to_owned())),
        }
    }
}

impl std::str::FromStr for ApplicationKind {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(String);

impl ClientId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for ClientId {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(PlatformStoreError::EmptyApplicationClientId);
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RedirectUri(String);

impl RedirectUri {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for RedirectUri {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(PlatformStoreError::EmptyApplicationRedirectUri);
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewApplication {
    pub app_id: ApplicationId,
    pub kind: ApplicationKind,
    pub launch_url: String,
    pub client_id: ClientId,
    pub redirect_uris: Vec<RedirectUri>,
    pub allowed_scopes: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationRecord {
    pub app_id: ApplicationId,
    pub kind: ApplicationKind,
    pub launch_url: String,
    pub client_id: ClientId,
    pub redirect_uris: Vec<RedirectUri>,
    pub allowed_scopes: Vec<String>,
    pub enabled: bool,
}

pub trait ApplicationRepository: Send + Sync {
    fn upsert_application<'a>(
        &'a self,
        application: NewApplication,
    ) -> Pin<Box<dyn Future<Output = Result<ApplicationRecord, PlatformStoreError>> + Send + 'a>>;
    fn find_application_by_app_id<'a>(
        &'a self,
        app_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn find_application_by_client_id<'a>(
        &'a self,
        client_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    >;
}

pub trait TopologyRepository: Send + Sync {
    fn register_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedDeviceToken {
    pub token_id: uuid::Uuid,
    pub device_id: String,
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
}

pub trait IdentityRepository: Send + Sync {
    fn resolve_active_device_token<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<AuthenticatedDeviceToken, PlatformStoreError>> + Send + 'a>,
    >;
}

pub trait DeviceAuthorizationRepository: Send + Sync {
    fn authorize_device_session<'a>(
        &'a self,
        token_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn authorize_gateway_token<'a>(
        &'a self,
        token_id: uuid::Uuid,
        gateway_device_id: &'a str,
        child_device_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
}

pub trait CommandRepository: Send + Sync {
    fn enqueue_command<'a>(
        &'a self,
        command: NewCommandOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<CommandOutboxRecord, PlatformStoreError>> + Send + 'a>>;
}

pub trait NotificationRepository: Send + Sync {
    fn claim_notifications<'a>(
        &'a self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn mark_notification_sent<'a>(
        &'a self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn release_notification_for_retry<'a>(
        &'a self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
}

pub trait CommandLifecycleRepository: Send + Sync {
    fn claim_commands<'a>(
        &'a self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn mark_command_published<'a>(
        &'a self,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn mark_command_failed<'a>(
        &'a self,
        command_id: uuid::Uuid,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn release_command_for_retry<'a>(
        &'a self,
        command_id: uuid::Uuid,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn expire_commands<'a>(
        &'a self,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn mark_command_responded<'a>(
        &'a self,
        command_id: uuid::Uuid,
        device_id: &'a str,
        token_id: uuid::Uuid,
        response: &'a str,
        responded_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
}

pub trait TelemetryRepository: Send + Sync {
    fn write_telemetry<'a>(
        &'a self,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountClass {
    System,
    Admin,
    User,
}

impl AccountClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Admin => "admin",
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourcePermission {
    Viewer,
    Controller,
    Manager,
    Owner,
}

impl ResourcePermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Controller => "controller",
            Self::Manager => "manager",
            Self::Owner => "owner",
        }
    }

    pub const fn allows(self, required: Self) -> bool {
        self as u8 >= required as u8
    }

    fn parse_share(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "controller" => Some(Self::Controller),
            "manager" => Some(Self::Manager),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Asset,
    Device,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asset => "asset",
            Self::Device => "device",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationSubject {
    pub user_id: uuid::Uuid,
    pub account_class: AccountClass,
}

pub trait AuthorizationRepository: Send + Sync {
    fn device_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn asset_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
}

impl PlatformStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, PlatformStoreError> {
        match configuration.storage {
            DatabaseStorage::Sqlite => Ok(Self::Sqlite(SqliteStore::open(configuration).await?)),
            DatabaseStorage::Timescale => {
                let database_url = configuration
                    .database_url
                    .as_deref()
                    .ok_or(PlatformStoreError::InvalidConfiguration)?;
                let migration_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(database_url)
                    .await?;
                migrate_platform_timescale(&migration_pool).await?;
                migration_pool.close().await;

                let pool = PgPoolOptions::new()
                    .max_connections(8)
                    .after_connect(|connection, _| {
                        Box::pin(async move {
                            connection
                                .execute("SET search_path TO iot_nano, public")
                                .await?;
                            Ok(())
                        })
                    })
                    .connect(database_url)
                    .await?;
                Ok(Self::Timescale(pool))
            }
        }
    }

    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match self {
            Self::Sqlite(store) => Some(store.pool()),
            Self::Timescale(_) => None,
        }
    }

    pub fn timescale_pool(&self) -> Option<&PgPool> {
        match self {
            Self::Sqlite(_) => None,
            Self::Timescale(pool) => Some(pool),
        }
    }

    pub async fn upsert_application(
        &self,
        mut application: NewApplication,
    ) -> Result<ApplicationRecord, PlatformStoreError> {
        validate_application(&mut application)?;
        match self {
            Self::Sqlite(store) => {
                let conflicting_app_id = sqlx::query_scalar::<_, String>(
                    "SELECT app_id FROM applications
                     WHERE client_id = ? AND app_id <> ?",
                )
                .bind(application.client_id.as_str())
                .bind(application.app_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                if conflicting_app_id.is_some() {
                    return Err(PlatformStoreError::ApplicationClientIdConflict(
                        application.client_id.as_str().to_owned(),
                    ));
                }
            }
            Self::Timescale(pool) => {
                let conflicting_app_id = sqlx::query_scalar::<_, String>(
                    "SELECT app_id FROM applications
                     WHERE client_id = $1 AND app_id <> $2",
                )
                .bind(application.client_id.as_str())
                .bind(application.app_id.as_str())
                .fetch_optional(pool)
                .await?;
                if conflicting_app_id.is_some() {
                    return Err(PlatformStoreError::ApplicationClientIdConflict(
                        application.client_id.as_str().to_owned(),
                    ));
                }
            }
        }
        let scopes = serde_json::to_string(&application.allowed_scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                sqlx::query(
                    "INSERT INTO applications (
                        app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = excluded.kind,
                        launch_url = excluded.launch_url,
                        client_id = excluded.client_id,
                        allowed_scopes_json = excluded.allowed_scopes_json,
                        enabled = excluded.enabled",
                )
                .bind(application.app_id.as_str())
                .bind(application.kind.as_str())
                .bind(&application.launch_url)
                .bind(application.client_id.as_str())
                .bind(&scopes)
                .bind(application.enabled)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    map_application_client_id_conflict(error, application.client_id.as_str())
                })?;
                sqlx::query("DELETE FROM application_redirect_uris WHERE app_id = ?")
                    .bind(application.app_id.as_str())
                    .execute(&mut *transaction)
                    .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, redirect_uri)
                         VALUES (?, ?)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(redirect_uri.as_str())
                    .execute(&mut *transaction)
                    .await?;
                }
                transaction.commit().await?;
                self.find_application_by_app_id(application.app_id.as_str())
                    .await?
                    .ok_or_else(|| {
                        PlatformStoreError::InvalidApplicationId(
                            application.app_id.as_str().to_owned(),
                        )
                    })
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                sqlx::query(
                    "INSERT INTO applications (
                        app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        launch_url = EXCLUDED.launch_url,
                        client_id = EXCLUDED.client_id,
                        allowed_scopes_json = EXCLUDED.allowed_scopes_json,
                        enabled = EXCLUDED.enabled",
                )
                .bind(application.app_id.as_str())
                .bind(application.kind.as_str())
                .bind(&application.launch_url)
                .bind(application.client_id.as_str())
                .bind(&scopes)
                .bind(application.enabled)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    map_application_client_id_conflict(error, application.client_id.as_str())
                })?;
                sqlx::query("DELETE FROM application_redirect_uris WHERE app_id = $1")
                    .bind(application.app_id.as_str())
                    .execute(&mut *transaction)
                    .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, redirect_uri)
                         VALUES ($1, $2)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(redirect_uri.as_str())
                    .execute(&mut *transaction)
                    .await?;
                }
                transaction.commit().await?;
                self.find_application_by_app_id(application.app_id.as_str())
                    .await?
                    .ok_or_else(|| {
                        PlatformStoreError::InvalidApplicationId(
                            application.app_id.as_str().to_owned(),
                        )
                    })
            }
        }
    }

    pub async fn find_application_by_app_id(
        &self,
        app_id: &str,
    ) -> Result<Option<ApplicationRecord>, PlatformStoreError> {
        let app_id = app_id.parse::<ApplicationId>()?;
        match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = ?",
                )
                .bind(app_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects).map(Some)
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = $1",
                )
                .bind(app_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .fetch_all(pool)
                .await?;
                postgres_application_record(row, redirects).map(Some)
            }
        }
    }

    pub async fn find_application_by_client_id(
        &self,
        client_id: &str,
    ) -> Result<Option<ApplicationRecord>, PlatformStoreError> {
        let client_id = client_id.parse::<ClientId>()?;
        let application = match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = ?",
                )
                .bind(client_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects)?
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = $1",
                )
                .bind(client_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .fetch_all(pool)
                .await?;
                postgres_application_record(row, redirects)?
            }
        };
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        Ok(Some(application))
    }

    pub async fn register_device(&self, device_id: &str) -> Result<(), PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                sqlx::query("INSERT INTO devices (device_id) VALUES (?) ON CONFLICT DO NOTHING")
                    .bind(device_id)
                    .execute(store.pool())
                    .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query("INSERT INTO devices (device_id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(device_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn resolve_active_device_token(
        &self,
        token: &str,
    ) -> Result<AuthenticatedDeviceToken, PlatformStoreError> {
        let token_prefix =
            device_token_prefix(token).map_err(|_| PlatformStoreError::DeviceTokenDenied)?;
        let last_used_at = Utc::now();
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let row = sqlx::query(
                    "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                            devices.is_gateway, devices.gateway_device_id
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     WHERE device_tokens.token_prefix = ?
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL",
                )
                .bind(token_prefix)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(PlatformStoreError::DeviceTokenDenied)?;

                let token_hash = row.try_get::<String, _>("token_hash")?;
                if !verify_device_token(token, &token_hash).unwrap_or(false) {
                    return Err(PlatformStoreError::DeviceTokenDenied);
                }
                let authenticated = AuthenticatedDeviceToken {
                    token_id: uuid::Uuid::parse_str(&row.try_get::<String, _>("id")?)
                        .map_err(|_| PlatformStoreError::DeviceTokenDenied)?,
                    device_id: row.try_get("device_id")?,
                    is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
                    gateway_device_id: row.try_get("gateway_device_id")?,
                };
                let updated = sqlx::query(
                    "UPDATE device_tokens
                     SET last_used_at = ?
                     WHERE id = ?
                       AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1
                           FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(last_used_at.to_rfc3339())
                .bind(authenticated.token_id.to_string())
                .execute(&mut *transaction)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(PlatformStoreError::DeviceTokenDenied);
                }
                transaction.commit().await?;
                Ok(authenticated)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let row = sqlx::query(
                    "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                            devices.is_gateway, devices.gateway_device_id
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     WHERE device_tokens.token_prefix = $1
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL
                     FOR UPDATE OF device_tokens, devices",
                )
                .bind(token_prefix)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(PlatformStoreError::DeviceTokenDenied)?;

                let token_hash = row.try_get::<String, _>("token_hash")?;
                if !verify_device_token(token, &token_hash).unwrap_or(false) {
                    return Err(PlatformStoreError::DeviceTokenDenied);
                }
                let authenticated = AuthenticatedDeviceToken {
                    token_id: row.try_get("id")?,
                    device_id: row.try_get("device_id")?,
                    is_gateway: row.try_get("is_gateway")?,
                    gateway_device_id: row.try_get("gateway_device_id")?,
                };
                let updated = sqlx::query(
                    "UPDATE device_tokens
                     SET last_used_at = $1
                       WHERE id = $2
                       AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1
                           FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(last_used_at)
                .bind(authenticated.token_id)
                .execute(&mut *transaction)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(PlatformStoreError::DeviceTokenDenied);
                }
                transaction.commit().await?;
                Ok(authenticated)
            }
        }
    }

    pub async fn authorize_device_session(
        &self,
        token_id: uuid::Uuid,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        let authorized = match self {
            Self::Sqlite(store) => sqlx::query_scalar::<_, i64>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     WHERE device_tokens.id = ?
                       AND device_tokens.device_id = ?
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL
                       AND devices.gateway_device_id IS NULL",
            )
            .bind(token_id.to_string())
            .bind(device_id)
            .fetch_optional(store.pool())
            .await?
            .is_some(),
            Self::Timescale(pool) => sqlx::query_scalar::<_, i32>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     WHERE device_tokens.id = $1
                       AND device_tokens.device_id = $2
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL
                       AND devices.gateway_device_id IS NULL",
            )
            .bind(token_id)
            .bind(device_id)
            .fetch_optional(pool)
            .await?
            .is_some(),
        };
        if authorized {
            Ok(())
        } else {
            Err(PlatformStoreError::DeviceTokenDenied)
        }
    }

    pub async fn authorize_gateway_token(
        &self,
        token_id: uuid::Uuid,
        gateway_device_id: &str,
        child_device_id: Option<&str>,
    ) -> Result<(), PlatformStoreError> {
        let authorized = match self {
            Self::Sqlite(store) => sqlx::query_scalar::<_, i64>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices AS gateways
                       ON gateways.device_id = device_tokens.device_id
                     WHERE device_tokens.id = ?
                       AND device_tokens.device_id = ?
                       AND device_tokens.revoked_at IS NULL
                       AND gateways.deleted_at IS NULL
                       AND gateways.is_gateway = 1
                       AND (
                           ? IS NULL
                           OR EXISTS (
                               SELECT 1
                               FROM devices AS children
                               WHERE children.device_id = ?
                                 AND children.gateway_device_id = gateways.device_id
                                 AND children.deleted_at IS NULL
                           )
                       )",
            )
            .bind(token_id.to_string())
            .bind(gateway_device_id)
            .bind(child_device_id)
            .bind(child_device_id)
            .fetch_optional(store.pool())
            .await?
            .is_some(),
            Self::Timescale(pool) => sqlx::query_scalar::<_, i32>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices AS gateways
                       ON gateways.device_id = device_tokens.device_id
                     WHERE device_tokens.id = $1
                       AND device_tokens.device_id = $2
                       AND device_tokens.revoked_at IS NULL
                       AND gateways.deleted_at IS NULL
                       AND gateways.is_gateway = TRUE
                       AND (
                           $3 IS NULL
                           OR EXISTS (
                               SELECT 1
                               FROM devices AS children
                               WHERE children.device_id = $3
                                 AND children.gateway_device_id = gateways.device_id
                                 AND children.deleted_at IS NULL
                           )
                       )",
            )
            .bind(token_id)
            .bind(gateway_device_id)
            .bind(child_device_id)
            .fetch_optional(pool)
            .await?
            .is_some(),
        };
        if authorized {
            Ok(())
        } else {
            Err(PlatformStoreError::DeviceTokenDenied)
        }
    }

    pub async fn device_permission(
        &self,
        subject: &AuthorizationSubject,
        device_id: &str,
    ) -> Result<Option<ResourcePermission>, PlatformStoreError> {
        if subject.account_class == AccountClass::Admin {
            return Ok(Some(ResourcePermission::Owner));
        }

        match self {
            Self::Sqlite(store) => {
                let exists = sqlx::query_scalar::<_, String>(
                    "SELECT device_id FROM devices
                     WHERE device_id = ? AND deleted_at IS NULL",
                )
                .bind(device_id)
                .fetch_optional(store.pool())
                .await?;
                if exists.is_none() {
                    return Ok(None);
                }
                let owner = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT owner_user_id FROM devices
                     WHERE device_id = ? AND deleted_at IS NULL",
                )
                .bind(device_id)
                .fetch_optional(store.pool())
                .await?
                .flatten();
                if owner.as_deref() == Some(&subject.user_id.to_string()) {
                    return Ok(Some(ResourcePermission::Owner));
                }
                let rows = sqlx::query_scalar::<_, String>(
                    "WITH RECURSIVE ancestors(id, depth) AS (
                        SELECT asset_id, 0 FROM devices
                        WHERE device_id = ? AND deleted_at IS NULL AND asset_id IS NOT NULL
                        UNION ALL
                        SELECT assets.parent_asset_id, ancestors.depth + 1
                        FROM ancestors JOIN assets ON assets.id = ancestors.id
                        WHERE assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     )
                     SELECT permission FROM resource_shares
                     WHERE resource_type = 'device' AND resource_id = ?
                       AND target_user_id = ? AND state = 'active'
                     UNION ALL
                     SELECT shares.permission FROM resource_shares AS shares
                     JOIN ancestors ON shares.resource_id = ancestors.id
                     WHERE shares.resource_type = 'asset' AND shares.target_user_id = ?
                       AND shares.state = 'active' AND shares.inherit_children = 1",
                )
                .bind(device_id)
                .bind(device_id)
                .bind(subject.user_id.to_string())
                .bind(subject.user_id.to_string())
                .fetch_all(store.pool())
                .await?;
                Ok(strongest_share_permission(rows))
            }
            Self::Timescale(pool) => {
                let exists = sqlx::query_scalar::<_, String>(
                    "SELECT device_id FROM devices
                     WHERE device_id = $1 AND deleted_at IS NULL",
                )
                .bind(device_id)
                .fetch_optional(pool)
                .await?;
                if exists.is_none() {
                    return Ok(None);
                }
                let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
                    "SELECT owner_user_id FROM devices
                     WHERE device_id = $1 AND deleted_at IS NULL",
                )
                .bind(device_id)
                .fetch_optional(pool)
                .await?
                .flatten();
                if owner == Some(subject.user_id) {
                    return Ok(Some(ResourcePermission::Owner));
                }
                let rows = sqlx::query_scalar::<_, String>(
                    "WITH RECURSIVE ancestors(id, depth) AS (
                        SELECT asset_id, 0 FROM devices
                        WHERE device_id = $1 AND deleted_at IS NULL AND asset_id IS NOT NULL
                        UNION ALL
                        SELECT assets.parent_asset_id, ancestors.depth + 1
                        FROM ancestors JOIN assets ON assets.id = ancestors.id
                        WHERE assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     )
                     SELECT permission FROM resource_shares
                     WHERE resource_type = 'device' AND resource_id = $1
                       AND target_user_id = $2 AND state = 'active'
                     UNION ALL
                     SELECT shares.permission FROM resource_shares AS shares
                     JOIN ancestors ON shares.resource_id = ancestors.id::text
                     WHERE shares.resource_type = 'asset' AND shares.target_user_id = $2
                       AND shares.state = 'active' AND shares.inherit_children = TRUE",
                )
                .bind(device_id)
                .bind(subject.user_id)
                .fetch_all(pool)
                .await?;
                Ok(strongest_share_permission(rows))
            }
        }
    }

    pub async fn asset_permission(
        &self,
        subject: &AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Result<Option<ResourcePermission>, PlatformStoreError> {
        if subject.account_class == AccountClass::Admin {
            return Ok(Some(ResourcePermission::Owner));
        }

        match self {
            Self::Sqlite(store) => {
                let asset_id = asset_id.to_string();
                let owner = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT owner_user_id FROM assets WHERE id = ?",
                )
                .bind(&asset_id)
                .fetch_optional(store.pool())
                .await?
                .flatten();
                if owner.as_deref() == Some(&subject.user_id.to_string()) {
                    return Ok(Some(ResourcePermission::Owner));
                }
                let rows = sqlx::query_scalar::<_, String>(
                    "WITH RECURSIVE ancestors(id, depth) AS (
                        SELECT ?, 0
                        UNION ALL
                        SELECT assets.parent_asset_id, ancestors.depth + 1
                        FROM ancestors JOIN assets ON assets.id = ancestors.id
                        WHERE assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     )
                     SELECT permission FROM resource_shares
                     WHERE resource_type = 'asset' AND resource_id = ?
                       AND target_user_id = ? AND state = 'active'
                     UNION ALL
                     SELECT shares.permission FROM resource_shares AS shares
                     JOIN ancestors ON shares.resource_id = ancestors.id
                     WHERE shares.resource_type = 'asset' AND shares.target_user_id = ?
                       AND shares.state = 'active' AND shares.inherit_children = 1",
                )
                .bind(&asset_id)
                .bind(&asset_id)
                .bind(subject.user_id.to_string())
                .bind(subject.user_id.to_string())
                .fetch_all(store.pool())
                .await?;
                Ok(strongest_share_permission(rows))
            }
            Self::Timescale(pool) => {
                let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
                    "SELECT owner_user_id FROM assets WHERE id = $1",
                )
                .bind(asset_id)
                .fetch_optional(pool)
                .await?
                .flatten();
                if owner == Some(subject.user_id) {
                    return Ok(Some(ResourcePermission::Owner));
                }
                let rows = sqlx::query_scalar::<_, String>(
                    "WITH RECURSIVE ancestors(id, depth) AS (
                        SELECT $1::uuid, 0
                        UNION ALL
                        SELECT assets.parent_asset_id, ancestors.depth + 1
                        FROM ancestors JOIN assets ON assets.id = ancestors.id
                        WHERE assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     )
                     SELECT permission FROM resource_shares
                     WHERE resource_type = 'asset' AND resource_id = $1::text
                       AND target_user_id = $2 AND state = 'active'
                     UNION ALL
                     SELECT shares.permission FROM resource_shares AS shares
                     JOIN ancestors ON shares.resource_id = ancestors.id::text
                     WHERE shares.resource_type = 'asset' AND shares.target_user_id = $2
                       AND shares.state = 'active' AND shares.inherit_children = TRUE",
                )
                .bind(asset_id)
                .bind(subject.user_id)
                .fetch_all(pool)
                .await?;
                Ok(strongest_share_permission(rows))
            }
        }
    }

    pub async fn enqueue_command(
        &self,
        mut command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, PlatformStoreError> {
        let id = uuid::Uuid::parse_str(&command.id)
            .map_err(|_| PlatformStoreError::InvalidCommandId(command.id.clone()))?;
        let params = serde_json::from_str::<serde_json::Value>(&command.params)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.id = id.to_string();
        command.params =
            serde_json::to_string(&params).map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.expires_at = canonical_command_timestamp(command.expires_at);
        command.next_attempt_at = canonical_command_timestamp(command.next_attempt_at);

        match self {
            Self::Sqlite(store) => {
                self.require_sqlite_registered_device(&command.device_id)
                    .await?;
                enqueue_sqlite_platform_command(store, &command).await
            }
            Self::Timescale(pool) => {
                enqueue_timescale_platform_command(pool, id, params, &command).await
            }
        }
    }

    pub async fn claim_commands(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store.claim_commands(now, lease_until, limit).await?),
            Self::Timescale(pool) => claim_timescale_commands(pool, now, lease_until, limit).await,
        }
    }

    pub async fn mark_command_published(
        &self,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_published(&command_id.to_string(), published_at)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_command_published(pool, command_id, published_at).await
            }
        }
    }

    pub async fn mark_command_failed(
        &self,
        command_id: uuid::Uuid,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_failed(&command_id.to_string(), error)
                .await?),
            Self::Timescale(pool) => mark_timescale_command_failed(pool, command_id, error).await,
        }
    }

    pub async fn release_command_for_retry(
        &self,
        command_id: uuid::Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_command_for_retry(&command_id.to_string(), error, next_attempt_at)
                .await?),
            Self::Timescale(pool) => {
                release_timescale_command_for_retry(pool, command_id, error, next_attempt_at).await
            }
        }
    }

    pub async fn expire_commands(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store.expire_commands(now).await?),
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "UPDATE command_outbox
                     SET state = 'expired', lease_until = NULL
                     WHERE (
                            state IN ('queued', 'leased')
                            OR (state = 'published_to_broker' AND mode = 'two_way')
                           )
                       AND expires_at <= $1
                     RETURNING
                        id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                        lease_until, attempt_count, last_error, published_at, response, responded_at",
                )
                .bind(now)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(postgres_command_outbox_record)
                    .collect()
            }
        }
    }

    pub async fn mark_command_responded(
        &self,
        command_id: uuid::Uuid,
        device_id: &str,
        token_id: uuid::Uuid,
        response: &str,
        responded_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        let response_value = serde_json::from_str::<serde_json::Value>(response)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        let response = serde_json::to_string(&response_value)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_responded(
                    &command_id.to_string(),
                    device_id,
                    &token_id.to_string(),
                    &response,
                    responded_at,
                )
                .await?),
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "UPDATE command_outbox AS command
                     SET state = CASE
                            WHEN command.state = 'responded' THEN command.state
                            ELSE 'responded'
                         END,
                         response = CASE
                            WHEN command.state = 'responded' THEN command.response
                            ELSE $1::jsonb
                         END,
                         responded_at = CASE
                            WHEN command.state = 'responded' THEN command.responded_at
                            ELSE $2
                         END,
                         lease_until = NULL
                     WHERE command.id = $3
                       AND command.device_id = $4
                       AND command.mode = 'two_way'
                       AND (
                            (command.state = 'published_to_broker' AND command.expires_at > $2)
                            OR (command.state = 'responded' AND command.response = $1::jsonb)
                           )
                       AND EXISTS (
                            SELECT 1
                            FROM device_tokens
                            WHERE id = $5
                              AND device_id = command.device_id
                              AND revoked_at IS NULL
                       )
                     RETURNING
                        id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                        lease_until, attempt_count, last_error, published_at, response, responded_at",
                )
                .bind(Json(response_value))
                .bind(responded_at)
                .bind(command_id)
                .bind(device_id)
                .bind(token_id)
                .fetch_optional(pool)
                .await?;
                row.map(postgres_command_outbox_record).transpose()
            }
        }
    }

    pub async fn claim_notifications(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store.claim_notifications(now, lease_until, limit).await?),
            Self::Timescale(pool) => {
                claim_timescale_notifications(pool, now, lease_until, limit).await
            }
        }
    }

    pub async fn mark_notification_sent(
        &self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_notification_sent(&notification_id.to_string(), expected_lease_until, sent_at)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_notification_sent(
                    pool,
                    notification_id,
                    expected_lease_until,
                    sent_at,
                )
                .await
            }
        }
    }

    pub async fn release_notification_for_retry(
        &self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_notification_for_retry(
                    &notification_id.to_string(),
                    expected_lease_until,
                    error,
                    next_attempt_at,
                )
                .await?),
            Self::Timescale(pool) => {
                release_timescale_notification_for_retry(
                    pool,
                    notification_id,
                    expected_lease_until,
                    error,
                    next_attempt_at,
                )
                .await
            }
        }
    }

    pub async fn write_telemetry(
        &self,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, PlatformStoreError> {
        let sequence = i64::try_from(event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;

        match self {
            Self::Sqlite(store) => {
                self.require_sqlite_registered_device(&event.device_id)
                    .await?;
                Ok(store.write_telemetry(event, received_at, topic).await?)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                if !timescale_device_is_locked(&mut transaction, &event.device_id).await? {
                    return Err(PlatformStoreError::UnknownDevice(event.device_id.clone()));
                }
                sqlx::query(
                    "INSERT INTO device_runtime_state (device_id, last_seen_at)
                     VALUES ($1, $2)
                     ON CONFLICT (device_id)
                     DO UPDATE SET last_seen_at = GREATEST(
                         COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                         EXCLUDED.last_seen_at
                     )",
                )
                .bind(&event.device_id)
                .bind(received_at)
                .execute(&mut *transaction)
                .await?;

                let result = sqlx::query(
                    "INSERT INTO telemetry (
                        event_at, received_at, device_id, boot_id, sequence, measurements, topic,
                        gateway_device_id
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (event_at, device_id, boot_id, sequence) DO NOTHING",
                )
                .bind(event.event_at)
                .bind(received_at)
                .bind(&event.device_id)
                .bind(event.boot_id)
                .bind(sequence)
                .bind(Json(serde_json::Value::Object(event.measurements.clone())))
                .bind(topic)
                .bind(&event.gateway_device_id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(result.rows_affected() == 1)
            }
        }
    }

    async fn require_sqlite_registered_device(
        &self,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        let Self::Sqlite(store) = self else {
            unreachable!("SQLite validation is only used by the SQLite adapter");
        };
        let registered = sqlx::query_scalar::<_, String>(
            "SELECT device_id FROM devices WHERE device_id = ? LIMIT 1",
        )
        .bind(device_id)
        .fetch_optional(store.pool())
        .await?
        .is_some();
        if registered {
            Ok(())
        } else {
            Err(PlatformStoreError::UnknownDevice(device_id.to_owned()))
        }
    }

    pub async fn backup_sqlite(&self) -> Result<PathBuf, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store.backup().await?),
            Self::Timescale(_) => Err(PlatformStoreError::BackupUnsupported),
        }
    }
}

async fn enqueue_sqlite_platform_command(
    store: &SqliteStore,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(&command.id)
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(&command.params)
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at.to_rfc3339())
    .bind(command.next_attempt_at.to_rfc3339())
    .fetch_optional(store.pool())
    .await?;
    if let Some(row) = row {
        return Ok(command_outbox_record(row)?);
    }

    let existing = sqlx::query(
        "SELECT
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(&command.id)
    .fetch_optional(store.pool())
    .await?
    .map(command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn enqueue_timescale_platform_command(
    pool: &PgPool,
    id: uuid::Uuid,
    params: serde_json::Value,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    if !timescale_device_is_locked(&mut transaction, &command.device_id).await? {
        return Err(PlatformStoreError::UnknownDevice(command.device_id.clone()));
    }

    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (id) DO NOTHING
         RETURNING
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(id)
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(Json(params))
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at)
    .bind(command.next_attempt_at)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(row) = row {
        let record = postgres_command_outbox_record(row)?;
        transaction.commit().await?;
        return Ok(record);
    }

    let existing = sqlx::query(
        "SELECT
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await?
    .map(postgres_command_outbox_record)
    .transpose()?;
    let result = match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    };
    transaction.commit().await?;
    result
}

async fn timescale_device_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE device_id = $1
         FOR KEY SHARE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

async fn claim_timescale_commands(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH due AS (
            SELECT id
            FROM command_outbox
            WHERE expires_at > $1
              AND (
                    (state = 'queued' AND next_attempt_at <= $1)
                    OR (state = 'leased' AND lease_until <= $1)
                  )
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
         )
         UPDATE command_outbox AS command
         SET state = 'leased',
             lease_until = $3,
             attempt_count = command.attempt_count + 1
         FROM due
         WHERE command.id = due.id
         RETURNING
            command.id, command.device_id, command.method, command.params, command.mode,
            command.state, command.expires_at, command.next_attempt_at, command.lease_until,
            command.attempt_count, command.last_error, command.published_at, command.response,
            command.responded_at",
    )
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_command_outbox_record)
        .collect()
}

async fn claim_timescale_notifications(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH due AS (
            SELECT id
            FROM notification_outbox
            WHERE (state = 'pending' AND next_attempt_at <= $1)
               OR (state = 'leased' AND lease_until <= $1)
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
         )
         UPDATE notification_outbox AS notification
         SET state = 'leased',
             lease_until = $3,
             attempt_count = notification.attempt_count + 1
         FROM due
         WHERE notification.id = due.id
         RETURNING
            notification.id, notification.incident_id, notification.kind,
            notification.dedupe_key, notification.subject, notification.body,
            notification.state, notification.next_attempt_at, notification.lease_until,
            notification.attempt_count, notification.last_error, notification.sent_at",
    )
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_notification_outbox_record)
        .collect()
}

async fn mark_timescale_notification_sent(
    pool: &PgPool,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    sent_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'sent', sent_at = $1, lease_until = NULL
         WHERE id = $2 AND state = 'leased' AND lease_until = $3
         RETURNING
            id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(sent_at)
    .bind(notification_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn release_timescale_notification_for_retry(
    pool: &PgPool,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'pending', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND state = 'leased' AND lease_until = $4
         RETURNING
            id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(notification_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn mark_timescale_command_published(
    pool: &PgPool,
    command_id: uuid::Uuid,
    published_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'published_to_broker', published_at = $1, lease_until = NULL
         WHERE id = $2 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(published_at)
    .bind(command_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn mark_timescale_command_failed(
    pool: &PgPool,
    command_id: uuid::Uuid,
    error: &str,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'failed', last_error = $1, lease_until = NULL
         WHERE id = $2 AND state = 'leased'
         RETURNING
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(error)
    .bind(command_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn release_timescale_command_for_retry(
    pool: &PgPool,
    command_id: uuid::Uuid,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'queued', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(command_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

fn command_payload_matches(
    existing: &CommandOutboxRecord,
    command: &NewCommandOutboxEntry,
) -> bool {
    existing.id == command.id
        && existing.device_id == command.device_id
        && existing.method == command.method
        && existing.params == command.params
        && existing.mode == command.mode
        && existing.expires_at == command.expires_at
        && existing.next_attempt_at == command.next_attempt_at
}

fn canonical_command_timestamp(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    timestamp
        .with_nanosecond(timestamp.nanosecond() / 1_000 * 1_000)
        .expect("a valid UTC timestamp can be represented at microsecond precision")
}

impl TopologyRepository for PlatformStore {
    fn register_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::register_device(self, device_id).await })
    }
}

impl IdentityRepository for PlatformStore {
    fn resolve_active_device_token<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<AuthenticatedDeviceToken, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::resolve_active_device_token(self, token).await })
    }
}

impl DeviceAuthorizationRepository for PlatformStore {
    fn authorize_device_session<'a>(
        &'a self,
        token_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(
            async move { PlatformStore::authorize_device_session(self, token_id, device_id).await },
        )
    }

    fn authorize_gateway_token<'a>(
        &'a self,
        token_id: uuid::Uuid,
        gateway_device_id: &'a str,
        child_device_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::authorize_gateway_token(
                self,
                token_id,
                gateway_device_id,
                child_device_id,
            )
            .await
        })
    }
}

impl CommandRepository for PlatformStore {
    fn enqueue_command<'a>(
        &'a self,
        command: NewCommandOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<CommandOutboxRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::enqueue_command(self, command).await })
    }
}

impl CommandLifecycleRepository for PlatformStore {
    fn claim_commands<'a>(
        &'a self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::claim_commands(self, now, lease_until, limit).await })
    }

    fn mark_command_published<'a>(
        &'a self,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_published(self, command_id, published_at).await
        })
    }

    fn mark_command_failed<'a>(
        &'a self,
        command_id: uuid::Uuid,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::mark_command_failed(self, command_id, error).await })
    }

    fn release_command_for_retry<'a>(
        &'a self,
        command_id: uuid::Uuid,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::release_command_for_retry(self, command_id, error, next_attempt_at).await
        })
    }

    fn expire_commands<'a>(
        &'a self,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::expire_commands(self, now).await })
    }

    fn mark_command_responded<'a>(
        &'a self,
        command_id: uuid::Uuid,
        device_id: &'a str,
        token_id: uuid::Uuid,
        response: &'a str,
        responded_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_responded(
                self,
                command_id,
                device_id,
                token_id,
                response,
                responded_at,
            )
            .await
        })
    }
}

impl TelemetryRepository for PlatformStore {
    fn write_telemetry<'a>(
        &'a self,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(
            async move { PlatformStore::write_telemetry(self, event, received_at, topic).await },
        )
    }
}

impl AuthorizationRepository for PlatformStore {
    fn device_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::device_permission(self, subject, device_id).await })
    }

    fn asset_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::asset_permission(self, subject, asset_id).await })
    }
}

impl NotificationRepository for PlatformStore {
    fn claim_notifications<'a>(
        &'a self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { PlatformStore::claim_notifications(self, now, lease_until, limit).await },
        )
    }

    fn mark_notification_sent<'a>(
        &'a self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_notification_sent(
                self,
                notification_id,
                expected_lease_until,
                sent_at,
            )
            .await
        })
    }

    fn release_notification_for_retry<'a>(
        &'a self,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::release_notification_for_retry(
                self,
                notification_id,
                expected_lease_until,
                error,
                next_attempt_at,
            )
            .await
        })
    }
}

impl ApplicationRepository for PlatformStore {
    fn upsert_application<'a>(
        &'a self,
        application: NewApplication,
    ) -> Pin<Box<dyn Future<Output = Result<ApplicationRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::upsert_application(self, application).await })
    }

    fn find_application_by_app_id<'a>(
        &'a self,
        app_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::find_application_by_app_id(self, app_id).await })
    }

    fn find_application_by_client_id<'a>(
        &'a self,
        client_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::find_application_by_client_id(self, client_id).await })
    }
}

fn strongest_share_permission(rows: Vec<String>) -> Option<ResourcePermission> {
    rows.into_iter()
        .filter_map(|value| ResourcePermission::parse_share(&value))
        .max()
}

fn validate_application(application: &mut NewApplication) -> Result<(), PlatformStoreError> {
    if application.launch_url.trim().is_empty() {
        return Err(PlatformStoreError::EmptyApplicationLaunchUrl);
    }
    let mut redirect_uris =
        std::collections::HashSet::with_capacity(application.redirect_uris.len());
    for redirect_uri in &application.redirect_uris {
        if !redirect_uris.insert(redirect_uri.as_str()) {
            return Err(PlatformStoreError::DuplicateApplicationRedirectUri(
                redirect_uri.as_str().to_owned(),
            ));
        }
    }
    for scope in &application.allowed_scopes {
        if scope.trim().is_empty() {
            return Err(PlatformStoreError::EmptyApplicationScope);
        }
    }
    application.allowed_scopes.sort();
    application.allowed_scopes.dedup();
    Ok(())
}

fn map_application_client_id_conflict(error: sqlx::Error, client_id: &str) -> PlatformStoreError {
    if error
        .as_database_error()
        .is_some_and(is_application_client_id_unique_violation)
    {
        PlatformStoreError::ApplicationClientIdConflict(client_id.to_owned())
    } else {
        PlatformStoreError::Database(error)
    }
}

fn is_application_client_id_unique_violation(
    database_error: &(dyn DatabaseError + 'static),
) -> bool {
    let code = database_error.code();
    match code.as_deref() {
        Some("23505") => {
            database_error.constraint() == Some("applications_client_id_key")
                || database_error
                    .message()
                    .contains("applications_client_id_key")
        }
        Some("19") | Some("2067") => database_error
            .message()
            .contains("UNIQUE constraint failed: applications.client_id"),
        _ => false,
    }
}

fn canonical_application_scopes(scopes: Vec<String>) -> Result<Vec<String>, PlatformStoreError> {
    if scopes.iter().any(|scope| scope.trim().is_empty()) {
        return Err(PlatformStoreError::InvalidApplicationScopes);
    }
    let mut scopes = scopes;
    scopes.sort();
    scopes.dedup();
    Ok(scopes)
}

fn sqlite_application_record(
    row: SqliteRow,
    redirects: Vec<String>,
) -> Result<ApplicationRecord, PlatformStoreError> {
    let scopes = canonical_application_scopes(
        serde_json::from_str(&row.try_get::<String, _>("allowed_scopes_json")?)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?,
    )?;
    Ok(ApplicationRecord {
        app_id: row.try_get::<String, _>("app_id")?.parse()?,
        kind: ApplicationKind::parse(&row.try_get::<String, _>("kind")?)?,
        launch_url: row.try_get("launch_url")?,
        client_id: row.try_get::<String, _>("client_id")?.parse()?,
        redirect_uris: redirects
            .into_iter()
            .map(|uri| uri.parse())
            .collect::<Result<Vec<_>, _>>()?,
        allowed_scopes: scopes,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
    })
}

fn postgres_application_record(
    row: PgRow,
    redirects: Vec<String>,
) -> Result<ApplicationRecord, PlatformStoreError> {
    let scopes = canonical_application_scopes(
        row.try_get::<Json<Vec<String>>, _>("allowed_scopes_json")?
            .0,
    )?;
    Ok(ApplicationRecord {
        app_id: row.try_get::<String, _>("app_id")?.parse()?,
        kind: ApplicationKind::parse(&row.try_get::<String, _>("kind")?)?,
        launch_url: row.try_get("launch_url")?,
        client_id: row.try_get::<String, _>("client_id")?.parse()?,
        redirect_uris: redirects
            .into_iter()
            .map(|uri| uri.parse())
            .collect::<Result<Vec<_>, _>>()?,
        allowed_scopes: scopes,
        enabled: row.try_get("enabled")?,
    })
}

async fn migrate_platform_timescale(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS iot_nano")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" WITH SCHEMA public")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO iot_nano, public")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(PLATFORM_POSTGRES_SCHEMA)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
}

fn postgres_command_outbox_record(row: PgRow) -> Result<CommandOutboxRecord, PlatformStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get::<uuid::Uuid, _>("id")?.to_string(),
        device_id: row.try_get("device_id")?,
        method: row.try_get("method")?,
        params: row
            .try_get::<Json<serde_json::Value>, _>("params")?
            .0
            .to_string(),
        mode: command_mode_from_database(&row.try_get::<String, _>("mode")?)?,
        state: CommandOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        expires_at: row.try_get("expires_at")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        lease_until: row.try_get("lease_until")?,
        attempt_count: i64::from(row.try_get::<i32, _>("attempt_count")?),
        last_error: row.try_get("last_error")?,
        published_at: row.try_get("published_at")?,
        response: row
            .try_get::<Option<Json<serde_json::Value>>, _>("response")?
            .map(|response| response.0.to_string()),
        responded_at: row.try_get("responded_at")?,
    })
}

fn postgres_notification_outbox_record(
    row: PgRow,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    Ok(NotificationOutboxRecord {
        id: row.try_get("id")?,
        incident_id: row.try_get("incident_id")?,
        kind: NotificationKind::from_database(&row.try_get::<String, _>("kind")?)?,
        dedupe_key: row.try_get("dedupe_key")?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        state: NotificationOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        lease_until: row.try_get("lease_until")?,
        attempt_count: i64::from(row.try_get::<i32, _>("attempt_count")?),
        last_error: row.try_get("last_error")?,
        sent_at: row.try_get("sent_at")?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutboxState {
    Queued,
    Leased,
    PublishedToBroker,
    Responded,
    Expired,
    Failed,
}

impl CommandOutboxState {
    fn from_database(value: &str) -> Result<Self, SqliteStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "published_to_broker" => Ok(Self::PublishedToBroker),
            "responded" => Ok(Self::Responded),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            _ => Err(SqliteStoreError::InvalidCommandState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCommandOutboxEntry {
    pub id: String,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutboxRecord {
    pub id: String,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub state: CommandOutboxState,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub response: Option<String>,
    pub responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    Opened,
    Resolved,
    Reminder,
}

impl NotificationKind {
    fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "opened" => Ok(Self::Opened),
            "resolved" => Ok(Self::Resolved),
            "reminder" => Ok(Self::Reminder),
            _ => Err(PlatformStoreError::InvalidNotificationKind(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationOutboxState {
    Pending,
    Leased,
    Sent,
}

impl NotificationOutboxState {
    fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "sent" => Ok(Self::Sent),
            _ => Err(PlatformStoreError::InvalidNotificationState(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationOutboxRecord {
    pub id: uuid::Uuid,
    pub incident_id: uuid::Uuid,
    pub kind: NotificationKind,
    pub dedupe_key: String,
    pub subject: String,
    pub body: String,
    pub state: NotificationOutboxState,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionResult {
    pub raw_rows: u64,
    pub rollup_rows: u64,
    pub event_evaluation_rows: u64,
    pub notification_rows: u64,
    pub resolved_incident_rows: u64,
}

impl SqliteStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, SqliteStoreError> {
        if configuration.storage != DatabaseStorage::Sqlite {
            return Err(SqliteStoreError::InvalidConfiguration);
        }
        let path = configuration
            .sqlite_path
            .as_ref()
            .ok_or(SqliteStoreError::InvalidConfiguration)?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            && !parent.exists()
        {
            fs::create_dir_all(parent)?;
            #[cfg(unix)]
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(configuration.sqlite_busy_timeout_ms));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&pool)
            .await?;
        sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await?;
        migrate_command_outbox_schema(&pool).await?;
        migrate_resource_authorization_schema(&pool).await?;
        Ok(Self {
            pool,
            path: path.clone(),
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn backup(&self) -> Result<PathBuf, SqliteStoreError> {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(SqliteStoreError::InvalidConfiguration)?;
        let backup_name = format!(
            "{file_name}.backup-{}",
            Utc::now().format("%Y%m%dT%H%M%S%fZ")
        );
        let backup_path = self.path.with_file_name(backup_name);

        sqlx::query("VACUUM INTO ?")
            .bind(backup_path.to_string_lossy().as_ref())
            .execute(&self.pool)
            .await?;
        #[cfg(unix)]
        fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600))?;
        Ok(backup_path)
    }

    pub async fn enqueue_command(
        &self,
        command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, SqliteStoreError> {
        let row = sqlx::query(
            "INSERT INTO command_outbox (
                id, device_id, method, params, mode, expires_at, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command.id)
        .bind(command.device_id)
        .bind(command.method)
        .bind(command.params)
        .bind(command_mode_value(command.mode))
        .bind(command.expires_at.to_rfc3339())
        .bind(command.next_attempt_at.to_rfc3339())
        .fetch_one(&self.pool)
        .await?;
        command_outbox_record(row)
    }

    pub async fn claim_commands(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = now.to_rfc3339();
        // This single write statement makes claiming atomic across SQLite connections.
        let rows = sqlx::query(
            "WITH due AS (
                SELECT id
                FROM command_outbox
                WHERE expires_at > ?
                  AND (
                      (state = 'queued' AND next_attempt_at <= ?)
                      OR (state = 'leased' AND lease_until <= ?)
                  )
                ORDER BY next_attempt_at, created_at, id
                LIMIT ?
             )
             UPDATE command_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE id IN (SELECT id FROM due)
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn mark_command_published(
        &self,
        command_id: &str,
        published_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let published_at = published_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'published_to_broker',
                 published_at = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&published_at)
        .bind(command_id)
        .bind(&published_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_failed(
        &self,
        command_id: &str,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'failed',
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(error)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn release_command_for_retry(
        &self,
        command_id: &str,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let next_attempt_at = next_attempt_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'queued',
                 next_attempt_at = ?,
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(command_id)
        .bind(&next_attempt_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn claim_notifications(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = now.to_rfc3339();
        let rows = sqlx::query(
            "WITH due AS (
                SELECT id
                FROM notification_outbox
                WHERE (state = 'pending' AND next_attempt_at <= ?)
                   OR (state = 'leased' AND lease_until <= ?)
                ORDER BY next_attempt_at, created_at, id
                LIMIT ?
             )
             UPDATE notification_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE id IN (SELECT id FROM due)
             RETURNING
                id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(notification_outbox_record).collect()
    }

    pub async fn mark_notification_sent(
        &self,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let sent_at = sent_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'sent', sent_at = ?, lease_until = NULL
             WHERE id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&sent_at)
        .bind(notification_id)
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn release_notification_for_retry(
        &self,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let next_attempt_at = next_attempt_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'pending', next_attempt_at = ?, last_error = ?, lease_until = NULL
             WHERE id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(notification_id)
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn expire_commands(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
        let rows = sqlx::query(
            "UPDATE command_outbox
             SET state = 'expired',
                 lease_until = NULL
             WHERE (
                    state IN ('queued', 'leased')
                    OR (state = 'published_to_broker' AND mode = 'two_way')
                   )
               AND expires_at <= ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(now.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn mark_command_responded(
        &self,
        command_id: &str,
        device_id: &str,
        token_id: &str,
        response: &str,
        responded_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let responded_at = responded_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox AS command
             SET state = 'responded',
                 response = CASE
                    WHEN command.state = 'responded' THEN command.response
                    ELSE ?
                 END,
                 responded_at = CASE
                    WHEN command.state = 'responded' THEN command.responded_at
                    ELSE ?
                 END,
                 lease_until = NULL
             WHERE command.id = ?
               AND command.device_id = ?
               AND command.mode = 'two_way'
               AND (
                    (command.state = 'published_to_broker' AND command.expires_at > ?)
                    OR (command.state = 'responded' AND command.response = ?)
               )
               AND EXISTS (
                    SELECT 1
                    FROM device_tokens
                    WHERE id = ?
                      AND device_id = command.device_id
                      AND revoked_at IS NULL
               )
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(response)
        .bind(&responded_at)
        .bind(command_id)
        .bind(device_id)
        .bind(&responded_at)
        .bind(response)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn write_telemetry(
        &self,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = self
            .write_telemetry_in_transaction(&mut transaction, event, received_at, topic)
            .await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn write_telemetry_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let received_at = received_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO devices (device_id, last_seen_at)
             VALUES (?, ?)
             ON CONFLICT(device_id) DO UPDATE SET
                 last_seen_at = CASE
                     WHEN devices.last_seen_at IS NULL OR excluded.last_seen_at > devices.last_seen_at
                     THEN excluded.last_seen_at
                     ELSE devices.last_seen_at
                 END",
        )
        .bind(&event.device_id)
        .bind(&received_at)
        .execute(transaction.as_mut())
        .await?;
        let insert = sqlx::query(
            "INSERT OR IGNORE INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic,
                gateway_device_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.event_at.to_rfc3339())
        .bind(&received_at)
        .bind(&event.device_id)
        .bind(event.boot_id.to_string())
        .bind(i64::try_from(event.sequence).map_err(|_| SqliteStoreError::SequenceOverflow)?)
        .bind(serde_json::to_string(&event.measurements).map_err(SqliteStoreError::Serialization)?)
        .bind(topic)
        .bind(&event.gateway_device_id)
        .execute(transaction.as_mut())
        .await?;
        let inserted = insert.rows_affected() == 1;
        if inserted {
            let metrics = MetricAverages::from_event(event);
            upsert_rollup(
                transaction,
                RollupTable::FiveMinute,
                bucket_start(event.event_at, 5 * 60),
                &event.device_id,
                metrics,
            )
            .await?;
            upsert_rollup(
                transaction,
                RollupTable::OneHour,
                bucket_start(event.event_at, 60 * 60),
                &event.device_id,
                metrics,
            )
            .await?;
        }
        Ok(inserted)
    }

    pub async fn enforce_retention(
        &self,
        raw_before: &str,
        rollup_before: &str,
        batch_size: u64,
    ) -> Result<RetentionResult, SqliteStoreError> {
        let batch_size = batch_size.max(1).min(10_000);
        let raw_rows =
            delete_before(&self.pool, RetentionTable::Raw, raw_before, batch_size).await?;
        let rollup_rows = delete_before(
            &self.pool,
            RetentionTable::FiveMinuteRollup,
            rollup_before,
            batch_size,
        )
        .await?
            + delete_before(
                &self.pool,
                RetentionTable::OneHourRollup,
                rollup_before,
                batch_size,
            )
            .await?;
        let event_evaluation_rows = delete_before(
            &self.pool,
            RetentionTable::AlertRuleEventEvaluations,
            raw_before,
            batch_size,
        )
        .await?;
        let notification_rows = delete_before(
            &self.pool,
            RetentionTable::SentNotifications,
            raw_before,
            batch_size,
        )
        .await?;
        let resolved_incident_rows = delete_before(
            &self.pool,
            RetentionTable::ResolvedIncidents,
            rollup_before,
            batch_size,
        )
        .await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA incremental_vacuum(64)")
            .execute(&self.pool)
            .await?;
        Ok(RetentionResult {
            raw_rows,
            rollup_rows,
            event_evaluation_rows,
            notification_rows,
            resolved_incident_rows,
        })
    }
}

async fn migrate_command_outbox_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let columns = sqlx::query("PRAGMA table_info(command_outbox)")
        .fetch_all(pool)
        .await?;
    for column in columns {
        if column.try_get::<String, _>("name")? == "mode" {
            refresh_command_outbox_expiring_index(pool).await?;
            return Ok(());
        }
    }

    let mut transaction = pool.begin().await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_due_index")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_expiring_index")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(
        "CREATE TABLE command_outbox_rebuild (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
            method TEXT NOT NULL CHECK (trim(method) <> ''),
            params TEXT NOT NULL DEFAULT '{}',
            mode TEXT NOT NULL DEFAULT 'one_way'
                CHECK (mode IN ('one_way', 'two_way')),
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
            expires_at TEXT NOT NULL,
            next_attempt_at TEXT NOT NULL,
            lease_until TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            last_error TEXT,
            published_at TEXT,
            response TEXT,
            responded_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO command_outbox_rebuild (
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, created_at
         )
         SELECT
            id, device_id, method, params, 'one_way', state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, created_at
         FROM command_outbox",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query("DROP TABLE command_outbox")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("ALTER TABLE command_outbox_rebuild RENAME TO command_outbox")
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_due_index
         ON command_outbox (state, next_attempt_at)
         WHERE state = 'queued'",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    refresh_command_outbox_expiring_index(pool).await
}

async fn migrate_resource_authorization_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if !sqlite_table_has_column(pool, "users", "account_class").await? {
        sqlx::query(
            "ALTER TABLE users
             ADD COLUMN account_class TEXT NOT NULL DEFAULT 'user'
             CHECK (account_class IN ('system', 'admin', 'user'))",
        )
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "UPDATE users
         SET account_class = CASE role
             WHEN 'admin' THEN 'admin'
             ELSE 'user'
         END
         WHERE account_class IS NULL
            OR account_class NOT IN ('system', 'admin', 'user')
            OR (account_class = 'user' AND role = 'admin')",
    )
    .execute(pool)
    .await?;

    if !sqlite_table_has_column(pool, "devices", "owner_user_id").await? {
        sqlx::query(
            "ALTER TABLE devices
             ADD COLUMN owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await?;
    }
    if !sqlite_table_has_column(pool, "devices", "claimed_at").await? {
        sqlx::query("ALTER TABLE devices ADD COLUMN claimed_at TEXT")
            .execute(pool)
            .await?;
    }
    if !sqlite_table_has_column(pool, "assets", "owner_user_id").await? {
        sqlx::query(
            "ALTER TABLE assets
             ADD COLUMN owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await?;
    }

    sqlx::raw_sql(
        "CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
             ON devices (owner_user_id)
             WHERE owner_user_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
             ON assets (owner_user_id)
             WHERE owner_user_id IS NOT NULL;
         CREATE TABLE IF NOT EXISTS resource_shares (
             id TEXT PRIMARY KEY,
             resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
             resource_id TEXT NOT NULL,
             target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
             permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
             inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
             state TEXT NOT NULL CHECK (
                 state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
             ),
             created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             responded_at TEXT,
             expires_at TEXT
         );
         CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
             ON resource_shares (resource_type, resource_id, target_user_id)
             WHERE state = 'pending';
         CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
             ON resource_shares (target_user_id, state, created_at DESC);
         CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
             ON resource_shares (resource_type, resource_id, state);
         CREATE TABLE IF NOT EXISTS audit_events (
             id TEXT PRIMARY KEY,
             actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
             actor_account_class TEXT NOT NULL
                 CHECK (actor_account_class IN ('system', 'admin', 'user')),
             resource_type TEXT NOT NULL,
             resource_id TEXT NOT NULL,
             action TEXT NOT NULL,
             before_value TEXT,
             after_value TEXT,
             request_id TEXT,
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         CREATE INDEX IF NOT EXISTS audit_events_resource_index
             ON audit_events (resource_type, resource_id, created_at DESC);
         CREATE INDEX IF NOT EXISTS audit_events_actor_index
             ON audit_events (actor_user_id, created_at DESC);
         CREATE TABLE IF NOT EXISTS device_claim_codes (
             device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
             code_hash TEXT NOT NULL,
             expires_at TEXT NOT NULL,
             issued_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
             issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             used_at TEXT
         );
         CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index
             ON device_claim_codes (expires_at)
             WHERE used_at IS NULL;",
    )
    .execute(pool)
    .await
    .map(|_| ())
}

async fn sqlite_table_has_column(
    pool: &SqlitePool,
    table: &str,
    column_name: &str,
) -> Result<bool, sqlx::Error> {
    let statement = match table {
        "users" => "PRAGMA table_info(users)",
        "devices" => "PRAGMA table_info(devices)",
        "assets" => "PRAGMA table_info(assets)",
        _ => unreachable!("only audited schema table names may be queried"),
    };
    let columns = sqlx::query(statement).fetch_all(pool).await?;
    for column in columns {
        if column.try_get::<String, _>("name")? == column_name {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn refresh_command_outbox_expiring_index(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_expiring_index")
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_expiring_index
         ON command_outbox (expires_at)
         WHERE state IN ('queued', 'leased')
            OR (state = 'published_to_broker' AND mode = 'two_way')",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

fn command_outbox_record(row: SqliteRow) -> Result<CommandOutboxRecord, SqliteStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        method: row.try_get("method")?,
        params: row.try_get("params")?,
        mode: command_mode_from_database(&row.try_get::<String, _>("mode")?)?,
        state: CommandOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        expires_at: command_timestamp(&row, "expires_at")?,
        next_attempt_at: command_timestamp(&row, "next_attempt_at")?,
        lease_until: command_optional_timestamp(&row, "lease_until")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        published_at: command_optional_timestamp(&row, "published_at")?,
        response: row.try_get("response")?,
        responded_at: command_optional_timestamp(&row, "responded_at")?,
    })
}

fn notification_outbox_record(
    row: SqliteRow,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let incident_id: String = row.try_get("incident_id")?;
    Ok(NotificationOutboxRecord {
        id: uuid::Uuid::parse_str(&id)
            .map_err(|_| PlatformStoreError::InvalidNotificationId(id))?,
        incident_id: uuid::Uuid::parse_str(&incident_id)
            .map_err(|_| PlatformStoreError::InvalidNotificationId(incident_id))?,
        kind: NotificationKind::from_database(&row.try_get::<String, _>("kind")?)?,
        dedupe_key: row.try_get("dedupe_key")?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        state: NotificationOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        next_attempt_at: notification_timestamp(&row, "next_attempt_at")?,
        lease_until: notification_optional_timestamp(&row, "lease_until")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        sent_at: notification_optional_timestamp(&row, "sent_at")?,
    })
}

fn notification_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    let value: String = row.try_get(column)?;
    parse_notification_timestamp(value, column)
}

fn notification_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| parse_notification_timestamp(value, column))
        .transpose()
}

fn parse_notification_timestamp(
    value: String,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|timestamp| timestamp.and_utc())
        })
        .map_err(|source| PlatformStoreError::InvalidNotificationTimestamp {
            column,
            value,
            source,
        })
}

fn command_mode_value(mode: RpcMode) -> &'static str {
    match mode {
        RpcMode::OneWay => "one_way",
        RpcMode::TwoWay => "two_way",
    }
}

fn command_mode_from_database(value: &str) -> Result<RpcMode, SqliteStoreError> {
    match value {
        "one_way" => Ok(RpcMode::OneWay),
        "two_way" => Ok(RpcMode::TwoWay),
        _ => Err(SqliteStoreError::InvalidCommandState(value.to_owned())),
    }
}

fn command_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, SqliteStoreError> {
    let value: String = row.try_get(column)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
            column,
            value,
            source,
        })
}

fn command_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, SqliteStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
                    column,
                    value,
                    source,
                })
        })
        .transpose()
}

async fn delete_before(
    pool: &SqlitePool,
    table: RetentionTable,
    before: &str,
    batch_size: u64,
) -> Result<u64, sqlx::Error> {
    let bound_batch_size = i64::try_from(batch_size).unwrap_or(10_000);
    let result = match table {
        RetentionTable::Raw => {
            sqlx::query(
                "DELETE FROM telemetry
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry
                     WHERE event_at < ?
                     ORDER BY event_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::FiveMinuteRollup => {
            sqlx::query(
                "DELETE FROM telemetry_rollups_5m
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry_rollups_5m
                     WHERE bucket_at < ?
                     ORDER BY bucket_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::OneHourRollup => {
            sqlx::query(
                "DELETE FROM telemetry_rollups_1h
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry_rollups_1h
                     WHERE bucket_at < ?
                     ORDER BY bucket_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::AlertRuleEventEvaluations => {
            sqlx::query(
                "DELETE FROM alert_rule_event_evaluations
                 WHERE rowid IN (
                     SELECT rowid
                     FROM alert_rule_event_evaluations
                     WHERE event_at < ?
                     ORDER BY event_at, rule_id, device_id, boot_id, sequence
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::SentNotifications => {
            sqlx::query(
                "DELETE FROM notification_outbox
                 WHERE rowid IN (
                     SELECT rowid
                     FROM notification_outbox
                     WHERE state = 'sent' AND sent_at < ?
                     ORDER BY sent_at, id
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::ResolvedIncidents => {
            sqlx::query(
                "DELETE FROM alert_incidents
                 WHERE rowid IN (
                     SELECT rowid
                     FROM alert_incidents
                     WHERE status = 'resolved'
                       AND resolved_at < ?
                       AND NOT EXISTS (
                           SELECT 1
                           FROM notification_outbox
                           WHERE notification_outbox.incident_id = alert_incidents.id
                             AND notification_outbox.state IN ('pending', 'leased')
                       )
                     ORDER BY resolved_at, id
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
    };
    Ok(result.rows_affected())
}

#[derive(Clone, Copy)]
enum RetentionTable {
    Raw,
    FiveMinuteRollup,
    OneHourRollup,
    AlertRuleEventEvaluations,
    SentNotifications,
    ResolvedIncidents,
}

#[derive(Clone, Copy)]
enum RollupTable {
    FiveMinute,
    OneHour,
}

#[derive(Clone, Copy)]
struct MetricAverages {
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
    voltage_v: Option<f64>,
    current_a: Option<f64>,
    power_w: Option<f64>,
    energy_kwh: Option<f64>,
}

impl MetricAverages {
    fn from_event(event: &TelemetryEvent) -> Self {
        Self {
            temperature_c: numeric_measurement(event, "temperature_c"),
            humidity_pct: numeric_measurement(event, "humidity_pct"),
            voltage_v: numeric_measurement(event, "voltage_v"),
            current_a: numeric_measurement(event, "current_a"),
            power_w: numeric_measurement(event, "power_w"),
            energy_kwh: numeric_measurement(event, "energy_kwh"),
        }
    }
}

fn numeric_measurement(event: &TelemetryEvent, key: &str) -> Option<f64> {
    event
        .measurements
        .get(key)
        .and_then(serde_json::Value::as_f64)
}

fn bucket_start(event_at: DateTime<Utc>, seconds: i64) -> String {
    let timestamp = event_at.timestamp();
    let bucket = timestamp - timestamp.rem_euclid(seconds);
    Utc.timestamp_opt(bucket, 0)
        .single()
        .expect("valid UTC bucket timestamp")
        .to_rfc3339()
}

async fn upsert_rollup(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: RollupTable,
    bucket_at: String,
    device_id: &str,
    metrics: MetricAverages,
) -> Result<(), sqlx::Error> {
    let query = match table {
        RollupTable::FiveMinute => {
            "INSERT INTO telemetry_rollups_5m (
                bucket_at, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket_at, device_id) DO UPDATE SET
                avg_temperature_c = CASE
                    WHEN excluded.temperature_count = 0 THEN telemetry_rollups_5m.avg_temperature_c
                    WHEN telemetry_rollups_5m.temperature_count = 0 THEN excluded.avg_temperature_c
                    ELSE (
                        telemetry_rollups_5m.avg_temperature_c * telemetry_rollups_5m.temperature_count
                        + excluded.avg_temperature_c * excluded.temperature_count
                    ) / (telemetry_rollups_5m.temperature_count + excluded.temperature_count)
                END,
                temperature_count = telemetry_rollups_5m.temperature_count + excluded.temperature_count,
                avg_humidity_pct = CASE
                    WHEN excluded.humidity_count = 0 THEN telemetry_rollups_5m.avg_humidity_pct
                    WHEN telemetry_rollups_5m.humidity_count = 0 THEN excluded.avg_humidity_pct
                    ELSE (
                        telemetry_rollups_5m.avg_humidity_pct * telemetry_rollups_5m.humidity_count
                        + excluded.avg_humidity_pct * excluded.humidity_count
                    ) / (telemetry_rollups_5m.humidity_count + excluded.humidity_count)
                END,
                humidity_count = telemetry_rollups_5m.humidity_count + excluded.humidity_count,
                avg_voltage_v = CASE
                    WHEN excluded.voltage_count = 0 THEN telemetry_rollups_5m.avg_voltage_v
                    WHEN telemetry_rollups_5m.voltage_count = 0 THEN excluded.avg_voltage_v
                    ELSE (
                        telemetry_rollups_5m.avg_voltage_v * telemetry_rollups_5m.voltage_count
                        + excluded.avg_voltage_v * excluded.voltage_count
                    ) / (telemetry_rollups_5m.voltage_count + excluded.voltage_count)
                END,
                voltage_count = telemetry_rollups_5m.voltage_count + excluded.voltage_count,
                avg_current_a = CASE
                    WHEN excluded.current_count = 0 THEN telemetry_rollups_5m.avg_current_a
                    WHEN telemetry_rollups_5m.current_count = 0 THEN excluded.avg_current_a
                    ELSE (
                        telemetry_rollups_5m.avg_current_a * telemetry_rollups_5m.current_count
                        + excluded.avg_current_a * excluded.current_count
                    ) / (telemetry_rollups_5m.current_count + excluded.current_count)
                END,
                current_count = telemetry_rollups_5m.current_count + excluded.current_count,
                avg_power_w = CASE
                    WHEN excluded.power_count = 0 THEN telemetry_rollups_5m.avg_power_w
                    WHEN telemetry_rollups_5m.power_count = 0 THEN excluded.avg_power_w
                    ELSE (
                        telemetry_rollups_5m.avg_power_w * telemetry_rollups_5m.power_count
                        + excluded.avg_power_w * excluded.power_count
                    ) / (telemetry_rollups_5m.power_count + excluded.power_count)
                END,
                power_count = telemetry_rollups_5m.power_count + excluded.power_count,
                avg_energy_kwh = CASE
                    WHEN excluded.energy_count = 0 THEN telemetry_rollups_5m.avg_energy_kwh
                    WHEN telemetry_rollups_5m.energy_count = 0 THEN excluded.avg_energy_kwh
                    ELSE (
                        telemetry_rollups_5m.avg_energy_kwh * telemetry_rollups_5m.energy_count
                        + excluded.avg_energy_kwh * excluded.energy_count
                    ) / (telemetry_rollups_5m.energy_count + excluded.energy_count)
                END,
                energy_count = telemetry_rollups_5m.energy_count + excluded.energy_count,
                event_count = telemetry_rollups_5m.event_count + 1"
        }
        RollupTable::OneHour => {
            "INSERT INTO telemetry_rollups_1h (
                bucket_at, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket_at, device_id) DO UPDATE SET
                avg_temperature_c = CASE
                    WHEN excluded.temperature_count = 0 THEN telemetry_rollups_1h.avg_temperature_c
                    WHEN telemetry_rollups_1h.temperature_count = 0 THEN excluded.avg_temperature_c
                    ELSE (
                        telemetry_rollups_1h.avg_temperature_c * telemetry_rollups_1h.temperature_count
                        + excluded.avg_temperature_c * excluded.temperature_count
                    ) / (telemetry_rollups_1h.temperature_count + excluded.temperature_count)
                END,
                temperature_count = telemetry_rollups_1h.temperature_count + excluded.temperature_count,
                avg_humidity_pct = CASE
                    WHEN excluded.humidity_count = 0 THEN telemetry_rollups_1h.avg_humidity_pct
                    WHEN telemetry_rollups_1h.humidity_count = 0 THEN excluded.avg_humidity_pct
                    ELSE (
                        telemetry_rollups_1h.avg_humidity_pct * telemetry_rollups_1h.humidity_count
                        + excluded.avg_humidity_pct * excluded.humidity_count
                    ) / (telemetry_rollups_1h.humidity_count + excluded.humidity_count)
                END,
                humidity_count = telemetry_rollups_1h.humidity_count + excluded.humidity_count,
                avg_voltage_v = CASE
                    WHEN excluded.voltage_count = 0 THEN telemetry_rollups_1h.avg_voltage_v
                    WHEN telemetry_rollups_1h.voltage_count = 0 THEN excluded.avg_voltage_v
                    ELSE (
                        telemetry_rollups_1h.avg_voltage_v * telemetry_rollups_1h.voltage_count
                        + excluded.avg_voltage_v * excluded.voltage_count
                    ) / (telemetry_rollups_1h.voltage_count + excluded.voltage_count)
                END,
                voltage_count = telemetry_rollups_1h.voltage_count + excluded.voltage_count,
                avg_current_a = CASE
                    WHEN excluded.current_count = 0 THEN telemetry_rollups_1h.avg_current_a
                    WHEN telemetry_rollups_1h.current_count = 0 THEN excluded.avg_current_a
                    ELSE (
                        telemetry_rollups_1h.avg_current_a * telemetry_rollups_1h.current_count
                        + excluded.avg_current_a * excluded.current_count
                    ) / (telemetry_rollups_1h.current_count + excluded.current_count)
                END,
                current_count = telemetry_rollups_1h.current_count + excluded.current_count,
                avg_power_w = CASE
                    WHEN excluded.power_count = 0 THEN telemetry_rollups_1h.avg_power_w
                    WHEN telemetry_rollups_1h.power_count = 0 THEN excluded.avg_power_w
                    ELSE (
                        telemetry_rollups_1h.avg_power_w * telemetry_rollups_1h.power_count
                        + excluded.avg_power_w * excluded.power_count
                    ) / (telemetry_rollups_1h.power_count + excluded.power_count)
                END,
                power_count = telemetry_rollups_1h.power_count + excluded.power_count,
                avg_energy_kwh = CASE
                    WHEN excluded.energy_count = 0 THEN telemetry_rollups_1h.avg_energy_kwh
                    WHEN telemetry_rollups_1h.energy_count = 0 THEN excluded.avg_energy_kwh
                    ELSE (
                        telemetry_rollups_1h.avg_energy_kwh * telemetry_rollups_1h.energy_count
                        + excluded.avg_energy_kwh * excluded.energy_count
                    ) / (telemetry_rollups_1h.energy_count + excluded.energy_count)
                END,
                energy_count = telemetry_rollups_1h.energy_count + excluded.energy_count,
                event_count = telemetry_rollups_1h.event_count + 1"
        }
    };
    let values = [
        metrics.temperature_c,
        metrics.humidity_pct,
        metrics.voltage_v,
        metrics.current_a,
        metrics.power_w,
        metrics.energy_kwh,
    ];
    let mut query = sqlx::query(query).bind(bucket_at).bind(device_id);
    for value in values {
        query = query.bind(value).bind(i64::from(value.is_some()));
    }
    query.execute(&mut **transaction).await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum SqliteStoreError {
    #[error("SQLite storage configuration is invalid")]
    InvalidConfiguration,
    #[error("invalid command outbox state: {0}")]
    InvalidCommandState(String),
    #[error("invalid command outbox {column} timestamp: {value}")]
    InvalidCommandTimestamp {
        column: &'static str,
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("telemetry sequence does not fit SQLite INTEGER")]
    SequenceOverflow,
    #[error("telemetry measurements cannot be serialized")]
    Serialization(#[source] serde_json::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Filesystem(#[from] std::io::Error),
}
