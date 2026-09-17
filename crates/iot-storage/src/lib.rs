#![forbid(unsafe_code)]

mod management;
mod public_api;
mod tenant_identity;

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, TimeZone, Timelike, Utc};
use iot_core::{
    DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent, device_token_prefix,
    verify_device_token,
};
use sha2::{Digest, Sha256};
use sqlx::{
    Executor, PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction,
    error::DatabaseError,
    postgres::{PgPoolOptions, PgRow},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
    types::Json,
};
use subtle::ConstantTimeEq;
use thiserror::Error;
use url::Url;

const PLATFORM_POSTGRES_SCHEMA: &str = include_str!("../migrations/0001_platform.sql");
const SQLITE_PLATFORM_SCHEMA_VERSION: i64 = 1;
const SET_SQLITE_PLATFORM_SCHEMA_VERSION: &str = "PRAGMA user_version = 1";

pub use management::{
    CreateManagementAsset, CreateManagementAssetProfile, CreateManagementDeviceProfile,
    CreateManagementUser, DeviceTokenRecord, DeviceTokenRepository, DeviceTokenRepositoryError,
    ManagementAsset, ManagementAssetError, ManagementAssetProfile, ManagementAssetProfileError,
    ManagementAssetProfileRepository, ManagementAssetRepository, ManagementChildStatus,
    ManagementDevice, ManagementDeviceError, ManagementDeviceHealth, ManagementDeviceProfile,
    ManagementDeviceProfileError, ManagementDeviceProfileRepository, ManagementDeviceRepository,
    ManagementDeviceTopology, ManagementGatewayStatus, ManagementUser, ManagementUserError,
    ManagementUserRepository, ManagementUserRole, NewDeviceToken, NewOwnedDeviceToken,
    UpdateManagementAsset, UpdateManagementAssetProfile, UpdateManagementDevice,
    UpdateManagementDeviceProfile, UpdateManagementUser,
};
pub use public_api::{
    NewPublicAsset, NewPublicDevice, NewPublicResourceGrant, PublicAlert, PublicApiRepository,
    PublicAsset, PublicDevice, PublicDeviceError, PublicPrincipal, PublicResourceGrant,
    PublicTelemetry,
};
pub use tenant_identity::{
    AccountStatus, NewSystemAccount, NewTenant, NewTenantAccount, SystemAccount,
    SystemAccountCredential, Tenant, TenantAccount, TenantAccountCredential, TenantIdentityError,
    TenantIdentityRepository, TenantStatus, TenantUserCredential,
};

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    display_name TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT,
    asset_id TEXT,
    device_profile_id TEXT,
    deleted_at TEXT,
    is_gateway INTEGER NOT NULL DEFAULT 0,
    gateway_device_id TEXT,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    owner_user_id TEXT,
    claimed_at TEXT,
    UNIQUE (device_id, tenant_id),
    FOREIGN KEY (asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        (is_gateway = 1 AND gateway_device_id IS NULL)
        OR (is_gateway = 0 AND gateway_device_id IS NOT device_id)
    )
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    UNIQUE (tenant_id, event_at, device_id, boot_id, sequence),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS telemetry_tenant_device_event_at_index
    ON telemetry (tenant_id, device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_tenant_gateway_device_event_at_index
    ON telemetry (tenant_id, gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, gateway_device_id, idempotency_key),
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS telemetry_rollups_5m (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
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
    PRIMARY KEY (tenant_id, bucket_at, device_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS telemetry_rollups_1h (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
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
    PRIMARY KEY (tenant_id, bucket_at, device_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_accounts (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS system_accounts_one_active_index
    ON system_accounts (status)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended', 'deleted')),
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tenant_accounts (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    credential_version INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS users_id_tenant_id_index
    ON users (id, tenant_id);
CREATE INDEX IF NOT EXISTS users_tenant_username_id_index
    ON users (tenant_id, username, id);
CREATE TRIGGER IF NOT EXISTS users_tenant_id_immutable
BEFORE UPDATE OF tenant_id ON users
FOR EACH ROW WHEN NEW.tenant_id IS NOT OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'users.tenant_id is immutable');
END;
CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, app_key)
);
CREATE TABLE IF NOT EXISTS applications (
    app_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('frontend', 'full_stack')),
    launch_url TEXT NOT NULL,
    client_id TEXT NOT NULL UNIQUE,
    allowed_scopes_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    UNIQUE (app_id, tenant_id)
);
CREATE TABLE IF NOT EXISTS application_redirect_uris (
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    PRIMARY KEY (app_id, tenant_id, redirect_uri),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS application_redirect_uris_lookup_index
    ON application_redirect_uris (app_id, tenant_id, redirect_uri);

CREATE TABLE IF NOT EXISTS oauth_client_secrets (
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, secret_hash),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS oauth_authorization_codes_active_index
    ON oauth_authorization_codes (tenant_id, app_id, expires_at)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS oauth_access_tokens (
    token_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    scopes_json TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS oauth_access_tokens_expiry_index
    ON oauth_access_tokens (expires_at);

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
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_profile_id TEXT REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id TEXT,
    owner_user_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, parent_asset_id, name),
    FOREIGN KEY (parent_asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS assets_tenant_parent_index
    ON assets (tenant_id, parent_asset_id);
CREATE UNIQUE INDEX IF NOT EXISTS assets_tenant_root_name_unique_index
    ON assets (tenant_id, name)
    WHERE parent_asset_id IS NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_asset_index
    ON devices (tenant_id, asset_id);
CREATE INDEX IF NOT EXISTS devices_tenant_active_index
    ON devices (tenant_id, device_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_gateway_active_index
    ON devices (tenant_id, gateway_device_id)
    WHERE deleted_at IS NULL AND gateway_device_id IS NOT NULL;
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

CREATE TABLE IF NOT EXISTS resource_grants (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    grantee_type TEXT NOT NULL CHECK (grantee_type IN ('user', 'application')),
    grantee_id TEXT NOT NULL,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    created_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (resource_type, resource_id, grantee_type, grantee_id)
);
CREATE INDEX IF NOT EXISTS resource_grants_resource_index
    ON resource_grants (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS resource_grants_grantee_index
    ON resource_grants (grantee_type, grantee_id);
CREATE INDEX IF NOT EXISTS resource_grants_tenant_id_index
    ON resource_grants (tenant_id, id);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum GatewayIngestValidationError {
    #[error("child telemetry requires a child device ID")]
    ChildTelemetryMissingChild,
    #[error("child telemetry requires a telemetry event")]
    ChildTelemetryMissingTelemetry,
    #[error("telemetry is only valid for child telemetry events")]
    TelemetryOnNonChildEvent,
    #[error("telemetry requires a child device ID")]
    TelemetryMissingChild,
    #[error("telemetry device ID does not match the child device ID")]
    TelemetryChildMismatch,
    #[error("telemetry gateway ID does not match the gateway device ID")]
    TelemetryGatewayMismatch,
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
    #[error("incident ID is not a UUID: {0:?}")]
    InvalidIncidentId(String),
    #[error("invalid alert incident status: {0:?}")]
    InvalidIncidentStatus(String),
    #[error("invalid alert incident {column} timestamp: {value}")]
    InvalidIncidentTimestamp {
        column: &'static str,
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("device is not registered: {0:?}")]
    UnknownDevice(String),
    #[error("tenant is not active: {0}")]
    UnknownTenant(uuid::Uuid),
    #[error("device {device_id:?} belongs to a different tenant: {tenant_id}")]
    DeviceTenantConflict {
        device_id: String,
        tenant_id: uuid::Uuid,
    },
    #[error("invalid gateway ingest: {0}")]
    InvalidGatewayIngest(#[from] GatewayIngestValidationError),
    #[error("device token authentication denied")]
    DeviceTokenDenied,
    #[error("telemetry sequence does not fit PostgreSQL BIGINT")]
    TelemetrySequenceOverflow,
    #[error("telemetry metric key must be a valid identifier: {0:?}")]
    InvalidTelemetryMetricKey(String),
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
    #[error("invalid application redirect URI: {0:?}")]
    InvalidApplicationRedirectUri(String),
    #[error("invalid device last-seen timestamp: {value}")]
    InvalidDeviceLastSeenTimestamp {
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("invalid authorization account class: {0:?}")]
    InvalidAuthorizationAccountClass(String),
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
    #[error("application ID belongs to a different tenant: {0}")]
    ApplicationTenantConflict(ApplicationId),
    #[error("OAuth application is not registered")]
    OAuthApplicationNotFound,
    #[error("OAuth client secret must not be empty")]
    EmptyOAuthClientSecret,
    #[error("OAuth authorization code must not be empty")]
    EmptyOAuthAuthorizationCode,
    #[error("OAuth code challenge must not be empty")]
    EmptyOAuthCodeChallenge,
    #[error("OAuth authorization code expiry must be after issuance")]
    InvalidOAuthAuthorizationCodeExpiry,
    #[error("OAuth access token expiry must be after issuance")]
    InvalidOAuthAccessTokenExpiry,
    #[error("OAuth redirect URI is not registered for the application")]
    OAuthRedirectUriDenied,
    #[error("OAuth requested scope is not allowed")]
    OAuthScopeDenied,
    #[error("OAuth authorization code is invalid")]
    OAuthAuthorizationCodeDenied,
    #[error("OAuth client authentication failed")]
    OAuthClientAuthenticationDenied,
    #[error("OAuth access token is invalid")]
    OAuthAccessTokenDenied,
    #[error("alert rule ID must be a UUID, got {0:?}")]
    InvalidAlertRuleId(String),
    #[error("invalid alert rule kind: {0:?}")]
    InvalidAlertRuleKind(String),
    #[error("invalid alert rule comparison: {0:?}")]
    InvalidAlertRuleComparison(String),
    #[error("invalid alert rule severity: {0:?}")]
    InvalidAlertRuleSeverity(String),
    #[error("invalid alert rule duration for {field}: {seconds}")]
    InvalidAlertRuleDuration { field: &'static str, seconds: i64 },
    #[error("invalid alert rule timestamp for {field}: {value}")]
    InvalidAlertRuleTimestamp {
        field: &'static str,
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("alert rule event sequence does not fit PostgreSQL BIGINT")]
    AlertRuleSequenceOverflow,
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
        if value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(PlatformStoreError::InvalidApplicationRedirectUri(
                value.to_owned(),
            ));
        }
        let parsed = Url::parse(value)
            .map_err(|_| PlatformStoreError::InvalidApplicationRedirectUri(value.to_owned()))?;
        if !matches!(parsed.scheme(), "https" | "http")
            || parsed.host_str().is_none()
            || parsed.fragment().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || redirect_uri_has_userinfo(value)
        {
            return Err(PlatformStoreError::InvalidApplicationRedirectUri(
                value.to_owned(),
            ));
        }
        Ok(Self(value.to_owned()))
    }
}

fn redirect_uri_has_userinfo(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, remainder)| {
        remainder
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewApplication {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
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
    pub tenant_id: uuid::Uuid,
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

pub struct NewOAuthClientSecret {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub client_secret: String,
}

pub struct NewOAuthAuthorizationCode {
    pub code: String,
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub redirect_uri: RedirectUri,
    pub code_challenge: String,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct OAuthAuthorizationCodeExchange {
    pub code: String,
    pub client_id: ClientId,
    pub redirect_uri: RedirectUri,
    pub code_verifier: String,
    pub client_secret: Option<String>,
    pub access_token: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct OAuthClientCredentialsToken {
    pub client_id: ClientId,
    pub client_secret: String,
    pub access_token: String,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthAccessTokenRecord {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub user_id: Option<uuid::Uuid>,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub trait OAuthRepository: Send + Sync {
    fn register_client_secret<'a>(
        &'a self,
        secret: NewOAuthClientSecret,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn issue_authorization_code<'a>(
        &'a self,
        code: NewOAuthAuthorizationCode,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn consume_authorization_code_and_issue_access_token<'a>(
        &'a self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
    fn issue_client_credentials_access_token<'a>(
        &'a self,
        request: OAuthClientCredentialsToken,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
    fn resolve_access_token<'a>(
        &'a self,
        access_token: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
}

pub trait TopologyRepository: Send + Sync {
    fn register_device<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayIngestEventKind {
    Connect,
    Disconnect,
    Heartbeat,
    ChildTelemetry,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GatewayIngestRequest {
    pub tenant_id: uuid::Uuid,
    pub gateway_device_id: String,
    pub child_device_id: Option<String>,
    pub event_kind: GatewayIngestEventKind,
    pub event_at: DateTime<Utc>,
    pub idempotency_key: String,
    pub telemetry_event: Option<TelemetryEvent>,
    pub topic: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayIngestResult {
    pub receipt_inserted: bool,
    pub telemetry_inserted: bool,
}

pub trait GatewayIngestRepository: Send + Sync {
    fn ingest_gateway<'a>(
        &'a self,
        request: GatewayIngestRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayIngestResult, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedDeviceToken {
    pub token_id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
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
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn authorize_gateway_token<'a>(
        &'a self,
        token_id: uuid::Uuid,
        tenant_id: uuid::Uuid,
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
    fn mark_legacy_command_failed<'a>(
        &'a self,
        command_id: &'a str,
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
        tenant_id: uuid::Uuid,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TelemetryAggregate {
    pub average: f64,
    pub sample_count: u64,
}

pub trait TelemetryAggregateRepository: Send + Sync {
    fn average_metric<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        metric_key: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TelemetryAggregate>, PlatformStoreError>> + Send + 'a,
        >,
    >;
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertRule {
    pub id: uuid::Uuid,
    pub name: String,
    pub enabled: bool,
    pub kind: AlertRuleKind,
    pub device_id: Option<String>,
    pub metric_key: String,
    pub comparison: AlertComparison,
    pub threshold: f64,
    pub window: Option<ChronoDuration>,
    pub for_duration: ChronoDuration,
    pub resolve_after: ChronoDuration,
    pub reopen_grace: ChronoDuration,
    pub hysteresis: Option<f64>,
    pub severity: AlertSeverity,
    pub reminder_interval: ChronoDuration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertRuleKind {
    EventThreshold,
    WindowAverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertEvaluationEvent {
    pub event_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub device_id: String,
    pub boot_id: uuid::Uuid,
    pub sequence: u64,
    pub measurements: serde_json::Map<String, serde_json::Value>,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlertEvaluationResult {
    pub evaluated: usize,
    pub opened: usize,
    pub resolved: usize,
    pub reminders: usize,
}

pub trait AlertEvaluationRepository: Send + Sync {
    fn evaluate_alert_events<'a>(
        &'a self,
        events: &'a [AlertEvaluationEvent],
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>;

    fn evaluate_alert_windows<'a>(
        &'a self,
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>;
}

pub trait AlertRepository: Send + Sync {
    fn load_active_rules<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AlertRule>, PlatformStoreError>> + Send + 'a>>;
    fn claim_rule_event<'a>(
        &'a self,
        rule_id: uuid::Uuid,
        event_at: DateTime<Utc>,
        device_id: &'a str,
        boot_id: uuid::Uuid,
        sequence: u64,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertIncidentStatus {
    Pending,
    Open,
    Resolved,
}

impl AlertIncidentStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "open" => Ok(Self::Open),
            "resolved" => Ok(Self::Resolved),
            _ => Err(PlatformStoreError::InvalidIncidentStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAlertIncident {
    pub id: uuid::Uuid,
    pub rule_id: uuid::Uuid,
    pub device_id: String,
    pub status: AlertIncidentStatus,
    pub condition_started_at: DateTime<Utc>,
    pub last_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertIncident {
    pub id: uuid::Uuid,
    pub rule_id: uuid::Uuid,
    pub device_id: String,
    pub status: AlertIncidentStatus,
    pub condition_started_at: DateTime<Utc>,
    pub recovery_started_at: Option<DateTime<Utc>>,
    pub opened_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub last_value: Option<f64>,
    pub last_notified_at: Option<DateTime<Utc>>,
    pub last_reminder_at: Option<DateTime<Utc>>,
    pub state_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNotificationOutboxEntry {
    pub id: uuid::Uuid,
    pub kind: NotificationKind,
    pub dedupe_key: String,
    pub subject: String,
    pub body: String,
    pub next_attempt_at: DateTime<Utc>,
}

pub trait AlertIncidentRepository: Send + Sync {
    fn create_incident<'a>(
        &'a self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn update_incident_last_value<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn open_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn open_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn recover_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn resolve_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn resolve_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn remind_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn remind_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn enqueue_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<
        Box<dyn Future<Output = Result<NotificationOutboxRecord, PlatformStoreError>> + Send + 'a>,
    >;
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

fn authorization_account_class(value: &str) -> Result<AccountClass, PlatformStoreError> {
    match value {
        "system" => Ok(AccountClass::System),
        "admin" => Ok(AccountClass::Admin),
        "user" => Ok(AccountClass::User),
        _ => Err(PlatformStoreError::InvalidAuthorizationAccountClass(
            value.to_owned(),
        )),
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

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedDeviceSummary {
    pub device_id: String,
    pub display_name: Option<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

pub trait AuthorizationRepository: Send + Sync {
    fn authorization_subject<'a>(
        &'a self,
        user_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizationSubject>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn list_authorized_devices<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn authorized_device<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
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

    /// Creates a coherent backup before a monolith upgrades an existing SQLite schema.
    pub async fn backup_sqlite_before_migration(
        configuration: &StorageConfiguration,
    ) -> Result<Option<PathBuf>, PlatformStoreError> {
        if configuration.storage != DatabaseStorage::Sqlite {
            return Ok(None);
        }
        let path = configuration
            .sqlite_path
            .as_ref()
            .ok_or(PlatformStoreError::InvalidConfiguration)?;
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SqliteStoreError::Filesystem(error).into()),
        };
        if !metadata.is_file() {
            return Err(SqliteStoreError::InvalidConfiguration.into());
        }
        if metadata.len() == 0 {
            return Ok(None);
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_backup_connect_options(
                path,
                configuration.sqlite_busy_timeout_ms,
            ))
            .await?;
        let schema_version = sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        let backup = if schema_version < SQLITE_PLATFORM_SCHEMA_VERSION {
            match existing_pre_migration_backup(path, schema_version)? {
                Some(path) => Some(path),
                None => Some(
                    backup_sqlite_pool_with_prefix(
                        &pool,
                        path,
                        &format!("backup-v{schema_version}"),
                    )
                    .await?,
                ),
            }
        } else {
            None
        };
        pool.close().await;
        Ok(backup)
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

    pub async fn load_active_alert_rules(&self) -> Result<Vec<AlertRule>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison,
                            threshold, window_seconds, for_seconds, resolve_after_seconds,
                            reopen_grace_seconds, hysteresis, severity, reminder_interval_seconds
                     FROM alert_rules
                     WHERE enabled = 1 AND archived_at IS NULL
                     ORDER BY created_at DESC, id",
                )
                .fetch_all(store.pool())
                .await?;
                rows.into_iter().map(sqlite_alert_rule_record).collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison,
                            threshold, window_seconds, for_seconds, resolve_after_seconds,
                            reopen_grace_seconds, hysteresis, severity, reminder_interval_seconds
                     FROM alert_rules
                     WHERE enabled AND archived_at IS NULL
                     ORDER BY created_at DESC, id",
                )
                .fetch_all(pool)
                .await?;
                rows.into_iter().map(postgres_alert_rule_record).collect()
            }
        }
    }

    pub async fn claim_alert_rule_event(
        &self,
        rule_id: uuid::Uuid,
        event_at: DateTime<Utc>,
        device_id: &str,
        boot_id: uuid::Uuid,
        sequence: u64,
    ) -> Result<bool, PlatformStoreError> {
        let event_at = canonical_postgres_timestamp(event_at);
        match self {
            Self::Sqlite(store) => {
                let result = sqlx::query(
                    "INSERT INTO alert_rule_event_evaluations
                        (rule_id, event_at, device_id, boot_id, sequence)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (rule_id, event_at, device_id, boot_id, sequence)
                     DO NOTHING",
                )
                .bind(rule_id.to_string())
                .bind(event_at.to_rfc3339())
                .bind(device_id)
                .bind(boot_id.to_string())
                .bind(sequence.to_string())
                .execute(store.pool())
                .await?;
                Ok(result.rows_affected() == 1)
            }
            Self::Timescale(pool) => {
                let sequence = i64::try_from(sequence)
                    .map_err(|_| PlatformStoreError::AlertRuleSequenceOverflow)?;
                let result = sqlx::query(
                    "INSERT INTO alert_rule_event_evaluations
                        (rule_id, event_at, device_id, boot_id, sequence)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (rule_id, event_at, device_id, boot_id, sequence)
                     DO NOTHING",
                )
                .bind(rule_id)
                .bind(event_at)
                .bind(device_id)
                .bind(boot_id)
                .bind(sequence)
                .execute(pool)
                .await?;
                Ok(result.rows_affected() == 1)
            }
        }
    }

    pub async fn evaluate_alert_events(
        &self,
        events: &[AlertEvaluationEvent],
        _evaluated_at: DateTime<Utc>,
    ) -> Result<AlertEvaluationResult, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => evaluate_sqlite_alert_events(store, events).await,
            Self::Timescale(pool) => evaluate_timescale_alert_events(pool, events).await,
        }
    }

    pub async fn evaluate_alert_windows(
        &self,
        evaluated_at: DateTime<Utc>,
    ) -> Result<AlertEvaluationResult, PlatformStoreError> {
        let evaluated_at = canonical_postgres_timestamp(evaluated_at);
        match self {
            Self::Sqlite(store) => evaluate_sqlite_alert_windows(store, evaluated_at).await,
            Self::Timescale(pool) => evaluate_timescale_alert_windows(pool, evaluated_at).await,
        }
    }

    pub async fn upsert_application(
        &self,
        mut application: NewApplication,
    ) -> Result<ApplicationRecord, PlatformStoreError> {
        validate_application(&mut application)?;
        if let Some(existing) = self
            .find_application_by_app_id(application.app_id.as_str())
            .await?
            && existing.tenant_id != application.tenant_id
        {
            return Err(PlatformStoreError::ApplicationTenantConflict(
                application.app_id.clone(),
            ));
        }
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
                let result = sqlx::query(
                    "INSERT INTO applications (
                        app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = excluded.kind,
                        launch_url = excluded.launch_url,
                        client_id = excluded.client_id,
                        allowed_scopes_json = excluded.allowed_scopes_json,
                        enabled = excluded.enabled
                     WHERE applications.tenant_id = excluded.tenant_id",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
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
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::ApplicationTenantConflict(
                        application.app_id.clone(),
                    ));
                }
                sqlx::query(
                    "DELETE FROM application_redirect_uris WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, tenant_id, redirect_uri)
                         VALUES (?, ?, ?)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(application.tenant_id.to_string())
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
                let result = sqlx::query(
                    "INSERT INTO applications (
                        app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        launch_url = EXCLUDED.launch_url,
                        client_id = EXCLUDED.client_id,
                        allowed_scopes_json = EXCLUDED.allowed_scopes_json,
                        enabled = EXCLUDED.enabled
                     WHERE applications.tenant_id = EXCLUDED.tenant_id",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
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
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::ApplicationTenantConflict(
                        application.app_id.clone(),
                    ));
                }
                sqlx::query(
                    "DELETE FROM application_redirect_uris WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .execute(&mut *transaction)
                .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, tenant_id, redirect_uri)
                         VALUES ($1, $2, $3)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(application.tenant_id)
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
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = ?",
                )
                .bind(app_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let tenant_id: String = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? AND tenant_id = ? ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects).map(Some)
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = $1",
                )
                .bind(app_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 AND tenant_id = $2 ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
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
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = ?",
                )
                .bind(client_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: String = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? AND tenant_id = ? ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .bind(tenant_id)
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects)?
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = $1",
                )
                .bind(client_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 AND tenant_id = $2 ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .bind(tenant_id)
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

    pub async fn register_client_secret(
        &self,
        secret: NewOAuthClientSecret,
    ) -> Result<(), PlatformStoreError> {
        if secret.client_secret.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthClientSecret);
        }
        let application = self
            .find_application_by_app_id(secret.app_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthApplicationNotFound)?;
        if application.tenant_id != secret.tenant_id {
            return Err(PlatformStoreError::OAuthApplicationNotFound);
        }
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        let secret_hash = sha256_hex(&secret.client_secret);
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO oauth_client_secrets (app_id, tenant_id, secret_hash)
                     VALUES (?, ?, ?)
                     ON CONFLICT DO NOTHING",
                )
                .bind(secret.app_id.as_str())
                .bind(secret.tenant_id.to_string())
                .bind(secret_hash)
                .execute(store.pool())
                .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO oauth_client_secrets (app_id, tenant_id, secret_hash)
                     VALUES ($1, $2, $3)
                     ON CONFLICT DO NOTHING",
                )
                .bind(secret.app_id.as_str())
                .bind(secret.tenant_id)
                .bind(secret_hash)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn issue_authorization_code(
        &self,
        code: NewOAuthAuthorizationCode,
    ) -> Result<(), PlatformStoreError> {
        if code.code.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthAuthorizationCode);
        }
        if code.code_challenge.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthCodeChallenge);
        }
        if code.expires_at <= code.issued_at {
            return Err(PlatformStoreError::InvalidOAuthAuthorizationCodeExpiry);
        }
        let scopes = canonical_application_scopes(code.scopes)?;
        let application = self
            .find_application_by_app_id(code.app_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthApplicationNotFound)?;
        if application.tenant_id != code.tenant_id
            || !oauth_user_belongs_to_tenant(self, code.user_id, code.tenant_id).await?
        {
            return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
        }
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        if !application
            .redirect_uris
            .iter()
            .any(|redirect_uri| redirect_uri == &code.redirect_uri)
        {
            return Err(PlatformStoreError::OAuthRedirectUriDenied);
        }
        if !scopes
            .iter()
            .all(|scope| application.allowed_scopes.binary_search(scope).is_ok())
        {
            return Err(PlatformStoreError::OAuthScopeDenied);
        }

        let code_hash = sha256_hex(&code.code);
        let scopes_json = serde_json::to_string(&scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO oauth_authorization_codes (
                        code_hash, app_id, tenant_id, user_id, redirect_uri, code_challenge, scopes_json,
                        issued_at, expires_at, consumed_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(code_hash)
                .bind(code.app_id.as_str())
                .bind(code.tenant_id.to_string())
                .bind(code.user_id.to_string())
                .bind(code.redirect_uri.as_str())
                .bind(code.code_challenge)
                .bind(scopes_json)
                .bind(code.issued_at.to_rfc3339())
                .bind(code.expires_at.to_rfc3339())
                .execute(store.pool())
                .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO oauth_authorization_codes (
                        code_hash, app_id, tenant_id, user_id, redirect_uri, code_challenge, scopes_json,
                        issued_at, expires_at, consumed_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, NULL)",
                )
                .bind(code_hash)
                .bind(code.app_id.as_str())
                .bind(code.tenant_id)
                .bind(code.user_id)
                .bind(code.redirect_uri.as_str())
                .bind(code.code_challenge)
                .bind(scopes_json)
                .bind(code.issued_at)
                .bind(code.expires_at)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn consume_authorization_code_and_issue_access_token(
        &self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        validate_oauth_access_token_expiry(exchange.issued_at, exchange.expires_at)?;
        let application = self
            .find_application_by_client_id(exchange.client_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthAuthorizationCodeDenied)?;
        let code_hash = sha256_hex(&exchange.code);
        let code_challenge = s256_code_challenge(&exchange.code_verifier);
        let token_hash = sha256_hex(&exchange.access_token);
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, user_id, scopes_json
                     FROM oauth_authorization_codes
                     WHERE code_hash = ? AND redirect_uri = ?
                       AND code_challenge = ?
                       AND app_id = ? AND tenant_id = ?
                       AND consumed_at IS NULL AND expires_at > ?",
                )
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .bind(exchange.issued_at.to_rfc3339())
                .fetch_optional(&mut *transaction)
                .await?;
                let Some(row) = row else {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: String = row.try_get("tenant_id")?;
                let user_id: String = row.try_get("user_id")?;
                let scopes_json: String = row.try_get("scopes_json")?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(&app_id)
                .bind(&tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if !oauth_client_secret_matches(&secret_hashes, exchange.client_secret.as_deref()) {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let record = oauth_access_token_record(
                    app_id,
                    tenant_id,
                    user_id,
                    &scopes_json,
                    exchange.issued_at,
                    exchange.expires_at,
                )?;
                let consumed = sqlx::query(
                    "UPDATE oauth_authorization_codes
                       SET consumed_at = ?
                     WHERE code_hash = ? AND redirect_uri = ?
                       AND code_challenge = ?
                       AND app_id = ? AND tenant_id = ?
                       AND consumed_at IS NULL AND expires_at > ?",
                )
                .bind(exchange.issued_at.to_rfc3339())
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id.to_string())
                .bind(exchange.issued_at.to_rfc3339())
                .execute(&mut *transaction)
                .await?;
                if consumed.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                }
                sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(token_hash)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id.to_string())
                .bind(
                    record
                        .user_id
                        .expect("authorization code user ID is present")
                        .to_string(),
                )
                .bind(scopes_json)
                .bind(exchange.issued_at.to_rfc3339())
                .bind(exchange.expires_at.to_rfc3339())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(record)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, user_id, scopes_json::text AS scopes_json
                     FROM oauth_authorization_codes
                     WHERE code_hash = $1 AND redirect_uri = $2
                       AND code_challenge = $3
                       AND app_id = $4 AND tenant_id = $5
                       AND consumed_at IS NULL AND expires_at > $6
                     FOR UPDATE",
                )
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .bind(exchange.issued_at)
                .fetch_optional(&mut *transaction)
                .await?;
                let Some(row) = row else {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let user_id: uuid::Uuid = row.try_get("user_id")?;
                let scopes_json: String = row.try_get("scopes_json")?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(&app_id)
                .bind(tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if !oauth_client_secret_matches(&secret_hashes, exchange.client_secret.as_deref()) {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let record = oauth_access_token_record(
                    app_id,
                    tenant_id.to_string(),
                    user_id.to_string(),
                    &scopes_json,
                    exchange.issued_at,
                    exchange.expires_at,
                )?;
                let consumed = sqlx::query(
                    "UPDATE oauth_authorization_codes
                     SET consumed_at = $1
                     WHERE code_hash = $2 AND redirect_uri = $3
                       AND code_challenge = $4
                       AND app_id = $5 AND tenant_id = $6
                       AND consumed_at IS NULL AND expires_at > $7",
                )
                .bind(exchange.issued_at)
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id)
                .bind(exchange.issued_at)
                .execute(&mut *transaction)
                .await?;
                if consumed.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                }
                sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)",
                )
                .bind(token_hash)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id)
                .bind(
                    record
                        .user_id
                        .expect("authorization code user ID is present"),
                )
                .bind(scopes_json)
                .bind(exchange.issued_at)
                .bind(exchange.expires_at)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(record)
            }
        }
    }

    pub async fn issue_client_credentials_access_token(
        &self,
        request: OAuthClientCredentialsToken,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        validate_oauth_access_token_expiry(request.issued_at, request.expires_at)?;
        let application = self
            .find_application_by_client_id(request.client_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthClientAuthenticationDenied)?;
        let scopes = canonical_application_scopes(request.scopes)?;
        if !scopes
            .iter()
            .all(|scope| application.allowed_scopes.binary_search(scope).is_ok())
        {
            return Err(PlatformStoreError::OAuthScopeDenied);
        }
        let token_hash = sha256_hex(&request.access_token);
        let scopes_json = serde_json::to_string(&scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        let record = OAuthAccessTokenRecord {
            app_id: application.app_id.clone(),
            tenant_id: application.tenant_id,
            user_id: None,
            scopes,
            issued_at: request.issued_at,
            expires_at: request.expires_at,
        };
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .fetch_all(&mut *transaction)
                .await?;
                if secret_hashes.is_empty()
                    || !oauth_client_secret_matches(&secret_hashes, Some(&request.client_secret))
                {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let inserted = sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     )
                     SELECT ?, app_id, tenant_id, NULL, ?, ?, ?
                     FROM applications
                     WHERE app_id = ? AND tenant_id = ? AND client_id = ? AND enabled = 1",
                )
                .bind(token_hash)
                .bind(scopes_json)
                .bind(request.issued_at.to_rfc3339())
                .bind(request.expires_at.to_rfc3339())
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .bind(request.client_id.as_str())
                .execute(&mut *transaction)
                .await?;
                if inserted.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                transaction.commit().await?;
                Ok(record)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if secret_hashes.is_empty()
                    || !oauth_client_secret_matches(&secret_hashes, Some(&request.client_secret))
                {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let inserted = sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     )
                     SELECT $1, app_id, tenant_id, NULL, $2::jsonb, $3, $4
                     FROM applications
                     WHERE app_id = $5 AND tenant_id = $6 AND client_id = $7 AND enabled = TRUE",
                )
                .bind(token_hash)
                .bind(scopes_json)
                .bind(request.issued_at)
                .bind(request.expires_at)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .bind(request.client_id.as_str())
                .execute(&mut *transaction)
                .await?;
                if inserted.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                transaction.commit().await?;
                Ok(record)
            }
        }
    }

    pub async fn resolve_access_token(
        &self,
        access_token: &str,
        now: DateTime<Utc>,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        let token_hash = sha256_hex(access_token);
        match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT token.app_id, token.tenant_id, token.user_id, token.scopes_json,
                            token.issued_at, token.expires_at
                     FROM oauth_access_tokens AS token
                     JOIN applications AS app
                       ON app.app_id = token.app_id AND app.tenant_id = token.tenant_id
                     WHERE token.token_hash = ? AND token.expires_at > ? AND app.enabled = 1",
                )
                .bind(token_hash)
                .bind(now.to_rfc3339())
                .fetch_optional(store.pool())
                .await?
                .ok_or(PlatformStoreError::OAuthAccessTokenDenied)?;
                oauth_resolved_access_token_record(
                    row.try_get("app_id")?,
                    row.try_get("tenant_id")?,
                    row.try_get("user_id")?,
                    &row.try_get::<String, _>("scopes_json")?,
                    parse_oauth_timestamp(&row.try_get::<String, _>("issued_at")?)?,
                    parse_oauth_timestamp(&row.try_get::<String, _>("expires_at")?)?,
                )
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT token.app_id, token.tenant_id, token.user_id,
                            token.scopes_json::text AS scopes_json,
                            token.issued_at, token.expires_at
                     FROM oauth_access_tokens AS token
                     JOIN applications AS app
                       ON app.app_id = token.app_id AND app.tenant_id = token.tenant_id
                     WHERE token.token_hash = $1 AND token.expires_at > $2 AND app.enabled = TRUE",
                )
                .bind(token_hash)
                .bind(now)
                .fetch_optional(pool)
                .await?
                .ok_or(PlatformStoreError::OAuthAccessTokenDenied)?;
                oauth_resolved_access_token_record(
                    row.try_get("app_id")?,
                    row.try_get::<uuid::Uuid, _>("tenant_id")?.to_string(),
                    row.try_get::<Option<uuid::Uuid>, _>("user_id")?
                        .map(|user_id| user_id.to_string()),
                    &row.try_get::<String, _>("scopes_json")?,
                    row.try_get("issued_at")?,
                    row.try_get("expires_at")?,
                )
            }
        }
    }

    pub async fn register_device(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let registered = sqlx::query(
                    "INSERT INTO devices (device_id, tenant_id)
                     SELECT ?, id
                     FROM tenants
                     WHERE id = ? AND status = 'active'
                     ON CONFLICT(device_id) DO UPDATE SET device_id = excluded.device_id
                     WHERE devices.tenant_id = excluded.tenant_id",
                )
                .bind(device_id)
                .bind(tenant_id.to_string())
                .execute(store.pool())
                .await?;
                if registered.rows_affected() != 1 {
                    let tenant_is_active = sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM tenants WHERE id = ? AND status = 'active' LIMIT 1",
                    )
                    .bind(tenant_id.to_string())
                    .fetch_optional(store.pool())
                    .await?
                    .is_some();
                    if !tenant_is_active {
                        return Err(PlatformStoreError::UnknownTenant(tenant_id));
                    }
                    return Err(PlatformStoreError::DeviceTenantConflict {
                        device_id: device_id.to_owned(),
                        tenant_id,
                    });
                }
            }
            Self::Timescale(pool) => {
                let registered = sqlx::query(
                    "INSERT INTO devices (device_id, tenant_id)
                     SELECT $1, id
                     FROM tenants
                     WHERE id = $2 AND status = 'active'
                     FOR UPDATE OF tenants
                     ON CONFLICT(device_id) DO UPDATE SET device_id = EXCLUDED.device_id
                     WHERE devices.tenant_id = EXCLUDED.tenant_id",
                )
                .bind(device_id)
                .bind(tenant_id)
                .execute(pool)
                .await?;
                if registered.rows_affected() != 1 {
                    let tenant_is_active = sqlx::query_scalar::<_, i32>(
                        "SELECT 1 FROM tenants WHERE id = $1 AND status = 'active' LIMIT 1",
                    )
                    .bind(tenant_id)
                    .fetch_optional(pool)
                    .await?
                    .is_some();
                    if !tenant_is_active {
                        return Err(PlatformStoreError::UnknownTenant(tenant_id));
                    }
                    return Err(PlatformStoreError::DeviceTenantConflict {
                        device_id: device_id.to_owned(),
                        tenant_id,
                    });
                }
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
                            devices.tenant_id, devices.is_gateway, devices.gateway_device_id
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     JOIN tenants ON tenants.id = devices.tenant_id AND tenants.status = 'active'
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
                    tenant_id: uuid::Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)
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
                            devices.tenant_id, devices.is_gateway, devices.gateway_device_id
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     JOIN tenants ON tenants.id = devices.tenant_id AND tenants.status = 'active'
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
                    tenant_id: row.try_get("tenant_id")?,
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
        tenant_id: uuid::Uuid,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        let authorized = match self {
            Self::Sqlite(store) => sqlx::query_scalar::<_, i64>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     JOIN tenants ON tenants.id = devices.tenant_id
                     WHERE device_tokens.id = ?
                       AND device_tokens.device_id = ?
                       AND devices.tenant_id = ?
                       AND tenants.status = 'active'
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL
                       AND devices.gateway_device_id IS NULL",
            )
            .bind(token_id.to_string())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .is_some(),
            Self::Timescale(pool) => sqlx::query_scalar::<_, i32>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices ON devices.device_id = device_tokens.device_id
                     JOIN tenants ON tenants.id = devices.tenant_id
                     WHERE device_tokens.id = $1
                       AND device_tokens.device_id = $2
                       AND devices.tenant_id = $3
                       AND tenants.status = 'active'
                       AND device_tokens.revoked_at IS NULL
                       AND devices.deleted_at IS NULL
                       AND devices.gateway_device_id IS NULL",
            )
            .bind(token_id)
            .bind(device_id)
            .bind(tenant_id)
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
        tenant_id: uuid::Uuid,
        gateway_device_id: &str,
        child_device_id: Option<&str>,
    ) -> Result<(), PlatformStoreError> {
        let authorized = match self {
            Self::Sqlite(store) => sqlx::query_scalar::<_, i64>(
                "SELECT 1
                     FROM device_tokens
                     JOIN devices AS gateways
                       ON gateways.device_id = device_tokens.device_id
                     JOIN tenants ON tenants.id = gateways.tenant_id
                     WHERE device_tokens.id = ?
                       AND device_tokens.device_id = ?
                       AND gateways.tenant_id = ?
                       AND tenants.status = 'active'
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
                                 AND children.tenant_id = gateways.tenant_id
                                 AND children.deleted_at IS NULL
                           )
                       )",
            )
            .bind(token_id.to_string())
            .bind(gateway_device_id)
            .bind(tenant_id.to_string())
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
                     JOIN tenants ON tenants.id = gateways.tenant_id
                     WHERE device_tokens.id = $1
                       AND device_tokens.device_id = $2
                       AND gateways.tenant_id = $3
                       AND tenants.status = 'active'
                       AND device_tokens.revoked_at IS NULL
                       AND gateways.deleted_at IS NULL
                       AND gateways.is_gateway = TRUE
                       AND (
                           $4 IS NULL
                           OR EXISTS (
                               SELECT 1
                               FROM devices AS children
                               WHERE children.device_id = $4
                                 AND children.gateway_device_id = gateways.device_id
                                 AND children.tenant_id = gateways.tenant_id
                                 AND children.deleted_at IS NULL
                           )
                       )",
            )
            .bind(token_id)
            .bind(gateway_device_id)
            .bind(tenant_id)
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

    pub async fn authorization_subject(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Option<AuthorizationSubject>, PlatformStoreError> {
        let account_class = match self {
            Self::Sqlite(store) => {
                sqlx::query_scalar::<_, String>("SELECT account_class FROM users WHERE id = ?")
                    .bind(user_id.to_string())
                    .fetch_optional(store.pool())
                    .await?
            }
            Self::Timescale(pool) => {
                sqlx::query_scalar::<_, String>("SELECT account_class FROM users WHERE id = $1")
                    .bind(user_id)
                    .fetch_optional(pool)
                    .await?
            }
        };
        account_class
            .map(|account_class| authorization_account_class(&account_class))
            .transpose()
            .map(|account_class| {
                account_class.map(|account_class| AuthorizationSubject {
                    user_id,
                    account_class,
                })
            })
    }

    pub async fn list_authorized_devices(
        &self,
        subject: &AuthorizationSubject,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AuthorizedDeviceSummary>, PlatformStoreError> {
        let limit = i64::from(limit);
        match self {
            Self::Sqlite(store) => {
                let user_id = subject.user_id.to_string();
                let rows = sqlx::query(
                    "SELECT d.device_id, d.display_name, d.last_seen_at
                     FROM devices AS d
                     WHERE d.deleted_at IS NULL
                       AND (? IS NULL OR d.device_id > ?)
                       AND (
                           ? = 1
                           OR d.owner_user_id = ?
                           OR EXISTS (
                               SELECT 1 FROM resource_shares AS shares
                               WHERE shares.resource_type = 'device'
                                 AND shares.resource_id = d.device_id
                                 AND shares.target_user_id = ?
                                 AND shares.state = 'active'
                           )
                           OR EXISTS (
                               WITH RECURSIVE ancestors(id, depth) AS (
                                   SELECT d.asset_id, 0 WHERE d.asset_id IS NOT NULL
                                   UNION ALL
                                   SELECT assets.parent_asset_id, ancestors.depth + 1
                                   FROM ancestors
                                   JOIN assets ON assets.id = ancestors.id
                                   WHERE assets.parent_asset_id IS NOT NULL
                                     AND ancestors.depth < 64
                               )
                               SELECT 1 FROM resource_shares AS shares
                               JOIN ancestors ON shares.resource_id = ancestors.id
                               WHERE shares.resource_type = 'asset'
                                 AND shares.target_user_id = ?
                                 AND shares.state = 'active'
                                 AND shares.inherit_children = 1
                           )
                       )
                     ORDER BY d.device_id
                     LIMIT ?",
                )
                .bind(after)
                .bind(after)
                .bind(i64::from(subject.account_class == AccountClass::Admin))
                .bind(&user_id)
                .bind(&user_id)
                .bind(&user_id)
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                rows.into_iter()
                    .map(|row| {
                        let last_seen_at = row
                            .try_get::<Option<String>, _>("last_seen_at")?
                            .map(|value| parse_authorized_device_timestamp(&value))
                            .transpose()?;
                        Ok(AuthorizedDeviceSummary {
                            device_id: row.try_get("device_id")?,
                            display_name: row.try_get("display_name")?,
                            last_seen_at,
                        })
                    })
                    .collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT d.device_id, d.display_name, runtime.last_seen_at
                     FROM devices AS d
                     LEFT JOIN device_runtime_state AS runtime
                       ON runtime.device_id = d.device_id
                     WHERE d.deleted_at IS NULL
                       AND ($1::text IS NULL OR d.device_id > $1)
                       AND (
                           $2::boolean
                           OR d.owner_user_id = $3
                           OR EXISTS (
                               SELECT 1 FROM resource_shares AS shares
                               WHERE shares.resource_type = 'device'
                                 AND shares.resource_id = d.device_id
                                 AND shares.target_user_id = $3
                                 AND shares.state = 'active'
                           )
                           OR EXISTS (
                               WITH RECURSIVE ancestors(id, depth) AS (
                                   SELECT d.asset_id, 0 WHERE d.asset_id IS NOT NULL
                                   UNION ALL
                                   SELECT assets.parent_asset_id, ancestors.depth + 1
                                   FROM ancestors
                                   JOIN assets ON assets.id = ancestors.id
                                   WHERE assets.parent_asset_id IS NOT NULL
                                     AND ancestors.depth < 64
                               )
                               SELECT 1 FROM resource_shares AS shares
                               JOIN ancestors ON shares.resource_id = ancestors.id::text
                               WHERE shares.resource_type = 'asset'
                                 AND shares.target_user_id = $3
                                 AND shares.state = 'active'
                                 AND shares.inherit_children = TRUE
                           )
                       )
                     ORDER BY d.device_id
                     LIMIT $4",
                )
                .bind(after)
                .bind(subject.account_class == AccountClass::Admin)
                .bind(subject.user_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(|row| {
                        Ok(AuthorizedDeviceSummary {
                            device_id: row.try_get("device_id")?,
                            display_name: row.try_get("display_name")?,
                            last_seen_at: row.try_get("last_seen_at")?,
                        })
                    })
                    .collect()
            }
        }
    }

    pub async fn authorized_device(
        &self,
        subject: &AuthorizationSubject,
        device_id: &str,
    ) -> Result<Option<AuthorizedDeviceSummary>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let user_id = subject.user_id.to_string();
                let row = sqlx::query(
                    "SELECT d.device_id, d.display_name, d.last_seen_at
                     FROM devices AS d
                     WHERE d.device_id = ?
                       AND d.deleted_at IS NULL
                       AND (
                           ? = 1
                           OR d.owner_user_id = ?
                           OR EXISTS (
                               SELECT 1 FROM resource_shares AS shares
                               WHERE shares.resource_type = 'device'
                                 AND shares.resource_id = d.device_id
                                 AND shares.target_user_id = ?
                                 AND shares.state = 'active'
                           )
                           OR EXISTS (
                               WITH RECURSIVE ancestors(id, depth) AS (
                                   SELECT d.asset_id, 0 WHERE d.asset_id IS NOT NULL
                                   UNION ALL
                                   SELECT assets.parent_asset_id, ancestors.depth + 1
                                   FROM ancestors
                                   JOIN assets ON assets.id = ancestors.id
                                   WHERE assets.parent_asset_id IS NOT NULL
                                     AND ancestors.depth < 64
                               )
                               SELECT 1 FROM resource_shares AS shares
                               JOIN ancestors ON shares.resource_id = ancestors.id
                               WHERE shares.resource_type = 'asset'
                                 AND shares.target_user_id = ?
                                 AND shares.state = 'active'
                                 AND shares.inherit_children = 1
                           )
                       )",
                )
                .bind(device_id)
                .bind(i64::from(subject.account_class == AccountClass::Admin))
                .bind(&user_id)
                .bind(&user_id)
                .bind(&user_id)
                .fetch_optional(store.pool())
                .await?;
                row.map(|row| {
                    let last_seen_at = row
                        .try_get::<Option<String>, _>("last_seen_at")?
                        .map(|value| parse_authorized_device_timestamp(&value))
                        .transpose()?;
                    Ok(AuthorizedDeviceSummary {
                        device_id: row.try_get("device_id")?,
                        display_name: row.try_get("display_name")?,
                        last_seen_at,
                    })
                })
                .transpose()
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT d.device_id, d.display_name, runtime.last_seen_at
                     FROM devices AS d
                     LEFT JOIN device_runtime_state AS runtime
                       ON runtime.device_id = d.device_id
                     WHERE d.device_id = $1
                       AND d.deleted_at IS NULL
                       AND (
                           $2::boolean
                           OR d.owner_user_id = $3
                           OR EXISTS (
                               SELECT 1 FROM resource_shares AS shares
                               WHERE shares.resource_type = 'device'
                                 AND shares.resource_id = d.device_id
                                 AND shares.target_user_id = $3
                                 AND shares.state = 'active'
                           )
                           OR EXISTS (
                               WITH RECURSIVE ancestors(id, depth) AS (
                                   SELECT d.asset_id, 0 WHERE d.asset_id IS NOT NULL
                                   UNION ALL
                                   SELECT assets.parent_asset_id, ancestors.depth + 1
                                   FROM ancestors
                                   JOIN assets ON assets.id = ancestors.id
                                   WHERE assets.parent_asset_id IS NOT NULL
                                     AND ancestors.depth < 64
                               )
                               SELECT 1 FROM resource_shares AS shares
                               JOIN ancestors ON shares.resource_id = ancestors.id::text
                               WHERE shares.resource_type = 'asset'
                                 AND shares.target_user_id = $3
                                 AND shares.state = 'active'
                                 AND shares.inherit_children = TRUE
                           )
                       )",
                )
                .bind(device_id)
                .bind(subject.account_class == AccountClass::Admin)
                .bind(subject.user_id)
                .fetch_optional(pool)
                .await?;
                row.map(|row| {
                    Ok(AuthorizedDeviceSummary {
                        device_id: row.try_get("device_id")?,
                        display_name: row.try_get("display_name")?,
                        last_seen_at: row.try_get("last_seen_at")?,
                    })
                })
                .transpose()
            }
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
        command.expires_at = canonical_postgres_timestamp(command.expires_at);
        command.next_attempt_at = canonical_postgres_timestamp(command.next_attempt_at);

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

    pub async fn mark_legacy_command_failed(
        &self,
        command_id: &str,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store.mark_command_failed(command_id, error).await?),
            Self::Timescale(pool) => {
                let command_id = uuid::Uuid::parse_str(command_id)
                    .map_err(|_| PlatformStoreError::InvalidCommandId(command_id.to_owned()))?;
                mark_timescale_command_failed(pool, command_id, error).await
            }
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
                        id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                        id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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

    pub async fn create_incident(
        &self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let incident = canonical_incident(incident);
        let opened_notification = opened_notification.map(canonical_notification);
        match self {
            Self::Sqlite(store) => Ok(store.create_incident(incident, opened_notification).await?),
            Self::Timescale(pool) => {
                create_timescale_incident(pool, incident, opened_notification).await
            }
        }
    }

    pub async fn update_incident_last_value(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_last_value(
                    &incident_id.to_string(),
                    expected_version,
                    last_value,
                    canonical_postgres_timestamp(updated_at),
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_last_value(
                    pool,
                    incident_id,
                    expected_version,
                    last_value,
                    canonical_postgres_timestamp(updated_at),
                )
                .await
            }
        }
    }

    pub async fn open_incident(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
        )
        .await
    }

    pub async fn open_incident_with_notification(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn recover_incident(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            incident_id,
            expected_version,
            AlertIncidentTransition::Recover(canonical_postgres_timestamp(recovery_started_at)),
        )
        .await
    }

    pub async fn resolve_incident(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
        )
        .await
    }

    pub async fn resolve_incident_with_notification(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn remind_incident(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
        )
        .await
    }

    pub async fn remind_incident_with_notification(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
            canonical_notification(notification),
        )
        .await
    }

    async fn update_incident_transition(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition(&incident_id.to_string(), expected_version, transition)
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition(
                    pool,
                    incident_id,
                    expected_version,
                    transition,
                )
                .await
            }
        }
    }

    async fn update_incident_transition_with_notification(
        &self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition_with_notification(
                    &incident_id.to_string(),
                    expected_version,
                    transition,
                    notification,
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition_with_notification(
                    pool,
                    incident_id,
                    expected_version,
                    transition,
                    notification,
                )
                .await
            }
        }
    }

    pub async fn enqueue_notification(
        &self,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        let notification = canonical_notification(notification);
        match self {
            Self::Sqlite(store) => Ok(store
                .enqueue_notification(&incident_id.to_string(), notification)
                .await?),
            Self::Timescale(pool) => {
                enqueue_timescale_notification(pool, incident_id, notification).await
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
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, PlatformStoreError> {
        let sequence = i64::try_from(event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;

        match self {
            Self::Sqlite(store) => {
                self.require_sqlite_tenant_device(tenant_id, &event.device_id)
                    .await?;
                Ok(store
                    .write_telemetry(tenant_id, event, received_at, topic)
                    .await?)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                if !timescale_tenant_device_is_locked(&mut transaction, tenant_id, &event.device_id)
                    .await?
                {
                    return Err(PlatformStoreError::UnknownDevice(event.device_id.clone()));
                }
                sqlx::query(
                    "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET last_seen_at = GREATEST(
                         COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                         EXCLUDED.last_seen_at
                     )",
                )
                .bind(tenant_id)
                .bind(&event.device_id)
                .bind(received_at)
                .execute(&mut *transaction)
                .await?;

                let result = sqlx::query(
                    "INSERT INTO telemetry (
                        event_at, received_at, tenant_id, device_id, boot_id, sequence,
                        measurements, topic, gateway_device_id
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (tenant_id, event_at, device_id, boot_id, sequence) DO NOTHING",
                )
                .bind(event.event_at)
                .bind(received_at)
                .bind(tenant_id)
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

    pub async fn ingest_gateway(
        &self,
        request: GatewayIngestRequest,
    ) -> Result<GatewayIngestResult, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => ingest_sqlite_gateway(store, request).await,
            Self::Timescale(pool) => ingest_timescale_gateway(pool, request).await,
        }
    }

    pub async fn average_metric(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
        metric_key: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Option<TelemetryAggregate>, PlatformStoreError> {
        validate_telemetry_metric_key(metric_key)?;
        let from = canonical_postgres_timestamp(from);
        let to = canonical_postgres_timestamp(to);
        match self {
            Self::Sqlite(store) => {
                let path = format!("$.{metric_key}");
                let row = sqlx::query(
                    "WITH canonical_telemetry AS (
                        SELECT measurements,
                               CAST(unixepoch(event_at) AS INTEGER) * 1000000
                               + CASE
                                   WHEN instr(event_at, '.') = 0 THEN 0
                                   ELSE CAST(
                                       substr(
                                           substr(
                                               event_at,
                                               instr(event_at, '.') + 1,
                                               CASE
                                                   WHEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       'Z'
                                                   ) > 0
                                                   THEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       'Z'
                                                   ) - 1
                                                   WHEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '+'
                                                   ) > 0
                                                   THEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '+'
                                                   ) - 1
                                                   ELSE instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '-'
                                                   ) - 1
                                               END
                                           ) || '000000',
                                           1,
                                           6
                                       ) AS INTEGER
                                   )
                        END AS event_at_micros
                        FROM telemetry
                        WHERE tenant_id = ? AND device_id = ?
                     )
                     SELECT AVG(json_extract(measurements, ?)) AS average,
                            COUNT(*) AS sample_count
                     FROM canonical_telemetry
                     WHERE event_at_micros >= ?
                       AND event_at_micros <= ?
                       AND json_type(measurements, ?) IN ('integer', 'real')
                       AND json_extract(measurements, ?) > -1.0e999
                       AND json_extract(measurements, ?) < 1.0e999",
                )
                .bind(tenant_id.to_string())
                .bind(device_id)
                .bind(&path)
                .bind(from.timestamp_micros())
                .bind(to.timestamp_micros())
                .bind(&path)
                .bind(&path)
                .bind(&path)
                .fetch_one(store.pool())
                .await?;
                let sample_count = row.get::<i64, _>("sample_count");
                if sample_count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(TelemetryAggregate {
                        average: row.get("average"),
                        sample_count: u64::try_from(sample_count)
                            .expect("SQLite COUNT(*) is nonnegative"),
                    }))
                }
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "WITH finite_telemetry AS (
                         SELECT CASE
                                    WHEN jsonb_typeof(measurements -> $1) = 'number'
                                    THEN CASE
                                        WHEN (measurements ->> $1)::numeric BETWEEN
                                                 '-1.7976931348623157e308'::numeric
                                             AND '1.7976931348623157e308'::numeric
                                        THEN (measurements ->> $1)::numeric
                                    END
                                END AS finite_value
                         FROM telemetry
                         WHERE tenant_id = $2
                           AND device_id = $3
                           AND event_at >= $4
                           AND event_at <= $5
                     )
                     SELECT (AVG(finite_value))::double precision AS average,
                            COUNT(finite_value) AS sample_count
                     FROM finite_telemetry",
                )
                .bind(metric_key)
                .bind(tenant_id)
                .bind(device_id)
                .bind(from)
                .bind(to)
                .fetch_one(pool)
                .await?;
                let sample_count = row.get::<i64, _>("sample_count");
                if sample_count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(TelemetryAggregate {
                        average: row.get("average"),
                        sample_count: u64::try_from(sample_count)
                            .expect("PostgreSQL COUNT(*) is nonnegative"),
                    }))
                }
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

    async fn require_sqlite_tenant_device(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        let Self::Sqlite(store) = self else {
            unreachable!("SQLite validation is only used by the SQLite adapter");
        };
        let registered = sqlx::query_scalar::<_, String>(
            "SELECT device_id
             FROM devices
             WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL
             LIMIT 1",
        )
        .bind(tenant_id.to_string())
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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

async fn ingest_sqlite_gateway(
    store: &SqliteStore,
    request: GatewayIngestRequest,
) -> Result<GatewayIngestResult, PlatformStoreError> {
    let mut transaction = store.pool().begin().await?;
    let receipt = sqlx::query(
        "INSERT OR IGNORE INTO gateway_event_receipts (
            tenant_id, gateway_device_id, idempotency_key, event_at, received_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(request.tenant_id.to_string())
    .bind(&request.gateway_device_id)
    .bind(&request.idempotency_key)
    .bind(request.event_at.to_rfc3339())
    .bind(request.received_at.to_rfc3339())
    .execute(transaction.as_mut())
    .await?;
    if receipt.rows_affected() == 0 {
        transaction.commit().await?;
        return Ok(GatewayIngestResult {
            receipt_inserted: false,
            telemetry_inserted: false,
        });
    }
    if let Err(error) = validate_gateway_ingest_request(&request) {
        transaction.rollback().await?;
        return Err(error);
    }
    if !sqlite_active_gateway_exists(
        &mut transaction,
        request.tenant_id,
        &request.gateway_device_id,
    )
    .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(request.gateway_device_id));
    }
    if let Some(child_device_id) = request.child_device_id.as_deref()
        && !sqlite_active_owned_child_exists(
            &mut transaction,
            request.tenant_id,
            child_device_id,
            &request.gateway_device_id,
        )
        .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(
            child_device_id.to_owned(),
        ));
    }

    let event_at = request.event_at.to_rfc3339();
    sqlx::query(
        "UPDATE devices
         SET last_seen_at = CASE
             WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
             ELSE last_seen_at
         END
         WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(&event_at)
    .bind(&event_at)
    .bind(request.tenant_id.to_string())
    .bind(&request.gateway_device_id)
    .execute(transaction.as_mut())
    .await?;
    let telemetry_inserted = if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        store
            .write_telemetry_in_transaction(
                &mut transaction,
                request.tenant_id,
                telemetry_event,
                request.received_at,
                &request.topic,
            )
            .await?
    } else {
        false
    };
    match request.event_kind {
        GatewayIngestEventKind::Disconnect => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "UPDATE devices
                     SET gateway_read_quality = 'unavailable'
                     WHERE tenant_id = ? AND device_id = ?",
                )
                .bind(request.tenant_id.to_string())
                .bind(child_device_id)
                .execute(transaction.as_mut())
                .await?;
            }
        }
        GatewayIngestEventKind::ChildTelemetry => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "UPDATE devices
                     SET gateway_last_read_at = CASE
                             WHEN gateway_last_read_at IS NULL OR gateway_last_read_at < ? THEN ?
                             ELSE gateway_last_read_at
                         END,
                         gateway_read_quality = 'good'
                     WHERE tenant_id = ? AND device_id = ?",
                )
                .bind(&event_at)
                .bind(&event_at)
                .bind(request.tenant_id.to_string())
                .bind(child_device_id)
                .execute(transaction.as_mut())
                .await?;
            }
        }
        GatewayIngestEventKind::Connect | GatewayIngestEventKind::Heartbeat => {}
    }

    transaction.commit().await?;
    Ok(GatewayIngestResult {
        receipt_inserted: true,
        telemetry_inserted,
    })
}

async fn ingest_timescale_gateway(
    pool: &PgPool,
    request: GatewayIngestRequest,
) -> Result<GatewayIngestResult, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let receipt = sqlx::query(
        "INSERT INTO gateway_event_receipts (
            tenant_id, gateway_device_id, idempotency_key, event_at, received_at
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tenant_id, gateway_device_id, idempotency_key) DO NOTHING",
    )
    .bind(request.tenant_id)
    .bind(&request.gateway_device_id)
    .bind(&request.idempotency_key)
    .bind(request.event_at)
    .bind(request.received_at)
    .execute(&mut *transaction)
    .await?;
    if receipt.rows_affected() == 0 {
        transaction.commit().await?;
        return Ok(GatewayIngestResult {
            receipt_inserted: false,
            telemetry_inserted: false,
        });
    }
    if let Err(error) = validate_gateway_ingest_request(&request) {
        transaction.rollback().await?;
        return Err(error);
    }
    if !timescale_active_gateway_is_locked(
        &mut transaction,
        request.tenant_id,
        &request.gateway_device_id,
    )
    .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(request.gateway_device_id));
    }
    if let Some(child_device_id) = request.child_device_id.as_deref()
        && !timescale_active_owned_child_is_locked(
            &mut transaction,
            request.tenant_id,
            child_device_id,
            &request.gateway_device_id,
        )
        .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(
            child_device_id.to_owned(),
        ));
    }

    sqlx::query(
        "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (tenant_id, device_id)
         DO UPDATE SET last_seen_at = GREATEST(
             COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
             EXCLUDED.last_seen_at
         )",
    )
    .bind(request.tenant_id)
    .bind(&request.gateway_device_id)
    .bind(request.event_at)
    .execute(&mut *transaction)
    .await?;
    let telemetry_inserted = if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        let sequence = i64::try_from(telemetry_event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;
        sqlx::query(
            "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (tenant_id, device_id)
             DO UPDATE SET last_seen_at = GREATEST(
                 COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                 EXCLUDED.last_seen_at
             )",
        )
        .bind(request.tenant_id)
        .bind(&telemetry_event.device_id)
        .bind(request.received_at)
        .execute(&mut *transaction)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements,
                topic, gateway_device_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (tenant_id, event_at, device_id, boot_id, sequence) DO NOTHING",
        )
        .bind(telemetry_event.event_at)
        .bind(request.received_at)
        .bind(request.tenant_id)
        .bind(&telemetry_event.device_id)
        .bind(telemetry_event.boot_id)
        .bind(sequence)
        .bind(Json(serde_json::Value::Object(
            telemetry_event.measurements.clone(),
        )))
        .bind(&request.topic)
        .bind(&telemetry_event.gateway_device_id)
        .execute(&mut *transaction)
        .await?;
        inserted.rows_affected() == 1
    } else {
        false
    };
    match request.event_kind {
        GatewayIngestEventKind::Disconnect => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "INSERT INTO device_runtime_state (
                         tenant_id, device_id, gateway_read_quality
                     ) VALUES ($1, $2, 'unavailable')
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET gateway_read_quality = EXCLUDED.gateway_read_quality",
                )
                .bind(request.tenant_id)
                .bind(child_device_id)
                .execute(&mut *transaction)
                .await?;
            }
        }
        GatewayIngestEventKind::ChildTelemetry => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "INSERT INTO device_runtime_state (
                         tenant_id, device_id, gateway_last_read_at, gateway_read_quality
                     ) VALUES ($1, $2, $3, 'good')
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET
                         gateway_last_read_at = GREATEST(
                             COALESCE(
                                 device_runtime_state.gateway_last_read_at,
                                 '-infinity'::timestamptz
                             ),
                             EXCLUDED.gateway_last_read_at
                         ),
                         gateway_read_quality = EXCLUDED.gateway_read_quality",
                )
                .bind(request.tenant_id)
                .bind(child_device_id)
                .bind(request.event_at)
                .execute(&mut *transaction)
                .await?;
            }
        }
        GatewayIngestEventKind::Connect | GatewayIngestEventKind::Heartbeat => {}
    }

    transaction.commit().await?;
    Ok(GatewayIngestResult {
        receipt_inserted: true,
        telemetry_inserted,
    })
}

fn validate_gateway_ingest_request(
    request: &GatewayIngestRequest,
) -> Result<(), PlatformStoreError> {
    match request.event_kind {
        GatewayIngestEventKind::ChildTelemetry => {
            if request.child_device_id.is_none() {
                return Err(GatewayIngestValidationError::ChildTelemetryMissingChild.into());
            }
            if request.telemetry_event.is_none() {
                return Err(GatewayIngestValidationError::ChildTelemetryMissingTelemetry.into());
            }
        }
        GatewayIngestEventKind::Connect
        | GatewayIngestEventKind::Disconnect
        | GatewayIngestEventKind::Heartbeat => {
            if request.telemetry_event.is_some() {
                return Err(GatewayIngestValidationError::TelemetryOnNonChildEvent.into());
            }
        }
    }

    if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        let Some(child_device_id) = request.child_device_id.as_deref() else {
            return Err(GatewayIngestValidationError::TelemetryMissingChild.into());
        };
        if telemetry_event.device_id != child_device_id {
            return Err(GatewayIngestValidationError::TelemetryChildMismatch.into());
        }
        if telemetry_event.gateway_device_id.as_deref() != Some(request.gateway_device_id.as_str())
        {
            return Err(GatewayIngestValidationError::TelemetryGatewayMismatch.into());
        }
        i64::try_from(telemetry_event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;
    }

    Ok(())
}

async fn sqlite_active_gateway_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL AND is_gateway = 1",
    )
    .bind(tenant_id.to_string())
    .bind(gateway_device_id)
    .fetch_optional(transaction.as_mut())
    .await
    .map(|device| device.is_some())
}

async fn sqlite_active_owned_child_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = ?
           AND device_id = ?
           AND deleted_at IS NULL
           AND is_gateway = 0
           AND gateway_device_id = ?",
    )
    .bind(tenant_id.to_string())
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(transaction.as_mut())
    .await
    .map(|device| device.is_some())
}

async fn timescale_active_gateway_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL AND is_gateway = TRUE
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

async fn timescale_active_owned_child_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = $1
           AND device_id = $2
           AND deleted_at IS NULL
           AND is_gateway = FALSE
           AND gateway_device_id = $3
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

async fn timescale_device_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE device_id = $1
         FOR UPDATE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

async fn timescale_tenant_device_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(tenant_id)
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
            command.state, command.created_at, command.expires_at, command.next_attempt_at, command.lease_until,
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

async fn create_timescale_incident(
    pool: &PgPool,
    incident: NewAlertIncident,
    opened_notification: Option<NewNotificationOutboxEntry>,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, last_value
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(incident.id)
    .bind(incident.rule_id)
    .bind(&incident.device_id)
    .bind(incident.status.as_str())
    .bind(incident.condition_started_at)
    .bind((incident.status == AlertIncidentStatus::Open).then_some(incident.condition_started_at))
    .bind(incident.last_value)
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(None);
    }
    if let Some(notification) = opened_notification {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(notification.id)
        .bind(incident.id)
        .bind(notification.kind.as_str())
        .bind(notification.dedupe_key)
        .bind(notification.subject)
        .bind(notification.body)
        .bind(notification.next_attempt_at)
        .execute(&mut *transaction)
        .await?;
    }
    let row = sqlx::query(
        "SELECT id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                state_version
         FROM alert_incidents WHERE id = $1",
    )
    .bind(incident.id)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    postgres_alert_incident_record(row).map(Some)
}

async fn update_timescale_incident_last_value(
    pool: &PgPool,
    incident_id: uuid::Uuid,
    expected_version: i64,
    last_value: Option<f64>,
    updated_at: DateTime<Utc>,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE alert_incidents
         SET last_value = $1, state_version = state_version + 1, updated_at = $2
         WHERE id = $3 AND state_version = $4 AND status IN ('pending', 'open')
         RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                   opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                   state_version",
    )
    .bind(last_value)
    .bind(updated_at)
    .bind(incident_id)
    .bind(expected_version)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition(
    pool: &PgPool,
    incident_id: uuid::Uuid,
    expected_version: i64,
    transition: AlertIncidentTransition,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let query = match transition {
        AlertIncidentTransition::Open(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', opened_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'pending'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => sqlx::query(
            "UPDATE alert_incidents
             SET recovery_started_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'open'
                   AND recovery_started_at IS NULL
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'open'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'open'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .fetch_optional(pool)
        .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition_with_notification(
    pool: &PgPool,
    incident_id: uuid::Uuid,
    expected_version: i64,
    transition: AlertIncidentTransition,
    notification: NewNotificationOutboxEntry,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let query = match transition {
        AlertIncidentTransition::Open(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', opened_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'pending'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'open'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND state_version = $3 AND status = 'open'
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => unreachable!(),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .fetch_optional(&mut *transaction)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(notification.id)
    .bind(incident_id)
    .bind(notification.kind.as_str())
    .bind(notification.dedupe_key)
    .bind(notification.subject)
    .bind(notification.body)
    .bind(notification.next_attempt_at)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    postgres_alert_incident_record(row).map(Some)
}

async fn enqueue_timescale_notification(
    pool: &PgPool,
    incident_id: uuid::Uuid,
    notification: NewNotificationOutboxEntry,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(notification.id)
    .bind(incident_id)
    .bind(notification.kind.as_str())
    .bind(&notification.dedupe_key)
    .bind(&notification.subject)
    .bind(&notification.body)
    .bind(notification.next_attempt_at)
    .execute(pool)
    .await?;
    let row = sqlx::query(
        "SELECT id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at
         FROM notification_outbox WHERE dedupe_key = $1",
    )
    .bind(notification.dedupe_key)
    .fetch_one(pool)
    .await?;
    postgres_notification_outbox_record(row)
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
            id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
}

fn canonical_postgres_timestamp(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    timestamp
        .with_nanosecond(timestamp.nanosecond() / 1_000 * 1_000)
        .expect("a valid UTC timestamp can be represented at microsecond precision")
}

fn event_condition(rule: &AlertRule, value: f64) -> Option<bool> {
    let hysteresis = rule.hysteresis.unwrap_or(0.0);
    Some(match rule.comparison {
        AlertComparison::GreaterThan if value > rule.threshold => true,
        AlertComparison::GreaterThan if value <= rule.threshold - hysteresis => false,
        AlertComparison::GreaterThanOrEqual if value >= rule.threshold => true,
        AlertComparison::GreaterThanOrEqual if value < rule.threshold - hysteresis => false,
        AlertComparison::LessThan if value < rule.threshold => true,
        AlertComparison::LessThan if value >= rule.threshold + hysteresis => false,
        AlertComparison::LessThanOrEqual if value <= rule.threshold => true,
        AlertComparison::LessThanOrEqual if value > rule.threshold + hysteresis => false,
        _ => return None,
    })
}

fn alert_severity_name(severity: AlertSeverity) -> &'static str {
    match severity {
        AlertSeverity::Info => "INFO",
        AlertSeverity::Warning => "WARNING",
        AlertSeverity::Critical => "CRITICAL",
    }
}

#[derive(Default)]
struct EventTransition {
    opened: bool,
    resolved: bool,
    reminder: bool,
}

struct EventIncident {
    id: uuid::Uuid,
    status: AlertIncidentStatus,
    condition_started_at: DateTime<Utc>,
    recovery_started_at: Option<DateTime<Utc>>,
    acknowledged_at: Option<DateTime<Utc>>,
    last_reminder_at: Option<DateTime<Utc>>,
    state_version: i64,
}

fn sqlite_event_incident(row: SqliteRow) -> Result<EventIncident, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    Ok(EventIncident {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidIncidentId(id))?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: incident_timestamp(&row, "condition_started_at")?,
        recovery_started_at: incident_optional_timestamp(&row, "recovery_started_at")?,
        acknowledged_at: incident_optional_timestamp(&row, "acknowledged_at")?,
        last_reminder_at: incident_optional_timestamp(&row, "last_reminder_at")?,
        state_version: row.try_get("state_version")?,
    })
}

fn postgres_event_incident(row: PgRow) -> Result<EventIncident, PlatformStoreError> {
    Ok(EventIncident {
        id: row.try_get("id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: row.try_get("condition_started_at")?,
        recovery_started_at: row.try_get("recovery_started_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        last_reminder_at: row.try_get("last_reminder_at")?,
        state_version: i64::from(row.try_get::<i32, _>("state_version")?),
    })
}

fn event_notification(
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> (String, String, String) {
    let dedupe_key = match kind {
        "opened" | "resolved" => format!("incident:{incident_id}:{kind}:{state_version}"),
        "reminder" => format!(
            "incident:{incident_id}:reminder:{state_version}:{}",
            created_at
                .timestamp()
                .div_euclid(rule.reminder_interval.num_seconds())
        ),
        _ => unreachable!("event notifications have a known kind"),
    };
    let severity = alert_severity_name(rule.severity);
    (
        dedupe_key,
        format!("[{severity}] {} {kind}", rule.name),
        format!(
            "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: {kind}\n",
            rule.name, rule.metric_key, rule.threshold
        ),
    )
}

async fn insert_sqlite_event_notification(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    let (dedupe_key, subject, body) = event_notification(
        rule,
        incident_id,
        device_id,
        value,
        kind,
        state_version,
        created_at,
    );
    let created_at = created_at.to_rfc3339();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(incident_id.to_string())
    .bind(kind)
    .bind(dedupe_key)
    .bind(subject)
    .bind(body)
    .bind(&created_at)
    .bind(&created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_timescale_event_notification(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    let (dedupe_key, subject, body) = event_notification(
        rule,
        incident_id,
        device_id,
        value,
        kind,
        state_version,
        created_at,
    );
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(incident_id)
    .bind(kind)
    .bind(dedupe_key)
    .bind(subject)
    .bind(body)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_sqlite_active_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = ? AND device_id = ? AND status IN ('pending', 'open')",
    )
    .bind(rule_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(sqlite_event_incident)
    .transpose()
}

async fn load_timescale_active_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = $1 AND device_id = $2 AND status IN ('pending', 'open')
         FOR UPDATE",
    )
    .bind(rule_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_event_incident)
    .transpose()
}

async fn load_sqlite_recent_resolved_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = ? AND device_id = ? AND status = 'resolved' AND resolved_at >= ?
         ORDER BY resolved_at DESC
         LIMIT 1",
    )
    .bind(rule_id.to_string())
    .bind(device_id)
    .bind(reopen_after.to_rfc3339())
    .fetch_optional(&mut **transaction)
    .await?
    .map(sqlite_event_incident)
    .transpose()
}

async fn load_timescale_recent_resolved_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = $1 AND device_id = $2 AND status = 'resolved' AND resolved_at >= $3
         ORDER BY resolved_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(rule_id)
    .bind(device_id)
    .bind(reopen_after)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_event_incident)
    .transpose()
}

async fn reopen_sqlite_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident: EventIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let at = evaluated_at.to_rfc3339();
    if rule.for_duration == ChronoDuration::zero() {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = ?, recovery_started_at = NULL,
                 opened_at = ?, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = ?, last_notified_at = ?,
                 last_reminder_at = ?, state_version = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(&at)
        .bind(&at)
        .bind(value)
        .bind(&at)
        .bind(&at)
        .bind(state_version)
        .bind(&at)
        .bind(incident.id.to_string())
        .execute(&mut **transaction)
        .await?;
        insert_sqlite_event_notification(
            transaction,
            rule,
            incident.id,
            device_id,
            value,
            "opened",
            state_version,
            evaluated_at,
        )
        .await?;
        return Ok(EventTransition {
            opened: true,
            ..EventTransition::default()
        });
    }
    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = ?, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(&at)
    .bind(value)
    .bind(&at)
    .bind(incident.id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(EventTransition::default())
}

async fn reopen_timescale_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident: EventIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    if rule.for_duration == ChronoDuration::zero() {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = $2, recovery_started_at = NULL,
                 opened_at = $2, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = $3, last_notified_at = $2,
                 last_reminder_at = $2, state_version = $4, updated_at = $2
             WHERE id = $1",
        )
        .bind(incident.id)
        .bind(evaluated_at)
        .bind(value)
        .bind(state_version as i32)
        .execute(&mut **transaction)
        .await?;
        insert_timescale_event_notification(
            transaction,
            rule,
            incident.id,
            device_id,
            value,
            "opened",
            state_version,
            evaluated_at,
        )
        .await?;
        return Ok(EventTransition {
            opened: true,
            ..EventTransition::default()
        });
    }
    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = $2, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = $3, updated_at = $2
         WHERE id = $1",
    )
    .bind(incident.id)
    .bind(evaluated_at)
    .bind(value)
    .execute(&mut **transaction)
    .await?;
    Ok(EventTransition::default())
}

async fn evaluate_sqlite_event_transition(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let condition = event_condition(rule, value);
    let at = evaluated_at.to_rfc3339();
    if condition != Some(true) {
        if condition == Some(false) {
            if let Some(incident) =
                load_sqlite_active_event_incident(transaction, rule.id, device_id).await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = ?")
                            .bind(incident.id.to_string())
                            .execute(&mut **transaction)
                            .await?;
                    }
                    AlertIncidentStatus::Open => {
                        let recovery_started_at =
                            incident.recovery_started_at.unwrap_or(evaluated_at);
                        if evaluated_at - recovery_started_at >= rule.resolve_after {
                            let state_version = incident.state_version + 1;
                            sqlx::query(
                                "UPDATE alert_incidents
                                 SET status = 'resolved', recovery_started_at = ?, resolved_at = ?,
                                     last_value = ?, last_notified_at = ?, state_version = ?,
                                     updated_at = ?
                                 WHERE id = ?",
                            )
                            .bind(&at)
                            .bind(&at)
                            .bind(value)
                            .bind(&at)
                            .bind(state_version)
                            .bind(&at)
                            .bind(incident.id.to_string())
                            .execute(&mut **transaction)
                            .await?;
                            insert_sqlite_event_notification(
                                transaction,
                                rule,
                                incident.id,
                                device_id,
                                value,
                                "resolved",
                                state_version,
                                evaluated_at,
                            )
                            .await?;
                            return Ok(EventTransition {
                                resolved: true,
                                ..EventTransition::default()
                            });
                        }
                        sqlx::query(
                            "UPDATE alert_incidents
                             SET recovery_started_at = ?, last_value = ?, updated_at = ?
                             WHERE id = ?",
                        )
                        .bind(recovery_started_at.to_rfc3339())
                        .bind(value)
                        .bind(&at)
                        .bind(incident.id.to_string())
                        .execute(&mut **transaction)
                        .await?;
                    }
                    AlertIncidentStatus::Resolved => {}
                }
            }
        }
        return Ok(EventTransition::default());
    }
    let Some(incident) = load_sqlite_active_event_incident(transaction, rule.id, device_id).await?
    else {
        if let Some(resolved) = load_sqlite_recent_resolved_event_incident(
            transaction,
            rule.id,
            device_id,
            evaluated_at - rule.reopen_grace,
        )
        .await?
        {
            return reopen_sqlite_event_incident(
                transaction,
                rule,
                resolved,
                device_id,
                value,
                evaluated_at,
            )
            .await;
        }
        let id = uuid::Uuid::new_v4();
        if rule.for_duration == ChronoDuration::zero() {
            sqlx::query(
                "INSERT INTO alert_incidents (id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(id.to_string())
            .bind(rule.id.to_string())
            .bind(device_id)
            .bind(&at)
            .bind(&at)
            .bind(value)
            .bind(&at)
            .bind(&at)
            .bind(&at)
            .bind(&at)
            .execute(&mut **transaction)
            .await?;
            insert_sqlite_event_notification(
                transaction,
                rule,
                id,
                device_id,
                value,
                "opened",
                1,
                evaluated_at,
            )
            .await?;
            return Ok(EventTransition {
                opened: true,
                ..EventTransition::default()
            });
        }
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(rule.id.to_string())
        .bind(device_id)
        .bind(&at)
        .bind(value)
        .bind(&at)
        .bind(&at)
        .execute(&mut **transaction)
        .await?;
        return Ok(EventTransition::default());
    };

    match incident.status {
        AlertIncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at >= rule.for_duration {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = ?,
                         last_value = ?, last_notified_at = ?, last_reminder_at = ?,
                         state_version = ?, updated_at = ?
                     WHERE id = ?",
                )
                .bind(&at)
                .bind(value)
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .execute(&mut **transaction)
                .await?;
                insert_sqlite_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "opened",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    opened: true,
                    ..EventTransition::default()
                });
            }
            sqlx::query("UPDATE alert_incidents SET last_value = ?, updated_at = ? WHERE id = ?")
                .bind(value)
                .bind(&at)
                .bind(incident.id.to_string())
                .execute(&mut **transaction)
                .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(value)
            .bind(&at)
            .bind(incident.id.to_string())
            .execute(&mut **transaction)
            .await?;
            let due = incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|last_reminder_at| {
                    evaluated_at - last_reminder_at >= rule.reminder_interval
                });
            if due {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = ?, last_notified_at = ?, state_version = ?,
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .execute(&mut **transaction)
                .await?;
                insert_sqlite_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "reminder",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    reminder: true,
                    ..EventTransition::default()
                });
            }
        }
        AlertIncidentStatus::Resolved => {}
    }
    Ok(EventTransition::default())
}

async fn evaluate_timescale_event_transition(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let condition = event_condition(rule, value);
    if condition != Some(true) {
        if condition == Some(false) {
            if let Some(incident) =
                load_timescale_active_event_incident(transaction, rule.id, device_id).await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = $1")
                            .bind(incident.id)
                            .execute(&mut **transaction)
                            .await?;
                    }
                    AlertIncidentStatus::Open => {
                        let recovery_started_at =
                            incident.recovery_started_at.unwrap_or(evaluated_at);
                        if evaluated_at - recovery_started_at >= rule.resolve_after {
                            let state_version = incident.state_version + 1;
                            sqlx::query(
                                "UPDATE alert_incidents
                                 SET status = 'resolved', recovery_started_at = $2, resolved_at = $2,
                                     last_value = $3, last_notified_at = $2, state_version = $4,
                                     updated_at = $2
                                 WHERE id = $1",
                            )
                            .bind(incident.id)
                            .bind(evaluated_at)
                            .bind(value)
                            .bind(state_version as i32)
                            .execute(&mut **transaction)
                            .await?;
                            insert_timescale_event_notification(
                                transaction,
                                rule,
                                incident.id,
                                device_id,
                                value,
                                "resolved",
                                state_version,
                                evaluated_at,
                            )
                            .await?;
                            return Ok(EventTransition {
                                resolved: true,
                                ..EventTransition::default()
                            });
                        }
                        sqlx::query(
                            "UPDATE alert_incidents
                             SET recovery_started_at = $2, last_value = $3, updated_at = $4
                             WHERE id = $1",
                        )
                        .bind(incident.id)
                        .bind(recovery_started_at)
                        .bind(value)
                        .bind(evaluated_at)
                        .execute(&mut **transaction)
                        .await?;
                    }
                    AlertIncidentStatus::Resolved => {}
                }
            }
        }
        return Ok(EventTransition::default());
    }
    let Some(incident) =
        load_timescale_active_event_incident(transaction, rule.id, device_id).await?
    else {
        if let Some(resolved) = load_timescale_recent_resolved_event_incident(
            transaction,
            rule.id,
            device_id,
            evaluated_at - rule.reopen_grace,
        )
        .await?
        {
            return reopen_timescale_event_incident(
                transaction,
                rule,
                resolved,
                device_id,
                value,
                evaluated_at,
            )
            .await;
        }
        let id = uuid::Uuid::new_v4();
        if rule.for_duration == ChronoDuration::zero() {
            sqlx::query(
                "INSERT INTO alert_incidents (id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES ($1, $2, $3, 'open', $4, $4, $5, $4, $4, 1, $4, $4)",
            )
            .bind(id)
            .bind(rule.id)
            .bind(device_id)
            .bind(evaluated_at)
            .bind(value)
            .execute(&mut **transaction)
            .await?;
            insert_timescale_event_notification(
                transaction,
                rule,
                id,
                device_id,
                value,
                "opened",
                1,
                evaluated_at,
            )
            .await?;
            return Ok(EventTransition {
                opened: true,
                ..EventTransition::default()
            });
        }
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES ($1, $2, $3, 'pending', $4, $5, $4, $4)",
        )
        .bind(id)
        .bind(rule.id)
        .bind(device_id)
        .bind(evaluated_at)
        .bind(value)
        .execute(&mut **transaction)
        .await?;
        return Ok(EventTransition::default());
    };

    match incident.status {
        AlertIncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at >= rule.for_duration {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = $2,
                         last_value = $3, last_notified_at = $2, last_reminder_at = $2,
                         state_version = $4, updated_at = $2
                     WHERE id = $1",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(value)
                .bind(state_version as i32)
                .execute(&mut **transaction)
                .await?;
                insert_timescale_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "opened",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    opened: true,
                    ..EventTransition::default()
                });
            }
            sqlx::query(
                "UPDATE alert_incidents SET last_value = $2, updated_at = $3 WHERE id = $1",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .execute(&mut **transaction)
            .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = $2, updated_at = $3
                 WHERE id = $1",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .execute(&mut **transaction)
            .await?;
            let due = incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|last_reminder_at| {
                    evaluated_at - last_reminder_at >= rule.reminder_interval
                });
            if due {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = $2, last_notified_at = $2, state_version = $3,
                         updated_at = $2
                     WHERE id = $1",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(state_version as i32)
                .execute(&mut **transaction)
                .await?;
                insert_timescale_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "reminder",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    reminder: true,
                    ..EventTransition::default()
                });
            }
        }
        AlertIncidentStatus::Resolved => {}
    }
    Ok(EventTransition::default())
}

async fn evaluate_sqlite_alert_events(
    store: &SqliteStore,
    events: &[AlertEvaluationEvent],
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = store.pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled = 1 AND archived_at IS NULL
           AND rule_type = 'event_threshold' ORDER BY created_at, id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(sqlite_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut result = AlertEvaluationResult::default();
    for event in events {
        for rule in &rules {
            if rule
                .device_id
                .as_deref()
                .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            let Some(value) = event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            let claim = sqlx::query(
                "INSERT INTO alert_rule_event_evaluations
                 (rule_id, event_at, device_id, boot_id, sequence) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (rule_id, event_at, device_id, boot_id, sequence) DO NOTHING",
            )
            .bind(rule.id.to_string())
            .bind(canonical_postgres_timestamp(event.event_at).to_rfc3339())
            .bind(&event.device_id)
            .bind(event.boot_id.to_string())
            .bind(event.sequence.to_string())
            .execute(&mut *transaction)
            .await?;
            if claim.rows_affected() == 0 {
                continue;
            }
            result.evaluated += 1;
            let transition = evaluate_sqlite_event_transition(
                &mut transaction,
                rule,
                &event.device_id,
                value,
                canonical_postgres_timestamp(event.received_at),
            )
            .await?;
            result.opened += usize::from(transition.opened);
            result.resolved += usize::from(transition.resolved);
            result.reminders += usize::from(transition.reminder);
        }
    }
    transaction.commit().await?;
    Ok(result)
}

async fn evaluate_timescale_alert_events(
    pool: &PgPool,
    events: &[AlertEvaluationEvent],
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled AND archived_at IS NULL
           AND rule_type = 'event_threshold' ORDER BY created_at, id FOR SHARE",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(postgres_alert_rule_record)
        .collect::<Result<_, _>>()?;

    // Lock every affected incident key in a stable order before any transition.
    // This prevents opposite event-batch orders from forming an advisory-lock cycle.
    let mut lock_keys = Vec::new();
    for event in events {
        for rule in &rules {
            if rule
                .device_id
                .as_deref()
                .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            if event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(f64::is_finite)
            {
                lock_keys.push((rule.id, event.device_id.clone()));
            }
        }
    }
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("iot_nano:alert-event:{rule_id}:{device_id}"))
            .execute(&mut *transaction)
            .await?;
    }

    let mut result = AlertEvaluationResult::default();
    for event in events {
        for rule in &rules {
            if rule
                .device_id
                .as_deref()
                .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            let Some(value) = event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            let sequence = i64::try_from(event.sequence)
                .map_err(|_| PlatformStoreError::AlertRuleSequenceOverflow)?;
            let claim = sqlx::query("INSERT INTO alert_rule_event_evaluations (rule_id, event_at, device_id, boot_id, sequence) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (rule_id, event_at, device_id, boot_id, sequence) DO NOTHING")
                .bind(rule.id).bind(canonical_postgres_timestamp(event.event_at)).bind(&event.device_id).bind(event.boot_id).bind(sequence).execute(&mut *transaction).await?;
            if claim.rows_affected() == 0 {
                continue;
            }
            result.evaluated += 1;
            let transition = evaluate_timescale_event_transition(
                &mut transaction,
                rule,
                &event.device_id,
                value,
                canonical_postgres_timestamp(event.received_at),
            )
            .await?;
            result.opened += usize::from(transition.opened);
            result.resolved += usize::from(transition.resolved);
            result.reminders += usize::from(transition.reminder);
        }
    }
    transaction.commit().await?;
    Ok(result)
}

async fn sqlite_window_aggregates(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    evaluated_at: DateTime<Utc>,
) -> Result<Vec<(String, f64)>, PlatformStoreError> {
    let Some(window) = rule.window else {
        return Ok(Vec::new());
    };
    let from = evaluated_at - window;
    let path = format!("$.{}", rule.metric_key);
    let rows = sqlx::query(
        "WITH canonical_telemetry AS (
            SELECT device_id, measurements,
                   CAST(unixepoch(event_at) AS INTEGER) * 1000000
                   + CASE
                       WHEN instr(event_at, '.') = 0 THEN 0
                       ELSE CAST(
                           substr(
                               substr(
                                   event_at,
                                   instr(event_at, '.') + 1,
                                   CASE
                                       WHEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           'Z'
                                       ) > 0
                                       THEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           'Z'
                                       ) - 1
                                       WHEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '+'
                                       ) > 0
                                       THEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '+'
                                       ) - 1
                                       ELSE instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '-'
                                       ) - 1
                                   END
                               ) || '000000',
                               1,
                               6
                           ) AS INTEGER
                       )
                   END AS event_at_micros
            FROM telemetry
         ),
         finite_telemetry AS (
             SELECT device_id, json_extract(measurements, ?) AS finite_value
             FROM canonical_telemetry
             WHERE event_at_micros >= ?
               AND event_at_micros <= ?
               AND (? IS NULL OR device_id = ?)
               AND json_type(measurements, ?) IN ('integer', 'real')
               AND json_extract(measurements, ?) > -1.0e999
               AND json_extract(measurements, ?) < 1.0e999
         ),
         device_scales AS (
             SELECT device_id, MAX(ABS(1.0 * finite_value)) AS scale
             FROM finite_telemetry
             GROUP BY device_id
         )
         SELECT finite_telemetry.device_id,
                CASE
                    WHEN device_scales.scale = 0.0 THEN 0.0
                    ELSE AVG(
                        1.0 * finite_telemetry.finite_value / NULLIF(device_scales.scale, 0.0)
                    ) * device_scales.scale
                END AS average
         FROM finite_telemetry
         JOIN device_scales ON device_scales.device_id = finite_telemetry.device_id
         GROUP BY finite_telemetry.device_id, device_scales.scale
         ORDER BY finite_telemetry.device_id",
    )
    .bind(&path)
    .bind(from.timestamp_micros())
    .bind(evaluated_at.timestamp_micros())
    .bind(rule.device_id.as_deref())
    .bind(rule.device_id.as_deref())
    .bind(&path)
    .bind(&path)
    .bind(&path)
    .fetch_all(&mut **transaction)
    .await?;
    let mut aggregates = Vec::with_capacity(rows.len());
    for row in rows {
        let average: f64 = row.try_get("average")?;
        if average.is_finite() {
            aggregates.push((row.try_get("device_id")?, average));
        }
    }
    Ok(aggregates)
}

async fn timescale_window_aggregates(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    evaluated_at: DateTime<Utc>,
) -> Result<Vec<(String, f64)>, PlatformStoreError> {
    let Some(window) = rule.window else {
        return Ok(Vec::new());
    };
    let from = evaluated_at - window;
    let rows = sqlx::query(
        "WITH finite_telemetry AS (
             SELECT device_id,
                    CASE
                        WHEN jsonb_typeof(measurements -> $1) = 'number'
                        THEN CASE
                            WHEN (measurements ->> $1)::numeric BETWEEN
                                     '-1.7976931348623157e308'::numeric
                                 AND '1.7976931348623157e308'::numeric
                            THEN (measurements ->> $1)::numeric
                        END
                    END AS finite_value
             FROM telemetry
             WHERE event_at >= $2
               AND event_at <= $3
               AND ($4::text IS NULL OR device_id = $4)
         )
         SELECT device_id, (AVG(finite_value))::double precision AS average
         FROM finite_telemetry
         GROUP BY device_id
         HAVING COUNT(finite_value) > 0
         ORDER BY device_id",
    )
    .bind(&rule.metric_key)
    .bind(from)
    .bind(evaluated_at)
    .bind(rule.device_id.as_deref())
    .fetch_all(&mut **transaction)
    .await?;
    let mut aggregates = Vec::with_capacity(rows.len());
    for row in rows {
        let average: f64 = row.try_get("average")?;
        if average.is_finite() {
            aggregates.push((row.try_get("device_id")?, average));
        }
    }
    Ok(aggregates)
}

async fn evaluate_sqlite_alert_windows(
    store: &SqliteStore,
    evaluated_at: DateTime<Utc>,
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = store.pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled = 1 AND archived_at IS NULL
           AND rule_type = 'window_average' ORDER BY created_at, id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(sqlite_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut evaluations = Vec::new();
    for rule in &rules {
        evaluations.extend(
            sqlite_window_aggregates(&mut transaction, rule, evaluated_at)
                .await?
                .into_iter()
                .map(|(device_id, value)| (rule.clone(), device_id, value)),
        );
    }

    let mut result = AlertEvaluationResult::default();
    for (rule, device_id, value) in evaluations {
        result.evaluated += 1;
        let transition = evaluate_sqlite_event_transition(
            &mut transaction,
            &rule,
            &device_id,
            value,
            evaluated_at,
        )
        .await?;
        result.opened += usize::from(transition.opened);
        result.resolved += usize::from(transition.resolved);
        result.reminders += usize::from(transition.reminder);
    }
    transaction.commit().await?;
    Ok(result)
}

async fn evaluate_timescale_alert_windows(
    pool: &PgPool,
    evaluated_at: DateTime<Utc>,
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled AND archived_at IS NULL
           AND rule_type = 'window_average' ORDER BY created_at, id FOR SHARE",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(postgres_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut evaluations = Vec::new();
    for rule in &rules {
        evaluations.extend(
            timescale_window_aggregates(&mut transaction, rule, evaluated_at)
                .await?
                .into_iter()
                .map(|(device_id, value)| (rule.clone(), device_id, value)),
        );
    }

    // Lock every affected incident key in a stable order before any transition.
    // This prevents opposite window-batch orders from forming an advisory-lock cycle.
    let mut lock_keys: Vec<_> = evaluations
        .iter()
        .map(|(rule, device_id, _)| (rule.id, device_id.clone()))
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("iot_nano:alert-window:{rule_id}:{device_id}"))
            .execute(&mut *transaction)
            .await?;
    }

    let mut result = AlertEvaluationResult::default();
    for (rule, device_id, value) in evaluations {
        result.evaluated += 1;
        let transition = evaluate_timescale_event_transition(
            &mut transaction,
            &rule,
            &device_id,
            value,
            evaluated_at,
        )
        .await?;
        result.opened += usize::from(transition.opened);
        result.resolved += usize::from(transition.resolved);
        result.reminders += usize::from(transition.reminder);
    }
    transaction.commit().await?;
    Ok(result)
}

fn validate_telemetry_metric_key(metric_key: &str) -> Result<(), PlatformStoreError> {
    let mut characters = metric_key.chars();
    let valid = matches!(characters.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(PlatformStoreError::InvalidTelemetryMetricKey(
            metric_key.to_owned(),
        ))
    }
}

fn canonical_incident(mut incident: NewAlertIncident) -> NewAlertIncident {
    incident.condition_started_at = canonical_postgres_timestamp(incident.condition_started_at);
    incident
}

fn canonical_notification(
    mut notification: NewNotificationOutboxEntry,
) -> NewNotificationOutboxEntry {
    notification.next_attempt_at = canonical_postgres_timestamp(notification.next_attempt_at);
    notification
}

enum AlertIncidentTransition {
    Open(DateTime<Utc>),
    Recover(DateTime<Utc>),
    Resolve(DateTime<Utc>),
    Remind(DateTime<Utc>),
}

impl AlertIncidentTransition {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Open(timestamp)
            | Self::Recover(timestamp)
            | Self::Resolve(timestamp)
            | Self::Remind(timestamp) => *timestamp,
        }
    }
}

impl AlertRepository for PlatformStore {
    fn load_active_rules<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AlertRule>, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { self.load_active_alert_rules().await })
    }

    fn claim_rule_event<'a>(
        &'a self,
        rule_id: uuid::Uuid,
        event_at: DateTime<Utc>,
        device_id: &'a str,
        boot_id: uuid::Uuid,
        sequence: u64,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            self.claim_alert_rule_event(rule_id, event_at, device_id, boot_id, sequence)
                .await
        })
    }
}

impl AlertEvaluationRepository for PlatformStore {
    fn evaluate_alert_events<'a>(
        &'a self,
        events: &'a [AlertEvaluationEvent],
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.evaluate_alert_events(events, evaluated_at).await })
    }

    fn evaluate_alert_windows<'a>(
        &'a self,
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.evaluate_alert_windows(evaluated_at).await })
    }
}

impl TopologyRepository for PlatformStore {
    fn register_device<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::register_device(self, tenant_id, device_id).await })
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
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::authorize_device_session(self, token_id, tenant_id, device_id).await
        })
    }

    fn authorize_gateway_token<'a>(
        &'a self,
        token_id: uuid::Uuid,
        tenant_id: uuid::Uuid,
        gateway_device_id: &'a str,
        child_device_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::authorize_gateway_token(
                self,
                token_id,
                tenant_id,
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

    fn mark_legacy_command_failed<'a>(
        &'a self,
        command_id: &'a str,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { PlatformStore::mark_legacy_command_failed(self, command_id, error).await },
        )
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
        tenant_id: uuid::Uuid,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::write_telemetry(self, tenant_id, event, received_at, topic).await
        })
    }
}

impl GatewayIngestRepository for PlatformStore {
    fn ingest_gateway<'a>(
        &'a self,
        request: GatewayIngestRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayIngestResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::ingest_gateway(self, request).await })
    }
}

impl TelemetryAggregateRepository for PlatformStore {
    fn average_metric<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        metric_key: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TelemetryAggregate>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::average_metric(self, tenant_id, device_id, metric_key, from, to).await
        })
    }
}

impl AuthorizationRepository for PlatformStore {
    fn authorization_subject<'a>(
        &'a self,
        user_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizationSubject>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::authorization_subject(self, user_id).await })
    }

    fn list_authorized_devices<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::list_authorized_devices(self, subject, after, limit).await
        })
    }

    fn authorized_device<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::authorized_device(self, subject, device_id).await })
    }

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

impl AlertIncidentRepository for PlatformStore {
    fn create_incident<'a>(
        &'a self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.create_incident(incident, opened_notification).await })
    }

    fn update_incident_last_value<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.update_incident_last_value(incident_id, expected_version, last_value, updated_at)
                .await
        })
    }

    fn open_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident(incident_id, expected_version, opened_at)
                .await
        })
    }

    fn open_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident_with_notification(
                incident_id,
                expected_version,
                opened_at,
                notification,
            )
            .await
        })
    }

    fn recover_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.recover_incident(incident_id, expected_version, recovery_started_at)
                .await
        })
    }

    fn resolve_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident(incident_id, expected_version, resolved_at)
                .await
        })
    }

    fn resolve_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident_with_notification(
                incident_id,
                expected_version,
                resolved_at,
                notification,
            )
            .await
        })
    }

    fn remind_incident<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident(incident_id, expected_version, reminded_at)
                .await
        })
    }

    fn remind_incident_with_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident_with_notification(
                incident_id,
                expected_version,
                reminded_at,
                notification,
            )
            .await
        })
    }

    fn enqueue_notification<'a>(
        &'a self,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<
        Box<dyn Future<Output = Result<NotificationOutboxRecord, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { self.enqueue_notification(incident_id, notification).await })
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

impl OAuthRepository for PlatformStore {
    fn register_client_secret<'a>(
        &'a self,
        secret: NewOAuthClientSecret,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::register_client_secret(self, secret).await })
    }

    fn issue_authorization_code<'a>(
        &'a self,
        code: NewOAuthAuthorizationCode,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::issue_authorization_code(self, code).await })
    }

    fn consume_authorization_code_and_issue_access_token<'a>(
        &'a self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            PlatformStore::consume_authorization_code_and_issue_access_token(self, exchange).await
        })
    }

    fn issue_client_credentials_access_token<'a>(
        &'a self,
        request: OAuthClientCredentialsToken,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            PlatformStore::issue_client_credentials_access_token(self, request).await
        })
    }

    fn resolve_access_token<'a>(
        &'a self,
        access_token: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::resolve_access_token(self, access_token, now).await })
    }
}

fn strongest_share_permission(rows: Vec<String>) -> Option<ResourcePermission> {
    rows.into_iter()
        .filter_map(|value| ResourcePermission::parse_share(&value))
        .max()
}

fn parse_authorized_device_timestamp(value: &str) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map(|timestamp| timestamp.and_utc())
        })
        .map_err(
            |source| PlatformStoreError::InvalidDeviceLastSeenTimestamp {
                value: value.to_owned(),
                source,
            },
        )
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

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn s256_code_challenge(code_verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()))
}

fn validate_oauth_access_token_expiry(
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    if expires_at <= issued_at {
        return Err(PlatformStoreError::InvalidOAuthAccessTokenExpiry);
    }
    Ok(())
}

async fn oauth_user_belongs_to_tenant(
    store: &PlatformStore,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
) -> Result<bool, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM users WHERE id = ? AND tenant_id = ?",
        )
        .bind(user_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .is_some()),
        PlatformStore::Timescale(pool) => Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2",
        )
        .bind(user_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .is_some()),
    }
}

fn oauth_client_secret_matches(stored_hashes: &[String], supplied_secret: Option<&str>) -> bool {
    if stored_hashes.is_empty() {
        return true;
    }
    let Some(supplied_secret) = supplied_secret else {
        return false;
    };
    let supplied_hash = sha256_hex(supplied_secret);
    let mut matched = 0_u8;
    for stored_hash in stored_hashes {
        matched |= supplied_hash
            .as_bytes()
            .ct_eq(stored_hash.as_bytes())
            .unwrap_u8();
    }
    matched != 0
}

fn oauth_access_token_record(
    app_id: String,
    tenant_id: String,
    user_id: String,
    scopes_json: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
    let app_id = app_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let tenant_id = tenant_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let user_id = user_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let scopes = serde_json::from_str(scopes_json)
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let scopes = canonical_application_scopes(scopes)
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    Ok(OAuthAccessTokenRecord {
        app_id,
        tenant_id,
        user_id: Some(user_id),
        scopes,
        issued_at,
        expires_at,
    })
}

fn parse_oauth_timestamp(value: &str) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)
}

fn oauth_resolved_access_token_record(
    app_id: String,
    tenant_id: String,
    user_id: Option<String>,
    scopes_json: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
    let app_id = app_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let tenant_id = tenant_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let user_id = user_id
        .map(|user_id| user_id.parse())
        .transpose()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let scopes = serde_json::from_str(scopes_json)
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let scopes = canonical_application_scopes(scopes)
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    Ok(OAuthAccessTokenRecord {
        app_id,
        tenant_id,
        user_id,
        scopes,
        issued_at,
        expires_at,
    })
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
        tenant_id: row
            .try_get::<String, _>("tenant_id")?
            .parse()
            .map_err(|_| PlatformStoreError::OAuthApplicationNotFound)?,
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
        tenant_id: row.try_get("tenant_id")?,
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

fn sqlite_alert_rule_record(row: SqliteRow) -> Result<AlertRule, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let rule = AlertRule {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidAlertRuleId(id))?,
        name: row.try_get("name")?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        kind: alert_rule_kind(&row.try_get::<String, _>("rule_type")?)?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison: alert_comparison(&row.try_get::<String, _>("comparison")?)?,
        threshold: row.try_get("threshold")?,
        window: row
            .try_get::<Option<i64>, _>("window_seconds")?
            .map(alert_duration)
            .transpose()?,
        for_duration: alert_duration(row.try_get("for_seconds")?)?,
        resolve_after: alert_duration(row.try_get("resolve_after_seconds")?)?,
        reopen_grace: alert_duration(row.try_get("reopen_grace_seconds")?)?,
        hysteresis: row.try_get("hysteresis")?,
        severity: alert_severity(&row.try_get::<String, _>("severity")?)?,
        reminder_interval: alert_positive_duration(
            row.try_get("reminder_interval_seconds")?,
            "reminder_interval_seconds",
        )?,
    };
    validate_alert_rule(&rule)
}

fn postgres_alert_rule_record(row: PgRow) -> Result<AlertRule, PlatformStoreError> {
    let rule = AlertRule {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        kind: alert_rule_kind(&row.try_get::<String, _>("rule_type")?)?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison: alert_comparison(&row.try_get::<String, _>("comparison")?)?,
        threshold: row.try_get("threshold")?,
        window: row
            .try_get::<Option<i32>, _>("window_seconds")?
            .map(i64::from)
            .map(alert_duration)
            .transpose()?,
        for_duration: alert_duration(i64::from(row.try_get::<i32, _>("for_seconds")?))?,
        resolve_after: alert_duration(i64::from(row.try_get::<i32, _>("resolve_after_seconds")?))?,
        reopen_grace: alert_duration(i64::from(row.try_get::<i32, _>("reopen_grace_seconds")?))?,
        hysteresis: row.try_get("hysteresis")?,
        severity: alert_severity(&row.try_get::<String, _>("severity")?)?,
        reminder_interval: alert_positive_duration(
            i64::from(row.try_get::<i32, _>("reminder_interval_seconds")?),
            "reminder_interval_seconds",
        )?,
    };
    validate_alert_rule(&rule)
}

fn alert_rule_kind(value: &str) -> Result<AlertRuleKind, PlatformStoreError> {
    match value {
        "event_threshold" => Ok(AlertRuleKind::EventThreshold),
        "window_average" => Ok(AlertRuleKind::WindowAverage),
        _ => Err(PlatformStoreError::InvalidAlertRuleKind(value.to_owned())),
    }
}

fn alert_comparison(value: &str) -> Result<AlertComparison, PlatformStoreError> {
    match value {
        "gt" => Ok(AlertComparison::GreaterThan),
        "gte" => Ok(AlertComparison::GreaterThanOrEqual),
        "lt" => Ok(AlertComparison::LessThan),
        "lte" => Ok(AlertComparison::LessThanOrEqual),
        _ => Err(PlatformStoreError::InvalidAlertRuleComparison(
            value.to_owned(),
        )),
    }
}

fn alert_severity(value: &str) -> Result<AlertSeverity, PlatformStoreError> {
    match value {
        "info" => Ok(AlertSeverity::Info),
        "warning" => Ok(AlertSeverity::Warning),
        "critical" => Ok(AlertSeverity::Critical),
        _ => Err(PlatformStoreError::InvalidAlertRuleSeverity(
            value.to_owned(),
        )),
    }
}

fn alert_duration(seconds: i64) -> Result<ChronoDuration, PlatformStoreError> {
    if seconds < 0 {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "duration",
            seconds,
        });
    }
    Ok(ChronoDuration::seconds(seconds))
}

fn alert_positive_duration(
    seconds: i64,
    field: &'static str,
) -> Result<ChronoDuration, PlatformStoreError> {
    if seconds <= 0 {
        return Err(PlatformStoreError::InvalidAlertRuleDuration { field, seconds });
    }
    Ok(ChronoDuration::seconds(seconds))
}

fn validate_alert_rule(rule: &AlertRule) -> Result<AlertRule, PlatformStoreError> {
    match (rule.kind, rule.window) {
        (AlertRuleKind::EventThreshold, Some(window)) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: window.num_seconds(),
            });
        }
        (AlertRuleKind::WindowAverage, None) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: 0,
            });
        }
        (AlertRuleKind::WindowAverage, Some(window)) if window < ChronoDuration::seconds(60) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: window.num_seconds(),
            });
        }
        _ => {}
    }
    if rule.threshold.is_nan() || rule.threshold.is_infinite() {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "threshold",
            seconds: 0,
        });
    }
    if rule
        .hysteresis
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "hysteresis",
            seconds: 0,
        });
    }
    Ok(rule.clone())
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
        created_at: row.try_get("created_at")?,
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
    pub created_at: DateTime<Utc>,
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
    fn as_str(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Resolved => "resolved",
            Self::Reminder => "reminder",
        }
    }

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

fn sqlite_connect_options(
    path: &Path,
    busy_timeout_ms: u64,
    create_if_missing: bool,
) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(create_if_missing)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_millis(busy_timeout_ms))
}

fn sqlite_backup_connect_options(path: &Path, busy_timeout_ms: u64) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .read_only(true)
        .busy_timeout(Duration::from_millis(busy_timeout_ms))
}

fn existing_pre_migration_backup(
    path: &Path,
    schema_version: i64,
) -> Result<Option<PathBuf>, SqliteStoreError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(SqliteStoreError::InvalidConfiguration)?;
    let parent = path
        .parent()
        .ok_or(SqliteStoreError::InvalidConfiguration)?;
    let prefix = format!("{file_name}.backup-v{schema_version}-");
    let mut backups = Vec::new();
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix))
        {
            backups.push(entry.path());
        }
    }
    backups.sort();
    Ok(backups.into_iter().next())
}

async fn backup_sqlite_pool(pool: &SqlitePool, path: &Path) -> Result<PathBuf, SqliteStoreError> {
    backup_sqlite_pool_with_prefix(pool, path, "backup").await
}

async fn backup_sqlite_pool_with_prefix(
    pool: &SqlitePool,
    path: &Path,
    prefix: &str,
) -> Result<PathBuf, SqliteStoreError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(SqliteStoreError::InvalidConfiguration)?;
    let backup_name = format!(
        "{file_name}.{prefix}-{}",
        Utc::now().format("%Y%m%dT%H%M%S%fZ")
    );
    let backup_path = path.with_file_name(backup_name);

    sqlx::query("VACUUM INTO ?")
        .bind(backup_path.to_string_lossy().as_ref())
        .execute(pool)
        .await?;
    #[cfg(unix)]
    fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600))?;
    Ok(backup_path)
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
        let options = sqlite_connect_options(path, configuration.sqlite_busy_timeout_ms, true);
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
        migrate_root_asset_name_uniqueness(&pool).await?;
        migrate_command_outbox_schema(&pool).await?;
        migrate_resource_authorization_schema(&pool).await?;
        sqlx::query(SET_SQLITE_PLATFORM_SCHEMA_VERSION)
            .execute(&pool)
            .await?;
        Ok(Self {
            pool,
            path: path.clone(),
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn backup(&self) -> Result<PathBuf, SqliteStoreError> {
        backup_sqlite_pool(&self.pool, &self.path).await
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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

    async fn create_incident(
        &self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO alert_incidents (
                id, rule_id, device_id, status, condition_started_at, opened_at, last_value
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT DO NOTHING",
        )
        .bind(incident.id.to_string())
        .bind(incident.rule_id.to_string())
        .bind(&incident.device_id)
        .bind(incident.status.as_str())
        .bind(incident.condition_started_at.to_rfc3339())
        .bind(
            (incident.status == AlertIncidentStatus::Open)
                .then(|| incident.condition_started_at.to_rfc3339()),
        )
        .bind(incident.last_value)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(None);
        }
        if let Some(notification) = opened_notification {
            sqlx::query(
                "INSERT INTO notification_outbox (
                    id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(notification.id.to_string())
            .bind(incident.id.to_string())
            .bind(notification.kind.as_str())
            .bind(notification.dedupe_key)
            .bind(notification.subject)
            .bind(notification.body)
            .bind(notification.next_attempt_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
        let row = sqlx::query(
            "SELECT id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                    opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                    state_version
             FROM alert_incidents WHERE id = ?",
        )
        .bind(incident.id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        sqlite_alert_incident_record(row).map(Some)
    }

    async fn update_incident_last_value(
        &self,
        incident_id: &str,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let row = sqlx::query(
            "UPDATE alert_incidents
             SET last_value = ?, state_version = state_version + 1, updated_at = ?
             WHERE id = ? AND state_version = ? AND status IN ('pending', 'open')
             RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        )
        .bind(last_value)
        .bind(updated_at.to_rfc3339())
        .bind(incident_id)
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition(
        &self,
        incident_id: &str,
        expected_version: i64,
        transition: AlertIncidentTransition,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let timestamp = transition.timestamp().to_rfc3339();
        let query = match transition {
            AlertIncidentTransition::Open(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'open', opened_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'open'
                       AND recovery_started_at IS NULL
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Resolve(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'resolved', resolved_at = ?,
                     recovery_started_at = COALESCE(recovery_started_at, ?), updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Remind(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET last_reminder_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
        };
        let row = query
            .bind(incident_id)
            .bind(expected_version)
            .fetch_optional(&self.pool)
            .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition_with_notification(
        &self,
        incident_id: &str,
        expected_version: i64,
        transition: AlertIncidentTransition,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let timestamp = transition.timestamp().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        let query = match transition {
            AlertIncidentTransition::Open(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'open', opened_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Resolve(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'resolved', resolved_at = ?,
                     recovery_started_at = COALESCE(recovery_started_at, ?), updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Remind(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET last_reminder_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => unreachable!(),
        };
        let row = query
            .bind(incident_id)
            .bind(expected_version)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(notification.id.to_string())
        .bind(incident_id)
        .bind(notification.kind.as_str())
        .bind(notification.dedupe_key)
        .bind(notification.subject)
        .bind(notification.body)
        .bind(notification.next_attempt_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        sqlite_alert_incident_record(row).map(Some)
    }

    async fn enqueue_notification(
        &self,
        incident_id: &str,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(dedupe_key) DO NOTHING",
        )
        .bind(notification.id.to_string())
        .bind(incident_id)
        .bind(notification.kind.as_str())
        .bind(&notification.dedupe_key)
        .bind(&notification.subject)
        .bind(&notification.body)
        .bind(notification.next_attempt_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT id, incident_id, kind, dedupe_key, subject, body, state,
                    next_attempt_at, lease_until, attempt_count, last_error, sent_at
             FROM notification_outbox WHERE dedupe_key = ?",
        )
        .bind(notification.dedupe_key)
        .fetch_one(&self.pool)
        .await?;
        notification_outbox_record(row)
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
                id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
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
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = self
            .write_telemetry_in_transaction(&mut transaction, tenant_id, event, received_at, topic)
            .await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn write_telemetry_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let received_at = received_at.to_rfc3339();
        sqlx::query(
            "UPDATE devices
             SET last_seen_at = CASE
                 WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
                 ELSE last_seen_at
             END
             WHERE tenant_id = ? AND device_id = ?",
        )
        .bind(&received_at)
        .bind(&received_at)
        .bind(tenant_id.to_string())
        .bind(&event.device_id)
        .execute(transaction.as_mut())
        .await?;
        let insert = sqlx::query(
            "INSERT OR IGNORE INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements,
                topic, gateway_device_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.event_at.to_rfc3339())
        .bind(&received_at)
        .bind(tenant_id.to_string())
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
                tenant_id,
                &event.device_id,
                metrics,
            )
            .await?;
            upsert_rollup(
                transaction,
                RollupTable::OneHour,
                bucket_start(event.event_at, 60 * 60),
                tenant_id,
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

async fn migrate_root_asset_name_uniqueness(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let duplicate_name = sqlx::query_scalar::<_, String>(
        "SELECT tenant_id || ':' || name
         FROM assets
         WHERE parent_asset_id IS NULL
         GROUP BY tenant_id, name
         HAVING COUNT(*) > 1
         ORDER BY name
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    if let Some(name) = duplicate_name {
        return Err(sqlx::Error::Protocol(format!(
            "duplicate tenant root asset name {name:?}; resolve duplicate root assets before migration"
        )));
    }
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS assets_tenant_root_name_unique_index
         ON assets (tenant_id, name)
         WHERE parent_asset_id IS NULL",
    )
    .execute(pool)
    .await?;
    Ok(())
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
        created_at: command_timestamp(&row, "created_at")?,
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

fn sqlite_alert_incident_record(row: SqliteRow) -> Result<AlertIncident, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let rule_id: String = row.try_get("rule_id")?;
    Ok(AlertIncident {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidIncidentId(id))?,
        rule_id: uuid::Uuid::parse_str(&rule_id)
            .map_err(|_| PlatformStoreError::InvalidIncidentId(rule_id))?,
        device_id: row.try_get("device_id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: incident_timestamp(&row, "condition_started_at")?,
        recovery_started_at: incident_optional_timestamp(&row, "recovery_started_at")?,
        opened_at: incident_optional_timestamp(&row, "opened_at")?,
        resolved_at: incident_optional_timestamp(&row, "resolved_at")?,
        last_value: row.try_get("last_value")?,
        last_notified_at: incident_optional_timestamp(&row, "last_notified_at")?,
        last_reminder_at: incident_optional_timestamp(&row, "last_reminder_at")?,
        state_version: row.try_get("state_version")?,
    })
}

fn postgres_alert_incident_record(row: PgRow) -> Result<AlertIncident, PlatformStoreError> {
    Ok(AlertIncident {
        id: row.try_get("id")?,
        rule_id: row.try_get("rule_id")?,
        device_id: row.try_get("device_id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: row.try_get("condition_started_at")?,
        recovery_started_at: row.try_get("recovery_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        last_value: row.try_get("last_value")?,
        last_notified_at: row.try_get("last_notified_at")?,
        last_reminder_at: row.try_get("last_reminder_at")?,
        state_version: i64::from(row.try_get::<i32, _>("state_version")?),
    })
}

fn incident_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    let value: String = row.try_get(column)?;
    parse_incident_timestamp(value, column)
}

fn incident_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| parse_incident_timestamp(value, column))
        .transpose()
}

fn parse_incident_timestamp(
    value: String,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|timestamp| timestamp.and_utc())
        })
        .map_err(|source| PlatformStoreError::InvalidIncidentTimestamp {
            column,
            value,
            source,
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
    parse_command_timestamp(&value).map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
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
            parse_command_timestamp(&value).map_err(|source| {
                SqliteStoreError::InvalidCommandTimestamp {
                    column,
                    value,
                    source,
                }
            })
        })
        .transpose()
}

fn parse_command_timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(&format!("{value} +00:00"), "%Y-%m-%d %H:%M:%S %z"))
        .map(|timestamp| timestamp.with_timezone(&Utc))
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
    tenant_id: uuid::Uuid,
    device_id: &str,
    metrics: MetricAverages,
) -> Result<(), sqlx::Error> {
    let query = match table {
        RollupTable::FiveMinute => {
            "INSERT INTO telemetry_rollups_5m (
                bucket_at, tenant_id, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, bucket_at, device_id) DO UPDATE SET
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
                bucket_at, tenant_id, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, bucket_at, device_id) DO UPDATE SET
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
    let mut query = sqlx::query(query)
        .bind(bucket_at)
        .bind(tenant_id.to_string())
        .bind(device_id);
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
