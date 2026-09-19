#![forbid(unsafe_code)]

mod audit;
mod contracts;
mod device_relations;
mod domain;
mod management;
mod public_api;
mod store;
mod tenant_identity;

use contracts::authorization_account_class;

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Timelike, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use sqlx::{
    PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction,
    postgres::PgRow,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
    types::Json,
};
use thiserror::Error;

const PLATFORM_POSTGRES_SCHEMA: &str = include_str!("../migrations/0001_platform.sql");
const SQLITE_PLATFORM_SCHEMA_VERSION: i64 = 2;
const SET_SQLITE_PLATFORM_SCHEMA_VERSION: &str = "PRAGMA user_version = 2";

pub use contracts::{
    AccountClass, AlertComparison, AlertEvaluationEvent, AlertEvaluationRepository,
    AlertEvaluationResult, AlertIncident, AlertIncidentRepository, AlertIncidentStatus, AlertRule,
    AlertRuleKind, AlertSeverity, ApplicationId, ApplicationKind, ApplicationRecord,
    ApplicationRepository, AuthenticatedDeviceToken, AuthorizationRepository, AuthorizationSubject,
    AuthorizedAssetListEntry, AuthorizedAssetSummary, AuthorizedDeviceListEntry,
    AuthorizedDeviceSummary, ClientId, CommandLifecycleRepository, CommandOutboxRecord,
    CommandOutboxState, CommandRepository, DeviceAuthorizationRepository, GatewayIngestEventKind,
    GatewayIngestRepository, GatewayIngestRequest, GatewayIngestResult,
    GatewayIngestValidationError, IdentityRepository, NewAlertIncident, NewApplication,
    NewCommandOutboxEntry, NewNotificationOutboxEntry, NewOAuthAuthorizationCode,
    NewOAuthClientSecret, NewResourcePermission, NewUserGroup, NotificationKind,
    NotificationOutboxRecord, NotificationOutboxState, NotificationRepository,
    OAuthAccessTokenRecord, OAuthAuthorizationCodeExchange, OAuthClientCredentialsToken,
    OAuthRepository, OwnershipTransferTarget, PermissionCreator, RedirectUri, ResourceAccess,
    ResourceAccessSource, ResourceKind, ResourcePermission, ResourcePermissionRecord,
    TelemetryAggregate, TelemetryAggregateRepository, TelemetryRepository,
    TenantAuthorizationError, TenantAuthorizationRepository, TenantUserGroup,
    TenantUserGroupMember, TopologyRepository, UserDeviceActivity, UserDeviceActivityRepository,
    UserDeviceAlert, UserDeviceTelemetry, UserGroup,
};
pub use domain::RetentionResult;
pub use store::{PlatformStore, PlatformStoreError, SqliteStore};

pub use audit::{
    AuditAction, AuditEvent, AuditEventCursor, AuditEventError, AuditEventRepository,
    AuditPrincipal, AuditTargetType,
};
pub use device_relations::{
    CreateDeviceRelation, DeviceRelation, DeviceRelationError, DeviceRelationRepository,
    RESERVED_GATEWAY_CHILD_RELATION_TYPE,
};
pub use management::{
    BUILT_IN_USER_WORKSPACE, CreateManagementAlertRule, CreateManagementAsset,
    CreateManagementAssetProfile, CreateManagementDeviceProfile, CreateManagementUser,
    DeviceTokenRecord, DeviceTokenRepository, DeviceTokenRepositoryError,
    MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT, MANAGEMENT_ALERT_LIST_LIMIT,
    MANAGEMENT_ALERT_RULE_LIST_LIMIT, ManagementAlert, ManagementAlertError,
    ManagementAlertIncident, ManagementAlertIncidentError, ManagementAlertIncidentRepository,
    ManagementAlertRepository, ManagementAlertRule, ManagementAlertRuleError,
    ManagementAlertRuleRepository, ManagementAsset, ManagementAssetError, ManagementAssetProfile,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementChildStatus, ManagementDevice, ManagementDeviceError, ManagementDeviceHealth,
    ManagementDeviceProfile, ManagementDeviceProfileError, ManagementDeviceProfileRepository,
    ManagementDeviceRepository, ManagementDeviceTopology, ManagementGatewayStatus, ManagementUser,
    ManagementUserError, ManagementUserRepository, ManagementUserRole, NewDeviceToken,
    NewOwnedDeviceToken, ProvisionManagementDevice, ProvisionManagementDeviceError,
    UpdateManagementAlertRule, UpdateManagementAsset, UpdateManagementAssetProfile,
    UpdateManagementDevice, UpdateManagementDeviceProfile, UpdateManagementUser,
};
pub use public_api::{
    NewPublicAsset, NewPublicDevice, PublicAlert, PublicApiRepository, PublicAsset,
    PublicAssetError, PublicDevice, PublicDeviceError, PublicPrincipal, PublicTelemetry,
};
#[cfg(feature = "test-support")]
pub use public_api::{PublicDeviceListHandoffHookGuard, install_public_device_list_handoff_hook};
pub use tenant_identity::{
    AccountStatus, NewSystemAccount, NewTenant, NewTenantAccount, SystemAccount,
    SystemAccountCredential, Tenant, TenantAccount, TenantAccountCredential, TenantIdentityError,
    TenantIdentityRepository, TenantStatus, TenantSummary, TenantUserCredential,
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
    gateway_topology_version INTEGER NOT NULL DEFAULT 0,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    owner_user_id TEXT,
    claimed_at TEXT,
    UNIQUE (device_id, tenant_id),
    FOREIGN KEY (asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_profile_id, tenant_id)
        REFERENCES device_profiles(id, tenant_id) ON DELETE RESTRICT,
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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id)
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
CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    app_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, app_key),
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (app_key, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
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
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    fields TEXT NOT NULL DEFAULT '{}',
    dashboard_defaults TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    telemetry_schema TEXT NOT NULL DEFAULT '{}',
    metric_mapping TEXT NOT NULL DEFAULT '{}',
    reporting_settings TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_profile_id TEXT,
    parent_asset_id TEXT,
    owner_user_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, parent_asset_id, name),
    FOREIGN KEY (asset_profile_id, tenant_id)
        REFERENCES asset_profiles(id, tenant_id) ON DELETE RESTRICT,
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
CREATE INDEX IF NOT EXISTS devices_tenant_device_profile_index
    ON devices (tenant_id, device_profile_id)
    WHERE device_profile_id IS NOT NULL;
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

CREATE TABLE IF NOT EXISTS device_relations (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    from_device_id TEXT NOT NULL,
    to_device_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tenant_id, from_device_id, to_device_id, relation_type),
    FOREIGN KEY (from_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK (from_device_id <> to_device_id),
    CHECK (
        length(relation_type) BETWEEN 1 AND 64
        AND relation_type NOT GLOB '*[^A-Za-z0-9_-]*'
        AND relation_type <> 'gateway_child'
    )
);
CREATE INDEX IF NOT EXISTS device_relations_tenant_list_index
    ON device_relations (tenant_id, relation_type, from_device_id, to_device_id, id);

CREATE TABLE IF NOT EXISTS user_groups (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    owner_user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (owner_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS user_group_members (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id, tenant_id)
        REFERENCES user_groups(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS user_group_members_tenant_user_group_index
    ON user_group_members (tenant_id, user_id, group_id);
CREATE TABLE IF NOT EXISTS resource_permissions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    subject_user_id TEXT,
    subject_group_id TEXT,
    asset_id TEXT,
    device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
    created_by_user_id TEXT,
    created_by_tenant_account_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TEXT,
    CHECK (
        (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
        OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
    ),
    CHECK (
        (asset_id IS NOT NULL AND device_id IS NULL)
        OR (asset_id IS NULL AND device_id IS NOT NULL)
    ),
    CHECK (
        (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
        OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
    ),
    CHECK (device_id IS NULL OR inherit_children = 0),
    FOREIGN KEY (subject_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (subject_group_id, tenant_id)
        REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (asset_id, tenant_id)
        REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_tenant_account_id, tenant_id)
        REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS resource_permissions_active_device_user_index
    ON resource_permissions (tenant_id, device_id, subject_user_id)
    WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_device_group_index
    ON resource_permissions (tenant_id, device_id, subject_group_id)
    WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_asset_user_index
    ON resource_permissions (tenant_id, asset_id, subject_user_id)
    WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_asset_group_index
    ON resource_permissions (tenant_id, asset_id, subject_group_id)
    WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_group_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    occurred_at TEXT NOT NULL,
    actor_principal_kind TEXT NOT NULL
        CHECK (actor_principal_kind IN ('system_account', 'tenant_account', 'user')),
    actor_principal_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    changes TEXT NOT NULL CHECK (json_type(changes) = 'object')
);
CREATE INDEX IF NOT EXISTS audit_events_tenant_occurred_at_id_index
    ON audit_events (tenant_id, occurred_at DESC, id DESC);
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_insert
BEFORE INSERT ON audit_events
FOR EACH ROW
WHEN EXISTS (SELECT 1 FROM audit_events WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_update
BEFORE UPDATE ON audit_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_delete
BEFORE DELETE ON audit_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;

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
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (tenant_id, rule_type, device_id)
    WHERE enabled = 1;
CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (tenant_id, created_at DESC, id)
    WHERE archived_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id TEXT NOT NULL,
    event_at TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    PRIMARY KEY (tenant_id, rule_id, event_at, device_id, boot_id, sequence),
    FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id TEXT NOT NULL,
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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (tenant_id, rule_id, device_id)
    WHERE status IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS alert_incidents_status_updated_index
    ON alert_incidents (tenant_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    incident_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, dedupe_key),
    FOREIGN KEY (incident_id, tenant_id)
        REFERENCES alert_incidents(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (tenant_id, state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
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
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (tenant_id, state, next_attempt_at)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS command_outbox_tenant_device_index
    ON command_outbox (tenant_id, device_id);
"#;

impl PlatformStore {
    pub async fn recent_user_device_activity(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
        limit: u32,
    ) -> Result<UserDeviceActivity, PlatformStoreError> {
        let limit = i64::from(limit.min(20));
        match self {
            Self::Sqlite(store) => {
                let tenant_id = tenant_id.to_string();
                let telemetry_rows = sqlx::query(
                    "SELECT event_at, measurements
                     FROM telemetry
                     WHERE tenant_id = ? AND device_id = ?
                     ORDER BY event_at DESC, sequence DESC
                     LIMIT ?",
                )
                .bind(&tenant_id)
                .bind(device_id)
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                let alert_rows = sqlx::query(
                    "SELECT rules.name AS rule_name, rules.severity, incidents.status, incidents.updated_at
                     FROM alert_incidents AS incidents
                     JOIN alert_rules AS rules
                       ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                     WHERE incidents.tenant_id = ? AND incidents.device_id = ?
                     ORDER BY incidents.updated_at DESC, incidents.id DESC
                     LIMIT ?",
                )
                .bind(&tenant_id)
                .bind(device_id)
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                Ok(UserDeviceActivity {
                    telemetry: telemetry_rows
                        .into_iter()
                        .map(|row| {
                            Ok(UserDeviceTelemetry {
                                event_at: parse_authorized_device_timestamp(
                                    &row.try_get::<String, _>("event_at")?,
                                )?,
                                measurements: row
                                    .try_get::<Json<serde_json::Value>, _>("measurements")?
                                    .0,
                            })
                        })
                        .collect::<Result<_, PlatformStoreError>>()?,
                    alerts: alert_rows
                        .into_iter()
                        .map(|row| {
                            Ok(UserDeviceAlert {
                                rule_name: row.try_get("rule_name")?,
                                severity: row.try_get("severity")?,
                                status: row.try_get("status")?,
                                updated_at: parse_authorized_device_timestamp(
                                    &row.try_get::<String, _>("updated_at")?,
                                )?,
                            })
                        })
                        .collect::<Result<_, PlatformStoreError>>()?,
                })
            }
            Self::Timescale(pool) => {
                let telemetry_rows = sqlx::query(
                    "SELECT event_at, measurements
                     FROM telemetry
                     WHERE tenant_id = $1 AND device_id = $2
                     ORDER BY event_at DESC, sequence DESC
                     LIMIT $3",
                )
                .bind(tenant_id)
                .bind(device_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                let alert_rows = sqlx::query(
                    "SELECT rules.name AS rule_name, rules.severity, incidents.status, incidents.updated_at
                     FROM alert_incidents AS incidents
                     JOIN alert_rules AS rules
                       ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                     WHERE incidents.tenant_id = $1 AND incidents.device_id = $2
                     ORDER BY incidents.updated_at DESC, incidents.id DESC
                     LIMIT $3",
                )
                .bind(tenant_id)
                .bind(device_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                Ok(UserDeviceActivity {
                    telemetry: telemetry_rows
                        .into_iter()
                        .map(|row| {
                            Ok(UserDeviceTelemetry {
                                event_at: row.try_get("event_at")?,
                                measurements: row
                                    .try_get::<Json<serde_json::Value>, _>("measurements")?
                                    .0,
                            })
                        })
                        .collect::<Result<_, PlatformStoreError>>()?,
                    alerts: alert_rows
                        .into_iter()
                        .map(|row| {
                            Ok(UserDeviceAlert {
                                rule_name: row.try_get("rule_name")?,
                                severity: row.try_get("severity")?,
                                status: row.try_get("status")?,
                                updated_at: row.try_get("updated_at")?,
                            })
                        })
                        .collect::<Result<_, PlatformStoreError>>()?,
                })
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

fn canonical_postgres_timestamp(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    timestamp
        .with_nanosecond(timestamp.nanosecond() / 1_000 * 1_000)
        .expect("a valid UTC timestamp can be represented at microsecond precision")
}

fn canonical_notification(
    mut notification: NewNotificationOutboxEntry,
) -> NewNotificationOutboxEntry {
    notification.next_attempt_at = canonical_postgres_timestamp(notification.next_attempt_at);
    notification
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

impl UserDeviceActivityRepository for PlatformStore {
    fn recent_user_device_activity<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<UserDeviceActivity, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            PlatformStore::recent_user_device_activity(self, tenant_id, device_id, limit).await
        })
    }
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

async fn migrate_platform_timescale(pool: &PgPool) -> Result<(), PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut *transaction)
        .await?;
    if let Some(table) = pre_tenant_platform_timescale_table(&mut transaction).await? {
        return Err(PlatformStoreError::ResetRequiredTimescaleSchema { table });
    }
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
    transaction.commit().await?;
    Ok(())
}

const SQLITE_PLATFORM_SCHEMA_TABLES: &[&str] = &[
    "devices",
    "telemetry",
    "gateway_event_receipts",
    "command_outbox",
    "telemetry_rollups_5m",
    "telemetry_rollups_1h",
    "api_access_tokens",
    "system_accounts",
    "tenants",
    "tenant_accounts",
    "users",
    "user_app_grants",
    "applications",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "assets",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "device_tokens",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
];

const SQLITE_PLATFORM_TENANT_TABLES: &[&str] = &[
    "devices",
    "telemetry",
    "gateway_event_receipts",
    "telemetry_rollups_5m",
    "telemetry_rollups_1h",
    "tenant_accounts",
    "users",
    "user_app_grants",
    "applications",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "assets",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
];

const TIMESCALE_PLATFORM_SCHEMA_TABLES: &[&str] = &[
    "api_access_tokens",
    "system_accounts",
    "tenants",
    "tenant_accounts",
    "users",
    "user_app_grants",
    "applications",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "assets",
    "devices",
    "device_tokens",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "device_runtime_state",
    "telemetry",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
    "gateway_event_receipts",
];

const TIMESCALE_PLATFORM_TENANT_TABLES: &[&str] = &[
    "tenant_accounts",
    "users",
    "user_app_grants",
    "applications",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "assets",
    "devices",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "device_runtime_state",
    "telemetry",
    "gateway_event_receipts",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
];

const LEGACY_RESOURCE_AUTHORIZATION_TABLES: &[&str] = &["resource_grants", "resource_shares"];

async fn pre_tenant_platform_sqlite_table(
    pool: &SqlitePool,
) -> Result<Option<String>, sqlx::Error> {
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    if let Some(table) = tables
        .iter()
        .find(|table| LEGACY_RESOURCE_AUTHORIZATION_TABLES.contains(&table.as_str()))
    {
        return Ok(Some(table.clone()));
    }
    let platform_tables = tables
        .into_iter()
        .filter(|table| SQLITE_PLATFORM_SCHEMA_TABLES.contains(&table.as_str()))
        .collect::<Vec<_>>();
    let Some(first_table) = platform_tables.first() else {
        return Ok(None);
    };
    if !platform_tables
        .iter()
        .any(|table| table.as_str() == "tenants")
    {
        return Ok(Some(first_table.clone()));
    }
    for &table in SQLITE_PLATFORM_TENANT_TABLES {
        if platform_tables
            .iter()
            .any(|existing| existing.as_str() == table)
        {
            if !sqlite_table_has_non_null_tenant_id(pool, table).await?
                || !sqlite_tenant_constraints_are_complete(pool, table).await?
            {
                return Ok(Some(table.to_owned()));
            }
        }
    }
    if platform_tables
        .iter()
        .any(|table| table.as_str() == "tenant_accounts")
        && !sqlite_tenant_account_creator_attribution_constraints_are_complete(pool).await?
    {
        return Ok(Some("tenant_accounts".to_owned()));
    }
    if platform_tables
        .iter()
        .any(|table| table.as_str() == "resource_permissions")
        && !sqlite_resource_permission_creator_attribution_constraints_are_complete(pool).await?
    {
        return Ok(Some("resource_permissions".to_owned()));
    }
    Ok(None)
}

async fn sqlite_tenant_account_creator_attribution_constraints_are_complete(
    pool: &SqlitePool,
) -> Result<bool, sqlx::Error> {
    sqlite_table_has_unique_columns(pool, "tenant_accounts", &["id", "tenant_id"]).await
}

async fn sqlite_resource_permission_creator_attribution_constraints_are_complete(
    pool: &SqlitePool,
) -> Result<bool, sqlx::Error> {
    if !sqlite_table_column_is_nullable(pool, "resource_permissions", "created_by_user_id").await?
        || !sqlite_table_column_is_nullable(
            pool,
            "resource_permissions",
            "created_by_tenant_account_id",
        )
        .await?
    {
        return Ok(false);
    }
    if !sqlite_table_has_composite_foreign_key(
        pool,
        "resource_permissions",
        "users",
        &[("created_by_user_id", "id"), ("tenant_id", "tenant_id")],
    )
    .await?
    {
        return Ok(false);
    }
    if !sqlite_table_has_composite_foreign_key(
        pool,
        "resource_permissions",
        "tenant_accounts",
        &[
            ("created_by_tenant_account_id", "id"),
            ("tenant_id", "tenant_id"),
        ],
    )
    .await?
    {
        return Ok(false);
    }
    sqlite_table_has_creator_attribution_xor_check(pool, "resource_permissions").await
}

async fn sqlite_table_column_is_nullable(
    pool: &SqlitePool,
    table: &str,
    column_name: &str,
) -> Result<bool, sqlx::Error> {
    let is_nullable: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM pragma_table_info(?)
             WHERE name = ? AND \"notnull\" = 0
         )",
    )
    .bind(table)
    .bind(column_name)
    .fetch_one(pool)
    .await?;
    Ok(is_nullable != 0)
}

async fn sqlite_table_has_unique_columns(
    pool: &SqlitePool,
    table: &str,
    expected_columns: &[&str],
) -> Result<bool, sqlx::Error> {
    let indexes = sqlx::query(
        "SELECT name
         FROM pragma_index_list(?)
         WHERE \"unique\" = 1 AND \"partial\" = 0",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    for index in indexes {
        let index_name: String = index.try_get("name")?;
        let columns = sqlx::query("SELECT name FROM pragma_index_info(?) ORDER BY seqno")
            .bind(index_name)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<Option<String>, _>("name"))
            .collect::<Result<Vec<_>, _>>()?;
        if columns
            .iter()
            .map(|column| column.as_deref())
            .eq(expected_columns.iter().copied().map(Some))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn sqlite_table_has_creator_attribution_xor_check(
    pool: &SqlitePool,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let table_sql: Option<String> = sqlx::query_scalar(
        "SELECT sql
         FROM sqlite_master
         WHERE type = 'table' AND name = ?",
    )
    .bind(table)
    .fetch_optional(pool)
    .await?;
    Ok(table_sql.is_some_and(|sql| creator_attribution_xor_check_is_present(&sql)))
}

fn creator_attribution_xor_check_is_present(schema_sql: &str) -> bool {
    let normalized = schema_sql
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized.contains(
        "created_by_user_idisnotnullandcreated_by_tenant_account_idisnullor\
         created_by_user_idisnullandcreated_by_tenant_account_idisnotnull",
    )
}

async fn sqlite_table_has_non_null_tenant_id(
    pool: &SqlitePool,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let has_tenant_id: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM pragma_table_info(?)
             WHERE name = 'tenant_id' AND \"notnull\" = 1
         )",
    )
    .bind(table)
    .fetch_one(pool)
    .await?;
    Ok(has_tenant_id != 0)
}

async fn sqlite_tenant_constraints_are_complete(
    pool: &SqlitePool,
    table: &str,
) -> Result<bool, sqlx::Error> {
    if !matches!(
        table,
        "alert_rules"
            | "alert_rule_event_evaluations"
            | "alert_incidents"
            | "notification_outbox"
            | "command_outbox"
    ) {
        return Ok(true);
    }
    if !sqlite_table_has_composite_foreign_key(pool, table, "tenants", &[("tenant_id", "id")])
        .await?
    {
        return Ok(false);
    }
    let constraints = match table {
        "alert_rules" => {
            sqlite_table_has_composite_foreign_key(
                pool,
                table,
                "devices",
                &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
            )
            .await?
        }
        "alert_rule_event_evaluations" => {
            sqlite_table_has_composite_foreign_key(
                pool,
                table,
                "alert_rules",
                &[("rule_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && sqlite_table_has_composite_foreign_key(
                    pool,
                    table,
                    "devices",
                    &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
                )
                .await?
        }
        "alert_incidents" => {
            sqlite_table_has_composite_foreign_key(
                pool,
                table,
                "alert_rules",
                &[("rule_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && sqlite_table_has_composite_foreign_key(
                    pool,
                    table,
                    "devices",
                    &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
                )
                .await?
        }
        "notification_outbox" => {
            sqlite_table_has_composite_foreign_key(
                pool,
                table,
                "alert_incidents",
                &[("incident_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && sqlite_notification_dedupe_constraints_are_complete(pool, table).await?
        }
        "command_outbox" => {
            sqlite_table_has_composite_foreign_key(
                pool,
                table,
                "devices",
                &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
            )
            .await?
        }
        _ => true,
    };
    Ok(constraints)
}

async fn sqlite_table_has_composite_foreign_key(
    pool: &SqlitePool,
    table: &str,
    target_table: &str,
    columns: &[(&str, &str)],
) -> Result<bool, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, \"table\" AS target_table, \"from\" AS source_column, \"to\" AS target_column
         FROM pragma_foreign_key_list(?)
         ORDER BY id, seq",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let mut foreign_keys: Vec<(i64, String, Vec<(String, String)>)> = Vec::new();
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let target: String = row.try_get("target_table")?;
        let source_column: String = row.try_get("source_column")?;
        let target_column: String = row.try_get("target_column")?;
        if let Some((previous_id, _, mapped_columns)) = foreign_keys.last_mut()
            && *previous_id == id
        {
            mapped_columns.push((source_column, target_column));
        } else {
            foreign_keys.push((id, target, vec![(source_column, target_column)]));
        }
    }
    Ok(foreign_keys.iter().any(|(_, target, mapped_columns)| {
        target == target_table
            && mapped_columns
                .iter()
                .map(|(source, target)| (source.as_str(), target.as_str()))
                .eq(columns.iter().copied())
    }))
}

async fn sqlite_notification_dedupe_constraints_are_complete(
    pool: &SqlitePool,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let indexes = sqlx::query(
        "SELECT name, \"partial\" AS is_partial
         FROM pragma_index_list(?)
         WHERE \"unique\" = 1",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    let mut has_tenant_dedupe_key = false;
    for index in indexes {
        let index_name: String = index.try_get("name")?;
        let is_partial: i64 = index.try_get("is_partial")?;
        if is_partial != 0 {
            return Ok(false);
        }
        let index_columns = sqlx::query("SELECT name FROM pragma_index_info(?) ORDER BY seqno")
            .bind(index_name)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<Option<String>, _>("name"))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(index_columns) = index_columns.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(false);
        };
        if index_columns.iter().any(|column| column == "dedupe_key") {
            if !index_columns.iter().any(|column| column == "tenant_id") {
                return Ok(false);
            }
            if index_columns
                .iter()
                .map(String::as_str)
                .eq(["tenant_id", "dedupe_key"].into_iter())
            {
                has_tenant_dedupe_key = true;
            }
        }
    }
    Ok(has_tenant_dedupe_key)
}

async fn pre_tenant_platform_timescale_table(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Option<String>, sqlx::Error> {
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano' AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .fetch_all(&mut **transaction)
    .await?;
    if let Some(table) = tables
        .iter()
        .find(|table| LEGACY_RESOURCE_AUTHORIZATION_TABLES.contains(&table.as_str()))
    {
        return Ok(Some(table.clone()));
    }
    let platform_tables = tables
        .into_iter()
        .filter(|table| TIMESCALE_PLATFORM_SCHEMA_TABLES.contains(&table.as_str()))
        .collect::<Vec<_>>();
    let Some(first_table) = platform_tables.first() else {
        return Ok(None);
    };
    if !platform_tables
        .iter()
        .any(|table| table.as_str() == "tenants")
    {
        return Ok(Some(first_table.clone()));
    }
    for &table in TIMESCALE_PLATFORM_TENANT_TABLES {
        if platform_tables
            .iter()
            .any(|existing| existing.as_str() == table)
        {
            let has_tenant_id = timescale_table_has_non_null_tenant_id(transaction, table).await?;
            let tenant_constraints_are_complete = if has_tenant_id {
                timescale_tenant_constraints_are_complete(transaction, table).await?
            } else {
                false
            };
            if !has_tenant_id || !tenant_constraints_are_complete {
                return Ok(Some(table.to_owned()));
            }
        }
    }
    if platform_tables
        .iter()
        .any(|table| table.as_str() == "tenant_accounts")
        && !timescale_tenant_account_creator_attribution_constraints_are_complete(transaction)
            .await?
    {
        return Ok(Some("tenant_accounts".to_owned()));
    }
    if platform_tables
        .iter()
        .any(|table| table.as_str() == "resource_permissions")
        && !timescale_resource_permission_creator_attribution_constraints_are_complete(transaction)
            .await?
    {
        return Ok(Some("resource_permissions".to_owned()));
    }
    Ok(None)
}

async fn timescale_tenant_account_creator_attribution_constraints_are_complete(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<bool, sqlx::Error> {
    timescale_table_has_unique_columns(transaction, "tenant_accounts", &["id", "tenant_id"]).await
}

async fn timescale_resource_permission_creator_attribution_constraints_are_complete(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<bool, sqlx::Error> {
    if !timescale_table_column_is_nullable(
        transaction,
        "resource_permissions",
        "created_by_user_id",
    )
    .await?
        || !timescale_table_column_is_nullable(
            transaction,
            "resource_permissions",
            "created_by_tenant_account_id",
        )
        .await?
    {
        return Ok(false);
    }
    if !timescale_table_has_composite_foreign_key(
        transaction,
        "resource_permissions",
        "users",
        &[("created_by_user_id", "id"), ("tenant_id", "tenant_id")],
    )
    .await?
    {
        return Ok(false);
    }
    if !timescale_table_has_composite_foreign_key(
        transaction,
        "resource_permissions",
        "tenant_accounts",
        &[
            ("created_by_tenant_account_id", "id"),
            ("tenant_id", "tenant_id"),
        ],
    )
    .await?
    {
        return Ok(false);
    }
    timescale_table_has_creator_attribution_xor_check(transaction, "resource_permissions").await
}

async fn timescale_table_has_unique_columns(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    expected_columns: &[&str],
) -> Result<bool, sqlx::Error> {
    let unique_columns = sqlx::query(
        "SELECT array_agg(attribute.attname::text ORDER BY key_column.ordinality) AS key_columns
         FROM pg_catalog.pg_constraint AS constraint_row
         JOIN pg_catalog.pg_class AS relation ON relation.oid = constraint_row.conrelid
         JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         CROSS JOIN LATERAL unnest(constraint_row.conkey) WITH ORDINALITY
             AS key_column(attnum, ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
             ON attribute.attrelid = constraint_row.conrelid
            AND attribute.attnum = key_column.attnum
            AND NOT attribute.attisdropped
         WHERE constraint_row.contype IN ('p', 'u')
           AND namespace.nspname = 'iot_nano'
           AND relation.relname = $1
         GROUP BY constraint_row.oid",
    )
    .bind(table)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(unique_columns.into_iter().any(|row| {
        row.try_get::<Vec<String>, _>("key_columns")
            .is_ok_and(|columns| {
                columns
                    .iter()
                    .map(String::as_str)
                    .eq(expected_columns.iter().copied())
            })
    }))
}

async fn timescale_table_column_is_nullable(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    column_name: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM pg_catalog.pg_attribute AS attribute
             JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'iot_nano'
               AND relation.relname = $1
               AND relation.relkind IN ('r', 'p')
               AND attribute.attname = $2
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
               AND NOT attribute.attnotnull
         )",
    )
    .bind(table)
    .bind(column_name)
    .fetch_one(&mut **transaction)
    .await
}

async fn timescale_table_has_creator_attribution_xor_check(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let checks = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(constraint_row.oid)
         FROM pg_catalog.pg_constraint AS constraint_row
         JOIN pg_catalog.pg_class AS relation ON relation.oid = constraint_row.conrelid
         JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         WHERE constraint_row.contype = 'c'
           AND namespace.nspname = 'iot_nano'
           AND relation.relname = $1",
    )
    .bind(table)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(checks
        .iter()
        .any(|check| creator_attribution_xor_check_is_present(check)))
}

async fn timescale_table_has_non_null_tenant_id(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM pg_catalog.pg_attribute AS attribute
             JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'iot_nano'
               AND relation.relname = $1
               AND relation.relkind IN ('r', 'p')
               AND attribute.attname = 'tenant_id'
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
               AND attribute.attnotnull
         )",
    )
    .bind(table)
    .fetch_one(&mut **transaction)
    .await
}

async fn timescale_tenant_constraints_are_complete(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
) -> Result<bool, sqlx::Error> {
    if !matches!(
        table,
        "alert_rules"
            | "alert_rule_event_evaluations"
            | "alert_incidents"
            | "notification_outbox"
            | "command_outbox"
    ) {
        return Ok(true);
    }
    if !timescale_table_has_composite_foreign_key(
        transaction,
        table,
        "tenants",
        &[("tenant_id", "id")],
    )
    .await?
    {
        return Ok(false);
    }
    let constraints = match table {
        "alert_rules" => {
            timescale_table_has_composite_foreign_key(
                transaction,
                table,
                "devices",
                &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
            )
            .await?
        }
        "alert_rule_event_evaluations" => {
            timescale_table_has_composite_foreign_key(
                transaction,
                table,
                "alert_rules",
                &[("rule_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && timescale_table_has_composite_foreign_key(
                    transaction,
                    table,
                    "devices",
                    &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
                )
                .await?
        }
        "alert_incidents" => {
            timescale_table_has_composite_foreign_key(
                transaction,
                table,
                "alert_rules",
                &[("rule_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && timescale_table_has_composite_foreign_key(
                    transaction,
                    table,
                    "devices",
                    &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
                )
                .await?
        }
        "notification_outbox" => {
            timescale_table_has_composite_foreign_key(
                transaction,
                table,
                "alert_incidents",
                &[("incident_id", "id"), ("tenant_id", "tenant_id")],
            )
            .await?
                && timescale_notification_dedupe_constraints_are_complete(transaction, table)
                    .await?
        }
        "command_outbox" => {
            timescale_table_has_composite_foreign_key(
                transaction,
                table,
                "devices",
                &[("device_id", "device_id"), ("tenant_id", "tenant_id")],
            )
            .await?
        }
        _ => true,
    };
    Ok(constraints)
}

async fn timescale_table_has_composite_foreign_key(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    target_table: &str,
    columns: &[(&str, &str)],
) -> Result<bool, sqlx::Error> {
    let expected_columns = columns
        .iter()
        .map(|(source, target)| (source.to_string(), target.to_string()))
        .collect::<Vec<_>>();
    let foreign_keys = sqlx::query(
        "SELECT array_agg(source_attribute.attname::text ORDER BY source_key.ordinality)
                    AS source_columns,
                array_agg(target_attribute.attname::text ORDER BY source_key.ordinality)
                    AS target_columns
         FROM pg_catalog.pg_constraint AS foreign_key
         JOIN pg_catalog.pg_class AS source_relation ON source_relation.oid = foreign_key.conrelid
         JOIN pg_catalog.pg_namespace AS source_namespace
             ON source_namespace.oid = source_relation.relnamespace
         JOIN pg_catalog.pg_class AS target_relation ON target_relation.oid = foreign_key.confrelid
         JOIN pg_catalog.pg_namespace AS target_namespace
             ON target_namespace.oid = target_relation.relnamespace
         CROSS JOIN LATERAL unnest(foreign_key.conkey) WITH ORDINALITY
             AS source_key(attnum, ordinality)
         JOIN LATERAL unnest(foreign_key.confkey) WITH ORDINALITY
             AS target_key(attnum, ordinality)
             ON target_key.ordinality = source_key.ordinality
         JOIN pg_catalog.pg_attribute AS source_attribute
             ON source_attribute.attrelid = foreign_key.conrelid
            AND source_attribute.attnum = source_key.attnum
            AND NOT source_attribute.attisdropped
         JOIN pg_catalog.pg_attribute AS target_attribute
             ON target_attribute.attrelid = foreign_key.confrelid
            AND target_attribute.attnum = target_key.attnum
            AND NOT target_attribute.attisdropped
         WHERE foreign_key.contype = 'f'
           AND foreign_key.convalidated
           AND source_namespace.nspname = 'iot_nano'
           AND source_relation.relname = $1
           AND target_namespace.nspname = 'iot_nano'
           AND target_relation.relname = $2
         GROUP BY foreign_key.oid",
    )
    .bind(table)
    .bind(target_table)
    .fetch_all(&mut **transaction)
    .await?;

    for foreign_key in foreign_keys {
        let source_columns: Vec<String> = foreign_key.try_get("source_columns")?;
        let target_columns: Vec<String> = foreign_key.try_get("target_columns")?;
        let actual_columns = source_columns
            .iter()
            .cloned()
            .zip(target_columns.iter().cloned())
            .collect::<Vec<_>>();
        if source_columns.len() == expected_columns.len()
            && target_columns.len() == expected_columns.len()
            && actual_columns
                .iter()
                .all(|column| expected_columns.contains(column))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn timescale_notification_dedupe_constraints_are_complete(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
) -> Result<bool, sqlx::Error> {
    let has_partial_or_expression_unique_index: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM pg_catalog.pg_index AS index_row
             JOIN pg_catalog.pg_class AS relation ON relation.oid = index_row.indrelid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'iot_nano'
               AND relation.relname = $1
               AND index_row.indisunique
               AND index_row.indisvalid
               AND index_row.indisready
               AND index_row.indislive
               AND (index_row.indpred IS NOT NULL OR index_row.indexprs IS NOT NULL)
         )",
    )
    .bind(table)
    .fetch_one(&mut **transaction)
    .await?;
    if has_partial_or_expression_unique_index {
        return Ok(false);
    }
    let indexes = sqlx::query(
        "SELECT array_agg(attribute.attname::text ORDER BY key_column.ordinality) AS key_columns,
                bool_or(index_row.indpred IS NOT NULL) AS is_partial
         FROM pg_catalog.pg_index AS index_row
         JOIN pg_catalog.pg_class AS relation ON relation.oid = index_row.indrelid
         JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         CROSS JOIN LATERAL unnest(index_row.indkey) WITH ORDINALITY
             AS key_column(attnum, ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
             ON attribute.attrelid = index_row.indrelid
            AND attribute.attnum = key_column.attnum
            AND NOT attribute.attisdropped
         WHERE namespace.nspname = 'iot_nano'
           AND relation.relname = $1
           AND index_row.indisunique
           AND index_row.indisvalid
           AND index_row.indisready
           AND index_row.indislive
           AND index_row.indexprs IS NULL
           AND key_column.ordinality <= index_row.indnkeyatts
         GROUP BY index_row.indexrelid",
    )
    .bind(table)
    .fetch_all(&mut **transaction)
    .await?;

    let mut has_tenant_dedupe_key = false;
    for index in indexes {
        let key_columns: Vec<String> = index.try_get("key_columns")?;
        let is_partial: bool = index.try_get("is_partial")?;
        if key_columns.iter().any(|column| column == "dedupe_key") {
            if is_partial || !key_columns.iter().any(|column| column == "tenant_id") {
                return Ok(false);
            }
            if key_columns == ["tenant_id", "dedupe_key"] {
                has_tenant_dedupe_key = true;
            }
        }
    }
    Ok(has_tenant_dedupe_key)
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
        if let Some(table) = pre_tenant_platform_sqlite_table(&pool).await? {
            pool.close().await;
            return Err(SqliteStoreError::ResetRequired { table });
        }
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&pool)
            .await?;
        let schema_version = sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        let audit_events_existed = sqlite_audit_events_table_exists(&pool).await?;
        sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await?;
        if schema_version < SQLITE_PLATFORM_SCHEMA_VERSION && audit_events_existed {
            migrate_audit_events_schema(&pool).await?;
        }
        migrate_root_asset_name_uniqueness(&pool).await?;
        migrate_command_outbox_schema(&pool).await?;
        migrate_resource_authorization_schema(&pool).await?;
        migrate_gateway_topology_schema(&pool).await?;
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
}

async fn migrate_command_outbox_schema(pool: &SqlitePool) -> Result<(), SqliteStoreError> {
    let columns = sqlx::query("PRAGMA table_info(command_outbox)")
        .fetch_all(pool)
        .await?;
    let column_names = columns
        .iter()
        .map(|column| column.try_get::<String, _>("name"))
        .collect::<Result<Vec<_>, _>>()?;
    let has_tenant_id = column_names.iter().any(|name| name == "tenant_id");
    if !has_tenant_id {
        return Err(SqliteStoreError::ResetRequired {
            table: "command_outbox".to_owned(),
        });
    }
    if sqlite_command_outbox_has_invalid_uuid_identifiers(pool).await? {
        return Err(SqliteStoreError::ResetRequired {
            table: "command_outbox".to_owned(),
        });
    }
    if column_names.iter().any(|name| name == "mode") {
        refresh_command_outbox_expiring_index(pool).await?;
        return Ok(());
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
            tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
            device_id TEXT NOT NULL,
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
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (device_id, tenant_id)
                REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
        )",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO command_outbox_rebuild (
            id, tenant_id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, created_at
         )
         SELECT
            id, tenant_id, device_id, method, params, 'one_way', state, expires_at, next_attempt_at,
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
         ON command_outbox (tenant_id, state, next_attempt_at)
         WHERE state = 'queued'",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(refresh_command_outbox_expiring_index(pool).await?)
}

async fn sqlite_command_outbox_has_invalid_uuid_identifiers(
    pool: &SqlitePool,
) -> Result<bool, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT CAST(id AS TEXT) AS id, CAST(tenant_id AS TEXT) AS tenant_id
         FROM command_outbox",
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let id: Option<String> = row.try_get("id")?;
        let tenant_id: Option<String> = row.try_get("tenant_id")?;
        let (Some(id), Some(tenant_id)) = (id, tenant_id) else {
            return Ok(true);
        };
        if uuid::Uuid::parse_str(&id).is_err() || uuid::Uuid::parse_str(&tenant_id).is_err() {
            return Ok(true);
        }
    }
    Ok(false)
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
             WHERE owner_user_id IS NOT NULL;",
    )
    .execute(pool)
    .await
    .map(|_| ())
}

async fn migrate_gateway_topology_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if !sqlite_table_has_column(pool, "devices", "gateway_topology_version").await? {
        sqlx::query(
            "ALTER TABLE devices
             ADD COLUMN gateway_topology_version INTEGER NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn migrate_audit_events_schema(pool: &SqlitePool) -> Result<(), SqliteStoreError> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let rows = sqlx::query(
        "SELECT
             id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
             action, target_type, target_id, changes
         FROM audit_events",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let occurred_at: String = row.try_get("occurred_at")?;
        let canonical_occurred_at = canonical_sqlite_audit_occurred_at(&occurred_at)?;
        events.push((
            row.try_get::<String, _>("id")?,
            row.try_get::<String, _>("tenant_id")?,
            canonical_occurred_at,
            row.try_get::<String, _>("actor_principal_kind")?,
            row.try_get::<String, _>("actor_principal_id")?,
            row.try_get::<String, _>("action")?,
            row.try_get::<String, _>("target_type")?,
            row.try_get::<String, _>("target_id")?,
            row.try_get::<String, _>("changes")?,
        ));
    }

    let non_object_changes: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM audit_events WHERE json_type(changes) IS NOT 'object'
         )",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if non_object_changes {
        return Err(SqliteStoreError::AuditChangesNotObject);
    }
    sqlx::raw_sql(
        "CREATE TABLE audit_events_rebuild (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             occurred_at TEXT NOT NULL,
             actor_principal_kind TEXT NOT NULL
                 CHECK (actor_principal_kind IN ('system_account', 'tenant_account', 'user')),
             actor_principal_id TEXT NOT NULL,
             action TEXT NOT NULL,
             target_type TEXT NOT NULL,
             target_id TEXT NOT NULL,
             changes TEXT NOT NULL CHECK (json_type(changes) = 'object')
         );",
    )
    .execute(&mut *transaction)
    .await?;
    for (
        id,
        tenant_id,
        occurred_at,
        actor_principal_kind,
        actor_principal_id,
        action,
        target_type,
        target_id,
        changes,
    ) in events
    {
        sqlx::query(
            "INSERT INTO audit_events_rebuild (
                id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                action, target_type, target_id, changes
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(tenant_id)
        .bind(occurred_at)
        .bind(actor_principal_kind)
        .bind(actor_principal_id)
        .bind(action)
        .bind(target_type)
        .bind(target_id)
        .bind(changes)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::raw_sql(
        "DROP TABLE audit_events;
         ALTER TABLE audit_events_rebuild RENAME TO audit_events;
         CREATE INDEX audit_events_tenant_occurred_at_id_index
             ON audit_events (tenant_id, occurred_at DESC, id DESC);
         CREATE TRIGGER audit_events_immutable_insert
         BEFORE INSERT ON audit_events
         FOR EACH ROW
         WHEN EXISTS (SELECT 1 FROM audit_events WHERE id = NEW.id)
         BEGIN
             SELECT RAISE(ABORT, 'audit_events are immutable');
         END;
         CREATE TRIGGER audit_events_immutable_update
         BEFORE UPDATE ON audit_events
         FOR EACH ROW
         BEGIN
             SELECT RAISE(ABORT, 'audit_events are immutable');
         END;
         CREATE TRIGGER audit_events_immutable_delete
         BEFORE DELETE ON audit_events
         FOR EACH ROW
         BEGIN
             SELECT RAISE(ABORT, 'audit_events are immutable');
         END;",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn sqlite_audit_events_table_exists(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'audit_events'
         )",
    )
    .fetch_one(pool)
    .await
}

fn canonical_sqlite_audit_occurred_at(value: &str) -> Result<String, SqliteStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| {
            timestamp
                .with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Nanos, true)
        })
        .map_err(|_| SqliteStoreError::InvalidAuditTimestamp(value.to_owned()))
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
    sqlx::query("DROP INDEX IF EXISTS command_outbox_due_index")
        .execute(&mut *connection)
        .await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_expiring_index")
        .execute(&mut *connection)
        .await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_tenant_device_index")
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_due_index
         ON command_outbox (tenant_id, state, next_attempt_at)
         WHERE state = 'queued'",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_tenant_device_index
         ON command_outbox (tenant_id, device_id)",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_expiring_index
         ON command_outbox (tenant_id, expires_at)
         WHERE state IN ('queued', 'leased')
            OR (state = 'published_to_broker' AND mode = 'two_way')",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum SqliteStoreError {
    #[error("SQLite storage configuration is invalid")]
    InvalidConfiguration,
    #[error("invalid command outbox state: {0}")]
    InvalidCommandState(String),
    #[error("invalid command outbox tenant ID")]
    InvalidCommandTenantId,
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
    #[error(
        "platform SQLite schema table {table:?} predates tenant scoping; reset the development database before starting iot-nano"
    )]
    ResetRequired { table: String },
    #[error("audit_events contains a non-object changes value and cannot be upgraded")]
    AuditChangesNotObject,
    #[error("audit_events contains an invalid occurred_at timestamp: {0}")]
    InvalidAuditTimestamp(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Filesystem(#[from] std::io::Error),
}
