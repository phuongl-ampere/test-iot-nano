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

use chrono::{
    DateTime, Duration as ChronoDuration, NaiveDateTime, SecondsFormat, TimeZone, Timelike, Utc,
};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
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
    BUILT_IN_USER_WORKSPACE, CreateManagementAsset, CreateManagementAssetProfile,
    CreateManagementDeviceProfile, CreateManagementUser, DeviceTokenRecord, DeviceTokenRepository,
    DeviceTokenRepositoryError, MANAGEMENT_ALERT_LIST_LIMIT, ManagementAlert, ManagementAlertError,
    ManagementAlertRepository, ManagementAsset, ManagementAssetError, ManagementAssetProfile,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementChildStatus, ManagementDevice, ManagementDeviceError, ManagementDeviceHealth,
    ManagementDeviceProfile, ManagementDeviceProfileError, ManagementDeviceProfileRepository,
    ManagementDeviceRepository, ManagementDeviceTopology, ManagementGatewayStatus, ManagementUser,
    ManagementUserError, ManagementUserRepository, ManagementUserRole, NewDeviceToken,
    NewOwnedDeviceToken, UpdateManagementAsset, UpdateManagementAssetProfile,
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
                self.require_sqlite_tenant_device(command.tenant_id, &command.device_id)
                    .await?;
                enqueue_sqlite_platform_command(store, &command).await
            }
            Self::Timescale(pool) => {
                enqueue_timescale_platform_command(pool, id, params, &command).await
            }
        }
    }

    pub async fn enqueue_authorized_command(
        &self,
        user_id: uuid::Uuid,
        mut command: NewCommandOutboxEntry,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
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
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                if !sqlite_user_can_issue_command(
                    &mut transaction,
                    user_id,
                    command.tenant_id,
                    &command.device_id,
                )
                .await?
                {
                    return Ok(None);
                }
                let record =
                    enqueue_sqlite_platform_command_in_transaction(&mut transaction, &command)
                        .await?;
                transaction.commit().await?;
                Ok(Some(record))
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    .execute(&mut *transaction)
                    .await?;
                if !timescale_user_can_issue_command(
                    &mut transaction,
                    user_id,
                    command.tenant_id,
                    &command.device_id,
                )
                .await?
                {
                    return Ok(None);
                }
                let record = enqueue_timescale_platform_command_in_transaction(
                    &mut transaction,
                    id,
                    &params,
                    &command,
                )
                .await?;
                transaction.commit().await?;
                Ok(Some(record))
            }
        }
    }

    pub async fn ready_command_tenants(
        &self,
        now: DateTime<Utc>,
        cursor: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let range = cursor.map_or(TenantCursorRange::All, TenantCursorRange::After);
        let mut tenant_ids = match self {
            Self::Sqlite(store) => {
                sqlite_ready_command_tenants(store.pool(), now, range, limit).await?
            }
            Self::Timescale(pool) => {
                timescale_ready_command_tenants(pool, now, range, limit).await?
            }
        };
        if let Some(cursor) = cursor {
            let remaining =
                limit.saturating_sub(u32::try_from(tenant_ids.len()).unwrap_or(u32::MAX));
            if remaining > 0 {
                let wrap_range = TenantCursorRange::Through(cursor);
                let wrapped = match self {
                    Self::Sqlite(store) => {
                        sqlite_ready_command_tenants(store.pool(), now, wrap_range, remaining)
                            .await?
                    }
                    Self::Timescale(pool) => {
                        timescale_ready_command_tenants(pool, now, wrap_range, remaining).await?
                    }
                };
                tenant_ids.extend(wrapped);
            }
        }
        Ok(tenant_ids)
    }

    pub async fn claim_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .claim_commands(tenant_id, now, lease_until, limit)
                .await?),
            Self::Timescale(pool) => {
                claim_timescale_commands(pool, tenant_id, now, lease_until, limit).await
            }
        }
    }

    pub async fn mark_command_published(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_published(tenant_id, &command_id.to_string(), published_at)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_command_published(pool, tenant_id, command_id, published_at).await
            }
        }
    }

    pub async fn mark_command_failed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_failed(tenant_id, &command_id.to_string(), error)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_command_failed(pool, tenant_id, command_id, error).await
            }
        }
    }

    pub async fn release_command_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_command_for_retry(
                    tenant_id,
                    &command_id.to_string(),
                    error,
                    next_attempt_at,
                )
                .await?),
            Self::Timescale(pool) => {
                release_timescale_command_for_retry(
                    pool,
                    tenant_id,
                    command_id,
                    error,
                    next_attempt_at,
                )
                .await
            }
        }
    }

    pub async fn expire_due_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        match self {
            Self::Sqlite(store) => Ok(store.expire_due_commands(tenant_id, now, limit).await?),
            Self::Timescale(pool) => {
                expire_timescale_due_commands(pool, tenant_id, now, limit).await
            }
        }
    }

    pub async fn expire_command_if_elapsed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .expire_command_if_elapsed(tenant_id, &command_id.to_string(), now)
                .await?),
            Self::Timescale(pool) => {
                expire_timescale_command_if_elapsed(pool, tenant_id, command_id, now).await
            }
        }
    }

    pub async fn mark_command_responded(
        &self,
        tenant_id: uuid::Uuid,
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
                    tenant_id,
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
                       AND command.tenant_id = $4
                       AND command.device_id = $5
                       AND command.mode = 'two_way'
                       AND (
                            (command.state = 'published_to_broker' AND command.expires_at > $2)
                            OR (command.state = 'responded' AND command.response = $1::jsonb)
                           )
                       AND EXISTS (
                            SELECT 1
                            FROM device_tokens
                            JOIN devices ON devices.device_id = device_tokens.device_id
                            WHERE device_tokens.id = $6
                              AND device_tokens.device_id = command.device_id
                              AND devices.tenant_id = command.tenant_id
                              AND device_tokens.revoked_at IS NULL
                              AND devices.deleted_at IS NULL
                       )
                     RETURNING
                        id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                        lease_until, attempt_count, last_error, published_at, response, responded_at",
                )
                .bind(Json(response_value))
                .bind(responded_at)
                .bind(command_id)
                .bind(tenant_id)
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
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .claim_notifications(tenant_id, now, lease_until, limit)
                .await?),
            Self::Timescale(pool) => {
                claim_timescale_notifications(pool, tenant_id, now, lease_until, limit).await
            }
        }
    }

    pub async fn ready_notification_tenants(
        &self,
        now: DateTime<Utc>,
        cursor: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let range = cursor.map_or(TenantCursorRange::All, TenantCursorRange::After);
        let mut tenant_ids = match self {
            Self::Sqlite(store) => {
                sqlite_ready_notification_tenants(store.pool(), now, range, limit).await?
            }
            Self::Timescale(pool) => {
                timescale_ready_notification_tenants(pool, now, range, limit).await?
            }
        };
        if let Some(cursor) = cursor {
            let remaining =
                limit.saturating_sub(u32::try_from(tenant_ids.len()).unwrap_or(u32::MAX));
            if remaining > 0 {
                let wrap_range = TenantCursorRange::Through(cursor);
                let wrapped = match self {
                    Self::Sqlite(store) => {
                        sqlite_ready_notification_tenants(store.pool(), now, wrap_range, remaining)
                            .await?
                    }
                    Self::Timescale(pool) => {
                        timescale_ready_notification_tenants(pool, now, wrap_range, remaining)
                            .await?
                    }
                };
                tenant_ids.extend(wrapped);
            }
        }
        Ok(tenant_ids)
    }

    pub async fn mark_notification_sent(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_notification_sent(
                    tenant_id,
                    &notification_id.to_string(),
                    expected_lease_until,
                    sent_at,
                )
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_notification_sent(
                    pool,
                    tenant_id,
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
        if opened_notification
            .as_ref()
            .is_some_and(|notification| notification.tenant_id != incident.tenant_id)
        {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store.create_incident(incident, opened_notification).await?),
            Self::Timescale(pool) => {
                create_timescale_incident(pool, incident, opened_notification).await
            }
        }
    }

    pub async fn update_incident_last_value(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_last_value(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    last_value,
                    canonical_postgres_timestamp(updated_at),
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_last_value(
                    pool,
                    tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
        )
        .await
    }

    pub async fn open_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn recover_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Recover(canonical_postgres_timestamp(recovery_started_at)),
        )
        .await
    }

    pub async fn resolve_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
        )
        .await
    }

    pub async fn resolve_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn remind_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
        )
        .await
    }

    pub async fn remind_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
            canonical_notification(notification),
        )
        .await
    }

    async fn update_incident_transition(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    transition,
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition(
                    pool,
                    tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        if notification.tenant_id != tenant_id {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition_with_notification(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    transition,
                    notification,
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition_with_notification(
                    pool,
                    tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        let notification = canonical_notification(notification);
        if notification.tenant_id != tenant_id {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store
                .enqueue_notification(tenant_id, &incident_id.to_string(), notification)
                .await?),
            Self::Timescale(pool) => {
                enqueue_timescale_notification(pool, tenant_id, incident_id, notification).await
            }
        }
    }

    pub async fn release_notification_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_notification_for_retry(
                    tenant_id,
                    &notification_id.to_string(),
                    expected_lease_until,
                    error,
                    next_attempt_at,
                )
                .await?),
            Self::Timescale(pool) => {
                release_timescale_notification_for_retry(
                    pool,
                    tenant_id,
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
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
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
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
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
    if !timescale_tenant_device_is_locked(&mut transaction, command.tenant_id, &command.device_id)
        .await?
    {
        return Err(PlatformStoreError::UnknownDevice(command.device_id.clone()));
    }

    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(id)
    .bind(command.tenant_id)
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
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(id)
    .bind(command.tenant_id)
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

async fn enqueue_sqlite_platform_command_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(&command.params)
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at.to_rfc3339())
    .bind(command.next_attempt_at.to_rfc3339())
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return Ok(command_outbox_record(row)?);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .map(command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn enqueue_timescale_platform_command_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: uuid::Uuid,
    params: &serde_json::Value,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(id)
    .bind(command.tenant_id)
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(Json(params.clone()))
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at)
    .bind(command.next_attempt_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return postgres_command_outbox_record(row);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(id)
    .bind(command.tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn sqlite_user_can_issue_command(
    transaction: &mut Transaction<'_, Sqlite>,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let identity = sqlx::query_as::<_, (String, String)>(
        "SELECT tenant_id, account_class FROM users WHERE id = ?",
    )
    .bind(user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((user_tenant_id, account_class)) = identity else {
        return Ok(false);
    };
    if user_tenant_id != tenant_id.to_string()
        || authorization_account_class(&account_class)? == AccountClass::System
    {
        return Ok(false);
    }

    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(owner) = owner else {
        return Ok(false);
    };
    let user_id = user_id.to_string();
    if owner.as_deref() == Some(user_id.as_str()) {
        return Ok(true);
    }

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = ?
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = ?
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (
                (permission.device_id = ? AND (
                    permission.subject_user_id = ?
                    OR EXISTS (
                        SELECT 1 FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = 1 AND (
                        permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                    ))
           )
         LIMIT 1",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(device_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_user_can_issue_command(
    transaction: &mut Transaction<'_, Postgres>,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let identity = sqlx::query_as::<_, (uuid::Uuid, String)>(
        "SELECT tenant_id, account_class FROM users WHERE id = $1 FOR SHARE",
    )
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((user_tenant_id, account_class)) = identity else {
        return Ok(false);
    };
    if user_tenant_id != tenant_id
        || authorization_account_class(&account_class)? == AccountClass::System
    {
        return Ok(false);
    }

    // Observe the asset without a row lock, then take the shared asset locks
    // before the device lock. Asset deletion uses the same asset -> device order.
    let observed_asset_id = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
        "SELECT asset_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(observed_asset_id) = observed_asset_id else {
        return Ok(false);
    };
    if let Some(asset_id) = observed_asset_id {
        lock_timescale_command_asset_ancestors(transaction, tenant_id, asset_id).await?;
    }

    let device = sqlx::query_as::<_, (Option<uuid::Uuid>, Option<uuid::Uuid>)>(
        "SELECT owner_user_id, asset_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR SHARE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((owner, asset_id)) = device else {
        return Ok(false);
    };
    if asset_id != observed_asset_id {
        return Ok(false);
    }
    if owner == Some(user_id) {
        return Ok(true);
    }
    sqlx::query(
        "SELECT group_id
         FROM user_group_members
         WHERE tenant_id = $1 AND user_id = $2
         FOR SHARE",
    )
    .bind(tenant_id)
    .bind(user_id)
    .fetch_all(&mut **transaction)
    .await?;

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = $2
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1::bigint
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = $2
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (
                (permission.device_id = $1 AND (
                    permission.subject_user_id = $3
                    OR EXISTS (
                        SELECT 1 FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = TRUE AND (
                        permission.subject_user_id = $3
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $3
                        )
                    ))
           )
         LIMIT 1
         FOR SHARE OF permission",
    )
    .bind(device_id)
    .bind(tenant_id)
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn lock_timescale_command_asset_ancestors(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    asset_id: uuid::Uuid,
) -> Result<(), PlatformStoreError> {
    sqlx::query(
        "WITH RECURSIVE ancestors(id) AS (
             SELECT $1::uuid
             UNION
             SELECT asset.parent_asset_id
             FROM assets AS asset
             JOIN ancestors ON asset.id = ancestors.id
             WHERE asset.tenant_id = $2 AND asset.parent_asset_id IS NOT NULL
         )
         SELECT asset.id
         FROM assets AS asset
         JOIN ancestors ON asset.id = ancestors.id AND asset.tenant_id = $2
         ORDER BY asset.id
         FOR SHARE OF asset",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

async fn ingest_sqlite_gateway(
    store: &SqliteStore,
    request: GatewayIngestRequest,
) -> Result<GatewayIngestResult, PlatformStoreError> {
    validate_gateway_ingest_request(&request)?;
    let mut transaction = store.pool().begin().await?;
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
    validate_gateway_ingest_request(&request)?;
    let mut transaction = pool.begin().await?;
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

#[derive(Clone, Copy)]
enum TenantCursorRange {
    All,
    After(uuid::Uuid),
    Through(uuid::Uuid),
}

impl TenantCursorRange {
    fn kind(self) -> i64 {
        match self {
            Self::All => 0,
            Self::After(_) => 1,
            Self::Through(_) => 2,
        }
    }

    fn sqlite_cursor(self) -> String {
        match self {
            Self::All => String::new(),
            Self::After(cursor) | Self::Through(cursor) => cursor.to_string(),
        }
    }

    fn timescale_cursor(self) -> Option<uuid::Uuid> {
        match self {
            Self::All => None,
            Self::After(cursor) | Self::Through(cursor) => Some(cursor),
        }
    }
}

async fn sqlite_ready_command_tenants(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    let now = now.to_rfc3339();
    let range_kind = range.kind();
    let cursor = range.sqlite_cursor();
    let tenant_ids = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT tenant_id
         FROM command_outbox
         WHERE (
                (state = 'queued' AND next_attempt_at <= ?)
                OR (state = 'leased' AND lease_until <= ?)
                OR (
                    (state IN ('queued', 'leased')
                     OR (state = 'published_to_broker' AND mode = 'two_way'))
                    AND expires_at <= ?
                )
               )
           AND (
                ? = 0
                OR (? = 1 AND tenant_id > ?)
                OR (? = 2 AND tenant_id <= ?)
               )
         ORDER BY tenant_id
         LIMIT ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(range_kind)
    .bind(range_kind)
    .bind(&cursor)
    .bind(range_kind)
    .bind(&cursor)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    tenant_ids
        .into_iter()
        .map(|tenant_id| {
            uuid::Uuid::parse_str(&tenant_id)
                .map_err(|_| PlatformStoreError::InvalidCommandTenantId(tenant_id))
        })
        .collect()
}

async fn timescale_ready_command_tenants(
    pool: &PgPool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT DISTINCT tenant_id
         FROM command_outbox
         WHERE (
                (state = 'queued' AND next_attempt_at <= $1)
                OR (state = 'leased' AND lease_until <= $1)
                OR (
                    (state IN ('queued', 'leased')
                     OR (state = 'published_to_broker' AND mode = 'two_way'))
                    AND expires_at <= $1
                )
               )
           AND (
                $2 = 0
                OR ($2 = 1 AND tenant_id > $3)
                OR ($2 = 2 AND tenant_id <= $3)
               )
         ORDER BY tenant_id
         LIMIT $4",
    )
    .bind(now)
    .bind(range.kind())
    .bind(range.timescale_cursor())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?)
}

async fn sqlite_ready_notification_tenants(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    let now = now.to_rfc3339();
    let range_kind = range.kind();
    let cursor = range.sqlite_cursor();
    let tenant_ids = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT tenant_id
         FROM notification_outbox
         WHERE (
                (state = 'pending' AND next_attempt_at <= ?)
                OR (state = 'leased' AND lease_until <= ?)
               )
           AND (
                ? = 0
                OR (? = 1 AND tenant_id > ?)
                OR (? = 2 AND tenant_id <= ?)
               )
         ORDER BY tenant_id
         LIMIT ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(range_kind)
    .bind(range_kind)
    .bind(&cursor)
    .bind(range_kind)
    .bind(&cursor)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    tenant_ids
        .into_iter()
        .map(|tenant_id| {
            uuid::Uuid::parse_str(&tenant_id)
                .map_err(|_| PlatformStoreError::InvalidNotificationTenantId(tenant_id))
        })
        .collect()
}

async fn timescale_ready_notification_tenants(
    pool: &PgPool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT DISTINCT tenant_id
         FROM notification_outbox
         WHERE (
                (state = 'pending' AND next_attempt_at <= $1)
                OR (state = 'leased' AND lease_until <= $1)
               )
           AND (
                $2 = 0
                OR ($2 = 1 AND tenant_id > $3)
                OR ($2 = 2 AND tenant_id <= $3)
               )
         ORDER BY tenant_id
         LIMIT $4",
    )
    .bind(now)
    .bind(range.kind())
    .bind(range.timescale_cursor())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?)
}

async fn claim_timescale_commands(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
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
            WHERE tenant_id = $1
              AND expires_at > $2
              AND (
                    (state = 'queued' AND next_attempt_at <= $2)
                    OR (state = 'leased' AND lease_until <= $2)
                  )
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE command_outbox AS command
         SET state = 'leased',
             lease_until = $4,
             attempt_count = command.attempt_count + 1
         FROM due
         WHERE command.id = due.id AND command.tenant_id = $1
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params, command.mode,
            command.state, command.created_at, command.expires_at, command.next_attempt_at, command.lease_until,
            command.attempt_count, command.last_error, command.published_at, command.response,
            command.responded_at",
    )
    .bind(tenant_id)
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_command_outbox_record)
        .collect()
}

async fn expire_timescale_due_commands(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    now: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH expired AS (
            SELECT id
            FROM command_outbox
            WHERE tenant_id = $1
              AND (
                    state IN ('queued', 'leased')
                    OR (state = 'published_to_broker' AND mode = 'two_way')
                  )
              AND expires_at <= $2
            ORDER BY expires_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         FROM expired
         WHERE command.id = expired.id
           AND command.tenant_id = $1
           AND (
                command.state IN ('queued', 'leased')
                OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
               )
           AND command.expires_at <= $2
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params,
            command.mode, command.state, command.created_at, command.expires_at,
            command.next_attempt_at, command.lease_until, command.attempt_count,
            command.last_error, command.published_at, command.response, command.responded_at",
    )
    .bind(tenant_id)
    .bind(now)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_command_outbox_record)
        .collect()
}

async fn expire_timescale_command_if_elapsed(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    now: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         WHERE command.id = $1
           AND command.tenant_id = $2
           AND (
                command.state IN ('queued', 'leased')
                OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
               )
           AND command.expires_at <= $3
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params,
            command.mode, command.state, command.created_at, command.expires_at,
            command.next_attempt_at, command.lease_until, command.attempt_count,
            command.last_error, command.published_at, command.response, command.responded_at",
    )
    .bind(command_id)
    .bind(tenant_id)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn claim_timescale_notifications(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
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
            WHERE tenant_id = $1
              AND ((state = 'pending' AND next_attempt_at <= $2)
                OR (state = 'leased' AND lease_until <= $2))
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE notification_outbox AS notification
         SET state = 'leased',
             lease_until = $4,
             attempt_count = notification.attempt_count + 1
         FROM due
         WHERE notification.id = due.id AND notification.tenant_id = $1
         RETURNING
            notification.id, notification.tenant_id, notification.incident_id, notification.kind,
            notification.dedupe_key, notification.subject, notification.body,
            notification.state, notification.next_attempt_at, notification.lease_until,
            notification.attempt_count, notification.last_error, notification.sent_at",
    )
    .bind(tenant_id)
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
            id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, last_value
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT DO NOTHING",
    )
    .bind(incident.id)
    .bind(incident.tenant_id)
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
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(notification.id)
        .bind(incident.tenant_id)
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
        "SELECT id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                state_version
         FROM alert_incidents WHERE id = $1 AND tenant_id = $2",
    )
    .bind(incident.id)
    .bind(incident.tenant_id)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    postgres_alert_incident_record(row).map(Some)
}

async fn update_timescale_incident_last_value(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    expected_version: i64,
    last_value: Option<f64>,
    updated_at: DateTime<Utc>,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE alert_incidents
         SET last_value = $1, state_version = state_version + 1, updated_at = $2
         WHERE id = $3 AND tenant_id = $5 AND state_version = $4 AND status IN ('pending', 'open')
         RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                   opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                   state_version",
    )
    .bind(last_value)
    .bind(updated_at)
    .bind(incident_id)
    .bind(expected_version)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    expected_version: i64,
    transition: AlertIncidentTransition,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let query = match transition {
        AlertIncidentTransition::Open(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', opened_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'pending'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => sqlx::query(
            "UPDATE alert_incidents
             SET recovery_started_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
                   AND recovery_started_at IS NULL
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition_with_notification(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
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
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'pending'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => unreachable!(),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .bind(tenant_id)
        .fetch_optional(&mut *transaction)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(notification.id)
    .bind(tenant_id)
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
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    notification: NewNotificationOutboxEntry,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (tenant_id, dedupe_key) DO NOTHING",
    )
    .bind(notification.id)
    .bind(tenant_id)
    .bind(incident_id)
    .bind(notification.kind.as_str())
    .bind(&notification.dedupe_key)
    .bind(&notification.subject)
    .bind(&notification.body)
    .bind(notification.next_attempt_at)
    .execute(pool)
    .await?;
    let row = sqlx::query(
        "SELECT id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at
         FROM notification_outbox WHERE dedupe_key = $1 AND tenant_id = $2",
    )
    .bind(notification.dedupe_key)
    .bind(tenant_id)
    .fetch_one(pool)
    .await?;
    postgres_notification_outbox_record(row)
}

async fn mark_timescale_notification_sent(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    sent_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'sent', sent_at = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased' AND lease_until = $4
         RETURNING
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(sent_at)
    .bind(notification_id)
    .bind(tenant_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn release_timescale_notification_for_retry(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'pending', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND tenant_id = $4 AND state = 'leased' AND lease_until = $5
         RETURNING
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(notification_id)
    .bind(tenant_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn mark_timescale_command_published(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    published_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'published_to_broker', published_at = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(published_at)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn mark_timescale_command_failed(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    error: &str,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'failed', last_error = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased'
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(error)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn release_timescale_command_for_retry(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'queued', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND tenant_id = $4 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

fn command_payload_matches(
    existing: &CommandOutboxRecord,
    command: &NewCommandOutboxEntry,
) -> bool {
    existing.id == command.id
        && existing.tenant_id == command.tenant_id
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
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(rule.tenant_id.to_string())
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
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(rule.tenant_id)
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
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = ? AND rule_id = ? AND device_id = ? AND status IN ('pending', 'open')",
    )
    .bind(tenant_id.to_string())
    .bind(rule_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(sqlite_event_incident)
    .transpose()
}

async fn load_timescale_active_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = $1 AND rule_id = $2 AND device_id = $3 AND status IN ('pending', 'open')
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(rule_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_event_incident)
    .transpose()
}

async fn load_sqlite_recent_resolved_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = ? AND rule_id = ? AND device_id = ?
           AND status = 'resolved' AND resolved_at >= ?
         ORDER BY resolved_at DESC
         LIMIT 1",
    )
    .bind(tenant_id.to_string())
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
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = $1 AND rule_id = $2 AND device_id = $3
           AND status = 'resolved' AND resolved_at >= $4
         ORDER BY resolved_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(tenant_id)
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
             WHERE id = ? AND tenant_id = ?",
        )
        .bind(&at)
        .bind(&at)
        .bind(value)
        .bind(&at)
        .bind(&at)
        .bind(state_version)
        .bind(&at)
        .bind(incident.id.to_string())
        .bind(rule.tenant_id.to_string())
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
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&at)
    .bind(value)
    .bind(&at)
    .bind(incident.id.to_string())
    .bind(rule.tenant_id.to_string())
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
             WHERE id = $1 AND tenant_id = $5",
        )
        .bind(incident.id)
        .bind(evaluated_at)
        .bind(value)
        .bind(state_version as i32)
        .bind(rule.tenant_id)
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
         WHERE id = $1 AND tenant_id = $4",
    )
    .bind(incident.id)
    .bind(evaluated_at)
    .bind(value)
    .bind(rule.tenant_id)
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
                load_sqlite_active_event_incident(transaction, rule.tenant_id, rule.id, device_id)
                    .await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = ? AND tenant_id = ?")
                            .bind(incident.id.to_string())
                            .bind(rule.tenant_id.to_string())
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
                                 WHERE id = ? AND tenant_id = ?",
                            )
                            .bind(&at)
                            .bind(&at)
                            .bind(value)
                            .bind(&at)
                            .bind(state_version)
                            .bind(&at)
                            .bind(incident.id.to_string())
                            .bind(rule.tenant_id.to_string())
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
                             WHERE id = ? AND tenant_id = ?",
                        )
                        .bind(recovery_started_at.to_rfc3339())
                        .bind(value)
                        .bind(&at)
                        .bind(incident.id.to_string())
                        .bind(rule.tenant_id.to_string())
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
        load_sqlite_active_event_incident(transaction, rule.tenant_id, rule.id, device_id).await?
    else {
        if let Some(resolved) = load_sqlite_recent_resolved_event_incident(
            transaction,
            rule.tenant_id,
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
                "INSERT INTO alert_incidents (id, tenant_id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(id.to_string())
            .bind(rule.tenant_id.to_string())
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
                id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(rule.tenant_id.to_string())
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
                     WHERE id = ? AND tenant_id = ?",
                )
                .bind(&at)
                .bind(value)
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
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
            sqlx::query("UPDATE alert_incidents SET last_value = ?, updated_at = ? WHERE id = ? AND tenant_id = ?")
                .bind(value)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
                .execute(&mut **transaction)
                .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(value)
            .bind(&at)
            .bind(incident.id.to_string())
            .bind(rule.tenant_id.to_string())
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
                     WHERE id = ? AND tenant_id = ?",
                )
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
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
            if let Some(incident) = load_timescale_active_event_incident(
                transaction,
                rule.tenant_id,
                rule.id,
                device_id,
            )
            .await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = $1 AND tenant_id = $2")
                            .bind(incident.id)
                            .bind(rule.tenant_id)
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
                                 WHERE id = $1 AND tenant_id = $5",
                            )
                            .bind(incident.id)
                            .bind(evaluated_at)
                            .bind(value)
                            .bind(state_version as i32)
                            .bind(rule.tenant_id)
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
                             WHERE id = $1 AND tenant_id = $5",
                        )
                        .bind(incident.id)
                        .bind(recovery_started_at)
                        .bind(value)
                        .bind(evaluated_at)
                        .bind(rule.tenant_id)
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
        load_timescale_active_event_incident(transaction, rule.tenant_id, rule.id, device_id)
            .await?
    else {
        if let Some(resolved) = load_timescale_recent_resolved_event_incident(
            transaction,
            rule.tenant_id,
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
                "INSERT INTO alert_incidents (id, tenant_id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES ($1, $2, $3, $4, 'open', $5, $5, $6, $5, $5, 1, $5, $5)",
            )
            .bind(id)
            .bind(rule.tenant_id)
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
                id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, 'pending', $5, $6, $5, $5)",
        )
        .bind(id)
        .bind(rule.tenant_id)
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
                     WHERE id = $1 AND tenant_id = $5",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(value)
                .bind(state_version as i32)
                .bind(rule.tenant_id)
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
                "UPDATE alert_incidents SET last_value = $2, updated_at = $3
                 WHERE id = $1 AND tenant_id = $4",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .bind(rule.tenant_id)
            .execute(&mut **transaction)
            .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = $2, updated_at = $3
                 WHERE id = $1 AND tenant_id = $4",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .bind(rule.tenant_id)
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
                     WHERE id = $1 AND tenant_id = $4",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(state_version as i32)
                .bind(rule.tenant_id)
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
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
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
            if rule.tenant_id != event.tenant_id
                || rule
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
                 (tenant_id, rule_id, event_at, device_id, boot_id, sequence)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT (tenant_id, rule_id, event_at, device_id, boot_id, sequence) DO NOTHING",
            )
            .bind(rule.tenant_id.to_string())
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
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
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
            if rule.tenant_id != event.tenant_id
                || rule
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
                lock_keys.push((rule.tenant_id, rule.id, event.device_id.clone()));
            }
        }
    }
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (tenant_id, rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "iot_nano:alert-event:{tenant_id}:{rule_id}:{device_id}"
            ))
            .execute(&mut *transaction)
            .await?;
    }

    let mut result = AlertEvaluationResult::default();
    for event in events {
        for rule in &rules {
            if rule.tenant_id != event.tenant_id
                || rule
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
            let claim = sqlx::query("INSERT INTO alert_rule_event_evaluations (tenant_id, rule_id, event_at, device_id, boot_id, sequence) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tenant_id, rule_id, event_at, device_id, boot_id, sequence) DO NOTHING")
                .bind(rule.tenant_id).bind(rule.id).bind(canonical_postgres_timestamp(event.event_at)).bind(&event.device_id).bind(event.boot_id).bind(sequence).execute(&mut *transaction).await?;
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
            SELECT tenant_id, device_id, measurements,
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
             WHERE tenant_id = ?
               AND event_at_micros >= ?
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
    .bind(rule.tenant_id.to_string())
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
             WHERE tenant_id = $2
               AND event_at >= $3
               AND event_at <= $4
               AND ($5::text IS NULL OR device_id = $5)
         )
         SELECT device_id, (AVG(finite_value))::double precision AS average
         FROM finite_telemetry
         GROUP BY device_id
         HAVING COUNT(finite_value) > 0
         ORDER BY device_id",
    )
    .bind(&rule.metric_key)
    .bind(rule.tenant_id)
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
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
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
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
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
        .map(|(rule, device_id, _)| (rule.tenant_id, rule.id, device_id.clone()))
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (tenant_id, rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "iot_nano:alert-window:{tenant_id}:{rule_id}:{device_id}"
            ))
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
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move {
            PlatformStore::claim_commands(self, tenant_id, now, lease_until, limit).await
        })
    }

    fn mark_command_published<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
            PlatformStore::mark_command_published(self, tenant_id, command_id, published_at).await
        })
    }

    fn mark_command_failed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_failed(self, tenant_id, command_id, error).await
        })
    }

    fn release_command_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
            PlatformStore::release_command_for_retry(
                self,
                tenant_id,
                command_id,
                error,
                next_attempt_at,
            )
            .await
        })
    }

    fn expire_due_commands<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(
            async move { PlatformStore::expire_due_commands(self, tenant_id, now, limit).await },
        )
    }

    fn expire_command_if_elapsed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::expire_command_if_elapsed(self, tenant_id, command_id, now).await
        })
    }

    fn mark_command_responded<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
                tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.update_incident_last_value(
                tenant_id,
                incident_id,
                expected_version,
                last_value,
                updated_at,
            )
            .await
        })
    }

    fn open_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident(tenant_id, incident_id, expected_version, opened_at)
                .await
        })
    }

    fn open_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident_with_notification(
                tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.recover_incident(
                tenant_id,
                incident_id,
                expected_version,
                recovery_started_at,
            )
            .await
        })
    }

    fn resolve_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident(tenant_id, incident_id, expected_version, resolved_at)
                .await
        })
    }

    fn resolve_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident_with_notification(
                tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident(tenant_id, incident_id, expected_version, reminded_at)
                .await
        })
    }

    fn remind_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident_with_notification(
                tenant_id,
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
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<
        Box<dyn Future<Output = Result<NotificationOutboxRecord, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.enqueue_notification(tenant_id, incident_id, notification)
                .await
        })
    }
}

impl NotificationRepository for PlatformStore {
    fn claim_notifications<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
        Box::pin(async move {
            PlatformStore::claim_notifications(self, tenant_id, now, lease_until, limit).await
        })
    }

    fn mark_notification_sent<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
                tenant_id,
                notification_id,
                expected_lease_until,
                sent_at,
            )
            .await
        })
    }

    fn release_notification_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
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
                tenant_id,
                notification_id,
                expected_lease_until,
                error,
                next_attempt_at,
            )
            .await
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

fn sqlite_alert_rule_record(row: SqliteRow) -> Result<AlertRule, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let rule = AlertRule {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidAlertRuleId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidAlertRuleTenantId(tenant_id))?,
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
        tenant_id: row.try_get("tenant_id")?,
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

fn postgres_command_outbox_record(row: PgRow) -> Result<CommandOutboxRecord, PlatformStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get::<uuid::Uuid, _>("id")?.to_string(),
        tenant_id: row.try_get("tenant_id")?,
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
        tenant_id: row.try_get("tenant_id")?,
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
pub struct RetentionResult {
    pub raw_rows: u64,
    pub rollup_rows: u64,
    pub event_evaluation_rows: u64,
    pub notification_rows: u64,
    pub resolved_incident_rows: u64,
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

    pub async fn enqueue_command(
        &self,
        command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, SqliteStoreError> {
        let row = sqlx::query(
            "INSERT INTO command_outbox (
                id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command.id)
        .bind(command.tenant_id.to_string())
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
        tenant_id: uuid::Uuid,
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
                WHERE tenant_id = ?
                  AND expires_at > ?
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
             WHERE tenant_id = ? AND id IN (SELECT id FROM due)
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(tenant_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .bind(tenant_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn mark_command_published(
        &self,
        tenant_id: uuid::Uuid,
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
               AND tenant_id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&published_at)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(&published_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_failed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: &str,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'failed',
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND tenant_id = ?
               AND state = 'leased'
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(error)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn release_command_for_retry(
        &self,
        tenant_id: uuid::Uuid,
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
               AND tenant_id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(&next_attempt_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn claim_notifications(
        &self,
        tenant_id: uuid::Uuid,
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
                WHERE tenant_id = ?
                  AND ((state = 'pending' AND next_attempt_at <= ?)
                    OR (state = 'leased' AND lease_until <= ?))
                ORDER BY next_attempt_at, created_at, id
                LIMIT ?
             )
             UPDATE notification_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE tenant_id = ? AND id IN (SELECT id FROM due)
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(tenant_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .bind(tenant_id.to_string())
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
                id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, last_value
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT DO NOTHING",
        )
        .bind(incident.id.to_string())
        .bind(incident.tenant_id.to_string())
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
                    id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(notification.id.to_string())
            .bind(incident.tenant_id.to_string())
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
            "SELECT id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                    opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                    state_version
             FROM alert_incidents WHERE id = ? AND tenant_id = ?",
        )
        .bind(incident.id.to_string())
        .bind(incident.tenant_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        sqlite_alert_incident_record(row).map(Some)
    }

    async fn update_incident_last_value(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: &str,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let row = sqlx::query(
            "UPDATE alert_incidents
             SET last_value = ?, state_version = state_version + 1, updated_at = ?
             WHERE id = ? AND tenant_id = ? AND state_version = ? AND status IN ('pending', 'open')
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        )
        .bind(last_value)
        .bind(updated_at.to_rfc3339())
        .bind(incident_id)
        .bind(tenant_id.to_string())
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition(
        &self,
        tenant_id: uuid::Uuid,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                       AND recovery_started_at IS NULL
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
        };
        let row = query
            .bind(incident_id)
            .bind(tenant_id.to_string())
            .bind(expected_version)
            .fetch_optional(&self.pool)
            .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition_with_notification(
        &self,
        tenant_id: uuid::Uuid,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
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
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => unreachable!(),
        };
        let row = query
            .bind(incident_id)
            .bind(tenant_id.to_string())
            .bind(expected_version)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(notification.id.to_string())
        .bind(tenant_id.to_string())
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
        tenant_id: uuid::Uuid,
        incident_id: &str,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, dedupe_key) DO NOTHING",
        )
        .bind(notification.id.to_string())
        .bind(tenant_id.to_string())
        .bind(incident_id)
        .bind(notification.kind.as_str())
        .bind(&notification.dedupe_key)
        .bind(&notification.subject)
        .bind(&notification.body)
        .bind(notification.next_attempt_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                    next_attempt_at, lease_until, attempt_count, last_error, sent_at
             FROM notification_outbox WHERE dedupe_key = ? AND tenant_id = ?",
        )
        .bind(notification.dedupe_key)
        .bind(tenant_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        notification_outbox_record(row)
    }

    pub async fn mark_notification_sent(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let sent_at = sent_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'sent', sent_at = ?, lease_until = NULL
             WHERE id = ? AND tenant_id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&sent_at)
        .bind(notification_id)
        .bind(tenant_id.to_string())
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn release_notification_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let next_attempt_at = next_attempt_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'pending', next_attempt_at = ?, last_error = ?, lease_until = NULL
             WHERE id = ? AND tenant_id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(notification_id)
        .bind(tenant_id.to_string())
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn expire_due_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let tenant_id = tenant_id.to_string();
        let now = now.to_rfc3339();
        // This single write statement selects and expires a bounded ordered set atomically.
        let rows = sqlx::query(
            "WITH expired AS (
                SELECT id
                FROM command_outbox
                WHERE tenant_id = ?
                  AND (
                        state IN ('queued', 'leased')
                        OR (state = 'published_to_broker' AND mode = 'two_way')
                      )
                  AND expires_at <= ?
                ORDER BY expires_at, created_at, id
                LIMIT ?
             )
             UPDATE command_outbox AS command
             SET state = 'expired',
                 lease_until = NULL
             WHERE command.tenant_id = ?
               AND command.id IN (SELECT id FROM expired)
               AND (
                    command.state IN ('queued', 'leased')
                    OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
                   )
               AND command.expires_at <= ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&tenant_id)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(&tenant_id)
        .bind(&now)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn expire_command_if_elapsed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox AS command
             SET state = 'expired',
                 lease_until = NULL
             WHERE command.id = ?
               AND command.tenant_id = ?
               AND (
                    command.state IN ('queued', 'leased')
                    OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
                   )
               AND command.expires_at <= ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_responded(
        &self,
        tenant_id: uuid::Uuid,
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
               AND command.tenant_id = ?
               AND command.device_id = ?
               AND command.mode = 'two_way'
               AND (
                    (command.state = 'published_to_broker' AND command.expires_at > ?)
                    OR (command.state = 'responded' AND command.response = ?)
               )
               AND EXISTS (
                    SELECT 1
                    FROM device_tokens
                    JOIN devices ON devices.device_id = device_tokens.device_id
                    WHERE device_tokens.id = ?
                      AND device_tokens.device_id = command.device_id
                      AND devices.tenant_id = command.tenant_id
                      AND device_tokens.revoked_at IS NULL
                      AND devices.deleted_at IS NULL
               )
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(response)
        .bind(&responded_at)
        .bind(command_id)
        .bind(tenant_id.to_string())
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

fn command_outbox_record(row: SqliteRow) -> Result<CommandOutboxRecord, SqliteStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get("id")?,
        tenant_id: uuid::Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)
            .map_err(|_| SqliteStoreError::InvalidCommandTenantId)?,
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
    let tenant_id: String = row.try_get("tenant_id")?;
    let rule_id: String = row.try_get("rule_id")?;
    Ok(AlertIncident {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidIncidentId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidIncidentTenantId(tenant_id))?,
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
        tenant_id: row.try_get("tenant_id")?,
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
    let tenant_id: String = row.try_get("tenant_id")?;
    let incident_id: String = row.try_get("incident_id")?;
    Ok(NotificationOutboxRecord {
        id: uuid::Uuid::parse_str(&id)
            .map_err(|_| PlatformStoreError::InvalidNotificationId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidNotificationTenantId(tenant_id))?,
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
