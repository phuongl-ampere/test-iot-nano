#![forbid(unsafe_code)]

mod audit;
mod contracts;
mod device_claims;
mod device_relations;
mod domain;
mod management;
mod public_api;
mod schema;
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

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use sqlx::{
    Postgres, Row, Sqlite, SqlitePool, Transaction,
    postgres::PgRow,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
    types::Json,
};
use thiserror::Error;

pub(crate) const PLATFORM_SCHEMA_VERSION: i64 = 8;

pub use contracts::{
    AccountClass, AlertComparison, AlertEvaluationEvent, AlertEvaluationRepository,
    AlertEvaluationResult, AlertIncident, AlertIncidentRepository, AlertIncidentStatus, AlertRule,
    AlertRuleKind, AlertSeverity, ApplicationAssetProfileRelation, ApplicationDomainProfile,
    ApplicationDomainProfileError, ApplicationDomainProfileRepository,
    ApplicationDomainResourceKind, ApplicationId, ApplicationKind, ApplicationRecord,
    ApplicationRepository, AuthenticatedDeviceToken, AuthorizationRepository, AuthorizationSubject,
    AuthorizedAssetListEntry, AuthorizedAssetSummary, AuthorizedDeviceListEntry,
    AuthorizedDeviceSummary, ClientId, CommandLifecycleRepository, CommandOutboxRecord,
    CommandOutboxState, CommandRepository, CreateApplicationAssetProfileRelation,
    CreateApplicationDomainProfile, DeviceAuthorizationRepository, GatewayIngestEventKind,
    GatewayIngestRepository, GatewayIngestRequest, GatewayIngestResult,
    GatewayIngestValidationError, IdentityRepository, NewAlertIncident, NewApplication,
    NewCommandOutboxEntry, NewNotificationOutboxEntry, NewOAuthAuthorizationCode,
    NewOAuthClientSecret, NewResourcePermission, NewUserGroup, NotificationKind,
    NotificationOutboxRecord, NotificationOutboxState, NotificationRepository,
    OAuthAccessTokenRecord, OAuthAuthorizationCodeExchange, OAuthClientCredentialsToken,
    OAuthRepository, OwnershipTransferTarget, PermissionCreator, RedirectUri, ResourceAccess,
    ResourceAccessSource, ResourceInvitation, ResourceInvitationRepository,
    ResourceInvitationState, ResourceKind, ResourcePermission, ResourcePermissionRecord,
    TelemetryAggregate, TelemetryAggregateRepository, TelemetryRepository, TenantActor,
    TenantAuthorizationError, TenantAuthorizationRepository, TenantProfileConfiguration,
    TenantProfileContainmentRule, TenantProfileDefinition, TenantProfileRepository,
    TenantUserGroup, TenantUserGroupMember, TopologyRepository, UpdateApplicationDomainProfile,
    UserDeviceActivity, UserDeviceActivityRepository, UserDeviceAlert, UserDeviceTelemetry,
    UserGroup,
};
pub use domain::RetentionResult;
pub use store::{PlatformStore, PlatformStoreError, SqliteStore};

pub use audit::{
    AuditAction, AuditEvent, AuditEventCursor, AuditEventError, AuditEventRepository,
    AuditPrincipal, AuditTargetType,
};
pub use device_claims::{
    ClaimedDevice, DeviceClaimCodeStatus, DeviceClaimError, DeviceClaimPolicy,
    DeviceClaimRepository, IssuedDeviceClaimCode,
};
pub use device_relations::{
    CreateDeviceAssetRelation, CreateDeviceRelation, DeviceAssetRelation, DeviceRelation,
    DeviceRelationError, DeviceRelationRepository, RESERVED_GATEWAY_CHILD_RELATION_TYPE,
};
pub use management::{
    CreateManagementAlertRule, CreateManagementAsset, CreateManagementAssetProfile,
    CreateManagementDeviceProfile, CreateManagementUser, DeviceTokenRecord, DeviceTokenRepository,
    DeviceTokenRepositoryError, DeviceTokenSecret, MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT,
    MANAGEMENT_ALERT_LIST_LIMIT, MANAGEMENT_ALERT_RULE_LIST_LIMIT,
    MANAGEMENT_DEVICE_TELEMETRY_LIMIT, ManagementAlert, ManagementAlertError,
    ManagementAlertIncident, ManagementAlertIncidentError, ManagementAlertIncidentRepository,
    ManagementAlertRepository, ManagementAlertRule, ManagementAlertRuleError,
    ManagementAlertRuleRepository, ManagementAsset, ManagementAssetError, ManagementAssetProfile,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementChildStatus, ManagementDevice, ManagementDeviceError, ManagementDeviceHealth,
    ManagementDeviceProfile, ManagementDeviceProfileError, ManagementDeviceProfileRepository,
    ManagementDeviceRepository, ManagementDeviceTelemetry, ManagementDeviceTelemetryRepository,
    ManagementDeviceTopology, ManagementGatewayStatus, ManagementUser, ManagementUserError,
    ManagementUserRepository, ManagementUserRole, NewDeviceToken, NewOtaDeployment,
    NewOwnedDeviceToken, NewTenantPersonalAccessToken, OtaArtifact, OtaDeployment,
    OtaDeploymentStatus, OtaPolicy, ProvisionManagementDevice, ProvisionManagementDeviceError,
    TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepository,
    TenantPersonalAccessTokenRepositoryError, UpdateManagementAlertRule, UpdateManagementAsset,
    UpdateManagementAssetProfile, UpdateManagementDevice, UpdateManagementDeviceProfile,
    UpdateManagementUser, UserCapability,
};
pub use public_api::{
    NewPublicAsset, NewPublicDevice, PublicAlert, PublicApiRepository, PublicAsset,
    PublicAssetError, PublicDevice, PublicDeviceError, PublicPrincipal, PublicTelemetry,
};
#[cfg(feature = "test-support")]
pub use public_api::{PublicDeviceListHandoffHookGuard, install_public_device_list_handoff_hook};
pub use tenant_identity::{
    AccountStatus, NewSystemAccount, NewTenant, NewTenantAccount, PlatformAccountCredential,
    SystemAccount, SystemAccountCredential, Tenant, TenantAccount, TenantAccountCredential,
    TenantIdentityError, TenantIdentityRepository, TenantStatus, TenantSummary,
    TenantUserCredential,
};

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

enum SqliteSchemaState {
    Fresh,
    Current,
}

async fn require_current_sqlite_schema_marker(
    pool: &SqlitePool,
) -> Result<SqliteSchemaState, SqliteStoreError> {
    let objects = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM sqlite_master
         WHERE type IN ('table', 'view', 'trigger') AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    if !tables.iter().any(|table| table == "platform_schema") {
        return match objects.first() {
            Some(table) => Err(SqliteStoreError::ResetRequired {
                table: table.clone(),
            }),
            None => Ok(SqliteSchemaState::Fresh),
        };
    }

    let marker = sqlx::query_as::<_, (i64, i64)>(
        "SELECT singleton, version FROM platform_schema ORDER BY singleton",
    )
    .fetch_all(pool)
    .await;
    if !matches!(marker, Ok(ref marker) if marker.as_slice() == [(1, PLATFORM_SCHEMA_VERSION)]) {
        return Err(SqliteStoreError::ResetRequired {
            table: "platform_schema".to_owned(),
        });
    }
    if let Some(table) = tables
        .iter()
        .find(|table| !schema::sqlite::CANONICAL_TABLES.contains(&table.as_str()))
    {
        return Err(SqliteStoreError::ResetRequired {
            table: table.clone(),
        });
    }
    if let Some(table) = schema::sqlite::CANONICAL_TABLES
        .iter()
        .find(|table| !tables.iter().any(|existing| existing == *table))
    {
        return Err(SqliteStoreError::ResetRequired {
            table: (*table).to_owned(),
        });
    }

    let has_legacy_default_app: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pragma_table_info('users') WHERE name = 'default_app'
         )",
    )
    .fetch_one(pool)
    .await?;
    if has_legacy_default_app {
        return Err(SqliteStoreError::ResetRequired {
            table: "users.default_app".to_owned(),
        });
    }
    Ok(SqliteSchemaState::Current)
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

async fn backup_sqlite_pool(pool: &SqlitePool, path: &Path) -> Result<PathBuf, SqliteStoreError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(SqliteStoreError::InvalidConfiguration)?;
    let backup_name = format!(
        "{file_name}.backup-{}",
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
        let schema_state = require_current_sqlite_schema_marker(&pool).await?;
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&pool)
            .await?;
        if matches!(schema_state, SqliteSchemaState::Fresh) {
            let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::raw_sql(schema::sqlite::SQLITE_SCHEMA)
                .execute(&mut *transaction)
                .await?;
            sqlx::query(
                "INSERT INTO platform_schema (singleton, version)
                 VALUES (1, ?)",
            )
            .bind(PLATFORM_SCHEMA_VERSION)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
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
        "platform SQLite schema is not the current canonical schema at {table:?}; reset the development database before starting iot-nano"
    )]
    ResetRequired { table: String },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Filesystem(#[from] std::io::Error),
}
