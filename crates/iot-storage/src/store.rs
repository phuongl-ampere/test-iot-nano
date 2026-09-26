use std::path::PathBuf;

use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use sqlx::{Executor, PgPool, SqlitePool, postgres::PgPoolOptions};
use thiserror::Error;

use crate::{ApplicationId, GatewayIngestValidationError, SqliteStoreError, schema::postgres};

#[derive(Clone)]
pub struct SqliteStore {
    pub(crate) pool: SqlitePool,
    pub(crate) path: PathBuf,
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
    #[error(
        "platform Timescale schema is not the current canonical schema at {table:?}; reset the development database before starting iot-nano"
    )]
    ResetRequiredTimescaleSchema { table: String },
    #[error(transparent)]
    Sqlite(#[from] SqliteStoreError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("command ID must be a UUID, got {0:?}")]
    InvalidCommandId(String),
    #[error("command tenant ID must be a UUID, got {0:?}")]
    InvalidCommandTenantId(String),
    #[error("command params must be valid JSON")]
    InvalidCommandParams,
    #[error("command payload conflicts with existing command ID: {0:?}")]
    CommandConflict(String),
    #[error("notification ID is not a UUID: {0:?}")]
    InvalidNotificationId(String),
    #[error("notification tenant ID is not a UUID: {0:?}")]
    InvalidNotificationTenantId(String),
    #[error("notification tenant does not match its incident tenant")]
    NotificationTenantMismatch,
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
    #[error("incident tenant ID is not a UUID: {0:?}")]
    InvalidIncidentTenantId(String),
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
    #[error("alert rule tenant ID must be a UUID, got {0:?}")]
    InvalidAlertRuleTenantId(String),
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
                postgres::migrate(&migration_pool).await?;
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
}
