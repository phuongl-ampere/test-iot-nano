use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use sqlx::{Postgres, Row, Sqlite, Transaction, types::Json};
use thiserror::Error;
use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub attributes: serde_json::Value,
    pub topology: ManagementDeviceTopology,
    pub health: ManagementDeviceHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementDeviceTopology {
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementDeviceHealth {
    pub online: bool,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub gateway_status: Option<ManagementGatewayStatus>,
    pub child_status: Option<ManagementChildStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementGatewayStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementChildStatus {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementDevice {
    pub display_name: String,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub attributes: Option<serde_json::Value>,
    pub topology: Option<ManagementDeviceTopology>,
}

#[derive(Debug, Error)]
pub enum ManagementDeviceError {
    #[error("invalid device ID: {0:?}")]
    InvalidDeviceId(String),
    #[error("invalid device display name")]
    InvalidDisplayName,
    #[error("device attributes must be an object")]
    AttributesMustBeObject,
    #[error("management device was not found")]
    DeviceNotFound,
    #[error("a gateway cannot be assigned to another gateway")]
    GatewayCannotHaveParent,
    #[error("a device cannot be its own gateway")]
    DeviceCannotBeOwnGateway,
    #[error("a gateway with assigned children cannot be changed or deleted")]
    GatewayHasChildren,
    #[error("assigned gateway device is unavailable")]
    GatewayUnavailable,
    #[error("assigned gateway device is not a gateway")]
    GatewayIsNotGateway,
    #[error("assigned asset is unavailable: {0}")]
    AssetUnavailable(Uuid),
    #[error("assigned device profile is unavailable: {0}")]
    DeviceProfileUnavailable(Uuid),
    #[error("stored device attributes are invalid")]
    InvalidStoredAttributes,
    #[error("stored device timestamp is invalid")]
    InvalidStoredTimestamp,
    #[error("management device storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementDeviceError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementDeviceError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementDeviceRepository: Send + Sync {
    fn list_management_devices<'a>(
        &'a self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    >;
    fn update_management_device<'a>(
        &'a self,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>;
    fn delete_management_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>>;
}

impl ManagementDeviceRepository for PlatformStore {
    fn list_management_devices<'a>(
        &'a self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    > {
        Box::pin(async move { list_management_devices(self).await })
    }

    fn update_management_device<'a>(
        &'a self,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>
    {
        Box::pin(async move { update_management_device(self, device_id, update).await })
    }

    fn delete_management_device<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>> {
        Box::pin(async move { delete_management_device(self, device_id).await })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeviceToken {
    pub id: Uuid,
    pub token_prefix: String,
    pub token_hash: String,
    pub token_ciphertext: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOwnedDeviceToken {
    pub display_name: String,
    pub owner_user_id: Uuid,
    pub asset_id: Option<Uuid>,
    pub token: NewDeviceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTokenRecord {
    pub id: Uuid,
    pub device_id: String,
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
pub enum DeviceTokenRepositoryError {
    #[error("device token device was not found")]
    DeviceNotFound,
    #[error("device token was not found")]
    TokenNotFound,
    #[error("gateway child devices cannot have MQTT tokens")]
    GatewayChild,
    #[error("device token prefix already exists")]
    TokenPrefixConflict,
    #[error("stored device token timestamp is invalid")]
    InvalidStoredTimestamp,
    #[error("device token storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for DeviceTokenRepositoryError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for DeviceTokenRepositoryError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait DeviceTokenRepository: Send + Sync {
    fn provision_device_token<'a>(
        &'a self,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn provision_owned_device_token<'a>(
        &'a self,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn create_device_token<'a>(
        &'a self,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn list_device_tokens<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn active_device_token<'a>(
        &'a self,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn rotate_device_token<'a>(
        &'a self,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn revoke_device_token<'a>(
        &'a self,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>>;
}

impl DeviceTokenRepository for PlatformStore {
    fn provision_device_token<'a>(
        &'a self,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_device_token(self, display_name, token).await })
    }

    fn provision_owned_device_token<'a>(
        &'a self,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_owned_device_token(self, device).await })
    }

    fn create_device_token<'a>(
        &'a self,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { create_device_token(self, device_id, token).await })
    }

    fn list_device_tokens<'a>(
        &'a self,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_device_tokens(self, device_id).await })
    }

    fn active_device_token<'a>(
        &'a self,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { active_device_token(self, token_id).await })
    }

    fn rotate_device_token<'a>(
        &'a self,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { rotate_device_token(self, token_id, token).await })
    }

    fn revoke_device_token<'a>(
        &'a self,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>> {
        Box::pin(async move { revoke_device_token(self, token_id).await })
    }
}

async fn list_management_devices(
    store: &PlatformStore,
) -> Result<Vec<ManagementDevice>, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE deleted_at IS NULL
                 ORDER BY device_id",
            )
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT d.device_id, d.display_name, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.deleted_at IS NULL
                 ORDER BY d.device_id",
            )
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_device_from_row)
                .collect()
        }
    }
}

async fn management_device(
    store: &PlatformStore,
    device_id: &str,
) -> Result<ManagementDevice, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE device_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            sqlite_management_device_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT d.device_id, d.display_name, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.device_id = $1 AND d.deleted_at IS NULL",
            )
            .bind(device_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            timescale_management_device_from_row(row)
        }
    }
}

fn sqlite_management_device_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementDevice, ManagementDeviceError> {
    let last_seen_at = sqlite_timestamp(row.try_get("last_seen_at")?)?;
    let gateway_last_read_at = sqlite_timestamp(row.try_get("gateway_last_read_at")?)?;
    let topology = ManagementDeviceTopology {
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
        gateway_device_id: row.try_get("gateway_device_id")?,
    };
    let health = management_health(
        &topology,
        last_seen_at,
        gateway_last_read_at,
        row.try_get::<Option<String>, _>("gateway_read_quality")?
            .as_deref(),
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row
            .try_get::<Option<String>, _>("asset_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        device_profile_id: row
            .try_get::<Option<String>, _>("device_profile_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        attributes: serde_json::from_str(&row.try_get::<String, _>("metadata")?)
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        topology,
        health,
    })
}

fn timescale_management_device_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementDevice, ManagementDeviceError> {
    let topology = ManagementDeviceTopology {
        is_gateway: row.try_get("is_gateway")?,
        gateway_device_id: row.try_get("gateway_device_id")?,
    };
    let health = management_health(
        &topology,
        row.try_get("last_seen_at")?,
        row.try_get("gateway_last_read_at")?,
        row.try_get::<Option<String>, _>("gateway_read_quality")?
            .as_deref(),
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row.try_get("asset_id")?,
        device_profile_id: row.try_get("device_profile_id")?,
        attributes: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        topology,
        health,
    })
}

fn management_health(
    topology: &ManagementDeviceTopology,
    last_seen_at: Option<DateTime<Utc>>,
    gateway_last_read_at: Option<DateTime<Utc>>,
    gateway_read_quality: Option<&str>,
) -> ManagementDeviceHealth {
    let fresh_after = Utc::now() - Duration::minutes(5);
    if topology.is_gateway {
        let online = last_seen_at.is_some_and(|seen| seen >= fresh_after);
        return ManagementDeviceHealth {
            online,
            last_seen_at,
            gateway_status: Some(if online {
                ManagementGatewayStatus::Online
            } else {
                ManagementGatewayStatus::Offline
            }),
            child_status: None,
        };
    }
    if topology.gateway_device_id.is_some() {
        let unavailable_after = fresh_after - Duration::minutes(10);
        let status = if gateway_read_quality == Some("unavailable") {
            ManagementChildStatus::Unavailable
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= fresh_after) {
            ManagementChildStatus::Fresh
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= unavailable_after) {
            ManagementChildStatus::Stale
        } else {
            ManagementChildStatus::Unavailable
        };
        return ManagementDeviceHealth {
            online: status == ManagementChildStatus::Fresh,
            last_seen_at: gateway_last_read_at,
            gateway_status: None,
            child_status: Some(status),
        };
    }
    ManagementDeviceHealth {
        online: last_seen_at.is_some_and(|seen| seen >= fresh_after),
        last_seen_at,
        gateway_status: None,
        child_status: None,
    }
}

fn sqlite_timestamp(value: Option<String>) -> Result<Option<DateTime<Utc>>, ManagementDeviceError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|value| value.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                        .map(|value| value.and_utc())
                })
                .map_err(|_| ManagementDeviceError::InvalidStoredTimestamp)
        })
        .transpose()
}

async fn update_management_device(
    store: &PlatformStore,
    device_id: &str,
    update: UpdateManagementDevice,
) -> Result<ManagementDevice, ManagementDeviceError> {
    validate_device_id(device_id)?;
    let display_name = validate_display_name(&update.display_name)?.to_owned();
    let attributes = validate_attributes(update.attributes)?;

    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            let current = sqlite_topology(&mut transaction, device_id).await?;
            let topology = update.topology.unwrap_or(current.clone());
            validate_sqlite_topology(device_id, &current, &topology, &mut transaction).await?;
            validate_sqlite_references(&mut transaction, update.asset_id, update.device_profile_id)
                .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = ?, asset_id = ?, device_profile_id = ?,
                     metadata = COALESCE(?, metadata), is_gateway = ?, gateway_device_id = ?
                 WHERE device_id = ? AND deleted_at IS NULL",
            )
            .bind(display_name)
            .bind(update.asset_id.map(|id| id.to_string()))
            .bind(update.device_profile_id.map(|id| id.to_string()))
            .bind(attributes.map(|value| value.to_string()))
            .bind(i64::from(topology.is_gateway))
            .bind(&topology.gateway_device_id)
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = ?
                     WHERE device_id = ? AND revoked_at IS NULL",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(device_id)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let current = timescale_topology(&mut transaction, device_id).await?;
            let topology = update.topology.unwrap_or(current.clone());
            validate_timescale_topology(device_id, &current, &topology, &mut transaction).await?;
            validate_timescale_references(
                &mut transaction,
                update.asset_id,
                update.device_profile_id,
            )
            .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = $2, asset_id = $3, device_profile_id = $4,
                     metadata = COALESCE($5, metadata), is_gateway = $6, gateway_device_id = $7
                 WHERE device_id = $1 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(display_name)
            .bind(update.asset_id)
            .bind(update.device_profile_id)
            .bind(attributes.map(Json))
            .bind(topology.is_gateway)
            .bind(&topology.gateway_device_id)
            .execute(&mut *transaction)
            .await?;
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = now()
                     WHERE device_id = $1 AND revoked_at IS NULL",
                )
                .bind(device_id)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
    }
    management_device(store, device_id).await
}

async fn delete_management_device(
    store: &PlatformStore,
    device_id: &str,
) -> Result<(), ManagementDeviceError> {
    validate_device_id(device_id)?;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            if sqlite_has_children(&mut transaction, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = ?
                 WHERE device_id = ? AND deleted_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = ?
                 WHERE device_id = ? AND revoked_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            if timescale_has_children(&mut transaction, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = now()
                 WHERE device_id = $1 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = now()
                 WHERE device_id = $1 AND revoked_at IS NULL",
            )
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn validate_device_id(device_id: &str) -> Result<(), ManagementDeviceError> {
    if !device_id.is_empty()
        && device_id.len() <= 64
        && device_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'-' || value == b'_')
    {
        Ok(())
    } else {
        Err(ManagementDeviceError::InvalidDeviceId(device_id.to_owned()))
    }
}

fn validate_display_name(value: &str) -> Result<&str, ManagementDeviceError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        Err(ManagementDeviceError::InvalidDisplayName)
    } else {
        Ok(value)
    }
}

fn validate_attributes(
    attributes: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, ManagementDeviceError> {
    attributes
        .map(|value| {
            if value.is_null() {
                Ok(serde_json::json!({}))
            } else if value.is_object() {
                Ok(value)
            } else {
                Err(ManagementDeviceError::AttributesMustBeObject)
            }
        })
        .transpose()
}

async fn sqlite_topology(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementDeviceError::DeviceNotFound)?;
    Ok(ManagementDeviceTopology {
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

async fn timescale_topology(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementDeviceError::DeviceNotFound)?;
    Ok(ManagementDeviceTopology {
        is_gateway: row.try_get("is_gateway")?,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

async fn sqlite_has_children(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = ? AND deleted_at IS NULL
         LIMIT 1",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_has_children(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn validate_sqlite_topology(
    device_id: &str,
    current: &ManagementDeviceTopology,
    topology: &ManagementDeviceTopology,
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), ManagementDeviceError> {
    if topology.is_gateway && topology.gateway_device_id.is_some() {
        return Err(ManagementDeviceError::GatewayCannotHaveParent);
    }
    if topology.gateway_device_id.as_deref() == Some(device_id) {
        return Err(ManagementDeviceError::DeviceCannotBeOwnGateway);
    }
    if current.is_gateway
        && !topology.is_gateway
        && sqlite_has_children(transaction, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, i64>(
            "SELECT is_gateway FROM devices
             WHERE device_id = ? AND deleted_at IS NULL",
        )
        .bind(gateway_device_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ManagementDeviceError::GatewayUnavailable)?;
        if is_gateway == 0 {
            return Err(ManagementDeviceError::GatewayIsNotGateway);
        }
    }
    Ok(())
}

async fn validate_timescale_topology(
    device_id: &str,
    current: &ManagementDeviceTopology,
    topology: &ManagementDeviceTopology,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ManagementDeviceError> {
    if topology.is_gateway && topology.gateway_device_id.is_some() {
        return Err(ManagementDeviceError::GatewayCannotHaveParent);
    }
    if topology.gateway_device_id.as_deref() == Some(device_id) {
        return Err(ManagementDeviceError::DeviceCannotBeOwnGateway);
    }
    if current.is_gateway
        && !topology.is_gateway
        && timescale_has_children(transaction, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, bool>(
            "SELECT is_gateway FROM devices
             WHERE device_id = $1 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(gateway_device_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ManagementDeviceError::GatewayUnavailable)?;
        if !is_gateway {
            return Err(ManagementDeviceError::GatewayIsNotGateway);
        }
    }
    Ok(())
}

async fn validate_sqlite_references(
    transaction: &mut Transaction<'_, Sqlite>,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ?")
            .bind(asset_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some();
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM device_profiles WHERE id = ?")
            .bind(device_profile_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some();
        if !exists {
            return Err(ManagementDeviceError::DeviceProfileUnavailable(
                device_profile_id,
            ));
        }
    }
    Ok(())
}

async fn validate_timescale_references(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1)")
                .bind(asset_id)
                .fetch_one(&mut **transaction)
                .await?;
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM device_profiles WHERE id = $1)",
        )
        .bind(device_profile_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementDeviceError::DeviceProfileUnavailable(
                device_profile_id,
            ));
        }
    }
    Ok(())
}

async fn provision_device_token(
    store: &PlatformStore,
    display_name: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            sqlx::query(
                "INSERT INTO devices (device_id, display_name)
                 VALUES (?, ?)",
            )
            .bind(&device_id)
            .bind(display_name)
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "INSERT INTO devices (device_id, display_name)
                 VALUES ($1, $2)",
            )
            .bind(&device_id)
            .bind(display_name)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn provision_owned_device_token(
    store: &PlatformStore,
    device: NewOwnedDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            sqlx::query(
                "INSERT INTO devices (
                     device_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&device_id)
            .bind(&device.display_name)
            .bind(device.owner_user_id.to_string())
            .bind(device.asset_id.map(|id| id.to_string()))
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_sqlite_device_token(&mut transaction, &device_id, device.token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "INSERT INTO devices (
                     device_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES ($1, $2, $3, $4, now())",
            )
            .bind(&device_id)
            .bind(&device.display_name)
            .bind(device.owner_user_id)
            .bind(device.asset_id)
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_timescale_device_token(&mut transaction, &device_id, device.token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn create_device_token(
    store: &PlatformStore,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            ensure_sqlite_token_eligible(&mut transaction, device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = ?
                 WHERE device_id = ? AND revoked_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_timescale_token_eligible(&mut transaction, device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = now()
                 WHERE device_id = $1 AND revoked_at IS NULL",
            )
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn list_device_tokens(
    store: &PlatformStore,
    device_id: &str,
) -> Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, device_id, token_prefix, created_at, last_used_at, revoked_at
                 FROM device_tokens
                 WHERE device_id = ?
                 ORDER BY created_at DESC, id DESC",
            )
            .bind(device_id)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_device_token_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, device_id, token_prefix, created_at, last_used_at, revoked_at
                 FROM device_tokens
                 WHERE device_id = $1
                 ORDER BY created_at DESC, id DESC",
            )
            .bind(device_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_device_token_record)
                .collect()
        }
    }
}

async fn active_device_token(
    store: &PlatformStore,
    token_id: Uuid,
) -> Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, device_id, token_prefix, created_at, last_used_at, revoked_at
             FROM device_tokens
             WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(token_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_device_token_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, device_id, token_prefix, created_at, last_used_at, revoked_at
             FROM device_tokens
             WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(token_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_device_token_record)
        .transpose(),
    }
}

async fn rotate_device_token(
    store: &PlatformStore,
    token_id: Uuid,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            let device_id = sqlite_active_token_device_id(&mut transaction, token_id).await?;
            ensure_sqlite_token_eligible(&mut transaction, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(token_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let device_id = timescale_active_token_device_id(&mut transaction, token_id).await?;
            ensure_timescale_token_eligible(&mut transaction, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
            )
            .bind(token_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn revoke_device_token(
    store: &PlatformStore,
    token_id: Uuid,
) -> Result<(), DeviceTokenRepositoryError> {
    let revoked = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(token_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(token_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if revoked == 0 {
        Err(DeviceTokenRepositoryError::TokenNotFound)
    } else {
        Ok(())
    }
}

async fn sqlite_active_token_device_id(
    transaction: &mut Transaction<'_, Sqlite>,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT device_id FROM device_tokens
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(token_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn timescale_active_token_device_id(
    transaction: &mut Transaction<'_, Postgres>,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT device_id FROM device_tokens
         WHERE id = $1 AND revoked_at IS NULL
         FOR UPDATE",
    )
    .bind(token_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn ensure_sqlite_token_eligible(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::DeviceNotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenRepositoryError::GatewayChild)
    } else {
        Ok(())
    }
}

async fn ensure_timescale_token_eligible(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::DeviceNotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenRepositoryError::GatewayChild)
    } else {
        Ok(())
    }
}

async fn insert_sqlite_device_token(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let created_at = Utc::now();
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO device_tokens (
             id, device_id, token_prefix, token_hash, token_ciphertext, created_at
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(token.id.to_string())
    .bind(device_id)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(&token.token_ciphertext)
    .bind(created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Err(DeviceTokenRepositoryError::TokenPrefixConflict);
    }
    Ok(DeviceTokenRecord {
        id: token.id,
        device_id: device_id.to_owned(),
        token_prefix: token.token_prefix,
        created_at,
        last_used_at: None,
        revoked_at: None,
    })
}

async fn insert_timescale_device_token(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let row = sqlx::query(
        "INSERT INTO device_tokens (
             id, device_id, token_prefix, token_hash, token_ciphertext
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (token_prefix) DO NOTHING
         RETURNING id, device_id, token_prefix, created_at, last_used_at, revoked_at",
    )
    .bind(token.id)
    .bind(device_id)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(&token.token_ciphertext)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenPrefixConflict)?;
    Ok(DeviceTokenRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn sqlite_device_token_record(
    row: sqlx::sqlite::SqliteRow,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    Ok(DeviceTokenRecord {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| DeviceTokenRepositoryError::TokenNotFound)?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: sqlite_device_token_timestamp(row.try_get("created_at")?)?,
        last_used_at: row
            .try_get::<Option<String>, _>("last_used_at")?
            .map(sqlite_device_token_timestamp)
            .transpose()?,
        revoked_at: row
            .try_get::<Option<String>, _>("revoked_at")?
            .map(sqlite_device_token_timestamp)
            .transpose()?,
    })
}

fn timescale_device_token_record(
    row: sqlx::postgres::PgRow,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    Ok(DeviceTokenRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn sqlite_device_token_timestamp(
    value: String,
) -> Result<DateTime<Utc>, DeviceTokenRepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc())
        })
        .map_err(|_| DeviceTokenRepositoryError::InvalidStoredTimestamp)
}
