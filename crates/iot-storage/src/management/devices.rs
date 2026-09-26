use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use sqlx::{Postgres, Row, Sqlite, Transaction, postgres::PgRow, sqlite::SqliteRow, types::Json};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AuditAction, AuditPrincipal, AuditTargetType, PlatformStore, PlatformStoreError, audit,
};

use super::tokens::{
    DeviceTokenRecord, DeviceTokenRepositoryError, NewDeviceToken, insert_sqlite_device_token,
    insert_timescale_device_token,
};

pub const MANAGEMENT_DEVICE_TELEMETRY_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub owner_user_id: Option<Uuid>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDeviceTelemetry {
    pub event_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub device_id: String,
    pub boot_id: String,
    pub sequence: i64,
    pub measurements: serde_json::Value,
    pub topic: String,
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
        tenant_id: Uuid,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    >;
    fn update_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>;
    fn delete_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>>;
}

impl ManagementDeviceRepository for PlatformStore {
    fn list_management_devices<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    > {
        Box::pin(async move { list_management_devices(self, tenant_id).await })
    }

    fn update_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>
    {
        Box::pin(async move {
            update_management_device(self, tenant_id, actor, device_id, update).await
        })
    }

    fn delete_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>> {
        Box::pin(async move { delete_management_device(self, tenant_id, actor, device_id).await })
    }
}

pub trait ManagementDeviceTelemetryRepository: Send + Sync {
    fn list_management_device_telemetry<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceTelemetry>, ManagementDeviceError>>
                + Send
                + 'a,
        >,
    >;
}

impl ManagementDeviceTelemetryRepository for PlatformStore {
    fn list_management_device_telemetry<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceTelemetry>, ManagementDeviceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            list_management_device_telemetry(self, tenant_id, device_id, from, to, limit).await
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProvisionManagementDevice {
    pub display_name: String,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub attributes: serde_json::Value,
    pub token: NewDeviceToken,
}

#[derive(Debug, Error)]
pub enum ProvisionManagementDeviceError {
    #[error("invalid device display name")]
    InvalidDisplayName,
    #[error("device attributes must be an object")]
    AttributesMustBeObject,
    #[error("assigned asset is unavailable: {0}")]
    AssetUnavailable(Uuid),
    #[error("assigned device profile is unavailable: {0}")]
    DeviceProfileUnavailable(Uuid),
    #[error("device token provisioning failed")]
    Token(#[source] DeviceTokenRepositoryError),
    #[error("management device provisioning storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<sqlx::Error> for ProvisionManagementDeviceError {
    fn from(source: sqlx::Error) -> Self {
        Self::Storage {
            source: PlatformStoreError::from(source),
        }
    }
}

impl PlatformStore {
    pub async fn provision_management_device_token(
        &self,
        tenant_id: Uuid,
        device: ProvisionManagementDevice,
    ) -> Result<DeviceTokenRecord, ProvisionManagementDeviceError> {
        let display_name = validate_display_name(&device.display_name)
            .map_err(map_management_device_provision_error)?
            .to_owned();
        if !device.attributes.is_object() {
            return Err(ProvisionManagementDeviceError::AttributesMustBeObject);
        }

        let device_id = Uuid::now_v7().to_string();
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                validate_sqlite_references(
                    &mut transaction,
                    tenant_id,
                    device.asset_id,
                    device.device_profile_id,
                )
                .await
                .map_err(map_management_device_provision_error)?;
                sqlx::query(
                    "INSERT INTO devices (
                         device_id, tenant_id, display_name, asset_id, device_profile_id, metadata
                     ) VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(&device_id)
                .bind(tenant_id.to_string())
                .bind(display_name)
                .bind(device.asset_id.map(|id| id.to_string()))
                .bind(device.device_profile_id.map(|id| id.to_string()))
                .bind(device.attributes.to_string())
                .execute(&mut *transaction)
                .await?;
                let record = insert_sqlite_device_token(&mut transaction, &device_id, device.token)
                    .await
                    .map_err(ProvisionManagementDeviceError::Token)?;
                transaction.commit().await?;
                Ok(record)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                validate_timescale_references(
                    &mut transaction,
                    tenant_id,
                    device.asset_id,
                    device.device_profile_id,
                )
                .await
                .map_err(map_management_device_provision_error)?;
                sqlx::query(
                    "INSERT INTO devices (
                         device_id, tenant_id, display_name, asset_id, device_profile_id, metadata
                     ) VALUES ($1, $2, $3, $4, $5, $6)",
                )
                .bind(&device_id)
                .bind(tenant_id)
                .bind(display_name)
                .bind(device.asset_id)
                .bind(device.device_profile_id)
                .bind(Json(device.attributes))
                .execute(&mut *transaction)
                .await?;
                let record =
                    insert_timescale_device_token(&mut transaction, &device_id, device.token)
                        .await
                        .map_err(ProvisionManagementDeviceError::Token)?;
                transaction.commit().await?;
                Ok(record)
            }
        }
    }
}

fn map_management_device_provision_error(
    error: ManagementDeviceError,
) -> ProvisionManagementDeviceError {
    match error {
        ManagementDeviceError::InvalidDisplayName => {
            ProvisionManagementDeviceError::InvalidDisplayName
        }
        ManagementDeviceError::AttributesMustBeObject => {
            ProvisionManagementDeviceError::AttributesMustBeObject
        }
        ManagementDeviceError::AssetUnavailable(asset_id) => {
            ProvisionManagementDeviceError::AssetUnavailable(asset_id)
        }
        ManagementDeviceError::DeviceProfileUnavailable(profile_id) => {
            ProvisionManagementDeviceError::DeviceProfileUnavailable(profile_id)
        }
        ManagementDeviceError::Storage { source } => {
            ProvisionManagementDeviceError::Storage { source }
        }
        ManagementDeviceError::InvalidDeviceId(_)
        | ManagementDeviceError::DeviceNotFound
        | ManagementDeviceError::GatewayCannotHaveParent
        | ManagementDeviceError::DeviceCannotBeOwnGateway
        | ManagementDeviceError::GatewayHasChildren
        | ManagementDeviceError::GatewayUnavailable
        | ManagementDeviceError::GatewayIsNotGateway
        | ManagementDeviceError::InvalidStoredAttributes
        | ManagementDeviceError::InvalidStoredTimestamp => {
            ProvisionManagementDeviceError::InvalidDisplayName
        }
    }
}

async fn list_management_devices(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementDevice>, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT device_id, display_name, owner_user_id, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE tenant_id = ? AND deleted_at IS NULL
                 ORDER BY device_id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT d.device_id, d.display_name, d.owner_user_id, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.tenant_id = $1 AND d.deleted_at IS NULL
                 ORDER BY d.device_id",
            )
            .bind(tenant_id)
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
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDevice, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT device_id, display_name, owner_user_id, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            sqlite_management_device_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT d.device_id, d.display_name, d.owner_user_id, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.device_id = $1 AND d.tenant_id = $2 AND d.deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            timescale_management_device_from_row(row)
        }
    }
}

async fn list_management_device_telemetry(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ManagementDeviceTelemetry>, ManagementDeviceError> {
    management_device(store, tenant_id, device_id).await?;
    let limit = i64::from(limit.min(MANAGEMENT_DEVICE_TELEMETRY_LIMIT as u32));
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT event_at, received_at, device_id, boot_id, sequence, measurements, topic
                 FROM telemetry
                 WHERE tenant_id = ? AND device_id = ? AND event_at >= ? AND event_at <= ?
                 ORDER BY event_at DESC, sequence DESC
                 LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(device_id)
            .bind(from.to_rfc3339())
            .bind(to.to_rfc3339())
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_telemetry_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT event_at, received_at, device_id, boot_id, sequence, measurements, topic
                 FROM telemetry
                 WHERE tenant_id = $1 AND device_id = $2 AND event_at >= $3 AND event_at <= $4
                 ORDER BY event_at DESC, sequence DESC
                 LIMIT $5",
            )
            .bind(tenant_id)
            .bind(device_id)
            .bind(from)
            .bind(to)
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_device_telemetry_from_row)
                .collect()
        }
    }
}

fn sqlite_management_device_telemetry_from_row(
    row: SqliteRow,
) -> Result<ManagementDeviceTelemetry, ManagementDeviceError> {
    Ok(ManagementDeviceTelemetry {
        event_at: sqlite_timestamp(row.try_get("event_at")?)?
            .ok_or(ManagementDeviceError::InvalidStoredTimestamp)?,
        received_at: sqlite_timestamp(row.try_get("received_at")?)?
            .ok_or(ManagementDeviceError::InvalidStoredTimestamp)?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get("boot_id")?,
        sequence: row.try_get("sequence")?,
        measurements: row.try_get::<Json<serde_json::Value>, _>("measurements")?.0,
        topic: row.try_get("topic")?,
    })
}

fn timescale_management_device_telemetry_from_row(
    row: PgRow,
) -> Result<ManagementDeviceTelemetry, ManagementDeviceError> {
    Ok(ManagementDeviceTelemetry {
        event_at: row.try_get("event_at")?,
        received_at: row.try_get("received_at")?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get("boot_id")?,
        sequence: row.try_get("sequence")?,
        measurements: row.try_get::<Json<serde_json::Value>, _>("measurements")?.0,
        topic: row.try_get("topic")?,
    })
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
        owner_user_id: row
            .try_get::<Option<String>, _>("owner_user_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
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
        owner_user_id: row.try_get("owner_user_id")?,
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
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device_id: &str,
    update: UpdateManagementDevice,
) -> Result<ManagementDevice, ManagementDeviceError> {
    validate_device_id(device_id)?;
    let display_name = validate_display_name(&update.display_name)?.to_owned();
    let attributes = validate_attributes(update.attributes)?;
    let user_actor_id = match actor {
        AuditPrincipal::User(user_id) => Some(user_id),
        AuditPrincipal::SystemAccount(_) | AuditPrincipal::TenantAccount(_) => None,
    };

    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if let Some(user_actor_id) = user_actor_id {
                sqlite_require_regular_management_device_user(
                    &mut transaction,
                    tenant_id,
                    user_actor_id,
                )
                .await?;
                sqlite_require_management_device_owner(
                    &mut transaction,
                    tenant_id,
                    device_id,
                    user_actor_id,
                )
                .await?;
                if let Some(asset_id) = update.asset_id {
                    sqlite_require_management_device_asset_owner(
                        &mut transaction,
                        tenant_id,
                        asset_id,
                        user_actor_id,
                    )
                    .await?;
                }
                if update.topology.is_some() {
                    return Err(ManagementDeviceError::DeviceNotFound);
                }
            }
            let current = sqlite_topology(&mut transaction, tenant_id, device_id).await?;
            let previous_asset_id: Option<String> = sqlx::query_scalar(
                "SELECT asset_id FROM devices
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let next_asset_id = update.asset_id.map(|id| id.to_string());
            let topology = update.topology.unwrap_or(current.clone());
            validate_sqlite_topology(tenant_id, device_id, &current, &topology, &mut transaction)
                .await?;
            let topology_changed = current.gateway_device_id != topology.gateway_device_id;
            validate_sqlite_references(
                &mut transaction,
                tenant_id,
                update.asset_id,
                update.device_profile_id,
            )
            .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = ?, asset_id = ?, device_profile_id = ?,
                     metadata = COALESCE(?, metadata), is_gateway = ?, gateway_device_id = ?,
                     gateway_topology_version = gateway_topology_version + ?
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(display_name)
            .bind(next_asset_id.as_deref())
            .bind(update.device_profile_id.map(|id| id.to_string()))
            .bind(attributes.map(|value| value.to_string()))
            .bind(i64::from(topology.is_gateway))
            .bind(&topology.gateway_device_id)
            .bind(i32::from(topology_changed))
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if previous_asset_id != next_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "asset_id": {
                            "before": previous_asset_id,
                            "after": next_asset_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            if topology_changed {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    gateway_audit_action(
                        current.gateway_device_id.as_deref(),
                        topology.gateway_device_id.as_deref(),
                    ),
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "gateway_device_id": {
                            "before": current.gateway_device_id,
                            "after": topology.gateway_device_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = ?
                     WHERE device_id = ? AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1 FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.tenant_id = ?
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(device_id)
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            // Match profile deletion before taking topology row locks.
            sqlx::query("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            if let Some(user_actor_id) = user_actor_id {
                timescale_require_regular_management_device_user(
                    &mut transaction,
                    tenant_id,
                    user_actor_id,
                )
                .await?;
                timescale_require_management_device_owner(
                    &mut transaction,
                    tenant_id,
                    device_id,
                    user_actor_id,
                )
                .await?;
                if let Some(asset_id) = update.asset_id {
                    timescale_require_management_device_asset_owner(
                        &mut transaction,
                        tenant_id,
                        asset_id,
                        user_actor_id,
                    )
                    .await?;
                }
                if update.topology.is_some() {
                    return Err(ManagementDeviceError::DeviceNotFound);
                }
            }
            let current = timescale_topology(&mut transaction, tenant_id, device_id).await?;
            let previous_asset_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT asset_id FROM devices
                 WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 FOR UPDATE",
            )
            .bind(device_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            let next_asset_id = update.asset_id;
            let topology = update.topology.unwrap_or(current.clone());
            validate_timescale_topology(
                tenant_id,
                device_id,
                &current,
                &topology,
                &mut transaction,
            )
            .await?;
            let topology_changed = current.gateway_device_id != topology.gateway_device_id;
            validate_timescale_references(
                &mut transaction,
                tenant_id,
                update.asset_id,
                update.device_profile_id,
            )
            .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = $2, asset_id = $3, device_profile_id = $4,
                     metadata = COALESCE($5, metadata), is_gateway = $6, gateway_device_id = $7,
                     gateway_topology_version = gateway_topology_version + $8
                 WHERE device_id = $1 AND tenant_id = $9 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(display_name)
            .bind(update.asset_id)
            .bind(update.device_profile_id)
            .bind(attributes.map(Json))
            .bind(topology.is_gateway)
            .bind(&topology.gateway_device_id)
            .bind(i32::from(topology_changed))
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            if previous_asset_id != next_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "asset_id": {
                            "before": previous_asset_id.map(|id| id.to_string()),
                            "after": next_asset_id.map(|id| id.to_string()),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            if topology_changed {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    gateway_audit_action(
                        current.gateway_device_id.as_deref(),
                        topology.gateway_device_id.as_deref(),
                    ),
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "gateway_device_id": {
                            "before": current.gateway_device_id,
                            "after": topology.gateway_device_id,
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = now()
                     WHERE device_id = $1 AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1 FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.tenant_id = $2
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(device_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
    }
    management_device(store, tenant_id, device_id).await
}

fn gateway_audit_action(
    previous_gateway_device_id: Option<&str>,
    next_gateway_device_id: Option<&str>,
) -> AuditAction {
    match (previous_gateway_device_id, next_gateway_device_id) {
        (None, Some(_)) => AuditAction::GatewayAssigned,
        (Some(_), None) => AuditAction::GatewayDetached,
        (Some(_), Some(_)) => AuditAction::GatewayReassigned,
        (None, None) => unreachable!("gateway audit action requires a topology change"),
    }
}

async fn delete_management_device(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device_id: &str,
) -> Result<(), ManagementDeviceError> {
    validate_device_id(device_id)?;
    let user_actor_id = match actor {
        AuditPrincipal::User(user_id) => Some(user_id),
        AuditPrincipal::SystemAccount(_) | AuditPrincipal::TenantAccount(_) => None,
    };
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if let Some(user_actor_id) = user_actor_id {
                sqlite_require_regular_management_device_user(
                    &mut transaction,
                    tenant_id,
                    user_actor_id,
                )
                .await?;
                sqlite_require_management_device_owner(
                    &mut transaction,
                    tenant_id,
                    device_id,
                    user_actor_id,
                )
                .await?;
            }
            if sqlite_has_children(&mut transaction, tenant_id, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = ?
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = ?
                 WHERE device_id = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = ?
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            if let Some(user_actor_id) = user_actor_id {
                timescale_require_regular_management_device_user(
                    &mut transaction,
                    tenant_id,
                    user_actor_id,
                )
                .await?;
                timescale_require_management_device_owner(
                    &mut transaction,
                    tenant_id,
                    device_id,
                    user_actor_id,
                )
                .await?;
            }
            if timescale_has_children(&mut transaction, tenant_id, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = now()
                 WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = now()
                 WHERE device_id = $1 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = $2
                   )",
            )
            .bind(device_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

pub(super) fn validate_device_id(device_id: &str) -> Result<(), ManagementDeviceError> {
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

async fn sqlite_require_regular_management_device_user(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let is_regular_user = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = ? AND tenant_id = ? AND account_class = 'user'",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if is_regular_user {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn sqlite_require_management_device_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
    owner_user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if owner.flatten().as_deref() == Some(owner_user_id.to_string().as_str()) {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn sqlite_require_management_device_asset_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
    owner_user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let owns_asset = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM assets WHERE id = ? AND tenant_id = ? AND owner_user_id = ?",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(owner_user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if owns_asset {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn timescale_require_regular_management_device_user(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let is_regular_user = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2 AND account_class = 'user'",
    )
    .bind(user_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if is_regular_user {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn timescale_require_management_device_owner(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
    owner_user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if owner.flatten() == Some(owner_user_id) {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn timescale_require_management_device_asset_owner(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    owner_user_id: Uuid,
) -> Result<(), ManagementDeviceError> {
    let owns_asset = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM assets
         WHERE id = $1 AND tenant_id = $2 AND owner_user_id = $3
         FOR SHARE",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .bind(owner_user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if owns_asset {
        Ok(())
    } else {
        Err(ManagementDeviceError::DeviceNotFound)
    }
}

async fn sqlite_topology(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
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
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
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
    tenant_id: Uuid,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = ? AND tenant_id = ? AND deleted_at IS NULL
         LIMIT 1",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_has_children(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn validate_sqlite_topology(
    tenant_id: Uuid,
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
        && sqlite_has_children(transaction, tenant_id, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, i64>(
            "SELECT is_gateway FROM devices
             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
        )
        .bind(gateway_device_id)
        .bind(tenant_id.to_string())
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
    tenant_id: Uuid,
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
        && timescale_has_children(transaction, tenant_id, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, bool>(
            "SELECT is_gateway FROM devices
             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(gateway_device_id)
        .bind(tenant_id)
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
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM device_profiles WHERE id = ? AND tenant_id = ?",
        )
        .bind(device_profile_id.to_string())
        .bind(tenant_id.to_string())
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
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
        )
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM device_profiles WHERE id = $1 AND tenant_id = $2
             )",
        )
        .bind(device_profile_id)
        .bind(tenant_id)
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
