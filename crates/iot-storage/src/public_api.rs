use std::collections::HashMap;

use super::*;
use sqlx::QueryBuilder;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicPrincipal {
    pub tenant_id: Uuid,
    pub user_id: Option<Uuid>,
    pub app_id: String,
    pub account_class: AccountClass,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicAsset {
    pub id: Uuid,
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub access: Option<ResourceAccess>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub metadata: serde_json::Value,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub access: Option<ResourceAccess>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewPublicDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub metadata: serde_json::Value,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum PublicDeviceError {
    #[error("public device operations require a user principal")]
    Unauthorized,
    #[error("public device asset is unavailable: {0}")]
    AssetUnavailable(Uuid),
    #[error("public device profile is unavailable: {0}")]
    DeviceProfileUnavailable(Uuid),
    #[error("public device storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for PublicDeviceError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for PublicDeviceError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

#[derive(Debug, Error)]
pub enum PublicAssetError {
    #[error("public asset operations require a user principal")]
    Unauthorized,
    #[error("public asset parent is unavailable: {0}")]
    ParentUnavailable(Uuid),
    #[error("public asset profile is unavailable: {0}")]
    AssetProfileUnavailable(Uuid),
    #[error("public asset storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for PublicAssetError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for PublicAssetError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewPublicAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicTelemetry {
    pub event_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub device_id: String,
    pub boot_id: String,
    pub sequence: i64,
    pub measurements: serde_json::Value,
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicAlert {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub severity: String,
    pub device_id: String,
    pub status: String,
    pub condition_started_at: DateTime<Utc>,
    pub opened_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub last_value: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

pub trait PublicApiRepository: Send + Sync {
    fn public_user_tenant_id<'a>(
        &'a self,
        user_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, PlatformStoreError>> + Send + 'a>>;
    fn public_device_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn public_asset_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn get_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicDevice>, PlatformStoreError>> + Send + 'a>>;
    fn list_public_devices<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicDevice>, PlatformStoreError>> + Send + 'a>>;
    fn create_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device: NewPublicDevice,
    ) -> Pin<Box<dyn Future<Output = Result<PublicDevice, PublicDeviceError>> + Send + 'a>>;
    fn update_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
        device: NewPublicDevice,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicDevice>, PublicDeviceError>> + Send + 'a>>;
    fn delete_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
    fn list_public_assets<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAsset>, PlatformStoreError>> + Send + 'a>>;
    fn get_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>;
    fn create_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<PublicAsset, PublicAssetError>> + Send + 'a>>;
    fn update_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PublicAssetError>> + Send + 'a>>;
    fn delete_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
    fn list_public_telemetry<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: Option<&'a str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicTelemetry>, PlatformStoreError>> + Send + 'a>>;
    fn list_public_alerts<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
    fn get_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
    fn acknowledge_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
        actor: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
}

impl PublicApiRepository for PlatformStore {
    fn public_user_tenant_id<'a>(
        &'a self,
        user_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Uuid>, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { public_user_tenant_id(self, user_id).await })
    }

    fn public_device_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { public_device_permission(self, principal, device_id).await })
    }

    fn public_asset_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { public_asset_permission(self, principal, asset_id).await })
    }

    fn get_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicDevice>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            if public_device_permission(self, principal, device_id)
                .await?
                .is_none()
            {
                return Ok(None);
            }
            get_public_device(self, principal, device_id).await
        })
    }

    fn list_public_devices<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicDevice>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { list_public_devices(self, principal, after, limit).await })
    }

    fn create_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device: NewPublicDevice,
    ) -> Pin<Box<dyn Future<Output = Result<PublicDevice, PublicDeviceError>> + Send + 'a>> {
        Box::pin(async move { create_public_device(self, principal, device).await })
    }

    fn update_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
        device: NewPublicDevice,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicDevice>, PublicDeviceError>> + Send + 'a>>
    {
        Box::pin(async move { update_public_device(self, principal, device_id, device).await })
    }

    fn delete_public_device<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { delete_public_device(self, principal, device_id).await })
    }

    fn list_public_assets<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAsset>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { list_public_assets(self, principal, after, limit).await })
    }

    fn get_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            if public_asset_permission(self, principal, asset_id)
                .await?
                .is_none()
            {
                return Ok(None);
            }
            get_public_asset(self, principal, asset_id).await
        })
    }

    fn create_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<PublicAsset, PublicAssetError>> + Send + 'a>> {
        Box::pin(async move { create_public_asset(self, principal, asset).await })
    }

    fn update_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PublicAssetError>> + Send + 'a>>
    {
        Box::pin(async move { update_public_asset(self, principal, asset_id, asset).await })
    }

    fn delete_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { delete_public_asset(self, principal, asset_id).await })
    }

    fn list_public_telemetry<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: Option<&'a str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicTelemetry>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            list_public_telemetry(self, principal, device_id, from, to, after, limit).await
        })
    }

    fn list_public_alerts<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { list_public_alerts(self, principal, after, limit).await })
    }

    fn get_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { get_public_alert(self, principal, alert_id).await })
    }

    fn acknowledge_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
        actor: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { acknowledge_public_alert(self, principal, alert_id, actor).await })
    }
}

async fn public_user_tenant_id(
    store: &PlatformStore,
    user_id: Uuid,
) -> Result<Option<Uuid>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query_scalar::<_, String>("SELECT tenant_id FROM users WHERE id = ?")
                .bind(user_id.to_string())
                .fetch_optional(store.pool())
                .await?
                .map(|tenant_id| {
                    Uuid::parse_str(&tenant_id).map_err(|_| {
                        PlatformStoreError::Database(sqlx::Error::Protocol(
                            "invalid public user tenant ID".to_owned(),
                        ))
                    })
                })
                .transpose()
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar("SELECT tenant_id FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_optional(pool)
                .await
                .map_err(PlatformStoreError::from)
        }
    }
}

async fn public_authorization_subject(
    store: &PlatformStore,
    principal: &PublicPrincipal,
) -> Result<Option<AuthorizationSubject>, PlatformStoreError> {
    let Some(user_id) = principal.user_id else {
        return Ok(None);
    };
    let subject = store.authorization_subject(user_id).await?;
    Ok(subject.filter(|subject| subject.tenant_id == principal.tenant_id))
}

async fn public_device_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(None);
    };
    store.device_permission(&subject, device_id).await
}

async fn get_public_device(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<Option<PublicDevice>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT device_id, display_name, metadata, asset_id, device_profile_id
             FROM devices
             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
        )
        .bind(device_id)
        .bind(principal.tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_device_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT device_id, display_name, metadata, asset_id, device_profile_id
             FROM devices
             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
        )
        .bind(device_id)
        .bind(principal.tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_device_record)
        .transpose(),
    }
}

async fn list_public_devices(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicDevice>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(Vec::new());
    };
    let authorized =
        AuthorizationRepository::list_authorized_devices(store, &subject, after, limit).await?;
    if authorized.is_empty() {
        return Ok(Vec::new());
    }
    let access_by_device_id = authorized
        .iter()
        .map(|device| (device.device_id.clone(), device.access))
        .collect::<HashMap<_, _>>();

    match store {
        PlatformStore::Sqlite(store) => {
            let mut query = QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT device_id, display_name, metadata, asset_id, device_profile_id
                 FROM devices
                 WHERE tenant_id = ",
            );
            query.push_bind(subject.tenant_id.to_string());
            query.push(" AND deleted_at IS NULL AND device_id IN (");
            for (index, authorized_device) in authorized.iter().enumerate() {
                if index > 0 {
                    query.push(", ");
                }
                query.push_bind(authorized_device.device_id.clone());
            }
            query.push(") ORDER BY device_id");
            query
                .build()
                .fetch_all(store.pool())
                .await?
                .into_iter()
                .map(|row| {
                    let mut device = sqlite_device_record(row)?;
                    device.access = access_by_device_id.get(&device.device_id).copied();
                    Ok(device)
                })
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let mut query = QueryBuilder::<sqlx::Postgres>::new(
                "SELECT device_id, display_name, metadata, asset_id, device_profile_id
                 FROM devices
                 WHERE tenant_id = ",
            );
            query.push_bind(subject.tenant_id);
            query.push(" AND deleted_at IS NULL AND device_id IN (");
            for (index, authorized_device) in authorized.iter().enumerate() {
                if index > 0 {
                    query.push(", ");
                }
                query.push_bind(authorized_device.device_id.clone());
            }
            query.push(") ORDER BY device_id");
            query
                .build()
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(|row| {
                    let mut device = timescale_device_record(row)?;
                    device.access = access_by_device_id.get(&device.device_id).copied();
                    Ok(device)
                })
                .collect()
        }
    }
}

async fn create_public_device(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device: NewPublicDevice,
) -> Result<PublicDevice, PublicDeviceError> {
    if principal.user_id.is_none() {
        return Err(PublicDeviceError::Unauthorized);
    }
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if let Some(asset_id) = device.asset_id {
                if !sqlite_public_asset_manager_permission(&mut transaction, principal, asset_id)
                    .await?
                {
                    return Err(PublicDeviceError::AssetUnavailable(asset_id));
                }
            }
            if let Some(device_profile_id) = device.device_profile_id {
                if !sqlite_public_device_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    device_profile_id,
                )
                .await?
                {
                    return Err(PublicDeviceError::DeviceProfileUnavailable(
                        device_profile_id,
                    ));
                }
            }
            let row = sqlx::query(
                "INSERT INTO devices (
                    device_id, tenant_id, display_name, metadata, asset_id, device_profile_id,
                    owner_user_id
                 )
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 RETURNING device_id, display_name, metadata, asset_id, device_profile_id",
            )
            .bind(&device.device_id)
            .bind(principal.tenant_id.to_string())
            .bind(&device.display_name)
            .bind(device.metadata.to_string())
            .bind(device.asset_id.map(|id| id.to_string()))
            .bind(device.device_profile_id.map(|id| id.to_string()))
            .bind(principal.user_id.map(|id| id.to_string()))
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| {
                map_public_device_reference_error(error, device.asset_id, device.device_profile_id)
            })?;
            let created = sqlite_device_record(row)?;
            transaction.commit().await?;
            Ok(created)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            lock_timescale_public_device_asset_assignment(
                &mut transaction,
                device.asset_id,
                principal.tenant_id,
            )
            .await?;
            if let Some(asset_id) = device.asset_id {
                if !timescale_public_asset_manager_permission(&mut transaction, principal, asset_id)
                    .await?
                {
                    return Err(PublicDeviceError::AssetUnavailable(asset_id));
                }
            }
            if let Some(device_profile_id) = device.device_profile_id {
                if !timescale_public_device_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    device_profile_id,
                )
                .await?
                {
                    return Err(PublicDeviceError::DeviceProfileUnavailable(
                        device_profile_id,
                    ));
                }
            }
            let row = sqlx::query(
                "INSERT INTO devices (
                    device_id, tenant_id, display_name, metadata, asset_id, device_profile_id,
                    owner_user_id
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 RETURNING device_id, display_name, metadata, asset_id, device_profile_id",
            )
            .bind(&device.device_id)
            .bind(principal.tenant_id)
            .bind(&device.display_name)
            .bind(sqlx::types::Json(device.metadata))
            .bind(device.asset_id)
            .bind(device.device_profile_id)
            .bind(principal.user_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| {
                map_public_device_reference_error(error, device.asset_id, device.device_profile_id)
            })?;
            let created = timescale_device_record(row)?;
            transaction.commit().await?;
            Ok(created)
        }
    }
}

async fn update_public_device(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
    device: NewPublicDevice,
) -> Result<Option<PublicDevice>, PublicDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if !sqlite_public_device_manager_permission(&mut transaction, principal, device_id)
                .await?
            {
                return Ok(None);
            }
            if let Some(asset_id) = device.asset_id {
                if !sqlite_public_asset_manager_permission(&mut transaction, principal, asset_id)
                    .await?
                {
                    return Ok(None);
                }
            }
            if let Some(device_profile_id) = device.device_profile_id {
                if !sqlite_public_device_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    device_profile_id,
                )
                .await?
                {
                    return Err(PublicDeviceError::DeviceProfileUnavailable(
                        device_profile_id,
                    ));
                }
            }
            let updated = sqlx::query(
                "UPDATE devices
                 SET display_name = ?, metadata = ?, asset_id = ?, device_profile_id = ?
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL
                 RETURNING device_id, display_name, metadata, asset_id, device_profile_id",
            )
            .bind(&device.display_name)
            .bind(device.metadata.to_string())
            .bind(device.asset_id.map(|id| id.to_string()))
            .bind(device.device_profile_id.map(|id| id.to_string()))
            .bind(device_id)
            .bind(principal.tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| {
                map_public_device_reference_error(error, device.asset_id, device.device_profile_id)
            })?
            .map(sqlite_device_record)
            .transpose()?;
            transaction.commit().await?;
            Ok(updated)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            if !timescale_public_device_manager_permission(&mut transaction, principal, device_id)
                .await?
            {
                return Ok(None);
            }
            lock_timescale_public_device_asset_assignment(
                &mut transaction,
                device.asset_id,
                principal.tenant_id,
            )
            .await?;
            if let Some(asset_id) = device.asset_id {
                if !timescale_public_asset_manager_permission(&mut transaction, principal, asset_id)
                    .await?
                {
                    return Ok(None);
                }
            }
            if let Some(device_profile_id) = device.device_profile_id {
                if !timescale_public_device_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    device_profile_id,
                )
                .await?
                {
                    return Err(PublicDeviceError::DeviceProfileUnavailable(
                        device_profile_id,
                    ));
                }
            }
            let updated = sqlx::query(
                "UPDATE devices
                 SET display_name = $2, metadata = $3, asset_id = $4, device_profile_id = $5
                 WHERE device_id = $1 AND tenant_id = $6 AND deleted_at IS NULL
                 RETURNING device_id, display_name, metadata, asset_id, device_profile_id",
            )
            .bind(device_id)
            .bind(&device.display_name)
            .bind(sqlx::types::Json(device.metadata))
            .bind(device.asset_id)
            .bind(device.device_profile_id)
            .bind(principal.tenant_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| {
                map_public_device_reference_error(error, device.asset_id, device.device_profile_id)
            })?
            .map(timescale_device_record)
            .transpose()?;
            transaction.commit().await?;
            Ok(updated)
        }
    }
}

async fn sqlite_public_asset_manager_permission(
    transaction: &mut Transaction<'_, Sqlite>,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    let asset_id = asset_id.to_string();
    let Some(user_id) = principal.user_id.map(|id| id.to_string()) else {
        return Ok(false);
    };
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id FROM assets WHERE id = ? AND tenant_id = ?",
    )
    .bind(&asset_id)
    .bind(principal.tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(owner) = owner else {
        return Ok(false);
    };
    if owner.as_deref() == Some(user_id.as_str()) {
        return Ok(true);
    }

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT ?, 0
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors JOIN assets ON assets.id = ancestors.id
            WHERE assets.tenant_id = ?
              AND assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1
         FROM resource_permissions AS permission
         JOIN ancestors ON permission.asset_id = ancestors.id
         WHERE permission.tenant_id = ?
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (ancestors.depth = 0 OR permission.inherit_children = 1)
           AND (
                permission.subject_user_id = ?
                OR EXISTS (
                    SELECT 1 FROM user_group_members AS membership
                    WHERE membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = ?
                )
           )
         LIMIT 1",
    )
    .bind(&asset_id)
    .bind(principal.tenant_id.to_string())
    .bind(principal.tenant_id.to_string())
    .bind(&user_id)
    .bind(&user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn sqlite_public_device_manager_permission(
    transaction: &mut Transaction<'_, Sqlite>,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let Some(user_id) = principal.user_id.map(|id| id.to_string()) else {
        return Ok(false);
    };
    let tenant_id = principal.tenant_id.to_string();
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(&tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(owner) = owner else {
        return Ok(false);
    };
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
    .bind(&tenant_id)
    .bind(&tenant_id)
    .bind(&tenant_id)
    .bind(device_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn sqlite_public_device_profile_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_profile_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    Ok(
        sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM device_profiles WHERE id = ? AND tenant_id = ?",
        )
        .bind(device_profile_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some(),
    )
}

async fn sqlite_public_asset_profile_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_profile_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    Ok(
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM asset_profiles WHERE id = ? AND tenant_id = ?")
            .bind(asset_profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some(),
    )
}

async fn sqlite_public_asset_parent_is_valid(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    parent_asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    let asset_id = asset_id.map(|id| id.to_string());
    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT id, 0
            FROM assets
            WHERE id = ? AND tenant_id = ?
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = ?
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         ),
         descendants(id, depth) AS (
            SELECT id, 0
            FROM assets
            WHERE id = ? AND tenant_id = ?
            UNION ALL
            SELECT child.id, descendants.depth + 1
            FROM descendants
            JOIN assets AS child
              ON child.parent_asset_id = descendants.id AND child.tenant_id = ?
            WHERE descendants.depth < 64
         )
         SELECT CASE
            WHEN NOT EXISTS (SELECT 1 FROM ancestors) THEN 0
            WHEN EXISTS (SELECT 1 FROM ancestors WHERE id = ?) THEN 0
            WHEN COALESCE((SELECT MAX(depth) FROM ancestors), -1)
               + 1
               + COALESCE((SELECT MAX(depth) FROM descendants), 0) > 64 THEN 0
            ELSE 1
         END",
    )
    .bind(parent_asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(&asset_id)
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(&asset_id)
    .fetch_one(&mut **transaction)
    .await?
        == 1)
}

async fn lock_timescale_public_device_asset_assignment(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Option<Uuid>,
    tenant_id: Uuid,
) -> Result<(), PlatformStoreError> {
    if let Some(asset_id) = asset_id {
        // Management asset deletion locks the asset before updating devices.
        // Follow that order so an assignment cannot form an asset/devices cycle.
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
         FOR SHARE OF asset",
        )
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_all(&mut **transaction)
        .await?;
        sqlx::query(
            "WITH RECURSIVE ancestors(id) AS (
             SELECT $1::uuid
             UNION
             SELECT asset.parent_asset_id
             FROM assets AS asset
             JOIN ancestors ON asset.id = ancestors.id
             WHERE asset.tenant_id = $2 AND asset.parent_asset_id IS NOT NULL
         )
         SELECT permission.id
         FROM resource_permissions AS permission
         JOIN ancestors ON permission.asset_id = ancestors.id
         WHERE permission.tenant_id = $2
         FOR SHARE OF permission",
        )
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_all(&mut **transaction)
        .await?;
    }
    sqlx::query("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn lock_timescale_public_asset_ancestors(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
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
         FOR SHARE OF asset",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

async fn timescale_public_asset_manager_permission(
    transaction: &mut Transaction<'_, Postgres>,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    let Some(user_id) = principal.user_id else {
        return Ok(false);
    };
    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id FROM assets WHERE id = $1 AND tenant_id = $2 FOR SHARE",
    )
    .bind(asset_id)
    .bind(principal.tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(owner) = owner else {
        return Ok(false);
    };
    if owner == Some(user_id) {
        return Ok(true);
    }

    lock_timescale_public_asset_ancestors(transaction, principal.tenant_id, asset_id).await?;

    sqlx::query(
        "SELECT group_id
         FROM user_group_members
         WHERE tenant_id = $1 AND user_id = $2
         FOR SHARE",
    )
    .bind(principal.tenant_id)
    .bind(user_id)
    .fetch_all(&mut **transaction)
    .await?;

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT $1::uuid, 0
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors JOIN assets ON assets.id = ancestors.id
            WHERE assets.tenant_id = $2
              AND assets.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1::bigint
         FROM resource_permissions AS permission
         JOIN ancestors ON permission.asset_id = ancestors.id
         WHERE permission.tenant_id = $2
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (ancestors.depth = 0 OR permission.inherit_children = TRUE)
           AND (
                permission.subject_user_id = $3
                OR EXISTS (
                    SELECT 1 FROM user_group_members AS membership
                    WHERE membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = $3
                )
           )
         LIMIT 1
         FOR SHARE OF permission",
    )
    .bind(asset_id)
    .bind(principal.tenant_id)
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_public_device_manager_permission(
    transaction: &mut Transaction<'_, Postgres>,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let Some(user_id) = principal.user_id else {
        return Ok(false);
    };
    let device = sqlx::query_as::<_, (Option<Uuid>, Option<Uuid>)>(
        "SELECT owner_user_id, asset_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR SHARE",
    )
    .bind(device_id)
    .bind(principal.tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((owner, asset_id)) = device else {
        return Ok(false);
    };
    if owner == Some(user_id) {
        return Ok(true);
    }

    if let Some(asset_id) = asset_id {
        lock_timescale_public_asset_ancestors(transaction, principal.tenant_id, asset_id).await?;
    }

    sqlx::query(
        "SELECT group_id
         FROM user_group_members
         WHERE tenant_id = $1 AND user_id = $2
         FOR SHARE",
    )
    .bind(principal.tenant_id)
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
    .bind(principal.tenant_id)
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_public_device_profile_exists(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_profile_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, Uuid>(
        "SELECT id
             FROM device_profiles
             WHERE id = $1 AND tenant_id = $2
             FOR KEY SHARE",
    )
    .bind(device_profile_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_public_asset_profile_exists(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_profile_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, Uuid>(
        "SELECT id
             FROM asset_profiles
             WHERE id = $1 AND tenant_id = $2
             FOR KEY SHARE",
    )
    .bind(asset_profile_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_public_asset_parent_is_valid(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    parent_asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT id, 0
            FROM assets
            WHERE id = $1 AND tenant_id = $2
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = $2
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         ),
         descendants(id, depth) AS (
            SELECT id, 0
            FROM assets
            WHERE id = $3 AND tenant_id = $2
            UNION ALL
            SELECT child.id, descendants.depth + 1
            FROM descendants
            JOIN assets AS child
              ON child.parent_asset_id = descendants.id AND child.tenant_id = $2
            WHERE descendants.depth < 64
         )
         SELECT CASE
            WHEN NOT EXISTS (SELECT 1 FROM ancestors) THEN 0::bigint
            WHEN EXISTS (SELECT 1 FROM ancestors WHERE id = $3) THEN 0::bigint
            WHEN COALESCE((SELECT MAX(depth) FROM ancestors), -1)
               + 1
               + COALESCE((SELECT MAX(depth) FROM descendants), 0) > 64 THEN 0::bigint
            ELSE 1::bigint
         END",
    )
    .bind(parent_asset_id)
    .bind(tenant_id)
    .bind(asset_id)
    .fetch_one(&mut **transaction)
    .await?
        == 1)
}

fn map_public_device_reference_error(
    error: sqlx::Error,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> PublicDeviceError {
    let Some(database) = error.as_database_error() else {
        return PublicDeviceError::from(error);
    };
    if database.is_foreign_key_violation() {
        match (database.constraint(), asset_id, device_profile_id) {
            (Some("devices_asset_id_fkey"), Some(asset_id), _) => {
                return PublicDeviceError::AssetUnavailable(asset_id);
            }
            (Some("devices_device_profile_id_fkey"), _, Some(device_profile_id)) => {
                return PublicDeviceError::DeviceProfileUnavailable(device_profile_id);
            }
            _ => {}
        }
    }
    PublicDeviceError::from(error)
}

async fn delete_public_device(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if !sqlite_public_device_manager_permission(&mut transaction, principal, device_id)
                .await?
            {
                return Ok(false);
            }
            let affected = sqlx::query(
                "UPDATE devices
                 SET deleted_at = CURRENT_TIMESTAMP
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(principal.tenant_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            Ok(affected == 1)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            if !timescale_public_device_manager_permission(&mut transaction, principal, device_id)
                .await?
            {
                return Ok(false);
            }
            let affected = sqlx::query(
                "UPDATE devices
                 SET deleted_at = now()
                 WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(principal.tenant_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            Ok(affected == 1)
        }
    }
}

fn public_cursor_parts(
    after: Option<&str>,
) -> Result<Option<(DateTime<Utc>, String, i64)>, PlatformStoreError> {
    let Some(after) = after else {
        return Ok(None);
    };
    let mut parts = after.splitn(3, '|');
    let event_at = parts
        .next()
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .parse::<DateTime<Utc>>()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?;
    let device_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .to_owned();
    let sequence = parts
        .next()
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .parse::<i64>()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?;
    Ok(Some((event_at, device_id, sequence)))
}

async fn list_public_telemetry(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: Option<&str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicTelemetry>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(Vec::new());
    };
    let cursor = public_cursor_parts(after)?;
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let tenant_id = subject.tenant_id.to_string();
            let user_id = subject.user_id.to_string();
            let cursor_at = cursor.as_ref().map(|value| value.0.to_rfc3339());
            let cursor_device = cursor.as_ref().map(|value| value.1.clone());
            let cursor_sequence = cursor.as_ref().map(|value| value.2);
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(device_id, asset_id, owner_user_id) AS (
                     SELECT device_id, asset_id, owner_user_id
                     FROM devices
                     WHERE tenant_id = ? AND deleted_at IS NULL
                 ),
                 ancestors(device_id, asset_id, depth) AS (
                     SELECT device_id, asset_id, 0
                     FROM candidates
                     WHERE asset_id IS NOT NULL
                     UNION ALL
                     SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = ?
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 authorized(device_id) AS (
                     SELECT device_id FROM candidates
                     WHERE owner_user_id = ?
                     UNION
                     SELECT candidate.device_id
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.device_id = candidate.device_id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                     UNION
                     SELECT ancestors.device_id
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = 1
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                 )
                 SELECT t.event_at, t.received_at, t.device_id, t.boot_id,
                        t.sequence, t.measurements, t.topic
                 FROM telemetry AS t
                 JOIN authorized ON authorized.device_id = t.device_id
                 WHERE t.tenant_id = ?
                   AND t.event_at >= ? AND t.event_at <= ?
                   AND (? IS NULL OR t.device_id = ?)
                   AND (? IS NULL OR t.event_at > ?
                        OR (t.event_at = ? AND (t.device_id > ?
                            OR (t.device_id = ? AND t.sequence > ?))))
                 ORDER BY t.event_at, t.device_id, t.sequence
                 LIMIT ?",
            )
            .bind(&tenant_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(from.to_rfc3339())
            .bind(to.to_rfc3339())
            .bind(device_id)
            .bind(device_id)
            .bind(&cursor_at)
            .bind(&cursor_at)
            .bind(&cursor_at)
            .bind(&cursor_device)
            .bind(&cursor_device)
            .bind(cursor_sequence)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_telemetry_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(device_id, asset_id, owner_user_id) AS (
                     SELECT device_id, asset_id, owner_user_id
                     FROM devices
                     WHERE tenant_id = $1 AND deleted_at IS NULL
                 ),
                 ancestors(device_id, asset_id, depth) AS (
                     SELECT device_id, asset_id, 0
                     FROM candidates
                     WHERE asset_id IS NOT NULL
                     UNION ALL
                     SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = $1
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 authorized(device_id) AS (
                     SELECT device_id FROM candidates
                     WHERE owner_user_id = $2
                     UNION
                     SELECT candidate.device_id
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.device_id = candidate.device_id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = $2
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $2
                        )
                     UNION
                     SELECT ancestors.device_id
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = TRUE
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = $2
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $2
                        )
                 )
                 SELECT t.event_at, t.received_at, t.device_id, t.boot_id,
                        t.sequence, t.measurements, t.topic
                 FROM telemetry AS t
                 JOIN authorized ON authorized.device_id = t.device_id
                 WHERE t.tenant_id = $1
                   AND t.event_at >= $3 AND t.event_at <= $4
                   AND ($5::text IS NULL OR t.device_id = $5)
                   AND ($6::timestamptz IS NULL OR t.event_at > $6
                        OR (t.event_at = $6 AND (t.device_id > $7
                            OR (t.device_id = $7 AND t.sequence > $8))))
                 ORDER BY t.event_at, t.device_id, t.sequence
                 LIMIT $9",
            )
            .bind(subject.tenant_id)
            .bind(subject.user_id)
            .bind(from)
            .bind(to)
            .bind(device_id)
            .bind(cursor.as_ref().map(|value| value.0))
            .bind(cursor.as_ref().map(|value| value.1.clone()))
            .bind(cursor.as_ref().map(|value| value.2))
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter().map(timescale_telemetry_record).collect()
        }
    }
}

fn parse_public_timestamp(value: String) -> Result<DateTime<Utc>, PlatformStoreError> {
    if let Ok(value) = DateTime::parse_from_rfc3339(&value) {
        return Ok(value.with_timezone(&Utc));
    }
    NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S"))
        .map(|value| value.and_utc())
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public timestamp".to_owned(),
            ))
        })
}

fn sqlite_telemetry_record(row: SqliteRow) -> Result<PublicTelemetry, PlatformStoreError> {
    Ok(PublicTelemetry {
        event_at: parse_public_timestamp(row.try_get("event_at")?)?,
        received_at: parse_public_timestamp(row.try_get("received_at")?)?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get("boot_id")?,
        sequence: row.try_get("sequence")?,
        measurements: serde_json::from_str(&row.try_get::<String, _>("measurements")?).map_err(
            |_| {
                PlatformStoreError::Database(sqlx::Error::Protocol(
                    "invalid public telemetry measurements".to_owned(),
                ))
            },
        )?,
        topic: row.try_get("topic")?,
    })
}

fn timescale_telemetry_record(row: PgRow) -> Result<PublicTelemetry, PlatformStoreError> {
    Ok(PublicTelemetry {
        event_at: row.try_get("event_at")?,
        received_at: row.try_get("received_at")?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get::<Uuid, _>("boot_id")?.to_string(),
        sequence: row.try_get("sequence")?,
        measurements: row.try_get::<Json<serde_json::Value>, _>("measurements")?.0,
        topic: row.try_get("topic")?,
    })
}

async fn list_public_alerts(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicAlert>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(Vec::new());
    };
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let tenant_id = subject.tenant_id.to_string();
            let user_id = subject.user_id.to_string();
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(device_id, asset_id, owner_user_id) AS (
                     SELECT device_id, asset_id, owner_user_id
                     FROM devices
                     WHERE tenant_id = ? AND deleted_at IS NULL
                 ),
                 ancestors(device_id, asset_id, depth) AS (
                     SELECT device_id, asset_id, 0
                     FROM candidates
                     WHERE asset_id IS NOT NULL
                     UNION ALL
                     SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = ?
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 authorized(device_id) AS (
                     SELECT device_id FROM candidates
                     WHERE owner_user_id = ?
                     UNION
                     SELECT candidate.device_id
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.device_id = candidate.device_id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                     UNION
                     SELECT ancestors.device_id
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = 1
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                 )
                 SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.condition_started_at,
                        incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                        incidents.acknowledged_by, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 JOIN authorized ON authorized.device_id = incidents.device_id
                 WHERE incidents.tenant_id = ?
                   AND (? IS NULL OR incidents.id > ?)
                 ORDER BY incidents.id
                 LIMIT ?",
            )
            .bind(&tenant_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(after)
            .bind(after)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_alert_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(device_id, asset_id, owner_user_id) AS (
                     SELECT device_id, asset_id, owner_user_id
                     FROM devices
                     WHERE tenant_id = $1 AND deleted_at IS NULL
                 ),
                 ancestors(device_id, asset_id, depth) AS (
                     SELECT device_id, asset_id, 0
                     FROM candidates
                     WHERE asset_id IS NOT NULL
                     UNION ALL
                     SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = $1
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 authorized(device_id) AS (
                     SELECT device_id FROM candidates
                     WHERE owner_user_id = $2
                     UNION
                     SELECT candidate.device_id
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.device_id = candidate.device_id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = $2
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $2
                        )
                     UNION
                     SELECT ancestors.device_id
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = TRUE
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = $2
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $2
                        )
                 )
                 SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.condition_started_at,
                        incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                        incidents.acknowledged_by, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 JOIN authorized ON authorized.device_id = incidents.device_id
                 WHERE incidents.tenant_id = $1
                   AND ($3::uuid IS NULL OR incidents.id > $3)
                 ORDER BY incidents.id
                 LIMIT $4",
            )
            .bind(subject.tenant_id)
            .bind(subject.user_id)
            .bind(after.and_then(|value| Uuid::parse_str(value).ok()))
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter().map(timescale_alert_record).collect()
        }
    }
}

async fn get_public_alert(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    alert_id: Uuid,
) -> Result<Option<PublicAlert>, PlatformStoreError> {
    let alert = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules
                ON rules.id = incidents.rule_id
               AND rules.tenant_id = incidents.tenant_id
             JOIN devices AS devices
                ON devices.device_id = incidents.device_id
               AND devices.tenant_id = incidents.tenant_id
             WHERE incidents.id = ? AND incidents.tenant_id = ?",
        )
        .bind(alert_id.to_string())
        .bind(principal.tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_alert_record)
        .transpose()?,
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules
                ON rules.id = incidents.rule_id
               AND rules.tenant_id = incidents.tenant_id
             JOIN devices AS devices
                ON devices.device_id = incidents.device_id
               AND devices.tenant_id = incidents.tenant_id
             WHERE incidents.id = $1 AND incidents.tenant_id = $2",
        )
        .bind(alert_id)
        .bind(principal.tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_alert_record)
        .transpose()?,
    };
    let Some(alert) = alert else {
        return Ok(None);
    };
    if public_device_permission(store, principal, &alert.device_id)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    Ok(Some(alert))
}

async fn acknowledge_public_alert(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    alert_id: Uuid,
    actor: &str,
) -> Result<Option<PublicAlert>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let device_id = sqlx::query_scalar::<_, String>(
                "SELECT incidents.device_id
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 JOIN devices AS devices
                    ON devices.device_id = incidents.device_id
                   AND devices.tenant_id = incidents.tenant_id
                 WHERE incidents.id = ? AND incidents.tenant_id = ?",
            )
            .bind(alert_id.to_string())
            .bind(principal.tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(device_id) = device_id else {
                return Ok(None);
            };
            if !sqlite_public_device_manager_permission(&mut transaction, principal, &device_id)
                .await?
            {
                return Ok(None);
            }
            sqlx::query(
                "UPDATE alert_incidents
                 SET acknowledged_at = ?, acknowledged_by = ?, updated_at = ?
                 WHERE id = ?
                   AND tenant_id = ?
                   AND EXISTS (
                       SELECT 1 FROM alert_rules
                       WHERE alert_rules.id = alert_incidents.rule_id
                         AND alert_rules.tenant_id = alert_incidents.tenant_id
                   )
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = alert_incidents.device_id
                         AND devices.tenant_id = alert_incidents.tenant_id
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(actor)
            .bind(Utc::now().to_rfc3339())
            .bind(alert_id.to_string())
            .bind(principal.tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            let device_id = sqlx::query_scalar::<_, String>(
                "SELECT incidents.device_id
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 JOIN devices AS devices
                    ON devices.device_id = incidents.device_id
                   AND devices.tenant_id = incidents.tenant_id
                 WHERE incidents.id = $1 AND incidents.tenant_id = $2
                 FOR SHARE OF incidents",
            )
            .bind(alert_id)
            .bind(principal.tenant_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(device_id) = device_id else {
                return Ok(None);
            };
            if !timescale_public_device_manager_permission(&mut transaction, principal, &device_id)
                .await?
            {
                return Ok(None);
            }
            sqlx::query(
                "UPDATE alert_incidents
                 SET acknowledged_at = now(), acknowledged_by = $2, updated_at = now()
                 WHERE id = $1
                   AND tenant_id = $3
                   AND EXISTS (
                       SELECT 1 FROM alert_rules
                       WHERE alert_rules.id = alert_incidents.rule_id
                         AND alert_rules.tenant_id = alert_incidents.tenant_id
                   )
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = alert_incidents.device_id
                         AND devices.tenant_id = alert_incidents.tenant_id
                   )",
            )
            .bind(alert_id)
            .bind(actor)
            .bind(principal.tenant_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    get_public_alert(store, principal, alert_id).await
}

fn sqlite_optional_timestamp(
    value: Option<String>,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    value.map(parse_public_timestamp).transpose()
}

fn sqlite_alert_record(row: SqliteRow) -> Result<PublicAlert, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let rule_id: String = row.try_get("rule_id")?;
    Ok(PublicAlert {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public alert ID".to_owned(),
            ))
        })?,
        rule_id: Uuid::parse_str(&rule_id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public alert rule ID".to_owned(),
            ))
        })?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: parse_public_timestamp(row.try_get("condition_started_at")?)?,
        opened_at: sqlite_optional_timestamp(row.try_get("opened_at")?)?,
        resolved_at: sqlite_optional_timestamp(row.try_get("resolved_at")?)?,
        acknowledged_at: sqlite_optional_timestamp(row.try_get("acknowledged_at")?)?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: parse_public_timestamp(row.try_get("updated_at")?)?,
    })
}

fn timescale_alert_record(row: PgRow) -> Result<PublicAlert, PlatformStoreError> {
    Ok(PublicAlert {
        id: row.try_get("id")?,
        rule_id: row.try_get("rule_id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: row.try_get("condition_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn public_asset_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(None);
    };
    store.asset_permission(&subject, asset_id).await
}

async fn list_public_assets(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicAsset>, PlatformStoreError> {
    let Some(subject) = public_authorization_subject(store, principal).await? else {
        return Ok(Vec::new());
    };
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let tenant_id = subject.tenant_id.to_string();
            let user_id = subject.user_id.to_string();
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(
                     id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                 ) AS (
                     SELECT id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                     FROM assets
                     WHERE tenant_id = ?
                       AND (? IS NULL OR id > ?)
                 ),
                 ancestors(candidate_id, asset_id, depth) AS (
                     SELECT id, id, 0 FROM candidates
                     UNION ALL
                     SELECT ancestors.candidate_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = ?
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 access_candidates(id, permission_rank, source_rank, access_source) AS (
                     SELECT id, 3, 1, 'owner'
                     FROM candidates
                     WHERE owner_user_id = ?
                     UNION ALL
                     SELECT candidate.id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            2,
                            'direct_user'
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = candidate.id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = ?
                     UNION ALL
                     SELECT candidate.id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            3,
                            'group'
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = candidate.id
                      AND permission.revoked_at IS NULL
                     JOIN user_group_members AS membership
                       ON membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = ?
                     UNION ALL
                     SELECT ancestors.candidate_id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            4,
                            'inherited_user'
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = 1
                      AND permission.revoked_at IS NULL
                     WHERE ancestors.depth > 0 AND permission.subject_user_id = ?
                     UNION ALL
                     SELECT ancestors.candidate_id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            5,
                            'inherited_group'
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = ?
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = 1
                      AND permission.revoked_at IS NULL
                     JOIN user_group_members AS membership
                       ON membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = ?
                     WHERE ancestors.depth > 0
                 ),
                 authorized(id, effective_permission, access_source) AS (
                     SELECT id,
                            CASE permission_rank
                                WHEN 3 THEN 'owner'
                                WHEN 2 THEN 'manager'
                                ELSE 'viewer'
                            END,
                            access_source
                     FROM (
                         SELECT access_candidates.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY id
                                    ORDER BY permission_rank DESC, source_rank ASC
                                ) AS access_rank
                         FROM access_candidates
                     ) AS ranked_access
                     WHERE access_rank = 1
                 )
                 SELECT candidates.id, candidates.name, candidates.asset_profile_id,
                        candidates.parent_asset_id, candidates.metadata,
                        authorized.effective_permission, authorized.access_source
                 FROM candidates
                 JOIN authorized ON authorized.id = candidates.id
                 ORDER BY candidates.id
                 LIMIT ?",
            )
            .bind(&tenant_id)
            .bind(after)
            .bind(after)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(&tenant_id)
            .bind(&user_id)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_authorized_asset_record)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "WITH RECURSIVE candidates(
                     id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                 ) AS (
                     SELECT id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                     FROM assets
                     WHERE tenant_id = $1
                       AND ($2::uuid IS NULL OR id > $2)
                 ),
                 ancestors(candidate_id, asset_id, depth) AS (
                     SELECT id, id, 0 FROM candidates
                     UNION ALL
                     SELECT ancestors.candidate_id, asset.parent_asset_id, ancestors.depth + 1
                     FROM ancestors
                     JOIN assets AS asset
                       ON asset.id = ancestors.asset_id AND asset.tenant_id = $1
                     WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                 ),
                 access_candidates(id, permission_rank, source_rank, access_source) AS (
                     SELECT id, 3, 1, 'owner'
                     FROM candidates
                     WHERE owner_user_id = $3
                     UNION ALL
                     SELECT candidate.id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            2,
                            'direct_user'
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = candidate.id
                      AND permission.revoked_at IS NULL
                     WHERE permission.subject_user_id = $3
                     UNION ALL
                     SELECT candidate.id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            3,
                            'group'
                     FROM candidates AS candidate
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = candidate.id
                      AND permission.revoked_at IS NULL
                     JOIN user_group_members AS membership
                       ON membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = $3
                     UNION ALL
                     SELECT ancestors.candidate_id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            4,
                            'inherited_user'
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = TRUE
                      AND permission.revoked_at IS NULL
                     WHERE ancestors.depth > 0 AND permission.subject_user_id = $3
                     UNION ALL
                     SELECT ancestors.candidate_id,
                            CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                            5,
                            'inherited_group'
                     FROM ancestors
                     JOIN resource_permissions AS permission
                       ON permission.tenant_id = $1
                      AND permission.asset_id = ancestors.asset_id
                      AND permission.inherit_children = TRUE
                      AND permission.revoked_at IS NULL
                     JOIN user_group_members AS membership
                       ON membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = $3
                     WHERE ancestors.depth > 0
                 ),
                 authorized(id, effective_permission, access_source) AS (
                     SELECT id,
                            CASE permission_rank
                                WHEN 3 THEN 'owner'
                                WHEN 2 THEN 'manager'
                                ELSE 'viewer'
                            END,
                            access_source
                     FROM (
                         SELECT access_candidates.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY id
                                    ORDER BY permission_rank DESC, source_rank ASC
                                ) AS access_rank
                         FROM access_candidates
                     ) AS ranked_access
                     WHERE access_rank = 1
                 )
                 SELECT candidates.id, candidates.name, candidates.asset_profile_id,
                        candidates.parent_asset_id, candidates.metadata,
                        authorized.effective_permission, authorized.access_source
                 FROM candidates
                 JOIN authorized ON authorized.id = candidates.id
                 ORDER BY candidates.id
                 LIMIT $4",
            )
            .bind(subject.tenant_id)
            .bind(after.and_then(|value| Uuid::parse_str(value).ok()))
            .bind(subject.user_id)
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_authorized_asset_record)
                .collect()
        }
    }
}

async fn get_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<Option<PublicAsset>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, name, asset_profile_id, parent_asset_id, metadata
             FROM assets WHERE id = ? AND tenant_id = ?",
        )
        .bind(asset_id.to_string())
        .bind(principal.tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_asset_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, name, asset_profile_id, parent_asset_id, metadata
             FROM assets WHERE id = $1 AND tenant_id = $2",
        )
        .bind(asset_id)
        .bind(principal.tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_asset_record)
        .transpose(),
    }
}

async fn create_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset: NewPublicAsset,
) -> Result<PublicAsset, PublicAssetError> {
    if principal.user_id.is_none() {
        return Err(PublicAssetError::Unauthorized);
    }
    let id = Uuid::now_v7();
    let created = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if !sqlite_public_asset_manager_permission(
                    &mut transaction,
                    principal,
                    parent_asset_id,
                )
                .await?
                    || !sqlite_public_asset_parent_is_valid(
                        &mut transaction,
                        principal.tenant_id,
                        None,
                        parent_asset_id,
                    )
                    .await?
                {
                    return Err(PublicAssetError::ParentUnavailable(parent_asset_id));
                }
            }
            if let Some(asset_profile_id) = asset.asset_profile_id {
                if !sqlite_public_asset_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    asset_profile_id,
                )
                .await?
                {
                    return Err(PublicAssetError::AssetProfileUnavailable(asset_profile_id));
                }
            }
            let row = sqlx::query(
                "INSERT INTO assets (
                    id, tenant_id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(id.to_string())
            .bind(principal.tenant_id.to_string())
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(asset.parent_asset_id.map(|id| id.to_string()))
            .bind(principal.user_id.map(|id| id.to_string()))
            .bind(asset.metadata.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let created = sqlite_asset_record(row)?;
            transaction.commit().await?;
            created
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if !timescale_public_asset_manager_permission(
                    &mut transaction,
                    principal,
                    parent_asset_id,
                )
                .await?
                    || !timescale_public_asset_parent_is_valid(
                        &mut transaction,
                        principal.tenant_id,
                        None,
                        parent_asset_id,
                    )
                    .await?
                {
                    return Err(PublicAssetError::ParentUnavailable(parent_asset_id));
                }
            }
            if let Some(asset_profile_id) = asset.asset_profile_id {
                if !timescale_public_asset_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    asset_profile_id,
                )
                .await?
                {
                    return Err(PublicAssetError::AssetProfileUnavailable(asset_profile_id));
                }
            }
            let row = sqlx::query(
                "INSERT INTO assets (
                    id, tenant_id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(id)
            .bind(principal.tenant_id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(principal.user_id)
            .bind(sqlx::types::Json(asset.metadata))
            .fetch_one(&mut *transaction)
            .await?;
            let created = timescale_asset_record(row)?;
            transaction.commit().await?;
            created
        }
    };
    Ok(created)
}

async fn update_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
    asset: NewPublicAsset,
) -> Result<Option<PublicAsset>, PublicAssetError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if !sqlite_public_asset_manager_permission(&mut transaction, principal, asset_id)
                .await?
            {
                return Ok(None);
            }
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if !sqlite_public_asset_manager_permission(
                    &mut transaction,
                    principal,
                    parent_asset_id,
                )
                .await?
                    || !sqlite_public_asset_parent_is_valid(
                        &mut transaction,
                        principal.tenant_id,
                        Some(asset_id),
                        parent_asset_id,
                    )
                    .await?
                {
                    return Err(PublicAssetError::ParentUnavailable(parent_asset_id));
                }
            }
            if let Some(asset_profile_id) = asset.asset_profile_id {
                if !sqlite_public_asset_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    asset_profile_id,
                )
                .await?
                {
                    return Err(PublicAssetError::AssetProfileUnavailable(asset_profile_id));
                }
            }
            let updated = sqlx::query(
                "UPDATE assets
                 SET name = ?, asset_profile_id = ?, parent_asset_id = ?, metadata = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(asset.parent_asset_id.map(|id| id.to_string()))
            .bind(asset.metadata.to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(asset_id.to_string())
            .bind(principal.tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .map(sqlite_asset_record)
            .transpose()?;
            transaction.commit().await?;
            Ok(updated)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            if !timescale_public_asset_manager_permission(&mut transaction, principal, asset_id)
                .await?
            {
                return Ok(None);
            }
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if !timescale_public_asset_manager_permission(
                    &mut transaction,
                    principal,
                    parent_asset_id,
                )
                .await?
                    || !timescale_public_asset_parent_is_valid(
                        &mut transaction,
                        principal.tenant_id,
                        Some(asset_id),
                        parent_asset_id,
                    )
                    .await?
                {
                    return Err(PublicAssetError::ParentUnavailable(parent_asset_id));
                }
            }
            if let Some(asset_profile_id) = asset.asset_profile_id {
                if !timescale_public_asset_profile_exists(
                    &mut transaction,
                    principal.tenant_id,
                    asset_profile_id,
                )
                .await?
                {
                    return Err(PublicAssetError::AssetProfileUnavailable(asset_profile_id));
                }
            }
            let updated = sqlx::query(
                "UPDATE assets
                 SET name = $2, asset_profile_id = $3, parent_asset_id = $4,
                     metadata = $5, updated_at = now()
                 WHERE id = $1 AND tenant_id = $6
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(asset_id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(sqlx::types::Json(asset.metadata))
            .bind(principal.tenant_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(timescale_asset_record)
            .transpose()?;
            transaction.commit().await?;
            Ok(updated)
        }
    }
}

async fn delete_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            if !sqlite_public_asset_manager_permission(&mut transaction, principal, asset_id)
                .await?
            {
                return Ok(false);
            }
            let affected = sqlx::query("DELETE FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(asset_id.to_string())
                .bind(principal.tenant_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            Ok(affected == 1)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut *transaction)
                .await?;
            if !timescale_public_asset_manager_permission(&mut transaction, principal, asset_id)
                .await?
            {
                return Ok(false);
            }
            let affected = sqlx::query("DELETE FROM assets WHERE id = $1 AND tenant_id = $2")
                .bind(asset_id)
                .bind(principal.tenant_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            Ok(affected == 1)
        }
    }
}

fn sqlite_device_record(row: SqliteRow) -> Result<PublicDevice, PlatformStoreError> {
    let metadata = serde_json::from_str(&row.try_get::<String, _>("metadata")?).map_err(|_| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid public device metadata".to_owned(),
        ))
    })?;
    let asset_id = row
        .try_get::<Option<String>, _>("asset_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public device asset ID".to_owned(),
            ))
        })?;
    let device_profile_id = row
        .try_get::<Option<String>, _>("device_profile_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public device profile ID".to_owned(),
            ))
        })?;
    Ok(PublicDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        metadata,
        asset_id,
        device_profile_id,
        access: None,
    })
}

fn timescale_device_record(row: PgRow) -> Result<PublicDevice, PlatformStoreError> {
    Ok(PublicDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        metadata: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        asset_id: row.try_get("asset_id")?,
        device_profile_id: row.try_get("device_profile_id")?,
        access: None,
    })
}

fn sqlite_asset_record(row: SqliteRow) -> Result<PublicAsset, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let asset_profile_id = row
        .try_get::<Option<String>, _>("asset_profile_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public asset profile ID".to_owned(),
            ))
        })?;
    let parent_asset_id = row
        .try_get::<Option<String>, _>("parent_asset_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public parent asset ID".to_owned(),
            ))
        })?;
    let metadata = serde_json::from_str(&row.try_get::<String, _>("metadata")?).map_err(|_| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid public asset metadata".to_owned(),
        ))
    })?;
    Ok(PublicAsset {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public asset ID".to_owned(),
            ))
        })?,
        name: row.try_get("name")?,
        asset_profile_id,
        parent_asset_id,
        metadata,
        access: None,
    })
}

fn timescale_asset_record(row: PgRow) -> Result<PublicAsset, PlatformStoreError> {
    Ok(PublicAsset {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        asset_profile_id: row.try_get("asset_profile_id")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        metadata: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        access: None,
    })
}

fn public_resource_access(
    permission: String,
    source: String,
) -> Result<ResourceAccess, PlatformStoreError> {
    let permission = ResourcePermission::parse(&permission).ok_or_else(|| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid public resource permission".to_owned(),
        ))
    })?;
    let source = ResourceAccessSource::parse(&source).ok_or_else(|| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid public resource access source".to_owned(),
        ))
    })?;
    Ok(ResourceAccess { permission, source })
}

fn sqlite_authorized_asset_record(row: SqliteRow) -> Result<PublicAsset, PlatformStoreError> {
    let access = public_resource_access(
        row.try_get("effective_permission")?,
        row.try_get("access_source")?,
    )?;
    let mut asset = sqlite_asset_record(row)?;
    asset.access = Some(access);
    Ok(asset)
}

fn timescale_authorized_asset_record(row: PgRow) -> Result<PublicAsset, PlatformStoreError> {
    let access = public_resource_access(
        row.try_get("effective_permission")?,
        row.try_get("access_source")?,
    )?;
    let mut asset = timescale_asset_record(row)?;
    asset.access = Some(access);
    Ok(asset)
}
