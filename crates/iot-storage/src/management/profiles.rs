use std::{future::Future, pin::Pin};

use chrono::Utc;
use sqlx::{Row, types::Json};
use thiserror::Error;
use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDeviceProfile {
    pub id: Uuid,
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementDeviceProfile {
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementDeviceProfile {
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum ManagementDeviceProfileError {
    #[error("invalid device profile name")]
    InvalidName,
    #[error("device profile telemetry schema must be an object")]
    TelemetrySchemaMustBeObject,
    #[error("device profile metric mapping must be an object")]
    MetricMappingMustBeObject,
    #[error("device profile name already exists: {0:?}")]
    NameConflict(String),
    #[error("management device profile was not found")]
    DeviceProfileNotFound,
    #[error("device profile is still referenced: {0}")]
    DeviceProfileInUse(Uuid),
    #[error("stored device profile is invalid")]
    InvalidStoredProfile,
    #[error("management device profile storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementDeviceProfileError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementDeviceProfileError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementDeviceProfileRepository: Send + Sync {
    fn list_management_device_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn create_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn update_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceProfileError>> + Send + 'a>>;
}

impl ManagementDeviceProfileRepository for PlatformStore {
    fn list_management_device_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_device_profiles(self, tenant_id).await })
    }

    fn create_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_management_device_profile(self, tenant_id, profile).await })
    }

    fn update_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            update_management_device_profile(self, tenant_id, profile_id, profile).await
        })
    }

    fn delete_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceProfileError>> + Send + 'a>> {
        Box::pin(async move { delete_management_device_profile(self, tenant_id, profile_id).await })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAssetProfile {
    pub id: Uuid,
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementAssetProfile {
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementAssetProfile {
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum ManagementAssetProfileError {
    #[error("invalid asset profile name")]
    InvalidName,
    #[error("asset profile fields must be an object")]
    FieldsMustBeObject,
    #[error("asset profile dashboard defaults must be an object")]
    DashboardDefaultsMustBeObject,
    #[error("asset profile name already exists: {0:?}")]
    NameConflict(String),
    #[error("management asset profile was not found")]
    AssetProfileNotFound,
    #[error("asset profile is still referenced: {0}")]
    AssetProfileInUse(Uuid),
    #[error("stored asset profile is invalid")]
    InvalidStoredProfile,
    #[error("management asset profile storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAssetProfileError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAssetProfileError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAssetProfileRepository: Send + Sync {
    fn list_management_asset_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn create_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn update_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetProfileError>> + Send + 'a>>;
}

impl ManagementAssetProfileRepository for PlatformStore {
    fn list_management_asset_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_asset_profiles(self, tenant_id).await })
    }

    fn create_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_management_asset_profile(self, tenant_id, profile).await })
    }

    fn update_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            update_management_asset_profile(self, tenant_id, profile_id, profile).await
        })
    }

    fn delete_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetProfileError>> + Send + 'a>> {
        Box::pin(async move { delete_management_asset_profile(self, tenant_id, profile_id).await })
    }
}

fn management_profile_name(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= 128).then(|| value.to_owned())
}

fn validate_management_device_profile(
    id: Uuid,
    name: String,
    telemetry_schema: serde_json::Value,
    metric_mapping: serde_json::Value,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let name = management_profile_name(name).ok_or(ManagementDeviceProfileError::InvalidName)?;
    if !telemetry_schema.is_object() {
        return Err(ManagementDeviceProfileError::TelemetrySchemaMustBeObject);
    }
    if !metric_mapping.is_object() {
        return Err(ManagementDeviceProfileError::MetricMappingMustBeObject);
    }
    Ok(ManagementDeviceProfile {
        id,
        name,
        telemetry_schema,
        metric_mapping,
    })
}

fn validate_management_asset_profile(
    id: Uuid,
    name: String,
    fields: serde_json::Value,
    dashboard_defaults: serde_json::Value,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let name = management_profile_name(name).ok_or(ManagementAssetProfileError::InvalidName)?;
    if !fields.is_object() {
        return Err(ManagementAssetProfileError::FieldsMustBeObject);
    }
    if !dashboard_defaults.is_object() {
        return Err(ManagementAssetProfileError::DashboardDefaultsMustBeObject);
    }
    Ok(ManagementAssetProfile {
        id,
        name,
        fields,
        dashboard_defaults,
    })
}

fn map_management_device_profile_conflict(
    error: sqlx::Error,
    name: &str,
) -> ManagementDeviceProfileError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementDeviceProfileError::NameConflict(name.to_owned())
    } else {
        ManagementDeviceProfileError::from(error)
    }
}

fn map_management_asset_profile_conflict(
    error: sqlx::Error,
    name: &str,
) -> ManagementAssetProfileError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementAssetProfileError::NameConflict(name.to_owned())
    } else {
        ManagementAssetProfileError::from(error)
    }
}

async fn list_management_device_profiles(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, telemetry_schema, metric_mapping
                 FROM device_profiles
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_profile_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, telemetry_schema, metric_mapping
                 FROM device_profiles
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_device_profile_from_row)
                .collect()
        }
    }
}

async fn create_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile: CreateManagementDeviceProfile,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let profile = validate_management_device_profile(
        Uuid::now_v7(),
        profile.name,
        profile.telemetry_schema,
        profile.metric_mapping,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO device_profiles (
                    id, tenant_id, name, telemetry_schema, metric_mapping, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(profile.id.to_string())
            .bind(tenant_id.to_string())
            .bind(&profile.name)
            .bind(profile.telemetry_schema.to_string())
            .bind(profile.metric_mapping.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO device_profiles (
                    id, tenant_id, name, telemetry_schema, metric_mapping
                 ) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(profile.id)
            .bind(tenant_id)
            .bind(&profile.name)
            .bind(Json(profile.telemetry_schema.clone()))
            .bind(Json(profile.metric_mapping.clone()))
            .execute(pool)
            .await
            .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn update_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
    profile: UpdateManagementDeviceProfile,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let profile = validate_management_device_profile(
        profile_id,
        profile.name,
        profile.telemetry_schema,
        profile.metric_mapping,
    )?;
    let updated = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_profiles
                 SET name = ?, telemetry_schema = ?, metric_mapping = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
        )
        .bind(&profile.name)
        .bind(profile.telemetry_schema.to_string())
        .bind(profile.metric_mapping.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(profile.id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await
        .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_profiles
                 SET name = $2, telemetry_schema = $3, metric_mapping = $4, updated_at = now()
                 WHERE id = $1 AND tenant_id = $5",
        )
        .bind(profile.id)
        .bind(&profile.name)
        .bind(Json(profile.telemetry_schema.clone()))
        .bind(Json(profile.metric_mapping.clone()))
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?
        .rows_affected(),
    };
    if updated == 0 {
        return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
    }
    Ok(profile)
}

async fn delete_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> Result<(), ManagementDeviceProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM device_profiles WHERE id = ? AND tenant_id = ?",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM devices
                    WHERE device_profile_id = ? AND tenant_id = ? AND deleted_at IS NULL
                 )",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?
                != 0;
            if referenced {
                return Err(ManagementDeviceProfileError::DeviceProfileInUse(profile_id));
            }
            sqlx::query(
                "UPDATE devices
                 SET device_profile_id = NULL
                 WHERE device_profile_id = ? AND tenant_id = ? AND deleted_at IS NOT NULL",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM device_profiles WHERE id = ? AND tenant_id = ?")
                .bind(profile_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM device_profiles WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM devices
                    WHERE device_profile_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 )",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if referenced {
                return Err(ManagementDeviceProfileError::DeviceProfileInUse(profile_id));
            }
            sqlx::query(
                "UPDATE devices
                 SET device_profile_id = NULL
                 WHERE device_profile_id = $1 AND tenant_id = $2 AND deleted_at IS NOT NULL",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM device_profiles WHERE id = $1 AND tenant_id = $2")
                .bind(profile_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn sqlite_management_device_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?;
    validate_management_device_profile(
        id,
        row.try_get("name")?,
        serde_json::from_str(&row.try_get::<String, _>("telemetry_schema")?)
            .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?,
        serde_json::from_str(&row.try_get::<String, _>("metric_mapping")?)
            .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?,
    )
    .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)
}

fn timescale_management_device_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    validate_management_device_profile(
        row.try_get("id")?,
        row.try_get("name")?,
        row.try_get::<Json<serde_json::Value>, _>("telemetry_schema")?
            .0,
        row.try_get::<Json<serde_json::Value>, _>("metric_mapping")?
            .0,
    )
    .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)
}

async fn list_management_asset_profiles(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, fields, dashboard_defaults
                 FROM asset_profiles
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_asset_profile_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, fields, dashboard_defaults
                 FROM asset_profiles
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_asset_profile_from_row)
                .collect()
        }
    }
}

async fn create_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile: CreateManagementAssetProfile,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let profile = validate_management_asset_profile(
        Uuid::now_v7(),
        profile.name,
        profile.fields,
        profile.dashboard_defaults,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO asset_profiles (
                    id, tenant_id, name, fields, dashboard_defaults, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(profile.id.to_string())
            .bind(tenant_id.to_string())
            .bind(&profile.name)
            .bind(profile.fields.to_string())
            .bind(profile.dashboard_defaults.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO asset_profiles (id, tenant_id, name, fields, dashboard_defaults)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(profile.id)
            .bind(tenant_id)
            .bind(&profile.name)
            .bind(Json(profile.fields.clone()))
            .bind(Json(profile.dashboard_defaults.clone()))
            .execute(pool)
            .await
            .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn update_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
    profile: UpdateManagementAssetProfile,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let profile = validate_management_asset_profile(
        profile_id,
        profile.name,
        profile.fields,
        profile.dashboard_defaults,
    )?;
    let updated = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE asset_profiles
                 SET name = ?, fields = ?, dashboard_defaults = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
        )
        .bind(&profile.name)
        .bind(profile.fields.to_string())
        .bind(profile.dashboard_defaults.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(profile.id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await
        .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE asset_profiles
                 SET name = $2, fields = $3, dashboard_defaults = $4, updated_at = now()
                 WHERE id = $1 AND tenant_id = $5",
        )
        .bind(profile.id)
        .bind(&profile.name)
        .bind(Json(profile.fields.clone()))
        .bind(Json(profile.dashboard_defaults.clone()))
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?
        .rows_affected(),
    };
    if updated == 0 {
        return Err(ManagementAssetProfileError::AssetProfileNotFound);
    }
    Ok(profile)
}

async fn delete_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> Result<(), ManagementAssetProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM asset_profiles WHERE id = ? AND tenant_id = ?",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementAssetProfileError::AssetProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM assets
                    WHERE asset_profile_id = ? AND tenant_id = ?
                 )",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?
                != 0;
            if referenced {
                return Err(ManagementAssetProfileError::AssetProfileInUse(profile_id));
            }
            sqlx::query("DELETE FROM asset_profiles WHERE id = ? AND tenant_id = ?")
                .bind(profile_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("LOCK TABLE assets IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM asset_profiles WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementAssetProfileError::AssetProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM assets
                    WHERE asset_profile_id = $1 AND tenant_id = $2
                 )",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if referenced {
                return Err(ManagementAssetProfileError::AssetProfileInUse(profile_id));
            }
            sqlx::query("DELETE FROM asset_profiles WHERE id = $1 AND tenant_id = $2")
                .bind(profile_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn sqlite_management_asset_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?;
    validate_management_asset_profile(
        id,
        row.try_get("name")?,
        serde_json::from_str(&row.try_get::<String, _>("fields")?)
            .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?,
        serde_json::from_str(&row.try_get::<String, _>("dashboard_defaults")?)
            .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?,
    )
    .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)
}

fn timescale_management_asset_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    validate_management_asset_profile(
        row.try_get("id")?,
        row.try_get("name")?,
        row.try_get::<Json<serde_json::Value>, _>("fields")?.0,
        row.try_get::<Json<serde_json::Value>, _>("dashboard_defaults")?
            .0,
    )
    .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)
}
