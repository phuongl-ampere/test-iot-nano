use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Sqlite, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AuditAction, AuditPrincipal, AuditTargetType, PlatformStore, PlatformStoreError, audit,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTokenSecret {
    pub record: DeviceTokenRecord,
    pub token_ciphertext: String,
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
        tenant_id: Uuid,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn provision_owned_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn create_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn list_device_tokens<'a>(
        &'a self,
        tenant_id: Uuid,
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
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn active_device_token_secret<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenSecret>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn rotate_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn revoke_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>>;
}

impl DeviceTokenRepository for PlatformStore {
    fn provision_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_device_token(self, tenant_id, display_name, token).await })
    }

    fn provision_owned_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_owned_device_token(self, tenant_id, actor, device).await })
    }

    fn create_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { create_device_token(self, tenant_id, device_id, token).await })
    }

    fn list_device_tokens<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_device_tokens(self, tenant_id, device_id).await })
    }

    fn active_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { active_device_token(self, tenant_id, token_id).await })
    }

    fn active_device_token_secret<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenSecret>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { active_device_token_secret(self, tenant_id, device_id).await })
    }

    fn rotate_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { rotate_device_token(self, tenant_id, token_id, token).await })
    }

    fn revoke_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>> {
        Box::pin(async move { revoke_device_token(self, tenant_id, token_id).await })
    }
}

async fn provision_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    display_name: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            sqlx::query(
                "INSERT INTO devices (device_id, tenant_id, display_name)
                 VALUES (?, ?, ?)",
            )
            .bind(&device_id)
            .bind(tenant_id.to_string())
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
                "INSERT INTO devices (device_id, tenant_id, display_name)
                 VALUES ($1, $2, $3)",
            )
            .bind(&device_id)
            .bind(tenant_id)
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
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device: NewOwnedDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    let owner_user_id = device.owner_user_id;
    let asset_id = device.asset_id;
    let user_actor_id = match actor {
        AuditPrincipal::User(user_id) => Some(user_id),
        AuditPrincipal::SystemAccount(_) | AuditPrincipal::TenantAccount(_) => None,
    };
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if let Some(user_actor_id) = user_actor_id {
                let user_can_create = user_actor_id == owner_user_id
                    && sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM users
                         WHERE id = ? AND tenant_id = ? AND account_class = 'user'",
                    )
                    .bind(user_actor_id.to_string())
                    .bind(tenant_id.to_string())
                    .fetch_optional(&mut *transaction)
                    .await?
                    .is_some();
                if !user_can_create {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
                if let Some(asset_id) = asset_id {
                    let owns_asset = sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM assets
                         WHERE id = ? AND tenant_id = ? AND owner_user_id = ?",
                    )
                    .bind(asset_id.to_string())
                    .bind(tenant_id.to_string())
                    .bind(user_actor_id.to_string())
                    .fetch_optional(&mut *transaction)
                    .await?
                    .is_some();
                    if !owns_asset {
                        return Err(DeviceTokenRepositoryError::DeviceNotFound);
                    }
                }
            }
            let owner_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = ? AND tenant_id = ?)",
            )
            .bind(owner_user_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            if !owner_exists {
                return Err(DeviceTokenRepositoryError::DeviceNotFound);
            }
            if let Some(asset_id) = asset_id {
                let asset_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?)",
                )
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
                if !asset_exists {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
            }
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&device_id)
            .bind(tenant_id.to_string())
            .bind(&device.display_name)
            .bind(owner_user_id.to_string())
            .bind(asset_id.map(|id| id.to_string()))
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_sqlite_device_token(&mut transaction, &device_id, device.token).await?;
            let ownership_event = audit::NewAuditEvent::new(
                tenant_id,
                actor,
                AuditAction::OwnershipTransferred,
                AuditTargetType::Device,
                device_id.clone(),
                serde_json::json!({
                    "owner_user_id": {
                        "before": null,
                        "after": owner_user_id.to_string(),
                    }
                }),
            );
            audit::insert_sqlite_audit_event(&mut transaction, &ownership_event).await?;
            if let Some(asset_id) = asset_id {
                let containment_event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.clone(),
                    serde_json::json!({
                        "asset_id": {
                            "before": null,
                            "after": asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &containment_event).await?;
            }
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            if let Some(user_actor_id) = user_actor_id {
                let user_can_create = user_actor_id == owner_user_id
                    && sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM users
                         WHERE id = $1 AND tenant_id = $2 AND account_class = 'user'",
                    )
                    .bind(user_actor_id)
                    .bind(tenant_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .is_some();
                if !user_can_create {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
                if let Some(asset_id) = asset_id {
                    let owns_asset = sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM assets
                         WHERE id = $1 AND tenant_id = $2 AND owner_user_id = $3
                         FOR SHARE",
                    )
                    .bind(asset_id)
                    .bind(tenant_id)
                    .bind(user_actor_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .is_some();
                    if !owns_asset {
                        return Err(DeviceTokenRepositoryError::DeviceNotFound);
                    }
                }
            }
            let owner_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2)",
            )
            .bind(owner_user_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if !owner_exists {
                return Err(DeviceTokenRepositoryError::DeviceNotFound);
            }
            if let Some(asset_id) = asset_id {
                let asset_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
                )
                .bind(asset_id)
                .bind(tenant_id)
                .fetch_one(&mut *transaction)
                .await?;
                if !asset_exists {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
            }
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES ($1, $2, $3, $4, $5, now())",
            )
            .bind(&device_id)
            .bind(tenant_id)
            .bind(&device.display_name)
            .bind(owner_user_id)
            .bind(asset_id)
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_timescale_device_token(&mut transaction, &device_id, device.token).await?;
            let ownership_event = audit::NewAuditEvent::new(
                tenant_id,
                actor,
                AuditAction::OwnershipTransferred,
                AuditTargetType::Device,
                device_id.clone(),
                serde_json::json!({
                    "owner_user_id": {
                        "before": null,
                        "after": owner_user_id.to_string(),
                    }
                }),
            );
            audit::insert_timescale_audit_event(&mut transaction, &ownership_event).await?;
            if let Some(asset_id) = asset_id {
                let containment_event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.clone(),
                    serde_json::json!({
                        "asset_id": {
                            "before": null,
                            "after": asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &containment_event).await?;
            }
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn create_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            ensure_sqlite_token_eligible(&mut transaction, tenant_id, device_id).await?;
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
            let record = insert_sqlite_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_timescale_token_eligible(&mut transaction, tenant_id, device_id).await?;
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
            let record = insert_timescale_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn list_device_tokens(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
                 FROM device_tokens dt
                 INNER JOIN devices d ON d.device_id = dt.device_id
                 WHERE dt.device_id = ? AND d.tenant_id = ? AND d.deleted_at IS NULL
                 ORDER BY dt.created_at DESC, dt.id DESC",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_device_token_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
                 FROM device_tokens dt
                 INNER JOIN devices d ON d.device_id = dt.device_id
                 WHERE dt.device_id = $1 AND d.tenant_id = $2 AND d.deleted_at IS NULL
                 ORDER BY dt.created_at DESC, dt.id DESC",
            )
            .bind(device_id)
            .bind(tenant_id)
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
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.id = ? AND dt.revoked_at IS NULL
               AND d.tenant_id = ? AND d.deleted_at IS NULL",
        )
        .bind(token_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_device_token_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.id = $1 AND dt.revoked_at IS NULL
               AND d.tenant_id = $2 AND d.deleted_at IS NULL",
        )
        .bind(token_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_device_token_record)
        .transpose(),
    }
}

async fn active_device_token_secret(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Option<DeviceTokenSecret>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at,
                    dt.revoked_at, dt.token_ciphertext
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.device_id = ? AND dt.revoked_at IS NULL
               AND d.tenant_id = ? AND d.deleted_at IS NULL
               AND d.gateway_device_id IS NULL",
        )
        .bind(device_id)
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_device_token_secret)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at,
                    dt.revoked_at, dt.token_ciphertext
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.device_id = $1 AND dt.revoked_at IS NULL
               AND d.tenant_id = $2 AND d.deleted_at IS NULL
               AND d.gateway_device_id IS NULL",
        )
        .bind(device_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_device_token_secret)
        .transpose(),
    }
}

async fn rotate_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    token_id: Uuid,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            let device_id =
                sqlite_active_token_device_id(&mut transaction, tenant_id, token_id).await?;
            ensure_sqlite_token_eligible(&mut transaction, tenant_id, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = ?
                 WHERE id = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = ?
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(token_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let device_id =
                timescale_active_token_device_id(&mut transaction, tenant_id, token_id).await?;
            ensure_timescale_token_eligible(&mut transaction, tenant_id, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = now()
                 WHERE id = $1 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = $2
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(token_id)
            .bind(tenant_id)
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
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<(), DeviceTokenRepositoryError> {
    let revoked = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = ?
             WHERE id = ? AND revoked_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM devices
                   WHERE devices.device_id = device_tokens.device_id
                     AND devices.tenant_id = ?
                     AND devices.deleted_at IS NULL
               )",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(token_id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = now()
             WHERE id = $1 AND revoked_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM devices
                   WHERE devices.device_id = device_tokens.device_id
                     AND devices.tenant_id = $2
                     AND devices.deleted_at IS NULL
               )",
        )
        .bind(token_id)
        .bind(tenant_id)
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
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT dt.device_id FROM device_tokens dt
         INNER JOIN devices d ON d.device_id = dt.device_id
         WHERE dt.id = ? AND dt.revoked_at IS NULL
           AND d.tenant_id = ? AND d.deleted_at IS NULL",
    )
    .bind(token_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn timescale_active_token_device_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT dt.device_id FROM device_tokens dt
         INNER JOIN devices d ON d.device_id = dt.device_id
         WHERE dt.id = $1 AND dt.revoked_at IS NULL
           AND d.tenant_id = $2 AND d.deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(token_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn ensure_sqlite_token_eligible(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
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
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::DeviceNotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenRepositoryError::GatewayChild)
    } else {
        Ok(())
    }
}

pub(super) async fn insert_sqlite_device_token(
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

pub(super) async fn insert_timescale_device_token(
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

fn sqlite_device_token_secret(
    row: sqlx::sqlite::SqliteRow,
) -> Result<DeviceTokenSecret, DeviceTokenRepositoryError> {
    let token_ciphertext = row
        .try_get::<Option<String>, _>("token_ciphertext")?
        .ok_or(DeviceTokenRepositoryError::TokenNotFound)?;
    Ok(DeviceTokenSecret {
        record: sqlite_device_token_record(row)?,
        token_ciphertext,
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

fn timescale_device_token_secret(
    row: sqlx::postgres::PgRow,
) -> Result<DeviceTokenSecret, DeviceTokenRepositoryError> {
    let token_ciphertext = row
        .try_get::<Option<String>, _>("token_ciphertext")?
        .ok_or(DeviceTokenRepositoryError::TokenNotFound)?;
    Ok(DeviceTokenSecret {
        record: timescale_device_token_record(row)?,
        token_ciphertext,
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
