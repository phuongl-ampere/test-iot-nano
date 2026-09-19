use std::{future::Future, pin::Pin};

use chrono::Utc;
use iot_nano_foundation::{device_token_prefix, verify_device_token};
use sqlx::Row;

use crate::{
    AuthenticatedDeviceToken, IdentityRepository, PlatformStore, PlatformStoreError,
    TopologyRepository,
};

impl PlatformStore {
    pub async fn register_device(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
    ) -> Result<(), PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                let tenant_status =
                    sqlx::query_scalar::<_, String>("SELECT status FROM tenants WHERE id = ?")
                        .bind(tenant_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?;
                if tenant_status.as_deref() != Some("active") {
                    return Err(PlatformStoreError::UnknownTenant(tenant_id));
                }
                let registered = sqlx::query(
                    "INSERT INTO devices (device_id, tenant_id)
                     VALUES (?, ?)
                     ON CONFLICT(device_id) DO UPDATE SET device_id = excluded.device_id
                     WHERE devices.tenant_id = excluded.tenant_id",
                )
                .bind(device_id)
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
                if registered.rows_affected() != 1 {
                    return Err(PlatformStoreError::DeviceTenantConflict {
                        device_id: device_id.to_owned(),
                        tenant_id,
                    });
                }
                transaction.commit().await?;
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let tenant_status = sqlx::query_scalar::<_, String>(
                    "SELECT status FROM tenants WHERE id = $1 FOR UPDATE",
                )
                .bind(tenant_id)
                .fetch_optional(&mut *transaction)
                .await?;
                if tenant_status.as_deref() != Some("active") {
                    return Err(PlatformStoreError::UnknownTenant(tenant_id));
                }
                let registered = sqlx::query(
                    "INSERT INTO devices (device_id, tenant_id)
                     VALUES ($1, $2)
                     ON CONFLICT(device_id) DO UPDATE SET device_id = EXCLUDED.device_id
                     WHERE devices.tenant_id = EXCLUDED.tenant_id",
                )
                .bind(device_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
                if registered.rows_affected() != 1 {
                    return Err(PlatformStoreError::DeviceTenantConflict {
                        device_id: device_id.to_owned(),
                        tenant_id,
                    });
                }
                transaction.commit().await?;
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
