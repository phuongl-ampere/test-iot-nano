use chrono::{DateTime, Utc};
use iot_core::{
    DeviceTokenError, device_token_prefix, generate_device_token, hash_device_token,
    verify_device_token,
};
use serde::Serialize;
use sqlx::{PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::token_vault::{TokenVault, TokenVaultError};
use iot_storage::PlatformStore;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DeviceTokenResponse {
    pub id: Uuid,
    pub device_id: String,
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedDeviceToken {
    pub device_id: String,
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum DeviceTokenStoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Token(#[from] DeviceTokenError),
    #[error(transparent)]
    Vault(#[from] TokenVaultError),
    #[error("device token was not found")]
    NotFound,
    #[error("gateway child devices cannot have MQTT tokens")]
    GatewayChild,
    #[error("could not allocate a unique device token")]
    AllocationFailed,
    #[error("platform storage backend is unavailable")]
    PlatformUnavailable,
}

pub async fn provision_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    display_name: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    if let Some(pool) = store.sqlite_pool() {
        return provision_sqlite(pool, vault, display_name).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or(DeviceTokenStoreError::PlatformUnavailable)?;
    provision(pool, vault, display_name).await
}

pub async fn create_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    if let Some(pool) = store.sqlite_pool() {
        return create_sqlite(pool, vault, device_id).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or(DeviceTokenStoreError::PlatformUnavailable)?;
    create(pool, vault, device_id).await
}

pub async fn list(
    pool: &PgPool,
    vault: &TokenVault,
    device_id: &str,
) -> Result<Vec<DeviceTokenResponse>, DeviceTokenStoreError> {
    let rows = sqlx::query(
        "SELECT id, device_id, token_prefix, token_ciphertext, created_at, last_used_at, revoked_at
         FROM device_tokens
         WHERE device_id = $1
         ORDER BY created_at DESC",
    )
    .bind(device_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let token_ciphertext = row.try_get::<Option<String>, _>("token_ciphertext")?;
            let mut response = row_to_response(row)?;
            if response.revoked_at.is_none() {
                response.token = token_ciphertext
                    .as_deref()
                    .map(|ciphertext| vault.decrypt(ciphertext))
                    .transpose()?;
            }
            Ok(response)
        })
        .collect()
}

pub async fn list_sqlite(
    pool: &SqlitePool,
    vault: &TokenVault,
    device_id: &str,
) -> Result<Vec<DeviceTokenResponse>, DeviceTokenStoreError> {
    let rows = sqlx::query(
        "SELECT id, device_id, token_prefix, token_ciphertext, created_at, last_used_at, revoked_at
         FROM device_tokens
         WHERE device_id = ?
         ORDER BY created_at DESC",
    )
    .bind(device_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let token_ciphertext = row.try_get::<Option<String>, _>("token_ciphertext")?;
            let mut response = sqlite_row_to_response(&row)?;
            if response.revoked_at.is_none() {
                response.token = token_ciphertext
                    .as_deref()
                    .map(|ciphertext| vault.decrypt(ciphertext))
                    .transpose()?;
            }
            Ok(response)
        })
        .collect()
}

pub async fn create(
    pool: &PgPool,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    ensure_token_eligible(&mut transaction, device_id).await?;
    sqlx::query(
        "UPDATE device_tokens
         SET revoked_at = now()
         WHERE device_id = $1 AND revoked_at IS NULL",
    )
    .bind(device_id)
    .execute(&mut *transaction)
    .await?;

    let response = insert_token(&mut transaction, vault, device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn create_sqlite(
    pool: &SqlitePool,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    ensure_token_eligible_sqlite(&mut transaction, device_id).await?;
    sqlx::query(
        "UPDATE device_tokens
         SET revoked_at = ?
         WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(device_id)
    .execute(&mut *transaction)
    .await?;
    let response = insert_token_sqlite(&mut transaction, vault, device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn provision(
    pool: &PgPool,
    vault: &TokenVault,
    display_name: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name)
         VALUES ($1, $2)",
    )
    .bind(&device_id)
    .bind(display_name)
    .execute(&mut *transaction)
    .await?;
    let response = insert_token(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn provision_sqlite(
    pool: &SqlitePool,
    vault: &TokenVault,
    display_name: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name)
         VALUES (?, ?)",
    )
    .bind(&device_id)
    .bind(display_name)
    .execute(&mut *transaction)
    .await?;
    let response = insert_token_sqlite(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn provision_owned(
    pool: &PgPool,
    vault: &TokenVault,
    display_name: &str,
    owner_user_id: Uuid,
    asset_id: Option<Uuid>,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO devices (
            device_id, display_name, owner_user_id, asset_id, claimed_at
         ) VALUES ($1, $2, $3, $4, now())",
    )
    .bind(&device_id)
    .bind(display_name)
    .bind(owner_user_id)
    .bind(asset_id)
    .execute(&mut *transaction)
    .await?;
    let response = insert_token(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn provision_owned_sqlite(
    pool: &SqlitePool,
    vault: &TokenVault,
    display_name: &str,
    owner_user_id: Uuid,
    asset_id: Option<Uuid>,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO devices (
            device_id, display_name, owner_user_id, asset_id, claimed_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&device_id)
    .bind(display_name)
    .bind(owner_user_id.to_string())
    .bind(asset_id.map(|id| id.to_string()))
    .bind(Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;
    let response = insert_token_sqlite(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn rotate(
    pool: &PgPool,
    vault: &TokenVault,
    id: Uuid,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = sqlx::query(
        "SELECT device_id
         FROM device_tokens
         WHERE id = $1 AND revoked_at IS NULL
         FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await?
    .map(|row| row.try_get::<String, _>("device_id"))
    .transpose()?
    .ok_or(DeviceTokenStoreError::NotFound)?;

    ensure_token_eligible(&mut transaction, &device_id).await?;
    sqlx::query("UPDATE device_tokens SET revoked_at = now() WHERE id = $1")
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    let response = insert_token(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn rotate_sqlite(
    pool: &SqlitePool,
    vault: &TokenVault,
    id: Uuid,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let mut transaction = pool.begin().await?;
    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM device_tokens
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DeviceTokenStoreError::NotFound)?;
    ensure_token_eligible_sqlite(&mut transaction, &device_id).await?;
    sqlx::query("UPDATE device_tokens SET revoked_at = ? WHERE id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(&mut *transaction)
        .await?;
    let response = insert_token_sqlite(&mut transaction, vault, &device_id).await?;
    transaction.commit().await?;
    Ok(response)
}

pub async fn revoke(pool: &PgPool, id: Uuid) -> Result<(), DeviceTokenStoreError> {
    let result = sqlx::query(
        "UPDATE device_tokens
         SET revoked_at = now()
         WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DeviceTokenStoreError::NotFound);
    }
    Ok(())
}

pub async fn revoke_sqlite(pool: &SqlitePool, id: Uuid) -> Result<(), DeviceTokenStoreError> {
    let result = sqlx::query(
        "UPDATE device_tokens
         SET revoked_at = ?
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        Err(DeviceTokenStoreError::NotFound)
    } else {
        Ok(())
    }
}

pub async fn resolve_active(
    pool: &PgPool,
    token: &str,
) -> Result<AuthenticatedDeviceToken, DeviceTokenStoreError> {
    let prefix = device_token_prefix(token)?;
    let row = sqlx::query(
        "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                devices.is_gateway, devices.gateway_device_id
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = $1
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL",
    )
    .bind(prefix)
    .fetch_optional(pool)
    .await?
    .ok_or(DeviceTokenStoreError::NotFound)?;

    let token_hash = row.try_get::<String, _>("token_hash")?;
    if !verify_device_token(token, &token_hash)? {
        return Err(DeviceTokenStoreError::NotFound);
    }

    let id = row.try_get::<Uuid, _>("id")?;
    sqlx::query(
        "UPDATE device_tokens
         SET last_used_at = now()
         WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(AuthenticatedDeviceToken {
        device_id: row.try_get("device_id")?,
        is_gateway: row.try_get("is_gateway")?,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

pub async fn resolve_active_sqlite(
    pool: &SqlitePool,
    token: &str,
) -> Result<AuthenticatedDeviceToken, DeviceTokenStoreError> {
    let prefix = device_token_prefix(token)?;
    let row = sqlx::query(
        "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                devices.is_gateway, devices.gateway_device_id
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = ?
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL",
    )
    .bind(prefix)
    .fetch_optional(pool)
    .await?
    .ok_or(DeviceTokenStoreError::NotFound)?;
    let token_hash = row.try_get::<String, _>("token_hash")?;
    if !verify_device_token(token, &token_hash)? {
        return Err(DeviceTokenStoreError::NotFound);
    }
    let id = row.try_get::<String, _>("id")?;
    sqlx::query(
        "UPDATE device_tokens
         SET last_used_at = ?
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(id)
    .execute(pool)
    .await?;
    Ok(AuthenticatedDeviceToken {
        device_id: row.try_get("device_id")?,
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

async fn insert_token(
    transaction: &mut Transaction<'_, Postgres>,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    ensure_token_eligible(transaction, device_id).await?;
    for _ in 0..8 {
        let token = generate_device_token();
        let prefix = device_token_prefix(&token)?;
        let hash = hash_device_token(&token)?;
        let ciphertext = vault.encrypt(&token)?;
        let row = sqlx::query(
            "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash, token_ciphertext)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (token_prefix) DO NOTHING
             RETURNING id, device_id, token_prefix, created_at, last_used_at, revoked_at",
        )
        .bind(Uuid::new_v4())
        .bind(device_id)
        .bind(prefix)
        .bind(hash)
        .bind(ciphertext)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(row) = row {
            let mut response = row_to_response(row)?;
            response.token = Some(token);
            return Ok(response);
        }
    }

    Err(DeviceTokenStoreError::AllocationFailed)
}

async fn insert_token_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    ensure_token_eligible_sqlite(transaction, device_id).await?;
    for _ in 0..8 {
        let token = generate_device_token();
        let prefix = device_token_prefix(&token)?;
        let hash = hash_device_token(&token)?;
        let ciphertext = vault.encrypt(&token)?;
        let id = Uuid::new_v4();
        let created_at = Utc::now();
        let result = sqlx::query(
            "INSERT OR IGNORE INTO device_tokens (
                id, device_id, token_prefix, token_hash, token_ciphertext, created_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(device_id)
        .bind(prefix)
        .bind(hash)
        .bind(ciphertext)
        .bind(created_at.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(DeviceTokenResponse {
                id,
                device_id: device_id.to_owned(),
                token_prefix: prefix.to_owned(),
                created_at,
                last_used_at: None,
                revoked_at: None,
                token: Some(token),
            });
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

async fn ensure_token_eligible(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
) -> Result<(), DeviceTokenStoreError> {
    let gateway_device_id = sqlx::query(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|row| row.try_get::<Option<String>, _>("gateway_device_id"))
    .transpose()?
    .ok_or(DeviceTokenStoreError::NotFound)?;
    if gateway_device_id.is_some() {
        return Err(DeviceTokenStoreError::GatewayChild);
    }
    Ok(())
}

async fn ensure_token_eligible_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
) -> Result<(), DeviceTokenStoreError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenStoreError::NotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenStoreError::GatewayChild)
    } else {
        Ok(())
    }
}

fn row_to_response(
    row: sqlx::postgres::PgRow,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    Ok(DeviceTokenResponse {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
        token: None,
    })
}

fn sqlite_row_to_response(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse::<Uuid>()
        .map_err(|_| DeviceTokenStoreError::NotFound)?;
    Ok(DeviceTokenResponse {
        id,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: sqlite_timestamp(row.try_get("created_at")?)?,
        last_used_at: row
            .try_get::<Option<String>, _>("last_used_at")?
            .map(sqlite_timestamp)
            .transpose()?,
        revoked_at: row
            .try_get::<Option<String>, _>("revoked_at")?
            .map(sqlite_timestamp)
            .transpose()?,
        token: None,
    })
}

fn sqlite_timestamp(value: String) -> Result<DateTime<Utc>, DeviceTokenStoreError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| DeviceTokenStoreError::NotFound)
}
