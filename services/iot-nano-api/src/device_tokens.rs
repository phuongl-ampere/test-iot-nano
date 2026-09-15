use chrono::{DateTime, Utc};
use iot_core::{DeviceTokenError, device_token_prefix, generate_device_token, hash_device_token};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::token_vault::{TokenVault, TokenVaultError};
use iot_storage::{
    DeviceTokenRecord, DeviceTokenRepository, DeviceTokenRepositoryError, IdentityRepository,
    NewDeviceToken, NewOwnedDeviceToken, PlatformStore, PlatformStoreError,
};

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
    pub token_id: Uuid,
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
    #[error("device token storage operation failed")]
    Storage(#[source] DeviceTokenRepositoryError),
    #[error("platform token operation failed")]
    Platform(#[source] PlatformStoreError),
}

pub async fn provision_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    display_name: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::provision_device_token(store, display_name, material).await {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn create_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::create_device_token(store, device_id, material).await {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn provision_owned_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    display_name: &str,
    owner_user_id: Uuid,
    asset_id: Option<Uuid>,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::provision_owned_device_token(
            store,
            NewOwnedDeviceToken {
                display_name: display_name.to_owned(),
                owner_user_id,
                asset_id,
                token: material,
            },
        )
        .await
        {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn list_platform_device_tokens(
    store: &PlatformStore,
    device_id: &str,
) -> Result<Vec<DeviceTokenResponse>, DeviceTokenStoreError> {
    DeviceTokenRepository::list_device_tokens(store, device_id)
        .await
        .map(|records| {
            records
                .into_iter()
                .map(platform_token_history_response)
                .collect()
        })
        .map_err(device_token_repository_error)
}

pub async fn active_platform_device_token(
    store: &PlatformStore,
    token_id: Uuid,
) -> Result<Option<DeviceTokenRecord>, DeviceTokenStoreError> {
    DeviceTokenRepository::active_device_token(store, token_id)
        .await
        .map_err(device_token_repository_error)
}

pub async fn rotate_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    token_id: Uuid,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::rotate_device_token(store, token_id, material).await {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn revoke_platform_device_token(
    store: &PlatformStore,
    token_id: Uuid,
) -> Result<(), DeviceTokenStoreError> {
    DeviceTokenRepository::revoke_device_token(store, token_id)
        .await
        .map_err(device_token_repository_error)
}

pub async fn resolve_platform_active_device_token(
    store: &PlatformStore,
    token: &str,
) -> Result<AuthenticatedDeviceToken, DeviceTokenStoreError> {
    IdentityRepository::resolve_active_device_token(store, token)
        .await
        .map(|token| AuthenticatedDeviceToken {
            token_id: token.token_id,
            device_id: token.device_id,
            is_gateway: token.is_gateway,
            gateway_device_id: token.gateway_device_id,
        })
        .map_err(platform_token_error)
}

fn new_platform_token(
    vault: &TokenVault,
) -> Result<(String, NewDeviceToken), DeviceTokenStoreError> {
    let token = generate_device_token();
    let token_prefix = device_token_prefix(&token)?.to_owned();
    let token_hash = hash_device_token(&token)?;
    let token_ciphertext = vault.encrypt(&token)?;
    Ok((
        token,
        NewDeviceToken {
            id: Uuid::new_v4(),
            token_prefix,
            token_hash,
            token_ciphertext,
        },
    ))
}

fn platform_token_response(record: DeviceTokenRecord, token: String) -> DeviceTokenResponse {
    DeviceTokenResponse {
        id: record.id,
        device_id: record.device_id,
        token_prefix: record.token_prefix,
        created_at: record.created_at,
        last_used_at: record.last_used_at,
        revoked_at: record.revoked_at,
        token: Some(token),
    }
}

fn platform_token_history_response(record: DeviceTokenRecord) -> DeviceTokenResponse {
    DeviceTokenResponse {
        id: record.id,
        device_id: record.device_id,
        token_prefix: record.token_prefix,
        created_at: record.created_at,
        last_used_at: record.last_used_at,
        revoked_at: record.revoked_at,
        token: None,
    }
}

fn device_token_repository_error(error: DeviceTokenRepositoryError) -> DeviceTokenStoreError {
    match error {
        DeviceTokenRepositoryError::DeviceNotFound | DeviceTokenRepositoryError::TokenNotFound => {
            DeviceTokenStoreError::NotFound
        }
        DeviceTokenRepositoryError::GatewayChild => DeviceTokenStoreError::GatewayChild,
        other => DeviceTokenStoreError::Storage(other),
    }
}

fn platform_token_error(error: PlatformStoreError) -> DeviceTokenStoreError {
    if matches!(error, PlatformStoreError::DeviceTokenDenied) {
        DeviceTokenStoreError::NotFound
    } else {
        DeviceTokenStoreError::Platform(error)
    }
}
