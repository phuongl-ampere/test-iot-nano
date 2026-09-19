use chrono::{DateTime, Utc};
use iot_nano_foundation::{
    DeviceTokenError, device_token_prefix, generate_device_token, hash_device_token,
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::token_vault::{TokenVault, TokenVaultError};
use iot_storage::{
    DeviceTokenRecord, DeviceTokenRepository, DeviceTokenRepositoryError, NewDeviceToken,
    PlatformStore, ProvisionManagementDevice, ProvisionManagementDeviceError,
};

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Error)]
pub enum DeviceTokenStoreError {
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
    #[error("device token storage operation failed")]
    Storage(#[source] DeviceTokenRepositoryError),
    #[error("management device provisioning failed")]
    ManagementProvision(#[source] ProvisionManagementDeviceError),
}

pub async fn provision_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    tenant_id: Uuid,
    display_name: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::provision_device_token(
            store,
            tenant_id,
            display_name,
            material,
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

pub async fn provision_management_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    tenant_id: Uuid,
    display_name: &str,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    attributes: serde_json::Value,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match store
            .provision_management_device_token(
                tenant_id,
                ProvisionManagementDevice {
                    display_name: display_name.to_owned(),
                    asset_id,
                    device_profile_id,
                    attributes: attributes.clone(),
                    token: material,
                },
            )
            .await
        {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(ProvisionManagementDeviceError::Token(
                DeviceTokenRepositoryError::TokenPrefixConflict,
            )) => continue,
            Err(error) => return Err(DeviceTokenStoreError::ManagementProvision(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn create_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::create_device_token(store, tenant_id, device_id, material)
            .await
        {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
}

pub async fn rotate_platform_device_token(
    store: &PlatformStore,
    vault: &TokenVault,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<DeviceTokenResponse, DeviceTokenStoreError> {
    for _ in 0..8 {
        let (token, material) = new_platform_token(vault)?;
        match DeviceTokenRepository::rotate_device_token(store, tenant_id, token_id, material).await
        {
            Ok(record) => return Ok(platform_token_response(record, token)),
            Err(DeviceTokenRepositoryError::TokenPrefixConflict) => continue,
            Err(error) => return Err(device_token_repository_error(error)),
        }
    }
    Err(DeviceTokenStoreError::AllocationFailed)
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

fn device_token_repository_error(error: DeviceTokenRepositoryError) -> DeviceTokenStoreError {
    match error {
        DeviceTokenRepositoryError::DeviceNotFound | DeviceTokenRepositoryError::TokenNotFound => {
            DeviceTokenStoreError::NotFound
        }
        DeviceTokenRepositoryError::GatewayChild => DeviceTokenStoreError::GatewayChild,
        other => DeviceTokenStoreError::Storage(other),
    }
}
