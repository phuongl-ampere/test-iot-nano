use std::fmt::Write as _;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{DateTime, Utc};
use rand_core::{OsRng, RngCore};
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{TelemetryEvent, TelemetryValidationError};

pub const DEVICE_TELEMETRY_TOPIC: &str = "v1/devices/me/telemetry";
pub const DEVICE_TOKEN_PREFIX_LENGTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeviceTelemetryPayload {
    pub schema_version: u16,
    #[serde(default)]
    pub device_id: Option<String>,
    pub boot_id: Uuid,
    pub sequence: u64,
    pub event_at: DateTime<Utc>,
    pub measurements: Map<String, Value>,
}

impl DeviceTelemetryPayload {
    pub fn into_event(
        self,
        device_id: impl Into<String>,
    ) -> Result<TelemetryEvent, TelemetryValidationError> {
        if self.device_id.is_some() {
            return Err(TelemetryValidationError::DeviceIdNotAllowed);
        }

        Ok(TelemetryEvent {
            schema_version: self.schema_version,
            device_id: device_id.into(),
            boot_id: self.boot_id,
            sequence: self.sequence,
            event_at: self.event_at,
            measurements: self.measurements,
            gateway_device_id: None,
        })
    }
}

#[derive(Debug, Error)]
pub enum DeviceTokenError {
    #[error("device token has an invalid format")]
    InvalidFormat,
    #[error("failed to hash device token")]
    Hashing,
    #[error("stored device token hash is invalid")]
    InvalidStoredHash,
}

pub fn generate_device_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);

    let mut token = String::from("iotd_");
    for byte in bytes {
        write!(&mut token, "{byte:02x}").expect("writing to a String cannot fail");
    }
    token
}

pub fn device_token_prefix(token: &str) -> Result<&str, DeviceTokenError> {
    validate_device_token(token)?;
    Ok(&token[..DEVICE_TOKEN_PREFIX_LENGTH])
}

pub fn hash_device_token(token: &str) -> Result<String, DeviceTokenError> {
    validate_device_token(token)?;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(token.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| DeviceTokenError::Hashing)
}

pub fn verify_device_token(token: &str, stored_hash: &str) -> Result<bool, DeviceTokenError> {
    validate_device_token(token)?;
    let stored_hash =
        PasswordHash::new(stored_hash).map_err(|_| DeviceTokenError::InvalidStoredHash)?;
    Ok(Argon2::default()
        .verify_password(token.as_bytes(), &stored_hash)
        .is_ok())
}

fn validate_device_token(token: &str) -> Result<(), DeviceTokenError> {
    let Some(value) = token.strip_prefix("iotd_") else {
        return Err(DeviceTokenError::InvalidFormat);
    };
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DeviceTokenError::InvalidFormat);
    }
    Ok(())
}
