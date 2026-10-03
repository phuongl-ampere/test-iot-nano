use std::fmt::Write as _;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{DateTime, TimeZone, Utc};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{TelemetryEvent, TelemetryValidationError};

pub const DEVICE_TELEMETRY_TOPIC: &str = "v1/devices/me/telemetry";
pub const DEVICE_TOKEN_PREFIX_LENGTH: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub struct DeviceTelemetryPayload {
    pub event_at: Option<DateTime<Utc>>,
    pub measurements: Map<String, Value>,
    boot_id: Option<Uuid>,
    device_id: Option<String>,
    sequence: Option<u64>,
}

#[derive(Deserialize)]
struct DirectTelemetryWirePayload {
    #[serde(default)]
    boot_id: Option<Uuid>,
    #[serde(default)]
    event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    measurements: Option<Map<String, Value>>,
    #[serde(default)]
    schema_version: Option<u16>,
    #[serde(default)]
    sequence: Option<u64>,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    values: Option<Map<String, Value>>,
    #[serde(default)]
    device_id: Option<String>,
    #[serde(flatten)]
    top_level_measurements: Map<String, Value>,
}

impl<'de> Deserialize<'de> for DeviceTelemetryPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DirectTelemetryWirePayload::deserialize(deserializer)?;
        let measurements = match (wire.measurements, wire.values) {
            (Some(measurements), None) if wire.top_level_measurements.is_empty() => measurements,
            (None, Some(values)) if wire.top_level_measurements.is_empty() => values,
            (None, None) => wire.top_level_measurements,
            _ => {
                return Err(D::Error::custom(
                    "direct telemetry must use one of top-level key-values, values, or measurements",
                ));
            }
        };
        if wire.schema_version.is_some_and(|version| version != 1) {
            return Err(D::Error::custom("schema_version must be 1"));
        }
        let event_at = match (wire.event_at, wire.ts) {
            (Some(_), Some(_)) => {
                return Err(D::Error::custom("direct telemetry must use either event_at or ts"));
            }
            (Some(event_at), None) => Some(event_at),
            (None, Some(timestamp)) => Some(
                Utc.timestamp_millis_opt(timestamp)
                    .single()
                    .ok_or_else(|| D::Error::custom("ts must be a valid Unix timestamp in milliseconds"))?,
            ),
            (None, None) => None,
        };

        Ok(Self {
            event_at,
            measurements,
            boot_id: wire.boot_id,
            device_id: wire.device_id,
            sequence: wire.sequence,
        })
    }
}

impl DeviceTelemetryPayload {
    pub fn into_event(
        self,
        device_id: impl Into<String>,
        received_at: DateTime<Utc>,
    ) -> Result<TelemetryEvent, TelemetryValidationError> {
        if self.device_id.is_some() {
            return Err(TelemetryValidationError::DeviceIdNotAllowed);
        }

        Ok(TelemetryEvent {
            schema_version: 1,
            device_id: device_id.into(),
            boot_id: self.boot_id.unwrap_or_else(Uuid::new_v4),
            sequence: self.sequence.unwrap_or(0),
            event_at: self.event_at.unwrap_or(received_at),
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
