use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RpcTarget {
    DirectDevice {
        device_id: String,
    },
    GatewayChild {
        gateway_device_id: String,
        child_device_id: String,
    },
}

impl RpcTarget {
    pub fn direct(device_id: impl Into<String>) -> Self {
        Self::DirectDevice {
            device_id: device_id.into(),
        }
    }

    pub fn gateway_child(
        gateway_device_id: impl Into<String>,
        child_device_id: impl Into<String>,
    ) -> Self {
        Self::GatewayChild {
            gateway_device_id: gateway_device_id.into(),
            child_device_id: child_device_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub id: Uuid,
    pub method: String,
    pub params: Value,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub mode: RpcMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcMode {
    #[default]
    OneWay,
    TwoWay,
}

impl RpcRequest {
    pub fn new(
        id: Uuid,
        method: impl Into<String>,
        params: Value,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, RpcRequestValidationError> {
        Self::with_mode(id, method, params, issued_at, expires_at, RpcMode::OneWay)
    }

    pub fn with_mode(
        id: Uuid,
        method: impl Into<String>,
        params: Value,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        mode: RpcMode,
    ) -> Result<Self, RpcRequestValidationError> {
        let request = Self {
            id,
            method: method.into(),
            params,
            issued_at,
            expires_at,
            mode,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), RpcRequestValidationError> {
        if self.id.get_version_num() != 7 {
            return Err(RpcRequestValidationError::IdMustBeUuidV7);
        }
        if !is_identifier(&self.method) {
            return Err(RpcRequestValidationError::InvalidMethod);
        }
        if !self.params.is_object() {
            return Err(RpcRequestValidationError::ParamsMustBeObject);
        }
        if self.expires_at <= self.issued_at {
            return Err(RpcRequestValidationError::ExpirationNotAfterIssuedAt);
        }
        Ok(())
    }
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|character| {
            character.is_ascii_alphanumeric() || character == b'-' || character == b'_'
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RpcRequestValidationError {
    #[error("RPC request ID must be UUIDv7")]
    IdMustBeUuidV7,
    #[error("RPC method must be 1 to 64 ASCII alphanumeric, underscore, or hyphen characters")]
    InvalidMethod,
    #[error("RPC params must be a JSON object")]
    ParamsMustBeObject,
    #[error("RPC expiration must be after issue time")]
    ExpirationNotAfterIssuedAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Queued,
    PublishedToBroker,
    Responded,
    Expired,
    Failed,
}
