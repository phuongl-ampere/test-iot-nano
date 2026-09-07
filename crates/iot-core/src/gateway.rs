use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::TelemetryEvent;

const SUPPORTED_SCHEMA_VERSION: u16 = 1;

pub const GATEWAY_CONNECT_TOPIC: &str = "v1/gateways/me/connect";
pub const GATEWAY_DISCONNECT_TOPIC: &str = "v1/gateways/me/disconnect";
pub const GATEWAY_TELEMETRY_TOPIC: &str = "v1/gateways/me/telemetry";

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewayTelemetryPayload {
    Heartbeat {
        schema_version: u16,
        boot_id: Uuid,
        sequence: u64,
        event_at: DateTime<Utc>,
    },
    ChildTelemetry {
        schema_version: u16,
        boot_id: Uuid,
        sequence: u64,
        event_at: DateTime<Utc>,
        child_device_id: String,
        measurements: Map<String, Value>,
    },
}

impl GatewayTelemetryPayload {
    pub fn into_event(
        self,
        gateway_device_id: impl Into<String>,
    ) -> Result<Option<TelemetryEvent>, GatewayPayloadError> {
        match self {
            Self::Heartbeat {
                schema_version,
                boot_id: _,
                sequence: _,
                event_at: _,
            } => {
                validate_schema_version(schema_version)?;
                Ok(None)
            }
            Self::ChildTelemetry {
                schema_version,
                boot_id,
                sequence,
                event_at,
                child_device_id,
                measurements,
            } => {
                validate_schema_version(schema_version)?;
                if child_device_id.trim().is_empty() {
                    return Err(GatewayPayloadError::EmptyChildDeviceId);
                }
                if measurements.is_empty() {
                    return Err(GatewayPayloadError::EmptyMeasurements);
                }
                Ok(Some(TelemetryEvent {
                    schema_version,
                    device_id: child_device_id,
                    boot_id,
                    sequence,
                    event_at,
                    measurements,
                    gateway_device_id: Some(gateway_device_id.into()),
                }))
            }
        }
    }

    pub fn event_at(&self) -> DateTime<Utc> {
        match self {
            Self::Heartbeat { event_at, .. } | Self::ChildTelemetry { event_at, .. } => *event_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GatewayChildLifecyclePayload {
    pub schema_version: u16,
    pub boot_id: Uuid,
    pub sequence: u64,
    pub event_at: DateTime<Utc>,
    pub child_device_id: String,
}

impl GatewayChildLifecyclePayload {
    pub fn validate(&self) -> Result<(), GatewayPayloadError> {
        validate_schema_version(self.schema_version)?;
        if self.child_device_id.trim().is_empty() {
            return Err(GatewayPayloadError::EmptyChildDeviceId);
        }
        Ok(())
    }
}

fn validate_schema_version(schema_version: u16) -> Result<(), GatewayPayloadError> {
    if schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(GatewayPayloadError::UnsupportedSchemaVersion(
            schema_version,
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GatewayPayloadError {
    #[error("gateway telemetry schema version {0} is not supported")]
    UnsupportedSchemaVersion(u16),
    #[error("gateway child device ID must not be empty")]
    EmptyChildDeviceId,
    #[error("gateway child telemetry measurements must not be empty")]
    EmptyMeasurements,
}
