use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{DEVICE_TELEMETRY_TOPIC, GATEWAY_TELEMETRY_TOPIC};

const SUPPORTED_SCHEMA_VERSION: u16 = 1;
pub const TELEMETRY_TOPIC_PREFIX: &str = "iot/v1/devices/";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub schema_version: u16,
    pub device_id: String,
    pub boot_id: Uuid,
    pub sequence: u64,
    pub event_at: DateTime<Utc>,
    pub measurements: Map<String, Value>,
    #[serde(default)]
    pub gateway_device_id: Option<String>,
}

impl TelemetryEvent {
    pub fn validate_for_topic(&self, topic: &str) -> Result<(), TelemetryValidationError> {
        if self.schema_version != SUPPORTED_SCHEMA_VERSION {
            return Err(TelemetryValidationError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }

        if self.device_id.trim().is_empty() {
            return Err(TelemetryValidationError::EmptyDeviceId);
        }

        if self.measurements.is_empty() {
            return Err(TelemetryValidationError::EmptyMeasurements);
        }

        if topic == DEVICE_TELEMETRY_TOPIC {
            return Ok(());
        }
        if topic == GATEWAY_TELEMETRY_TOPIC {
            return self
                .gateway_device_id
                .as_deref()
                .filter(|gateway_device_id| !gateway_device_id.trim().is_empty())
                .map(|_| ())
                .ok_or(TelemetryValidationError::GatewayProvenanceRequired);
        }

        let topic_device_id = telemetry_topic_device_id(topic)?;

        if topic_device_id != self.device_id {
            return Err(TelemetryValidationError::TopicDeviceMismatch {
                topic_device_id: topic_device_id.to_owned(),
                payload_device_id: self.device_id.clone(),
            });
        }

        Ok(())
    }
}

fn telemetry_topic_device_id(topic: &str) -> Result<&str, TelemetryValidationError> {
    let mut segments = topic.split('/');
    let expected = [
        Some("iot"),
        Some("v1"),
        Some("devices"),
        None,
        Some("telemetry"),
    ];

    for expected_segment in expected {
        let actual = segments.next();
        match (expected_segment, actual) {
            (Some(expected), Some(actual)) if expected == actual => {}
            (None, Some(actual)) if !actual.trim().is_empty() => {}
            _ => {
                return Err(TelemetryValidationError::InvalidTelemetryTopic(
                    topic.to_owned(),
                ));
            }
        }
    }

    if segments.next().is_some() {
        return Err(TelemetryValidationError::InvalidTelemetryTopic(
            topic.to_owned(),
        ));
    }

    topic
        .split('/')
        .nth(3)
        .ok_or_else(|| TelemetryValidationError::InvalidTelemetryTopic(topic.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TelemetryValidationError {
    #[error("telemetry schema version {0} is not supported")]
    UnsupportedSchemaVersion(u16),
    #[error("device_id must not be empty")]
    EmptyDeviceId,
    #[error("device_id must not be supplied in token-authenticated telemetry")]
    DeviceIdNotAllowed,
    #[error("gateway telemetry requires gateway provenance")]
    GatewayProvenanceRequired,
    #[error("measurements must not be empty")]
    EmptyMeasurements,
    #[error("topic {0:?} is not a telemetry topic")]
    InvalidTelemetryTopic(String),
    #[error(
        "topic device ID {topic_device_id:?} does not match payload device ID {payload_device_id:?}"
    )]
    TopicDeviceMismatch {
        topic_device_id: String,
        payload_device_id: String,
    },
}
