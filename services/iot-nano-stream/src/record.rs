use chrono::{DateTime, Utc};
use iot_core::TelemetryEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Offset, PartitionId, StreamError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub tenant_id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub event: TelemetryEvent,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayEventKind {
    Connect,
    Disconnect,
    Heartbeat,
    ChildTelemetry,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayEvent {
    pub schema_version: u8,
    pub gateway_device_id: String,
    pub child_device_id: Option<String>,
    pub token_id: Uuid,
    pub session_id: Option<String>,
    pub event_kind: GatewayEventKind,
    pub event_at: DateTime<Utc>,
    pub payload: Value,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayMessage {
    pub tenant_id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub gateway_event: GatewayEvent,
    #[serde(default)]
    pub telemetry_event: Option<TelemetryEvent>,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record_type", content = "message", rename_all = "snake_case")]
pub enum StreamMessage {
    Telemetry(TelemetryMessage),
    Gateway(GatewayMessage),
}

impl From<TelemetryMessage> for StreamMessage {
    fn from(message: TelemetryMessage) -> Self {
        Self::Telemetry(message)
    }
}

impl From<GatewayMessage> for StreamMessage {
    fn from(message: GatewayMessage) -> Self {
        Self::Gateway(message)
    }
}

impl StreamMessage {
    pub fn partition_key(&self) -> &str {
        match self {
            Self::Telemetry(message) => &message.event.device_id,
            Self::Gateway(message) => &message.gateway_event.gateway_device_id,
        }
    }

    pub fn received_at(&self) -> DateTime<Utc> {
        match self {
            Self::Telemetry(message) => message.received_at,
            Self::Gateway(message) => message.received_at,
        }
    }

    pub fn tenant_id(&self) -> Uuid {
        match self {
            Self::Telemetry(message) => message.tenant_id,
            Self::Gateway(message) => message.tenant_id,
        }
    }

    pub fn telemetry(&self) -> Option<&TelemetryMessage> {
        match self {
            Self::Telemetry(message) => Some(message),
            Self::Gateway(_) => None,
        }
    }

    pub fn telemetry_parts(&self) -> Option<(&TelemetryEvent, DateTime<Utc>, &str)> {
        match self {
            Self::Telemetry(message) => Some((&message.event, message.received_at, &message.topic)),
            Self::Gateway(message) => message
                .telemetry_event
                .as_ref()
                .map(|event| (event, message.received_at, message.topic.as_str())),
        }
    }

    pub(crate) fn idempotency_key(&self) -> String {
        match self {
            Self::Telemetry(message) => format!(
                "telemetry:{}:{}:{}",
                message.event.device_id, message.event.boot_id, message.event.sequence
            ),
            Self::Gateway(message) => message.gateway_event.idempotency_key.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        match self {
            Self::Telemetry(message) => message.event.validate_for_topic(&message.topic)?,
            Self::Gateway(message) => {
                let event = &message.gateway_event;
                if event.schema_version != 1
                    || event.gateway_device_id.trim().is_empty()
                    || event.idempotency_key.trim().is_empty()
                    || event
                        .child_device_id
                        .as_deref()
                        .is_some_and(|device_id| device_id.trim().is_empty())
                    || !event.payload.is_object()
                {
                    return Err(StreamError::InvalidGatewayEvent);
                }
                let valid_kind_shape = match event.event_kind {
                    GatewayEventKind::Heartbeat => {
                        event.child_device_id.is_none() && message.telemetry_event.is_none()
                    }
                    GatewayEventKind::Connect | GatewayEventKind::Disconnect => {
                        event.child_device_id.is_some() && message.telemetry_event.is_none()
                    }
                    GatewayEventKind::ChildTelemetry => {
                        let Some(child_device_id) = event.child_device_id.as_deref() else {
                            return Err(StreamError::InvalidGatewayEvent);
                        };
                        let Some(telemetry_event) = message.telemetry_event.as_ref() else {
                            return Err(StreamError::InvalidGatewayEvent);
                        };
                        telemetry_event.device_id == child_device_id
                            && telemetry_event.gateway_device_id.as_deref()
                                == Some(event.gateway_device_id.as_str())
                    }
                };
                if !valid_kind_shape {
                    return Err(StreamError::InvalidGatewayEvent);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub partition: PartitionId,
    pub offset: Offset,
    pub message: StreamMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub partition: PartitionId,
    pub offset: Offset,
}

pub(crate) fn decode_message(payload: &str) -> Result<StreamMessage, StreamError> {
    let message = serde_json::from_str::<StreamMessage>(payload)?;
    message.validate()?;
    Ok(message)
}
