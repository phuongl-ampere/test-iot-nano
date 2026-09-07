use chrono::{DateTime, Utc};
use iot_core::TelemetryEvent;
use serde::{Deserialize, Serialize};

use crate::{Offset, PartitionId, StreamError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub event: TelemetryEvent,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub partition: PartitionId,
    pub offset: Offset,
    pub message: TelemetryMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedRecord {
    pub partition: PartitionId,
    pub offset: Offset,
}

pub(crate) fn encode_message(message: &TelemetryMessage) -> Result<Vec<u8>, StreamError> {
    Ok(serde_json::to_vec(message)?)
}

pub(crate) fn decode_message(payload: &[u8]) -> Result<TelemetryMessage, StreamError> {
    let message = serde_json::from_slice::<TelemetryMessage>(payload)?;
    message.event.validate_for_topic(&message.topic)?;
    Ok(message)
}
