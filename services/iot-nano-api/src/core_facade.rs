use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use iot_core::RpcMode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct CoreCommandCreateRequest {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    pub method: String,
    pub params: serde_json::Value,
    pub mode: RpcMode,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoreCommandResponseRequest {
    pub command_id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    #[serde(skip_serializing)]
    pub token_id: Uuid,
    pub response: serde_json::Value,
    pub responded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreCommandRecord {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    pub state: String,
    pub expires_at: DateTime<Utc>,
    pub mode: RpcMode,
    pub response: Option<serde_json::Value>,
    pub responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreTelemetryBucket {
    Raw,
    FiveMinutes,
    OneHour,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreTelemetryPoint {
    pub at: DateTime<Utc>,
    pub temperature_c: Option<f64>,
    pub humidity_pct: Option<f64>,
    pub event_count: i64,
}

#[derive(Debug, Clone)]
pub struct CoreTelemetryQuery {
    pub device_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket: CoreTelemetryBucket,
}

#[derive(Debug, Error)]
pub enum CoreFacadeError {
    #[error("core command was not found")]
    NotFound,
    #[error("core command service returned status {0}")]
    Rejected(u16),
    #[error("core command service is unavailable")]
    Unavailable,
}

pub trait CoreFacade: Send + Sync {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>>;

    fn get_command(
        &self,
        tenant_id: Uuid,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>>;

    fn record_command_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>>;

    fn telemetry(
        &self,
        query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>;
}
