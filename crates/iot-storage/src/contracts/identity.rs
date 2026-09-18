use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use iot_core::TelemetryEvent;
use thiserror::Error;

use crate::PlatformStoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum GatewayIngestValidationError {
    #[error("child telemetry requires a child device ID")]
    ChildTelemetryMissingChild,
    #[error("child telemetry requires a telemetry event")]
    ChildTelemetryMissingTelemetry,
    #[error("telemetry is only valid for child telemetry events")]
    TelemetryOnNonChildEvent,
    #[error("telemetry requires a child device ID")]
    TelemetryMissingChild,
    #[error("telemetry device ID does not match the child device ID")]
    TelemetryChildMismatch,
    #[error("telemetry gateway ID does not match the gateway device ID")]
    TelemetryGatewayMismatch,
}

pub trait TopologyRepository: Send + Sync {
    fn register_device<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayIngestEventKind {
    Connect,
    Disconnect,
    Heartbeat,
    ChildTelemetry,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GatewayIngestRequest {
    pub tenant_id: uuid::Uuid,
    pub gateway_device_id: String,
    pub child_device_id: Option<String>,
    pub event_kind: GatewayIngestEventKind,
    pub event_at: DateTime<Utc>,
    pub idempotency_key: String,
    pub telemetry_event: Option<TelemetryEvent>,
    pub topic: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayIngestResult {
    pub receipt_inserted: bool,
    pub telemetry_inserted: bool,
}

pub trait GatewayIngestRepository: Send + Sync {
    fn ingest_gateway<'a>(
        &'a self,
        request: GatewayIngestRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayIngestResult, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedDeviceToken {
    pub token_id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub device_id: String,
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
}

pub trait IdentityRepository: Send + Sync {
    fn resolve_active_device_token<'a>(
        &'a self,
        token: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<AuthenticatedDeviceToken, PlatformStoreError>> + Send + 'a>,
    >;
}

pub trait DeviceAuthorizationRepository: Send + Sync {
    fn authorize_device_session<'a>(
        &'a self,
        token_id: uuid::Uuid,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn authorize_gateway_token<'a>(
        &'a self,
        token_id: uuid::Uuid,
        tenant_id: uuid::Uuid,
        gateway_device_id: &'a str,
        child_device_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
}
