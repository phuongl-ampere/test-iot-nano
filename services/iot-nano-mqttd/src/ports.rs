use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Utc};
use iot_nano_foundation::{DeviceTelemetryPayload, GatewayTelemetryPayload};
use iot_nano_stream::{
    GatewayEvent, GatewayEventKind, GatewayMessage, StreamPort, TelemetryMessage,
};
use thiserror::Error;
use uuid::Uuid;

use crate::transport::{
    AuthenticatedDevice, DIRECT_TELEMETRY_TOPIC, DeviceAuthenticator, GATEWAY_TOPICS,
    RpcResponseForwarder, TransportAuthRequest, TransportError, TransportRpcResponse,
    TransportUplink, UplinkForwarder,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthorizationError {
    #[error("authorization denied")]
    Denied,
    #[error("authorization service unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommandResponseError {
    #[error("command response service unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CacheError {
    #[error("cache key cannot be empty")]
    EmptyKey,
    #[error("cache expiration timestamp is out of range")]
    InvalidExpiration,
    #[error("cache service unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub expires_at_ms: u64,
}

pub trait CachePort: Send + Sync {
    fn get(
        &self,
        key: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, CacheError>> + Send + '_>>;

    fn put(
        &self,
        entry: CacheEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), CacheError>> + Send + '_>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuthorizationRequest {
    pub tenant_id: Uuid,
    pub gateway_device_id: String,
    pub token_id: Uuid,
    pub child_device_id: Option<String>,
    pub topic: String,
    pub event_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GatewayAuthorization {
    pub tenant_id: Uuid,
    pub gateway_device_id: String,
    pub token_id: Uuid,
    pub child_device_id: Option<String>,
    pub topic: String,
    pub event_kind: String,
}

impl GatewayAuthorization {
    pub(crate) fn matches(
        &self,
        device: &AuthenticatedDevice,
        topic: &str,
        event_kind: &str,
        child_device_id: Option<&str>,
    ) -> bool {
        self.tenant_id == device.tenant_id
            && self.gateway_device_id == device.device_id
            && self.token_id == device.token_id
            && self.topic == topic
            && self.event_kind == event_kind
            && self.child_device_id.as_deref() == child_device_id
    }
}

pub trait DeviceAuthorizationPort: Send + Sync {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>;

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>>;

    fn authorize_gateway_uplink(
        &self,
        request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>;
}

pub trait CommandResponsePort: Send + Sync {
    fn record_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>>;
}

#[derive(Clone)]
pub struct LocalDeviceAuthenticator {
    authorization: Arc<dyn DeviceAuthorizationPort>,
}

impl LocalDeviceAuthenticator {
    pub fn new(authorization: Arc<dyn DeviceAuthorizationPort>) -> Self {
        Self { authorization }
    }
}

impl DeviceAuthenticator for LocalDeviceAuthenticator {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        let authorization = Arc::clone(&self.authorization);
        Box::pin(async move {
            authorization
                .authenticate(request)
                .await
                .map_err(authorization_error)
        })
    }

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let authorization = Arc::clone(&self.authorization);
        Box::pin(async move {
            authorization
                .authorize_session(device)
                .await
                .map_err(authorization_error)
        })
    }
}

#[derive(Clone)]
pub struct LocalStreamUplinkForwarder {
    authorization: Arc<dyn DeviceAuthorizationPort>,
    stream: Arc<dyn StreamPort>,
}

pub type LocalUplinkForwarder = LocalStreamUplinkForwarder;

impl LocalStreamUplinkForwarder {
    pub fn new(
        authorization: Arc<dyn DeviceAuthorizationPort>,
        stream: Arc<dyn StreamPort>,
    ) -> Self {
        Self {
            authorization,
            stream,
        }
    }
}

impl UplinkForwarder for LocalStreamUplinkForwarder {
    fn forward(
        &self,
        _token: &str,
        message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let authorization = Arc::clone(&self.authorization);
        let stream = Arc::clone(&self.stream);
        Box::pin(async move {
            if message.topic == DIRECT_TELEMETRY_TOPIC && !message.device.is_gateway {
                let payload: DeviceTelemetryPayload = serde_json::from_slice(&message.payload)
                    .map_err(|_| TransportError::InvalidUplinkPayload)?;
                let event = payload
                    .into_event(message.device.device_id.clone())
                    .map_err(|_| TransportError::InvalidUplinkPayload)?;
                stream
                    .append(
                        TelemetryMessage {
                            tenant_id: message.device.tenant_id,
                            topic: format!("iot/v1/devices/{}/telemetry", event.device_id),
                            payload: message.payload,
                            event,
                            received_at: message.received_at,
                        }
                        .into(),
                    )
                    .await
                    .map_err(stream_error)?;
                return Ok(());
            }

            if !message.device.is_gateway || !GATEWAY_TOPICS.contains(&message.topic.as_str()) {
                return Err(TransportError::ForbiddenTopic);
            }

            let payload: serde_json::Value = serde_json::from_slice(&message.payload)
                .map_err(|_| TransportError::InvalidUplinkPayload)?;
            let event_kind = payload["kind"]
                .as_str()
                .ok_or(TransportError::InvalidUplinkPayload)?;
            let child_device_id = payload["child_device_id"].as_str();
            let telemetry_event = if event_kind == "child_telemetry" {
                serde_json::from_slice::<GatewayTelemetryPayload>(&message.payload)
                    .map_err(|_| TransportError::InvalidUplinkPayload)?
                    .into_event(message.device.device_id.clone())
                    .map_err(|_| TransportError::InvalidUplinkPayload)?
            } else {
                None
            };
            let authorization = authorization
                .authorize_gateway_uplink(GatewayAuthorizationRequest {
                    tenant_id: message.device.tenant_id,
                    gateway_device_id: message.device.device_id.clone(),
                    token_id: message.device.token_id,
                    child_device_id: child_device_id.map(str::to_owned),
                    topic: message.topic.clone(),
                    event_kind: event_kind.to_owned(),
                })
                .await
                .map_err(authorization_error)?;
            if !authorization.matches(&message.device, &message.topic, event_kind, child_device_id)
            {
                return Err(TransportError::Unauthorized);
            }
            let event_at = serde_json::from_value::<DateTime<Utc>>(payload["event_at"].clone())
                .map_err(|_| TransportError::InvalidUplinkPayload)?;
            let event_kind = gateway_event_kind(event_kind)?;
            let idempotency_key = format!(
                "{}:{}:{}",
                message.device.device_id, payload["boot_id"], payload["sequence"]
            );
            stream
                .append(
                    GatewayMessage {
                        tenant_id: authorization.tenant_id,
                        topic: format!(
                            "iot/v1/gateways/{}/events",
                            authorization.gateway_device_id
                        ),
                        payload: message.payload,
                        gateway_event: GatewayEvent {
                            schema_version: 1,
                            gateway_device_id: authorization.gateway_device_id,
                            child_device_id: authorization.child_device_id,
                            token_id: authorization.token_id,
                            session_id: None,
                            event_kind,
                            event_at,
                            payload,
                            idempotency_key,
                        },
                        telemetry_event,
                        received_at: message.received_at,
                    }
                    .into(),
                )
                .await
                .map_err(stream_error)?;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct LocalRpcResponseForwarder {
    responses: Arc<dyn CommandResponsePort>,
}

impl LocalRpcResponseForwarder {
    pub fn new(responses: Arc<dyn CommandResponsePort>) -> Self {
        Self { responses }
    }
}

impl RpcResponseForwarder for LocalRpcResponseForwarder {
    fn forward_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let responses = Arc::clone(&self.responses);
        Box::pin(async move {
            responses
                .record_response(response)
                .await
                .map_err(command_response_error)
        })
    }
}

fn authorization_error(error: AuthorizationError) -> TransportError {
    match error {
        AuthorizationError::Denied => TransportError::Unauthorized,
        AuthorizationError::Unavailable(reason) => TransportError::AuthorizationUnavailable(reason),
    }
}

fn command_response_error(error: CommandResponseError) -> TransportError {
    match error {
        CommandResponseError::Unavailable(reason) => {
            TransportError::CommandResponseUnavailable(reason)
        }
    }
}

fn stream_error(error: iot_nano_stream::StreamError) -> TransportError {
    TransportError::StreamAppendFailed(error.to_string())
}

fn gateway_event_kind(value: &str) -> Result<GatewayEventKind, TransportError> {
    match value {
        "connect" => Ok(GatewayEventKind::Connect),
        "disconnect" => Ok(GatewayEventKind::Disconnect),
        "heartbeat" => Ok(GatewayEventKind::Heartbeat),
        "child_telemetry" => Ok(GatewayEventKind::ChildTelemetry),
        _ => Err(TransportError::InvalidUplinkPayload),
    }
}
