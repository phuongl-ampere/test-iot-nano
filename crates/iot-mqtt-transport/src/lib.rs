#![forbid(unsafe_code)]

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use iot_core::{RpcMode, RpcRequest};
use rumqttc::{
    ConnectReturnCode, Packet, PubAck, Publish, QoS, SubscribeReasonCode,
    mqttbytes::v4::{Codec, ConnAck, SubAck},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;
use uuid::Uuid;

const MAX_PACKET_BYTES: usize = 1024 * 1024;
const SESSION_COMMAND_CAPACITY: usize = 64;
const COMMAND_PUBACK_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECT_RPC_FILTER: &str = "v1/devices/me/rpc/request/+";
const GATEWAY_RPC_FILTER: &str = "v1/gateways/me/rpc/request/+";
const DIRECT_RPC_RESPONSE_PREFIX: &str = "v1/devices/me/rpc/response/";
const GATEWAY_RPC_RESPONSE_PREFIX: &str = "v1/gateways/me/rpc/response/";
const DIRECT_TELEMETRY_TOPIC: &str = "v1/devices/me/telemetry";
const GATEWAY_TOPICS: [&str; 3] = [
    "v1/gateways/me/connect",
    "v1/gateways/me/disconnect",
    "v1/gateways/me/telemetry",
];

#[derive(Debug, Clone)]
pub struct SessionRegistration {
    pub token_id: Uuid,
    pub device_id: String,
    pub client_id: String,
    pub connection_id: String,
    pub is_gateway: bool,
    pub connected_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ActiveDeviceSession {
    pub request: RpcRequest,
    published: Option<oneshot::Sender<()>>,
}

impl ActiveDeviceSession {
    pub fn acknowledge_published(mut self) -> Result<(), SessionError> {
        self.published
            .take()
            .ok_or(SessionError::AcknowledgementAlreadyConsumed)?
            .send(())
            .map_err(|_| SessionError::PublicationWaiterUnavailable)
    }

    pub(crate) fn take_published_acknowledgement(&mut self) -> Option<oneshot::Sender<()>> {
        self.published.take()
    }
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("device is not connected")]
    DeviceOffline,
    #[error("active device session cannot receive commands")]
    SessionUnavailable,
    #[error("command publication was not acknowledged before the timeout")]
    PublicationTimeout,
    #[error("command publication waiter is unavailable")]
    PublicationWaiterUnavailable,
    #[error("command publication acknowledgement was already consumed")]
    AcknowledgementAlreadyConsumed,
}

#[derive(Debug, Clone, Default)]
pub struct RpcSessionRouter {
    sessions: Arc<Mutex<HashMap<String, RegisteredSession>>>,
    pending_responses: Arc<Mutex<HashMap<Uuid, PendingRpcResponse>>>,
}

#[derive(Debug)]
struct RegisteredSession {
    registration: SessionRegistration,
    sender: mpsc::Sender<ActiveDeviceSession>,
}

#[derive(Debug, Clone)]
struct PendingRpcResponse {
    command_id: Uuid,
    device_id: String,
    token_id: Uuid,
    connection_id: String,
    is_gateway: bool,
    expires_at: DateTime<Utc>,
}

impl PendingRpcResponse {
    fn matches(&self, device: &AuthenticatedDevice, connection_id: &str, command_id: Uuid) -> bool {
        self.command_id == command_id
            && self.device_id == device.device_id
            && self.token_id == device.token_id
            && self.connection_id == connection_id
            && self.is_gateway == device.is_gateway
    }
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    registration: SessionRegistration,
    sender: mpsc::Sender<ActiveDeviceSession>,
}

impl SessionSnapshot {
    fn authenticated_device(&self) -> AuthenticatedDevice {
        AuthenticatedDevice {
            token_id: self.registration.token_id,
            device_id: self.registration.device_id.clone(),
            is_gateway: self.registration.is_gateway,
        }
    }

    fn matches(&self, session: &RegisteredSession) -> bool {
        self.registration.token_id == session.registration.token_id
            && self.registration.connection_id == session.registration.connection_id
            && self.sender.same_channel(&session.sender)
    }
}

impl RpcSessionRouter {
    pub async fn register(
        &self,
        registration: SessionRegistration,
    ) -> mpsc::Receiver<ActiveDeviceSession> {
        let (sender, receiver) = mpsc::channel(SESSION_COMMAND_CAPACITY);
        self.sessions.lock().await.insert(
            registration.device_id.clone(),
            RegisteredSession {
                registration: registration.clone(),
                sender,
            },
        );
        self.pending_responses.lock().await.retain(|_, pending| {
            pending.expires_at > Utc::now() && pending.device_id != registration.device_id
        });
        receiver
    }

    pub async fn unregister(&self, device_id: &str, connection_id: &str) {
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(device_id)
            .is_some_and(|session| session.registration.connection_id == connection_id)
        {
            sessions.remove(device_id);
            self.pending_responses.lock().await.retain(|_, pending| {
                pending.device_id != device_id || pending.connection_id != connection_id
            });
        }
    }

    pub async fn revoke_session(&self, device_id: &str, token_id: Uuid) -> bool {
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(device_id)
            .is_some_and(|session| session.registration.token_id == token_id)
        {
            sessions.remove(device_id);
            self.pending_responses.lock().await.retain(|_, pending| {
                pending.device_id != device_id || pending.token_id != token_id
            });
            true
        } else {
            false
        }
    }

    pub async fn active_device(&self, device_id: &str) -> Option<AuthenticatedDevice> {
        self.active_snapshot(device_id)
            .await
            .map(|snapshot| snapshot.authenticated_device())
    }

    pub async fn active_snapshot(&self, device_id: &str) -> Option<SessionSnapshot> {
        self.sessions
            .lock()
            .await
            .get(device_id)
            .map(|session| SessionSnapshot {
                registration: session.registration.clone(),
                sender: session.sender.clone(),
            })
    }

    pub async fn publish_to_device(
        &self,
        device_id: &str,
        request: RpcRequest,
    ) -> Result<(), SessionError> {
        let snapshot = self
            .active_snapshot(device_id)
            .await
            .ok_or(SessionError::DeviceOffline)?;
        self.publish_to_snapshot(&snapshot, request).await
    }

    pub async fn publish_to_snapshot(
        &self,
        snapshot: &SessionSnapshot,
        request: RpcRequest,
    ) -> Result<(), SessionError> {
        let sender = self
            .sessions
            .lock()
            .await
            .get(&snapshot.registration.device_id)
            .filter(|session| snapshot.matches(session))
            .map(|session| session.sender.clone())
            .ok_or(SessionError::SessionUnavailable)?;
        let pending = if request.mode == RpcMode::TwoWay {
            let pending = PendingRpcResponse {
                command_id: request.id,
                device_id: snapshot.registration.device_id.clone(),
                token_id: snapshot.registration.token_id,
                connection_id: snapshot.registration.connection_id.clone(),
                is_gateway: snapshot.registration.is_gateway,
                expires_at: request.expires_at,
            };
            self.pending_responses
                .lock()
                .await
                .insert(request.id, pending.clone());
            Some(pending)
        } else {
            None
        };
        let (published, published_confirmation) = oneshot::channel();
        if sender
            .send(ActiveDeviceSession {
                request,
                published: Some(published),
            })
            .await
            .is_err()
        {
            if let Some(pending) = pending.as_ref() {
                self.remove_pending_response(pending).await;
            }
            return Err(SessionError::SessionUnavailable);
        }
        let result = timeout(COMMAND_PUBACK_TIMEOUT, published_confirmation)
            .await
            .map_err(|_| SessionError::PublicationTimeout)?
            .map_err(|_| SessionError::PublicationWaiterUnavailable);
        if result.is_err()
            && let Some(pending) = pending.as_ref()
        {
            self.remove_pending_response(pending).await;
        }
        result
    }

    async fn pending_response(
        &self,
        device: &AuthenticatedDevice,
        connection_id: &str,
        command_id: Uuid,
    ) -> Option<PendingRpcResponse> {
        let now = Utc::now();
        let mut pending = self.pending_responses.lock().await;
        pending.retain(|_, candidate| candidate.expires_at > now);
        pending
            .get(&command_id)
            .filter(|candidate| candidate.matches(device, connection_id, command_id))
            .cloned()
    }

    async fn remove_pending_response(&self, pending: &PendingRpcResponse) {
        let mut responses = self.pending_responses.lock().await;
        if responses.get(&pending.command_id).is_some_and(|candidate| {
            candidate.matches(
                &AuthenticatedDevice {
                    token_id: pending.token_id,
                    device_id: pending.device_id.clone(),
                    is_gateway: pending.is_gateway,
                },
                &pending.connection_id,
                pending.command_id,
            )
        }) {
            responses.remove(&pending.command_id);
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticatedDevice {
    pub token_id: Uuid,
    pub device_id: String,
    pub is_gateway: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportAuthRequest {
    pub client_id: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct TransportUplink {
    pub device: AuthenticatedDevice,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransportRpcResponse {
    pub command_id: Uuid,
    pub device_id: String,
    pub token_id: Uuid,
    pub response: serde_json::Value,
}

pub trait DeviceAuthenticator: Send + Sync {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>;

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait UplinkForwarder: Send + Sync {
    fn forward(
        &self,
        token: &str,
        message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}

pub trait RpcResponseForwarder: Send + Sync {
    fn forward_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}

#[derive(Clone)]
pub struct HttpDeviceAuthenticator {
    client: reqwest::Client,
    session_url: String,
    authorization_url: String,
    secret: Arc<str>,
}

impl HttpDeviceAuthenticator {
    pub fn new(api_base_url: &str, secret: impl AsRef<str>) -> Result<Self, TransportError> {
        let session_url = format!(
            "{}/internal/mqtt-transport/session-resolution",
            api_base_url.trim_end_matches('/')
        );
        reqwest::Url::parse(&session_url)
            .map_err(|error| TransportError::Configuration(error.to_string()))?;
        let authorization_url = format!(
            "{}/internal/mqtt-transport/session-authorization",
            api_base_url.trim_end_matches('/')
        );
        Ok(Self {
            client: reqwest::Client::new(),
            session_url,
            authorization_url,
            secret: Arc::from(secret.as_ref()),
        })
    }
}

impl DeviceAuthenticator for HttpDeviceAuthenticator {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        let client = self.client.clone();
        let session_url = self.session_url.clone();
        let secret = Arc::clone(&self.secret);
        Box::pin(async move {
            let response = client
                .post(session_url)
                .header("x-iot-mqtt-transport-secret", secret.as_ref())
                .json(&request)
                .send()
                .await?;
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) {
                return Err(TransportError::Unauthorized);
            }
            if !response.status().is_success() {
                return Err(TransportError::AuthenticationServiceUnavailable(
                    response.status().as_u16(),
                ));
            }
            response
                .json::<AuthenticatedDevice>()
                .await
                .map_err(Into::into)
        })
    }

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let client = self.client.clone();
        let authorization_url = self.authorization_url.clone();
        let secret = Arc::clone(&self.secret);
        Box::pin(async move {
            let response = client
                .post(authorization_url)
                .header("x-iot-mqtt-transport-secret", secret.as_ref())
                .json(&serde_json::json!({
                    "device_id": device.device_id,
                    "token_id": device.token_id,
                }))
                .send()
                .await?;
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) {
                return Err(TransportError::Unauthorized);
            }
            if response.status().is_success() {
                Ok(())
            } else {
                Err(TransportError::AuthenticationServiceUnavailable(
                    response.status().as_u16(),
                ))
            }
        })
    }
}

#[derive(Clone)]
pub struct HttpUplinkForwarder {
    client: reqwest::Client,
    webhook_url: String,
    secret: Arc<str>,
}

impl HttpUplinkForwarder {
    pub fn new(
        webhook_url: impl Into<String>,
        secret: impl AsRef<str>,
    ) -> Result<Self, TransportError> {
        let webhook_url = webhook_url.into();
        reqwest::Url::parse(&webhook_url)
            .map_err(|error| TransportError::Configuration(error.to_string()))?;
        Ok(Self {
            client: reqwest::Client::new(),
            webhook_url,
            secret: Arc::from(secret.as_ref()),
        })
    }
}

impl UplinkForwarder for HttpUplinkForwarder {
    fn forward(
        &self,
        token: &str,
        message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let client = self.client.clone();
        let webhook_url = self.webhook_url.clone();
        let secret = Arc::clone(&self.secret);
        let token = token.to_owned();
        Box::pin(async move {
            let payload = String::from_utf8(message.payload)
                .map_err(|_| TransportError::InvalidUplinkPayload)?;
            let response = client
                .post(webhook_url)
                .header("x-iot-mqtt-transport-webhook", secret.as_ref())
                .json(&serde_json::json!({
                    "action": "message_publish",
                    "from_username": token,
                    "topic": message.topic,
                    "qos": match message.qos {
                        QoS::AtMostOnce => 0_u8,
                        QoS::AtLeastOnce => 1_u8,
                        QoS::ExactlyOnce => return Err(TransportError::ForbiddenTopic),
                    },
                    "ts": message.received_at.timestamp_millis(),
                    "payload": payload,
                }))
                .send()
                .await?;
            if response.status().is_success() {
                Ok(())
            } else {
                Err(TransportError::UplinkRejected(response.status().as_u16()))
            }
        })
    }
}

#[derive(Clone)]
pub struct HttpRpcResponseForwarder {
    client: reqwest::Client,
    response_url: String,
    secret: Arc<str>,
}

impl HttpRpcResponseForwarder {
    pub fn new(api_base_url: &str, secret: impl AsRef<str>) -> Result<Self, TransportError> {
        let response_url = format!(
            "{}/internal/mqtt-transport/rpc-response",
            api_base_url.trim_end_matches('/')
        );
        reqwest::Url::parse(&response_url)
            .map_err(|error| TransportError::Configuration(error.to_string()))?;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|error| TransportError::Configuration(error.to_string()))?,
            response_url,
            secret: Arc::from(secret.as_ref()),
        })
    }
}

impl RpcResponseForwarder for HttpRpcResponseForwarder {
    fn forward_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let client = self.client.clone();
        let response_url = self.response_url.clone();
        let secret = Arc::clone(&self.secret);
        Box::pin(async move {
            let response = client
                .post(response_url)
                .header("x-iot-mqtt-transport-secret", secret.as_ref())
                .json(&response)
                .send()
                .await?;
            if response.status() == StatusCode::NO_CONTENT {
                Ok(())
            } else {
                Err(TransportError::RpcResponseRejected(
                    response.status().as_u16(),
                ))
            }
        })
    }
}

#[derive(Clone)]
struct RejectingRpcResponseForwarder;

impl RpcResponseForwarder for RejectingRpcResponseForwarder {
    fn forward_response(
        &self,
        _response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Err(TransportError::RpcResponseForwarderUnavailable) })
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("MQTT device authentication failed")]
    Unauthorized,
    #[error("MQTT protocol error: {0}")]
    Protocol(#[from] rumqttc::mqttbytes::Error),
    #[error("MQTT connection closed")]
    ConnectionClosed,
    #[error("unsupported MQTT packet")]
    UnsupportedPacket,
    #[error("MQTT topic is not permitted for this device")]
    ForbiddenTopic,
    #[error("transport configuration is invalid: {0}")]
    Configuration(String),
    #[error("MQTT authentication service returned status {0}")]
    AuthenticationServiceUnavailable(u16),
    #[error("uplink webhook returned status {0}")]
    UplinkRejected(u16),
    #[error("uplink payload must be UTF-8 JSON")]
    InvalidUplinkPayload,
    #[error("RPC response payload must be JSON")]
    InvalidRpcResponsePayload,
    #[error("RPC response callback returned status {0}")]
    RpcResponseRejected(u16),
    #[error("RPC response callback is not configured")]
    RpcResponseForwarderUnavailable,
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone)]
struct InternalRpcState {
    transport: MqttTransport,
    secret: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct InternalRpcPublishRequest {
    device_id: String,
    id: Uuid,
    method: String,
    params: serde_json::Value,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    #[serde(default)]
    mode: RpcMode,
}

#[derive(Debug, Deserialize)]
struct InternalSessionRevokeRequest {
    device_id: String,
    token_id: Uuid,
}

#[derive(Clone)]
pub struct MqttTransport {
    router: RpcSessionRouter,
    authenticator: Arc<dyn DeviceAuthenticator>,
    uplink: Arc<dyn UplinkForwarder>,
    rpc_response_forwarder: Arc<dyn RpcResponseForwarder>,
}

impl MqttTransport {
    pub fn new(
        authenticator: impl DeviceAuthenticator + 'static,
        uplink: impl UplinkForwarder + 'static,
    ) -> Self {
        Self {
            router: RpcSessionRouter::default(),
            authenticator: Arc::new(authenticator),
            uplink: Arc::new(uplink),
            rpc_response_forwarder: Arc::new(RejectingRpcResponseForwarder),
        }
    }

    pub fn with_rpc_response_forwarder(
        mut self,
        rpc_response_forwarder: impl RpcResponseForwarder + 'static,
    ) -> Self {
        self.rpc_response_forwarder = Arc::new(rpc_response_forwarder);
        self
    }

    pub fn router(&self) -> RpcSessionRouter {
        self.router.clone()
    }

    async fn authorize_active_session(
        &self,
        device_id: &str,
    ) -> Result<SessionSnapshot, TransportError> {
        let snapshot = self
            .router
            .active_snapshot(device_id)
            .await
            .ok_or(SessionError::DeviceOffline)?;
        self.authenticator
            .authorize_session(snapshot.authenticated_device())
            .await?;
        Ok(snapshot)
    }

    pub fn internal_router(&self, secret: impl AsRef<str>) -> Router {
        Router::new()
            .route("/internal/rpc/publish", post(publish_internal_rpc))
            .route("/internal/sessions/revoke", post(revoke_internal_session))
            .with_state(InternalRpcState {
                transport: self.clone(),
                secret: Arc::from(secret.as_ref()),
            })
    }

    pub async fn serve_connection<S>(&self, stream: S) -> Result<(), TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut framed = Framed::new(
            stream,
            Codec {
                max_incoming_size: MAX_PACKET_BYTES,
                max_outgoing_size: MAX_PACKET_BYTES,
            },
        );
        let connect = match next_packet(&mut framed).await? {
            Packet::Connect(connect) => connect,
            _ => return Err(TransportError::UnsupportedPacket),
        };
        let login = connect.login.as_ref().ok_or(TransportError::Unauthorized)?;
        let device = match self
            .authenticator
            .authenticate(TransportAuthRequest {
                client_id: connect.client_id.clone(),
                username: login.username.clone(),
                password: login.password.clone(),
            })
            .await
        {
            Ok(device) => device,
            Err(_) => {
                framed
                    .send(Packet::ConnAck(ConnAck::new(
                        ConnectReturnCode::NotAuthorized,
                        false,
                    )))
                    .await?;
                return Err(TransportError::Unauthorized);
            }
        };
        framed
            .send(Packet::ConnAck(ConnAck::new(
                ConnectReturnCode::Success,
                false,
            )))
            .await?;

        let connection_id = Uuid::now_v7().to_string();
        let result = self
            .serve_authenticated_connection(
                &mut framed,
                &device,
                &login.username,
                &connect.client_id,
                &connection_id,
            )
            .await;
        self.router
            .unregister(&device.device_id, &connection_id)
            .await;
        result
    }

    async fn serve_authenticated_connection<S>(
        &self,
        framed: &mut Framed<S, Codec>,
        device: &AuthenticatedDevice,
        token: &str,
        client_id: &str,
        connection_id: &str,
    ) -> Result<(), TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut commands = None;
        let mut pending: HashMap<u16, oneshot::Sender<()>> = HashMap::new();
        let mut next_packet_id = 1_u16;

        loop {
            tokio::select! {
                packet = next_packet(framed) => {
                    let packet = match packet {
                        Ok(packet) => packet,
                        Err(TransportError::ConnectionClosed) => break,
                        Err(error) => return Err(error),
                    };
                    match packet {
                        Packet::Subscribe(subscribe) => {
                            let expected_filter = if device.is_gateway {
                                GATEWAY_RPC_FILTER
                            } else {
                                DIRECT_RPC_FILTER
                            };
                            let return_codes = subscribe.filters.iter().map(|filter| {
                                if filter.path == expected_filter && filter.qos != QoS::ExactlyOnce {
                                    SubscribeReasonCode::Success(filter.qos)
                                } else {
                                    SubscribeReasonCode::Failure
                                }
                            }).collect::<Vec<_>>();
                            let accepted = return_codes.iter().any(|code| {
                                matches!(code, SubscribeReasonCode::Success(_))
                            });
                            framed.send(Packet::SubAck(SubAck::new(subscribe.pkid, return_codes))).await?;
                            if accepted && commands.is_none() {
                                commands = Some(self.router.register(SessionRegistration {
                                    token_id: device.token_id,
                                    device_id: device.device_id.clone(),
                                    client_id: client_id.to_owned(),
                                    connection_id: connection_id.to_owned(),
                                    is_gateway: device.is_gateway,
                                    connected_at: Utc::now(),
                                }).await);
                            }
                        }
                        Packet::Publish(publish) => {
                            if let Some(command_id) = rpc_response_command_id(device, &publish.topic) {
                                if publish.qos != QoS::AtLeastOnce {
                                    return Err(TransportError::ForbiddenTopic);
                                }
                                let Some(pending) = self
                                    .router
                                    .pending_response(device, connection_id, command_id)
                                    .await
                                else {
                                    framed.send(Packet::PubAck(PubAck::new(publish.pkid))).await?;
                                    continue;
                                };
                                let response = serde_json::from_slice(&publish.payload)
                                    .map_err(|_| TransportError::InvalidRpcResponsePayload)?;
                                if self
                                    .rpc_response_forwarder
                                    .forward_response(TransportRpcResponse {
                                        command_id,
                                        device_id: device.device_id.clone(),
                                        token_id: device.token_id,
                                        response,
                                    })
                                    .await
                                    .is_err()
                                {
                                    continue;
                                }
                                self.router.remove_pending_response(&pending).await;
                                framed.send(Packet::PubAck(PubAck::new(publish.pkid))).await?;
                                continue;
                            }
                            if publish.qos == QoS::ExactlyOnce || !is_allowed_uplink(&device, &publish.topic) {
                                return Err(TransportError::ForbiddenTopic);
                            }
                            self.uplink.forward(
                                token,
                                TransportUplink {
                                    device: device.clone(),
                                    topic: publish.topic,
                                    payload: publish.payload.to_vec(),
                                    qos: publish.qos,
                                    received_at: Utc::now(),
                                },
                            ).await?;
                            if publish.qos == QoS::AtLeastOnce {
                                framed.send(Packet::PubAck(PubAck::new(publish.pkid))).await?;
                            }
                        }
                        Packet::PubAck(ack) => {
                            if let Some(published) = pending.remove(&ack.pkid) {
                                let _ = published.send(());
                            }
                        }
                        Packet::PingReq => {
                            framed.send(Packet::PingResp).await?;
                        }
                        Packet::Disconnect => break,
                        _ => return Err(TransportError::UnsupportedPacket),
                    }
                }
                command = receive_command(&mut commands), if commands.is_some() => {
                    let Some(mut command) = command else {
                        break;
                    };
                    let packet_id = next_packet_id;
                    next_packet_id = next_packet_id.wrapping_add(1).max(1);
                    let payload = serde_json::to_vec(&serde_json::json!({
                        "id": command.request.id,
                        "method": command.request.method,
                        "params": command.request.params,
                        "issued_at": command.request.issued_at,
                        "expires_at": command.request.expires_at,
                        "mode": command.request.mode,
                    })).map_err(|_| TransportError::UnsupportedPacket)?;
                    let topic = if device.is_gateway {
                        format!("v1/gateways/me/rpc/request/{}", command.request.id)
                    } else {
                        format!("v1/devices/me/rpc/request/{}", command.request.id)
                    };
                    let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                    publish.pkid = packet_id;
                    if let Some(published) = command.take_published_acknowledgement() {
                        pending.insert(packet_id, published);
                    }
                    framed.send(Packet::Publish(publish)).await?;
                }
            }
        }

        Ok(())
    }
}

async fn receive_command(
    receiver: &mut Option<mpsc::Receiver<ActiveDeviceSession>>,
) -> Option<ActiveDeviceSession> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => None,
    }
}

async fn next_packet<S>(framed: &mut Framed<S, Codec>) -> Result<Packet, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .next()
        .await
        .ok_or(TransportError::ConnectionClosed)?
        .map_err(TransportError::Protocol)
}

fn is_allowed_uplink(device: &AuthenticatedDevice, topic: &str) -> bool {
    if device.is_gateway {
        GATEWAY_TOPICS.contains(&topic)
    } else {
        topic == DIRECT_TELEMETRY_TOPIC
    }
}

fn rpc_response_command_id(device: &AuthenticatedDevice, topic: &str) -> Option<Uuid> {
    let prefix = if device.is_gateway {
        GATEWAY_RPC_RESPONSE_PREFIX
    } else {
        DIRECT_RPC_RESPONSE_PREFIX
    };
    let command_id = topic.strip_prefix(prefix)?;
    if command_id.is_empty() || command_id.contains('/') {
        return None;
    }
    let command_id = Uuid::parse_str(command_id).ok()?;
    (command_id.get_version_num() == 7).then_some(command_id)
}

async fn publish_internal_rpc(
    State(state): State<InternalRpcState>,
    headers: HeaderMap,
    Json(request): Json<InternalRpcPublishRequest>,
) -> Result<StatusCode, StatusCode> {
    let supplied_secret = headers
        .get("x-iot-mqtt-transport-secret")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !constant_time_equal(state.secret.as_bytes(), supplied_secret.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let rpc = RpcRequest::with_mode(
        request.id,
        request.method,
        request.params,
        request.issued_at,
        request.expires_at,
        request.mode,
    )
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    let snapshot = match state
        .transport
        .authorize_active_session(&request.device_id)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return match error {
                TransportError::Unauthorized => Err(StatusCode::UNAUTHORIZED),
                TransportError::Session(SessionError::DeviceOffline) => {
                    Err(StatusCode::SERVICE_UNAVAILABLE)
                }
                _ => Err(StatusCode::SERVICE_UNAVAILABLE),
            };
        }
    };
    match state
        .transport
        .router
        .publish_to_snapshot(&snapshot, rpc)
        .await
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(SessionError::DeviceOffline) => Err(StatusCode::SERVICE_UNAVAILABLE),
        Err(SessionError::PublicationTimeout) => Err(StatusCode::GATEWAY_TIMEOUT),
        Err(SessionError::SessionUnavailable | SessionError::PublicationWaiterUnavailable) => {
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
        Err(SessionError::AcknowledgementAlreadyConsumed) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn revoke_internal_session(
    State(state): State<InternalRpcState>,
    headers: HeaderMap,
    Json(request): Json<InternalSessionRevokeRequest>,
) -> Result<StatusCode, StatusCode> {
    let supplied_secret = headers
        .get("x-iot-mqtt-transport-secret")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !constant_time_equal(state.secret.as_bytes(), supplied_secret.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .transport
        .router
        .revoke_session(&request.device_id, request.token_id)
        .await;
    Ok(StatusCode::NO_CONTENT)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}
