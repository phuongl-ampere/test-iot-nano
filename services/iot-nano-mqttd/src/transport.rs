#![forbid(unsafe_code)]

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use iot_nano_foundation::{RpcMode, RpcRequest};
use iot_nano_stream::StreamPort;
use rumqttc::v5::mqttbytes::{
    QoS as V5QoS,
    v5::{
        Codec as V5Codec, ConnAck as V5ConnAck, ConnectReturnCode as V5ConnectReturnCode,
        Packet as V5Packet, PingResp as V5PingResp, PubAck as V5PubAck, SubAck as V5SubAck,
        SubscribeReasonCode as V5SubscribeReasonCode,
    },
};
use rumqttc::{
    ConnectReturnCode, Packet, PubAck, Publish, QoS, SubscribeReasonCode,
    mqttbytes::v4::{Codec, ConnAck, SubAck},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, OwnedMutexGuard, mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::ports::{
    CommandResponsePort, DeviceAuthorizationPort, DeviceClaimCodeError, DeviceClaimCodeOutcome,
    DeviceClaimCodePort, DeviceClaimCodeRejection, DeviceClaimCodeRequest,
    LocalDeviceAuthenticator, LocalRpcResponseForwarder, LocalStreamUplinkForwarder,
};

const MAX_PACKET_BYTES: usize = 1024 * 1024;
const SESSION_COMMAND_CAPACITY: usize = 64;
const COMMAND_PUBACK_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECT_RPC_FILTER: &str = "v1/devices/me/rpc/request/+";
const GATEWAY_RPC_FILTER: &str = "v1/gateways/me/rpc/request/+";
const DIRECT_RPC_RESPONSE_PREFIX: &str = "v1/devices/me/rpc/response/";
const DIRECT_PAIRING_REQUEST_TOPIC: &str = "v1/devices/me/pairing/request";
const DIRECT_PAIRING_RESPONSE_FILTER: &str = "v1/devices/me/pairing/response/+";
const DIRECT_PAIRING_RESPONSE_PREFIX: &str = "v1/devices/me/pairing/response/";
const GATEWAY_RPC_RESPONSE_PREFIX: &str = "v1/gateways/me/rpc/response/";
pub(crate) const DIRECT_TELEMETRY_TOPIC: &str = "v1/devices/me/telemetry";
pub(crate) const GATEWAY_TOPICS: [&str; 3] = [
    "v1/gateways/me/connect",
    "v1/gateways/me/disconnect",
    "v1/gateways/me/telemetry",
];

#[derive(Debug, Clone)]
pub struct SessionRegistration {
    pub token_id: Uuid,
    pub tenant_id: Uuid,
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
    lifecycle: Arc<SessionLifecycle>,
}

impl ActiveDeviceSession {
    pub async fn acquire_delivery_lease(&self) -> Option<SessionDeliveryLease> {
        Arc::clone(&self.lifecycle).acquire_delivery_lease().await
    }

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

pub struct SessionDeliveryLease {
    _guard: OwnedMutexGuard<()>,
}

#[derive(Debug)]
struct SessionLifecycle {
    active: AtomicBool,
    delivery_gate: Arc<Mutex<()>>,
}

impl SessionLifecycle {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(true),
            delivery_gate: Arc::new(Mutex::new(())),
        }
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    async fn wait_for_active_delivery(&self) {
        let _guard = Arc::clone(&self.delivery_gate).lock_owned().await;
    }

    async fn acquire_delivery_lease(self: Arc<Self>) -> Option<SessionDeliveryLease> {
        let guard = Arc::clone(&self.delivery_gate).lock_owned().await;
        self.is_active()
            .then_some(SessionDeliveryLease { _guard: guard })
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
    state: Arc<Mutex<RouterState>>,
}

#[derive(Debug)]
struct RegisteredSession {
    registration: SessionRegistration,
    sender: mpsc::Sender<ActiveDeviceSession>,
    lifecycle: Arc<SessionLifecycle>,
}

#[derive(Debug, Default)]
struct RouterState {
    sessions: HashMap<String, RegisteredSession>,
    pending_responses: HashMap<Uuid, PendingRpcResponse>,
    revoked_tokens: HashSet<(String, Uuid)>,
}

#[derive(Debug, Clone)]
struct PendingRpcResponse {
    command_id: Uuid,
    tenant_id: Uuid,
    device_id: String,
    token_id: Uuid,
    connection_id: String,
    is_gateway: bool,
    expires_at: DateTime<Utc>,
}

impl PendingRpcResponse {
    fn matches(&self, device: &AuthenticatedDevice, connection_id: &str, command_id: Uuid) -> bool {
        self.command_id == command_id
            && self.tenant_id == device.tenant_id
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
    lifecycle: Arc<SessionLifecycle>,
}

impl SessionSnapshot {
    pub fn authenticated_device(&self) -> AuthenticatedDevice {
        AuthenticatedDevice {
            token_id: self.registration.token_id,
            tenant_id: self.registration.tenant_id,
            device_id: self.registration.device_id.clone(),
            is_gateway: self.registration.is_gateway,
        }
    }

    fn matches(&self, session: &RegisteredSession) -> bool {
        self.registration.token_id == session.registration.token_id
            && self.registration.connection_id == session.registration.connection_id
            && self.sender.same_channel(&session.sender)
            && Arc::ptr_eq(&self.lifecycle, &session.lifecycle)
    }
}

impl RpcSessionRouter {
    pub async fn register(
        &self,
        registration: SessionRegistration,
    ) -> mpsc::Receiver<ActiveDeviceSession> {
        let (sender, receiver) = mpsc::channel(SESSION_COMMAND_CAPACITY);
        let lifecycle = Arc::new(SessionLifecycle::new());
        let previous = {
            let mut state = self.state.lock().await;
            if state
                .revoked_tokens
                .contains(&(registration.device_id.clone(), registration.token_id))
            {
                return receiver;
            }
            let previous = state
                .sessions
                .get(&registration.device_id)
                .map(|session| Arc::clone(&session.lifecycle));
            if let Some(previous) = &previous {
                previous.deactivate();
            }
            state.sessions.insert(
                registration.device_id.clone(),
                RegisteredSession {
                    registration: registration.clone(),
                    sender,
                    lifecycle,
                },
            );
            state.pending_responses.retain(|_, pending| {
                pending.expires_at > Utc::now() && pending.device_id != registration.device_id
            });
            previous
        };
        if let Some(previous) = previous {
            previous.wait_for_active_delivery().await;
        }
        receiver
    }

    pub async fn unregister(&self, device_id: &str, connection_id: &str) {
        let lifecycle = {
            let mut state = self.state.lock().await;
            let lifecycle = state
                .sessions
                .get(device_id)
                .filter(|session| session.registration.connection_id == connection_id)
                .map(|session| Arc::clone(&session.lifecycle));
            if let Some(lifecycle) = &lifecycle {
                lifecycle.deactivate();
                state.sessions.remove(device_id);
                state.pending_responses.retain(|_, pending| {
                    pending.device_id != device_id || pending.connection_id != connection_id
                });
            }
            lifecycle
        };
        if let Some(lifecycle) = lifecycle {
            lifecycle.wait_for_active_delivery().await;
        }
    }

    pub async fn revoke_session(&self, device_id: &str, token_id: Uuid) -> bool {
        let lifecycle = {
            let mut state = self.state.lock().await;
            state
                .revoked_tokens
                .insert((device_id.to_owned(), token_id));
            let lifecycle = state
                .sessions
                .get(device_id)
                .filter(|session| session.registration.token_id == token_id)
                .map(|session| Arc::clone(&session.lifecycle));
            if let Some(lifecycle) = &lifecycle {
                lifecycle.deactivate();
                state.sessions.remove(device_id);
                state.pending_responses.retain(|_, pending| {
                    pending.device_id != device_id || pending.token_id != token_id
                });
            }
            lifecycle
        };
        if let Some(lifecycle) = lifecycle {
            lifecycle.wait_for_active_delivery().await;
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

    async fn is_revoked(&self, device: &AuthenticatedDevice) -> bool {
        self.state
            .lock()
            .await
            .revoked_tokens
            .contains(&(device.device_id.clone(), device.token_id))
    }

    pub async fn active_snapshot(&self, device_id: &str) -> Option<SessionSnapshot> {
        self.state
            .lock()
            .await
            .sessions
            .get(device_id)
            .filter(|session| session.lifecycle.is_active())
            .map(|session| SessionSnapshot {
                registration: session.registration.clone(),
                sender: session.sender.clone(),
                lifecycle: Arc::clone(&session.lifecycle),
            })
    }

    pub async fn publish_to_device(
        &self,
        tenant_id: Uuid,
        device_id: &str,
        request: RpcRequest,
    ) -> Result<(), SessionError> {
        let snapshot = self
            .active_snapshot(device_id)
            .await
            .ok_or(SessionError::DeviceOffline)?;
        self.publish_to_snapshot(tenant_id, &snapshot, request)
            .await
    }

    pub async fn publish_to_snapshot(
        &self,
        tenant_id: Uuid,
        snapshot: &SessionSnapshot,
        request: RpcRequest,
    ) -> Result<(), SessionError> {
        let (sender, lifecycle, pending) = {
            let mut state = self.state.lock().await;
            let (sender, lifecycle) = state
                .sessions
                .get(&snapshot.registration.device_id)
                .filter(|session| {
                    snapshot.matches(session) && session.registration.tenant_id == tenant_id
                })
                .map(|session| (session.sender.clone(), Arc::clone(&session.lifecycle)))
                .ok_or(SessionError::SessionUnavailable)?;
            if !lifecycle.is_active() {
                return Err(SessionError::SessionUnavailable);
            }
            let pending = if request.mode == RpcMode::TwoWay {
                let pending = PendingRpcResponse {
                    command_id: request.id,
                    tenant_id,
                    device_id: snapshot.registration.device_id.clone(),
                    token_id: snapshot.registration.token_id,
                    connection_id: snapshot.registration.connection_id.clone(),
                    is_gateway: snapshot.registration.is_gateway,
                    expires_at: request.expires_at,
                };
                state.pending_responses.insert(request.id, pending.clone());
                Some(pending)
            } else {
                None
            };
            (sender, lifecycle, pending)
        };
        let (published, published_confirmation) = oneshot::channel();
        if sender
            .send(ActiveDeviceSession {
                request,
                published: Some(published),
                lifecycle,
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
        let mut state = self.state.lock().await;
        state
            .pending_responses
            .retain(|_, candidate| candidate.expires_at > now);
        state
            .pending_responses
            .get(&command_id)
            .filter(|candidate| candidate.matches(device, connection_id, command_id))
            .cloned()
    }

    async fn remove_pending_response(&self, pending: &PendingRpcResponse) {
        let mut state = self.state.lock().await;
        if state
            .pending_responses
            .get(&pending.command_id)
            .is_some_and(|candidate| {
                candidate.matches(
                    &AuthenticatedDevice {
                        token_id: pending.token_id,
                        tenant_id: pending.tenant_id,
                        device_id: pending.device_id.clone(),
                        is_gateway: pending.is_gateway,
                    },
                    &pending.connection_id,
                    pending.command_id,
                )
            })
        {
            state.pending_responses.remove(&pending.command_id);
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticatedDevice {
    pub token_id: Uuid,
    pub tenant_id: Uuid,
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
    pub tenant_id: Uuid,
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
struct RejectingRpcResponseForwarder;

impl RpcResponseForwarder for RejectingRpcResponseForwarder {
    fn forward_response(
        &self,
        _response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Err(TransportError::RpcResponseForwarderUnavailable) })
    }
}

#[derive(Clone)]
struct RejectingDeviceClaimCodePort;

impl DeviceClaimCodePort for RejectingDeviceClaimCodePort {
    fn issue(
        &self,
        _request: DeviceClaimCodeRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceClaimCodeOutcome, DeviceClaimCodeError>> + Send + '_>,
    > {
        Box::pin(async {
            Ok(DeviceClaimCodeOutcome::Rejected {
                reason: DeviceClaimCodeRejection::Unavailable,
            })
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairingRequest {
    request_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PairingResponse {
    Issued {
        device_id: String,
        code: String,
        expires_at: DateTime<Utc>,
    },
    Rejected {
        reason: DeviceClaimCodeRejection,
    },
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("MQTT device authentication failed")]
    Unauthorized,
    #[error("MQTT protocol error: {0}")]
    Protocol(#[from] rumqttc::mqttbytes::Error),
    #[error("MQTT 5 protocol error: {0}")]
    ProtocolV5(#[from] rumqttc::v5::mqttbytes::Error),
    #[error("MQTT connection closed")]
    ConnectionClosed,
    #[error("unsupported MQTT packet")]
    UnsupportedPacket,
    #[error("MQTT topic is not permitted for this device")]
    ForbiddenTopic,
    #[error("MQTT authorization port is unavailable: {0}")]
    AuthorizationUnavailable(String),
    #[error("durable stream append failed: {0}")]
    StreamAppendFailed(String),
    #[error("uplink payload must be UTF-8 JSON")]
    InvalidUplinkPayload,
    #[error("RPC response payload must be JSON")]
    InvalidRpcResponsePayload,
    #[error("pairing request payload is invalid")]
    InvalidPairingRequest,
    #[error("RPC response callback is not configured")]
    RpcResponseForwarderUnavailable,
    #[error("command response port is unavailable: {0}")]
    CommandResponseUnavailable(String),
    #[error(transparent)]
    Session(#[from] SessionError),
}

#[derive(Clone)]
pub struct MqttdDeviceTransport {
    router: RpcSessionRouter,
    authenticator: Arc<dyn DeviceAuthenticator>,
    uplink: Arc<dyn UplinkForwarder>,
    rpc_response_forwarder: Arc<dyn RpcResponseForwarder>,
    device_claim_codes: Arc<dyn DeviceClaimCodePort>,
}

impl MqttdDeviceTransport {
    pub fn with_local_ports(
        authorization: Arc<dyn DeviceAuthorizationPort>,
        stream: Arc<dyn StreamPort>,
        command_responses: Arc<dyn CommandResponsePort>,
    ) -> Self {
        Self::with_local_ports_and_router(
            RpcSessionRouter::default(),
            authorization,
            stream,
            command_responses,
        )
    }

    pub fn with_local_ports_and_router(
        router: RpcSessionRouter,
        authorization: Arc<dyn DeviceAuthorizationPort>,
        stream: Arc<dyn StreamPort>,
        command_responses: Arc<dyn CommandResponsePort>,
    ) -> Self {
        let mut transport = Self::new(
            LocalDeviceAuthenticator::new(Arc::clone(&authorization)),
            LocalStreamUplinkForwarder::new(authorization, stream),
        )
        .with_rpc_response_forwarder(LocalRpcResponseForwarder::new(command_responses));
        transport.router = router;
        transport
    }

    pub fn new(
        authenticator: impl DeviceAuthenticator + 'static,
        uplink: impl UplinkForwarder + 'static,
    ) -> Self {
        Self {
            router: RpcSessionRouter::default(),
            authenticator: Arc::new(authenticator),
            uplink: Arc::new(uplink),
            rpc_response_forwarder: Arc::new(RejectingRpcResponseForwarder),
            device_claim_codes: Arc::new(RejectingDeviceClaimCodePort),
        }
    }

    pub fn with_rpc_response_forwarder(
        mut self,
        rpc_response_forwarder: impl RpcResponseForwarder + 'static,
    ) -> Self {
        self.rpc_response_forwarder = Arc::new(rpc_response_forwarder);
        self
    }

    pub fn with_device_claim_code_port(
        mut self,
        device_claim_codes: Arc<dyn DeviceClaimCodePort>,
    ) -> Self {
        self.device_claim_codes = device_claim_codes;
        self
    }

    pub fn router(&self) -> RpcSessionRouter {
        self.router.clone()
    }

    async fn pairing_response(
        &self,
        device: &AuthenticatedDevice,
        payload: &[u8],
    ) -> Result<(Uuid, Vec<u8>), TransportError> {
        if device.is_gateway {
            return Err(TransportError::ForbiddenTopic);
        }
        let request: PairingRequest =
            serde_json::from_slice(payload).map_err(|_| TransportError::InvalidPairingRequest)?;
        if request.request_id.get_version_num() != 7 {
            return Err(TransportError::InvalidPairingRequest);
        }
        let response = match self
            .device_claim_codes
            .issue(DeviceClaimCodeRequest {
                tenant_id: device.tenant_id,
                device_id: device.device_id.clone(),
                request_id: request.request_id,
            })
            .await
        {
            Ok(DeviceClaimCodeOutcome::Issued {
                device_id,
                code,
                expires_at,
            }) => PairingResponse::Issued {
                device_id,
                code,
                expires_at,
            },
            Ok(DeviceClaimCodeOutcome::Rejected { reason }) => PairingResponse::Rejected { reason },
            Err(_) => PairingResponse::Rejected {
                reason: DeviceClaimCodeRejection::Unavailable,
            },
        };
        let payload =
            serde_json::to_vec(&response).map_err(|_| TransportError::InvalidPairingRequest)?;
        Ok((request.request_id, payload))
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
        if self
            .authenticator
            .authorize_session(device.clone())
            .await
            .is_err()
        {
            framed
                .send(Packet::ConnAck(ConnAck::new(
                    ConnectReturnCode::NotAuthorized,
                    false,
                )))
                .await?;
            return Err(TransportError::Unauthorized);
        }
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

    pub async fn serve_v5_connection<S>(&self, stream: S) -> Result<(), TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut framed = Framed::new(
            stream,
            V5Codec {
                max_incoming_size: Some(MAX_PACKET_BYTES as u32),
                max_outgoing_size: Some(MAX_PACKET_BYTES as u32),
            },
        );
        let (connect, login) = match next_v5_packet(&mut framed).await? {
            V5Packet::Connect(connect, _, login) => (connect, login),
            _ => return Err(TransportError::UnsupportedPacket),
        };
        let login = login.ok_or(TransportError::Unauthorized)?;
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
                    .send(V5Packet::ConnAck(V5ConnAck {
                        session_present: false,
                        code: V5ConnectReturnCode::NotAuthorized,
                        properties: None,
                    }))
                    .await?;
                return Err(TransportError::Unauthorized);
            }
        };
        if self
            .authenticator
            .authorize_session(device.clone())
            .await
            .is_err()
        {
            framed
                .send(V5Packet::ConnAck(V5ConnAck {
                    session_present: false,
                    code: V5ConnectReturnCode::NotAuthorized,
                    properties: None,
                }))
                .await?;
            return Err(TransportError::Unauthorized);
        }
        framed
            .send(V5Packet::ConnAck(V5ConnAck {
                session_present: false,
                code: V5ConnectReturnCode::Success,
                properties: None,
            }))
            .await?;

        let connection_id = Uuid::now_v7().to_string();
        let result = self
            .serve_authenticated_v5_connection(
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
                    if self.router.is_revoked(device).await {
                        return Err(TransportError::Unauthorized);
                    }
                    match packet {
                        Packet::Subscribe(subscribe) => {
                            let rpc_filter = if device.is_gateway {
                                GATEWAY_RPC_FILTER
                            } else {
                                DIRECT_RPC_FILTER
                            };
                            let return_codes = subscribe.filters.iter().map(|filter| {
                                let allowed = (filter.path == rpc_filter
                                    || (!device.is_gateway
                                        && filter.path == DIRECT_PAIRING_RESPONSE_FILTER))
                                    && filter.qos != QoS::ExactlyOnce;
                                if allowed {
                                    SubscribeReasonCode::Success(filter.qos)
                                } else {
                                    SubscribeReasonCode::Failure
                                }
                            }).collect::<Vec<_>>();
                            let rpc_accepted = subscribe.filters.iter().any(|filter| {
                                filter.path == rpc_filter && filter.qos != QoS::ExactlyOnce
                            });
                            framed.send(Packet::SubAck(SubAck::new(subscribe.pkid, return_codes))).await?;
                            if rpc_accepted && commands.is_none() {
                                commands = Some(self.router.register(SessionRegistration {
                                    token_id: device.token_id,
                                    tenant_id: device.tenant_id,
                                    device_id: device.device_id.clone(),
                                    client_id: client_id.to_owned(),
                                    connection_id: connection_id.to_owned(),
                                    is_gateway: device.is_gateway,
                                    connected_at: Utc::now(),
                                }).await);
                            }
                        }
                        Packet::Publish(publish) => {
                            if publish.topic == DIRECT_PAIRING_REQUEST_TOPIC {
                                if device.is_gateway
                                    || publish.qos != QoS::AtLeastOnce
                                    || publish.retain
                                {
                                    return Err(TransportError::ForbiddenTopic);
                                }
                                let (request_id, payload) = self.pairing_response(device, &publish.payload).await?;
                                framed.send(Packet::PubAck(PubAck::new(publish.pkid))).await?;
                                let packet_id = next_packet_id;
                                next_packet_id = next_packet_id.wrapping_add(1).max(1);
                                let mut response = Publish::new(
                                    format!("{DIRECT_PAIRING_RESPONSE_PREFIX}{request_id}"),
                                    QoS::AtLeastOnce,
                                    payload,
                                );
                                response.pkid = packet_id;
                                framed.send(Packet::Publish(response)).await?;
                                continue;
                            }
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
                                        tenant_id: pending.tenant_id,
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
                    let Some(_delivery_lease) = command.acquire_delivery_lease().await else {
                        continue;
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

    async fn serve_authenticated_v5_connection<S>(
        &self,
        framed: &mut Framed<S, V5Codec>,
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
            packet = next_v5_packet(framed) => {
                if self.router.is_revoked(device).await {
                    return Err(TransportError::Unauthorized);
                }
                match packet {
                Ok(V5Packet::Subscribe(subscribe)) => {
                    let rpc_filter = if device.is_gateway {
                        GATEWAY_RPC_FILTER
                    } else {
                        DIRECT_RPC_FILTER
                    };
                    let return_codes = subscribe
                        .filters
                        .iter()
                        .map(|filter| {
                            let allowed = (filter.path == rpc_filter
                                || (!device.is_gateway
                                    && filter.path == DIRECT_PAIRING_RESPONSE_FILTER))
                                && filter.qos != V5QoS::ExactlyOnce;
                            if allowed {
                                V5SubscribeReasonCode::Success(filter.qos)
                            } else {
                                V5SubscribeReasonCode::Failure
                            }
                        })
                        .collect::<Vec<_>>();
                    let rpc_accepted = subscribe.filters.iter().any(|filter| {
                        filter.path == rpc_filter && filter.qos != V5QoS::ExactlyOnce
                    });
                    framed
                        .send(V5Packet::SubAck(V5SubAck {
                            pkid: subscribe.pkid,
                            return_codes,
                            properties: None,
                        }))
                        .await?;
                    if rpc_accepted && commands.is_none() {
                        commands = Some(
                            self.router
                                .register(SessionRegistration {
                                    token_id: device.token_id,
                                    tenant_id: device.tenant_id,
                                    device_id: device.device_id.clone(),
                                    client_id: client_id.to_owned(),
                                    connection_id: connection_id.to_owned(),
                                    is_gateway: device.is_gateway,
                                    connected_at: Utc::now(),
                                })
                                .await,
                        );
                    }
                }
                Ok(V5Packet::Publish(publish)) => {
                    let topic = String::from_utf8_lossy(&publish.topic).into_owned();
                    if topic == DIRECT_PAIRING_REQUEST_TOPIC {
                        if device.is_gateway
                            || publish.qos != V5QoS::AtLeastOnce
                            || publish.retain
                        {
                            return Err(TransportError::ForbiddenTopic);
                        }
                        let (request_id, payload) = self.pairing_response(device, &publish.payload).await?;
                        framed
                            .send(V5Packet::PubAck(V5PubAck::new(publish.pkid, None)))
                            .await?;
                        let packet_id = next_packet_id;
                        next_packet_id = next_packet_id.wrapping_add(1).max(1);
                        let mut response = rumqttc::v5::mqttbytes::v5::Publish::new(
                            format!("{DIRECT_PAIRING_RESPONSE_PREFIX}{request_id}"),
                            V5QoS::AtLeastOnce,
                            payload,
                            None,
                        );
                        response.pkid = packet_id;
                        framed.send(V5Packet::Publish(response)).await?;
                        continue;
                    }
                    if let Some(command_id) = rpc_response_command_id(device, &topic) {
                        if publish.qos != V5QoS::AtLeastOnce {
                            return Err(TransportError::ForbiddenTopic);
                        }
                        let Some(pending_response) = self
                            .router
                            .pending_response(device, connection_id, command_id)
                            .await
                        else {
                            framed
                                .send(V5Packet::PubAck(V5PubAck::new(publish.pkid, None)))
                                .await?;
                            continue;
                        };
                        let response = serde_json::from_slice(&publish.payload)
                            .map_err(|_| TransportError::InvalidRpcResponsePayload)?;
                        if self
                            .rpc_response_forwarder
                            .forward_response(TransportRpcResponse {
                                command_id,
                                tenant_id: pending_response.tenant_id,
                                device_id: device.device_id.clone(),
                                token_id: device.token_id,
                                response,
                            })
                            .await
                            .is_err()
                        {
                            continue;
                        }
                        self.router.remove_pending_response(&pending_response).await;
                        framed
                            .send(V5Packet::PubAck(V5PubAck::new(publish.pkid, None)))
                            .await?;
                        continue;
                    }
                    if publish.qos == V5QoS::ExactlyOnce
                        || !is_allowed_uplink(device, &topic)
                    {
                        return Err(TransportError::ForbiddenTopic);
                    }
                    self.uplink
                        .forward(
                            token,
                            TransportUplink {
                                device: device.clone(),
                                topic,
                                payload: publish.payload.to_vec(),
                                qos: v5_qos_to_v4(publish.qos),
                                received_at: Utc::now(),
                            },
                        )
                        .await?;
                    if publish.qos == V5QoS::AtLeastOnce {
                        framed
                            .send(V5Packet::PubAck(V5PubAck::new(publish.pkid, None)))
                            .await?;
                    }
                }
                Ok(V5Packet::PubAck(ack)) => {
                    if let Some(published) = pending.remove(&ack.pkid) {
                        let _ = published.send(());
                    }
                }
                Ok(V5Packet::PingReq(_)) => {
                    framed.send(V5Packet::PingResp(V5PingResp)).await?;
                }
                Ok(V5Packet::Disconnect(_)) | Err(TransportError::ConnectionClosed) => break,
                Ok(_) => return Err(TransportError::UnsupportedPacket),
                    Err(error) => return Err(error),
                }
            },
            command = receive_command(&mut commands), if commands.is_some() => {
                let Some(mut command) = command else {
                    break;
                };
                let Some(_delivery_lease) = command.acquire_delivery_lease().await else {
                    continue;
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
                let mut publish = rumqttc::v5::mqttbytes::v5::Publish::new(
                    topic,
                    V5QoS::AtLeastOnce,
                    payload,
                    None,
                );
                publish.pkid = packet_id;
                if let Some(published) = command.take_published_acknowledgement() {
                    pending.insert(packet_id, published);
                }
                framed.send(V5Packet::Publish(publish)).await?;
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

async fn next_v5_packet<S>(framed: &mut Framed<S, V5Codec>) -> Result<V5Packet, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .next()
        .await
        .ok_or(TransportError::ConnectionClosed)?
        .map_err(TransportError::ProtocolV5)
}

fn v5_qos_to_v4(qos: V5QoS) -> QoS {
    match qos {
        V5QoS::AtMostOnce => QoS::AtMostOnce,
        V5QoS::AtLeastOnce => QoS::AtLeastOnce,
        V5QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
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
    Uuid::parse_str(command_id).ok()
}
