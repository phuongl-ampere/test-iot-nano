use std::{future::Future, pin::Pin, sync::Arc};

use chrono::Utc;
use iot_core::RpcRequest;
use iot_nano_core::{
    CommandTransport, CommandTransportError, TransportRpcPublishRequest,
};
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, CommandResponseError, CommandResponsePort,
    DeviceAuthorizationPort, GatewayAuthorization, GatewayAuthorizationRequest,
    RpcSessionRouter, SessionError, TransportAuthRequest, TransportRpcResponse,
};
use iot_storage::{
    DeviceAuthorizationRepository, IdentityRepository, PlatformStore, PlatformStoreError,
};

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";
const STORAGE_UNAVAILABLE: &str = "platform storage unavailable";
const COMMAND_PUBLICATION_UNAVAILABLE: &str = "command publication unavailable";
const COMMAND_REQUEST_EXPIRED: &str = "command request has expired";

pub struct PlatformCommandTransport {
    router: RpcSessionRouter,
}

impl PlatformCommandTransport {
    pub fn new(router: RpcSessionRouter) -> Self {
        Self { router }
    }

    async fn publish_request(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Result<(), CommandTransportError> {
        if request.expires_at <= Utc::now() {
            return Err(CommandTransportError::Unavailable(
                COMMAND_REQUEST_EXPIRED.to_owned(),
            ));
        }
        let device_id = request.device_id.clone();
        let rpc = RpcRequest::with_mode(
            request.id,
            request.method,
            request.params,
            request.issued_at,
            request.expires_at,
            request.mode,
        )
        .map_err(|_| {
            CommandTransportError::Configuration("request is not valid".to_owned())
        })?;
        self.router
            .publish_to_device(&device_id, rpc)
            .await
            .map_err(map_session_error)
    }
}

impl CommandTransport for PlatformCommandTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(self.publish_request(request))
    }
}

fn map_session_error(error: SessionError) -> CommandTransportError {
    match error {
        SessionError::DeviceOffline => CommandTransportError::NoActiveSession,
        SessionError::SessionUnavailable
        | SessionError::PublicationTimeout
        | SessionError::PublicationWaiterUnavailable
        | SessionError::AcknowledgementAlreadyConsumed => {
            CommandTransportError::Unavailable(COMMAND_PUBLICATION_UNAVAILABLE.to_owned())
        }
    }
}

pub struct PlatformCommandResponse {
    store: Arc<PlatformStore>,
}

impl PlatformCommandResponse {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }
}

impl CommandResponsePort for PlatformCommandResponse {
    fn record_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let response_json = serde_json::to_string(&response.response)
                .map_err(|_| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?;
            store
                .mark_command_responded(
                    response.command_id,
                    &response.device_id,
                    response.token_id,
                    &response_json,
                    Utc::now(),
                )
                .await
                .map_err(|_| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?
                .ok_or_else(|| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?;
            Ok(())
        })
    }
}

pub struct PlatformDeviceAuthorization {
    store: Arc<PlatformStore>,
}

impl PlatformDeviceAuthorization {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }
}

impl DeviceAuthorizationPort for PlatformDeviceAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            if request.username != DEVICE_TOKEN_USERNAME {
                return Err(AuthorizationError::Denied);
            }
            IdentityRepository::resolve_active_device_token(store.as_ref(), &request.password)
                .await
                .map(|device| AuthenticatedDevice {
                    token_id: device.token_id,
                    device_id: device.device_id,
                    is_gateway: device.is_gateway,
                })
                .map_err(map_storage_error)
        })
    }

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            DeviceAuthorizationRepository::authorize_device_session(
                store.as_ref(),
                device.token_id,
                &device.device_id,
            )
            .await
            .map_err(map_storage_error)
        })
    }

    fn authorize_gateway_uplink(
        &self,
        request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            DeviceAuthorizationRepository::authorize_gateway_token(
                store.as_ref(),
                request.token_id,
                &request.gateway_device_id,
                request.child_device_id.as_deref(),
            )
            .await
            .map_err(map_storage_error)?;
            Ok(GatewayAuthorization {
                gateway_device_id: request.gateway_device_id,
                token_id: request.token_id,
                child_device_id: request.child_device_id,
                topic: request.topic,
                event_kind: request.event_kind,
            })
        })
    }
}

fn map_storage_error(error: PlatformStoreError) -> AuthorizationError {
    match error {
        PlatformStoreError::DeviceTokenDenied => AuthorizationError::Denied,
        _ => AuthorizationError::Unavailable(STORAGE_UNAVAILABLE.to_owned()),
    }
}
