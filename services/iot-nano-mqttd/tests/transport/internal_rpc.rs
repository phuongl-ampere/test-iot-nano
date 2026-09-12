use std::{future::Future, pin::Pin, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::{Duration, Utc};
use iot_core::RpcRequest;
use iot_nano_mqttd::{
    AuthenticatedDevice, DeviceAuthenticator, MqttdDeviceTransport, SessionRegistration,
    TransportAuthRequest, TransportError, TransportUplink, UplinkForwarder,
};
use tokio::sync::Notify;
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Clone)]
struct NoopAuthenticator;

impl DeviceAuthenticator for NoopAuthenticator {
    fn authenticate(
        &self,
        _request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        Box::pin(async { Err(TransportError::Unauthorized) })
    }
}

#[derive(Clone)]
struct RevokedAuthenticator;

impl DeviceAuthenticator for RevokedAuthenticator {
    fn authenticate(
        &self,
        _request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        Box::pin(async { Err(TransportError::Unauthorized) })
    }

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Err(TransportError::Unauthorized) })
    }
}

#[derive(Clone)]
struct BlockingAuthorizer {
    authorization_started: Arc<Notify>,
    continue_authorization: Arc<Notify>,
}

impl DeviceAuthenticator for BlockingAuthorizer {
    fn authenticate(
        &self,
        _request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        Box::pin(async { Err(TransportError::Unauthorized) })
    }

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        let authorization_started = Arc::clone(&self.authorization_started);
        let continue_authorization = Arc::clone(&self.continue_authorization);
        Box::pin(async move {
            authorization_started.notify_one();
            continue_authorization.notified().await;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct NoopUplink;

impl UplinkForwarder for NoopUplink {
    fn forward(
        &self,
        _token: &str,
        _message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn internal_rpc_publish_waits_for_the_target_device_puback() {
    let transport = MqttdDeviceTransport::new(NoopAuthenticator, NoopUplink);
    let router = transport.router();
    let mut session = router
        .register(SessionRegistration {
            token_id: Uuid::now_v7(),
            device_id: "device-a".to_owned(),
            client_id: "client-a".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let app = transport.internal_router(
        "api-secret-32-characters-minimum",
        "transport-secret-32-characters-minimum",
    );
    let issued_at = Utc::now();
    let rpc = RpcRequest::new(
        Uuid::now_v7(),
        "sample_now",
        serde_json::json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/rpc/publish")
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            "x-iot-nano-core-mqttd-secret",
            "transport-secret-32-characters-minimum",
        )
        .body(Body::from(
            serde_json::json!({
                "device_id": "device-a",
                "id": rpc.id,
                "method": rpc.method,
                "params": rpc.params,
                "issued_at": rpc.issued_at,
                "expires_at": rpc.expires_at,
            })
            .to_string(),
        ))
        .unwrap();

    let response = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
    let command = session.recv().await.unwrap();
    command.acknowledge_published().unwrap();

    assert_eq!(response.await.unwrap().status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn internal_rpc_publish_rechecks_the_active_token_before_delivery() {
    let transport = MqttdDeviceTransport::new(RevokedAuthenticator, NoopUplink);
    let router = transport.router();
    let mut session = router
        .register(SessionRegistration {
            token_id: Uuid::now_v7(),
            device_id: "device-a".to_owned(),
            client_id: "client-a".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let app = transport.internal_router(
        "api-secret-32-characters-minimum",
        "transport-secret-32-characters-minimum",
    );
    let issued_at = Utc::now();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/rpc/publish")
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            "x-iot-nano-core-mqttd-secret",
            "transport-secret-32-characters-minimum",
        )
        .body(Body::from(
            serde_json::json!({
                "device_id": "device-a",
                "id": Uuid::now_v7(),
                "method": "reboot",
                "params": {},
                "issued_at": issued_at,
                "expires_at": issued_at + Duration::seconds(30),
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(session.try_recv().is_err());
}

#[tokio::test]
async fn internal_session_revoke_removes_only_the_matching_token_session() {
    let transport = MqttdDeviceTransport::new(NoopAuthenticator, NoopUplink);
    let router = transport.router();
    let token_id = Uuid::now_v7();
    router
        .register(SessionRegistration {
            token_id,
            device_id: "device-a".to_owned(),
            client_id: "client-a".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let app = transport.internal_router(
        "api-secret-32-characters-minimum",
        "transport-secret-32-characters-minimum",
    );

    let wrong_token = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/sessions/revoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    "x-iot-nano-api-mqttd-secret",
                    "api-secret-32-characters-minimum",
                )
                .body(Body::from(
                    serde_json::json!({
                        "device_id": "device-a",
                        "token_id": Uuid::now_v7(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_token.status(), StatusCode::NO_CONTENT);
    assert!(router.active_snapshot("device-a").await.is_some());

    let revoked = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/sessions/revoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    "x-iot-nano-api-mqttd-secret",
                    "api-secret-32-characters-minimum",
                )
                .body(Body::from(
                    serde_json::json!({
                        "device_id": "device-a",
                        "token_id": token_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
    assert!(router.active_snapshot("device-a").await.is_none());
}

#[tokio::test]
async fn replacement_session_cannot_receive_a_command_authorized_for_the_old_session() {
    let authorization_started = Arc::new(Notify::new());
    let continue_authorization = Arc::new(Notify::new());
    let transport = MqttdDeviceTransport::new(
        BlockingAuthorizer {
            authorization_started: Arc::clone(&authorization_started),
            continue_authorization: Arc::clone(&continue_authorization),
        },
        NoopUplink,
    );
    let router = transport.router();
    let mut old_session = router
        .register(SessionRegistration {
            token_id: Uuid::now_v7(),
            device_id: "device-a".to_owned(),
            client_id: "client-old".to_owned(),
            connection_id: "connection-old".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let app = transport.internal_router(
        "api-secret-32-characters-minimum",
        "transport-secret-32-characters-minimum",
    );
    let issued_at = Utc::now();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/rpc/publish")
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            "x-iot-nano-core-mqttd-secret",
            "transport-secret-32-characters-minimum",
        )
        .body(Body::from(
            serde_json::json!({
                "device_id": "device-a",
                "id": Uuid::now_v7(),
                "method": "reboot",
                "params": {},
                "issued_at": issued_at,
                "expires_at": issued_at + Duration::seconds(30),
            })
            .to_string(),
        ))
        .unwrap();

    let mut response = tokio::spawn(async move { app.oneshot(request).await.unwrap() });
    authorization_started.notified().await;

    let mut replacement_session = router
        .register(SessionRegistration {
            token_id: Uuid::now_v7(),
            device_id: "device-a".to_owned(),
            client_id: "client-new".to_owned(),
            connection_id: "connection-new".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    continue_authorization.notify_one();

    tokio::select! {
        response = &mut response => {
            assert_eq!(
                response.unwrap().status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        command = replacement_session.recv() => {
            command
                .expect("a replacement session must not receive an old authorization's command")
                .acknowledge_published()
                .unwrap();
            assert_eq!(
                response.await.unwrap().status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
    }
    assert!(old_session.try_recv().is_err());
    assert!(replacement_session.try_recv().is_err());
}
