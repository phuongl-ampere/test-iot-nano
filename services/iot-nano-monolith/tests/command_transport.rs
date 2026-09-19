use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use iot_nano_core::{CommandTransport, CommandTransportError, TransportRpcPublishRequest};
use iot_nano_foundation::{
    DatabaseStorage, RpcMode, StorageConfiguration, device_token_prefix, generate_device_token,
    hash_device_token,
};
use iot_nano_monolith::{PlatformCommandTransport, PlatformDeviceAuthorization};
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, RpcSessionRouter, SessionRegistration, TransportAuthRequest,
};
use iot_storage::PlatformStore;
use serde_json::json;
use uuid::Uuid;

const TENANT_ID: Uuid = Uuid::from_u128(1);

#[derive(Default)]
struct AllowingAuthorization;

impl DeviceAuthorizationPort for AllowingAuthorization {
    fn authenticate(
        &self,
        _request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async {
            Ok(AuthenticatedDevice {
                token_id: Uuid::now_v7(),
                tenant_id: TENANT_ID,
                device_id: "device-a".to_owned(),
                is_gateway: false,
            })
        })
    }

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn authorize_gateway_uplink(
        &self,
        _request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async { Err(AuthorizationError::Denied) })
    }
}

fn allowing_authorization() -> Arc<dyn DeviceAuthorizationPort> {
    Arc::new(AllowingAuthorization)
}

fn registration(device_id: &str, connection_id: &str) -> SessionRegistration {
    SessionRegistration {
        token_id: Uuid::now_v7(),
        tenant_id: TENANT_ID,
        device_id: device_id.to_owned(),
        client_id: format!("client-{device_id}"),
        connection_id: connection_id.to_owned(),
        is_gateway: false,
        connected_at: Utc::now(),
    }
}

fn request(device_id: &str) -> TransportRpcPublishRequest {
    let issued_at = Utc::now();
    TransportRpcPublishRequest {
        tenant_id: TENANT_ID,
        device_id: device_id.to_owned(),
        id: Uuid::now_v7(),
        method: "device_read".to_owned(),
        params: json!({"channel": "temperature"}),
        mode: RpcMode::TwoWay,
        issued_at,
        expires_at: issued_at + ChronoDuration::seconds(30),
    }
}

#[tokio::test]
async fn publish_succeeds_only_after_the_active_session_acknowledges() {
    let router = RpcSessionRouter::default();
    let mut session = router
        .register(registration("device-a", "connection-a"))
        .await;
    let transport = PlatformCommandTransport::new(router.clone(), allowing_authorization());
    let publish = tokio::spawn(async move { transport.publish(request("device-a")).await });

    let command = session.recv().await.unwrap();
    assert_eq!(command.request.method, "device_read");
    assert!(!publish.is_finished());
    command.acknowledge_published().unwrap();
    assert_eq!(publish.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn offline_device_maps_to_no_active_session() {
    let transport =
        PlatformCommandTransport::new(RpcSessionRouter::default(), allowing_authorization());

    assert_eq!(
        transport.publish(request("offline")).await,
        Err(CommandTransportError::NoActiveSession)
    );
}

#[tokio::test]
async fn stale_session_cannot_satisfy_publication_for_the_replaced_session() {
    let router = RpcSessionRouter::default();
    let mut stale = router
        .register(registration("device-a", "connection-old"))
        .await;
    let mut active = router
        .register(registration("device-a", "connection-new"))
        .await;
    let transport = PlatformCommandTransport::new(router, allowing_authorization());
    let publish = tokio::spawn(async move { transport.publish(request("device-a")).await });

    assert!(stale.try_recv().is_err());
    let command = active.recv().await.unwrap();
    command.acknowledge_published().unwrap();
    assert_eq!(publish.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn expired_request_is_rejected_without_publishing() {
    let router = RpcSessionRouter::default();
    let mut session = router
        .register(registration("device-a", "connection-a"))
        .await;
    let transport = PlatformCommandTransport::new(router, allowing_authorization());
    let now = Utc::now();
    let mut expired = request("device-a");
    expired.issued_at = now - ChronoDuration::minutes(1);
    expired.expires_at = now - ChronoDuration::seconds(1);

    assert_eq!(
        transport.publish(expired).await,
        Err(CommandTransportError::Unavailable(
            "command request has expired".to_owned()
        ))
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(session.try_recv().is_err());
}

#[tokio::test]
async fn storage_revocation_blocks_an_active_session_before_command_publication() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("platform.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'command-transport', 'active', '{}')",
    )
    .bind(TENANT_ID.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('device-a', ?, 0, NULL)",
    )
    .bind(TENANT_ID.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let token = generate_device_token();
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'device-a', ?, ?)",
    )
    .bind(token_id.to_string())
    .bind(device_token_prefix(&token).unwrap())
    .bind(hash_device_token(&token).unwrap())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let authorization: Arc<dyn DeviceAuthorizationPort> =
        Arc::new(PlatformDeviceAuthorization::new(store.clone()));
    let device = authorization
        .authenticate(TransportAuthRequest {
            client_id: "device-client".to_owned(),
            username: "iotd_device_token".to_owned(),
            password: token,
        })
        .await
        .unwrap();
    let router = RpcSessionRouter::default();
    let mut active_session = router
        .register(SessionRegistration {
            token_id: device.token_id,
            tenant_id: device.tenant_id,
            device_id: device.device_id.clone(),
            client_id: "device-client".to_owned(),
            connection_id: "active-connection".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let transport = PlatformCommandTransport::new(router, authorization);

    sqlx::query("UPDATE device_tokens SET revoked_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(token_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    assert_eq!(
        transport.publish(request("device-a")).await,
        Err(CommandTransportError::NoActiveSession)
    );
    assert!(active_session.try_recv().is_err());
}
