use std::sync::Arc;

use iot_core::{
    DatabaseStorage, StorageConfiguration, device_token_prefix, generate_device_token,
    hash_device_token,
};
use iot_nano_monolith::PlatformDeviceAuthorization;
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, TransportAuthRequest,
};
use iot_storage::PlatformStore;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

async fn fixture() -> (
    tempfile::TempDir,
    Arc<PlatformStore>,
    String,
    String,
    String,
    Uuid,
) {
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
        "INSERT INTO devices (device_id, is_gateway, gateway_device_id)
         VALUES ('direct', 0, NULL), ('gateway', 1, NULL), ('child', 0, 'gateway')",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let direct = generate_device_token();
    let gateway = generate_device_token();
    let revoked_gateway = generate_device_token();
    let direct_id = Uuid::now_v7();
    let gateway_id = Uuid::now_v7();
    for (id, device_id, value) in [
        (direct_id, "direct", direct.as_str()),
        (gateway_id, "gateway", gateway.as_str()),
    ] {
        sqlx::query(
            "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
             VALUES (?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(device_id)
        .bind(device_token_prefix(value).unwrap())
        .bind(hash_device_token(value).unwrap())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO device_tokens
            (id, device_id, token_prefix, token_hash, revoked_at)
         VALUES (?, 'gateway', ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(device_token_prefix(&revoked_gateway).unwrap())
    .bind(hash_device_token(&revoked_gateway).unwrap())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (
        directory,
        store,
        direct,
        gateway,
        revoked_gateway,
        gateway_id,
    )
}

fn auth_request(password: String) -> TransportAuthRequest {
    TransportAuthRequest {
        client_id: "device-client".to_owned(),
        username: "iotd_device_token".to_owned(),
        password,
    }
}

#[tokio::test]
async fn monolith_device_authorization_uses_storage_without_http_and_preserves_gateway_request() {
    let (_directory, store, direct_token, gateway_token, _revoked_gateway, gateway_id) =
        fixture().await;
    let adapter = PlatformDeviceAuthorization::new(store);

    let direct = adapter
        .authenticate(auth_request(direct_token))
        .await
        .unwrap();
    assert_eq!(direct.device_id, "direct");
    adapter.authorize_session(direct.clone()).await.unwrap();

    let gateway = adapter
        .authenticate(auth_request(gateway_token))
        .await
        .unwrap();
    adapter.authorize_session(gateway.clone()).await.unwrap();
    let request = GatewayAuthorizationRequest {
        gateway_device_id: "gateway".to_owned(),
        token_id: gateway_id,
        child_device_id: Some("child".to_owned()),
        topic: "v1/gateways/me/telemetry".to_owned(),
        event_kind: "child_telemetry".to_owned(),
    };
    assert_eq!(
        adapter
            .authorize_gateway_uplink(request.clone())
            .await
            .unwrap(),
        GatewayAuthorization {
            gateway_device_id: request.gateway_device_id.clone(),
            token_id: request.token_id,
            child_device_id: request.child_device_id.clone(),
            topic: request.topic.clone(),
            event_kind: request.event_kind.clone(),
        }
    );
    let no_child_request = GatewayAuthorizationRequest {
        child_device_id: None,
        ..request
    };
    assert_eq!(
        adapter
            .authorize_gateway_uplink(no_child_request.clone())
            .await
            .unwrap(),
        GatewayAuthorization {
            gateway_device_id: no_child_request.gateway_device_id,
            token_id: no_child_request.token_id,
            child_device_id: None,
            topic: no_child_request.topic,
            event_kind: no_child_request.event_kind,
        }
    );
    assert!(gateway.is_gateway);
}

#[tokio::test]
async fn monolith_device_authorization_maps_denials_and_storage_failures() {
    let (_directory, store, direct_token, _gateway_token, revoked_gateway, gateway_id) =
        fixture().await;
    let adapter = PlatformDeviceAuthorization::new(store);
    assert!(matches!(
        adapter
            .authenticate(TransportAuthRequest {
                username: "not-device".to_owned(),
                ..auth_request(direct_token)
            })
            .await,
        Err(AuthorizationError::Denied)
    ));
    assert!(matches!(
        adapter.authenticate(auth_request(revoked_gateway)).await,
        Err(AuthorizationError::Denied)
    ));
    assert!(matches!(
        adapter
            .authorize_session(AuthenticatedDevice {
                token_id: gateway_id,
                device_id: "child".to_owned(),
                is_gateway: false,
            })
            .await,
        Err(AuthorizationError::Denied)
    ));
    assert!(matches!(
        adapter
            .authorize_gateway_uplink(GatewayAuthorizationRequest {
                gateway_device_id: "wrong".to_owned(),
                token_id: gateway_id,
                child_device_id: None,
                topic: "topic".to_owned(),
                event_kind: "event".to_owned(),
            })
            .await,
        Err(AuthorizationError::Denied)
    ));
    let unavailable = PlatformDeviceAuthorization::new(Arc::new(PlatformStore::Timescale(
        PgPoolOptions::new()
            .connect_lazy("postgres://127.0.0.1:1/iot_nano")
            .unwrap(),
    )));
    assert!(matches!(
        unavailable
            .authorize_session(AuthenticatedDevice {
                token_id: gateway_id,
                device_id: "direct".to_owned(),
                is_gateway: false,
            })
            .await,
        Err(AuthorizationError::Unavailable(reason)) if reason == "platform storage unavailable"
    ));
}
