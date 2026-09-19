use chrono::{Duration, Utc};
use iot_nano_foundation::RpcRequest;
use iot_nano_mqttd::{RpcSessionRouter, SessionRegistration};
use serde_json::json;
use uuid::Uuid;

const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

fn registration(device_id: &str, connection_id: &str) -> SessionRegistration {
    SessionRegistration {
        token_id: Uuid::now_v7(),
        tenant_id: TEST_TENANT_ID,
        device_id: device_id.to_owned(),
        client_id: format!("client-{device_id}"),
        connection_id: connection_id.to_owned(),
        is_gateway: false,
        connected_at: Utc::now(),
    }
}

fn request(method: &str) -> RpcRequest {
    let issued_at = Utc::now();
    RpcRequest::new(
        Uuid::now_v7(),
        method,
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap()
}

#[tokio::test]
async fn routes_a_virtual_rpc_to_only_the_target_session() {
    let router = RpcSessionRouter::default();
    let mut device_a = router
        .register(registration("device-a", "connection-a"))
        .await;
    let mut device_b = router
        .register(registration("device-b", "connection-b"))
        .await;

    let router_for_publish = router.clone();
    let publish = tokio::spawn(async move {
        router_for_publish
            .publish_to_device(TEST_TENANT_ID, "device-a", request("sample_now"))
            .await
    });

    let command = device_a.recv().await.unwrap();
    assert_eq!(command.request.method, "sample_now");
    assert!(device_b.try_recv().is_err());
    command.acknowledge_published().unwrap();
    assert!(publish.await.unwrap().is_ok());
}

#[tokio::test]
async fn a_new_session_replaces_the_old_device_connection() {
    let router = RpcSessionRouter::default();
    let mut old = router
        .register(registration("device-a", "connection-old"))
        .await;
    let mut current = router
        .register(registration("device-a", "connection-new"))
        .await;

    let router_for_publish = router.clone();
    let publish = tokio::spawn(async move {
        router_for_publish
            .publish_to_device(TEST_TENANT_ID, "device-a", request("reboot"))
            .await
    });

    assert!(old.try_recv().is_err());
    let command = current.recv().await.unwrap();
    assert_eq!(command.request.method, "reboot");
    command.acknowledge_published().unwrap();
    assert!(publish.await.unwrap().is_ok());
}

#[tokio::test]
async fn command_publication_waits_for_the_device_puback() {
    let router = RpcSessionRouter::default();
    let mut device = router
        .register(registration("device-a", "connection-a"))
        .await;
    let router_for_publish = router.clone();

    let publish = tokio::spawn(async move {
        router_for_publish
            .publish_to_device(TEST_TENANT_ID, "device-a", request("sample_now"))
            .await
    });
    let command = device.recv().await.unwrap();

    assert!(!publish.is_finished());
    command.acknowledge_published().unwrap();
    assert!(publish.await.unwrap().is_ok());
}
