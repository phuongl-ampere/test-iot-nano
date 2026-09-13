use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use iot_core::RpcMode;
use iot_nano_monolith::PlatformCommandTransport;
use iot_nano_mqttd::{RpcSessionRouter, SessionRegistration};
use iot_nano_core::{
    CommandTransport, CommandTransportError, TransportRpcPublishRequest,
};
use serde_json::json;
use uuid::Uuid;

fn registration(device_id: &str, connection_id: &str) -> SessionRegistration {
    SessionRegistration {
        token_id: Uuid::now_v7(),
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
    let transport = PlatformCommandTransport::new(router.clone());
    let publish = tokio::spawn(async move { transport.publish(request("device-a")).await });

    let command = session.recv().await.unwrap();
    assert_eq!(command.request.method, "device_read");
    assert!(!publish.is_finished());
    command.acknowledge_published().unwrap();
    assert_eq!(publish.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn offline_device_maps_to_no_active_session() {
    let transport = PlatformCommandTransport::new(RpcSessionRouter::default());

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
    let transport = PlatformCommandTransport::new(router);
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
    let transport = PlatformCommandTransport::new(router);
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
