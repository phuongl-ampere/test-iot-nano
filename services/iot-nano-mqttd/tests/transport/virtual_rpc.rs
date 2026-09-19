use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{Duration, Utc};
use futures_util::{SinkExt, StreamExt};
use iot_nano_mqttd::{
    AuthenticatedDevice, DeviceAuthenticator, MqttdDeviceTransport, RpcResponseForwarder,
    TransportAuthRequest, TransportError, TransportRpcResponse, TransportUplink, UplinkForwarder,
};
use rumqttc::{
    Connect, ConnectReturnCode, Packet, PubAck, Publish, QoS, Subscribe, SubscribeFilter,
    mqttbytes::v4::Codec,
};
use tokio::{io::duplex, sync::Mutex};
use tokio_util::codec::Framed;
use uuid::Uuid;

const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

#[derive(Clone)]
struct StaticAuthenticator;

impl DeviceAuthenticator for StaticAuthenticator {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        Box::pin(async move {
            if request.username != "iotd_test_device_token" || !request.password.is_empty() {
                return Err(TransportError::Unauthorized);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::now_v7(),
                tenant_id: TEST_TENANT_ID,
                device_id: "device-a".to_owned(),
                is_gateway: false,
            })
        })
    }
}

#[derive(Clone)]
struct StaticGatewayAuthenticator;

impl DeviceAuthenticator for StaticGatewayAuthenticator {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
    {
        Box::pin(async move {
            if request.username != "iotd_test_gateway_token" || !request.password.is_empty() {
                return Err(TransportError::Unauthorized);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::now_v7(),
                tenant_id: TEST_TENANT_ID,
                device_id: "gateway-a".to_owned(),
                is_gateway: true,
            })
        })
    }
}

#[derive(Clone, Default)]
struct RecordedUplink {
    messages: Arc<Mutex<Vec<TransportUplink>>>,
}

#[derive(Clone, Default)]
struct RecordedRpcResponses {
    responses: Arc<Mutex<Vec<TransportRpcResponse>>>,
}

impl RpcResponseForwarder for RecordedRpcResponses {
    fn forward_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async move {
            self.responses.lock().await.push(response);
            Ok(())
        })
    }
}

impl UplinkForwarder for RecordedUplink {
    fn forward(
        &self,
        _token: &str,
        message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async move {
            self.messages.lock().await.push(message);
            Ok(())
        })
    }
}

#[derive(Clone)]
struct FailingUplink;

impl UplinkForwarder for FailingUplink {
    fn forward(
        &self,
        _token: &str,
        _message: TransportUplink,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async {
            Err(TransportError::StreamAppendFailed(
                "injected failure".into(),
            ))
        })
    }
}

#[tokio::test]
async fn unregisters_a_subscribed_session_when_uplink_forwarding_fails() {
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, FailingUplink);
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: 1024 * 1024,
            max_outgoing_size: 1024 * 1024,
        },
    );

    let mut connect = Connect::new("esp-client-a");
    connect.set_login("iotd_test_device_token", "");
    client.send(Packet::Connect(connect)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));
    client
        .send(Packet::Subscribe(Subscribe {
            pkid: 7,
            filters: vec![SubscribeFilter {
                path: "v1/devices/me/rpc/request/+".to_owned(),
                qos: QoS::AtLeastOnce,
            }],
        }))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack) if ack.pkid == 7
    ));
    assert!(router.active_device("device-a").await.is_some());

    client
        .send(Packet::Publish(Publish::new(
            "v1/devices/me/telemetry",
            QoS::AtMostOnce,
            br#"{"schema_version":1}"#,
        )))
        .await
        .unwrap();

    assert!(matches!(
        server_task.await.unwrap(),
        Err(TransportError::StreamAppendFailed(message)) if message == "injected failure"
    ));
    assert!(router.active_device("device-a").await.is_none());
}

#[tokio::test]
async fn token_authenticated_device_receives_virtual_me_rpc_and_acknowledges_it() {
    let uplink = RecordedUplink::default();
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, uplink.clone());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: 1024 * 1024,
            max_outgoing_size: 1024 * 1024,
        },
    );

    let mut connect = Connect::new("esp-client-a");
    connect.set_login("iotd_test_device_token", "");
    client.send(Packet::Connect(connect)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));

    client
        .send(Packet::Subscribe(Subscribe {
            pkid: 7,
            filters: vec![SubscribeFilter {
                path: "v1/devices/me/rpc/request/+".to_owned(),
                qos: QoS::AtLeastOnce,
            }],
        }))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack) if ack.pkid == 7
    ));

    let command_id = Uuid::now_v7();
    let issued_at = Utc::now();
    let request = iot_nano_foundation::RpcRequest::new(
        command_id,
        "sample_now",
        serde_json::json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();
    let router_for_publish = router.clone();
    let publication = tokio::spawn(async move {
        router_for_publish
            .publish_to_device(TEST_TENANT_ID, "device-a", request)
            .await
    });
    let publish = match client.next().await.unwrap().unwrap() {
        Packet::Publish(publish) => publish,
        packet => panic!("expected RPC publish, got {packet:?}"),
    };

    assert_eq!(
        publish.topic,
        format!("v1/devices/me/rpc/request/{command_id}")
    );
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    client
        .send(Packet::PubAck(PubAck::new(publish.pkid)))
        .await
        .unwrap();
    assert!(publication.await.unwrap().is_ok());

    let mut telemetry = Publish::new(
        "v1/devices/me/telemetry",
        QoS::AtLeastOnce,
        br#"{"schema_version":1}"#,
    );
    telemetry.pkid = 8;
    client.send(Packet::Publish(telemetry)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::PubAck(ack) if ack.pkid == 8
    ));
    assert_eq!(uplink.messages.lock().await.len(), 1);

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn gateway_session_receives_child_command_on_the_gateway_virtual_topic() {
    let transport =
        MqttdDeviceTransport::new(StaticGatewayAuthenticator, RecordedUplink::default());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: 1024 * 1024,
            max_outgoing_size: 1024 * 1024,
        },
    );

    let mut connect = Connect::new("gateway-client-a");
    connect.set_login("iotd_test_gateway_token", "");
    client.send(Packet::Connect(connect)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));
    client
        .send(Packet::Subscribe(Subscribe {
            pkid: 17,
            filters: vec![SubscribeFilter {
                path: "v1/gateways/me/rpc/request/+".to_owned(),
                qos: QoS::AtLeastOnce,
            }],
        }))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack) if ack.pkid == 17
    ));

    let issued_at = Utc::now();
    let request = iot_nano_foundation::RpcRequest::new(
        Uuid::now_v7(),
        "gateway_child_rpc",
        serde_json::json!({
            "child_device_id": "child-a",
            "method": "sample_now",
            "params": {},
        }),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();
    let expected_id = request.id;
    let router_for_publish = router.clone();
    let publication = tokio::spawn(async move {
        router_for_publish
            .publish_to_device(TEST_TENANT_ID, "gateway-a", request)
            .await
    });
    let publish = match client.next().await.unwrap().unwrap() {
        Packet::Publish(publish) => publish,
        packet => panic!("expected gateway RPC publish, got {packet:?}"),
    };

    assert_eq!(
        publish.topic,
        format!("v1/gateways/me/rpc/request/{expected_id}")
    );
    let payload: serde_json::Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["method"], "gateway_child_rpc");
    assert_eq!(payload["params"]["child_device_id"], "child-a");
    client
        .send(Packet::PubAck(PubAck::new(publish.pkid)))
        .await
        .unwrap();
    assert!(publication.await.unwrap().is_ok());

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn two_way_response_is_forwarded_only_after_the_matching_virtual_rpc_is_published() {
    let responses = RecordedRpcResponses::default();
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, RecordedUplink::default())
        .with_rpc_response_forwarder(responses.clone());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: 1024 * 1024,
            max_outgoing_size: 1024 * 1024,
        },
    );

    let mut connect = Connect::new("esp-client-two-way");
    connect.set_login("iotd_test_device_token", "");
    client.send(Packet::Connect(connect)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));
    client
        .send(Packet::Subscribe(Subscribe {
            pkid: 18,
            filters: vec![SubscribeFilter {
                path: "v1/devices/me/rpc/request/+".to_owned(),
                qos: QoS::AtLeastOnce,
            }],
        }))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack) if ack.pkid == 18
    ));

    let issued_at = Utc::now();
    let request = iot_nano_foundation::RpcRequest::with_mode(
        Uuid::now_v7(),
        "sample_now",
        serde_json::json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
        iot_nano_foundation::RpcMode::TwoWay,
    )
    .unwrap();
    let command_id = request.id;
    let publication_router = router.clone();
    let publication = tokio::spawn(async move {
        publication_router
            .publish_to_device(TEST_TENANT_ID, "device-a", request)
            .await
    });
    let command = match client.next().await.unwrap().unwrap() {
        Packet::Publish(publish) => publish,
        packet => panic!("expected RPC publish, got {packet:?}"),
    };
    let command_payload: serde_json::Value = serde_json::from_slice(&command.payload).unwrap();
    assert_eq!(command_payload["mode"], "two_way");
    client
        .send(Packet::PubAck(PubAck::new(command.pkid)))
        .await
        .unwrap();
    assert!(publication.await.unwrap().is_ok());

    let mut response = Publish::new(
        format!("v1/devices/me/rpc/response/{command_id}"),
        QoS::AtLeastOnce,
        br#"{"ok":true,"sampled":true}"#,
    );
    response.pkid = 19;
    client.send(Packet::Publish(response)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::PubAck(ack) if ack.pkid == 19
    ));
    let forwarded = responses.responses.lock().await;
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0].command_id, command_id);
    assert_eq!(forwarded[0].tenant_id, TEST_TENANT_ID);
    assert_eq!(forwarded[0].device_id, "device-a");
    assert_eq!(
        forwarded[0].response,
        serde_json::json!({"ok":true,"sampled":true})
    );

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}
