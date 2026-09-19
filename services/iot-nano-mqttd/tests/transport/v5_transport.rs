use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{Duration, Utc};
use futures_util::{SinkExt, StreamExt};
use iot_nano_foundation::{RpcMode, RpcRequest};
use iot_nano_mqttd::{
    AuthenticatedDevice, DeviceAuthenticator, MqttdDeviceTransport, RpcResponseForwarder,
    TransportAuthRequest, TransportError, TransportRpcResponse, TransportUplink, UplinkForwarder,
};
use rumqttc::v5::mqttbytes::{
    QoS,
    v5::{
        Codec, Connect, ConnectReturnCode, Filter, Login, Packet, PubAck, Publish, Subscribe,
        SubscribeReasonCode,
    },
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
            if request.username != "iotd_v5_device_token" || !request.password.is_empty() {
                return Err(TransportError::Unauthorized);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::now_v7(),
                tenant_id: TEST_TENANT_ID,
                device_id: "v5-device".to_owned(),
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
            if request.username != "iotd_v5_gateway_token" || !request.password.is_empty() {
                return Err(TransportError::Unauthorized);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::now_v7(),
                tenant_id: TEST_TENANT_ID,
                device_id: "v5-gateway".to_owned(),
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

#[tokio::test]
async fn token_authenticated_mqtt5_device_forwards_qos1_telemetry() {
    let uplink = RecordedUplink::default();
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, uplink.clone());
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_v5_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: Some(1024 * 1024),
            max_outgoing_size: Some(1024 * 1024),
        },
    );

    client
        .send(Packet::Connect(
            Connect {
                keep_alive: 60,
                client_id: "v5-device-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(Login::new("iotd_v5_device_token", "")),
        ))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));

    let mut telemetry = Publish::new(
        "v1/devices/me/telemetry",
        QoS::AtLeastOnce,
        br#"{"temperature_c":42}"#.as_slice(),
        None,
    );
    telemetry.pkid = 7;
    client.send(Packet::Publish(telemetry)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::PubAck(PubAck { pkid: 7, .. })
    ));
    assert_eq!(uplink.messages.lock().await.len(), 1);

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn token_authenticated_mqtt5_device_subscribes_to_virtual_rpc() {
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, RecordedUplink::default());
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_v5_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: Some(1024 * 1024),
            max_outgoing_size: Some(1024 * 1024),
        },
    );

    client
        .send(Packet::Connect(
            Connect {
                keep_alive: 60,
                client_id: "v5-rpc-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(Login::new("iotd_v5_device_token", "")),
        ))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));

    let mut subscribe = Subscribe::new(
        Filter::new("v1/devices/me/rpc/request/+", QoS::AtLeastOnce),
        None,
    );
    subscribe.pkid = 8;
    client.send(Packet::Subscribe(subscribe)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack)
            if ack.pkid == 8
                && matches!(
                    ack.return_codes.as_slice(),
                    [SubscribeReasonCode::Success(QoS::AtLeastOnce)]
                )
    ));

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn mqtt5_virtual_rpc_command_waits_for_device_puback() {
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, RecordedUplink::default());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_v5_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: Some(1024 * 1024),
            max_outgoing_size: Some(1024 * 1024),
        },
    );

    client
        .send(Packet::Connect(
            Connect {
                keep_alive: 60,
                client_id: "v5-rpc-command-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(Login::new("iotd_v5_device_token", "")),
        ))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::ConnAck(ack) if ack.code == ConnectReturnCode::Success
    ));
    let mut subscribe = Subscribe::new(
        Filter::new("v1/devices/me/rpc/request/+", QoS::AtLeastOnce),
        None,
    );
    subscribe.pkid = 9;
    client.send(Packet::Subscribe(subscribe)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::SubAck(ack) if ack.pkid == 9
    ));

    let issued_at = Utc::now();
    let request = RpcRequest::new(
        Uuid::now_v7(),
        "relay_on",
        serde_json::json!({"channel": 1}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();
    let publish = tokio::spawn(async move {
        router
            .publish_to_device(TEST_TENANT_ID, "v5-device", request)
            .await
    });
    let command = client.next().await.unwrap().unwrap();
    let packet_id = match command {
        Packet::Publish(publish) => {
            assert_eq!(publish.qos, QoS::AtLeastOnce);
            assert!(
                String::from_utf8_lossy(&publish.topic).starts_with("v1/devices/me/rpc/request/")
            );
            publish.pkid
        }
        packet => panic!("expected MQTT5 RPC publish, got {packet:?}"),
    };
    assert!(!publish.is_finished());
    client
        .send(Packet::PubAck(PubAck::new(packet_id, None)))
        .await
        .unwrap();
    assert!(publish.await.unwrap().is_ok());

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn mqtt5_two_way_rpc_response_requires_matching_pending_command() {
    let responses = RecordedRpcResponses::default();
    let transport = MqttdDeviceTransport::new(StaticAuthenticator, RecordedUplink::default())
        .with_rpc_response_forwarder(responses.clone());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_v5_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: Some(1024 * 1024),
            max_outgoing_size: Some(1024 * 1024),
        },
    );

    client
        .send(Packet::Connect(
            Connect {
                keep_alive: 60,
                client_id: "v5-two-way-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(Login::new("iotd_v5_device_token", "")),
        ))
        .await
        .unwrap();
    let _ = client.next().await.unwrap().unwrap();
    let mut subscribe = Subscribe::new(
        Filter::new("v1/devices/me/rpc/request/+", QoS::AtLeastOnce),
        None,
    );
    subscribe.pkid = 10;
    client.send(Packet::Subscribe(subscribe)).await.unwrap();
    let _ = client.next().await.unwrap().unwrap();

    let issued_at = Utc::now();
    let command_id = Uuid::now_v7();
    let request = RpcRequest::with_mode(
        command_id,
        "set_mode",
        serde_json::json!({"mode": "auto"}),
        issued_at,
        issued_at + Duration::seconds(30),
        RpcMode::TwoWay,
    )
    .unwrap();
    let publish = tokio::spawn(async move {
        router
            .publish_to_device(TEST_TENANT_ID, "v5-device", request)
            .await
    });
    let packet_id = match client.next().await.unwrap().unwrap() {
        Packet::Publish(publish) => publish.pkid,
        packet => panic!("expected MQTT5 RPC publish, got {packet:?}"),
    };
    client
        .send(Packet::PubAck(PubAck::new(packet_id, None)))
        .await
        .unwrap();
    assert!(publish.await.unwrap().is_ok());

    let mut response = Publish::new(
        format!("v1/devices/me/rpc/response/{command_id}"),
        QoS::AtLeastOnce,
        br#"{"ok":true}"#.as_slice(),
        None,
    );
    response.pkid = 11;
    client.send(Packet::Publish(response)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Packet::PubAck(PubAck { pkid: 11, .. })
    ));
    assert_eq!(responses.responses.lock().await.len(), 1);

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn mqtt5_gateway_receives_child_command_on_gateway_topic() {
    let transport =
        MqttdDeviceTransport::new(StaticGatewayAuthenticator, RecordedUplink::default());
    let router = transport.router();
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_v5_connection(server).await });
    let mut client = Framed::new(
        client,
        Codec {
            max_incoming_size: Some(1024 * 1024),
            max_outgoing_size: Some(1024 * 1024),
        },
    );

    client
        .send(Packet::Connect(
            Connect {
                keep_alive: 60,
                client_id: "v5-gateway-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(Login::new("iotd_v5_gateway_token", "")),
        ))
        .await
        .unwrap();
    let _ = client.next().await.unwrap().unwrap();
    let mut subscribe = Subscribe::new(
        Filter::new("v1/gateways/me/rpc/request/+", QoS::AtLeastOnce),
        None,
    );
    subscribe.pkid = 12;
    client.send(Packet::Subscribe(subscribe)).await.unwrap();
    let _ = client.next().await.unwrap().unwrap();

    let issued_at = Utc::now();
    let request = RpcRequest::new(
        Uuid::now_v7(),
        "child_read",
        serde_json::json!({"child_device_id": "child-a"}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();
    let publish = tokio::spawn(async move {
        router
            .publish_to_device(TEST_TENANT_ID, "v5-gateway", request)
            .await
    });
    let packet_id = match client.next().await.unwrap().unwrap() {
        Packet::Publish(publish) => {
            assert!(
                String::from_utf8_lossy(&publish.topic).starts_with("v1/gateways/me/rpc/request/")
            );
            publish.pkid
        }
        packet => panic!("expected MQTT5 gateway RPC publish, got {packet:?}"),
    };
    client
        .send(Packet::PubAck(PubAck::new(packet_id, None)))
        .await
        .unwrap();
    assert!(publish.await.unwrap().is_ok());

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}
