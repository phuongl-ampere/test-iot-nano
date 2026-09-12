use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use async_tungstenite::{
    client_async,
    tokio::connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use axum::{Router, http::HeaderMap, routing::post};
use futures_util::{SinkExt, StreamExt};
use iot_nano_mqttd as iot_mqttd;
use iot_nano_mqttd::{
    ListenerConfiguration, broker_config, start_broker, start_broker_with_timeout,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{BridgeConfig as CoreBridgeConfig, ConnectionSettings, Transport};
use rustls_pemfile::certs;
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};
use tokio_util::compat::TokioAsyncReadCompatExt;

#[test]
fn broker_configuration_exposes_plaintext_and_tls_mqtt_listeners() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let config = broker_config(&ListenerConfiguration {
        plaintext_address: "127.0.0.1:1883".parse::<SocketAddr>().unwrap(),
        tls_address: "127.0.0.1:8883".parse::<SocketAddr>().unwrap(),
        v311_backend_address: "127.0.0.1:18831".parse::<SocketAddr>().unwrap(),
        v5_backend_address: "127.0.0.1:18832".parse::<SocketAddr>().unwrap(),
        tls_cert_path: PathBuf::from(&certificate),
        tls_key_path: PathBuf::from(&key),
        websocket_address: Some("127.0.0.1:9001".parse::<SocketAddr>().unwrap()),
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    })
    .unwrap();

    let v311 = config.v4.unwrap();
    let v5 = config.v5.unwrap();
    assert_eq!(v311["v311"].listen, "127.0.0.1:18831".parse().unwrap());
    assert!(v311["v311"].tls.is_none());
    assert_eq!(v5["v5"].listen, "127.0.0.1:18832".parse().unwrap());
    assert!(v5["v5"].tls.is_none());
    let ws = config.ws.unwrap();
    assert_eq!(ws["ws-v311"].listen, "127.0.0.1:9001".parse().unwrap());
    assert!(ws["ws-v311"].tls.is_none());
}

#[tokio::test]
async fn websocket_listener_accepts_a_binary_mqtt311_connect() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let websocket_address = reserve_address().await;
    let broker = start_broker(ListenerConfiguration {
        plaintext_address: reserve_address().await,
        tls_address: reserve_address().await,
        v311_backend_address: reserve_address().await,
        v5_backend_address: reserve_address().await,
        tls_cert_path: certificate,
        tls_key_path: key,
        websocket_address: Some(websocket_address),
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap();

    let (mut socket, _) = connect_async(format!("ws://{websocket_address}"))
        .await
        .unwrap();
    socket
        .send(Message::Binary(vec![
            0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 4, 0x02, 0x00, 0x3c, 0x00, 0x02, b'i',
            b'd',
        ]))
        .await
        .unwrap();
    let packet = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(packet, Message::Binary(vec![0x20, 0x02, 0x00, 0x00]));
    drop(socket);
    drop(broker);
}

#[tokio::test]
async fn secure_websocket_listener_accepts_a_binary_mqtt311_connect() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::copy(fixtures.join("server.crt"), &certificate).unwrap();
    std::fs::copy(fixtures.join("server.key"), &key).unwrap();
    let websocket_address = reserve_address().await;
    let broker = start_broker(ListenerConfiguration {
        plaintext_address: reserve_address().await,
        tls_address: reserve_address().await,
        v311_backend_address: reserve_address().await,
        v5_backend_address: reserve_address().await,
        tls_cert_path: certificate.clone(),
        tls_key_path: key,
        websocket_address: Some(websocket_address),
        websocket_tls: true,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap();

    let tls_stream = tls_connect(websocket_address, &certificate).await;
    let request = format!("wss://localhost:{}/mqtt", websocket_address.port())
        .into_client_request()
        .unwrap();
    let (mut socket, _) = client_async(request, tls_stream.compat()).await.unwrap();
    socket
        .send(Message::Binary(vec![
            0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 4, 0x02, 0x00, 0x3c, 0x00, 0x02, b'i',
            b'd',
        ]))
        .await
        .unwrap();
    let packet = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(packet, Message::Binary(vec![0x20, 0x02, 0x00, 0x00]));
    drop(socket);
    drop(broker);
}

#[tokio::test]
async fn tcp_bridge_forwards_upstream_publish_to_a_local_subscriber() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let upstream_v4 = reserve_address().await;
    let local_v4 = reserve_address().await;
    let upstream = start_broker(listener_configuration(
        upstream_v4,
        reserve_address().await,
        &certificate,
        &key,
        None,
    ))
    .await
    .unwrap();
    let local = start_broker(listener_configuration(
        local_v4,
        reserve_address().await,
        &certificate,
        &key,
        Some(CoreBridgeConfig {
            name: "test-bridge".to_owned(),
            addr: upstream_v4.to_string(),
            qos: 1,
            sub_path: "bridge/#".to_owned(),
            reconnection_delay: 1,
            ping_delay: 30,
            connections: bridge_connection_settings(),
            transport: Transport::Tcp,
        }),
    ))
    .await
    .unwrap();

    let mut subscriber_options = MqttOptions::new(
        "bridge-subscriber",
        local_v4.ip().to_string(),
        local_v4.port(),
    );
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe("bridge/telemetry", QoS::AtLeastOnce)
        .await
        .unwrap();
    let receive = tokio::spawn(async move {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                return publish.payload;
            }
        }
    });

    let mut publisher_options = MqttOptions::new(
        "bridge-publisher",
        upstream_v4.ip().to_string(),
        upstream_v4.port(),
    );
    publisher_options.set_clean_session(true);
    let (publisher, mut publisher_events) = AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            let _ = publisher_events.poll().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher
        .publish("bridge/telemetry", QoS::AtLeastOnce, false, "forwarded")
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..], b"forwarded");
    publisher_task.abort();
    drop(local);
    drop(upstream);
}

#[tokio::test]
async fn republish_rule_forwards_source_payload_to_target_topic() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let v4 = reserve_address().await;
    let broker = start_broker(listener_configuration(
        v4,
        reserve_address().await,
        &certificate,
        &key,
        None,
    ))
    .await
    .unwrap();
    broker
        .spawn_republish_rule_worker(iot_mqttd::RuleConfig {
            name: "forward".into(),
            enabled: true,
            expression: String::new(),
            source_topic: "rules/source/#".into(),
            target_topic: "rules/target".into(),
        })
        .unwrap();

    let mut subscriber_options =
        MqttOptions::new("rule-subscriber", v4.ip().to_string(), v4.port());
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe("rules/target", QoS::AtLeastOnce)
        .await
        .unwrap();
    let receive = tokio::spawn(async move {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                return publish.payload;
            }
        }
    });

    let mut publisher_options = MqttOptions::new("rule-publisher", v4.ip().to_string(), v4.port());
    publisher_options.set_clean_session(true);
    let (publisher, mut publisher_events) = AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            let _ = publisher_events.poll().await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    publisher
        .publish("rules/source/one", QoS::AtLeastOnce, false, "republished")
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(3), receive)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..], b"republished");

    publisher_task.abort();
    drop(broker);
}

#[tokio::test]
async fn broker_startup_fails_promptly_when_an_internal_backend_is_occupied() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_address = occupied.local_addr().unwrap();
    let v5_address = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        start_broker_with_timeout(
            ListenerConfiguration {
                plaintext_address: "127.0.0.1:0".parse().unwrap(),
                tls_address: "127.0.0.1:0".parse().unwrap(),
                v311_backend_address: occupied_address,
                v5_backend_address: v5_address,
                tls_cert_path: certificate,
                tls_key_path: key,
                websocket_address: None,
                websocket_tls: false,
                bridge: None,
                max_connections: 100,
                max_payload_size: 1024,
                max_inflight_count: 10,
                token_authenticator: None,
                auth_handler: None,
                authorization_handler: None,
            },
            Duration::from_millis(100),
        ),
    )
    .await
    .expect("occupied backend must fail without waiting for the default timeout");

    assert!(result.is_err());
}

#[tokio::test]
async fn plaintext_listener_delivers_a_qos_one_publish_to_a_subscriber() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let _broker = start_broker(ListenerConfiguration {
        plaintext_address: "127.0.0.1:18983".parse().unwrap(),
        tls_address: "127.0.0.1:18984".parse().unwrap(),
        v311_backend_address: "127.0.0.1:18983".parse().unwrap(),
        v5_backend_address: "127.0.0.1:18984".parse().unwrap(),
        tls_cert_path: certificate,
        tls_key_path: key,
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut subscriber_options = MqttOptions::new("iot-mqttd-test-sub", "127.0.0.1", 18983);
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe("test/iot-mqttd", QoS::AtLeastOnce)
        .await
        .unwrap();
    let subscriber_task = tokio::spawn(async move {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                return publish.payload;
            }
        }
    });

    let mut publisher_options = MqttOptions::new("iot-mqttd-test-pub", "127.0.0.1", 18983);
    publisher_options.set_clean_session(true);
    let (publisher, mut publisher_events) = AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            publisher_events.poll().await.unwrap();
        }
    });
    publisher
        .publish("test/iot-mqttd", QoS::AtLeastOnce, false, "hello")
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(3), subscriber_task)
        .await
        .unwrap();
    publisher_task.abort();
    assert_eq!(&received.unwrap()[..], b"hello");
}

#[tokio::test]
async fn token_authenticator_uses_session_resolution_with_an_empty_password() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/internal/mqttd/session-resolution",
                post(|headers: HeaderMap, body: String| async move {
                    if headers
                        .get("x-iot-nano-mqttd-api-secret")
                        .is_some_and(|value| value == "test-transport-secret-must-have-32")
                        && body.contains("\"username\":\"iotd_token\"")
                        && body.contains("\"password\":\"\"")
                    {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::UNAUTHORIZED
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });

    let authenticator = iot_mqttd::HttpTokenAuthenticator::new(
        &format!("http://{address}"),
        "test-transport-secret-must-have-32",
    )
    .unwrap();
    assert!(
        authenticator
            .authenticate(
                "device-client".to_owned(),
                "iotd_token".to_owned(),
                String::new(),
            )
            .await
    );
    assert!(
        !authenticator
            .authenticate(
                "device-client".to_owned(),
                "iotd_token".to_owned(),
                "not-empty".to_owned(),
            )
            .await
    );
}

async fn reserve_address() -> SocketAddr {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn tls_connect(
    address: SocketAddr,
    certificate: &PathBuf,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let mut roots = RootCertStore::empty();
    let file = std::fs::File::open(certificate).unwrap();
    let mut reader = std::io::BufReader::new(file);
    for certificate in certs(&mut reader) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap()
}

fn listener_configuration(
    v4: SocketAddr,
    v5: SocketAddr,
    certificate: &PathBuf,
    key: &PathBuf,
    bridge: Option<CoreBridgeConfig>,
) -> ListenerConfiguration {
    ListenerConfiguration {
        plaintext_address: reserve_address_blocking(),
        tls_address: reserve_address_blocking(),
        v311_backend_address: v4,
        v5_backend_address: v5,
        tls_cert_path: certificate.clone(),
        tls_key_path: key.clone(),
        websocket_address: None,
        websocket_tls: false,
        bridge,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    }
}

fn bridge_connection_settings() -> ConnectionSettings {
    ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: 1024,
        max_inflight_count: 10,
        auth: None,
        external_auth: None,
        authorization_handler: None,
        dynamic_filters: true,
    }
}

fn reserve_address_blocking() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test]
async fn broker_listener_applies_platform_token_authentication_during_connect() {
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_address = api_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            api_listener,
            Router::new().route(
                "/internal/mqttd/session-resolution",
                post(|body: String| async move {
                    if body.contains("\"username\":\"iotd_allowed\"")
                        && body.contains("\"password\":\"\"")
                    {
                        (axum::http::StatusCode::OK, "{}")
                    } else {
                        (axum::http::StatusCode::UNAUTHORIZED, "{}")
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let token_authenticator = iot_mqttd::HttpTokenAuthenticator::new(
        &format!("http://{api_address}"),
        "test-transport-secret-must-have-32",
    )
    .unwrap();
    let _broker = start_broker(ListenerConfiguration {
        plaintext_address: "127.0.0.1:18987".parse().unwrap(),
        tls_address: "127.0.0.1:18988".parse().unwrap(),
        v311_backend_address: "127.0.0.1:18987".parse().unwrap(),
        v5_backend_address: "127.0.0.1:18988".parse().unwrap(),
        tls_cert_path: certificate,
        tls_key_path: key,
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        token_authenticator: Some(token_authenticator),
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut allowed_options = MqttOptions::new("iot-mqttd-auth-ok", "127.0.0.1", 18987);
    allowed_options.set_credentials("iotd_allowed", "");
    let (_, mut allowed_events) = AsyncClient::new(allowed_options, 10);
    let allowed = tokio::time::timeout(Duration::from_secs(3), allowed_events.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(allowed, Event::Incoming(Packet::ConnAck(_))));

    let mut rejected_options = MqttOptions::new("iot-mqttd-auth-rejected", "127.0.0.1", 18987);
    rejected_options.set_credentials("iotd_allowed", "not-empty");
    let (_, mut rejected_events) = AsyncClient::new(rejected_options, 10);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), rejected_events.poll())
            .await
            .unwrap()
            .is_err()
    );
}
