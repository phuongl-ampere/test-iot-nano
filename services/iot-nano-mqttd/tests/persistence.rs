use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    sync::OnceLock,
    thread,
    time::Duration,
};

use iot_nano_mqttd as iot_mqttd;
use iot_nano_mqttd::{
    BrokerLifecycleHandle, BrokerStorage, ListenerConfiguration, MemoryStorage, MuxSettings,
    ProtocolBackends, RetentionPolicy, SqliteStorage, StoredInflight, StoredPublish, StoredSession,
    start_broker_with_storage, start_broker_with_storage_and_policy,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::InboundQos2CommitResult;
use rumqttd::{ConnectionEvents, OutboundQos2Phase, Tracker};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};

struct TestBroker {
    port: u16,
    v5_port: u16,
    handle: Option<BrokerLifecycleHandle>,
    _directory: tempfile::TempDir,
}

static BROKER_START_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn start_test_broker(database: &Path) -> TestBroker {
    let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(database).unwrap());
    start_test_broker_with_storage(storage).await
}

async fn start_test_broker_with_storage(storage: Arc<dyn BrokerStorage>) -> TestBroker {
    start_test_broker_with_storage_and_policy(storage, RetentionPolicy::default()).await
}

async fn start_test_broker_with_storage_and_policy(
    storage: Arc<dyn BrokerStorage>,
    policy: RetentionPolicy,
) -> TestBroker {
    let _guard = BROKER_START_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let port = reserve_port().await;
    let v5 = reserve_port().await;
    let handle = start_broker_with_storage_and_policy(
        ListenerConfiguration {
            plaintext_address: "127.0.0.1:0".parse().unwrap(),
            tls_address: "127.0.0.1:0".parse().unwrap(),
            v311_backend_address: SocketAddr::from(([127, 0, 0, 1], port)),
            v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5)),
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
        storage,
        policy,
    )
    .await
    .unwrap();
    TestBroker {
        port,
        v5_port: v5,
        handle: Some(handle),
        _directory: directory,
    }
}

impl TestBroker {
    fn signal_shutdown(&self) {
        self.handle
            .as_ref()
            .expect("broker lifecycle handle is available")
            .shutdown();
    }

    fn spawn_public_plaintext_mux(
        &self,
        listener: std::net::TcpListener,
    ) -> Result<(), iot_mqttd::MqttdError> {
        self.handle
            .as_ref()
            .expect("broker lifecycle handle is available")
            .spawn_public_plaintext_mux(
                listener,
                ProtocolBackends {
                    v311: SocketAddr::from(([127, 0, 0, 1], self.port)),
                    v5: SocketAddr::from(([127, 0, 0, 1], self.v5_port)),
                    device_v311: None,
                    device_v5: None,
                },
                MuxSettings::default(),
            )
    }

    fn spawn_public_tls_mux(
        &self,
        listener: std::net::TcpListener,
        certificate: &Path,
        key: &Path,
    ) -> Result<(), iot_mqttd::MqttdError> {
        let acceptor =
            iot_mqttd::load_tls_acceptor(&certificate.to_path_buf(), &key.to_path_buf())?;
        self.handle
            .as_ref()
            .expect("broker lifecycle handle is available")
            .spawn_public_tls_mux(
                listener,
                acceptor,
                ProtocolBackends {
                    v311: SocketAddr::from(([127, 0, 0, 1], self.port)),
                    v5: SocketAddr::from(([127, 0, 0, 1], self.v5_port)),
                    device_v311: None,
                    device_v5: None,
                },
                MuxSettings::default(),
            )
    }

    fn join(mut self) -> u16 {
        let port = self.port;
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
        port
    }

    fn shutdown(self) -> u16 {
        self.signal_shutdown();
        self.join()
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown();
            let _ = handle.join();
        }
    }
}

async fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn now_ms_for_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn assert_stopped(port: u16) {
    let stopped = match timeout(
        Duration::from_millis(500),
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await
    {
        Err(_) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
    };
    assert!(
        stopped,
        "broker backend {port} still accepts connections after shutdown"
    );
}

async fn assert_connection_closed(stream: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    match timeout(Duration::from_secs(1), stream.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("broker sent a packet instead of closing after storage failure"),
        Err(_) => panic!("broker did not close after storage failure"),
    }
}

fn options(client_id: &str, port: u16, clean: bool) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, "127.0.0.1", port);
    options.set_clean_session(clean);
    options
}

async fn publish_once(port: u16, topic: &str, payload: &str, retain: bool, qos: QoS) {
    let (client, mut events) = AsyncClient::new(options("publisher", port, true), 10);
    let task = tokio::spawn(async move {
        loop {
            events.poll().await.unwrap();
        }
    });
    client.publish(topic, qos, retain, payload).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    task.abort();
}

async fn connect_persistent_subscriber(
    port: u16,
    client_id: &str,
    topic: &str,
) -> (AsyncClient, tokio::task::JoinHandle<Vec<u8>>) {
    let (client, mut events) = AsyncClient::new(options(client_id, port, false), 10);
    client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    let task = tokio::spawn(async move {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) = events.poll().await.unwrap() {
                return publish.payload.to_vec();
            }
        }
    });
    sleep(Duration::from_millis(100)).await;
    (client, task)
}

#[tokio::test]
async fn retained_publish_is_delivered_after_sqlite_broker_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    publish_once(
        first.port,
        "restart/retained",
        "retained",
        true,
        QoS::AtLeastOnce,
    )
    .await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    let (client, mut events) = AsyncClient::new(options("retained-reader", second.port, true), 10);
    client
        .subscribe("restart/retained", QoS::AtLeastOnce)
        .await
        .unwrap();
    let payload = timeout(Duration::from_secs(3), async {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) = events.poll().await.unwrap() {
                break publish.payload.to_vec();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(payload, b"retained");
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn retained_message_expiry_is_not_extended_by_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut publisher = raw_v5_connect(first.v5_port, "retained-expiry").await;
    raw_v5_retained_expiry(&mut publisher, "retained/expiry", "payload", 1, 9).await;
    let puback = read_mqtt_packet(&mut publisher).await;
    assert_eq!(puback[0] & 0xf0, 0x40);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(publisher);
    sleep(Duration::from_millis(1_100)).await;

    let second = start_test_broker(&database).await;
    let (client, mut events) = AsyncClient::new(options("expiry-reader", second.port, true), 10);
    client
        .subscribe("retained/expiry", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(300), async {
            loop {
                if let Event::Incoming(Packet::Publish(_)) = events.poll().await.unwrap() {
                    return;
                }
            }
        })
        .await
        .is_err()
    );
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn malformed_sqlite_snapshot_refuses_startup_before_backend_bind() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let storage = Arc::new(SqliteStorage::open(&database).unwrap());
    let malformed = StoredPublish {
        publish: rumqttd::protocol::Publish::new(
            String::from("actual/topic"),
            String::from("payload"),
            true,
        ),
        properties: None,
        stored_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };
    rusqlite::Connection::open(&database)
        .unwrap()
        .execute(
            "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "wrong/topic",
                serde_json::to_vec(&malformed).unwrap(),
                malformed.stored_at_ms as i64
            ],
        )
        .unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();
    let port = reserve_port().await;
    let v5_port = reserve_port().await;
    let result = start_broker_with_storage(
        ListenerConfiguration {
            plaintext_address: "127.0.0.1:0".parse().unwrap(),
            tls_address: "127.0.0.1:0".parse().unwrap(),
            v311_backend_address: SocketAddr::from(([127, 0, 0, 1], port)),
            v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5_port)),
            tls_cert_path: certificate,
            tls_key_path: key,
            websocket_address: None,
            websocket_tls: false,
            bridge: None,
            max_connections: 10,
            max_payload_size: 1024,
            max_inflight_count: 10,
            token_authenticator: None,
            auth_handler: None,
            authorization_handler: None,
        },
        storage,
    )
    .await;
    assert!(result.is_err());
    assert_stopped(port).await;
}

#[tokio::test]
async fn malformed_offline_inbound_and_session_rows_refuse_startup_before_bind() {
    for kind in [
        "offline", "inbound", "journal", "session", "inflight", "phase",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("broker.sqlite");
        let storage = Arc::new(SqliteStorage::open(&database).unwrap());
        let connection = rusqlite::Connection::open(&database).unwrap();
        match kind {
            "offline" => {
                connection
                    .execute(
                        "INSERT INTO offline_queue(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                        rusqlite::params!["missing-client", b"bad-json", now_ms_for_test() as i64],
                    )
                    .unwrap();
            }
            "inbound" => {
                connection
                    .execute(
                        "INSERT INTO inbound_qos(client_id, packet_id, qos, value, stored_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params!["", 7_i64, 2_i64, b"bad-json", now_ms_for_test() as i64],
                    )
                    .unwrap();
            }
            "journal" => {
                connection
                    .execute(
                        "INSERT INTO inbound_qos2_journal(
                            client_id, packet_id, qos, value, stored_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params!["client", 7_i64, 2_i64, b"bad-json", 0_i64],
                    )
                    .unwrap();
            }
            "session" => {
                let session = StoredSession {
                    client_id: "client".into(),
                    tracker: Tracker::new("wrong-tracker".into()),
                    subscriptions: vec![],
                    unacked_pubrels: vec![],
                    inflight: vec![],
                    qos2_leases: vec![],
                    outbound_qos2: vec![],
                    qos2_publishes: vec![],
                    metrics: ConnectionEvents::default(),
                    stored_at_ms: now_ms_for_test(),
                };
                connection
                    .execute(
                        "INSERT INTO sessions(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            "client",
                            serde_json::to_vec(&session).unwrap(),
                            now_ms_for_test() as i64
                        ],
                    )
                    .unwrap();
            }
            "inflight" => {
                let now = now_ms_for_test();
                let session = StoredSession {
                    client_id: "client".into(),
                    tracker: Tracker::new("client".into()),
                    subscriptions: vec![],
                    unacked_pubrels: vec![],
                    inflight: vec![StoredInflight {
                        publish: rumqttd::protocol::Publish::new(
                            String::from("invalid/topic"),
                            String::from("payload"),
                            false,
                        ),
                        properties: None,
                        stored_at_ms: now,
                        pkid: 7,
                        cursor: Some((0, 1)),
                        filter_idx: 99,
                        filter: None,
                        offline_lease_id: None,
                    }],
                    qos2_leases: vec![],
                    outbound_qos2: vec![],
                    qos2_publishes: vec![],
                    metrics: ConnectionEvents::default(),
                    stored_at_ms: now,
                };
                connection
                    .execute(
                        "INSERT INTO sessions(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            "client",
                            serde_json::to_vec(&session).unwrap(),
                            now as i64
                        ],
                    )
                    .unwrap();
            }
            "phase" => {
                let now = now_ms_for_test();
                let session = StoredSession {
                    client_id: "client".into(),
                    tracker: Tracker::new("client".into()),
                    subscriptions: vec![],
                    unacked_pubrels: vec![],
                    inflight: vec![],
                    qos2_leases: vec![(8, 99)],
                    outbound_qos2: vec![OutboundQos2Phase {
                        packet_id: 7,
                        offline_lease_id: Some(99),
                        stored_at_ms: now,
                    }],
                    qos2_publishes: vec![],
                    metrics: ConnectionEvents::default(),
                    stored_at_ms: now,
                };
                connection
                    .execute(
                        "INSERT INTO sessions(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            "client",
                            serde_json::to_vec(&session).unwrap(),
                            now as i64
                        ],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let certificate = directory.path().join("cert.pem");
        let key = directory.path().join("key.pem");
        std::fs::write(&certificate, "certificate").unwrap();
        std::fs::write(&key, "key").unwrap();
        let port = reserve_port().await;
        let v5_port = reserve_port().await;
        let result = start_broker_with_storage(
            ListenerConfiguration {
                plaintext_address: "127.0.0.1:0".parse().unwrap(),
                tls_address: "127.0.0.1:0".parse().unwrap(),
                v311_backend_address: SocketAddr::from(([127, 0, 0, 1], port)),
                v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5_port)),
                tls_cert_path: certificate,
                tls_key_path: key,
                websocket_address: None,
                websocket_tls: false,
                bridge: None,
                max_connections: 10,
                max_payload_size: 1024,
                max_inflight_count: 10,
                token_authenticator: None,
                auth_handler: None,
                authorization_handler: None,
            },
            storage,
        )
        .await;
        assert!(result.is_err(), "{kind} row must reject startup");
        assert_stopped(port).await;
    }
}

#[tokio::test]
async fn mqtt5_alias_retained_publish_is_recovered_after_restart() {
    const TOPIC_ALIAS: u16 = 1;
    const RETAINED_PACKET_ID: u16 = 11;

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut publisher = raw_v5_connect(first.v5_port, "alias-retained").await;
    raw_v5_publish(
        &mut publisher,
        0x30,
        "restart/alias",
        "mapping",
        TOPIC_ALIAS,
        None,
    )
    .await;
    raw_v5_publish(
        &mut publisher,
        0x33,
        "",
        "retained",
        TOPIC_ALIAS,
        Some(RETAINED_PACKET_ID),
    )
    .await;
    let puback = read_mqtt_packet(&mut publisher).await;
    assert_eq!(puback[0] & 0xf0, 0x40);
    drop(publisher);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    let storage = SqliteStorage::open(&database).unwrap();
    let retained = storage
        .load(0)
        .unwrap()
        .retained
        .remove("restart/alias")
        .expect("retained publish must use the resolved alias topic");
    assert_eq!(retained.publish.topic_string(), "restart/alias");
    assert_eq!(&retained.publish.payload[..], b"retained");

    let second = start_test_broker(&database).await;
    let (client, mut events) = AsyncClient::new(options("alias-reader", second.port, true), 10);
    client
        .subscribe("restart/alias", QoS::AtLeastOnce)
        .await
        .unwrap();
    let payload = timeout(Duration::from_secs(3), async {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) = events.poll().await.unwrap() {
                break publish.payload.to_vec();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(payload, b"retained");
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn persistent_session_reconnect_restores_subscription_and_offline_queue() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let (client, task) =
        connect_persistent_subscriber(first.port, "persistent-reader", "restart/offline").await;
    drop(client);
    task.abort();
    sleep(Duration::from_millis(200)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    publish_once(
        second.port,
        "restart/offline",
        "queued",
        false,
        QoS::AtLeastOnce,
    )
    .await;
    let (client, task) =
        connect_persistent_subscriber(second.port, "persistent-reader", "restart/offline").await;
    let payload = timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload, b"queued");
    drop(client);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn offline_qos1_lease_redelivers_after_restart_and_deletes_on_puback() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "lease-qos1", false).await;
    raw_subscribe(&mut subscriber, "lease/qos1", 1).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    publish_once(
        second.port,
        "lease/qos1",
        "payload",
        false,
        QoS::AtLeastOnce,
    )
    .await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;

    let third = start_test_broker(&database).await;
    let mut subscriber = raw_connect(third.port, "lease-qos1", false).await;
    subscriber_connack(&mut subscriber).await;
    let first_delivery = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(first_delivery[0] & 0x06, 0x02);
    let packet_id = publish_packet_id(&first_delivery);
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
    drop(subscriber);

    let fourth = start_test_broker(&database).await;
    let mut subscriber = raw_connect(fourth.port, "lease-qos1", false).await;
    subscriber_connack(&mut subscriber).await;
    let redelivery = read_mqtt_packet(&mut subscriber).await;
    assert_ne!(redelivery[0] & 0x08, 0);
    assert_eq!(publish_packet_id(&redelivery), packet_id);
    raw_puback(&mut subscriber, packet_id).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    sleep(Duration::from_millis(100)).await;
    let fourth_port = fourth.shutdown();
    assert_stopped(fourth_port).await;
    drop(subscriber);

    let fifth = start_test_broker(&database).await;
    let mut subscriber = raw_connect(fifth.port, "lease-qos1", false).await;
    subscriber_connack(&mut subscriber).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    drop(subscriber);
    let fifth_port = fifth.shutdown();
    assert_stopped(fifth_port).await;
}

#[tokio::test]
async fn expired_offline_lease_redelivers_at_reconnect() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let policy = RetentionPolicy {
        offline_lease_ms: 1,
        prune_interval_ms: 1,
        offline_ttl_ms: 1_000,
        ..RetentionPolicy::default()
    };
    let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(&database).unwrap());
    let first = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    let mut subscriber = raw_connect(first.port, "lease-expiry", false).await;
    raw_subscribe(&mut subscriber, "lease/expiry", 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    publish_once(
        second.port,
        "lease/expiry",
        "payload",
        false,
        QoS::AtLeastOnce,
    )
    .await;
    let mut subscriber = raw_connect(second.port, "lease-expiry", false).await;
    subscriber_connack(&mut subscriber).await;
    let first_delivery = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&first_delivery);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    drop(subscriber);
    sleep(Duration::from_millis(110)).await;

    let third = start_test_broker_with_storage_and_policy(storage, policy).await;
    let mut subscriber = raw_connect(third.port, "lease-expiry", false).await;
    subscriber_connack(&mut subscriber).await;
    let redelivery = read_mqtt_packet(&mut subscriber).await;
    assert_ne!(redelivery[0] & 0x08, 0);
    assert_eq!(publish_packet_id(&redelivery), packet_id);
    raw_puback(&mut subscriber, packet_id).await;
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn offline_ttl_expires_before_reconnect_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let policy = RetentionPolicy {
        offline_lease_ms: 10,
        prune_interval_ms: 1,
        offline_ttl_ms: 1,
        ..RetentionPolicy::default()
    };
    let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(&database).unwrap());
    let first = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    let mut subscriber = raw_connect(first.port, "ttl-expiry", false).await;
    raw_subscribe(&mut subscriber, "lease/ttl", 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    publish_once(second.port, "lease/ttl", "payload", false, QoS::AtLeastOnce).await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    sleep(Duration::from_millis(10)).await;

    let third = start_test_broker_with_storage_and_policy(storage, policy).await;
    let mut subscriber = raw_connect(third.port, "ttl-expiry", false).await;
    subscriber_connack(&mut subscriber).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn offline_mqtt_message_expiry_discards_before_reconnect_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "offline-message-expiry", false).await;
    raw_subscribe(&mut subscriber, "offline/message-expiry", 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    let mut publisher = raw_v5_connect(second.v5_port, "expiry-publisher").await;
    raw_v5_publish_expiry(
        &mut publisher,
        0x32,
        "offline/message-expiry",
        "payload",
        1,
        7,
    )
    .await;
    let puback = read_mqtt_packet(&mut publisher).await;
    assert_eq!(puback[0] & 0xf0, 0x40);
    drop(publisher);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    sleep(Duration::from_millis(1_100)).await;

    let third = start_test_broker(&database).await;
    let mut subscriber = raw_connect(third.port, "offline-message-expiry", false).await;
    subscriber_connack(&mut subscriber).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn offline_mqtt_message_expiry_is_reduced_by_offline_residence() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber =
        raw_v5_connect_with_clean(first.v5_port, "offline-message-remaining-ttl", false).await;
    raw_v5_subscribe(&mut subscriber, "offline/message-remaining-ttl", 1, 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    let mut publisher = raw_v5_connect(second.v5_port, "remaining-ttl-publisher").await;
    raw_v5_publish_expiry(
        &mut publisher,
        0x32,
        "offline/message-remaining-ttl",
        "payload",
        10,
        8,
    )
    .await;
    let puback = read_mqtt_packet(&mut publisher).await;
    assert_eq!(puback[0] & 0xf0, 0x40);
    drop(publisher);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    sleep(Duration::from_millis(1_100)).await;

    let third = start_test_broker(&database).await;
    let mut subscriber =
        raw_v5_connect_with_clean(third.v5_port, "offline-message-remaining-ttl", false).await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(publish[0] & 0xf0, 0x30);
    assert_eq!(
        mqtt5_publish_topic(&publish),
        "offline/message-remaining-ttl"
    );
    assert!(
        mqtt5_publish_message_expiry_interval(&publish).is_some_and(|expiry| expiry < 10),
        "forwarded MQTT5 expiry must be reduced by offline residence time"
    );
    raw_puback(&mut subscriber, publish_packet_id(&publish)).await;
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn expired_offline_qos1_inflight_is_not_replayed_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "expired-inflight-reader", false).await;
    raw_subscribe(&mut subscriber, "expired/inflight", 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    let mut publisher = raw_v5_connect(second.v5_port, "expired-inflight-publisher").await;
    raw_v5_publish_expiry(&mut publisher, 0x32, "expired/inflight", "payload", 1, 13).await;
    let puback = read_mqtt_packet(&mut publisher).await;
    assert_eq!(puback[0] & 0xf0, 0x40);
    let mut subscriber = raw_connect(second.port, "expired-inflight-reader", false).await;
    subscriber_connack(&mut subscriber).await;
    let delivery = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(delivery[0] & 0x06, 0x02);
    drop(subscriber);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    drop(publisher);
    sleep(Duration::from_millis(1_100)).await;

    let third = start_test_broker(&database).await;
    let mut subscriber = raw_connect(third.port, "expired-inflight-reader", false).await;
    subscriber_connack(&mut subscriber).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn overlapping_offline_filters_enqueue_one_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "overlap-reader", false).await;
    raw_subscribe(&mut subscriber, "overlap/#", 1).await;
    raw_subscribe_connected(&mut subscriber, "overlap/topic", 1, 2).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    publish_once(
        second.port,
        "overlap/topic",
        "payload",
        false,
        QoS::AtLeastOnce,
    )
    .await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;

    let third = start_test_broker(&database).await;
    let mut subscriber = raw_connect(third.port, "overlap-reader", false).await;
    subscriber_connack(&mut subscriber).await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    raw_puback(&mut subscriber, publish_packet_id(&publish)).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err()
    );
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn offline_qos2_lease_first_delivery_completes_pubrec_pubrel_pubcomp() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "lease-qos2-live", false).await;
    raw_subscribe(&mut subscriber, "lease/qos2/live", 2).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    publish_once(
        second.port,
        "lease/qos2/live",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let mut subscriber = raw_connect(second.port, "lease-qos2-live", false).await;
    subscriber_connack(&mut subscriber).await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(publish[0] & 0x06, 0x04);
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    let pubrel = loop {
        let packet = read_mqtt_packet(&mut subscriber).await;
        match packet[0] & 0xf0 {
            0x30 => raw_pubrec(&mut subscriber, publish_packet_id(&packet)).await,
            0x60 => break packet,
            packet_type => panic!("unexpected packet type {packet_type:#x}"),
        }
    };
    raw_pubcomp(&mut subscriber, u16::from_be_bytes([pubrel[2], pubrel[3]])).await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn offline_qos2_lease_pubrel_survives_restart_until_pubcomp() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "lease-qos2-restart", false).await;
    raw_subscribe(&mut subscriber, "lease/qos2/restart", 2).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    publish_once(
        second.port,
        "lease/qos2/restart",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let mut subscriber = raw_connect(second.port, "lease-qos2-restart", false).await;
    subscriber_connack(&mut subscriber).await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    sleep(Duration::from_millis(100)).await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    drop(subscriber);

    let third = start_test_broker(&database).await;
    let mut subscriber = raw_connect(third.port, "lease-qos2-restart", false).await;
    subscriber_connack(&mut subscriber).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    raw_pubcomp(&mut subscriber, u16::from_be_bytes([pubrel[2], pubrel[3]])).await;
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn expired_offline_qos2_lease_with_restored_pubrel_phase_does_not_duplicate_publish() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let policy = RetentionPolicy {
        offline_lease_ms: 100,
        ..RetentionPolicy::default()
    };
    let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(&database).unwrap());

    let first = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    let mut subscriber = raw_connect(first.port, "lease-qos2-expired-phase", false).await;
    raw_subscribe(&mut subscriber, "lease/qos2/expired-phase", 2).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    publish_once(
        second.port,
        "lease/qos2/expired-phase",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let mut subscriber = raw_connect(second.port, "lease-qos2-expired-phase", false).await;
    subscriber_connack(&mut subscriber).await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    let pubrel = loop {
        let packet = read_mqtt_packet(&mut subscriber).await;
        match packet[0] & 0xf0 {
            0x30 => raw_pubrec(&mut subscriber, publish_packet_id(&packet)).await,
            0x60 => break packet,
            packet_type => panic!("unexpected packet type {packet_type:#x}"),
        }
    };
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
    drop(subscriber);
    let snapshot = storage.load(now_ms_for_test()).unwrap();
    assert!(
        snapshot
            .sessions
            .iter()
            .find(|session| session.client_id == "lease-qos2-expired-phase")
            .is_some_and(|session| {
                session
                    .outbound_qos2
                    .iter()
                    .any(|phase| phase.offline_lease_id.is_some())
            }),
        "controlled shutdown must persist the QoS2 PubRel lease phase"
    );
    sleep(Duration::from_millis(110)).await;

    let third = start_test_broker_with_storage_and_policy(storage, policy).await;
    let mut subscriber = raw_connect(third.port, "lease-qos2-expired-phase", false).await;
    subscriber_connack(&mut subscriber).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    if let Ok(packet) = timeout(
        Duration::from_millis(300),
        read_mqtt_packet(&mut subscriber),
    )
    .await
    {
        assert_ne!(
            packet[0] & 0xf0,
            0x30,
            "restored PubRel phase must suppress a second leased QoS2 PUBLISH"
        );
    }
    raw_pubcomp(&mut subscriber, u16::from_be_bytes([pubrel[2], pubrel[3]])).await;
    let third_port = third.shutdown();
    assert_stopped(third_port).await;
}

#[tokio::test]
async fn outbound_qos2_pubrel_phase_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "outbound-qos2", false).await;
    raw_subscribe(&mut subscriber, "outbound/qos2", 2).await;
    publish_once(
        first.port,
        "outbound/qos2",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(subscriber);

    let second = start_test_broker(&database).await;
    let mut subscriber = raw_connect(second.port, "outbound-qos2", false).await;
    subscriber_connack(&mut subscriber).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    raw_pubcomp(&mut subscriber, u16::from_be_bytes([pubrel[2], pubrel[3]])).await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn duplicate_outbound_qos2_pubrec_retransmits_pubrel_without_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let mut subscriber = raw_connect(broker.port, "duplicate-pubrec-reader", false).await;
    raw_subscribe(&mut subscriber, "duplicate/pubrec", 2).await;
    publish_once(
        broker.port,
        "duplicate/pubrec",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    raw_pubrec(&mut subscriber, packet_id).await;
    let retransmitted = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(retransmitted[0] & 0xf0, 0x60);
    raw_pubcomp(
        &mut subscriber,
        u16::from_be_bytes([retransmitted[2], retransmitted[3]]),
    )
    .await;
    subscriber.write_all(&[0xc0, 0x00]).await.unwrap();
    let pingresp = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pingresp[0] & 0xf0, 0xd0);
    drop(subscriber);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn duplicate_inbound_qos2_pubrel_retransmits_pubcomp_without_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let mut publisher = raw_connect(broker.port, "duplicate-pubrel-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "duplicate/pubrel", "payload", 43).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 43).await;
    let pubcomp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubcomp[0] & 0xf0, 0x70);
    raw_pubrel(&mut publisher, 43).await;
    let retransmitted = read_mqtt_packet(&mut publisher).await;
    assert_eq!(retransmitted[0] & 0xf0, 0x70);
    publisher.write_all(&[0xc0, 0x00]).await.unwrap();
    let pingresp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pingresp[0] & 0xf0, 0xd0);
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn duplicate_inbound_qos2_pubrel_does_not_deliver_twice() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let mut reader = raw_connect(broker.port, "duplicate-delivery-reader", false).await;
    raw_subscribe(&mut reader, "duplicate/delivery", 1).await;
    let mut publisher = raw_connect(broker.port, "duplicate-delivery-writer", false).await;
    subscriber_connack(&mut publisher).await;

    raw_publish_qos2(&mut publisher, "duplicate/delivery", "payload", 44).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 44).await;
    let pubcomp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubcomp[0] & 0xf0, 0x70);

    let delivery = read_mqtt_packet(&mut reader).await;
    assert_eq!(delivery[0] & 0xf0, 0x30);
    raw_puback(&mut reader, publish_packet_id(&delivery)).await;

    raw_pubrel(&mut publisher, 44).await;
    let retransmitted = read_mqtt_packet(&mut publisher).await;
    assert_eq!(retransmitted[0] & 0xf0, 0x70);
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "duplicate inbound PUBREL must not append a second delivery"
    );

    drop(publisher);
    drop(reader);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn completed_inbound_qos2_packet_id_starts_a_new_transaction_when_dup_is_clear() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let mut reader = raw_connect(broker.port, "qos2-pid-reuse-reader", false).await;
    raw_subscribe(&mut reader, "qos2/pid-reuse", 1).await;
    let mut publisher = raw_connect(broker.port, "qos2-pid-reuse-writer", false).await;
    subscriber_connack(&mut publisher).await;

    raw_publish_qos2(&mut publisher, "qos2/pid-reuse", "first", 46).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 46).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x70);
    let first = read_mqtt_packet(&mut reader).await;
    raw_puback(&mut reader, publish_packet_id(&first)).await;

    raw_publish_qos2(&mut publisher, "qos2/pid-reuse", "second", 46).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 46).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x70);
    let second = read_mqtt_packet(&mut reader).await;
    assert_ne!(first, second);
    raw_puback(&mut reader, publish_packet_id(&second)).await;

    raw_publish(&mut publisher, 0x3c, "qos2/pid-reuse", "second", Some(46)).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 46).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x70);
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "DUP PUBLISH/PUBREL must not append a third delivery"
    );

    drop(publisher);
    drop(reader);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn duplicate_outbound_qos2_pubcomp_does_not_disconnect() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let mut subscriber = raw_connect(broker.port, "duplicate-pubcomp-reader", false).await;
    raw_subscribe(&mut subscriber, "duplicate/pubcomp", 2).await;
    publish_once(
        broker.port,
        "duplicate/pubcomp",
        "payload",
        false,
        QoS::ExactlyOnce,
    )
    .await;
    let publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&publish);
    raw_pubrec(&mut subscriber, packet_id).await;
    let pubrel = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(pubrel[0] & 0xf0, 0x60);
    raw_pubcomp(&mut subscriber, packet_id).await;
    raw_pubcomp(&mut subscriber, packet_id).await;
    subscriber.write_all(&[0xc0, 0x00]).await.unwrap();
    assert_eq!(read_mqtt_packet(&mut subscriber).await[0] & 0xf0, 0xd0);

    drop(subscriber);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn outbound_qos1_inflight_survives_restart_with_dup_and_topic_identity() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "outbound-qos1", false).await;
    raw_subscribe(&mut subscriber, "outbound/qos1/filter", 1).await;
    publish_once(
        first.port,
        "outbound/qos1/filter",
        "payload",
        false,
        QoS::AtLeastOnce,
    )
    .await;
    let first_publish = read_mqtt_packet(&mut subscriber).await;
    let packet_id = publish_packet_id(&first_publish);
    assert_eq!(
        &first_publish[4..4 + "outbound/qos1/filter".len()],
        b"outbound/qos1/filter"
    );
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(subscriber);

    let second = start_test_broker(&database).await;
    let mut subscriber = raw_connect(second.port, "outbound-qos1", false).await;
    subscriber_connack(&mut subscriber).await;
    let redelivery = read_mqtt_packet(&mut subscriber).await;
    assert_ne!(redelivery[0] & 0x08, 0);
    assert_eq!(publish_packet_id(&redelivery), packet_id);
    assert_eq!(
        &redelivery[4..4 + "outbound/qos1/filter".len()],
        b"outbound/qos1/filter"
    );
    raw_puback(&mut subscriber, packet_id).await;
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn active_persistent_session_is_snapshotted_during_controlled_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "active-reader", false).await;
    raw_subscribe(&mut subscriber, "shutdown/active", 1).await;

    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(subscriber);

    let second = start_test_broker(&database).await;
    let mut reconnect = raw_connect(second.port, "active-reader", false).await;
    let connack = subscriber_connack(&mut reconnect).await;
    assert_eq!(connack[2], 1);
    drop(reconnect);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn clean_session_is_not_restored_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut subscriber = raw_connect(first.port, "clean-reader", true).await;
    raw_subscribe(&mut subscriber, "restart/clean", 1).await;
    drop(subscriber);
    sleep(Duration::from_millis(200)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker(&database).await;
    let mut subscriber = raw_connect(second.port, "clean-reader", false).await;
    let connack = subscriber_connack(&mut subscriber).await;
    assert_eq!(connack[2], 0);
    drop(subscriber);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn clean_session_delete_failure_refuses_connect_without_resurrection() {
    let storage = Arc::new(MemoryStorage::new());
    let first = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(first.port, "clean-delete", false).await;
    raw_subscribe(&mut subscriber, "clean/delete", 1).await;
    drop(subscriber);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(1);
    let mut clean = raw_connect(second.port, "clean-delete", true).await;
    assert_connection_closed(&mut clean).await;
    drop(clean);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn reconnect_prune_failure_closes_before_connack_and_preserves_session() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(broker.port, "reconnect-prune-failure", false).await;
    raw_subscribe(&mut subscriber, "failure/reconnect/prune", 1).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    storage.fail_on_write(1);
    let mut rejected = raw_connect(broker.port, "reconnect-prune-failure", false).await;
    assert_connection_closed(&mut rejected).await;
    drop(rejected);

    let mut restored = raw_connect(broker.port, "reconnect-prune-failure", false).await;
    let connack = subscriber_connack(&mut restored).await;
    assert_eq!(
        connack[2], 1,
        "failed reconnect must not consume the session"
    );
    drop(restored);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn reconnect_lease_failure_closes_before_connack() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(2);
    let mut rejected = raw_connect(broker.port, "reconnect-lease-failure", false).await;
    assert_connection_closed(&mut rejected).await;
    drop(rejected);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn inbound_qos2_publish_is_completed_after_restart_from_durable_state() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut publisher = raw_connect(first.port, "qos2-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "restart/qos2", "inflight", 7).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(publisher);

    let second = start_test_broker(&database).await;
    let mut publisher = raw_connect(second.port, "qos2-publisher", false).await;
    let connack = subscriber_connack(&mut publisher).await;
    assert_eq!(connack[0], 0x20);
    raw_pubrel(&mut publisher, 7).await;
    let pubcomp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubcomp[0] & 0xf0, 0x70);
    drop(publisher);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn retained_qos2_publish_is_not_restored_before_pubrel() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let first = start_test_broker(&database).await;
    let mut publisher = raw_connect(first.port, "retained-qos2-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish(
        &mut publisher,
        0x35,
        "retained/qos2/pending",
        "must-not-be-visible",
        Some(47),
    )
    .await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;
    drop(publisher);

    let second = start_test_broker(&database).await;
    let mut subscriber = raw_connect(second.port, "retained-qos2-reader", true).await;
    raw_subscribe(&mut subscriber, "retained/qos2/pending", 1).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err(),
        "a retained QoS2 publish must not become visible before PUBREL"
    );
    drop(subscriber);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn inbound_qos2_publish_survives_killed_broker_process() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let first_port = reserve_port().await;
    let first_v5 = reserve_port().await;
    let mut first = spawn_crash_child(&database, &certificate, &key, first_port, first_v5);
    wait_for_backend(first_port).await;
    let mut publisher = raw_connect(first_port, "killed-qos2", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "crash/qos2", "inflight", 22).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    first.kill().unwrap();
    first.wait().unwrap();
    drop(publisher);

    let second_port = reserve_port().await;
    let second_v5 = reserve_port().await;
    let mut second = spawn_crash_child(&database, &certificate, &key, second_port, second_v5);
    wait_for_backend(second_port).await;
    let mut publisher = raw_connect(second_port, "killed-qos2", false).await;
    subscriber_connack(&mut publisher).await;
    raw_pubrel(&mut publisher, 22).await;
    let pubcomp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubcomp[0] & 0xf0, 0x70);
    second.kill().unwrap();
    second.wait().unwrap();
}

#[tokio::test]
async fn retained_qos2_publish_is_not_visible_after_crash_before_pubrel() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let first_port = reserve_port().await;
    let first_v5 = reserve_port().await;
    let mut first = spawn_crash_child(&database, &certificate, &key, first_port, first_v5);
    wait_for_backend(first_port).await;
    let mut publisher = raw_connect(first_port, "pending-retained-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish(
        &mut publisher,
        0x35,
        "pending/retained",
        "payload",
        Some(47),
    )
    .await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    first.kill().unwrap();
    first.wait().unwrap();
    drop(publisher);

    let second_port = reserve_port().await;
    let second_v5 = reserve_port().await;
    let mut second = spawn_crash_child(&database, &certificate, &key, second_port, second_v5);
    wait_for_backend(second_port).await;
    let mut reader = raw_connect(second_port, "pending-retained-reader", true).await;
    raw_subscribe(&mut reader, "pending/retained", 1).await;
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "a retained QoS2 publish must not become visible before PUBREL"
    );
    drop(reader);
    second.kill().unwrap();
    second.wait().unwrap();
}

#[tokio::test]
async fn committed_qos2_retries_finalize_after_post_append_failure_and_restart() {
    let storage = Arc::new(MemoryStorage::new());
    let first = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(first.port, "qos2-finalize-reader", false).await;
    raw_subscribe(&mut subscriber, "qos2/finalize", 1).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    let mut publisher = raw_connect(first.port, "qos2-finalize-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "qos2/finalize", "payload", 48).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    storage.fail_on_write(2);
    raw_pubrel(&mut publisher, 48).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let second = start_test_broker_with_storage(storage).await;
    let mut publisher = raw_connect(second.port, "qos2-finalize-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_pubrel(&mut publisher, 48).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x70);

    let mut subscriber = raw_connect(second.port, "qos2-finalize-reader", false).await;
    subscriber_connack(&mut subscriber).await;
    let delivery = read_mqtt_packet(&mut subscriber).await;
    assert_eq!(mqtt5_publish_topic(&delivery), "qos2/finalize");
    raw_puback(&mut subscriber, publish_packet_id(&delivery)).await;
    assert!(
        timeout(
            Duration::from_millis(300),
            read_mqtt_packet(&mut subscriber)
        )
        .await
        .is_err(),
        "recovery must produce exactly one visible delivery"
    );

    drop(subscriber);
    drop(publisher);
    let second_port = second.shutdown();
    assert_stopped(second_port).await;
}

#[tokio::test]
async fn killed_after_qos2_journal_commit_replays_once_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let first = start_test_broker(&database).await;
    let mut reader = raw_connect(first.port, "journal-replay-reader", false).await;
    raw_subscribe(&mut reader, "journal/replay", 1).await;
    drop(reader);
    sleep(Duration::from_millis(100)).await;
    let first_port = first.shutdown();
    assert_stopped(first_port).await;

    let first_port = reserve_port().await;
    let first_v5 = reserve_port().await;
    let mut child = spawn_crash_child(&database, &certificate, &key, first_port, first_v5);
    wait_for_backend(first_port).await;
    let mut publisher = raw_connect(first_port, "journal-replay-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "journal/replay", "payload", 45).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);

    let value: Vec<u8> = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT value FROM inbound_qos2_journal
             WHERE client_id = ?1 AND packet_id = ?2",
            rusqlite::params!["journal-replay-writer", 45_i64],
            |row| row.get(0),
        )
        .unwrap();
    let stored: StoredPublish = serde_json::from_slice(&value).unwrap();
    let storage = SqliteStorage::open(&database).unwrap();
    assert!(matches!(
        storage
            .commit_inbound_qos2("journal-replay-writer", 45, now_ms_for_test())
            .unwrap(),
        InboundQos2CommitResult::AppendRequired { publish } if publish == stored
    ));

    child.kill().unwrap();
    child.wait().unwrap();
    drop(publisher);

    let second_port = reserve_port().await;
    let second_v5 = reserve_port().await;
    let mut restarted = spawn_crash_child(&database, &certificate, &key, second_port, second_v5);
    wait_for_backend(second_port).await;
    let journal_state: i64 = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT state FROM inbound_qos2_journal
             WHERE client_id = ?1 AND packet_id = ?2",
            rusqlite::params!["journal-replay-writer", 45_i64],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        journal_state, 2,
        "startup replay must durably complete the row"
    );

    let mut reader = raw_connect(second_port, "journal-replay-reader", false).await;
    subscriber_connack(&mut reader).await;
    let replayed = read_mqtt_packet(&mut reader).await;
    assert_eq!(replayed[0] & 0xf0, 0x30);
    assert_eq!(mqtt5_publish_topic(&replayed), "journal/replay");
    raw_puback(&mut reader, publish_packet_id(&replayed)).await;

    let mut publisher = raw_connect(second_port, "journal-replay-writer", false).await;
    subscriber_connack(&mut publisher).await;
    raw_pubrel(&mut publisher, 45).await;
    let pubcomp = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubcomp[0] & 0xf0, 0x70);
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "replayed journal entry must not be appended again on duplicate PUBREL"
    );

    drop(publisher);
    drop(reader);
    sleep(Duration::from_millis(100)).await;
    restarted.kill().unwrap();
    restarted.wait().unwrap();

    let third_port = reserve_port().await;
    let third_v5 = reserve_port().await;
    let mut third = spawn_crash_child(&database, &certificate, &key, third_port, third_v5);
    wait_for_backend(third_port).await;
    let mut reader = raw_connect(third_port, "journal-replay-reader", false).await;
    subscriber_connack(&mut reader).await;
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "completed journal rows must not append a second visible delivery after restart"
    );
    drop(reader);
    third.kill().unwrap();
    third.wait().unwrap();
}

#[tokio::test]
async fn commit_inbound_failure_closes_connection_before_qos_acknowledgement() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(3);
    let mut publisher = raw_connect(broker.port, "failing-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos1_retained(&mut publisher, "failure/qos1", "payload", 9).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn complete_inbound_failure_closes_connection_before_puback() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(5);
    let mut publisher = raw_connect(broker.port, "complete-failure", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos1_retained(&mut publisher, "failure/complete", "payload", 10).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn qos2_commit_failure_closes_connection_without_pubrec() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(3);
    let mut publisher = raw_connect(broker.port, "qos2-commit-failure", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "failure/qos2-commit", "payload", 30).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn qos2_complete_failure_closes_connection_without_pubcomp() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    storage.fail_on_write(5);
    let mut publisher = raw_connect(broker.port, "qos2-complete-failure", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "failure/qos2-complete", "payload", 31).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    raw_pubrel(&mut publisher, 31).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn offline_enqueue_failure_closes_qos1_publisher_before_puback() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(broker.port, "offline-enqueue-qos1", false).await;
    raw_subscribe(&mut subscriber, "failure/offline/qos1", 1).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    let mut publisher = raw_connect(broker.port, "offline-enqueue-qos1-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    storage.fail_on_write(3);
    raw_publish(
        &mut publisher,
        0x32,
        "failure/offline/qos1",
        "payload",
        Some(41),
    )
    .await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn offline_enqueue_failure_closes_qos2_publisher_before_pubcomp() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(broker.port, "offline-enqueue-qos2", false).await;
    raw_subscribe(&mut subscriber, "failure/offline/qos2", 2).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;

    let mut publisher = raw_connect(broker.port, "offline-enqueue-qos2-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "failure/offline/qos2", "payload", 42).await;
    let pubrec = read_mqtt_packet(&mut publisher).await;
    assert_eq!(pubrec[0] & 0xf0, 0x50);
    storage.fail_on_write(2);
    raw_pubrel(&mut publisher, 42).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn committed_qos2_retry_waits_for_durable_completion_before_pubcomp() {
    let storage = Arc::new(MemoryStorage::new());
    let broker = start_test_broker_with_storage(storage.clone()).await;
    let mut subscriber = raw_connect(broker.port, "qos2-retry-offline-reader", false).await;
    raw_subscribe(&mut subscriber, "failure/qos2-retry", 2).await;
    drop(subscriber);
    sleep(Duration::from_millis(100)).await;
    let mut live = raw_connect(broker.port, "qos2-retry-live-reader", true).await;
    raw_subscribe(&mut live, "failure/qos2-retry", 1).await;

    let mut publisher = raw_connect(broker.port, "qos2-retry-publisher", false).await;
    subscriber_connack(&mut publisher).await;
    raw_publish_qos2(&mut publisher, "failure/qos2-retry", "payload", 48).await;
    assert_eq!(read_mqtt_packet(&mut publisher).await[0] & 0xf0, 0x50);
    storage.fail_on_write(2);
    raw_pubrel(&mut publisher, 48).await;
    assert_connection_closed(&mut publisher).await;
    drop(publisher);

    let mut retry = raw_connect(broker.port, "qos2-retry-publisher", false).await;
    subscriber_connack(&mut retry).await;
    storage.fail_on_write(2);
    raw_pubrel(&mut retry, 48).await;
    assert_connection_closed(&mut retry).await;
    drop(retry);

    let mut recovered = raw_connect(broker.port, "qos2-retry-publisher", false).await;
    subscriber_connack(&mut recovered).await;
    raw_pubrel(&mut recovered, 48).await;
    assert_eq!(read_mqtt_packet(&mut recovered).await[0] & 0xf0, 0x70);
    let delivered = timeout(Duration::from_millis(300), read_mqtt_packet(&mut live))
        .await
        .expect("durable QoS2 retry must wake online subscribers");
    assert_eq!(delivered[0] & 0xf0, 0x30);
    assert!(delivered.ends_with(b"payload"));
    drop(recovered);
    drop(live);
    let port = broker.shutdown();
    assert_stopped(port).await;
}

#[tokio::test]
async fn public_mux_stops_when_broker_lifecycle_is_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let public_port = listener.local_addr().unwrap().port();
    broker.spawn_public_plaintext_mux(listener).unwrap();

    broker.signal_shutdown();
    let broker_port = broker.join();
    assert_stopped(broker_port).await;
    assert_stopped(public_port).await;
}

#[tokio::test]
async fn shutdown_cancels_stalled_pre_connect_worker() {
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let _stalled = TcpStream::connect(("127.0.0.1", broker.port))
        .await
        .unwrap();
    sleep(Duration::from_millis(100)).await;

    let (sender, receiver) = tokio::sync::oneshot::channel();
    thread::spawn(move || {
        let _ = sender.send(broker.shutdown());
    });
    let port = timeout(Duration::from_secs(1), receiver)
        .await
        .expect("shutdown must not wait for the MQTT CONNECT timeout")
        .expect("shutdown worker must return");
    assert_stopped(port).await;
}

#[tokio::test]
async fn shutdown_cancels_stalled_tls_handshake_worker() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let directory = tempfile::tempdir().unwrap();
    let broker = start_test_broker(&directory.path().join("broker.sqlite")).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tls_port = listener.local_addr().unwrap().port();
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    broker
        .spawn_public_tls_mux(
            listener,
            &fixtures.join("server.crt"),
            &fixtures.join("server.key"),
        )
        .unwrap();
    let _stalled = TcpStream::connect(("127.0.0.1", tls_port)).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    let (sender, receiver) = tokio::sync::oneshot::channel();
    thread::spawn(move || {
        let _ = sender.send(broker.shutdown());
    });
    let broker_port = timeout(Duration::from_secs(1), receiver)
        .await
        .expect("shutdown must not wait for the TLS handshake timeout")
        .expect("shutdown worker must return");
    assert_stopped(broker_port).await;
    assert_stopped(tls_port).await;
}

#[tokio::test]
async fn expired_sqlite_state_is_pruned_before_broker_restore() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("broker.sqlite");
    let storage = SqliteStorage::open(&database).unwrap();
    let publish = StoredPublish {
        publish: rumqttd::protocol::Publish::new("expired".to_owned(), "payload".to_owned(), true),
        properties: None,
        stored_at_ms: 0,
    };
    storage.save_retained("expired", &publish, 0).unwrap();
    storage
        .save_session(
            &StoredSession {
                client_id: "expired-client".into(),
                tracker: Tracker::new("expired-client".into()),
                subscriptions: vec!["expired".into()],
                unacked_pubrels: vec![],
                inflight: vec![],
                qos2_leases: vec![],
                outbound_qos2: vec![],
                qos2_publishes: vec![],
                metrics: ConnectionEvents::default(),
                stored_at_ms: 0,
            },
            0,
        )
        .unwrap();
    storage
        .prune(
            11,
            RetentionPolicy {
                retained_ttl_ms: 10,
                session_ttl_ms: 10,
                offline_ttl_ms: 10,
                max_offline_messages: 10,
                offline_lease_ms: 1,
                prune_interval_ms: 1,
            },
        )
        .unwrap();
    let state = storage.load(11).unwrap();
    assert!(state.retained.is_empty());
    assert!(state.sessions.is_empty());

    let broker = start_test_broker(&database).await;
    let (client, mut events) = AsyncClient::new(options("expired-reader", broker.port, true), 10);
    client.subscribe("expired", QoS::AtLeastOnce).await.unwrap();
    let result = timeout(Duration::from_millis(300), async {
        loop {
            if let Event::Incoming(Packet::Publish(_)) = events.poll().await.unwrap() {
                return true;
            }
        }
    })
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn periodic_prune_removes_expired_state_while_broker_is_running() {
    let storage = Arc::new(MemoryStorage::new());
    let policy = RetentionPolicy {
        retained_ttl_ms: 1,
        session_ttl_ms: 1,
        offline_ttl_ms: 1,
        max_offline_messages: 10,
        offline_lease_ms: 1,
        prune_interval_ms: 10,
    };
    let broker = start_test_broker_with_storage_and_policy(storage.clone(), policy).await;
    let expired = StoredPublish {
        publish: rumqttd::protocol::Publish::new(
            String::from("periodic/topic"),
            String::from("payload"),
            true,
        ),
        properties: None,
        stored_at_ms: 0,
    };
    let session = StoredSession {
        client_id: "periodic-client".into(),
        tracker: Tracker::new("periodic-client".into()),
        subscriptions: vec![],
        unacked_pubrels: vec![],
        inflight: vec![],
        qos2_leases: vec![],
        outbound_qos2: vec![],
        qos2_publishes: vec![],
        metrics: ConnectionEvents::default(),
        stored_at_ms: 0,
    };
    storage
        .save_retained("periodic/topic", &expired, 0)
        .unwrap();
    storage.save_session(&session, 0).unwrap();
    storage
        .enqueue_offline("periodic-client", &expired, 0, policy)
        .unwrap();
    let mut traffic = raw_connect(broker.port, "periodic-traffic", true).await;
    subscriber_connack(&mut traffic).await;
    for _ in 0..20 {
        traffic.write_all(&[0xc0, 0x00]).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    sleep(Duration::from_millis(100)).await;
    let state = storage.load(100).unwrap();
    assert!(state.retained.is_empty());
    assert!(state.sessions.is_empty());
    assert!(
        storage
            .lease_offline("periodic-client", 100, policy)
            .unwrap()
            .is_empty()
    );
    let port = timeout(Duration::from_secs(1), async move { broker.shutdown() })
        .await
        .expect("periodic prune must not block lifecycle shutdown");
    assert_stopped(port).await;
}

async fn raw_connect(port: u16, client_id: &str, clean: bool) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(&connect_packet(client_id, clean))
        .await
        .unwrap();
    stream
}

#[test]
fn crash_child_broker_process() {
    if std::env::var("IOT_MQTTD_CRASH_CHILD").as_deref() != Ok("1") {
        return;
    }
    let database = PathBuf::from(std::env::var("IOT_MQTTD_CRASH_DB").unwrap());
    let certificate = PathBuf::from(std::env::var("IOT_MQTTD_CRASH_CERT").unwrap());
    let key = PathBuf::from(std::env::var("IOT_MQTTD_CRASH_KEY").unwrap());
    let port = std::env::var("IOT_MQTTD_CRASH_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let v5_port = std::env::var("IOT_MQTTD_CRASH_V5_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(database).unwrap());
    let _broker = runtime
        .block_on(start_broker_with_storage(
            ListenerConfiguration {
                plaintext_address: "127.0.0.1:0".parse().unwrap(),
                tls_address: "127.0.0.1:0".parse().unwrap(),
                v311_backend_address: SocketAddr::from(([127, 0, 0, 1], port)),
                v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5_port)),
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
            storage,
        ))
        .unwrap();
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn spawn_crash_child(
    database: &Path,
    certificate: &Path,
    key: &Path,
    port: u16,
    v5_port: u16,
) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_child_broker_process")
        .arg("--nocapture")
        .env("IOT_MQTTD_CRASH_CHILD", "1")
        .env("IOT_MQTTD_CRASH_DB", database)
        .env("IOT_MQTTD_CRASH_CERT", certificate)
        .env("IOT_MQTTD_CRASH_KEY", key)
        .env("IOT_MQTTD_CRASH_PORT", port.to_string())
        .env("IOT_MQTTD_CRASH_V5_PORT", v5_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

async fn wait_for_backend(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "crash child did not bind backend {port}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn raw_v5_connect(port: u16, client_id: &str) -> TcpStream {
    raw_v5_connect_with_clean(port, client_id, true).await
}

async fn raw_v5_connect_with_clean(port: u16, client_id: &str, clean: bool) -> TcpStream {
    let remaining = 11 + 2 + client_id.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        if clean { 0x02 } else { 0x00 },
        0,
        60,
        0,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&packet).await.unwrap();
    let connack = read_mqtt_packet(&mut stream).await;
    assert_eq!(connack[0], 0x20);
    stream
}

async fn raw_v5_subscribe(stream: &mut TcpStream, topic: &str, qos: u8, packet_id: u16) {
    let remaining = 2 + 1 + 2 + topic.len() + 1;
    let mut packet = vec![0x82, remaining as u8];
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.push(0);
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(qos);
    stream.write_all(&packet).await.unwrap();
    let suback = read_mqtt_packet(stream).await;
    assert_eq!(suback[0] & 0xf0, 0x90);
}

async fn raw_subscribe(stream: &mut TcpStream, topic: &str, qos: u8) {
    let connack = subscriber_connack(stream).await;
    assert_eq!(connack[0], 0x20);
    let remaining = 2 + 2 + topic.len() + 1;
    let mut packet = vec![0x82, remaining as u8, 0, 1];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(qos);
    stream.write_all(&packet).await.unwrap();
    let suback = read_exact_packet(stream, 5).await;
    assert_eq!(suback[0], 0x90);
}

async fn raw_subscribe_connected(stream: &mut TcpStream, topic: &str, qos: u8, packet_id: u16) {
    let remaining = 2 + 2 + topic.len() + 1;
    let mut packet = vec![0x82, remaining as u8];
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(qos);
    stream.write_all(&packet).await.unwrap();
    let suback = read_mqtt_packet(stream).await;
    assert_eq!(suback[0], 0x90);
}

async fn raw_publish_qos1_retained(stream: &mut TcpStream, topic: &str, payload: &str, pkid: u16) {
    raw_publish(stream, 0x33, topic, payload, Some(pkid)).await;
}

async fn raw_publish_qos2(stream: &mut TcpStream, topic: &str, payload: &str, pkid: u16) {
    raw_publish(stream, 0x34, topic, payload, Some(pkid)).await;
}

async fn raw_v5_publish(
    stream: &mut TcpStream,
    header: u8,
    topic: &str,
    payload: &str,
    alias: u16,
    packet_id: Option<u16>,
) {
    let remaining = 2 + topic.len() + packet_id.map_or(0, |_| 2) + 1 + 3 + payload.len();
    let mut packet = vec![header, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    if let Some(packet_id) = packet_id {
        packet.extend_from_slice(&packet_id.to_be_bytes());
    }
    packet.extend_from_slice(&[3, 0x23]);
    packet.extend_from_slice(&alias.to_be_bytes());
    packet.extend_from_slice(payload.as_bytes());
    stream.write_all(&packet).await.unwrap();
}

async fn raw_v5_retained_expiry(
    stream: &mut TcpStream,
    topic: &str,
    payload: &str,
    expiry_seconds: u32,
    packet_id: u16,
) {
    let remaining = 2 + topic.len() + 2 + 1 + 5 + payload.len();
    let mut packet = vec![0x33, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(&[5, 0x02]);
    packet.extend_from_slice(&expiry_seconds.to_be_bytes());
    packet.extend_from_slice(payload.as_bytes());
    stream.write_all(&packet).await.unwrap();
}

async fn raw_v5_publish_expiry(
    stream: &mut TcpStream,
    header: u8,
    topic: &str,
    payload: &str,
    expiry_seconds: u32,
    packet_id: u16,
) {
    let remaining = 2 + topic.len() + 2 + 1 + 5 + payload.len();
    let mut packet = vec![header, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(&[5, 0x02]);
    packet.extend_from_slice(&expiry_seconds.to_be_bytes());
    packet.extend_from_slice(payload.as_bytes());
    stream.write_all(&packet).await.unwrap();
}

async fn raw_publish(
    stream: &mut TcpStream,
    header: u8,
    topic: &str,
    payload: &str,
    pkid: Option<u16>,
) {
    let remaining = 2 + topic.len() + pkid.map_or(0, |_| 2) + payload.len();
    let mut packet = vec![header, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    if let Some(pkid) = pkid {
        packet.extend_from_slice(&pkid.to_be_bytes());
    }
    packet.extend_from_slice(payload.as_bytes());
    stream.write_all(&packet).await.unwrap();
}

async fn raw_pubrel(stream: &mut TcpStream, pkid: u16) {
    stream
        .write_all(&[0x62, 0x02, (pkid >> 8) as u8, pkid as u8])
        .await
        .unwrap();
}

async fn raw_puback(stream: &mut TcpStream, pkid: u16) {
    stream
        .write_all(&[0x40, 0x02, (pkid >> 8) as u8, pkid as u8])
        .await
        .unwrap();
}

async fn raw_pubrec(stream: &mut TcpStream, pkid: u16) {
    stream
        .write_all(&[0x50, 0x02, (pkid >> 8) as u8, pkid as u8])
        .await
        .unwrap();
}

async fn raw_pubcomp(stream: &mut TcpStream, pkid: u16) {
    stream
        .write_all(&[0x70, 0x02, (pkid >> 8) as u8, pkid as u8])
        .await
        .unwrap();
}

fn publish_packet_id(packet: &[u8]) -> u16 {
    let body = mqtt_packet_body_offset(packet);
    let topic_len = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    let index = body + 2 + topic_len;
    u16::from_be_bytes([packet[index], packet[index + 1]])
}

fn mqtt5_publish_topic(packet: &[u8]) -> &str {
    let body = mqtt_packet_body_offset(packet);
    let topic_len = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    std::str::from_utf8(&packet[body + 2..body + 2 + topic_len]).unwrap()
}

fn mqtt5_publish_message_expiry_interval(packet: &[u8]) -> Option<u32> {
    let mut cursor = mqtt_packet_body_offset(packet);
    let topic_len = usize::from(u16::from_be_bytes([packet[cursor], packet[cursor + 1]]));
    cursor += 2 + topic_len;
    if packet[0] & 0x06 != 0 {
        cursor += 2;
    }
    let (properties_len, property_len_bytes) = mqtt_variable_byte_integer(&packet[cursor..]);
    cursor += property_len_bytes;
    let end = cursor + properties_len;
    let mut message_expiry_interval = None;
    while cursor < end {
        let property = packet[cursor];
        cursor += 1;
        match property {
            0x02 => {
                message_expiry_interval = Some(u32::from_be_bytes(
                    packet[cursor..cursor + 4].try_into().unwrap(),
                ));
                cursor += 4;
            }
            0x01 => cursor += 1,
            0x08 | 0x03 => {
                let len = usize::from(u16::from_be_bytes(
                    packet[cursor..cursor + 2].try_into().unwrap(),
                ));
                cursor += 2 + len;
            }
            0x09 => {
                let len = usize::from(u16::from_be_bytes(
                    packet[cursor..cursor + 2].try_into().unwrap(),
                ));
                cursor += 2 + len;
            }
            0x23 => cursor += 2,
            0x0b => {
                let (_, len_bytes) = mqtt_variable_byte_integer(&packet[cursor..]);
                cursor += len_bytes;
            }
            0x26 => {
                for _ in 0..2 {
                    let len = usize::from(u16::from_be_bytes(
                        packet[cursor..cursor + 2].try_into().unwrap(),
                    ));
                    cursor += 2 + len;
                }
            }
            _ => panic!("unexpected MQTT5 PUBLISH property {property:#x}"),
        }
    }
    message_expiry_interval
}

fn mqtt_packet_body_offset(packet: &[u8]) -> usize {
    let (_, remaining_len_bytes) = mqtt_variable_byte_integer(&packet[1..]);
    1 + remaining_len_bytes
}

fn mqtt_variable_byte_integer(bytes: &[u8]) -> (usize, usize) {
    let mut value = 0_usize;
    let mut multiplier = 1_usize;
    for (index, byte) in bytes.iter().enumerate() {
        value += usize::from(byte & 0x7f) * multiplier;
        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
        multiplier *= 128;
    }
    panic!("malformed MQTT variable byte integer");
}

async fn subscriber_connack(stream: &mut TcpStream) -> Vec<u8> {
    read_exact_packet(stream, 4).await
}

fn connect_packet(client_id: &str, clean: bool) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        if clean { 0x02 } else { 0x00 },
        0,
        60,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet
}

async fn read_mqtt_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut first = [0_u8; 2];
    stream.read_exact(&mut first).await.unwrap();
    let mut packet = first.to_vec();
    let mut encoded = first[1];
    let mut multiplier = 1_usize;
    let mut remaining = usize::from(first[1] & 0x7f);
    while encoded & 0x80 != 0 {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        packet.push(byte[0]);
        multiplier *= 128;
        remaining += usize::from(byte[0] & 0x7f) * multiplier;
        encoded = byte[0];
    }
    let mut body = vec![0_u8; remaining];
    stream.read_exact(&mut body).await.unwrap();
    packet.extend(body);
    packet
}

async fn read_exact_packet(stream: &mut TcpStream, length: usize) -> Vec<u8> {
    let mut packet = vec![0_u8; length];
    timeout(Duration::from_secs(3), stream.read_exact(&mut packet))
        .await
        .unwrap()
        .unwrap();
    packet
}
