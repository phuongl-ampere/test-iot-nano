use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use iot_nano_mqttd::{
    AclRule, BrokerFileConfig, BrokerLifecycleHandle, ListenerConfiguration, StaticAclConfig,
    StaticUser, build_policy, start_broker,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{AuthorizationAction, AuthorizationHandler, AuthorizationRequest};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    time::{sleep, timeout},
};

struct TestBroker {
    v311: u16,
    v5: u16,
    _handle: BrokerLifecycleHandle,
    _directory: tempfile::TempDir,
}

async fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start_test_broker(handler: Option<AuthorizationHandler>) -> TestBroker {
    let v311 = reserve_port().await;
    let v5 = reserve_port().await;
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let mut configuration = ListenerConfiguration {
        plaintext_address: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        tls_address: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        v311_backend_address: SocketAddr::from(([127, 0, 0, 1], v311)),
        v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5)),
        tls_cert_path: PathBuf::from(&certificate),
        tls_key_path: PathBuf::from(&key),
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        auth_handler: None,
        authorization_handler: None,
    };
    configuration.authorization_handler = handler;

    let handle = start_broker(configuration).await.unwrap();
    TestBroker {
        v311,
        v5,
        _handle: handle,
        _directory: directory,
    }
}

async fn start_configured_broker(config: BrokerFileConfig) -> TestBroker {
    let v311 = reserve_port().await;
    let v5 = reserve_port().await;
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "certificate").unwrap();
    std::fs::write(&key, "key").unwrap();

    let policy = build_policy(&config).unwrap();
    let mut listener = ListenerConfiguration {
        plaintext_address: "127.0.0.1:0".parse().unwrap(),
        tls_address: "127.0.0.1:0".parse().unwrap(),
        v311_backend_address: SocketAddr::from(([127, 0, 0, 1], v311)),
        v5_backend_address: SocketAddr::from(([127, 0, 0, 1], v5)),
        tls_cert_path: certificate,
        tls_key_path: key,
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        auth_handler: None,
        authorization_handler: None,
    };
    listener.auth_handler = policy.auth_handler;
    listener.authorization_handler = policy.authorization_handler;
    let handle = start_broker(listener).await.unwrap();
    TestBroker {
        v311,
        v5,
        _handle: handle,
        _directory: directory,
    }
}

fn static_config(rules: Vec<AclRule>) -> BrokerFileConfig {
    BrokerFileConfig {
        static_acl: Some(StaticAclConfig {
            enabled: true,
            deny_action: "disconnect".into(),
            users: vec![StaticUser {
                username: "alice".into(),
                password: "secret".into(),
            }],
            rules,
        }),
        ..BrokerFileConfig::default()
    }
}

fn mqtt311_options(client_id: &str, port: u16) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, "127.0.0.1", port);
    options.set_credentials("alice", "secret");
    options.set_clean_session(true);
    options
}

fn mqtt5_options(client_id: &str, port: u16) -> rumqttc::v5::MqttOptions {
    let mut options = rumqttc::v5::MqttOptions::new(client_id, "127.0.0.1", port);
    options.set_credentials("alice", "secret");
    options.set_clean_start(true);
    options
}

fn allow_all() -> AuthorizationHandler {
    Arc::new(|_request: AuthorizationRequest| Box::pin(async { true }))
}

fn policy<F>(function: F) -> AuthorizationHandler
where
    F: Fn(AuthorizationRequest) -> bool + Send + Sync + 'static,
{
    Arc::new(move |request| {
        let allowed = function(request);
        Box::pin(async move { allowed })
    })
}

fn v311_connect(client_id: &str, username: Option<&str>) -> Vec<u8> {
    let username_len = username.map_or(0, |username| 2 + username.len());
    let remaining = 10 + 2 + client_id.len() + username_len;
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        if username.is_some() { 0x82 } else { 0x02 },
        0x00,
        0x3c,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    if let Some(username) = username {
        packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
        packet.extend_from_slice(username.as_bytes());
    }
    packet
}

fn v311_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let remaining = 2 + topic.len() + payload.len();
    let mut packet = vec![0x30, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(payload);
    packet
}

fn v5_connect(client_id: &str) -> Vec<u8> {
    let remaining = 11 + 2 + client_id.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        0x02,
        0x00,
        0x3c,
        0x00,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet
}

fn v311_connect_with_credentials(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        0xc2,
        0x00,
        0x3c,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
    packet.extend_from_slice(password.as_bytes());
    packet
}

fn v5_connect_with_credentials(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 11 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        0xc2,
        0x00,
        0x3c,
        0x00,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
    packet.extend_from_slice(password.as_bytes());
    packet
}

fn v311_subscribe_filter(filter: &str) -> Vec<u8> {
    let remaining = 2 + 2 + filter.len() + 1;
    let mut packet = vec![0x82, remaining as u8, 0x00, 0x01];
    packet.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    packet.extend_from_slice(filter.as_bytes());
    packet.push(0);
    packet
}

fn v5_subscribe_filter(filter: &str) -> Vec<u8> {
    let remaining = 2 + 1 + 2 + filter.len() + 1;
    let mut packet = vec![0x82, remaining as u8, 0x00, 0x01, 0x00];
    packet.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    packet.extend_from_slice(filter.as_bytes());
    packet.push(0);
    packet
}

fn v5_publish(topic: &str, payload: &[u8], topic_alias: Option<u16>) -> Vec<u8> {
    let properties_len = usize::from(topic_alias.is_some()) * 3;
    let remaining = 2 + topic.len() + 1 + properties_len + payload.len();
    let mut packet = vec![0x30, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(properties_len as u8);
    if let Some(topic_alias) = topic_alias {
        packet.push(0x23);
        packet.extend_from_slice(&topic_alias.to_be_bytes());
    }
    packet.extend_from_slice(payload);
    packet
}

async fn connect_raw(port: u16, packet: Vec<u8>, connack_len: usize) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(&packet).await.unwrap();
    let mut connack = vec![0_u8; connack_len];
    timeout(Duration::from_secs(3), stream.read_exact(&mut connack))
        .await
        .unwrap()
        .unwrap();
    stream
}

async fn assert_v311_pubsub(port: u16, suffix: &str) {
    let topic = format!("task4/{suffix}/v311");
    let mut subscriber_options =
        MqttOptions::new(format!("task4-{suffix}-v311-subscriber"), "127.0.0.1", port);
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe(&topic, QoS::AtLeastOnce)
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

    let mut publisher_options =
        MqttOptions::new(format!("task4-{suffix}-v311-publisher"), "127.0.0.1", port);
    publisher_options.set_clean_session(true);
    let (publisher, mut publisher_events) = AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            publisher_events.poll().await.unwrap();
        }
    });
    publisher
        .publish(&topic, QoS::AtLeastOnce, false, "hello-v311")
        .await
        .unwrap();

    let payload = timeout(Duration::from_secs(3), subscriber_task)
        .await
        .unwrap()
        .unwrap();
    publisher_task.abort();
    assert_eq!(&payload[..], b"hello-v311");
}

async fn assert_v5_pubsub(port: u16, suffix: &str) {
    let topic = format!("task4/{suffix}/v5");
    let mut subscriber_options =
        rumqttc::v5::MqttOptions::new(format!("task4-{suffix}-v5-subscriber"), "127.0.0.1", port);
    subscriber_options.set_clean_start(true);
    let (subscriber, mut subscriber_events) = rumqttc::v5::AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe(&topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
        .await
        .unwrap();
    let subscriber_task = tokio::spawn(async move {
        loop {
            if let rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                return publish.payload;
            }
        }
    });

    let mut publisher_options =
        rumqttc::v5::MqttOptions::new(format!("task4-{suffix}-v5-publisher"), "127.0.0.1", port);
    publisher_options.set_clean_start(true);
    let (publisher, mut publisher_events) = rumqttc::v5::AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            publisher_events.poll().await.unwrap();
        }
    });
    publisher
        .publish(
            &topic,
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            false,
            "hello-v5",
        )
        .await
        .unwrap();

    let payload = timeout(Duration::from_secs(3), subscriber_task)
        .await
        .unwrap()
        .unwrap();
    publisher_task.abort();
    assert_eq!(&payload[..], b"hello-v5");
}

async fn start_v5_subscriber(port: u16, client_id: &str, topic: &str) -> mpsc::Receiver<Vec<u8>> {
    let mut options = rumqttc::v5::MqttOptions::new(client_id, "127.0.0.1", port);
    options.set_clean_start(true);
    let (client, mut events) = rumqttc::v5::AsyncClient::new(options, 10);
    client
        .subscribe(topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
        .await
        .unwrap();
    let (sender, receiver) = mpsc::channel(10);
    tokio::spawn(async move {
        loop {
            match events.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Publish(publish))) => {
                    sender.send(publish.payload.to_vec()).await.unwrap();
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    sleep(Duration::from_millis(100)).await;
    receiver
}

#[tokio::test]
async fn allow_all_preserves_mqtt_311_and_mqtt5_pub_sub() {
    let broker = start_test_broker(Some(allow_all())).await;
    assert_v311_pubsub(broker.v311, "allow").await;
    assert_v5_pubsub(broker.v5, "allow").await;
}

#[tokio::test]
async fn connect_denial_yields_no_accepted_session() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_policy = Arc::clone(&calls);
    let broker = start_test_broker(Some(policy(move |request| {
        assert_eq!(request.action, AuthorizationAction::Connect);
        calls_for_policy.fetch_add(1, Ordering::SeqCst);
        false
    })))
    .await;

    let options = MqttOptions::new("task4-connect-denied", "127.0.0.1", broker.v311);
    let (_, mut events) = AsyncClient::new(options, 10);
    assert!(
        timeout(Duration::from_secs(3), async {
            loop {
                if events.poll().await.is_err() {
                    break;
                }
            }
        })
        .await
        .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn publish_denial_delivers_nothing_to_a_subscriber() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Publish
    })))
    .await;
    let topic = "task4/publish-denied";

    let mut subscriber_options =
        MqttOptions::new("task4-publish-subscriber", "127.0.0.1", broker.v311);
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    let subscriber_task = tokio::spawn(async move {
        loop {
            match subscriber_events.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => return Some(publish.payload),
                Ok(_) => {}
                Err(_) => return None,
            }
        }
    });

    let mut publisher_options =
        MqttOptions::new("task4-publish-denied-publisher", "127.0.0.1", broker.v311);
    publisher_options.set_clean_session(true);
    let (publisher, mut publisher_events) = AsyncClient::new(publisher_options, 10);
    let publisher_task = tokio::spawn(async move {
        loop {
            if publisher_events.poll().await.is_err() {
                return;
            }
        }
    });
    sleep(Duration::from_millis(100)).await;
    publisher
        .publish(topic, QoS::AtLeastOnce, false, "must-not-arrive")
        .await
        .unwrap();

    assert!(
        timeout(Duration::from_millis(500), subscriber_task)
            .await
            .is_err()
    );
    publisher_task.abort();
}

#[tokio::test]
async fn subscribe_denial_does_not_register_a_subscription() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Subscribe
    })))
    .await;

    let mut options = MqttOptions::new("task4-subscribe-denied", "127.0.0.1", broker.v311);
    options.set_clean_session(true);
    let (client, mut events) = AsyncClient::new(options, 10);
    client
        .subscribe("task4/subscribe-denied", QoS::AtLeastOnce)
        .await
        .unwrap();

    let denied = timeout(Duration::from_secs(3), async {
        loop {
            if events.poll().await.is_err() {
                break true;
            }
        }
    })
    .await
    .unwrap();
    assert!(denied);
}

#[tokio::test]
async fn authorization_uses_effective_assigned_identity_and_principal() {
    let requests = Arc::new(Mutex::new(Vec::<AuthorizationRequest>::new()));
    let requests_for_policy = Arc::clone(&requests);
    let broker = start_test_broker(Some(policy(move |request| {
        requests_for_policy.lock().unwrap().push(request);
        true
    })))
    .await;

    let mut publisher = connect_raw(broker.v311, v311_connect("", Some("principal-user")), 4).await;
    publisher
        .write_all(&v311_publish("task4/effective-identity", b"payload"))
        .await
        .unwrap();

    timeout(Duration::from_secs(3), async {
        loop {
            if requests.lock().unwrap().len() >= 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let requests = requests.lock().unwrap();
    let connect = requests
        .iter()
        .find(|request| request.action == AuthorizationAction::Connect)
        .unwrap();
    let publish = requests
        .iter()
        .find(|request| request.action == AuthorizationAction::Publish)
        .unwrap();
    assert!(!connect.client_id.is_empty());
    assert_eq!(connect.client_id, publish.client_id);
    assert_eq!(connect.username.as_deref(), Some("principal-user"));
    assert_eq!(connect.principal, None);
    assert_eq!(publish.principal, None);
}

#[tokio::test]
async fn allowed_then_denied_packets_in_one_readv_batch_reach_no_router_state() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Publish
            || request.topic.as_deref() == Some("task4/batch-allowed")
    })))
    .await;
    let topic = "task4/batch/#";

    let mut subscriber_options =
        MqttOptions::new("task4-batch-subscriber", "127.0.0.1", broker.v311);
    subscriber_options.set_clean_session(true);
    let (subscriber, mut subscriber_events) = AsyncClient::new(subscriber_options, 10);
    subscriber.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    let subscriber_task = tokio::spawn(async move {
        loop {
            match subscriber_events.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => return Some(publish.payload),
                Ok(_) => {}
                Err(_) => return None,
            }
        }
    });
    sleep(Duration::from_millis(100)).await;

    let mut publisher =
        connect_raw(broker.v311, v311_connect("task4-batch-publisher", None), 4).await;
    let mut batch = v311_publish("task4/batch-allowed", b"must-not-arrive");
    batch.extend(v311_publish("task4/batch-denied", b"denied"));
    publisher.write_all(&batch).await.unwrap();

    assert!(
        timeout(Duration::from_millis(700), subscriber_task)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mqtt5_publish_denial_delivers_nothing_to_a_subscriber() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Publish
    })))
    .await;
    let mut receiver =
        start_v5_subscriber(broker.v5, "task4-v5-publish-subscriber", "task4/v5-denied").await;
    let mut publisher = connect_raw(broker.v5, v5_connect("task4-v5-publish-denied"), 8).await;
    publisher
        .write_all(&v5_publish("task4/v5-denied", b"must-not-arrive", None))
        .await
        .unwrap();

    let mut publisher_bytes = Vec::new();
    timeout(
        Duration::from_secs(3),
        publisher.read_to_end(&mut publisher_bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(publisher_bytes.is_empty());
    assert!(
        timeout(Duration::from_millis(700), receiver.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mqtt5_subscribe_denial_closes_without_a_suback() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Subscribe
    })))
    .await;
    let mut options =
        rumqttc::v5::MqttOptions::new("task4-v5-subscribe-denied", "127.0.0.1", broker.v5);
    options.set_clean_start(true);
    let (client, mut events) = rumqttc::v5::AsyncClient::new(options, 10);
    client
        .subscribe(
            "task4/v5-subscribe-denied",
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        )
        .await
        .unwrap();

    let saw_suback = timeout(Duration::from_secs(3), async {
        loop {
            match events.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::SubAck(_))) => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        }
    })
    .await
    .unwrap();
    assert!(!saw_suback);
}

#[tokio::test]
async fn mqtt5_connect_denial_closes_without_a_connack() {
    let broker = start_test_broker(Some(policy(|request| {
        request.action != AuthorizationAction::Connect
    })))
    .await;
    let options = rumqttc::v5::MqttOptions::new("task4-v5-connect-denied", "127.0.0.1", broker.v5);
    let (_, mut events) = rumqttc::v5::AsyncClient::new(options, 10);

    assert!(
        timeout(Duration::from_secs(3), events.poll())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn mqtt5_alias_only_publish_is_authorized_against_the_canonical_topic() {
    let authorized_topics = Arc::new(Mutex::new(Vec::new()));
    let captured_topics = Arc::clone(&authorized_topics);
    let broker = start_test_broker(Some(policy(move |request| {
        if request.action != AuthorizationAction::Publish {
            return true;
        }
        let topic = request.topic.unwrap_or_default();
        captured_topics.lock().unwrap().push(topic.clone());
        topic == "task4/alias-mapped"
    })))
    .await;
    let mut receiver =
        start_v5_subscriber(broker.v5, "task4-alias-subscriber", "task4/alias-mapped").await;
    let mut publisher = connect_raw(broker.v5, v5_connect("task4-alias-publisher"), 8).await;
    publisher
        .write_all(&v5_publish("task4/alias-mapped", b"first", Some(1)))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap(),
        b"first"
    );

    publisher
        .write_all(&v5_publish("", b"second", Some(1)))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap(),
        b"second"
    );
    assert_eq!(
        authorized_topics.lock().unwrap().as_slice(),
        ["task4/alias-mapped", "task4/alias-mapped"]
    );
}

#[tokio::test]
async fn mqtt5_unknown_alias_is_rejected_before_policy_evaluation() {
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let captured_calls = Arc::clone(&policy_calls);
    let broker = start_test_broker(Some(policy(move |request| {
        if request.action == AuthorizationAction::Publish {
            captured_calls.fetch_add(1, Ordering::SeqCst);
        }
        true
    })))
    .await;
    let mut publisher = connect_raw(broker.v5, v5_connect("task4-unknown-alias"), 8).await;

    publisher
        .write_all(&v5_publish("", b"unknown", Some(1)))
        .await
        .unwrap();
    let mut response = [0_u8; 1];
    let result = timeout(Duration::from_secs(3), publisher.read(&mut response))
        .await
        .unwrap();
    assert!(matches!(result, Ok(0) | Err(_)));
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn static_policy_allows_matching_publish_and_subscription_for_mqtt311_and_mqtt5() {
    let broker = start_configured_broker(static_config(vec![AclRule {
        identity: "alice".into(),
        topic: "task5/allowed/#".into(),
        publish: true,
        subscribe: true,
    }]))
    .await;

    let (subscriber, mut subscriber_events) = AsyncClient::new(
        mqtt311_options("task5-static-v311-subscriber", broker.v311),
        10,
    );
    subscriber
        .subscribe("task5/allowed/+", QoS::AtLeastOnce)
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
    let (publisher, mut publisher_events) = AsyncClient::new(
        mqtt311_options("task5-static-v311-publisher", broker.v311),
        10,
    );
    let publisher_task =
        tokio::spawn(async move { while publisher_events.poll().await.is_ok() {} });
    publisher
        .publish("task5/allowed/one", QoS::AtLeastOnce, false, "v311")
        .await
        .unwrap();
    assert_eq!(
        &timeout(Duration::from_secs(3), subscriber_task)
            .await
            .unwrap()
            .unwrap()[..],
        b"v311"
    );
    publisher_task.abort();

    let subscriber_options = mqtt5_options("task5-static-v5-subscriber", broker.v5);
    let (subscriber, mut subscriber_events) =
        rumqttc::v5::AsyncClient::new(subscriber_options.clone(), 10);
    subscriber
        .subscribe("task5/allowed/+", rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
        .await
        .unwrap();
    let subscriber_task = tokio::spawn(async move {
        loop {
            if let rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::Publish(publish)) =
                subscriber_events.poll().await.unwrap()
            {
                return publish.payload;
            }
        }
    });
    let (publisher, mut publisher_events) =
        rumqttc::v5::AsyncClient::new(mqtt5_options("task5-static-v5-publisher", broker.v5), 10);
    let publisher_task =
        tokio::spawn(async move { while publisher_events.poll().await.is_ok() {} });
    publisher
        .publish(
            "task5/allowed/one",
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            false,
            "v5",
        )
        .await
        .unwrap();
    assert_eq!(
        &timeout(Duration::from_secs(3), subscriber_task)
            .await
            .unwrap()
            .unwrap()[..],
        b"v5"
    );
    publisher_task.abort();
}

#[tokio::test]
async fn static_auth_rejects_unknown_users_without_changing_the_deny_result() {
    let policy = build_policy(&static_config(vec![])).unwrap();
    let authenticator = policy.auth_handler.unwrap();
    assert!(
        !authenticator(
            "task5-unknown-user".into(),
            "unknown".into(),
            "secret".into()
        )
        .await
    );
    assert!(
        !authenticator(
            "task5-known-user-wrong-password".into(),
            "alice".into(),
            "wrong".into()
        )
        .await
    );
    assert!(authenticator("task5-known-user".into(), "alice".into(), "secret".into()).await);
}

#[tokio::test]
async fn static_policy_denies_unmatched_publish_and_subscription_on_private_backends() {
    let broker = start_configured_broker(static_config(vec![AclRule {
        identity: "alice".into(),
        topic: "task5/allowed".into(),
        publish: true,
        subscribe: true,
    }]))
    .await;

    let mut subscriber_options = mqtt311_options("task5-static-sub-denied", broker.v311);
    subscriber_options.set_clean_session(true);
    let (subscriber, mut events) = AsyncClient::new(subscriber_options, 10);
    subscriber
        .subscribe("task5/denied", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(3), async {
            loop {
                if events.poll().await.is_err() {
                    break;
                }
            }
        })
        .await
        .is_ok()
    );

    let (allowed_subscriber, mut allowed_events) = AsyncClient::new(
        mqtt311_options("task5-static-publish-subscriber", broker.v311),
        10,
    );
    allowed_subscriber
        .subscribe("task5/allowed", QoS::AtLeastOnce)
        .await
        .unwrap();
    let allowed_task = tokio::spawn(async move {
        loop {
            match allowed_events.poll().await {
                Ok(Event::Incoming(Packet::Publish(_))) => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    });
    sleep(Duration::from_millis(100)).await;

    let (publisher, mut publisher_events) = AsyncClient::new(
        mqtt311_options("task5-static-publisher-denied", broker.v311),
        10,
    );
    let publisher_task = tokio::spawn(async move {
        loop {
            if publisher_events.poll().await.is_err() {
                return;
            }
        }
    });
    publisher
        .publish("task5/denied", QoS::AtLeastOnce, false, "denied")
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(700), allowed_task)
            .await
            .is_err()
    );
    publisher_task.abort();

    let mut v5_options = mqtt5_options("task5-static-v5-sub-denied", broker.v5);
    v5_options.set_clean_start(true);
    let (v5_client, mut v5_events) = rumqttc::v5::AsyncClient::new(v5_options, 10);
    v5_client
        .subscribe("task5/denied", rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(3), async {
            loop {
                if v5_events.poll().await.is_err() {
                    break;
                }
            }
        })
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn static_policy_rejects_malformed_subscribe_filters_for_mqtt311_and_mqtt5() {
    let broker = start_configured_broker(static_config(vec![AclRule {
        identity: "alice".into(),
        topic: "#".into(),
        publish: false,
        subscribe: true,
    }]))
    .await;

    let mut v311 = connect_raw(
        broker.v311,
        v311_connect_with_credentials("task5-invalid-v311", "alice", "secret"),
        4,
    )
    .await;
    v311.write_all(&v311_subscribe_filter("task5/#/malformed"))
        .await
        .unwrap();
    let mut v311_response = Vec::new();
    timeout(Duration::from_secs(3), v311.read_to_end(&mut v311_response))
        .await
        .unwrap()
        .unwrap();
    assert!(v311_response.is_empty());

    let mut v5 = connect_raw(
        broker.v5,
        v5_connect_with_credentials("task5-invalid-v5", "alice", "secret"),
        8,
    )
    .await;
    v5.write_all(&v5_subscribe_filter("task5/++/malformed"))
        .await
        .unwrap();
    let mut v5_response = Vec::new();
    timeout(Duration::from_secs(3), v5.read_to_end(&mut v5_response))
        .await
        .unwrap()
        .unwrap();
    assert!(v5_response.is_empty());
}

#[tokio::test]
async fn static_policy_rejects_wildcard_publish_topics_for_mqtt311_and_mqtt5() {
    let broker = start_configured_broker(static_config(vec![AclRule {
        identity: "alice".into(),
        topic: "#".into(),
        publish: true,
        subscribe: false,
    }]))
    .await;

    let mut v311 = connect_raw(
        broker.v311,
        v311_connect_with_credentials("task5-invalid-publish-v311", "alice", "secret"),
        4,
    )
    .await;
    v311.write_all(&v311_publish("task5/#", b"invalid"))
        .await
        .unwrap();
    let mut v311_response = Vec::new();
    timeout(Duration::from_secs(3), v311.read_to_end(&mut v311_response))
        .await
        .unwrap()
        .unwrap();
    assert!(v311_response.is_empty());

    let mut v5 = connect_raw(
        broker.v5,
        v5_connect_with_credentials("task5-invalid-publish-v5", "alice", "secret"),
        8,
    )
    .await;
    v5.write_all(&v5_publish("task5/+", b"invalid", None))
        .await
        .unwrap();
    let mut v5_response = Vec::new();
    timeout(Duration::from_secs(3), v5.read_to_end(&mut v5_response))
        .await
        .unwrap()
        .unwrap();
    assert!(v5_response.is_empty());
}
