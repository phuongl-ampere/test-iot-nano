use std::{
    net::{Ipv4Addr, TcpListener as StdTcpListener},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use iot_nano_mqttd::{
    ListenerConfiguration, MemoryStorage, PreboundBackendListeners,
    start_broker_with_prebound_listeners,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const CONNECT_V311: &[u8] = &[
    0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 4, 0x02, 0x00, 0x3c, 0x00, 0x02, b'i', b'd',
];
const CONNECT_V5: &[u8] = &[
    0x10, 0x0f, 0x00, 0x04, b'M', b'Q', b'T', b'T', 5, 0x02, 0x00, 0x3c, 0x00, 0x00, 0x02, b'i',
    b'd',
];

#[tokio::test]
async fn prebound_private_listeners_are_consumed_for_both_protocols_and_released_after_shutdown() {
    let v311_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let v5_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let v311_address = v311_listener.local_addr().unwrap();
    let v5_address = v5_listener.local_addr().unwrap();
    let v311_guard = v311_listener.try_clone().unwrap();
    let v5_guard = v5_listener.try_clone().unwrap();

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let broker = start_broker_with_prebound_listeners(
        ListenerConfiguration {
            plaintext_address: "127.0.0.1:0".parse().unwrap(),
            tls_address: "127.0.0.1:0".parse().unwrap(),
            v311_backend_address: v311_address,
            v5_backend_address: v5_address,
            tls_cert_path: fixtures.join("server.crt"),
            tls_key_path: fixtures.join("server.key"),
            websocket_address: None,
            websocket_tls: false,
            bridge: None,
            max_connections: 32,
            max_payload_size: 1024 * 1024,
            max_inflight_count: 16,
            auth_handler: None,
            authorization_handler: None,
        },
        PreboundBackendListeners {
            v311: v311_listener,
            v5: v5_listener,
        },
        Arc::new(MemoryStorage::new()),
    )
    .await
    .unwrap();

    assert_connack(v311_address, CONNECT_V311, &[0x20, 0x02, 0x00, 0x00]).await;
    assert_connack(v5_address, CONNECT_V5, &[0x20, 0x06, 0x00, 0x00, 0x03]).await;

    broker.shutdown();
    broker.join().unwrap();
    drop((v311_guard, v5_guard));

    StdTcpListener::bind(v311_address).unwrap();
    StdTcpListener::bind(v5_address).unwrap();
}

async fn assert_connack(address: std::net::SocketAddr, connect: &[u8], expected: &[u8]) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(connect).await.unwrap();
    let mut connack = vec![0_u8; expected.len()];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut connack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connack, expected);
}
