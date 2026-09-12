use std::time::Duration;

use iot_nano_mqttd::{ProtocolBackends, serve_plaintext_mux};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn connect_packet(protocol_level: u8) -> Vec<u8> {
    let mut packet = vec![
        0x10,
        0x0e,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        protocol_level,
        0x02,
        0x00,
        0x3c,
        0x00,
        0x02,
        b'i',
        b'd',
    ];
    if protocol_level == 5 {
        packet[1] = 0x0f;
        packet.insert(12, 0x00);
    }
    packet
}

fn token_connect_packet() -> Vec<u8> {
    let username = "iotd_token";
    let remaining = 10 + 4 + 2 + username.len();
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
        0x82,
        0x00,
        0x3c,
        0x00,
        0x02,
        b'i',
        b'd',
    ];
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet
}

fn v5_token_connect_packet() -> Vec<u8> {
    let username = "iotd_token";
    let remaining = 11 + 4 + 2 + username.len();
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
        0x82,
        0x00,
        0x3c,
        0x00,
        0x00,
        0x02,
        b'i',
        b'd',
    ];
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet
}

#[tokio::test]
async fn plaintext_mux_routes_v311_and_v5_to_distinct_internal_backends() {
    let v311_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v5_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let device_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v311_address = v311_listener.local_addr().unwrap();
    let v5_address = v5_listener.local_addr().unwrap();
    let device_address = device_listener.local_addr().unwrap();
    let public_address = public_listener.local_addr().unwrap();

    let v311_task = tokio::spawn(async move {
        let (mut stream, _) = v311_listener.accept().await.unwrap();
        let mut packet = [0_u8; 16];
        stream.read_exact(&mut packet).await.unwrap();
        stream.write_all(b"v311").await.unwrap();
    });
    let v5_task = tokio::spawn(async move {
        let (mut stream, _) = v5_listener.accept().await.unwrap();
        let mut packet = [0_u8; 16];
        stream.read_exact(&mut packet).await.unwrap();
        stream.write_all(b"v5").await.unwrap();
    });
    let device_task = tokio::spawn(async move {
        let (mut stream, _) = device_listener.accept().await.unwrap();
        let mut packet = [0_u8; 28];
        stream.read_exact(&mut packet).await.unwrap();
        stream.write_all(b"device").await.unwrap();
    });
    let mux_task = tokio::spawn(serve_plaintext_mux(
        public_listener,
        ProtocolBackends {
            v311: v311_address,
            v5: v5_address,
            device_v311: Some(device_address),
            device_v5: None,
        },
    ));

    for (protocol_level, expected) in [(4, b"v311".as_slice()), (5, b"v5".as_slice())] {
        let mut client = tokio::net::TcpStream::connect(public_address)
            .await
            .unwrap();
        client
            .write_all(&connect_packet(protocol_level))
            .await
            .unwrap();
        let mut received = vec![0_u8; expected.len()];
        tokio::time::timeout(Duration::from_secs(3), client.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, expected);
    }

    let mut device_client = tokio::net::TcpStream::connect(public_address)
        .await
        .unwrap();
    device_client
        .write_all(&token_connect_packet())
        .await
        .unwrap();
    let mut device_received = [0_u8; 6];
    tokio::time::timeout(
        Duration::from_secs(3),
        device_client.read_exact(&mut device_received),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&device_received, b"device");

    v311_task.await.unwrap();
    v5_task.await.unwrap();
    device_task.await.unwrap();
    mux_task.abort();
}

#[tokio::test]
async fn plaintext_mux_closes_mqtt5_device_tokens_before_v5_backend_connect() {
    let v5_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let v5_address = v5_listener.local_addr().unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let v5_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(300), v5_listener.accept()).await
    });
    let mux_task = tokio::spawn(serve_plaintext_mux(
        public_listener,
        ProtocolBackends {
            v311: v5_address,
            v5: v5_address,
            device_v311: None,
            device_v5: None,
        },
    ));

    let mut client = tokio::net::TcpStream::connect(public_address)
        .await
        .unwrap();
    client.write_all(&v5_token_connect_packet()).await.unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(received.is_empty());
    assert!(v5_task.await.unwrap().is_err());
    mux_task.abort();
}

#[tokio::test]
async fn plaintext_mux_routes_mqtt5_device_tokens_to_native_backend() {
    let generic_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let device_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let generic_address = generic_listener.local_addr().unwrap();
    let device_address = device_listener.local_addr().unwrap();
    let public_address = public_listener.local_addr().unwrap();

    let generic_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(300), generic_listener.accept()).await
    });
    let device_task = tokio::spawn(async move {
        let (mut stream, _) = device_listener.accept().await.unwrap();
        let mut packet = vec![0_u8; v5_token_connect_packet().len()];
        stream.read_exact(&mut packet).await.unwrap();
        stream.write_all(b"device-v5").await.unwrap();
    });
    let mux_task = tokio::spawn(serve_plaintext_mux(
        public_listener,
        ProtocolBackends {
            v311: generic_address,
            v5: generic_address,
            device_v311: None,
            device_v5: Some(device_address),
        },
    ));

    let mut client = tokio::net::TcpStream::connect(public_address)
        .await
        .unwrap();
    client.write_all(&v5_token_connect_packet()).await.unwrap();
    let mut received = [0_u8; 9];
    tokio::time::timeout(Duration::from_secs(3), client.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received, b"device-v5");
    assert!(generic_task.await.unwrap().is_err());
    device_task.await.unwrap();
    mux_task.abort();
}
