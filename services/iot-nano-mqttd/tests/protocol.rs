use iot_nano_mqttd::{ConnectRoute, MqttProtocol, detect_connect_protocol, detect_connect_route};

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

fn v311_connect_with_username(username: &str) -> Vec<u8> {
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

fn v5_connect_with_username(username: &str) -> Vec<u8> {
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

fn v311_connect_with_will_and_username(username: &str) -> Vec<u8> {
    let remaining = 10 + 4 + 5 + 4 + 2 + username.len();
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
        0x86,
        0x00,
        0x3c,
        0x00,
        0x02,
        b'i',
        b'd',
        0x00,
        0x03,
        b'w',
        b'i',
        b'l',
        0x00,
        0x02,
        0xff,
        0x00,
    ];
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet
}

#[test]
fn detect_connect_protocol_distinguishes_mqtt_311_and_5() {
    assert_eq!(
        detect_connect_protocol(&connect_packet(4)).unwrap(),
        MqttProtocol::V311
    );
    assert_eq!(
        detect_connect_protocol(&connect_packet(5)).unwrap(),
        MqttProtocol::V5
    );
}

#[test]
fn detect_connect_protocol_rejects_non_connect_and_truncated_packets() {
    assert!(detect_connect_protocol(&[0x30, 0x00]).is_err());
    assert!(detect_connect_protocol(&[0x10, 0x0e, 0x00, 0x04, b'M']).is_err());
}

#[test]
fn connect_route_sends_token_devices_to_the_native_transport_backend() {
    assert_eq!(
        detect_connect_route(&v311_connect_with_username("iotd_token")).unwrap(),
        ConnectRoute::DeviceV311
    );
    assert_eq!(
        detect_connect_route(&v311_connect_with_username("generic-user")).unwrap(),
        ConnectRoute::Broker(MqttProtocol::V311)
    );
    assert_eq!(
        detect_connect_route(&connect_packet(5)).unwrap(),
        ConnectRoute::Broker(MqttProtocol::V5)
    );
}

#[test]
fn mqtt5_device_tokens_route_to_the_native_transport_backend() {
    assert_eq!(
        detect_connect_route(&v5_connect_with_username("iotd_token")).unwrap(),
        ConnectRoute::DeviceV5
    );
    assert_eq!(
        detect_connect_route(&v5_connect_with_username("generic-user")).unwrap(),
        ConnectRoute::Broker(MqttProtocol::V5)
    );
}

#[test]
fn mqtt311_will_payload_is_skipped_before_device_username() {
    assert_eq!(
        detect_connect_route(&v311_connect_with_will_and_username("iotd_token")).unwrap(),
        ConnectRoute::DeviceV311
    );
}
