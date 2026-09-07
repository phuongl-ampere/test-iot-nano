use iot_core::{
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, GATEWAY_TELEMETRY_TOPIC,
    GatewayTelemetryPayload,
};
use serde_json::json;

fn payload(value: serde_json::Value) -> GatewayTelemetryPayload {
    serde_json::from_value(value).unwrap()
}

#[test]
fn child_telemetry_builds_an_event_for_the_assigned_child() {
    let event = payload(json!({
        "schema_version": 1,
        "kind": "child_telemetry",
        "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "sequence": 1842,
        "event_at": "2026-09-07T00:00:00Z",
        "child_device_id": "019d018d-7f4e-7a92-9180-a1b2c3d4e5f6",
        "measurements": {"temperature_c": 25.4}
    }))
    .into_event("gateway-001")
    .unwrap()
    .unwrap();

    assert_eq!(event.device_id, "019d018d-7f4e-7a92-9180-a1b2c3d4e5f6");
    assert_eq!(event.gateway_device_id.as_deref(), Some("gateway-001"));
    assert_eq!(event.sequence, 1842);
}

#[test]
fn heartbeat_has_no_child_telemetry_event() {
    let event = payload(json!({
        "schema_version": 1,
        "kind": "heartbeat",
        "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "sequence": 1842,
        "event_at": "2026-09-07T00:00:00Z"
    }))
    .into_event("gateway-001")
    .unwrap();

    assert_eq!(event, None);
}

#[test]
fn gateway_topics_are_fixed_token_only_endpoints() {
    assert_eq!(GATEWAY_CONNECT_TOPIC, "v1/gateways/me/connect");
    assert_eq!(GATEWAY_DISCONNECT_TOPIC, "v1/gateways/me/disconnect");
    assert_eq!(GATEWAY_TELEMETRY_TOPIC, "v1/gateways/me/telemetry");
}
