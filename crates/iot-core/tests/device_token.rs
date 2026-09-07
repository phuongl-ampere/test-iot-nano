use chrono::{TimeZone, Utc};
use iot_core::{
    DEVICE_TELEMETRY_TOPIC, DeviceTelemetryPayload, TelemetryValidationError, device_token_prefix,
    generate_device_token,
};
use serde_json::json;
use uuid::Uuid;

fn payload() -> DeviceTelemetryPayload {
    DeviceTelemetryPayload {
        schema_version: 1,
        device_id: None,
        boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
        sequence: 1842,
        event_at: Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap(),
        measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
    }
}

#[test]
fn generated_device_tokens_are_unique_and_identifiable_by_prefix() {
    let first = generate_device_token();
    let second = generate_device_token();

    assert!(first.starts_with("iotd_"));
    assert!(second.starts_with("iotd_"));
    assert_ne!(first, second);
    assert_eq!(device_token_prefix(&first).unwrap().len(), 16);
}

#[test]
fn token_payload_maps_to_the_resolved_device_and_validates_for_me_topic() {
    let event = payload().into_event("esp-000123").unwrap();

    assert_eq!(event.device_id, "esp-000123");
    assert_eq!(event.validate_for_topic(DEVICE_TELEMETRY_TOPIC), Ok(()));
}

#[test]
fn token_payload_rejects_a_client_supplied_device_id() {
    let payload = r#"{
        "schema_version": 1,
        "device_id": "esp-attacker",
        "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "sequence": 1842,
        "event_at": "2026-09-04T10:12:00Z",
        "measurements": {"temperature_c": 26.4}
    }"#;
    let parsed = serde_json::from_str::<DeviceTelemetryPayload>(payload).unwrap();

    assert_eq!(
        parsed.into_event("esp-000123"),
        Err(TelemetryValidationError::DeviceIdNotAllowed)
    );
}
