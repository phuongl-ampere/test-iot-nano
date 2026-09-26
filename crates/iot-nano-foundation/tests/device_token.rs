use chrono::{TimeZone, Utc};
use iot_nano_foundation::{
    DEVICE_TELEMETRY_TOPIC, DeviceTelemetryPayload, TelemetryValidationError, device_token_prefix,
    generate_device_token,
};
use serde_json::json;

fn payload() -> DeviceTelemetryPayload {
    serde_json::from_str(r#"{"ts":1780000000123,"values":{"temperature_c":26.4}}"#).unwrap()
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
fn direct_payload_accepts_thingsboard_key_value_format() {
    let parsed =
        serde_json::from_str::<DeviceTelemetryPayload>(r#"{"temperature_c":26.4,"enabled":true}"#)
            .unwrap();

    assert_eq!(parsed.measurements["temperature_c"], json!(26.4));
    assert_eq!(parsed.measurements["enabled"], json!(true));
}

#[test]
fn direct_payload_accepts_thingsboard_timestamped_values_format() {
    let parsed = serde_json::from_str::<DeviceTelemetryPayload>(
        r#"{"ts":1780000000123,"values":{"temperature_c":26.4,"enabled":true}}"#,
    )
    .unwrap();

    assert_eq!(parsed.measurements["temperature_c"], json!(26.4));
    assert_eq!(parsed.measurements["enabled"], json!(true));
}

#[test]
fn token_payload_maps_to_the_resolved_device_and_validates_for_me_topic() {
    let event = payload()
        .into_event(
            "esp-000123",
            Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap(),
        )
        .unwrap();

    assert_eq!(event.device_id, "esp-000123");
    assert_eq!(event.validate_for_topic(DEVICE_TELEMETRY_TOPIC), Ok(()));
}

#[test]
fn direct_payload_uses_receive_time_when_ts_is_absent() {
    let received_at = Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap();
    let parsed =
        serde_json::from_str::<DeviceTelemetryPayload>(r#"{"temperature_c":26.4}"#).unwrap();

    let event = parsed.into_event("esp-000123", received_at).unwrap();

    assert_eq!(event.event_at, received_at);
    assert_eq!(event.schema_version, 1);
    assert_eq!(event.sequence, 0);
    assert_ne!(event.boot_id, uuid::Uuid::nil());
}

#[test]
fn direct_payload_uses_thingsboard_timestamp_when_supplied() {
    let received_at = Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap();
    let parsed = serde_json::from_str::<DeviceTelemetryPayload>(
        r#"{"ts":1780000000123,"values":{"temperature_c":26.4}}"#,
    )
    .unwrap();

    let event = parsed.into_event("esp-000123", received_at).unwrap();

    assert_eq!(event.event_at.timestamp_millis(), 1_780_000_000_123);
}

#[test]
fn direct_payload_allows_ts_without_a_values_wrapper() {
    let received_at = Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap();
    let parsed = serde_json::from_str::<DeviceTelemetryPayload>(
        r#"{"ts":1780000000123,"temperature_c":26.4}"#,
    )
    .unwrap();

    let event = parsed.into_event("esp-000123", received_at).unwrap();

    assert_eq!(event.event_at.timestamp_millis(), 1_780_000_000_123);
    assert_eq!(event.measurements["temperature_c"], json!(26.4));
}

#[test]
fn token_payload_rejects_a_client_supplied_device_id() {
    let payload = r#"{
        "device_id": "esp-attacker",
        "temperature_c": 26.4
    }"#;
    let parsed = serde_json::from_str::<DeviceTelemetryPayload>(payload).unwrap();

    assert_eq!(
        parsed.into_event(
            "esp-000123",
            Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap(),
        ),
        Err(TelemetryValidationError::DeviceIdNotAllowed)
    );
}
