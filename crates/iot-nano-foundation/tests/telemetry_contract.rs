use chrono::{TimeZone, Utc};
use iot_nano_foundation::{TelemetryEvent, TelemetryValidationError};
use serde_json::json;
use uuid::Uuid;

fn event(
    device_id: &str,
    measurements: serde_json::Map<String, serde_json::Value>,
) -> TelemetryEvent {
    TelemetryEvent {
        schema_version: 1,
        device_id: device_id.to_owned(),
        boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
        sequence: 1842,
        event_at: Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap(),
        measurements,
        gateway_device_id: None,
    }
}

#[test]
fn accepts_a_v1_event_for_its_device_topic() {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));

    let actual =
        event("esp-000123", measurements).validate_for_topic("iot/v1/devices/esp-000123/telemetry");

    assert_eq!(actual, Ok(()));
}

#[test]
fn rejects_a_topic_whose_device_id_does_not_match_the_payload() {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));

    let actual =
        event("esp-000123", measurements).validate_for_topic("iot/v1/devices/esp-000456/telemetry");

    assert_eq!(
        actual,
        Err(TelemetryValidationError::TopicDeviceMismatch {
            topic_device_id: "esp-000456".to_owned(),
            payload_device_id: "esp-000123".to_owned(),
        })
    );
}

#[test]
fn rejects_an_unsupported_schema_version() {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));
    let mut event = event("esp-000123", measurements);
    event.schema_version = 2;

    let actual = event.validate_for_topic("iot/v1/devices/esp-000123/telemetry");

    assert_eq!(
        actual,
        Err(TelemetryValidationError::UnsupportedSchemaVersion(2))
    );
}

#[test]
fn rejects_an_empty_measurement_set() {
    let actual = event("esp-000123", serde_json::Map::new())
        .validate_for_topic("iot/v1/devices/esp-000123/telemetry");

    assert_eq!(actual, Err(TelemetryValidationError::EmptyMeasurements));
}
