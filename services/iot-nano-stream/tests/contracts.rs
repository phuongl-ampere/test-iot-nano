use serde_json::Value;

fn contract(name: &str) -> Value {
    let path = format!("{}/../../contracts/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn versioned_contracts_keep_required_internal_boundaries() {
    let telemetry = contract("telemetry-v1.json");
    assert_eq!(telemetry["$id"], "iot-nano/telemetry-v1");
    assert_eq!(telemetry["properties"]["schema_version"]["const"], 1);

    let stream = contract("stream-v1.json");
    assert_eq!(
        stream["append"]["secret_header"],
        "x-iot-nano-mqttd-stream-secret"
    );
    assert_eq!(
        stream["groups"]["secret_header"],
        "x-iot-nano-core-stream-secret"
    );
    assert_eq!(
        stream["append"]["path"],
        "/internal/streams/telemetry/append"
    );

    let rpc = contract("rpc-v1.json");
    assert_eq!(rpc["$id"], "iot-nano/rpc-v1");
    assert_eq!(rpc["properties"]["mode"]["enum"][0], "one_way");
    assert_eq!(rpc["properties"]["mode"]["enum"][1], "two_way");
}

#[test]
fn gateway_telemetry_contract_requires_authorized_idempotent_events() {
    let gateway = contract("gateway-telemetry-v1.json");
    assert_eq!(gateway["$id"], "iot-nano/gateway-telemetry-v1");
    assert_eq!(gateway["properties"]["schema_version"]["const"], 1);
    assert_eq!(
        gateway["properties"]["event_kind"]["enum"],
        serde_json::json!(["connect", "disconnect", "heartbeat", "child_telemetry"])
    );

    let required = gateway["required"].as_array().unwrap();
    for field in [
        "gateway_device_id",
        "token_id",
        "event_kind",
        "event_at",
        "payload",
        "idempotency_key",
    ] {
        assert!(
            required.iter().any(|value| value == field),
            "{field} must be required"
        );
    }
}

#[test]
fn internal_api_contract_uses_target_service_identities() {
    let internal = contract("internal-api-v1.json");

    assert_eq!(
        internal["services"]["mqttd_to_api"]["header"],
        "x-iot-nano-mqttd-api-secret"
    );
    assert!(
        internal["services"]["mqttd_to_api"]["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .any(|endpoint| endpoint == "/internal/mqttd/gateway-authorization")
    );
    assert_eq!(
        internal["services"]["mqttd_to_stream"]["header"],
        "x-iot-nano-mqttd-stream-secret"
    );
    assert_eq!(
        internal["services"]["core_to_stream"]["header"],
        "x-iot-nano-core-stream-secret"
    );
    assert_eq!(
        internal["services"]["api_to_core"]["header"],
        "x-iot-nano-api-core-secret"
    );
    assert!(
        internal["services"]["api_to_core"]["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .any(|endpoint| endpoint == "/internal/telemetry/devices/{device_id}")
    );
}
