use chrono::{Duration, TimeZone, Utc};
use iot_nano_foundation::{
    CommandState, RpcMode, RpcRequest, RpcRequestValidationError, RpcTarget,
};
use serde_json::json;
use uuid::Uuid;

fn issued_at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 7, 8, 0, 0).unwrap()
}

fn valid_request() -> RpcRequest {
    let issued_at = issued_at();
    RpcRequest::new(
        Uuid::now_v7(),
        "sample_now",
        json!({ "force": true }),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap()
}

#[test]
fn builds_and_validates_a_one_way_rpc_request() {
    let request = valid_request();

    assert_eq!(request.method, "sample_now");
    assert_eq!(request.params, json!({ "force": true }));
    assert_eq!(request.mode, RpcMode::OneWay);
    assert_eq!(request.validate(), Ok(()));
}

#[test]
fn builds_and_validates_a_two_way_rpc_request() {
    let issued_at = issued_at();
    let request = RpcRequest::with_mode(
        Uuid::now_v7(),
        "sample_now",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
        RpcMode::TwoWay,
    )
    .unwrap();

    assert_eq!(request.mode, RpcMode::TwoWay);
    assert_eq!(
        serde_json::to_value(request).unwrap()["mode"],
        json!("two_way")
    );
}

#[test]
fn rejects_a_request_id_that_is_not_uuidv7_or_uuidv5() {
    let issued_at = issued_at();

    let actual = RpcRequest::new(
        Uuid::new_v4(),
        "sample_now",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    );

    assert_eq!(actual, Err(RpcRequestValidationError::IdMustBeUuidV7OrV5));
}

#[test]
fn builds_a_deterministic_uuidv5_rpc_request() {
    let issued_at = issued_at();
    let actual = RpcRequest::new(
        Uuid::new_v5(&Uuid::NAMESPACE_URL, b"public-command"),
        "sample_now",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    );

    assert!(actual.is_ok());
}

#[test]
fn rejects_invalid_method_names() {
    let issued_at = issued_at();
    let invalid_methods = [
        "".to_owned(),
        "sample now".to_owned(),
        "sample.now".to_owned(),
        "nhiet_do".replace("do", "độ"),
        "a".repeat(65),
    ];

    for method in invalid_methods {
        let actual = RpcRequest::new(
            Uuid::now_v7(),
            method,
            json!({}),
            issued_at,
            issued_at + Duration::seconds(30),
        );

        assert_eq!(actual, Err(RpcRequestValidationError::InvalidMethod));
    }
}

#[test]
fn rejects_non_object_params() {
    let issued_at = issued_at();

    for params in [json!(null), json!(true), json!(42), json!("on"), json!([])] {
        let actual = RpcRequest::new(
            Uuid::now_v7(),
            "sample_now",
            params,
            issued_at,
            issued_at + Duration::seconds(30),
        );

        assert_eq!(actual, Err(RpcRequestValidationError::ParamsMustBeObject));
    }
}

#[test]
fn rejects_an_expiry_at_or_before_issue_time() {
    let issued_at = issued_at();

    for expires_at in [issued_at, issued_at - Duration::seconds(1)] {
        let actual = RpcRequest::new(
            Uuid::now_v7(),
            "sample_now",
            json!({}),
            issued_at,
            expires_at,
        );

        assert_eq!(
            actual,
            Err(RpcRequestValidationError::ExpirationNotAfterIssuedAt)
        );
    }
}

#[test]
fn provides_target_constructors_and_stable_command_state_wire_values() {
    assert_eq!(
        RpcTarget::direct("device-01"),
        RpcTarget::DirectDevice {
            device_id: "device-01".to_owned(),
        }
    );
    assert_eq!(
        RpcTarget::gateway_child("gateway-01", "child-01"),
        RpcTarget::GatewayChild {
            gateway_device_id: "gateway-01".to_owned(),
            child_device_id: "child-01".to_owned(),
        }
    );
    assert_eq!(
        serde_json::to_value(CommandState::PublishedToBroker).unwrap(),
        json!("published_to_broker")
    );
    assert_eq!(
        serde_json::to_value(CommandState::Responded).unwrap(),
        json!("responded")
    );
}
