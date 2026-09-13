use std::fs;

use serde_json::Value;

const CONTRACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/public-api-v1.json"
);

#[test]
fn public_api_v1_contract_contains_only_the_required_public_surface() {
    let contract: Value = serde_json::from_str(
        &fs::read_to_string(CONTRACT_PATH).expect("public API contract must exist"),
    )
    .expect("public API contract must be valid JSON");

    assert_eq!(contract["version"], "v1");

    for resource in [
        "devices",
        "assets",
        "telemetry",
        "alerts",
        "commands",
        "authorization",
    ] {
        let resource_definition = &contract["resources"][resource];
        assert!(
            resource_definition.is_object(),
            "missing required resource: {resource}"
        );
        assert!(
            resource_definition["operations"].is_object(),
            "resource {resource} must define operations"
        );
        for operation in resource_definition["operations"]
            .as_object()
            .expect("operations must be an object")
            .values()
        {
            assert!(
                operation["scope"]
                    .as_str()
                    .is_some_and(|scope| !scope.is_empty()),
                "resource {resource} operation must define an OAuth scope"
            );
            assert!(
                operation["path"]
                    .as_str()
                    .is_some_and(|path| path.starts_with("/api/v1/")),
                "resource {resource} operation must be mounted under /api/v1"
            );
        }
    }

    let error = &contract["error_envelope"];
    for field in ["code", "message", "request_id"] {
        assert!(
            error["required"]
                .as_array()
                .is_some_and(|fields| fields.iter().any(|value| value == field)),
            "error envelope must require {field}"
        );
    }

    assert_eq!(contract["pagination"]["style"], "cursor");
    assert_eq!(
        contract["resources"]["commands"]["idempotency"]["header"],
        "Idempotency-Key"
    );
    assert_eq!(
        contract["resources"]["commands"]["idempotency"]["replay_behavior"],
        "same_key_and_payload_returns_original_result"
    );

    assert_eq!(
        contract["oauth"]["authorization_code_pkce"]["path"],
        "/oauth/authorize"
    );
    assert_eq!(
        contract["oauth"]["client_credentials"]["path"],
        "/oauth/token"
    );
    for flow in ["authorization_code_pkce", "client_credentials"] {
        assert!(
            contract["oauth"][flow]["path"]
                .as_str()
                .is_some_and(|path| path.starts_with("/oauth/")),
            "OAuth flow {flow} must be mounted under /oauth"
        );
    }
    for error_value in [
        "invalid_request",
        "invalid_client",
        "invalid_grant",
        "invalid_scope",
        "unauthorized_client",
    ] {
        assert!(
            contract["oauth"]["error_values"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == error_value)),
            "OAuth error values must include {error_value}"
        );
    }

    let serialized = contract.to_string().to_ascii_lowercase();
    assert!(!serialized.contains("/internal"));
    assert!(!serialized.contains("powermonitor"));
    assert!(!serialized.contains("x-iot-nano"));
    assert!(!serialized.contains("database_url"));
    assert!(!serialized.contains("postgres"));
}
