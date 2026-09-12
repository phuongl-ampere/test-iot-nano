use std::{net::SocketAddr, path::PathBuf};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::Engine as _;
use iot_nano_mqttd as iot_mqttd;
use iot_nano_mqttd::{
    BrokerFileConfig, Capability, ConfigError, DeviceTransportEndpoints, ListenerConfiguration,
    ManagementConfig, RuntimeAddressConfiguration, RuntimeConfigurationError,
    management_router_with_config, management_router_with_config_and_runtime_and_policy,
    resolve_runtime_configuration,
};
use rumqttd::Transport;
use tower::ServiceExt;

fn minimal_config() -> BrokerFileConfig {
    BrokerFileConfig::default()
}

#[test]
fn broker_uses_the_audited_local_core_upstream() {
    assert_eq!(rumqttd::IOT_MQTT_CORE_UPSTREAM, "bytebeamio/rumqtt@0.20.0");
}

#[test]
fn defaults_are_versioned_and_match_the_current_listener_contract() {
    let config = minimal_config();

    assert_eq!(config.version, 1);
    assert_eq!(
        config.listeners.tcp.address,
        "0.0.0.0:1883".parse().unwrap()
    );
    assert_eq!(
        config.listeners.tls.address,
        "0.0.0.0:8883".parse().unwrap()
    );
    assert_eq!(config.broker.max_connections, 10_000);
    assert_eq!(config.broker.max_payload_size, 2 * 1024 * 1024);
    assert_eq!(config.management.address, "127.0.0.1:8082".parse().unwrap());
    assert!(config.storage.is_memory());
    assert!(BrokerFileConfig::supported_capabilities().contains(&Capability::Mqtt5TopicAlias));
    assert_eq!(
        config.device_transport.protocol,
        iot_mqttd::NativeDeviceProtocol::Both
    );
}

#[test]
fn parser_rejects_unknown_keys_and_invalid_semantics() {
    let unknown = toml::from_str::<BrokerFileConfig>("version = 1\nunknown = true");
    assert!(unknown.is_err());
    let unknown_storage = toml::from_str::<BrokerFileConfig>(
        "version = 1\n[storage]\nkind = \"memory\"\nextra = true",
    );
    assert!(unknown_storage.is_err());

    let invalid = BrokerFileConfig {
        version: 2,
        ..minimal_config()
    };
    assert!(matches!(
        invalid.validate(),
        Err(ConfigError::UnsupportedSchemaVersion(2))
    ));

    let invalid_limit = BrokerFileConfig {
        broker: iot_mqttd::BrokerLimits {
            max_payload_size: 0,
            ..minimal_config().broker
        },
        ..minimal_config()
    };
    assert!(matches!(
        invalid_limit.validate(),
        Err(ConfigError::InvalidValue {
            field: "broker.max_payload_size",
            ..
        })
    ));

    let mut empty_management_password = minimal_config();
    empty_management_password.management.username = Some("admin".into());
    empty_management_password.management.password = Some(String::new());
    assert!(matches!(
        empty_management_password.validate(),
        Err(ConfigError::InvalidValue {
            field: "management.credentials",
            ..
        })
    ));
}

#[test]
fn sqlite_storage_configuration_is_enabled_with_a_database_path() {
    let config = BrokerFileConfig::from_toml(
        "version = 1\n[storage]\nkind = \"sqlite\"\npath = \"broker.sqlite\"",
    )
    .unwrap();

    assert!(matches!(
        config.storage,
        iot_mqttd::StorageConfig::Sqlite { .. }
    ));
    assert!(BrokerFileConfig::supported_capabilities().contains(&Capability::SqlitePersistence));
}

#[test]
fn enabled_policy_configurations_are_complete_and_mutually_exclusive() {
    let mut static_config = minimal_config();
    static_config.static_acl = Some(iot_mqttd::StaticAclConfig {
        enabled: true,
        deny_action: "disconnect".into(),
        users: vec![iot_mqttd::StaticUser {
            username: "alice".into(),
            password: "secret".into(),
        }],
        rules: vec![],
    });
    assert!(static_config.validate().is_ok());

    let mut missing_static_user = static_config.clone();
    missing_static_user
        .static_acl
        .as_mut()
        .unwrap()
        .users
        .clear();
    assert!(matches!(
        missing_static_user.validate(),
        Err(ConfigError::InvalidValue {
            field: "static_acl.users",
            ..
        })
    ));

    let mut http_config = minimal_config();
    http_config.http_authorization = Some(iot_mqttd::HttpAuthorizationConfig {
        enabled: true,
        url: "http://127.0.0.1:8080/authorize".into(),
        secret: "authorization-secret".into(),
        timeout_ms: 100,
        cache_ttl_seconds: 1,
        cache_capacity: 8,
        deny_action: "disconnect".into(),
    });
    assert!(http_config.validate().is_ok());

    let mut missing_http_url = http_config.clone();
    missing_http_url
        .http_authorization
        .as_mut()
        .unwrap()
        .url
        .clear();
    assert!(matches!(
        missing_http_url.validate(),
        Err(ConfigError::InvalidValue {
            field: "http_authorization.url",
            ..
        })
    ));

    let mut both = static_config;
    both.http_authorization = http_config.http_authorization;
    assert!(matches!(
        both.validate(),
        Err(ConfigError::InvalidValue {
            field: "policy",
            ..
        })
    ));
}

#[test]
fn static_users_and_policy_cache_settings_are_validated() {
    let mut duplicate_users = minimal_config();
    duplicate_users.static_acl = Some(iot_mqttd::StaticAclConfig {
        enabled: true,
        deny_action: "disconnect".into(),
        users: vec![
            iot_mqttd::StaticUser {
                username: "alice".into(),
                password: "first".into(),
            },
            iot_mqttd::StaticUser {
                username: "alice".into(),
                password: "second".into(),
            },
        ],
        rules: vec![],
    });
    assert!(matches!(
        duplicate_users.validate(),
        Err(ConfigError::InvalidValue {
            field: "static_acl.users",
            ..
        })
    ));

    let mut empty_password = duplicate_users.clone();
    empty_password
        .static_acl
        .as_mut()
        .unwrap()
        .users
        .truncate(1);
    empty_password.static_acl.as_mut().unwrap().users[0]
        .password
        .clear();
    assert!(matches!(
        empty_password.validate(),
        Err(ConfigError::InvalidValue {
            field: "static_acl.users",
            ..
        })
    ));

    let mut unsupported_action = duplicate_users.clone();
    unsupported_action
        .static_acl
        .as_mut()
        .unwrap()
        .users
        .truncate(1);
    unsupported_action.static_acl.as_mut().unwrap().deny_action = "reject-packet".into();
    assert!(matches!(
        unsupported_action.validate(),
        Err(ConfigError::InvalidValue {
            field: "static_acl.deny_action",
            ..
        })
    ));

    let mut invalid_cache = minimal_config();
    invalid_cache.http_authorization = Some(iot_mqttd::HttpAuthorizationConfig {
        enabled: true,
        url: "http://127.0.0.1:8080/authorize".into(),
        secret: "authorization-secret".into(),
        timeout_ms: 100,
        cache_ttl_seconds: 0,
        cache_capacity: 0,
        deny_action: "disconnect".into(),
    });
    assert!(matches!(
        invalid_cache.validate(),
        Err(ConfigError::InvalidValue {
            field: "http_authorization.cache_capacity",
            ..
        })
    ));

    invalid_cache
        .http_authorization
        .as_mut()
        .unwrap()
        .cache_capacity = 8;
    invalid_cache
        .http_authorization
        .as_mut()
        .unwrap()
        .cache_ttl_seconds = u64::MAX;
    assert!(matches!(
        invalid_cache.validate(),
        Err(ConfigError::InvalidValue {
            field: "http_authorization.cache_ttl_seconds",
            ..
        })
    ));
}

#[tokio::test]
async fn policy_capabilities_are_reported_only_after_runtime_wiring() {
    let mut config = minimal_config();
    config.static_acl = Some(iot_mqttd::StaticAclConfig {
        enabled: true,
        deny_action: "disconnect".into(),
        users: vec![iot_mqttd::StaticUser {
            username: "alice".into(),
            password: "secret".into(),
        }],
        rules: vec![],
    });
    config.management.username = Some("admin".into());
    config.management.password = Some("password".into());

    let before = management_router_with_config(config.clone());
    let before_response = before
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(before_response.status(), StatusCode::OK);
    let before_body = to_bytes(before_response.into_body(), 16 * 1024)
        .await
        .unwrap();
    let before_body: serde_json::Value = serde_json::from_slice(&before_body).unwrap();
    assert!(
        !before_body["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "static_acl")
    );

    let after = management_router_with_config_and_runtime_and_policy(config, false, true);
    let after_response = after
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_response.status(), StatusCode::OK);
    let body = to_bytes(after_response.into_body(), 16 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        body["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "static_acl")
    );
}

#[test]
fn every_unsupported_enabled_capability_is_rejected_explicitly() {
    let cases: Vec<(&str, Box<dyn Fn(&mut BrokerFileConfig)>, Capability)> = vec![(
        "quic",
        Box::new(|config: &mut BrokerFileConfig| config.listeners.quic.enabled = true),
        Capability::Quic,
    )];

    for (name, enable, capability) in cases {
        let mut config = minimal_config();
        enable(&mut config);
        let error = config.validate().expect_err(name);
        let message = error.to_string();
        assert!(matches!(
            error,
            ConfigError::UnsupportedCapability(found) if found == capability
        ));
        assert!(message.contains("unsupported capability"));
    }
}

#[test]
fn websocket_and_wss_configuration_are_supported() {
    let mut websocket = minimal_config();
    websocket.listeners.websocket.enabled = true;
    assert!(websocket.validate().is_ok());

    let mut wss = minimal_config();
    wss.listeners.websocket.enabled = true;
    wss.listeners.websocket.tls = true;
    assert!(wss.validate().is_ok());
}

#[test]
fn config_converts_to_the_existing_supported_listener_configuration() {
    let mut config = minimal_config();
    config.listeners.tcp.address = "127.0.0.1:2883".parse().unwrap();
    config.listeners.tls.address = "127.0.0.1:2884".parse().unwrap();
    config.listeners.tls.certificate_path = "cert.pem".into();
    config.listeners.tls.key_path = "key.pem".into();
    config.broker.max_connections = 42;
    config.broker.max_payload_size = 8192;
    config.broker.max_inflight_count = 7;

    let listener: ListenerConfiguration = config.to_listener_configuration().unwrap();
    assert_eq!(
        listener.plaintext_address,
        "127.0.0.1:2883".parse().unwrap()
    );
    assert_eq!(listener.tls_address, "127.0.0.1:2884".parse().unwrap());
    assert_eq!(listener.max_connections, 42);
    assert_eq!(listener.max_payload_size, 8192);
    assert_eq!(listener.max_inflight_count, 7);
    assert_eq!(listener.tls_cert_path, std::path::Path::new("cert.pem"));
    assert_eq!(listener.tls_key_path, std::path::Path::new("key.pem"));
}

#[test]
fn supplied_config_addresses_are_the_resolved_public_and_backend_addresses() {
    let mut config = minimal_config();
    config.listeners.tcp.address = "127.0.0.1:2883".parse().unwrap();
    config.listeners.tls.address = "127.0.0.1:2884".parse().unwrap();
    config.listeners.v311_backend_address = "127.0.0.1:28831".parse().unwrap();
    config.listeners.v5_backend_address = "127.0.0.1:28832".parse().unwrap();
    config.listeners.device_backend_address = "127.0.0.1:28833".parse().unwrap();
    config.listeners.device_v5_backend_address = "127.0.0.1:28834".parse().unwrap();

    let resolved = resolve_runtime_configuration(
        Some(&config),
        RuntimeAddressConfiguration {
            plaintext_address: "127.0.0.1:3883".parse().unwrap(),
            tls_address: "127.0.0.1:3884".parse().unwrap(),
            v311_backend_address: "127.0.0.1:38831".parse().unwrap(),
            v5_backend_address: "127.0.0.1:38832".parse().unwrap(),
            device_backend_address: "127.0.0.1:38833".parse().unwrap(),
            device_v5_backend_address: "127.0.0.1:38834".parse().unwrap(),
            ..RuntimeAddressConfiguration::default()
        },
    )
    .unwrap();

    assert_eq!(
        resolved.listener.plaintext_address,
        config.listeners.tcp.address
    );
    assert_eq!(resolved.listener.tls_address, config.listeners.tls.address);
    assert_eq!(
        resolved.listener.v311_backend_address,
        config.listeners.v311_backend_address
    );
    assert_eq!(
        resolved.listener.v5_backend_address,
        config.listeners.v5_backend_address
    );
    assert_eq!(
        resolved.backends.v311,
        config.listeners.v311_backend_address
    );
    assert_eq!(resolved.backends.v5, config.listeners.v5_backend_address);
    assert_eq!(
        resolved.device_backend_address,
        config.listeners.device_backend_address
    );
    assert_eq!(
        resolved.device_v5_backend_address,
        config.listeners.device_v5_backend_address
    );
}

#[test]
fn mqtt5_native_device_transport_configuration_is_supported() {
    let mut config = minimal_config();
    config.device_transport.enabled = true;
    config.device_transport.protocol = iot_mqttd::NativeDeviceProtocol::Mqtt5;

    assert!(config.validate().is_ok());
    let resolved =
        resolve_runtime_configuration(Some(&config), RuntimeAddressConfiguration::default())
            .unwrap();
    assert!(resolved.backends.device_v311.is_none());
    assert_eq!(
        resolved.backends.device_v5,
        Some(config.listeners.device_v5_backend_address)
    );
}

#[test]
fn tcp_bridge_configuration_maps_to_the_controlled_broker() {
    let mut config = minimal_config();
    config.bridges.push(iot_mqttd::BridgeConfig {
        name: "upstream".into(),
        enabled: true,
        address: "127.0.0.1:1883".into(),
        tls: false,
        ca_certificate_path: None,
    });
    let listener = config.to_listener_configuration().unwrap();
    let bridge = listener.bridge.expect("enabled bridge is mapped");
    assert_eq!(bridge.name, "upstream");
    assert_eq!(bridge.addr, "127.0.0.1:1883");
    assert_eq!(bridge.sub_path, "#");
    assert_eq!(bridge.qos, 1);
}

#[test]
fn tls_bridge_configuration_requires_and_maps_a_ca_path() {
    let mut config = minimal_config();
    config.bridges.push(iot_mqttd::BridgeConfig {
        name: "upstream-tls".into(),
        enabled: true,
        address: "localhost:8883".into(),
        tls: true,
        ca_certificate_path: None,
    });
    assert!(config.validate().is_err());

    config.bridges[0].ca_certificate_path = Some("/etc/iot/ca.pem".into());
    let listener = config.to_listener_configuration().unwrap();
    assert!(matches!(
        listener.bridge.unwrap().transport,
        Transport::Tls { ca, client_auth: None } if ca == PathBuf::from("/etc/iot/ca.pem")
    ));
}

#[test]
fn webhook_configuration_is_rejected() {
    let result = BrokerFileConfig::from_toml(
        "version = 1
         [webhook]
         enabled = true
         url = \"http://127.0.0.1:18081/hook\"
         secret = \"webhook-secret\"
         topics = [\"telemetry/#\"]
         pool_size = 2
         queue_capacity = 16
         timeout_ms = 500
         retries = 1",
    );

    assert!(result.is_err());
}

#[test]
fn republish_rule_configuration_requires_a_non_looping_topic_mapping() {
    let mut config = minimal_config();
    config.rules.push(iot_mqttd::RuleConfig {
        name: "forward".into(),
        enabled: true,
        expression: String::new(),
        source_topic: "source/#".into(),
        target_topic: "target/telemetry".into(),
    });
    assert!(config.validate().is_ok());
}

#[test]
fn config_mode_controls_native_device_transport_activation() {
    let endpoints = DeviceTransportEndpoints {
        api_base_url: Some("http://api".into()),
        transport_secret: Some("transport-secret".into()),
    };
    let disabled = resolve_runtime_configuration(
        Some(&minimal_config()),
        RuntimeAddressConfiguration::default(),
    )
    .unwrap();
    assert!(!disabled.device_transport_enabled);
    assert!(disabled.backends.device_v311.is_none());
    assert!(disabled.backends.device_v5.is_none());
    assert!(
        disabled
            .validate_device_transport_endpoints(&endpoints)
            .is_ok()
    );

    let mut enabled_config = minimal_config();
    enabled_config.device_transport.enabled = true;
    let resolved = resolve_runtime_configuration(
        Some(&enabled_config),
        RuntimeAddressConfiguration::default(),
    )
    .unwrap();
    assert_eq!(
        resolved.backends.device_v311,
        Some(enabled_config.listeners.device_backend_address)
    );
    assert_eq!(
        resolved.backends.device_v5,
        Some(enabled_config.listeners.device_v5_backend_address)
    );
    let incomplete_endpoints = DeviceTransportEndpoints {
        api_base_url: None,
        transport_secret: None,
    };
    let error = resolved
        .validate_device_transport_endpoints(&incomplete_endpoints)
        .expect_err("enabled native transport must require endpoint dependencies");
    assert!(matches!(
        error,
        RuntimeConfigurationError::IncompleteDeviceTransportEndpoints
    ));
}

#[tokio::test]
async fn management_requires_basic_auth_and_redacts_secrets() {
    let config = ManagementConfig {
        address: "127.0.0.1:8082".parse::<SocketAddr>().unwrap(),
        username: Some("admin".into()),
        password: Some("do-not-return".into()),
    };
    let mut broker_config = minimal_config();
    broker_config.management = config;
    let router = management_router_with_config(broker_config);

    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let wrong_credentials = base64::engine::general_purpose::STANDARD.encode("admin:wrong");
    let wrong = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/config")
                .header(header::AUTHORIZATION, format!("Basic {wrong_credentials}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let credentials = base64::engine::general_purpose::STANDARD.encode("admin:do-not-return");
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/config")
                .header(header::AUTHORIZATION, format!("Basic {credentials}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body = String::from_utf8(body.into()).unwrap();
    assert!(!body.contains("do-not-return"));
    assert!(!body.contains("webhook-secret"));
    assert!(body.contains("[REDACTED]"));
}

#[tokio::test]
async fn sensitive_management_routes_are_omitted_without_credentials() {
    let router = management_router_with_config(minimal_config());
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn management_status_reports_only_real_capabilities() {
    let mut config = minimal_config();
    config.management.username = Some("admin".into());
    config.management.password = Some("password".into());
    let router = management_router_with_config(config);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body = String::from_utf8(body.into()).unwrap();
    assert!(body.contains("\"mqtt_tcp\""));
    assert!(body.contains("\"mqtt_tls\""));
    assert!(!body.contains("\"mqtt_311_native_device_transport\""));
    assert!(!body.contains("\"mqtt5_native_device_transport\""));
    assert!(body.contains("\"sqlite_persistence\""));
    assert!(!body.contains("\"websocket\""));
    assert!(!body.contains("retained_messages"));
    assert!(!body.contains("connections"));

    let mut websocket_config = minimal_config();
    websocket_config.listeners.websocket.enabled = true;
    websocket_config.management.username = Some("admin".into());
    websocket_config.management.password = Some("password".into());
    let websocket_router = management_router_with_config(websocket_config);
    let response = websocket_router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body = String::from_utf8(body.into()).unwrap();
    assert!(body.contains("\"websocket\""));

    let mut wss_config = minimal_config();
    wss_config.listeners.websocket.enabled = true;
    wss_config.listeners.websocket.tls = true;
    wss_config.management.username = Some("admin".into());
    wss_config.management.password = Some("password".into());
    let wss_router = management_router_with_config(wss_config);
    let response = wss_router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body = String::from_utf8(body.into()).unwrap();
    assert!(body.contains("\"wss\""));
}

#[tokio::test]
async fn management_status_advertises_enabled_sqlite_persistence() {
    let mut config = minimal_config();
    config.storage = iot_mqttd::StorageConfig::Sqlite {
        path: "broker.sqlite".into(),
        prune_interval_ms: 60_000,
    };
    config.management.username = Some("admin".into());
    config.management.password = Some("password".into());
    let router = management_router_with_config(config);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/broker/status")
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:password")
                    ),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body = String::from_utf8(body.into()).unwrap();
    assert!(body.contains("\"sqlite_persistence\""));
}
