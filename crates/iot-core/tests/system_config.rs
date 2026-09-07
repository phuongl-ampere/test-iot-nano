use std::collections::BTreeMap;

use iot_core::{
    IngestTuning, MqttConfigurationUpdate, SmtpConfigurationUpdate, SystemConfigurationError,
    SystemConfigurationUpdate, apply_system_configuration_update, read_system_configuration,
};

fn environment() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("DATABASE_URL".to_owned(), "postgres://secret".to_owned()),
        ("MQTT_BROKER_HOST".to_owned(), "broker.internal".to_owned()),
        ("MQTT_BROKER_PORT".to_owned(), "1883".to_owned()),
        (
            "IOT_STREAM_DIR".to_owned(),
            "/var/lib/iot-ingest/stream".to_owned(),
        ),
        ("IOT_STREAM_PARTITIONS".to_owned(), "8".to_owned()),
        ("SMTP_HOST".to_owned(), "smtp.internal".to_owned()),
        ("SMTP_PORT".to_owned(), "465".to_owned()),
        ("SMTP_USERNAME".to_owned(), "alerts@example.test".to_owned()),
        ("SMTP_PASSWORD".to_owned(), "not-returned".to_owned()),
        (
            "ALERT_EMAIL_FROM".to_owned(),
            "alerts@example.test".to_owned(),
        ),
        ("ALERT_EMAIL_TO".to_owned(), "ops@example.test".to_owned()),
        ("SMTP_TIMEOUT_SECONDS".to_owned(), "15".to_owned()),
        (
            "IOT_STREAM_RETENTION_BYTES".to_owned(),
            (2_u64 * 1024 * 1024 * 1024).to_string(),
        ),
        (
            "IOT_STREAM_RETENTION_SECONDS".to_owned(),
            "86400".to_owned(),
        ),
        (
            "IOT_STREAM_SEGMENT_BYTES".to_owned(),
            (128_u64 * 1024 * 1024).to_string(),
        ),
        (
            "IOT_STREAM_MAX_RECORD_BYTES".to_owned(),
            (1024_u64 * 1024).to_string(),
        ),
    ])
}

#[test]
fn reading_system_configuration_redacts_the_smtp_password() {
    let configuration = read_system_configuration(&environment()).unwrap();

    assert!(configuration.smtp.enabled);
    assert!(configuration.smtp.password_configured);
    assert_eq!(configuration.smtp.host.as_deref(), Some("smtp.internal"));
    assert_eq!(configuration.tuning.retention_seconds, 86_400);
    let serialized = serde_json::to_string(&configuration).unwrap();
    assert!(!serialized.contains("not-returned"));
    assert!(!serialized.contains("password\":"));
}

#[test]
fn update_preserves_smtp_password_and_locked_connection_values() {
    let mut values = environment();
    let update = SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled: true,
            host: Some("smtp.changed".to_owned()),
            port: 587,
            username: Some("new-alerts@example.test".to_owned()),
            password: None,
            from: Some("new-alerts@example.test".to_owned()),
            to: Some("oncall@example.test".to_owned()),
            timeout_seconds: 30,
        },
        mqtt: None,
        tuning: IngestTuning {
            retention_seconds: 172_800,
            ..read_system_configuration(&values).unwrap().tuning
        },
    };

    let configuration = apply_system_configuration_update(&mut values, &update).unwrap();

    assert_eq!(values["SMTP_PASSWORD"], "not-returned");
    assert_eq!(values["DATABASE_URL"], "postgres://secret");
    assert_eq!(values["MQTT_BROKER_HOST"], "broker.internal");
    assert_eq!(values["IOT_STREAM_PARTITIONS"], "8");
    assert_eq!(configuration.smtp.host.as_deref(), Some("smtp.changed"));
    assert_eq!(configuration.tuning.retention_seconds, 172_800);
}

#[test]
fn disabling_smtp_removes_every_smtp_secret_and_setting() {
    let mut values = environment();
    let update = SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled: false,
            host: None,
            port: 465,
            username: None,
            password: None,
            from: None,
            to: None,
            timeout_seconds: 15,
        },
        mqtt: None,
        tuning: read_system_configuration(&values).unwrap().tuning,
    };

    let configuration = apply_system_configuration_update(&mut values, &update).unwrap();

    assert!(!configuration.smtp.enabled);
    assert!(values.keys().all(|key| !key.starts_with("SMTP_")));
    assert!(!values.contains_key("ALERT_EMAIL_FROM"));
    assert!(!values.contains_key("ALERT_EMAIL_TO"));
}

#[test]
fn rejecting_invalid_tuning_prevents_any_environment_write() {
    let mut values = environment();
    let before = values.clone();
    let update = SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled: false,
            host: None,
            port: 465,
            username: None,
            password: None,
            from: None,
            to: None,
            timeout_seconds: 15,
        },
        mqtt: None,
        tuning: IngestTuning {
            retention_seconds: 30,
            ..read_system_configuration(&values).unwrap().tuning
        },
    };

    let error = apply_system_configuration_update(&mut values, &update).unwrap_err();

    assert!(matches!(error, SystemConfigurationError::InvalidTuning(_)));
    assert_eq!(values, before);
}

#[test]
fn update_changes_mqtt_host_and_port_without_touching_locked_database_values() {
    let mut values = environment();
    let update = SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled: false,
            host: None,
            port: 465,
            username: None,
            password: None,
            from: None,
            to: None,
            timeout_seconds: 15,
        },
        mqtt: Some(MqttConfigurationUpdate {
            host: "mqtt.example.test".to_owned(),
            port: 1884,
        }),
        tuning: read_system_configuration(&values).unwrap().tuning,
    };

    let configuration = apply_system_configuration_update(&mut values, &update).unwrap();

    assert_eq!(values["MQTT_BROKER_HOST"], "mqtt.example.test");
    assert_eq!(values["MQTT_BROKER_PORT"], "1884");
    assert_eq!(values["DATABASE_URL"], "postgres://secret");
    assert_eq!(configuration.mqtt.host, "mqtt.example.test");
    assert_eq!(configuration.mqtt.port, 1884);
}
