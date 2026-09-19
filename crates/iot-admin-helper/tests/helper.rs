use std::{fs, os::unix::fs::PermissionsExt};

use iot_admin_helper::{
    apply_config_file, apply_config_files, apply_config_files_with_api, read_config_file,
};
use iot_nano_foundation::{
    IngestTuning, MqttConfigurationUpdate, SmtpConfigurationUpdate, SystemConfigurationUpdate,
};

fn update(enabled: bool) -> SystemConfigurationUpdate {
    SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled,
            host: enabled.then(|| "smtp.changed".to_owned()),
            port: 465,
            username: enabled.then(|| "alerts@example.test".to_owned()),
            password: None,
            from: enabled.then(|| "alerts@example.test".to_owned()),
            to: enabled.then(|| "ops@example.test".to_owned()),
            timeout_seconds: 15,
        },
        mqtt: None,
        tuning: IngestTuning::default(),
    }
}

#[test]
fn helper_read_redacts_smtp_password() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ingest.env");
    fs::write(
        &path,
        "DATABASE_URL=postgres://secret\nSMTP_HOST=smtp.example.test\nSMTP_USERNAME=alerts\nSMTP_PASSWORD=not-returned\nALERT_EMAIL_FROM=alerts@example.test\nALERT_EMAIL_TO=ops@example.test\n",
    )
    .unwrap();

    let configuration = read_config_file(&path).unwrap();
    let serialized = serde_json::to_string(&configuration).unwrap();

    assert!(configuration.smtp.password_configured);
    assert!(!serialized.contains("not-returned"));
}

#[test]
fn helper_apply_preserves_locked_keys_and_existing_smtp_password() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ingest.env");
    fs::write(
        &path,
        "DATABASE_URL=postgres://secret\nMQTT_BROKER_HOST=broker.internal\nIOT_STREAM_PARTITIONS=8\nSMTP_HOST=smtp.example.test\nSMTP_USERNAME=alerts\nSMTP_PASSWORD=not-returned\nALERT_EMAIL_FROM=alerts@example.test\nALERT_EMAIL_TO=ops@example.test\n",
    )
    .unwrap();

    apply_config_file(&path, &update(true)).unwrap();
    let written = fs::read_to_string(&path).unwrap();

    assert!(written.contains("DATABASE_URL=postgres://secret"));
    assert!(written.contains("MQTT_BROKER_HOST=broker.internal"));
    assert!(written.contains("IOT_STREAM_PARTITIONS=8"));
    assert!(written.contains("SMTP_PASSWORD=not-returned"));
    assert!(written.contains("SMTP_HOST=smtp.changed"));
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn helper_apply_disabling_smtp_removes_all_smtp_values() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ingest.env");
    fs::write(
        &path,
        "DATABASE_URL=postgres://secret\nSMTP_HOST=smtp.example.test\nSMTP_USERNAME=alerts\nSMTP_PASSWORD=not-returned\nALERT_EMAIL_FROM=alerts@example.test\nALERT_EMAIL_TO=ops@example.test\n",
    )
    .unwrap();

    let configuration = apply_config_file(&path, &update(false)).unwrap();
    let written = fs::read_to_string(&path).unwrap();

    assert!(!configuration.smtp.enabled);
    assert!(!written.contains("SMTP_"));
    assert!(!written.contains("ALERT_EMAIL_"));
}

#[test]
fn helper_updates_mqtt_host_and_port_in_both_service_environment_files() {
    let directory = tempfile::tempdir().unwrap();
    let ingest_path = directory.path().join("ingest.env");
    let smtp_path = directory.path().join("smtp.env");
    let api_path = directory.path().join("api.env");
    fs::write(
        &ingest_path,
        "DATABASE_URL=postgres://ingest\nMQTT_BROKER_HOST=old-broker\nMQTT_BROKER_PORT=1883\n",
    )
    .unwrap();
    fs::write(
        &api_path,
        "DATABASE_URL=postgres://api\nMQTT_BROKER_HOST=old-broker\nMQTT_BROKER_PORT=1883\nMQTT_API_PASSWORD=secret\n",
    )
    .unwrap();

    let mut next = update(false);
    next.mqtt = Some(MqttConfigurationUpdate {
        host: "mqtt.remote.test".to_owned(),
        port: 1884,
    });
    apply_config_files_with_api(&ingest_path, &smtp_path, &api_path, &next).unwrap();

    let ingest = fs::read_to_string(&ingest_path).unwrap();
    let api = fs::read_to_string(&api_path).unwrap();
    assert!(ingest.contains("MQTT_BROKER_HOST=mqtt.remote.test"));
    assert!(ingest.contains("MQTT_BROKER_PORT=1884"));
    assert!(api.contains("MQTT_BROKER_HOST=mqtt.remote.test"));
    assert!(api.contains("MQTT_BROKER_PORT=1884"));
    assert!(api.contains("MQTT_API_PASSWORD=secret"));
}

#[test]
fn helper_writes_smtp_passwords_without_shell_escaping_dollar_signs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ingest.env");
    fs::write(&path, "DATABASE_URL=postgres://secret\n").unwrap();
    let mut configuration = update(true);
    configuration.smtp.password = Some("pa$word with space".to_owned());

    apply_config_file(&path, &configuration).unwrap();

    let written = fs::read_to_string(&path).unwrap();
    assert!(written.contains("SMTP_PASSWORD=\"pa$word with space\""));
    assert!(!written.contains("SMTP_PASSWORD=\"pa\\$word with space\""));
}

#[test]
fn helper_applies_smtp_to_a_live_file_without_exposing_database_settings() {
    let directory = tempfile::tempdir().unwrap();
    let ingest_path = directory.path().join("ingest.env");
    let smtp_path = directory.path().join("smtp.env");
    fs::write(
        &ingest_path,
        "DATABASE_URL=postgres://secret\nMQTT_BROKER_HOST=broker.internal\nSMTP_PASSWORD=legacy-secret\n",
    )
    .unwrap();
    let mut configuration = update(true);
    configuration.smtp.password = Some("live-secret".to_owned());

    let saved = apply_config_files(&ingest_path, &smtp_path, &configuration).unwrap();
    let ingest = fs::read_to_string(&ingest_path).unwrap();
    let smtp = fs::read_to_string(&smtp_path).unwrap();

    assert!(saved.smtp.password_configured);
    assert!(ingest.contains("DATABASE_URL=postgres://secret"));
    assert!(ingest.contains("MQTT_BROKER_HOST=broker.internal"));
    assert!(!ingest.contains("SMTP_PASSWORD"));
    assert!(smtp.contains("SMTP_PASSWORD=live-secret"));
    assert_eq!(
        fs::metadata(&smtp_path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}
