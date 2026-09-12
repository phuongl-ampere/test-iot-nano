use std::collections::BTreeMap;
use std::process::Command;

use iot_nano_monolith::{ConfigError, MonolithConfig, RETIRED_ENVIRONMENT_NAMES};

fn values(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn sqlite_values() -> BTreeMap<String, String> {
    values(&[
        ("IOT_NANO_STORAGE", "sqlite"),
        ("IOT_NANO_SQLITE_PATH", "/var/lib/iot-nano/platform.sqlite"),
        ("IOT_NANO_INTERNAL_DIR", "/var/lib/iot-nano/internal"),
        ("IOT_NANO_TLS_CERT_PATH", "/run/tls/server.crt"),
        ("IOT_NANO_TLS_KEY_PATH", "/run/tls/server.key"),
    ])
}

fn timescale_values() -> BTreeMap<String, String> {
    values(&[
        ("IOT_NANO_STORAGE", "timescale"),
        ("DATABASE_URL", "postgres://iot:secret@db.example/iot"),
        ("IOT_NANO_INTERNAL_DIR", "/var/lib/iot-nano/internal"),
        ("IOT_NANO_TLS_CERT_PATH", "/run/tls/server.crt"),
        ("IOT_NANO_TLS_KEY_PATH", "/run/tls/server.key"),
    ])
}

#[test]
fn config_accepts_only_complete_sqlite_or_timescale_storage() {
    let sqlite = MonolithConfig::from_values(sqlite_values()).unwrap();
    assert!(sqlite.storage.is_sqlite());
    assert_eq!(
        sqlite.internal_dir,
        std::path::PathBuf::from("/var/lib/iot-nano/internal")
    );

    let timescale = MonolithConfig::from_values(timescale_values()).unwrap();
    assert!(timescale.storage.is_timescale());
}

#[test]
fn config_rejects_incomplete_or_contradictory_storage() {
    let missing_storage = values(&[
        ("IOT_NANO_INTERNAL_DIR", "/var/lib/iot-nano/internal"),
        ("IOT_NANO_TLS_CERT_PATH", "/run/tls/server.crt"),
        ("IOT_NANO_TLS_KEY_PATH", "/run/tls/server.key"),
    ]);
    assert!(matches!(
        MonolithConfig::from_values(missing_storage),
        Err(ConfigError::MissingStorage)
    ));

    let mut sqlite_with_database = sqlite_values();
    sqlite_with_database.insert(
        "DATABASE_URL".to_owned(),
        "postgres://iot:secret@db.example/iot".to_owned(),
    );
    assert!(matches!(
        MonolithConfig::from_values(sqlite_with_database),
        Err(ConfigError::ContradictoryStorage(_))
    ));

    let mut timescale_with_sqlite = timescale_values();
    timescale_with_sqlite.insert(
        "IOT_NANO_SQLITE_PATH".to_owned(),
        "/var/lib/iot-nano/platform.sqlite".to_owned(),
    );
    assert!(matches!(
        MonolithConfig::from_values(timescale_with_sqlite),
        Err(ConfigError::ContradictoryStorage(_))
    ));
}

#[test]
fn config_rejects_unsafe_paths_incomplete_tls_and_duplicate_listener_addresses() {
    let mut relative_path = sqlite_values();
    relative_path.insert(
        "IOT_NANO_SQLITE_PATH".to_owned(),
        "platform.sqlite".to_owned(),
    );
    assert!(matches!(
        MonolithConfig::from_values(relative_path),
        Err(ConfigError::InvalidAbsolutePath { .. })
    ));

    let mut incomplete_tls = sqlite_values();
    incomplete_tls.remove("IOT_NANO_TLS_KEY_PATH");
    assert!(matches!(
        MonolithConfig::from_values(incomplete_tls),
        Err(ConfigError::IncompleteTls)
    ));

    let mut duplicate_addresses = sqlite_values();
    duplicate_addresses.insert(
        "IOT_NANO_PUBLIC_HTTP_ADDRESS".to_owned(),
        "127.0.0.1:8080".to_owned(),
    );
    duplicate_addresses.insert(
        "IOT_NANO_MANAGEMENT_ADDRESS".to_owned(),
        "127.0.0.1:8080".to_owned(),
    );
    assert!(matches!(
        MonolithConfig::from_values(duplicate_addresses),
        Err(ConfigError::DuplicateListenerAddress(_))
    ));
}

#[test]
fn config_rejects_every_retired_internal_service_environment_variable() {
    for name in RETIRED_ENVIRONMENT_NAMES {
        let mut configuration = sqlite_values();
        configuration.insert((*name).to_owned(), "retired-value".to_owned());

        assert!(
            matches!(
                MonolithConfig::from_values(configuration),
                Err(ConfigError::RetiredEnvironment(found)) if found == *name
            ),
            "expected {name} to be rejected"
        );
    }
}

#[test]
fn retired_environment_denylist_covers_all_current_topology_variables() {
    let expected = [
        "IOT_API_ADDRESS",
        "IOT_DATABASE_STORAGE",
        "IOT_MQTTD_API_BASE_URL",
        "IOT_MQTTD_CONFIG",
        "IOT_MQTTD_DEVICE_BACKEND_ADDRESS",
        "IOT_MQTTD_DEVICE_V5_BACKEND_ADDRESS",
        "IOT_MQTTD_INTERNAL_V311_ADDRESS",
        "IOT_MQTTD_INTERNAL_V5_ADDRESS",
        "IOT_MQTTD_MANAGEMENT_ADDRESS",
        "IOT_MQTTD_MAX_CONNECTIONS",
        "IOT_MQTTD_PLAIN_ADDRESS",
        "IOT_MQTTD_TLS_ADDRESS",
        "IOT_MQTTD_TLS_CERT_PATH",
        "IOT_MQTTD_TLS_KEY_PATH",
        "IOT_MQTTD_TRANSPORT_INTERNAL_ADDRESS",
        "IOT_NANO_API_CORE_SECRET",
        "IOT_NANO_API_MQTTD_SECRET",
        "IOT_NANO_API_SQLITE_PATH",
        "IOT_NANO_CORE_HEALTH_ADDRESS",
        "IOT_NANO_CORE_MQTTD_SECRET",
        "IOT_NANO_CORE_SQLITE_PATH",
        "IOT_NANO_CORE_STREAM_SECRET",
        "IOT_NANO_CORE_URL",
        "IOT_NANO_MQTTD_API_SECRET",
        "IOT_NANO_MQTTD_INTERNAL_URL",
        "IOT_NANO_MQTTD_STREAM_SECRET",
        "IOT_NANO_STREAM_ADDRESS",
        "IOT_NANO_STREAM_DIR",
        "IOT_NANO_STREAM_MAX_RECORD_BYTES",
        "IOT_NANO_STREAM_PARTITIONS",
        "IOT_NANO_STREAM_RETENTION_BYTES",
        "IOT_NANO_STREAM_RETENTION_INTERVAL_SECONDS",
        "IOT_NANO_STREAM_RETENTION_SECONDS",
        "IOT_NANO_STREAM_SECRET",
        "IOT_NANO_STREAM_SEGMENT_BYTES",
        "IOT_NANO_STREAM_URL",
        "IOT_SQLITE_BUSY_TIMEOUT_MS",
        "IOT_SQLITE_PATH",
        "USE_DATABASE_STORAGE",
    ];

    let actual = RETIRED_ENVIRONMENT_NAMES
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn config_check_validates_without_logging_secret_values() {
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");

    let valid = Command::new(binary)
        .arg("--config-check")
        .env_clear()
        .envs(sqlite_values())
        .output()
        .unwrap();
    assert!(valid.status.success(), "{valid:?}");

    let invalid = Command::new(binary)
        .arg("--config-check")
        .env_clear()
        .envs(sqlite_values())
        .env("IOT_NANO_API_CORE_SECRET", "not-for-output")
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("not-for-output"));
}
