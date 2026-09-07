use std::collections::BTreeMap;

use iot_core::{DatabaseStorage, StorageConfiguration, StorageConfigurationError};

#[test]
fn parses_timescale_and_sqlite_startup_configuration() {
    let timescale = StorageConfiguration::from_values(&BTreeMap::from([
        ("IOT_DATABASE_STORAGE".to_owned(), "timescale".to_owned()),
        (
            "DATABASE_URL".to_owned(),
            "postgres://iot:secret@db:5432/iot".to_owned(),
        ),
    ]))
    .unwrap();
    assert_eq!(timescale.storage, DatabaseStorage::Timescale);

    let sqlite = StorageConfiguration::from_values(&BTreeMap::from([
        ("USE_DATABASE_STORAGE".to_owned(), "sqlite".to_owned()),
        (
            "IOT_SQLITE_PATH".to_owned(),
            "/var/lib/rush-iot-nano/rush.db".to_owned(),
        ),
    ]))
    .unwrap();
    assert_eq!(sqlite.storage, DatabaseStorage::Sqlite);
}

#[test]
fn rejects_incomplete_or_unsafe_storage_configuration() {
    let missing_timescale = StorageConfiguration::from_values(&BTreeMap::from([(
        "IOT_DATABASE_STORAGE".to_owned(),
        "timescale".to_owned(),
    )]));
    assert!(matches!(
        missing_timescale,
        Err(StorageConfigurationError::MissingDatabaseUrl)
    ));

    let relative_sqlite = StorageConfiguration::from_values(&BTreeMap::from([
        ("IOT_DATABASE_STORAGE".to_owned(), "sqlite".to_owned()),
        ("IOT_SQLITE_PATH".to_owned(), "rush.db".to_owned()),
    ]));
    assert!(matches!(
        relative_sqlite,
        Err(StorageConfigurationError::InvalidSqlitePath)
    ));
}
