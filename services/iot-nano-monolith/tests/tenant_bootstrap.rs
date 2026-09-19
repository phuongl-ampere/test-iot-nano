use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::bootstrap_system;
use iot_storage::PlatformStore;

#[tokio::test]
async fn system_bootstrap_creates_exactly_one_system_account() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    let system = bootstrap_system(&store, "initial-system", "SystemAccount@2026")
        .await
        .unwrap();
    assert_eq!(system.username, "initial-system");
    assert!(
        bootstrap_system(&store, "second-system", "SystemAccount@2026")
            .await
            .is_err()
    );
}
