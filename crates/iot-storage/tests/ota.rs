use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{OtaPolicy, PlatformStore};
use tempfile::TempDir;
use uuid::Uuid;

async fn store() -> (TempDir, PlatformStore, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("platform.sqlite");
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, 'ota', 'active', '{}')",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store, tenant_id)
}

#[tokio::test]
async fn sqlite_ota_policy_defaults_to_both_device_safety_checks() {
    let (_directory, store, tenant_id) = store().await;
    assert_eq!(
        store.ota_policy(tenant_id).await.unwrap(),
        OtaPolicy::default()
    );
    store
        .set_ota_policy(
            tenant_id,
            OtaPolicy {
                require_matching_device_profile: false,
                require_newer_version: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.ota_policy(tenant_id).await.unwrap(),
        OtaPolicy {
            require_matching_device_profile: false,
            require_newer_version: false
        }
    );
}

#[tokio::test]
async fn sqlite_ota_artifact_rejects_non_semver_metadata() {
    let (_directory, store, tenant_id) = store().await;
    let result = store
        .create_ota_artifact(&iot_storage::OtaArtifact {
            id: Uuid::now_v7(),
            tenant_id,
            device_profile_id: Uuid::now_v7(),
            version: "latest".to_owned(),
            filename: "firmware.bin".to_owned(),
            storage_path: "/tmp/firmware.bin".to_owned(),
            sha256: "0".repeat(64),
            size_bytes: 1,
        })
        .await;
    assert!(result.is_err());
}
