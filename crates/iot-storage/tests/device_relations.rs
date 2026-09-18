use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateDeviceRelation, DeviceRelationError, DeviceRelationRepository, PlatformStore,
};
use sqlx::SqlitePool;
use uuid::Uuid;

const TENANT_A: Uuid = Uuid::from_u128(1);
const TENANT_B: Uuid = Uuid::from_u128(2);
const GATEWAY_A: &str = "gateway-a";
const DEVICE_A: &str = "device-a";
const DEVICE_B: &str = "device-b";
const DEVICE_OTHER_TENANT: &str = "device-other-tenant";

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_devices(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'tenant-a', 'active'), (?, 'tenant-b', 'active')",
    )
    .bind(TENANT_A.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, is_gateway, gateway_device_id)
         VALUES (?, ?, 'Gateway A', 1, NULL),
                (?, ?, 'Device A', 0, ?),
                (?, ?, 'Device B', 0, NULL),
                (?, ?, 'Other tenant device', 0, NULL)",
    )
    .bind(GATEWAY_A)
    .bind(TENANT_A.to_string())
    .bind(DEVICE_A)
    .bind(TENANT_A.to_string())
    .bind(GATEWAY_A)
    .bind(DEVICE_B)
    .bind(TENANT_A.to_string())
    .bind(DEVICE_OTHER_TENANT)
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
}

fn relation(from_device_id: &str, to_device_id: &str, relation_type: &str) -> CreateDeviceRelation {
    CreateDeviceRelation {
        from_device_id: from_device_id.to_owned(),
        to_device_id: to_device_id.to_owned(),
        relation_type: relation_type.to_owned(),
    }
}

#[tokio::test]
async fn sqlite_device_relations_are_tenant_scoped_and_do_not_change_gateway_topology() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_devices(pool).await;

    let created = DeviceRelationRepository::create_device_relation(
        &store,
        TENANT_A,
        relation(DEVICE_A, DEVICE_B, "monitors"),
    )
    .await
    .unwrap();
    assert_eq!(created.tenant_id, TENANT_A);
    assert_eq!(created.from_device_id, DEVICE_A);
    assert_eq!(created.to_device_id, DEVICE_B);
    assert_eq!(created.relation_type, "monitors");

    assert_eq!(
        DeviceRelationRepository::list_device_relations(&store, TENANT_A)
            .await
            .unwrap(),
        vec![created.clone()]
    );
    assert!(
        DeviceRelationRepository::list_device_relations(&store, TENANT_B)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT gateway_device_id FROM devices WHERE device_id = ?",
        )
        .bind(DEVICE_A)
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(GATEWAY_A.to_owned())
    );

    assert!(
        DeviceRelationRepository::delete_device_relation(&store, TENANT_A, created.id)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT gateway_device_id FROM devices WHERE device_id = ?",
        )
        .bind(DEVICE_A)
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(GATEWAY_A.to_owned())
    );
}

#[tokio::test]
async fn sqlite_device_relations_reject_invalid_or_cross_tenant_endpoints() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_devices(pool).await;

    assert!(matches!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_A,
            relation(DEVICE_A, DEVICE_OTHER_TENANT, "monitors"),
        )
        .await,
        Err(DeviceRelationError::DeviceNotFound { .. })
    ));
    assert!(matches!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_A,
            relation(DEVICE_A, DEVICE_A, "monitors"),
        )
        .await,
        Err(DeviceRelationError::SelfRelation)
    ));
    assert!(matches!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_A,
            relation(DEVICE_A, DEVICE_B, "gateway_child"),
        )
        .await,
        Err(DeviceRelationError::ReservedRelationType)
    ));
    assert!(matches!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_A,
            relation(DEVICE_A, DEVICE_B, "invalid relation"),
        )
        .await,
        Err(DeviceRelationError::InvalidRelationType(_))
    ));

    let created = DeviceRelationRepository::create_device_relation(
        &store,
        TENANT_A,
        relation(DEVICE_A, DEVICE_B, "monitors"),
    )
    .await
    .unwrap();
    assert!(matches!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_A,
            relation(DEVICE_A, DEVICE_B, "monitors"),
        )
        .await,
        Err(DeviceRelationError::RelationConflict)
    ));
    assert!(matches!(
        DeviceRelationRepository::delete_device_relation(&store, TENANT_B, created.id).await,
        Err(DeviceRelationError::RelationNotFound)
    ));
    assert_eq!(
        DeviceRelationRepository::list_device_relations(&store, TENANT_A)
            .await
            .unwrap()
            .len(),
        1
    );
}
