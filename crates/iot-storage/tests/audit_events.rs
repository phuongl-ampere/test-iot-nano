use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AuditAction, AuditEventRepository, AuditPrincipal, AuditTargetType, CreateDeviceRelation,
    CreateManagementAsset, DeviceRelationRepository, DeviceTokenRepository,
    ManagementAssetRepository, ManagementDeviceRepository, ManagementDeviceTopology,
    NewDeviceToken, NewOwnedDeviceToken, OwnershipTransferTarget, PlatformStore,
    TenantAuthorizationRepository, UpdateManagementAsset, UpdateManagementDevice,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

mod common;

const TENANT_ID: Uuid = Uuid::from_u128(1_001);
const TENANT_ACCOUNT_ID: Uuid = Uuid::from_u128(1_002);
const PARENT_ASSET_ID: Uuid = Uuid::from_u128(1_003);
const CHILD_ASSET_ID: Uuid = Uuid::from_u128(1_004);
const GATEWAY_A: &str = "audit-gateway-a";
const GATEWAY_B: &str = "audit-gateway-b";
const CHILD_DEVICE: &str = "audit-child";
const RELATION_FROM_DEVICE: &str = "audit-relation-from";
const RELATION_TO_DEVICE: &str = "audit-relation-to";
const OWNER_A: Uuid = Uuid::from_u128(1_005);
const OWNER_B: Uuid = Uuid::from_u128(1_006);
const OTHER_TENANT_ID: Uuid = Uuid::from_u128(1_007);
const OTHER_TENANT_USER_ID: Uuid = Uuid::from_u128(1_008);
const OTHER_TENANT_ACCOUNT_ID: Uuid = Uuid::from_u128(1_015);
const OWNED_ASSET_ID: Uuid = Uuid::from_u128(1_009);
const DEVICE_CONTAINMENT_ASSET_ID: Uuid = Uuid::from_u128(1_010);
const DEVICE_CONTAINMENT_DEVICE: &str = "audit-containment-device";
const DELETE_PARENT_ASSET_ID: Uuid = Uuid::from_u128(1_011);
const DELETE_CHILD_ASSET_ID: Uuid = Uuid::from_u128(1_012);
const DELETE_CHILD_DEVICE: &str = "audit-delete-child-device";
const WHOLE_SECOND_AUDIT_ID: Uuid = Uuid::from_u128(1_013);
const FRACTIONAL_SECOND_AUDIT_ID: Uuid = Uuid::from_u128(1_014);

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

async fn seed_tenant(store: &PlatformStore) {
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'audit-events', 'active')",
    )
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?1, ?2, ?1, 'unused', 'active', 1)",
    )
    .bind(TENANT_ACCOUNT_ID.to_string())
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_user(store: &PlatformStore, user_id: Uuid, username: &str) {
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(TENANT_ID.to_string())
    .bind(username)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

fn tenant_actor() -> AuditPrincipal {
    AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
}

#[tokio::test]
async fn sqlite_audit_events_reject_replace_with_recursive_triggers_disabled() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let mut connection = store.sqlite_pool().unwrap().acquire().await.unwrap();
    sqlx::query("PRAGMA recursive_triggers = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();

    let event_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO audit_events (
            id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
            action, target_type, target_id, changes
         ) VALUES (?, ?, '2026-01-01T00:00:00.000000000Z', 'tenant_account', ?,
                   'permission.granted', 'resource_permission', 'original-target',
                   '{\"state\":\"original\"}')",
    )
    .bind(&event_id)
    .bind(TENANT_ID.to_string())
    .bind(TENANT_ACCOUNT_ID.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();

    let replacement = sqlx::query(
        "INSERT OR REPLACE INTO audit_events (
            id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
            action, target_type, target_id, changes
         ) VALUES (?, ?, '2026-01-02T00:00:00.000000000Z', 'user', ?,
                   'permission.revoked', 'device', 'replacement-target',
                   '{\"state\":\"replacement\"}')",
    )
    .bind(&event_id)
    .bind(TENANT_ID.to_string())
    .bind(OWNER_A.to_string())
    .execute(&mut *connection)
    .await;
    assert!(replacement.is_err());

    let original = sqlx::query(
        "SELECT occurred_at, actor_principal_kind, actor_principal_id, action,
                target_type, target_id, changes
         FROM audit_events
         WHERE id = ?",
    )
    .bind(&event_id)
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert_eq!(
        original.try_get::<String, _>("occurred_at").unwrap(),
        "2026-01-01T00:00:00.000000000Z"
    );
    assert_eq!(
        original
            .try_get::<String, _>("actor_principal_kind")
            .unwrap(),
        "tenant_account"
    );
    assert_eq!(
        original.try_get::<String, _>("actor_principal_id").unwrap(),
        TENANT_ACCOUNT_ID.to_string()
    );
    assert_eq!(
        original.try_get::<String, _>("action").unwrap(),
        "permission.granted"
    );
    assert_eq!(
        original.try_get::<String, _>("target_type").unwrap(),
        "resource_permission"
    );
    assert_eq!(
        original.try_get::<String, _>("target_id").unwrap(),
        "original-target"
    );
    assert_eq!(
        original.try_get::<String, _>("changes").unwrap(),
        "{\"state\":\"original\"}"
    );
}

#[tokio::test]
async fn sqlite_audit_keyset_order_is_chronological_for_whole_and_fractional_seconds() {
    let (directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    for (id, occurred_at) in [
        (WHOLE_SECOND_AUDIT_ID, "2026-01-01T00:00:00Z"),
        (FRACTIONAL_SECOND_AUDIT_ID, "2026-01-01T00:00:00.500Z"),
    ] {
        sqlx::query(
            "INSERT INTO audit_events (
                id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                action, target_type, target_id, changes
             ) VALUES (?, ?, ?, 'tenant_account', ?,
                       'permission.granted', 'resource_permission', ?, '{}')",
        )
        .bind(id.to_string())
        .bind(TENANT_ID.to_string())
        .bind(occurred_at)
        .bind(TENANT_ACCOUNT_ID.to_string())
        .bind(id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("PRAGMA user_version = 1")
        .execute(pool)
        .await
        .unwrap();
    drop(store);

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT occurred_at FROM audit_events WHERE id = ?",)
            .bind(WHOLE_SECOND_AUDIT_ID.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "2026-01-01T00:00:00.000000000Z"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT occurred_at FROM audit_events WHERE id = ?",)
            .bind(FRACTIONAL_SECOND_AUDIT_ID.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "2026-01-01T00:00:00.500000000Z"
    );
    let first_page = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 1)
        .await
        .unwrap();
    assert_eq!(first_page[0].id, FRACTIONAL_SECOND_AUDIT_ID);
    let second_page = AuditEventRepository::list_tenant_audit_events(
        &store,
        TENANT_ID,
        Some(iot_storage::AuditEventCursor {
            occurred_at: first_page[0].occurred_at,
            id: first_page[0].id,
        }),
        1,
    )
    .await
    .unwrap();
    assert_eq!(second_page[0].id, WHOLE_SECOND_AUDIT_ID);
}

#[tokio::test]
async fn sqlite_asset_creation_with_containment_records_the_explicit_user_actor() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    seed_user(&store, OWNER_A, "audit-asset-creator").await;
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Parent')")
        .bind(PARENT_ASSET_ID.to_string())
        .bind(TENANT_ID.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let asset = ManagementAssetRepository::create_management_asset(
        &store,
        TENANT_ID,
        AuditPrincipal::User(OWNER_A),
        CreateManagementAsset {
            name: "Child".to_owned(),
            asset_profile_id: None,
            parent_asset_id: Some(PARENT_ASSET_ID),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor, AuditPrincipal::User(OWNER_A));
    assert_eq!(events[0].action, AuditAction::AssetContainmentChanged);
    assert_eq!(events[0].target_type, AuditTargetType::Asset);
    assert_eq!(events[0].target_id, asset.id.to_string());
    assert_eq!(
        events[0].changes,
        json!({
            "parent_asset_id": {
                "before": null,
                "after": PARENT_ASSET_ID.to_string(),
            }
        })
    );
}

#[tokio::test]
async fn sqlite_asset_creation_rolls_back_when_containment_audit_insert_fails() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    seed_user(&store, OWNER_A, "audit-asset-rollback-creator").await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Parent')")
        .bind(PARENT_ASSET_ID.to_string())
        .bind(TENANT_ID.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER reject_asset_creation_audit
         BEFORE INSERT ON audit_events
         BEGIN
             SELECT RAISE(ABORT, 'reject asset creation audit');
         END;",
    )
    .execute(pool)
    .await
    .unwrap();

    assert!(
        ManagementAssetRepository::create_management_asset(
            &store,
            TENANT_ID,
            AuditPrincipal::User(OWNER_A),
            CreateManagementAsset {
                name: "Failed child".to_owned(),
                asset_profile_id: None,
                parent_asset_id: Some(PARENT_ASSET_ID),
                metadata: json!({}),
                attributes: None,
            },
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets WHERE tenant_id = ? AND name = 'Failed child'",
        )
        .bind(TENANT_ID.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_owned_device_provisioning_records_explicit_actor_ownership_and_containment() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    seed_user(&store, OWNER_A, "audit-owned-device-owner").await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Owned asset')")
        .bind(OWNED_ASSET_ID.to_string())
        .bind(TENANT_ID.to_string())
        .execute(pool)
        .await
        .unwrap();

    let device = DeviceTokenRepository::provision_owned_device_token(
        &store,
        TENANT_ID,
        tenant_actor(),
        NewOwnedDeviceToken {
            display_name: "Owned device".to_owned(),
            owner_user_id: OWNER_A,
            asset_id: Some(OWNED_ASSET_ID),
            token: NewDeviceToken {
                id: Uuid::now_v7(),
                token_prefix: "audit-owned-device-token".to_owned(),
                token_hash: "audit-owned-device-token-hash".to_owned(),
                token_ciphertext: "audit-owned-device-token-ciphertext".to_owned(),
            },
        },
    )
    .await
    .unwrap();
    assert!(!device.device_id.is_empty());

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| {
        event.actor == tenant_actor()
            && event.target_type == AuditTargetType::Device
            && event.target_id == device.device_id
    }));
    assert!(events.iter().any(|event| {
        event.action == AuditAction::OwnershipTransferred
            && event.changes
                == json!({
                    "owner_user_id": {
                        "before": null,
                        "after": OWNER_A.to_string(),
                    }
                })
    }));
    assert!(events.iter().any(|event| {
        event.action == AuditAction::AssetContainmentChanged
            && event.changes
                == json!({
                    "asset_id": {
                        "before": null,
                        "after": OWNED_ASSET_ID.to_string(),
                    }
                })
    }));
}

#[tokio::test]
async fn sqlite_owned_device_provisioning_rolls_back_when_audit_insert_fails() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    seed_user(&store, OWNER_A, "audit-owned-device-rollback-owner").await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER reject_owned_device_audit
         BEFORE INSERT ON audit_events
         BEGIN
             SELECT RAISE(ABORT, 'reject owned device audit');
         END;",
    )
    .execute(pool)
    .await
    .unwrap();

    assert!(
        DeviceTokenRepository::provision_owned_device_token(
            &store,
            TENANT_ID,
            tenant_actor(),
            NewOwnedDeviceToken {
                display_name: "Failed owned device".to_owned(),
                owner_user_id: OWNER_A,
                asset_id: None,
                token: NewDeviceToken {
                    id: Uuid::now_v7(),
                    token_prefix: "audit-owned-device-failed-token".to_owned(),
                    token_hash: "audit-owned-device-failed-token-hash".to_owned(),
                    token_ciphertext: "audit-owned-device-failed-token-ciphertext".to_owned(),
                },
            },
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM devices WHERE tenant_id = ?")
            .bind(TENANT_ID.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_tokens")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_asset_containment_change_emits_before_and_after_audit_values() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'Parent'), (?, ?, 'Child')",
    )
    .bind(PARENT_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(CHILD_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    ManagementAssetRepository::update_management_asset(
        &store,
        TENANT_ID,
        tenant_actor(),
        CHILD_ASSET_ID,
        UpdateManagementAsset {
            name: "Child".to_owned(),
            asset_profile_id: None,
            parent_asset_id: Some(PARENT_ASSET_ID),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].actor,
        AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
    );
    assert_eq!(events[0].action, AuditAction::AssetContainmentChanged);
    assert_eq!(events[0].target_type, AuditTargetType::Asset);
    assert_eq!(events[0].target_id, CHILD_ASSET_ID.to_string());
    assert_eq!(
        events[0].changes,
        json!({
            "parent_asset_id": {
                "before": null,
                "after": PARENT_ASSET_ID.to_string(),
            }
        })
    );
}

fn device_update(gateway_device_id: Option<&str>) -> UpdateManagementDevice {
    UpdateManagementDevice {
        display_name: "Audited child".to_owned(),
        asset_id: None,
        device_profile_id: None,
        attributes: None,
        topology: Some(ManagementDeviceTopology {
            is_gateway: false,
            gateway_device_id: gateway_device_id.map(ToOwned::to_owned),
        }),
    }
}

#[tokio::test]
async fn sqlite_gateway_assignment_reassignment_and_detach_emit_distinct_audit_actions() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata, is_gateway)
         VALUES (?, ?, 'Gateway A', '{}', 1),
                (?, ?, 'Gateway B', '{}', 1),
                (?, ?, 'Child', '{}', 0)",
    )
    .bind(GATEWAY_A)
    .bind(TENANT_ID.to_string())
    .bind(GATEWAY_B)
    .bind(TENANT_ID.to_string())
    .bind(CHILD_DEVICE)
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    ManagementDeviceRepository::update_management_device(
        &store,
        TENANT_ID,
        tenant_actor(),
        CHILD_DEVICE,
        device_update(Some(GATEWAY_A)),
    )
    .await
    .unwrap();
    ManagementDeviceRepository::update_management_device(
        &store,
        TENANT_ID,
        tenant_actor(),
        CHILD_DEVICE,
        device_update(Some(GATEWAY_B)),
    )
    .await
    .unwrap();
    ManagementDeviceRepository::update_management_device(
        &store,
        TENANT_ID,
        tenant_actor(),
        CHILD_DEVICE,
        device_update(None),
    )
    .await
    .unwrap();

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|event| event.action).collect::<Vec<_>>(),
        vec![
            AuditAction::GatewayDetached,
            AuditAction::GatewayReassigned,
            AuditAction::GatewayAssigned,
        ]
    );
    assert!(events.iter().all(|event| {
        event.actor == AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
            && event.target_type == AuditTargetType::Device
            && event.target_id == CHILD_DEVICE
    }));
    assert_eq!(
        events[2].changes,
        json!({"gateway_device_id": {"before": null, "after": GATEWAY_A}})
    );
    assert_eq!(
        events[1].changes,
        json!({"gateway_device_id": {"before": GATEWAY_A, "after": GATEWAY_B}})
    );
    assert_eq!(
        events[0].changes,
        json!({"gateway_device_id": {"before": GATEWAY_B, "after": null}})
    );
}

#[tokio::test]
async fn sqlite_device_asset_assignment_emits_containment_audit_values() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'Device container')",
    )
    .bind(DEVICE_CONTAINMENT_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata)
         VALUES (?, ?, 'Containment device', '{}')",
    )
    .bind(DEVICE_CONTAINMENT_DEVICE)
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    ManagementDeviceRepository::update_management_device(
        &store,
        TENANT_ID,
        tenant_actor(),
        DEVICE_CONTAINMENT_DEVICE,
        UpdateManagementDevice {
            display_name: "Containment device".to_owned(),
            asset_id: Some(DEVICE_CONTAINMENT_ASSET_ID),
            device_profile_id: None,
            attributes: None,
            topology: None,
        },
    )
    .await
    .unwrap();

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].actor,
        AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
    );
    assert_eq!(events[0].action, AuditAction::AssetContainmentChanged);
    assert_eq!(events[0].target_type, AuditTargetType::Device);
    assert_eq!(events[0].target_id, DEVICE_CONTAINMENT_DEVICE);
    assert_eq!(
        events[0].changes,
        json!({
            "asset_id": {
                "before": null,
                "after": DEVICE_CONTAINMENT_ASSET_ID.to_string(),
            }
        })
    );
}

#[tokio::test]
async fn sqlite_asset_delete_audits_child_asset_and_device_detachments() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id)
         VALUES (?, ?, 'Delete parent', NULL), (?, ?, 'Delete child', ?)",
    )
    .bind(DELETE_PARENT_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(DELETE_CHILD_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(DELETE_PARENT_ASSET_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata, asset_id)
         VALUES (?, ?, 'Delete child device', '{}', ?)",
    )
    .bind(DELETE_CHILD_DEVICE)
    .bind(TENANT_ID.to_string())
    .bind(DELETE_PARENT_ASSET_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    ManagementAssetRepository::delete_management_asset(
        &store,
        TENANT_ID,
        tenant_actor(),
        DELETE_PARENT_ASSET_ID,
    )
    .await
    .unwrap();

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| {
        event.actor == AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
            && event.action == AuditAction::AssetContainmentChanged
    }));
    assert!(events.iter().any(|event| {
        event.target_type == AuditTargetType::Asset
            && event.target_id == DELETE_CHILD_ASSET_ID.to_string()
            && event.changes
                == json!({
                    "parent_asset_id": {
                        "before": DELETE_PARENT_ASSET_ID.to_string(),
                        "after": null,
                    }
                })
    }));
    assert!(events.iter().any(|event| {
        event.target_type == AuditTargetType::Device
            && event.target_id == DELETE_CHILD_DEVICE
            && event.changes
                == json!({
                    "asset_id": {
                        "before": DELETE_PARENT_ASSET_ID.to_string(),
                        "after": null,
                    }
                })
    }));
}

#[tokio::test]
async fn sqlite_device_relation_create_and_delete_emit_tenant_audit_events() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata)
         VALUES (?, ?, 'Relation source', '{}'), (?, ?, 'Relation target', '{}')",
    )
    .bind(RELATION_FROM_DEVICE)
    .bind(TENANT_ID.to_string())
    .bind(RELATION_TO_DEVICE)
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    let relation = DeviceRelationRepository::create_device_relation(
        &store,
        TENANT_ID,
        tenant_actor(),
        CreateDeviceRelation {
            from_device_id: RELATION_FROM_DEVICE.to_owned(),
            to_device_id: RELATION_TO_DEVICE.to_owned(),
            relation_type: "paired_with".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(
        DeviceRelationRepository::delete_device_relation(
            &store,
            TENANT_ID,
            tenant_actor(),
            relation.id,
        )
        .await
        .unwrap()
    );

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|event| event.action).collect::<Vec<_>>(),
        vec![
            AuditAction::DeviceRelationDeleted,
            AuditAction::DeviceRelationCreated,
        ]
    );
    assert!(events.iter().all(|event| {
        event.actor == AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
            && event.target_type == AuditTargetType::DeviceRelation
            && event.target_id == relation.id.to_string()
    }));
    assert_eq!(
        events[1].changes,
        json!({
            "from_device_id": RELATION_FROM_DEVICE,
            "to_device_id": RELATION_TO_DEVICE,
            "relation_type": "paired_with",
        })
    );
    assert_eq!(events[0].changes, events[1].changes);
}

#[tokio::test]
async fn sqlite_audited_mutation_rejects_an_actor_from_another_tenant() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'other-audit-actor', 'active')")
        .bind(OTHER_TENANT_ID.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?1, ?2, ?1, 'unused', 'active', 1)",
    )
    .bind(OTHER_TENANT_ACCOUNT_ID.to_string())
    .bind(OTHER_TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata)
         VALUES (?, ?, 'Relation source', '{}'), (?, ?, 'Relation target', '{}')",
    )
    .bind(RELATION_FROM_DEVICE)
    .bind(TENANT_ID.to_string())
    .bind(RELATION_TO_DEVICE)
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(
        DeviceRelationRepository::create_device_relation(
            &store,
            TENANT_ID,
            AuditPrincipal::TenantAccount(OTHER_TENANT_ACCOUNT_ID),
            CreateDeviceRelation {
                from_device_id: RELATION_FROM_DEVICE.to_owned(),
                to_device_id: RELATION_TO_DEVICE.to_owned(),
                relation_type: "paired_with".to_owned(),
            },
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_relations")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn sqlite_ownership_transfer_is_audited_and_rejects_cross_tenant_owners() {
    let (_directory, store) = sqlite_store().await;
    seed_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'other-audit-events', 'active')",
    )
    .bind(OTHER_TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'audit-owner-a', 'unused', 'viewer', 'user'),
                (?, ?, 'audit-owner-b', 'unused', 'viewer', 'user'),
                (?, ?, 'other-audit-owner', 'unused', 'viewer', 'user')",
    )
    .bind(OWNER_A.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_B.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OTHER_TENANT_USER_ID.to_string())
    .bind(OTHER_TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id)
         VALUES (?, ?, 'Owned asset', ?)",
    )
    .bind(OWNED_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_A.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(
        TenantAuthorizationRepository::transfer_resource_ownership(
            &store,
            TENANT_ID,
            AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID),
            OwnershipTransferTarget::Asset(OWNED_ASSET_ID),
            Some(OWNER_B),
        )
        .await
        .unwrap()
    );
    assert!(
        TenantAuthorizationRepository::transfer_resource_ownership(
            &store,
            TENANT_ID,
            AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID),
            OwnershipTransferTarget::Asset(OWNED_ASSET_ID),
            Some(OTHER_TENANT_USER_ID),
        )
        .await
        .is_err()
    );

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_ID, None, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].actor,
        AuditPrincipal::TenantAccount(TENANT_ACCOUNT_ID)
    );
    assert_eq!(events[0].action, AuditAction::OwnershipTransferred);
    assert_eq!(events[0].target_type, AuditTargetType::Asset);
    assert_eq!(events[0].target_id, OWNED_ASSET_ID.to_string());
    assert_eq!(
        events[0].changes,
        json!({
            "owner_user_id": {
                "before": OWNER_A.to_string(),
                "after": OWNER_B.to_string(),
            }
        })
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_audit_events_cover_permission_relation_and_containment_mutations() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let pool = store.timescale_pool().unwrap();
    let tenant_id = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    let tenant_account_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES ($1, 'audit-timescale', 'active')")
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'audit-timescale-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES ($1, $2, $1, 'unused', 'active', 1)",
    )
    .bind(tenant_account_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id)
         VALUES ('audit-timescale-device', $1),
                ('audit-timescale-relation-device', $1)",
    )
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let permission = store
        .create_resource_permission(iot_storage::NewResourcePermission {
            tenant_id,
            subject_user_id: Some(user_id),
            subject_group_id: None,
            asset_id: None,
            device_id: Some("audit-timescale-device".to_owned()),
            permission: iot_storage::ResourcePermission::Viewer,
            inherit_children: false,
            created_by: iot_storage::PermissionCreator::User(user_id),
        })
        .await
        .unwrap();
    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::TenantAccount(tenant_account_id),
        CreateManagementAsset {
            name: "Timescale audit parent".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::User(user_id),
        CreateManagementAsset {
            name: "Timescale audit child".to_owned(),
            asset_profile_id: None,
            parent_asset_id: Some(parent.id),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();
    let relation = DeviceRelationRepository::create_device_relation(
        &store,
        tenant_id,
        AuditPrincipal::TenantAccount(tenant_account_id),
        CreateDeviceRelation {
            from_device_id: "audit-timescale-device".to_owned(),
            to_device_id: "audit-timescale-relation-device".to_owned(),
            relation_type: "paired_with".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(
        DeviceRelationRepository::delete_device_relation(
            &store,
            tenant_id,
            AuditPrincipal::TenantAccount(tenant_account_id),
            relation.id,
        )
        .await
        .unwrap()
    );

    let events = AuditEventRepository::list_tenant_audit_events(&store, tenant_id, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 4);
    assert!(events.iter().any(|event| {
        event.actor == AuditPrincipal::User(user_id)
            && event.action == AuditAction::PermissionGranted
            && event.target_id == permission.id.to_string()
    }));
    assert!(events.iter().any(|event| {
        event.actor == AuditPrincipal::User(user_id)
            && event.action == AuditAction::AssetContainmentChanged
            && event.target_type == AuditTargetType::Asset
            && event.target_id == child.id.to_string()
            && event.changes
                == json!({
                    "parent_asset_id": {
                        "before": null,
                        "after": parent.id.to_string(),
                    }
                })
    }));
    assert!(events.iter().any(|event| {
        event.actor == AuditPrincipal::TenantAccount(tenant_account_id)
            && event.action == AuditAction::DeviceRelationCreated
            && event.target_id == relation.id.to_string()
    }));
    assert!(events.iter().any(|event| {
        event.actor == AuditPrincipal::TenantAccount(tenant_account_id)
            && event.action == AuditAction::DeviceRelationDeleted
            && event.target_id == relation.id.to_string()
    }));
    assert!(
        sqlx::query("UPDATE audit_events SET action = 'changed' WHERE id = $1")
            .bind(events[0].id)
            .execute(pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM audit_events WHERE id = $1")
            .bind(events[0].id)
            .execute(pool)
            .await
            .is_err()
    );
    let truncate_error = sqlx::query("TRUNCATE audit_events")
        .execute(pool)
        .await
        .unwrap_err();
    assert!(
        truncate_error
            .to_string()
            .contains("audit_events are immutable")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_events")
            .fetch_one(pool)
            .await
            .unwrap(),
        4
    );
}
