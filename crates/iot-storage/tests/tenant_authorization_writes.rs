use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AuditAction, AuditEventCursor, AuditEventError, AuditEventRepository, AuditPrincipal,
    AuditTargetType, NewResourcePermission, NewUserGroup, PermissionCreator, PlatformStore,
    ResourcePermission, TenantAuthorizationError, TenantAuthorizationRepository,
};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

const TENANT_A: Uuid = Uuid::from_u128(1);
const TENANT_B: Uuid = Uuid::from_u128(2);
const OWNER_A: Uuid = Uuid::from_u128(10);
const MEMBER_A: Uuid = Uuid::from_u128(11);
const CREATOR_A: Uuid = Uuid::from_u128(12);
const USER_B: Uuid = Uuid::from_u128(20);
const TENANT_ACCOUNT_A: Uuid = Uuid::from_u128(21);
const TENANT_ACCOUNT_B: Uuid = Uuid::from_u128(22);
const ASSET_A: Uuid = Uuid::from_u128(30);
const ASSET_B: Uuid = Uuid::from_u128(31);
const DEVICE_A: &str = "device-a";
const DEVICE_B: &str = "device-b";

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

async fn insert_user(pool: &SqlitePool, user_id: Uuid, tenant_id: Uuid, username: &str) {
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .bind(username)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_tenants(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'tenant-a', 'active'), (?, 'tenant-b', 'active')",
    )
    .bind(TENANT_A.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    insert_user(pool, OWNER_A, TENANT_A, "owner-a").await;
    insert_user(pool, MEMBER_A, TENANT_A, "member-a").await;
    insert_user(pool, CREATOR_A, TENANT_A, "creator-a").await;
    insert_user(pool, USER_B, TENANT_B, "user-b").await;
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, password_hash, status, credential_version
         ) VALUES (?, ?, 'unused', 'active', 1), (?, ?, 'unused', 'active', 1)",
    )
    .bind(TENANT_ACCOUNT_A.to_string())
    .bind(TENANT_A.to_string())
    .bind(TENANT_ACCOUNT_B.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'asset-a'), (?, ?, 'asset-b')",
    )
    .bind(ASSET_A.to_string())
    .bind(TENANT_A.to_string())
    .bind(ASSET_B.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id)
         VALUES (?, ?), (?, ?)",
    )
    .bind(DEVICE_A)
    .bind(TENANT_A.to_string())
    .bind(DEVICE_B)
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
}

fn direct_device_permission() -> NewResourcePermission {
    NewResourcePermission {
        tenant_id: TENANT_A,
        subject_user_id: Some(MEMBER_A),
        subject_group_id: None,
        asset_id: None,
        device_id: Some(DEVICE_A.to_owned()),
        permission: ResourcePermission::Viewer,
        inherit_children: false,
        created_by: PermissionCreator::User(CREATOR_A),
    }
}

fn tenant_account_actor(tenant_id: Uuid) -> AuditPrincipal {
    match tenant_id {
        TENANT_A => AuditPrincipal::TenantAccount(TENANT_ACCOUNT_A),
        TENANT_B => AuditPrincipal::TenantAccount(TENANT_ACCOUNT_B),
        _ => unreachable!("test actor requested for an unseeded tenant"),
    }
}

#[tokio::test]
async fn sqlite_permission_grant_emits_immutable_tenant_audit_event() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();

    let event = sqlx::query(
        "SELECT id, tenant_id, actor_principal_kind, actor_principal_id, action,
                target_type, target_id, changes
         FROM audit_events",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let event_id: String = event.try_get("id").unwrap();
    let changes: serde_json::Value =
        serde_json::from_str(&event.try_get::<String, _>("changes").unwrap()).unwrap();

    assert_eq!(
        event.try_get::<String, _>("tenant_id").unwrap(),
        TENANT_A.to_string()
    );
    assert_eq!(
        event.try_get::<String, _>("actor_principal_kind").unwrap(),
        "user"
    );
    assert_eq!(
        event.try_get::<String, _>("actor_principal_id").unwrap(),
        CREATOR_A.to_string()
    );
    assert_eq!(
        event.try_get::<String, _>("action").unwrap(),
        "permission.granted"
    );
    assert_eq!(
        event.try_get::<String, _>("target_type").unwrap(),
        "resource_permission"
    );
    assert_eq!(
        event.try_get::<String, _>("target_id").unwrap(),
        permission.id.to_string()
    );
    assert_eq!(
        changes,
        json!({
            "subject": {"kind": "user", "id": MEMBER_A.to_string()},
            "resource": {"kind": "device", "id": DEVICE_A},
            "permission": "viewer",
            "inherit_children": false,
        })
    );
    assert!(
        sqlx::query("UPDATE audit_events SET action = 'changed' WHERE id = ?")
            .bind(&event_id)
            .execute(pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM audit_events WHERE id = ?")
            .bind(&event_id)
            .execute(pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_tenant_audit_events_use_tenant_scoped_deterministic_keyset_pagination() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let first_permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();
    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "audit-readers".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    let second_permission = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: TENANT_A,
            subject_user_id: None,
            subject_group_id: Some(group.id),
            asset_id: Some(ASSET_A),
            device_id: None,
            permission: ResourcePermission::Manager,
            inherit_children: true,
            created_by: PermissionCreator::User(CREATOR_A),
        })
        .await
        .unwrap();

    let first_page = AuditEventRepository::list_tenant_audit_events(&store, TENANT_A, None, 1)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].tenant_id, TENANT_A);
    assert_eq!(first_page[0].actor, AuditPrincipal::User(CREATOR_A));
    assert_eq!(first_page[0].action, AuditAction::PermissionGranted);
    assert_eq!(
        first_page[0].target_type,
        AuditTargetType::ResourcePermission
    );
    assert_eq!(first_page[0].target_id, second_permission.id.to_string());

    let second_page = AuditEventRepository::list_tenant_audit_events(
        &store,
        TENANT_A,
        Some(AuditEventCursor {
            occurred_at: first_page[0].occurred_at,
            id: first_page[0].id,
        }),
        1,
    )
    .await
    .unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].target_id, first_permission.id.to_string());
    assert_ne!(second_page[0].id, first_page[0].id);
    assert!(
        AuditEventRepository::list_tenant_audit_events(&store, TENANT_B, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        AuditEventRepository::list_tenant_audit_events(&store, TENANT_A, None, 0).await,
        Err(AuditEventError::InvalidLimit { .. })
    ));
}

#[tokio::test]
async fn sqlite_tenant_audit_list_is_tenant_scoped_keyset_ordered_and_limited_to_100_records() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    for _ in 0..101 {
        store
            .create_resource_permission(direct_device_permission())
            .await
            .unwrap();
    }

    let first_page = AuditEventRepository::list_tenant_audit_events(&store, TENANT_A, None, 100)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 100);
    assert!(first_page.iter().all(|event| event.tenant_id == TENANT_A));
    assert!(first_page.windows(2).all(|events| {
        (events[0].occurred_at, events[0].id) > (events[1].occurred_at, events[1].id)
    }));

    let second_page = AuditEventRepository::list_tenant_audit_events(
        &store,
        TENANT_A,
        Some(AuditEventCursor {
            occurred_at: first_page.last().unwrap().occurred_at,
            id: first_page.last().unwrap().id,
        }),
        100,
    )
    .await
    .unwrap();
    assert_eq!(second_page.len(), 1);
    assert!(second_page.iter().all(|event| event.tenant_id == TENANT_A));
    assert!(
        AuditEventRepository::list_tenant_audit_events(&store, TENANT_B, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn sqlite_membership_and_permission_revocation_emit_tenant_audit_events() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "audited-membership".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    assert!(
        store
            .add_user_to_group(TENANT_A, tenant_account_actor(TENANT_A), group.id, MEMBER_A)
            .await
            .unwrap()
    );
    let permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();
    assert!(
        store
            .revoke_resource_permission(TENANT_A, tenant_account_actor(TENANT_A), permission.id,)
            .await
            .unwrap()
    );
    assert!(
        store
            .remove_user_from_group(TENANT_A, tenant_account_actor(TENANT_A), group.id, MEMBER_A,)
            .await
            .unwrap()
    );

    let events = AuditEventRepository::list_tenant_audit_events(&store, TENANT_A, None, 10)
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|event| event.action).collect::<Vec<_>>(),
        vec![
            AuditAction::GroupMemberRemoved,
            AuditAction::PermissionRevoked,
            AuditAction::PermissionGranted,
            AuditAction::GroupMemberAdded,
        ]
    );
    assert_eq!(
        events[0].actor,
        AuditPrincipal::TenantAccount(TENANT_ACCOUNT_A)
    );
    assert_eq!(events[0].target_type, AuditTargetType::UserGroup);
    assert_eq!(events[0].target_id, group.id.to_string());
    assert_eq!(
        events[0].changes,
        json!({"member_user_id": MEMBER_A.to_string()})
    );
    assert_eq!(
        events[1].actor,
        AuditPrincipal::TenantAccount(TENANT_ACCOUNT_A)
    );
    assert_eq!(events[1].target_type, AuditTargetType::ResourcePermission);
    assert_eq!(events[1].target_id, permission.id.to_string());
    assert_eq!(events[1].changes, json!({"revoked": true}));
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_create_direct_and_group_permissions() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "operators".to_owned(),
            metadata: json!({"site": "north"}),
        })
        .await
        .unwrap();
    assert_eq!(group.tenant_id, TENANT_A);
    assert_eq!(group.owner_user_id, OWNER_A);
    assert_eq!(group.name, "operators");
    assert!(
        store
            .add_user_to_group(TENANT_A, tenant_account_actor(TENANT_A), group.id, MEMBER_A)
            .await
            .unwrap()
    );

    let direct_permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();
    let group_permission = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: TENANT_A,
            subject_user_id: None,
            subject_group_id: Some(group.id),
            asset_id: Some(ASSET_A),
            device_id: None,
            permission: ResourcePermission::Manager,
            inherit_children: true,
            created_by: PermissionCreator::User(CREATOR_A),
        })
        .await
        .unwrap();

    assert_eq!(direct_permission.subject_user_id, Some(MEMBER_A));
    assert_eq!(direct_permission.device_id.as_deref(), Some(DEVICE_A));
    assert_eq!(group_permission.subject_group_id, Some(group.id));
    assert_eq!(group_permission.asset_id, Some(ASSET_A));
    assert!(group_permission.inherit_children);
    assert!(
        store
            .remove_user_from_group(TENANT_A, tenant_account_actor(TENANT_A), group.id, MEMBER_A,)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM user_group_members WHERE group_id = ? AND user_id = ?",
        )
        .bind(group.id.to_string())
        .bind(MEMBER_A.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_reject_cross_tenant_and_invalid_permission_shapes() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;
    let group_a = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "tenant-a-group".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    let group_b = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_B,
            owner_user_id: USER_B,
            name: "tenant-b-group".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();

    assert!(matches!(
        store
            .create_user_group(NewUserGroup {
                tenant_id: TENANT_A,
                owner_user_id: USER_B,
                name: "invalid-owner".to_owned(),
                metadata: json!({}),
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .add_user_to_group(TENANT_A, tenant_account_actor(TENANT_A), group_a.id, USER_B,)
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: Some(USER_B),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: None,
                subject_group_id: Some(group_b.id),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::GroupNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                asset_id: Some(ASSET_B),
                device_id: None,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::AssetNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                device_id: Some(DEVICE_B.to_owned()),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::DeviceNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                created_by: PermissionCreator::User(USER_B),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: None,
                subject_group_id: None,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionSubject)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_group_id: Some(group_a.id),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionSubject)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                asset_id: Some(ASSET_A),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionResource)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                permission: ResourcePermission::Owner,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionLevel {
            permission: ResourcePermission::Owner
        })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                inherit_children: true,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::DevicePermissionCannotInherit)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_permissions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_revoke_only_the_tenant_permission() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;
    let permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();

    assert!(matches!(
        store
            .revoke_resource_permission(TENANT_B, tenant_account_actor(TENANT_B), permission.id,)
            .await,
        Err(TenantAuthorizationError::PermissionNotFound { .. })
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    assert!(
        store
            .revoke_resource_permission(TENANT_A, tenant_account_actor(TENANT_A), permission.id,)
            .await
            .unwrap()
    );
    assert!(
        !store
            .revoke_resource_permission(TENANT_A, tenant_account_actor(TENANT_A), permission.id,)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NOT NULL",
        )
        .bind(permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_tenant_authorization_lists_tenant_groups_and_active_permissions() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let group_a = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "operators".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    store
        .add_user_to_group(
            TENANT_A,
            tenant_account_actor(TENANT_A),
            group_a.id,
            MEMBER_A,
        )
        .await
        .unwrap();
    let group_b = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_B,
            owner_user_id: USER_B,
            name: "other-operators".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    store
        .add_user_to_group(TENANT_B, tenant_account_actor(TENANT_B), group_b.id, USER_B)
        .await
        .unwrap();

    let active = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: TENANT_A,
            subject_user_id: None,
            subject_group_id: Some(group_a.id),
            asset_id: Some(ASSET_A),
            device_id: None,
            permission: ResourcePermission::Manager,
            inherit_children: true,
            created_by: PermissionCreator::User(CREATOR_A),
        })
        .await
        .unwrap();
    let revoked = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();
    store
        .revoke_resource_permission(TENANT_A, tenant_account_actor(TENANT_A), revoked.id)
        .await
        .unwrap();

    let groups = TenantAuthorizationRepository::list_tenant_user_groups(&store, TENANT_A)
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].id, group_a.id);
    assert_eq!(groups[0].name, "operators");
    assert_eq!(groups[0].members.len(), 1);
    assert_eq!(groups[0].members[0].user_id, MEMBER_A);
    assert_eq!(groups[0].members[0].username, "member-a");

    let permissions =
        TenantAuthorizationRepository::list_active_resource_permissions(&store, TENANT_A)
            .await
            .unwrap();
    assert_eq!(permissions, vec![active]);
}
