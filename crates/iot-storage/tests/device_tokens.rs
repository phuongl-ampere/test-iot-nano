use iot_nano_foundation::{
    DatabaseStorage, StorageConfiguration, device_token_prefix, generate_device_token,
    hash_device_token,
};
use iot_storage::{
    AuditPrincipal, DeviceTokenRepository, DeviceTokenRepositoryError, IdentityRepository,
    ManagementDeviceError, ManagementDeviceRepository, ManagementDeviceTopology, NewDeviceToken,
    NewOwnedDeviceToken, PlatformStore, UpdateManagementDevice,
};
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use uuid::Uuid;

mod common;

fn provisioning_tenant_id() -> Uuid {
    Uuid::from_u128(10_008)
}

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("device-tokens.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'device-tokens', 'active')")
        .bind(provisioning_tenant_id().to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?1, ?2, ?1, 'unused', 'active', 1)",
    )
    .bind(provisioning_tenant_id().to_string())
    .bind(provisioning_tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store)
}

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_store() -> (TimescaleTestLock, PlatformStore) {
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
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES ($1, 'device-tokens', 'active')")
        .bind(provisioning_tenant_id())
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES ($1, $2, $1, 'unused', 'active', 1)",
    )
    .bind(provisioning_tenant_id())
    .bind(provisioning_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

fn token(prefix: &str) -> NewDeviceToken {
    NewDeviceToken {
        id: Uuid::now_v7(),
        token_prefix: prefix.to_owned(),
        token_hash: format!("{prefix}-hash"),
        token_ciphertext: format!("{prefix}-ciphertext"),
    }
}

fn tenant_actor(tenant_id: Uuid) -> AuditPrincipal {
    AuditPrincipal::TenantAccount(tenant_id)
}

fn generated_token() -> (String, NewDeviceToken) {
    let value = generate_device_token();
    let token = NewDeviceToken {
        id: Uuid::now_v7(),
        token_prefix: device_token_prefix(&value).unwrap().to_owned(),
        token_hash: hash_device_token(&value).unwrap(),
        token_ciphertext: "encrypted-token-material".to_owned(),
    };
    (value, token)
}

#[tokio::test]
async fn sqlite_device_token_repository_provisions_rotates_and_rejects_gateway_children() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway) VALUES
             ('token-direct', ?, 0),
             ('token-gateway', ?, 1),
             ('token-child', ?, 0)",
    )
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET gateway_device_id = 'token-gateway'
         WHERE device_id = 'token-child'",
    )
    .execute(pool)
    .await
    .unwrap();

    let provisioned = DeviceTokenRepository::provision_device_token(
        &store,
        tenant_id,
        "Provisioned device",
        token("provisioned-token"),
    )
    .await
    .unwrap();
    assert_eq!(provisioned.token_prefix, "provisioned-token");
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT display_name FROM devices WHERE device_id = ?",
        )
        .bind(&provisioned.device_id)
        .fetch_one(pool)
        .await
        .unwrap()
        .is_some()
    );

    let first = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-direct",
        token("direct-token-one"),
    )
    .await
    .unwrap();
    let replacement = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-direct",
        token("direct-token-two"),
    )
    .await
    .unwrap();
    assert_eq!(replacement.device_id, "token-direct");
    assert_eq!(replacement.token_prefix, "direct-token-two");
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE id = ?",
        )
        .bind(first.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
        .is_some(),
        true
    );

    let child_error = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-child",
        token("child-token"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        child_error,
        DeviceTokenRepositoryError::GatewayChild
    ));

    let missing_error = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "missing-device",
        token("missing-token"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        missing_error,
        DeviceTokenRepositoryError::DeviceNotFound
    ));
}

#[tokio::test]
async fn sqlite_device_token_repository_rejects_cross_tenant_token_issuance_and_rotation() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let other_tenant_id = Uuid::from_u128(10_009);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'other-device-tokens', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?1, ?2, ?1, 'unused', 'active', 1)",
    )
    .bind(other_tenant_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let issued = DeviceTokenRepository::provision_device_token(
        &store,
        tenant_id,
        "Tenant A device",
        token("tenant-a-token"),
    )
    .await
    .unwrap();
    let history_before =
        DeviceTokenRepository::list_device_tokens(&store, tenant_id, &issued.device_id)
            .await
            .unwrap();
    assert_eq!(history_before.len(), 1);
    assert_eq!(history_before[0].id, issued.id);
    assert!(history_before[0].revoked_at.is_none());

    let issuance_error = DeviceTokenRepository::create_device_token(
        &store,
        other_tenant_id,
        &issued.device_id,
        token("cross-tenant-issue"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        issuance_error,
        DeviceTokenRepositoryError::DeviceNotFound
    ));

    let rotation_error = DeviceTokenRepository::rotate_device_token(
        &store,
        other_tenant_id,
        issued.id,
        token("cross-tenant-rotate"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        rotation_error,
        DeviceTokenRepositoryError::TokenNotFound
    ));
    let history_after =
        DeviceTokenRepository::list_device_tokens(&store, tenant_id, &issued.device_id)
            .await
            .unwrap();
    assert_eq!(history_after.len(), 1);
    assert_eq!(history_after[0].id, issued.id);
    assert!(history_after[0].revoked_at.is_none());

    assert!(
        DeviceTokenRepository::list_device_tokens(&store, other_tenant_id, &issued.device_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        DeviceTokenRepository::active_device_token(&store, other_tenant_id, issued.id)
            .await
            .unwrap()
            .is_none()
    );
    let revoke_error =
        DeviceTokenRepository::revoke_device_token(&store, other_tenant_id, issued.id)
            .await
            .unwrap_err();
    assert!(matches!(
        revoke_error,
        DeviceTokenRepositoryError::TokenNotFound
    ));

    assert_eq!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, issued.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        issued.id
    );
}

#[tokio::test]
async fn sqlite_management_token_revocations_remain_tenant_scoped_after_device_mutations() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let other_tenant_id = Uuid::from_u128(10_009);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'management-token-other', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?1, ?2, ?1, 'unused', 'active', 1)",
    )
    .bind(other_tenant_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway) VALUES
             ('topology-gateway', ?, 1),
             ('topology-race', ?, 0),
             ('delete-race', ?, 0)",
    )
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let topology_token = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "topology-race",
        token("topology-race-token"),
    )
    .await
    .unwrap();
    let deleted_token = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "delete-race",
        token("delete-race-token"),
    )
    .await
    .unwrap();

    let topology_error = ManagementDeviceRepository::update_management_device(
        &store,
        other_tenant_id,
        tenant_actor(other_tenant_id),
        "topology-race",
        UpdateManagementDevice {
            display_name: "Cross-tenant topology".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: None,
            topology: Some(ManagementDeviceTopology {
                is_gateway: false,
                gateway_device_id: Some("topology-gateway".to_owned()),
            }),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        topology_error,
        ManagementDeviceError::DeviceNotFound
    ));

    let delete_error = ManagementDeviceRepository::delete_management_device(
        &store,
        other_tenant_id,
        tenant_actor(other_tenant_id),
        "delete-race",
    )
    .await
    .unwrap_err();
    assert!(matches!(
        delete_error,
        ManagementDeviceError::DeviceNotFound
    ));

    let devices = ManagementDeviceRepository::list_management_devices(&store, tenant_id)
        .await
        .unwrap();
    assert_eq!(
        devices
            .iter()
            .find(|device| device.device_id == "topology-race")
            .unwrap()
            .topology,
        ManagementDeviceTopology {
            is_gateway: false,
            gateway_device_id: None,
        }
    );
    assert!(
        devices
            .iter()
            .any(|device| device.device_id == "delete-race")
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM devices WHERE device_id = 'delete-race'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
        .is_none()
    );

    for (device_id, token_id) in [
        ("topology-race", topology_token.id),
        ("delete-race", deleted_token.id),
    ] {
        let active = DeviceTokenRepository::active_device_token(&store, tenant_id, token_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.id, token_id);
        let history = DeviceTokenRepository::list_device_tokens(&store, tenant_id, device_id)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, token_id);
        assert!(history[0].revoked_at.is_none());
    }
}

#[tokio::test]
async fn sqlite_management_token_revocations_require_tenant_after_post_mutation_change() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let other_tenant_id = Uuid::from_u128(10_009);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'post-mutation-other', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway) VALUES
             ('post-mutation-gateway', ?, 1),
             ('post-mutation-topology', ?, 0),
             ('post-mutation-delete', ?, 0)",
    )
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let topology_token = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "post-mutation-topology",
        token("post-mutation-topology-token"),
    )
    .await
    .unwrap();
    let deleted_token = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "post-mutation-delete",
        token("post-mutation-delete-token"),
    )
    .await
    .unwrap();

    let topology_trigger = format!(
        "CREATE TRIGGER move_post_mutation_topology_to_other_tenant
         AFTER UPDATE OF gateway_device_id ON devices
         WHEN NEW.device_id = 'post-mutation-topology' AND NEW.tenant_id = '{tenant_id}'
         BEGIN
             UPDATE devices
             SET tenant_id = '{other_tenant_id}', gateway_device_id = NULL
             WHERE device_id = 'post-mutation-topology';
         END",
    );
    sqlx::query(AssertSqlSafe(topology_trigger))
        .execute(pool)
        .await
        .unwrap();
    assert!(matches!(
        ManagementDeviceRepository::update_management_device(
            &store,
            tenant_id,
            tenant_actor(tenant_id),
            "post-mutation-topology",
            UpdateManagementDevice {
                display_name: "Post-mutation topology".to_owned(),
                asset_id: None,
                device_profile_id: None,
                attributes: None,
                topology: Some(ManagementDeviceTopology {
                    is_gateway: false,
                    gateway_device_id: Some("post-mutation-gateway".to_owned()),
                }),
            },
        )
        .await
        .unwrap_err(),
        ManagementDeviceError::DeviceNotFound
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT tenant_id FROM devices WHERE device_id = 'post-mutation-topology'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        other_tenant_id.to_string()
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE id = ?",
        )
        .bind(topology_token.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
        .is_none()
    );

    let delete_trigger = format!(
        "CREATE TRIGGER move_post_mutation_delete_to_other_tenant
         AFTER UPDATE OF deleted_at ON devices
         WHEN NEW.device_id = 'post-mutation-delete' AND NEW.tenant_id = '{tenant_id}'
         BEGIN
             UPDATE devices
             SET tenant_id = '{other_tenant_id}'
             WHERE device_id = 'post-mutation-delete';
         END",
    );
    sqlx::query(AssertSqlSafe(delete_trigger))
        .execute(pool)
        .await
        .unwrap();
    ManagementDeviceRepository::delete_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "post-mutation-delete",
    )
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM devices WHERE device_id = 'post-mutation-delete'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE id = ?",
        )
        .bind(deleted_token.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn sqlite_device_token_repository_reports_prefix_conflicts_without_revoking_active_tokens() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('token-conflict', ?)")
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();

    let active = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-conflict",
        token("conflict-active"),
    )
    .await
    .unwrap();
    let conflict = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-conflict",
        NewDeviceToken {
            id: Uuid::now_v7(),
            token_prefix: active.token_prefix.clone(),
            token_hash: "duplicate-hash".to_owned(),
            token_ciphertext: "duplicate-ciphertext".to_owned(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        conflict,
        DeviceTokenRepositoryError::TokenPrefixConflict
    ));
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE id = ?",
        )
        .bind(active.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn sqlite_device_token_repository_lists_rotates_and_revokes_opaque_history() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('token-history', ?)")
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();

    let first = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-history",
        token("history-token-one"),
    )
    .await
    .unwrap();
    let initial_history =
        DeviceTokenRepository::list_device_tokens(&store, tenant_id, "token-history")
            .await
            .unwrap();
    assert_eq!(initial_history.len(), 1);
    assert_eq!(initial_history[0].id, first.id);
    assert_eq!(initial_history[0].token_prefix, "history-token-one");
    assert!(initial_history[0].revoked_at.is_none());
    assert_eq!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, first.id)
            .await
            .unwrap()
            .unwrap()
            .device_id,
        "token-history"
    );

    let rotated = DeviceTokenRepository::rotate_device_token(
        &store,
        tenant_id,
        first.id,
        token("history-token-two"),
    )
    .await
    .unwrap();
    assert_eq!(rotated.token_prefix, "history-token-two");
    assert!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, first.id)
            .await
            .unwrap()
            .is_none()
    );

    let history = DeviceTokenRepository::list_device_tokens(&store, tenant_id, "token-history")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, rotated.id);
    assert!(history[0].revoked_at.is_none());
    assert_eq!(history[1].id, first.id);
    assert!(history[1].revoked_at.is_some());

    DeviceTokenRepository::revoke_device_token(&store, tenant_id, rotated.id)
        .await
        .unwrap();
    assert!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, rotated.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_device_token_repository_provisions_owned_devices_and_identity_resolves_them() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let owner_user_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'token-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_user_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'token asset')")
        .bind(asset_id.to_string())
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    let (raw_token, token) = generated_token();

    let issued = DeviceTokenRepository::provision_owned_device_token(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        NewOwnedDeviceToken {
            display_name: "Owned device".to_owned(),
            owner_user_id,
            asset_id: Some(asset_id),
            token,
        },
    )
    .await
    .unwrap();
    let ownership: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT tenant_id, owner_user_id, asset_id, claimed_at
         FROM devices WHERE device_id = ?",
    )
    .bind(&issued.device_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(ownership.0, tenant_id.to_string());
    assert_eq!(ownership.1, owner_user_id.to_string());
    assert_eq!(ownership.2, asset_id.to_string());
    assert!(ownership.3.is_some());

    let resolved = IdentityRepository::resolve_active_device_token(&store, &raw_token)
        .await
        .unwrap();
    assert_eq!(resolved.token_id, issued.id);
    assert_eq!(resolved.device_id, issued.device_id);
}

#[tokio::test]
async fn sqlite_owned_token_provision_rejects_cross_tenant_references() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let other_tenant_id = Uuid::from_u128(10_009);
    let owner_user_id = Uuid::now_v7();
    let other_owner_user_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'other-device-tokens', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'cross-tenant-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_user_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'other-cross-tenant-owner', 'unused', 'viewer', 'user')",
    )
    .bind(other_owner_user_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'other tenant asset')")
        .bind(asset_id.to_string())
        .bind(other_tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();

    let error = DeviceTokenRepository::provision_owned_device_token(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        NewOwnedDeviceToken {
            display_name: "Cross-tenant owned device".to_owned(),
            owner_user_id,
            asset_id: Some(asset_id),
            token: token("cross-tenant-owned-token"),
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, DeviceTokenRepositoryError::DeviceNotFound));

    let cross_tenant_owner_error = DeviceTokenRepository::provision_owned_device_token(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        NewOwnedDeviceToken {
            display_name: "Cross-tenant owner device".to_owned(),
            owner_user_id: other_owner_user_id,
            asset_id: None,
            token: token("cross-tenant-owner-token"),
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        cross_tenant_owner_error,
        DeviceTokenRepositoryError::DeviceNotFound
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM devices")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_token_repository_matches_sqlite_lifecycle_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('token-history', $1)")
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();

    let first = DeviceTokenRepository::create_device_token(
        &store,
        tenant_id,
        "token-history",
        token("timescale-history-token-one"),
    )
    .await
    .unwrap();
    let other_tenant_id = Uuid::from_u128(10_009);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'other-device-tokens', 'active')",
    )
    .bind(other_tenant_id)
    .execute(pool)
    .await
    .unwrap();
    let history_before =
        DeviceTokenRepository::list_device_tokens(&store, tenant_id, "token-history")
            .await
            .unwrap();
    assert_eq!(history_before.len(), 1);
    assert_eq!(history_before[0].id, first.id);
    assert!(history_before[0].revoked_at.is_none());
    assert!(
        DeviceTokenRepository::list_device_tokens(&store, other_tenant_id, "token-history")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        DeviceTokenRepository::active_device_token(&store, other_tenant_id, first.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        DeviceTokenRepository::rotate_device_token(
            &store,
            other_tenant_id,
            first.id,
            token("timescale-cross-tenant-rotate"),
        )
        .await
        .unwrap_err(),
        DeviceTokenRepositoryError::TokenNotFound
    ));
    let history_after =
        DeviceTokenRepository::list_device_tokens(&store, tenant_id, "token-history")
            .await
            .unwrap();
    assert_eq!(history_after.len(), 1);
    assert_eq!(history_after[0].id, first.id);
    assert!(history_after[0].revoked_at.is_none());
    assert!(matches!(
        DeviceTokenRepository::revoke_device_token(&store, other_tenant_id, first.id)
            .await
            .unwrap_err(),
        DeviceTokenRepositoryError::TokenNotFound
    ));
    assert_eq!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, first.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        first.id
    );
    let rotated = DeviceTokenRepository::rotate_device_token(
        &store,
        tenant_id,
        first.id,
        token("timescale-history-token-two"),
    )
    .await
    .unwrap();
    let history = DeviceTokenRepository::list_device_tokens(&store, tenant_id, "token-history")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, rotated.id);
    assert!(history[0].revoked_at.is_none());
    assert_eq!(history[1].id, first.id);
    assert!(history[1].revoked_at.is_some());

    DeviceTokenRepository::revoke_device_token(&store, tenant_id, rotated.id)
        .await
        .unwrap();
    assert!(
        DeviceTokenRepository::active_device_token(&store, tenant_id, rotated.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_token_repository_provisions_owned_devices_and_identity_resolves_them() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let tenant_id = provisioning_tenant_id();
    let owner_user_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'token-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_user_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES ($1, $2, 'token asset')")
        .bind(asset_id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    let (raw_token, issued_token) = generated_token();

    let issued = DeviceTokenRepository::provision_owned_device_token(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        NewOwnedDeviceToken {
            display_name: "Owned device".to_owned(),
            owner_user_id,
            asset_id: Some(asset_id),
            token: issued_token,
        },
    )
    .await
    .unwrap();
    let ownership: (Uuid, Uuid, Uuid, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT tenant_id, owner_user_id, asset_id, claimed_at
         FROM devices WHERE device_id = $1",
    )
    .bind(&issued.device_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(ownership.0, tenant_id);
    assert_eq!(ownership.1, owner_user_id);
    assert_eq!(ownership.2, asset_id);
    assert!(ownership.3.is_some());

    let resolved = IdentityRepository::resolve_active_device_token(&store, &raw_token)
        .await
        .unwrap();
    assert_eq!(resolved.token_id, issued.id);
    assert_eq!(resolved.device_id, issued.device_id);

    let other_tenant_id = Uuid::from_u128(10_009);
    let other_owner_user_id = Uuid::now_v7();
    let other_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'other-device-tokens', 'active')",
    )
    .bind(other_tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'other-token-owner', 'unused', 'viewer', 'user')",
    )
    .bind(other_owner_user_id)
    .bind(other_tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES ($1, $2, 'other token asset')")
        .bind(other_asset_id)
        .bind(other_tenant_id)
        .execute(pool)
        .await
        .unwrap();
    assert!(matches!(
        DeviceTokenRepository::provision_owned_device_token(
            &store,
            tenant_id,
            tenant_actor(tenant_id),
            NewOwnedDeviceToken {
                display_name: "Cross-tenant asset device".to_owned(),
                owner_user_id,
                asset_id: Some(other_asset_id),
                token: token("timescale-cross-tenant-owned-asset"),
            },
        )
        .await
        .unwrap_err(),
        DeviceTokenRepositoryError::DeviceNotFound
    ));
    assert!(matches!(
        DeviceTokenRepository::provision_owned_device_token(
            &store,
            tenant_id,
            tenant_actor(tenant_id),
            NewOwnedDeviceToken {
                display_name: "Cross-tenant owner device".to_owned(),
                owner_user_id: other_owner_user_id,
                asset_id: None,
                token: token("timescale-cross-tenant-owned-owner"),
            },
        )
        .await
        .unwrap_err(),
        DeviceTokenRepositoryError::DeviceNotFound
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM devices")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
}
