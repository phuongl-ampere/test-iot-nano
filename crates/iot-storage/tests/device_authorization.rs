use iot_core::{
    DatabaseStorage, StorageConfiguration, device_token_prefix, generate_device_token,
    hash_device_token,
};
use iot_storage::{PlatformStore, PlatformStoreError, TenantIdentityRepository};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

mod common;

fn tenant_id() -> Uuid {
    Uuid::from_u128(1)
}

async fn seed_tenant(store: &PlatformStore) {
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT OR IGNORE INTO tenants (id, slug, status, metadata)
                 VALUES (?, 'device-authorization', 'active', '{}')",
            )
            .bind(tenant_id().to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO tenants (id, slug, status, metadata)
                 VALUES ($1, 'device-authorization', 'active', '{}'::jsonb)
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(tenant_id())
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

async fn store() -> (tempfile::TempDir, PlatformStore) {
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

async fn token(store: &PlatformStore, device_id: &str, value: &str, revoked: bool) -> Uuid {
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash, revoked_at)
         VALUES (?, ?, ?, ?, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END)",
    )
    .bind(token_id.to_string())
    .bind(device_id)
    .bind(device_token_prefix(value).unwrap())
    .bind(hash_device_token(value).unwrap())
    .bind(revoked)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    token_id
}

async fn timescale_store() -> (PgConnection, PlatformStore) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(database_name.starts_with("iot_nano_test_"));
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
    (connection, store)
}

#[tokio::test]
async fn sqlite_device_authorization_rejects_revoked_child_and_mismatched_sessions() {
    let (_directory, store) = store().await;
    seed_tenant(&store).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('direct', ?, 0, NULL), ('gateway', ?, 1, NULL), ('child', ?, 0, 'gateway')",
    )
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let direct = generate_device_token();
    let gateway = generate_device_token();
    let revoked = generate_device_token();
    let child = generate_device_token();
    let direct_id = token(&store, "direct", &direct, false).await;
    let gateway_id = token(&store, "gateway", &gateway, false).await;
    let revoked_id = token(&store, "direct", &revoked, true).await;
    let child_id = token(&store, "child", &child, false).await;

    assert!(
        store
            .authorize_device_session(direct_id, tenant_id(), "direct")
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_device_session(gateway_id, tenant_id(), "gateway")
            .await
            .is_ok()
    );
    let cross_tenant_id = Uuid::from_u128(2);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'other-device-authorization', 'active', '{}')",
    )
    .bind(cross_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert!(matches!(
        store
            .authorize_device_session(direct_id, cross_tenant_id, "direct")
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
    for denied in [
        store
            .authorize_device_session(direct_id, tenant_id(), "other")
            .await,
        store
            .authorize_device_session(child_id, tenant_id(), "child")
            .await,
        store
            .authorize_device_session(revoked_id, tenant_id(), "direct")
            .await,
    ] {
        assert!(matches!(denied, Err(PlatformStoreError::DeviceTokenDenied)));
    }
}

#[tokio::test]
async fn sqlite_gateway_authorization_requires_exact_active_gateway_and_child() {
    let (_directory, store) = store().await;
    seed_tenant(&store).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('gateway', ?, 1, NULL), ('deleted-gateway', ?, 1, NULL),
                ('other-gateway', ?, 1, NULL), ('child', ?, 0, 'gateway'),
                ('other-child', ?, 0, 'other-gateway'), ('direct', ?, 0, NULL)",
    )
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let gateway = generate_device_token();
    let gateway_id = token(&store, "gateway", &gateway, false).await;
    let revoked_gateway = generate_device_token();
    let revoked_gateway_id = token(&store, "gateway", &revoked_gateway, true).await;
    let deleted_gateway = generate_device_token();
    let deleted_gateway_id = token(&store, "deleted-gateway", &deleted_gateway, false).await;
    sqlx::query(
        "UPDATE devices SET deleted_at = CURRENT_TIMESTAMP WHERE device_id = 'deleted-gateway'",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert!(
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", Some("child"))
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", None)
            .await
            .is_ok()
    );
    for denied in [
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "other-gateway", Some("child"))
            .await,
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", Some("other-child"))
            .await,
        store
            .authorize_gateway_token(revoked_gateway_id, tenant_id(), "gateway", None)
            .await,
        store
            .authorize_gateway_token(deleted_gateway_id, tenant_id(), "deleted-gateway", None)
            .await,
    ] {
        assert!(matches!(denied, Err(PlatformStoreError::DeviceTokenDenied)));
    }

    sqlx::query("UPDATE devices SET deleted_at = CURRENT_TIMESTAMP WHERE device_id = 'child'")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", Some("child"))
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
    let is_gateway: i64 = sqlx::query("SELECT is_gateway FROM devices WHERE device_id = 'direct'")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .try_get("is_gateway")
        .unwrap();
    assert_eq!(is_gateway, 0);
}

#[tokio::test]
async fn sqlite_suspended_tenant_rejects_previously_authenticated_device_and_gateway_sessions() {
    let (_directory, store) = store().await;
    seed_tenant(&store).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('direct', ?, 0, NULL), ('gateway', ?, 1, NULL), ('child', ?, 0, 'gateway')",
    )
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let direct = generate_device_token();
    let gateway = generate_device_token();
    token(&store, "direct", &direct, false).await;
    token(&store, "gateway", &gateway, false).await;

    let authenticated_direct = store.resolve_active_device_token(&direct).await.unwrap();
    let authenticated_gateway = store.resolve_active_device_token(&gateway).await.unwrap();
    assert!(
        store
            .authorize_device_session(authenticated_direct.token_id, tenant_id(), "direct")
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_gateway_token(
                authenticated_gateway.token_id,
                tenant_id(),
                "gateway",
                Some("child"),
            )
            .await
            .is_ok()
    );

    TenantIdentityRepository::suspend_tenant(&store, "device-authorization")
        .await
        .unwrap();

    assert!(matches!(
        store
            .authorize_device_session(authenticated_direct.token_id, tenant_id(), "direct")
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
    assert!(matches!(
        store
            .authorize_gateway_token(
                authenticated_gateway.token_id,
                tenant_id(),
                "gateway",
                Some("child"),
            )
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
}

#[tokio::test]
async fn sqlite_suspended_tenant_registration_returns_unknown_tenant_without_creating_device() {
    let (_directory, store) = store().await;
    seed_tenant(&store).await;
    TenantIdentityRepository::suspend_tenant(&store, "device-authorization")
        .await
        .unwrap();

    assert!(matches!(
        store.register_device(tenant_id(), "suspended-device").await,
        Err(PlatformStoreError::UnknownTenant(id)) if id == tenant_id()
    ));
    let device_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_id = 'suspended-device'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(device_count, 0);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_authorization_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    seed_tenant(&store).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('gateway', $1, TRUE, NULL), ('deleted-gateway', $1, TRUE, NULL),
                ('other-gateway', $1, TRUE, NULL), ('child', $1, FALSE, 'gateway'),
                ('other-child', $1, FALSE, 'other-gateway'), ('direct', $1, FALSE, NULL)",
    )
    .bind(tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let direct = generate_device_token();
    let gateway = generate_device_token();
    let revoked = generate_device_token();
    let deleted = generate_device_token();
    let direct_id = token_timescale(&store, "direct", &direct, false).await;
    let gateway_id = token_timescale(&store, "gateway", &gateway, false).await;
    let revoked_id = token_timescale(&store, "gateway", &revoked, true).await;
    let deleted_id = token_timescale(&store, "deleted-gateway", &deleted, false).await;
    sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = 'deleted-gateway'")
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();

    assert!(
        store
            .authorize_device_session(direct_id, tenant_id(), "direct")
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_device_session(gateway_id, tenant_id(), "gateway")
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_device_session(gateway_id, tenant_id(), "child")
            .await
            .is_err()
    );
    assert!(
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", None)
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", Some("child"))
            .await
            .is_ok()
    );
    for denied in [
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "wrong-gateway", None)
            .await,
        store
            .authorize_gateway_token(gateway_id, tenant_id(), "gateway", Some("other-child"))
            .await,
        store
            .authorize_gateway_token(revoked_id, tenant_id(), "gateway", None)
            .await,
        store
            .authorize_gateway_token(deleted_id, tenant_id(), "deleted-gateway", None)
            .await,
    ] {
        assert!(matches!(denied, Err(PlatformStoreError::DeviceTokenDenied)));
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_suspended_tenant_rejects_previously_authenticated_device_and_gateway_sessions() {
    let (_lock, store) = timescale_store().await;
    seed_tenant(&store).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway, gateway_device_id)
         VALUES ('direct', $1, FALSE, NULL), ('gateway', $1, TRUE, NULL),
                ('child', $1, FALSE, 'gateway')",
    )
    .bind(tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let direct = generate_device_token();
    let gateway = generate_device_token();
    token_timescale(&store, "direct", &direct, false).await;
    token_timescale(&store, "gateway", &gateway, false).await;

    let authenticated_direct = store.resolve_active_device_token(&direct).await.unwrap();
    let authenticated_gateway = store.resolve_active_device_token(&gateway).await.unwrap();
    assert!(
        store
            .authorize_device_session(authenticated_direct.token_id, tenant_id(), "direct")
            .await
            .is_ok()
    );
    assert!(
        store
            .authorize_gateway_token(
                authenticated_gateway.token_id,
                tenant_id(),
                "gateway",
                Some("child"),
            )
            .await
            .is_ok()
    );

    TenantIdentityRepository::suspend_tenant(&store, "device-authorization")
        .await
        .unwrap();

    assert!(matches!(
        store
            .authorize_device_session(authenticated_direct.token_id, tenant_id(), "direct")
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
    assert!(matches!(
        store
            .authorize_gateway_token(
                authenticated_gateway.token_id,
                tenant_id(),
                "gateway",
                Some("child"),
            )
            .await,
        Err(PlatformStoreError::DeviceTokenDenied)
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_suspended_tenant_registration_returns_unknown_tenant_without_creating_device() {
    let (_lock, store) = timescale_store().await;
    seed_tenant(&store).await;
    TenantIdentityRepository::suspend_tenant(&store, "device-authorization")
        .await
        .unwrap();

    assert!(matches!(
        store.register_device(tenant_id(), "suspended-device").await,
        Err(PlatformStoreError::UnknownTenant(id)) if id == tenant_id()
    ));
    let device_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_id = 'suspended-device'")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(device_count, 0);
}

async fn token_timescale(
    store: &PlatformStore,
    device_id: &str,
    value: &str,
    revoked: bool,
) -> Uuid {
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash, revoked_at)
         VALUES ($1, $2, $3, $4, CASE WHEN $5 THEN now() ELSE NULL END)",
    )
    .bind(token_id)
    .bind(device_id)
    .bind(device_token_prefix(value).unwrap())
    .bind(hash_device_token(value).unwrap())
    .bind(revoked)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    token_id
}
