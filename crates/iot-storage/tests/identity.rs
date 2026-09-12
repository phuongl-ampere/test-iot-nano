use chrono::{DateTime, Utc};
use iot_core::{
    DatabaseStorage, StorageConfiguration, device_token_prefix, generate_device_token,
    hash_device_token,
};
use iot_storage::{IdentityRepository, PlatformStore, PlatformStoreError};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

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

async fn insert_device_token(
    store: &PlatformStore,
    device_id: &str,
    token: &str,
    revoked: bool,
) -> Uuid {
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash, revoked_at)
         VALUES (?, ?, ?, ?, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END)",
    )
    .bind(token_id.to_string())
    .bind(device_id)
    .bind(device_token_prefix(token).unwrap())
    .bind(hash_device_token(token).unwrap())
    .bind(revoked)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    token_id
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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:platform-storage-test'))")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut connection)
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
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

async fn insert_timescale_device_token(
    store: &PlatformStore,
    device_id: &str,
    token: &str,
    revoked: bool,
) -> Uuid {
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash, revoked_at)
         VALUES ($1, $2, $3, $4, CASE WHEN $5 THEN now() ELSE NULL END)",
    )
    .bind(token_id)
    .bind(device_id)
    .bind(device_token_prefix(token).unwrap())
    .bind(hash_device_token(token).unwrap())
    .bind(revoked)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    token_id
}

fn assert_denied(result: Result<impl Sized, PlatformStoreError>) {
    assert!(matches!(result, Err(PlatformStoreError::DeviceTokenDenied)));
}

#[tokio::test]
async fn sqlite_identity_repository_authenticates_active_tokens_and_denies_invalid_tokens() {
    let (_directory, store) = sqlite_store().await;
    sqlx::query(
        "INSERT INTO devices (device_id, is_gateway, gateway_device_id)
         VALUES
            ('direct-device', 0, NULL),
            ('gateway-device', 1, NULL),
            ('gateway-child', 0, 'gateway-device'),
            ('deleted-device', 0, NULL),
            ('mismatch-device', 0, NULL)",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices SET deleted_at = CURRENT_TIMESTAMP WHERE device_id = 'deleted-device'",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let direct_token = generate_device_token();
    let gateway_token = generate_device_token();
    let child_token = generate_device_token();
    let revoked_token = generate_device_token();
    let deleted_token = generate_device_token();
    let direct_token_id = insert_device_token(&store, "direct-device", &direct_token, false).await;
    let gateway_token_id =
        insert_device_token(&store, "gateway-device", &gateway_token, false).await;
    insert_device_token(&store, "gateway-child", &child_token, false).await;
    insert_device_token(&store, "direct-device", &revoked_token, true).await;
    insert_device_token(&store, "deleted-device", &deleted_token, false).await;

    let authentication_started_at = Utc::now();
    let direct = store
        .resolve_active_device_token(&direct_token)
        .await
        .unwrap();
    assert_eq!(direct.token_id, direct_token_id);
    assert_eq!(direct.device_id, "direct-device");
    assert!(!direct.is_gateway);
    assert_eq!(direct.gateway_device_id, None);

    let gateway = IdentityRepository::resolve_active_device_token(&store, &gateway_token)
        .await
        .unwrap();
    assert_eq!(gateway.token_id, gateway_token_id);
    assert_eq!(gateway.device_id, "gateway-device");
    assert!(gateway.is_gateway);
    assert_eq!(gateway.gateway_device_id, None);

    let child = store
        .resolve_active_device_token(&child_token)
        .await
        .unwrap();
    assert_eq!(child.gateway_device_id.as_deref(), Some("gateway-device"));

    let used_at: String = sqlx::query("SELECT last_used_at FROM device_tokens WHERE id = ?")
        .bind(direct_token_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .try_get("last_used_at")
        .unwrap();
    let used_at = DateTime::parse_from_rfc3339(&used_at)
        .unwrap()
        .with_timezone(&Utc);
    assert!(used_at >= authentication_started_at);

    let denied_cases = [
        "not-a-device-token".to_owned(),
        generate_device_token(),
        revoked_token,
        deleted_token,
    ];
    for token in denied_cases {
        assert_denied(store.resolve_active_device_token(&token).await);
    }

    let mut mismatched_token = generate_device_token();
    let mismatched_hash = hash_device_token(&mismatched_token).unwrap();
    mismatched_token.replace_range(
        5..6,
        if &mismatched_token[5..6] == "0" {
            "1"
        } else {
            "0"
        },
    );
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'mismatch-device', ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(device_token_prefix(&mismatched_token).unwrap())
    .bind(mismatched_hash)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_denied(store.resolve_active_device_token(&mismatched_token).await);

    let denied_used_at: Option<String> =
        sqlx::query("SELECT last_used_at FROM device_tokens WHERE token_prefix = ?")
            .bind(device_token_prefix(&mismatched_token).unwrap())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap()
            .try_get("last_used_at")
            .unwrap();
    assert_eq!(denied_used_at, None);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_identity_repository_matches_sqlite_identity_contract() {
    let (_test_lock, store) = timescale_store().await;
    sqlx::query(
        "INSERT INTO devices (device_id, is_gateway, gateway_device_id)
         VALUES
            ('direct-device', FALSE, NULL),
            ('gateway-device', TRUE, NULL),
            ('gateway-child', FALSE, 'gateway-device'),
            ('deleted-device', FALSE, NULL),
            ('mismatch-device', FALSE, NULL)",
    )
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = 'deleted-device'")
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();

    let direct_token = generate_device_token();
    let gateway_token = generate_device_token();
    let child_token = generate_device_token();
    let revoked_token = generate_device_token();
    let deleted_token = generate_device_token();
    let direct_token_id =
        insert_timescale_device_token(&store, "direct-device", &direct_token, false).await;
    let gateway_token_id =
        insert_timescale_device_token(&store, "gateway-device", &gateway_token, false).await;
    insert_timescale_device_token(&store, "gateway-child", &child_token, false).await;
    insert_timescale_device_token(&store, "direct-device", &revoked_token, true).await;
    insert_timescale_device_token(&store, "deleted-device", &deleted_token, false).await;

    let authentication_started_at = Utc::now();
    let direct = store
        .resolve_active_device_token(&direct_token)
        .await
        .unwrap();
    assert_eq!(direct.token_id, direct_token_id);
    assert_eq!(direct.device_id, "direct-device");
    assert!(!direct.is_gateway);
    assert_eq!(direct.gateway_device_id, None);

    let gateway = IdentityRepository::resolve_active_device_token(&store, &gateway_token)
        .await
        .unwrap();
    assert_eq!(gateway.token_id, gateway_token_id);
    assert_eq!(gateway.device_id, "gateway-device");
    assert!(gateway.is_gateway);
    assert_eq!(gateway.gateway_device_id, None);

    let child = store
        .resolve_active_device_token(&child_token)
        .await
        .unwrap();
    assert_eq!(child.gateway_device_id.as_deref(), Some("gateway-device"));

    let used_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT last_used_at FROM device_tokens WHERE id = $1")
            .bind(direct_token_id)
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert!(used_at >= authentication_started_at);

    for token in [
        "not-a-device-token".to_owned(),
        generate_device_token(),
        revoked_token,
        deleted_token,
    ] {
        assert_denied(store.resolve_active_device_token(&token).await);
    }

    let mut mismatched_token = generate_device_token();
    let mismatched_hash = hash_device_token(&mismatched_token).unwrap();
    mismatched_token.replace_range(
        5..6,
        if &mismatched_token[5..6] == "0" {
            "1"
        } else {
            "0"
        },
    );
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ($1, 'mismatch-device', $2, $3)",
    )
    .bind(Uuid::now_v7())
    .bind(device_token_prefix(&mismatched_token).unwrap())
    .bind(mismatched_hash)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    assert_denied(store.resolve_active_device_token(&mismatched_token).await);

    let denied_used_at: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT last_used_at FROM device_tokens WHERE token_prefix = $1")
            .bind(device_token_prefix(&mismatched_token).unwrap())
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(denied_used_at, None);
}
