use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    DeviceTokenRepository, DeviceTokenRepositoryError, NewDeviceToken, PlatformStore,
};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:device-token-repository-test'))")
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

fn token(prefix: &str) -> NewDeviceToken {
    NewDeviceToken {
        id: Uuid::now_v7(),
        token_prefix: prefix.to_owned(),
        token_hash: format!("{prefix}-hash"),
        token_ciphertext: format!("{prefix}-ciphertext"),
    }
}

#[tokio::test]
async fn sqlite_device_token_repository_provisions_rotates_and_rejects_gateway_children() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, is_gateway) VALUES
             ('token-direct', 0),
             ('token-gateway', 1),
             ('token-child', 0)",
    )
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
        "token-direct",
        token("direct-token-one"),
    )
    .await
    .unwrap();
    let replacement = DeviceTokenRepository::create_device_token(
        &store,
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

    let child_error =
        DeviceTokenRepository::create_device_token(&store, "token-child", token("child-token"))
            .await
            .unwrap_err();
    assert!(matches!(
        child_error,
        DeviceTokenRepositoryError::GatewayChild
    ));

    let missing_error = DeviceTokenRepository::create_device_token(
        &store,
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
async fn sqlite_device_token_repository_reports_prefix_conflicts_without_revoking_active_tokens() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('token-conflict')")
        .execute(pool)
        .await
        .unwrap();

    let active = DeviceTokenRepository::create_device_token(
        &store,
        "token-conflict",
        token("conflict-active"),
    )
    .await
    .unwrap();
    let conflict = DeviceTokenRepository::create_device_token(
        &store,
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
    sqlx::query("INSERT INTO devices (device_id) VALUES ('token-history')")
        .execute(pool)
        .await
        .unwrap();

    let first = DeviceTokenRepository::create_device_token(
        &store,
        "token-history",
        token("history-token-one"),
    )
    .await
    .unwrap();
    let initial_history = DeviceTokenRepository::list_device_tokens(&store, "token-history")
        .await
        .unwrap();
    assert_eq!(initial_history.len(), 1);
    assert_eq!(initial_history[0].id, first.id);
    assert_eq!(initial_history[0].token_prefix, "history-token-one");
    assert!(initial_history[0].revoked_at.is_none());
    assert_eq!(
        DeviceTokenRepository::active_device_token(&store, first.id)
            .await
            .unwrap()
            .unwrap()
            .device_id,
        "token-history"
    );

    let rotated =
        DeviceTokenRepository::rotate_device_token(&store, first.id, token("history-token-two"))
            .await
            .unwrap();
    assert_eq!(rotated.token_prefix, "history-token-two");
    assert!(
        DeviceTokenRepository::active_device_token(&store, first.id)
            .await
            .unwrap()
            .is_none()
    );

    let history = DeviceTokenRepository::list_device_tokens(&store, "token-history")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, rotated.id);
    assert!(history[0].revoked_at.is_none());
    assert_eq!(history[1].id, first.id);
    assert!(history[1].revoked_at.is_some());

    DeviceTokenRepository::revoke_device_token(&store, rotated.id)
        .await
        .unwrap();
    assert!(
        DeviceTokenRepository::active_device_token(&store, rotated.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_token_repository_matches_sqlite_lifecycle_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('token-history')")
        .execute(pool)
        .await
        .unwrap();

    let first = DeviceTokenRepository::create_device_token(
        &store,
        "token-history",
        token("timescale-history-token-one"),
    )
    .await
    .unwrap();
    let rotated = DeviceTokenRepository::rotate_device_token(
        &store,
        first.id,
        token("timescale-history-token-two"),
    )
    .await
    .unwrap();
    let history = DeviceTokenRepository::list_device_tokens(&store, "token-history")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, rotated.id);
    assert!(history[0].revoked_at.is_none());
    assert_eq!(history[1].id, first.id);
    assert!(history[1].revoked_at.is_some());

    DeviceTokenRepository::revoke_device_token(&store, rotated.id)
        .await
        .unwrap();
    assert!(
        DeviceTokenRepository::active_device_token(&store, rotated.id)
            .await
            .unwrap()
            .is_none()
    );
}
