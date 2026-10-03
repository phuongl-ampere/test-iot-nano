use chrono::{Duration, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewTenant, NewTenantAccount, NewTenantPersonalAccessToken, PlatformStore,
    TenantIdentityRepository, TenantPersonalAccessTokenRepository,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("personal-access-tokens.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn tenant_account(store: &PlatformStore, slug: &str) -> (Uuid, Uuid) {
    let (tenant, account) = TenantIdentityRepository::create_tenant_with_account(
        store,
        NewTenant {
            slug: slug.to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: format!("{slug}-password-hash"),
        },
    )
    .await
    .unwrap();
    (tenant.id, account.id)
}

fn token_request(name: &str, secret: &str) -> NewTenantPersonalAccessToken {
    NewTenantPersonalAccessToken {
        id: Uuid::now_v7(),
        name: name.to_owned(),
        token_prefix: secret[..14].to_owned(),
        token_hash: format!("{:x}", Sha256::digest(secret.as_bytes())),
    }
}

#[tokio::test]
async fn sqlite_tenant_personal_access_tokens_are_hash_only_rotated_and_tenant_scoped() {
    let (_directory, store) = sqlite_store().await;
    let (tenant_a_id, tenant_a_account_id) = tenant_account(&store, "north").await;
    let (tenant_b_id, _tenant_b_account_id) = tenant_account(&store, "south").await;
    let first_secret = "iotpat_first-secret-value";
    let second_secret = "iotpat_second-secret-value";
    let now = Utc::now();

    let first = store
        .rotate_tenant_personal_access_token(
            tenant_a_id,
            tenant_a_account_id,
            token_request("CI", first_secret),
            now,
        )
        .await
        .unwrap();
    assert_eq!(first.name, "CI");
    assert_eq!(first.token_prefix, "iotpat_first-s");

    let stored_hash: String =
        sqlx::query_scalar("SELECT token_hash FROM tenant_personal_access_tokens WHERE id = ?")
            .bind(first.id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_ne!(stored_hash, first_secret);
    assert_eq!(
        stored_hash,
        format!("{:x}", Sha256::digest(first_secret.as_bytes()))
    );

    let second = store
        .rotate_tenant_personal_access_token(
            tenant_a_id,
            tenant_a_account_id,
            token_request("Deploy", second_secret),
            now + Duration::seconds(1),
        )
        .await
        .unwrap();
    assert_eq!(second.name, "Deploy");
    assert!(
        store
            .resolve_tenant_personal_access_token(
                &format!("{:x}", Sha256::digest(first_secret.as_bytes())),
                now + Duration::seconds(2),
            )
            .await
            .unwrap()
            .is_none()
    );

    let resolved = store
        .resolve_tenant_personal_access_token(
            &format!("{:x}", Sha256::digest(second_secret.as_bytes())),
            now + Duration::seconds(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.tenant_id, tenant_a_id);
    assert_ne!(resolved.tenant_id, tenant_b_id);
    assert_eq!(resolved.tenant_account_user_id, tenant_a_account_id);
    assert_eq!(resolved.last_used_at, Some(now + Duration::seconds(2)));

    let active = store
        .active_tenant_personal_access_token(tenant_a_id, tenant_a_account_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.id, second.id);
    let active_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tenant_personal_access_tokens
         WHERE tenant_id = ? AND tenant_account_user_id = ? AND revoked_at IS NULL",
    )
    .bind(tenant_a_id.to_string())
    .bind(tenant_a_account_id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(active_count, 1);
}
