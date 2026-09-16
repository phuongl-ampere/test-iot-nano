use iot_api::{
    AuthError, PrincipalKind, authenticate_system_account, authenticate_tenant_account,
    authenticate_user_account, hash_password,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewSystemAccount, NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository,
};

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("tenant-auth.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

#[tokio::test]
async fn tenant_authenticates_system_and_tenant_account_credentials() {
    let (_directory, store) = sqlite_store().await;
    let system = TenantIdentityRepository::bootstrap_system_account(
        &store,
        NewSystemAccount {
            username: "system".to_owned(),
            password_hash: hash_password("SystemAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let (tenant, tenant_account) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "north".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("TenantAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();

    let system_principal = authenticate_system_account(&store, "system", "SystemAccount@2026")
        .await
        .unwrap();
    assert_eq!(system_principal.kind, PrincipalKind::System);
    assert_eq!(system_principal.principal_id, system.id);
    assert_eq!(system_principal.tenant_id, None);

    let tenant_principal = authenticate_tenant_account(&store, "north", "TenantAccount@2026")
        .await
        .unwrap();
    assert_eq!(tenant_principal.kind, PrincipalKind::Tenant);
    assert_eq!(tenant_principal.principal_id, tenant_account.id);
    assert_eq!(tenant_principal.tenant_id, Some(tenant.id));
}

#[tokio::test]
async fn tenant_auth_rejects_invalid_system_or_tenant_credentials() {
    let (_directory, store) = sqlite_store().await;
    TenantIdentityRepository::bootstrap_system_account(
        &store,
        NewSystemAccount {
            username: "system".to_owned(),
            password_hash: hash_password("SystemAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        authenticate_system_account(&store, "system", "wrong-password").await,
        Err(AuthError::AuthenticationFailed)
    ));
    assert!(matches!(
        authenticate_tenant_account(&store, "missing", "TenantAccount@2026").await,
        Err(AuthError::AuthenticationFailed)
    ));
}

#[tokio::test]
async fn tenant_authenticates_user_only_with_the_matching_tenant_slug() {
    let (_directory, store) = sqlite_store().await;
    let (north, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "north".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("TenantAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let (south, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "south".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("TenantAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let user_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (
            id, tenant_id, username, password_hash, role, account_class, default_app
         ) VALUES (?, ?, 'north-user', ?, 'viewer', 'user', '/app')",
    )
    .bind(user_id.to_string())
    .bind(north.id.to_string())
    .bind(hash_password("NorthUser@2026").unwrap())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let principal = authenticate_user_account(&store, "north", "north-user", "NorthUser@2026")
        .await
        .unwrap();
    assert_eq!(principal.kind, PrincipalKind::User);
    assert_eq!(principal.principal_id, user_id);
    assert_eq!(principal.tenant_id, Some(north.id));
    assert!(matches!(
        authenticate_user_account(&store, "south", "north-user", "NorthUser@2026").await,
        Err(AuthError::AuthenticationFailed)
    ));
    assert_ne!(north.id, south.id);
}
