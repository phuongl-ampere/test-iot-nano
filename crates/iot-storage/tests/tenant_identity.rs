use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewSystemAccount, NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository,
};

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("tenant-identity.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

#[tokio::test]
async fn sqlite_schema_enforces_system_tenant_and_tenant_account_identity_roots() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO system_accounts (id, username, password_hash, status)
         VALUES (?, ?, ?, 'active')",
    )
    .bind("system-1")
    .bind("system")
    .bind("hash")
    .execute(pool)
    .await
    .unwrap();
    let duplicate_system = sqlx::query(
        "INSERT INTO system_accounts (id, username, password_hash, status)
         VALUES (?, ?, ?, 'active')",
    )
    .bind("system-2")
    .bind("system-2")
    .bind("hash")
    .execute(pool)
    .await;
    assert!(duplicate_system.is_err());

    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')")
        .bind("tenant-1")
        .bind("north")
        .execute(pool)
        .await
        .unwrap();
    let duplicate_slug = sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')",
    )
    .bind("tenant-2")
    .bind("north")
    .execute(pool)
    .await;
    assert!(duplicate_slug.is_err());

    sqlx::query(
        "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
         VALUES (?, ?, ?, 'active', 1)",
    )
    .bind("tenant-account-1")
    .bind("tenant-1")
    .bind("hash")
    .execute(pool)
    .await
    .unwrap();
    let duplicate_tenant_account = sqlx::query(
        "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
         VALUES (?, ?, ?, 'active', 1)",
    )
    .bind("tenant-account-2")
    .bind("tenant-1")
    .bind("hash")
    .execute(pool)
    .await;
    assert!(duplicate_tenant_account.is_err());
}

#[tokio::test]
async fn sqlite_tenant_identity_repository_bootstraps_system_and_creates_tenant_atomically() {
    let (_directory, store) = sqlite_store().await;

    let system = TenantIdentityRepository::bootstrap_system_account(
        &store,
        NewSystemAccount {
            username: "system".to_owned(),
            password_hash: "system-hash".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(system.username, "system");

    let (tenant, tenant_account) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "north".to_owned(),
            metadata: serde_json::json!({ "region": "north" }),
        },
        NewTenantAccount {
            password_hash: "tenant-hash".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(tenant.slug, "north");
    assert_eq!(tenant_account.tenant_id, tenant.id);

    let system_credential = TenantIdentityRepository::system_account_credential(&store, "system")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(system_credential.account.id, system.id);
    assert_eq!(system_credential.password_hash, "system-hash");

    let tenant_credential = TenantIdentityRepository::tenant_account_credential(&store, "north")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tenant_credential.account.id, tenant_account.id);
    assert_eq!(tenant_credential.tenant.id, tenant.id);
    assert_eq!(tenant_credential.password_hash, "tenant-hash");
}

#[tokio::test]
async fn sqlite_tenant_identity_repository_controls_tenant_and_credential_lifecycle() {
    let (_directory, store) = sqlite_store().await;
    let (tenant, tenant_account) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "north".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: "original-hash".to_owned(),
        },
    )
    .await
    .unwrap();

    TenantIdentityRepository::suspend_tenant(&store, "north")
        .await
        .unwrap();
    assert!(
        TenantIdentityRepository::tenant_account_credential(&store, "north")
            .await
            .unwrap()
            .is_none()
    );

    TenantIdentityRepository::reactivate_tenant(&store, "north")
        .await
        .unwrap();
    let reset = TenantIdentityRepository::reset_tenant_account_password(
        &store,
        "north",
        "replacement-hash".to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(reset.id, tenant_account.id);
    assert_eq!(reset.tenant_id, tenant.id);
    assert_eq!(reset.credential_version, 2);
    assert_eq!(
        TenantIdentityRepository::tenant_account_credential(&store, "north")
            .await
            .unwrap()
            .unwrap()
            .password_hash,
        "replacement-hash"
    );

    TenantIdentityRepository::delete_tenant(&store, "north")
        .await
        .unwrap();
    assert!(
        TenantIdentityRepository::tenant_account_credential(&store, "north")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_schema_requires_tenant_id_for_every_user() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let missing_tenant = sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class, default_app)
         VALUES (?, ?, ?, 'viewer', 'user', '/app')",
    )
    .bind("user-without-tenant")
    .bind("without-tenant")
    .bind("hash")
    .execute(pool)
    .await;
    assert!(missing_tenant.is_err());

    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')")
        .bind("tenant-1")
        .bind("north")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class, default_app)
         VALUES (?, ?, ?, ?, 'viewer', 'user', '/app')",
    )
    .bind("user-with-tenant")
    .bind("tenant-1")
    .bind("with-tenant")
    .bind("hash")
    .execute(pool)
    .await
    .unwrap();
}
