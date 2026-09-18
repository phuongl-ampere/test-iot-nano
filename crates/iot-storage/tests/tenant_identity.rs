use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewSystemAccount, NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository,
    TenantStatus,
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
async fn sqlite_tenant_identity_repository_lists_tenants_without_deleted_rows() {
    let (_directory, store) = sqlite_store().await;
    for slug in ["north", "south"] {
        TenantIdentityRepository::create_tenant_with_account(
            &store,
            NewTenant {
                slug: slug.to_owned(),
                metadata: serde_json::json!({}),
            },
            NewTenantAccount {
                password_hash: "tenant-account-hash".to_owned(),
            },
        )
        .await
        .unwrap();
    }
    TenantIdentityRepository::delete_tenant(&store, "south")
        .await
        .unwrap();

    let tenants = TenantIdentityRepository::list_tenants(&store)
        .await
        .unwrap();
    assert_eq!(
        tenants
            .into_iter()
            .map(|tenant| tenant.slug)
            .collect::<Vec<_>>(),
        ["north"]
    );
}

#[tokio::test]
async fn sqlite_tenant_identity_repository_summarizes_tenants_without_deserializing_metadata() {
    let (_directory, store) = sqlite_store().await;
    TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "north".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: "tenant-account-hash".to_owned(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE tenants SET metadata = ? WHERE slug = ?")
        .bind("sensitive metadata that is not JSON")
        .bind("north")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let summaries = TenantIdentityRepository::list_tenant_summaries(&store)
        .await
        .unwrap();

    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].slug, "north");
    assert_eq!(summaries[0].status, TenantStatus::Active);
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

#[tokio::test]
async fn sqlite_schema_requires_same_tenant_identity_for_assets_and_devices() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')")
        .bind("tenant-north")
        .bind("north")
        .execute(pool)
        .await
        .unwrap();

    let missing_asset_tenant =
        sqlx::query("INSERT INTO assets (id, name, metadata) VALUES (?, ?, '{}')")
            .bind("00000000-0000-0000-0000-000000000010")
            .bind("asset-without-tenant")
            .execute(pool)
            .await;
    assert!(missing_asset_tenant.is_err());

    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, metadata)
         VALUES (?, ?, ?, '{}')",
    )
    .bind("00000000-0000-0000-0000-000000000010")
    .bind("tenant-north")
    .bind("north-asset")
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')")
        .bind("tenant-south")
        .bind("south")
        .execute(pool)
        .await
        .unwrap();
    let missing_device_tenant =
        sqlx::query("INSERT INTO devices (device_id, asset_id) VALUES (?, ?)")
            .bind("device-without-tenant")
            .bind("00000000-0000-0000-0000-000000000010")
            .execute(pool)
            .await;
    assert!(missing_device_tenant.is_err());
    let cross_tenant_assignment = sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, asset_id)
         VALUES (?, ?, ?)",
    )
    .bind("south-device")
    .bind("tenant-south")
    .bind("00000000-0000-0000-0000-000000000010")
    .execute(pool)
    .await;
    assert!(cross_tenant_assignment.is_err());
}

#[tokio::test]
async fn sqlite_schema_scopes_root_asset_name_uniqueness_to_a_tenant() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    for (id, slug) in [("tenant-north", "north"), ("tenant-south", "south")] {
        sqlx::query(
            "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, 'active', '{}')",
        )
        .bind(id)
        .bind(slug)
        .execute(pool)
        .await
        .unwrap();
    }
    for (id, tenant_id) in [
        ("00000000-0000-0000-0000-000000000011", "tenant-north"),
        ("00000000-0000-0000-0000-000000000012", "tenant-south"),
    ] {
        sqlx::query(
            "INSERT INTO assets (id, tenant_id, name, metadata) VALUES (?, ?, 'root', '{}')",
        )
        .bind(id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    }
    let duplicate = sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, metadata) VALUES (?, ?, 'root', '{}')",
    )
    .bind("00000000-0000-0000-0000-000000000013")
    .bind("tenant-north")
    .execute(pool)
    .await;
    assert!(duplicate.is_err());
}
