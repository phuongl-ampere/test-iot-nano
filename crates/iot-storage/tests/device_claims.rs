use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateManagementUser, DeviceClaimError, DeviceClaimPolicy, DeviceClaimRepository,
    ManagementUserRepository, PlatformStore,
};
use sqlx::Row;
use uuid::Uuid;

const TENANT_ID: &str = "00000000-0000-0000-0000-0000000000c1";

fn tenant_id() -> Uuid {
    Uuid::parse_str(TENANT_ID).unwrap()
}

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("device-claims.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, 'claims', 'active', '{}')",
    )
    .bind(TENANT_ID)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store)
}

async fn create_user(store: &PlatformStore, username: &str) -> Uuid {
    ManagementUserRepository::create_management_user(
        store,
        CreateManagementUser {
            tenant_id: tenant_id(),
            username: username.to_owned(),
            password_hash: "stored-password-hash".to_owned(),
        },
    )
    .await
    .unwrap()
    .id
}

async fn create_unassigned_device(store: &PlatformStore, device_id: &str) {
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata)
         VALUES (?, ?, 'Claimable', '{}')",
    )
    .bind(device_id)
    .bind(TENANT_ID)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

fn enabled_policy() -> DeviceClaimPolicy {
    DeviceClaimPolicy {
        enabled: true,
        ttl_seconds: 900,
        code_length: 12,
        max_failed_attempts: 5,
        request_cooldown_seconds: 30,
    }
}

async fn enable_claims(store: &PlatformStore, tenant_id: Uuid) {
    DeviceClaimRepository::update_device_claim_policy(store, tenant_id, enabled_policy())
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_claim_code_is_hashed_and_claims_an_unassigned_device_once() {
    let (_directory, store) = sqlite_store().await;
    let user_id = create_user(&store, "claim-user").await;
    create_unassigned_device(&store, "claimable-device").await;

    assert_eq!(
        DeviceClaimRepository::get_device_claim_policy(&store, tenant_id())
            .await
            .unwrap(),
        DeviceClaimPolicy::default()
    );
    enable_claims(&store, tenant_id()).await;

    let issued =
        DeviceClaimRepository::issue_device_claim_code(&store, tenant_id(), "claimable-device")
            .await
            .unwrap();
    assert_eq!(issued.code.len(), 14);
    let stored = sqlx::query(
        "SELECT code_hash, consumed_at FROM device_claim_codes
         WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(TENANT_ID)
    .bind("claimable-device")
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_ne!(stored.get::<String, _>("code_hash"), issued.code);
    assert!(stored.get::<Option<String>, _>("consumed_at").is_none());

    let claimed = DeviceClaimRepository::claim_device_with_code(
        &store,
        tenant_id(),
        user_id,
        "claimable-device",
        &issued.code,
    )
    .await
    .unwrap();
    assert_eq!(claimed.device_id, "claimable-device");
    assert_eq!(claimed.owner_user_id, user_id);
    assert!(claimed.claimed_at <= chrono::Utc::now());

    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner_user_id FROM devices WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(TENANT_ID)
    .bind("claimable-device")
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(owner.as_deref(), Some(user_id.to_string().as_str()));
    let error = DeviceClaimRepository::claim_device_with_code(
        &store,
        tenant_id(),
        user_id,
        "claimable-device",
        &issued.code,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, DeviceClaimError::DeviceUnavailable));
}

#[tokio::test]
async fn sqlite_claim_policy_rejects_invalid_values_and_disabled_issuance() {
    let (_directory, store) = sqlite_store().await;
    create_unassigned_device(&store, "policy-device").await;

    let invalid = DeviceClaimPolicy {
        ttl_seconds: 59,
        ..DeviceClaimPolicy::default()
    };
    let error = DeviceClaimRepository::update_device_claim_policy(&store, tenant_id(), invalid)
        .await
        .unwrap_err();
    assert!(matches!(error, DeviceClaimError::InvalidPolicy));

    let error =
        DeviceClaimRepository::issue_device_claim_code(&store, tenant_id(), "policy-device")
            .await
            .unwrap_err();
    assert!(matches!(error, DeviceClaimError::PolicyDisabled));
}

#[tokio::test]
async fn sqlite_claim_code_cooldown_and_attempt_limit_do_not_transfer_ownership() {
    let (_directory, store) = sqlite_store().await;
    let user_id = create_user(&store, "lockout-user").await;
    create_unassigned_device(&store, "lockout-device").await;
    DeviceClaimRepository::update_device_claim_policy(
        &store,
        tenant_id(),
        DeviceClaimPolicy {
            max_failed_attempts: 1,
            request_cooldown_seconds: 10,
            ..enabled_policy()
        },
    )
    .await
    .unwrap();

    let issued =
        DeviceClaimRepository::issue_device_claim_code(&store, tenant_id(), "lockout-device")
            .await
            .unwrap();
    let cooldown =
        DeviceClaimRepository::issue_device_claim_code(&store, tenant_id(), "lockout-device")
            .await
            .unwrap_err();
    assert!(matches!(cooldown, DeviceClaimError::RequestCoolingDown));

    let rejected = DeviceClaimRepository::claim_device_with_code(
        &store,
        tenant_id(),
        user_id,
        "lockout-device",
        "WRONG-CODE",
    )
    .await
    .unwrap_err();
    assert!(matches!(rejected, DeviceClaimError::CodeUnavailable));
    assert!(
        DeviceClaimRepository::active_device_claim_code(&store, tenant_id(), "lockout-device")
            .await
            .unwrap()
            .is_none()
    );

    let consumed_after_lockout = DeviceClaimRepository::claim_device_with_code(
        &store,
        tenant_id(),
        user_id,
        "lockout-device",
        &issued.code,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        consumed_after_lockout,
        DeviceClaimError::CodeUnavailable
    ));
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner_user_id FROM devices WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(TENANT_ID)
    .bind("lockout-device")
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert!(owner.is_none());
}

#[tokio::test]
async fn sqlite_claim_code_is_expired_and_tenant_scoped() {
    let (_directory, store) = sqlite_store().await;
    let user_id = create_user(&store, "tenant-a-user").await;
    create_unassigned_device(&store, "tenant-scope-device").await;
    enable_claims(&store, tenant_id()).await;
    let issued =
        DeviceClaimRepository::issue_device_claim_code(&store, tenant_id(), "tenant-scope-device")
            .await
            .unwrap();

    let other_tenant_id = Uuid::now_v7();
    let other_user_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, 'claims-other', 'active', '{}')")
        .bind(other_tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'claims-other-user', 'unused', 'viewer', 'user')",
    )
    .bind(other_user_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    enable_claims(&store, other_tenant_id).await;

    let cross_tenant = DeviceClaimRepository::claim_device_with_code(
        &store,
        other_tenant_id,
        other_user_id,
        "tenant-scope-device",
        &issued.code,
    )
    .await
    .unwrap_err();
    assert!(matches!(cross_tenant, DeviceClaimError::DeviceUnavailable));

    sqlx::query(
        "UPDATE device_claim_codes
         SET issued_at = '2000-01-01T00:00:00.000000000Z',
             expires_at = '2000-01-01T00:00:01.000000000Z'
         WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(TENANT_ID)
    .bind("tenant-scope-device")
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let expired = DeviceClaimRepository::claim_device_with_code(
        &store,
        tenant_id(),
        user_id,
        "tenant-scope-device",
        &issued.code,
    )
    .await
    .unwrap_err();
    assert!(matches!(expired, DeviceClaimError::CodeUnavailable));
    assert!(DeviceClaimRepository::active_device_claim_code(
        &store,
        tenant_id(),
        "tenant-scope-device",
    )
    .await
    .unwrap()
    .is_none());
}
