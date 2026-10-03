use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AuditPrincipal, AuthorizationRepository, CreateManagementAsset, DeviceTokenRepository,
    DeviceTokenRepositoryError, ManagementAssetError, ManagementAssetRepository,
    ManagementDeviceError, ManagementDeviceRepository, NewDeviceToken, NewOwnedDeviceToken,
    OwnershipTransferTarget, PermissionCreator, PlatformStore, ResourceAccessSource,
    ResourceInvitationRepository, ResourceInvitationState, ResourcePermission, TenantActor,
    TenantAuthorizationError, TenantAuthorizationRepository, UpdateManagementAsset,
    UpdateManagementDevice,
};
use serde_json::json;
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore, Uuid, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("resource-ownership.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = Uuid::now_v7();
    let tenant_account_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'resource-ownership', 'active')",
    )
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, username, password_hash, status, credential_version
         ) VALUES (?, ?, 'resource-ownership-tenant', 'unused', 'active', 1)",
    )
    .bind(tenant_account_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    (directory, store, tenant_id, tenant_account_id)
}

async fn seed_regular_user(store: &PlatformStore, tenant_id: Uuid, username: &str) -> Uuid {
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .bind(username)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    user_id
}

#[tokio::test]
async fn transferring_device_owner_revokes_active_resource_grants() {
    let (_directory, store, tenant_id, tenant_account_id) = sqlite_store().await;
    let former_owner = seed_regular_user(&store, tenant_id, "former-owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    let next_owner = seed_regular_user(&store, tenant_id, "next-owner").await;
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('owner-transfer-device', ?, 'Owner transfer device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(former_owner.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, created_by_user_id
         ) VALUES (?, ?, ?, 'owner-transfer-device', 'manager', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(tenant_id.to_string())
    .bind(recipient.to_string())
    .bind(former_owner.to_string())
    .execute(pool)
    .await
    .unwrap();

    let transfer = TenantAuthorizationRepository::transfer_resource_ownership(
        &store,
        tenant_id,
        AuditPrincipal::TenantAccount(tenant_account_id),
        OwnershipTransferTarget::Device("owner-transfer-device".to_owned()),
        Some(next_owner),
    )
    .await
    .unwrap();

    assert!(transfer);
    assert_eq!(
        TenantAuthorizationRepository::list_active_resource_permissions(&store, tenant_id)
            .await
            .unwrap()
            .iter()
            .filter(|permission| {
                permission.device_id.as_deref() == Some("owner-transfer-device")
                    && permission.permission == ResourcePermission::Manager
            })
            .count(),
        0
    );
}

#[tokio::test]
async fn only_the_current_owner_can_create_or_revoke_a_direct_device_share() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    let other_user = seed_regular_user(&store, tenant_id, "other-user").await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('owner-shared-device', ?, 'Owner shared device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(pool)
    .await
    .unwrap();

    let share = TenantAuthorizationRepository::create_owner_resource_permission(
        &store,
        tenant_id,
        owner,
        recipient,
        OwnershipTransferTarget::Device("owner-shared-device".to_owned()),
        ResourcePermission::Manager,
    )
    .await
    .unwrap();

    assert_eq!(share.subject_user_id, Some(recipient));
    assert_eq!(share.permission, ResourcePermission::Manager);
    assert!(!share.inherit_children);
    assert!(matches!(
        TenantAuthorizationRepository::create_owner_resource_permission(
            &store,
            tenant_id,
            other_user,
            recipient,
            OwnershipTransferTarget::Device("owner-shared-device".to_owned()),
            ResourcePermission::Viewer,
        )
        .await,
        Err(TenantAuthorizationError::ResourceOwnerRequired { .. })
    ));
    assert!(matches!(
        TenantAuthorizationRepository::revoke_owner_resource_permission(
            &store,
            tenant_id,
            other_user,
            OwnershipTransferTarget::Device("owner-shared-device".to_owned()),
            share.id,
        )
        .await,
        Err(TenantAuthorizationError::ResourceOwnerRequired { .. })
    ));
    assert!(
        TenantAuthorizationRepository::revoke_owner_resource_permission(
            &store,
            tenant_id,
            owner,
            OwnershipTransferTarget::Device("owner-shared-device".to_owned()),
            share.id,
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn regular_user_cannot_transfer_resource_ownership() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let next_owner = seed_regular_user(&store, tenant_id, "next-owner").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('ownership-guard-device', ?, 'Ownership guard device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let transfer = TenantAuthorizationRepository::transfer_resource_ownership(
        &store,
        tenant_id,
        AuditPrincipal::User(owner),
        OwnershipTransferTarget::Device("ownership-guard-device".to_owned()),
        Some(next_owner),
    )
    .await;

    assert!(matches!(
        transfer,
        Err(TenantAuthorizationError::TenantAccountRequiredForOwnership)
    ));
}

#[tokio::test]
async fn pending_owner_invitation_does_not_authorize_the_recipient() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('invited-device', ?, 'Invited device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let invitation = ResourceInvitationRepository::create_owner_resource_invitation(
        &store,
        tenant_id,
        TenantActor::TenantUser(owner),
        recipient,
        OwnershipTransferTarget::Device("invited-device".to_owned()),
        ResourcePermission::Viewer,
    )
    .await
    .unwrap();

    assert_eq!(invitation.state, ResourceInvitationState::Pending);
    let subject = AuthorizationRepository::authorization_subject(&store, recipient)
        .await
        .unwrap()
        .unwrap();
    assert!(
        AuthorizationRepository::authorized_device(&store, &subject, "invited-device")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn accepting_owner_invitation_creates_one_direct_permission() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('accepted-device', ?, 'Accepted device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let invitation = ResourceInvitationRepository::create_owner_resource_invitation(
        &store,
        tenant_id,
        TenantActor::TenantUser(owner),
        recipient,
        OwnershipTransferTarget::Device("accepted-device".to_owned()),
        ResourcePermission::Manager,
    )
    .await
    .unwrap();
    let accepted = ResourceInvitationRepository::accept_resource_invitation(
        &store,
        tenant_id,
        recipient,
        invitation.id,
    )
    .await
    .unwrap();

    assert_eq!(accepted.state, ResourceInvitationState::Accepted);
    let active_permissions =
        TenantAuthorizationRepository::list_active_resource_permissions(&store, tenant_id)
            .await
            .unwrap();
    assert_eq!(
        active_permissions
            .iter()
            .filter(|permission| permission.device_id.as_deref() == Some("accepted-device"))
            .count(),
        1
    );
    let subject = AuthorizationRepository::authorization_subject(&store, recipient)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        AuthorizationRepository::authorized_device(&store, &subject, "accepted-device")
            .await
            .unwrap()
            .unwrap()
            .access
            .source,
        ResourceAccessSource::DirectUser
    );
}

#[tokio::test]
async fn accepting_a_higher_access_invitation_updates_an_existing_direct_grant() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('upgraded-device', ?, 'Upgraded device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    TenantAuthorizationRepository::create_owner_resource_permission(
        &store,
        tenant_id,
        owner,
        recipient,
        OwnershipTransferTarget::Device("upgraded-device".to_owned()),
        ResourcePermission::Viewer,
    )
    .await
    .unwrap();
    let invitation = ResourceInvitationRepository::create_owner_resource_invitation(
        &store,
        tenant_id,
        TenantActor::TenantUser(owner),
        recipient,
        OwnershipTransferTarget::Device("upgraded-device".to_owned()),
        ResourcePermission::Manager,
    )
    .await
    .unwrap();

    ResourceInvitationRepository::accept_resource_invitation(
        &store,
        tenant_id,
        recipient,
        invitation.id,
    )
    .await
    .unwrap();

    let grants = TenantAuthorizationRepository::list_active_resource_permissions(&store, tenant_id)
        .await
        .unwrap();
    let matching = grants
        .into_iter()
        .filter(|grant| grant.device_id.as_deref() == Some("upgraded-device"))
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].permission, ResourcePermission::Manager);
}

#[tokio::test]
async fn tenant_account_invitation_sender_is_preserved_without_a_tenant_user() {
    let (_directory, store, tenant_id, tenant_account_id) = sqlite_store().await;
    let recipient = seed_regular_user(&store, tenant_id, "tenant-account-recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('tenant-account-invited-device', ?, 'Tenant account invited device')",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let invitation = ResourceInvitationRepository::create_owner_resource_invitation(
        &store,
        tenant_id,
        TenantActor::TenantAccount(tenant_account_id),
        recipient,
        OwnershipTransferTarget::Device("tenant-account-invited-device".to_owned()),
        ResourcePermission::Manager,
    )
    .await
    .unwrap();
    assert_eq!(
        invitation.sender,
        TenantActor::TenantAccount(tenant_account_id)
    );

    ResourceInvitationRepository::accept_resource_invitation(
        &store,
        tenant_id,
        recipient,
        invitation.id,
    )
    .await
    .unwrap();
    let permission =
        TenantAuthorizationRepository::list_active_resource_permissions(&store, tenant_id)
            .await
            .unwrap()
            .into_iter()
            .find(|permission| {
                permission.device_id.as_deref() == Some("tenant-account-invited-device")
            })
            .unwrap();
    assert_eq!(
        permission.created_by,
        PermissionCreator::TenantAccount(tenant_account_id)
    );
}

#[tokio::test]
async fn regular_user_created_asset_is_owned_by_the_creator() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;

    let asset = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::User(owner),
        CreateManagementAsset {
            name: "Owner farm".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(asset.owner_user_id, Some(owner));
}

#[tokio::test]
async fn shared_recipient_cannot_update_an_owner_asset() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    let asset = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::User(owner),
        CreateManagementAsset {
            name: "Owner farm".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();

    let result = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::User(recipient),
        asset.id,
        UpdateManagementAsset {
            name: "Recipient change".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(ManagementAssetError::ParentAssetNotOwned)
    ));
}

#[tokio::test]
async fn user_cannot_provision_a_device_into_another_users_asset() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    let asset = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        AuditPrincipal::User(owner),
        CreateManagementAsset {
            name: "Owner farm".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();

    let result = DeviceTokenRepository::provision_owned_device_token(
        &store,
        tenant_id,
        AuditPrincipal::User(recipient),
        NewOwnedDeviceToken {
            display_name: "Recipient device".to_owned(),
            owner_user_id: recipient,
            asset_id: Some(asset.id),
            token: NewDeviceToken {
                id: Uuid::now_v7(),
                token_prefix: "recipient-device-token".to_owned(),
                token_hash: "recipient-device-token-hash".to_owned(),
                token_ciphertext: "recipient-device-token-ciphertext".to_owned(),
            },
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(DeviceTokenRepositoryError::DeviceNotFound)
    ));
}

#[tokio::test]
async fn shared_recipient_cannot_update_an_owner_device() {
    let (_directory, store, tenant_id, _tenant_account_id) = sqlite_store().await;
    let owner = seed_regular_user(&store, tenant_id, "owner").await;
    let recipient = seed_regular_user(&store, tenant_id, "recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id)
         VALUES ('owner-device', ?, 'Owner device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(owner.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let result = ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        AuditPrincipal::User(recipient),
        "owner-device",
        UpdateManagementDevice {
            display_name: "Recipient change".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: None,
            topology: None,
        },
    )
    .await;

    assert!(matches!(result, Err(ManagementDeviceError::DeviceNotFound)));
}
