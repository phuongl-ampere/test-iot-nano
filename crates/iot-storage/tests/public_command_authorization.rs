#[cfg(feature = "test-support")]
use std::sync::Arc;

use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
#[cfg(feature = "test-support")]
use iot_storage::{
    AccountClass, PublicApiRepository, PublicPrincipal, install_public_device_list_handoff_hook,
};
use iot_storage::{NewCommandOutboxEntry, PlatformStore};
#[cfg(feature = "test-support")]
use tokio::{
    sync::{Notify, oneshot},
    time::{Duration as TokioDuration, timeout},
};
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-command-authorization.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'public-command', 'active')")
        .bind(tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    (directory, store, tenant_id)
}

fn command(tenant_id: Uuid, device_id: &str) -> NewCommandOutboxEntry {
    let issued_at = Utc::now();
    NewCommandOutboxEntry {
        id: Uuid::now_v7().to_string(),
        tenant_id,
        device_id: device_id.to_owned(),
        method: "setRelay".to_owned(),
        params: "{\"enabled\":true}".to_owned(),
        mode: RpcMode::OneWay,
        expires_at: issued_at + Duration::minutes(1),
        next_attempt_at: issued_at,
    }
}

#[tokio::test]
async fn sqlite_authorized_command_denies_after_permission_revocation_or_device_move() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let manager_id = Uuid::now_v7();
    let group_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let direct_permission_id = Uuid::now_v7();
    let inherited_permission_id = Uuid::now_v7();
    let device_id = format!("public-command-device-{}", Uuid::now_v7());

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-command-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-command-manager', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(tenant_id.to_string())
    .bind(manager_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id) VALUES (?, ?, 'command-root', ?)",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, asset_id, owner_user_id)
         VALUES (?, ?, ?, ?)",
    )
    .bind(&device_id)
    .bind(tenant_id.to_string())
    .bind(asset_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'manager', 0, ?)",
    )
    .bind(direct_permission_id.to_string())
    .bind(tenant_id.to_string())
    .bind(manager_id.to_string())
    .bind(&device_id)
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let direct_command = command(tenant_id, &device_id);
    let created = store
        .enqueue_authorized_command(manager_id, direct_command.clone())
        .await
        .unwrap()
        .unwrap();
    let replay = store
        .enqueue_authorized_command(manager_id, direct_command)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created.id, replay.id);
    sqlx::query("UPDATE resource_permissions SET revoked_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(direct_permission_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .enqueue_authorized_command(manager_id, command(tenant_id, &device_id))
            .await
            .unwrap(),
        None
    );

    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'command-group')",
    )
    .bind(group_id.to_string())
    .bind(tenant_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO user_group_members (tenant_id, group_id, user_id) VALUES (?, ?, ?)")
        .bind(tenant_id.to_string())
        .bind(group_id.to_string())
        .bind(manager_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_group_id, asset_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'manager', 1, ?)",
    )
    .bind(inherited_permission_id.to_string())
    .bind(tenant_id.to_string())
    .bind(group_id.to_string())
    .bind(asset_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(
        store
            .enqueue_authorized_command(manager_id, command(tenant_id, &device_id))
            .await
            .unwrap()
            .is_some()
    );
    sqlx::query("UPDATE devices SET asset_id = NULL WHERE device_id = ? AND tenant_id = ?")
        .bind(&device_id)
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .enqueue_authorized_command(manager_id, command(tenant_id, &device_id))
            .await
            .unwrap(),
        None
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn sqlite_public_device_list_handoff_only_pauses_the_selected_request() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let store = Arc::new(store);
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let viewer_id = Uuid::now_v7();
    let permission_id = Uuid::now_v7();
    let device_id = format!("public-list-handoff-{}", Uuid::now_v7());

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-list-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-list-viewer', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(tenant_id.to_string())
    .bind(viewer_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id, owner_user_id) VALUES (?, ?, ?)")
        .bind(&device_id)
        .bind(tenant_id.to_string())
        .bind(owner_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', 0, ?)",
    )
    .bind(permission_id.to_string())
    .bind(tenant_id.to_string())
    .bind(viewer_id.to_string())
    .bind(&device_id)
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let principal = PublicPrincipal {
        tenant_id,
        user_id: Some(viewer_id),
        app_id: "public-list-handoff-app".to_owned(),
        account_class: AccountClass::User,
    };
    let entered = Arc::new(Notify::new());
    let (release_sender, release_receiver) = oneshot::channel();
    let _hook = install_public_device_list_handoff_hook(
        tenant_id,
        viewer_id,
        principal.app_id.clone(),
        Arc::clone(&entered),
        release_receiver,
    );
    let entered_wait = entered.notified();
    let list_store = Arc::clone(&store);
    let mut task = tokio::spawn(async move {
        PublicApiRepository::list_public_devices(list_store.as_ref(), &principal, None, 100).await
    });
    timeout(TokioDuration::from_secs(1), entered_wait)
        .await
        .expect("selected list request must reach the handoff hook");

    let unrelated_store = Arc::clone(&store);
    let unrelated_principal = PublicPrincipal {
        tenant_id,
        user_id: Some(viewer_id),
        app_id: "public-list-same-user-other-app".to_owned(),
        account_class: AccountClass::User,
    };
    let mut unrelated_task = tokio::spawn(async move {
        PublicApiRepository::list_public_devices(
            unrelated_store.as_ref(),
            &unrelated_principal,
            None,
            100,
        )
        .await
    });
    let unrelated_before_release = timeout(TokioDuration::from_secs(1), &mut unrelated_task).await;

    sqlx::query("UPDATE resource_permissions SET revoked_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(permission_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    release_sender
        .send(())
        .expect("selected list request must still await the test release");

    let selected = match timeout(TokioDuration::from_secs(1), &mut task).await {
        Ok(result) => result,
        Err(_) => {
            task.abort();
            panic!("selected list request did not finish after the hook release");
        }
    };
    assert!(
        selected.unwrap().unwrap().is_empty(),
        "the detail query must not use identifiers authorized before revocation"
    );

    match unrelated_before_release {
        Ok(result) => assert!(
            result
                .expect("unrelated list task must not panic")
                .expect("unrelated list request must succeed")
                .len()
                == 1,
            "same-user request with a different app id must not be captured by the hook"
        ),
        Err(_) => {
            let _ = timeout(TokioDuration::from_secs(1), unrelated_task)
                .await
                .expect("unrelated list request must finish after the hook is released");
            panic!("the handoff hook captured an unrelated public device list request");
        }
    }
}
