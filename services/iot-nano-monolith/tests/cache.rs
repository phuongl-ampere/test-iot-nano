use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iot_nano_monolith::{CacheEntry, CacheError, PersistentCache};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn cache_persists_live_entries_and_expires_entries_on_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    let cache = PersistentCache::open(&path).await.unwrap();

    cache
        .put(CacheEntry {
            key: "device:meter-a".to_owned(),
            value: b"authorized".to_vec(),
            expires_at_ms: now_ms() + 60_000,
        })
        .await
        .unwrap();
    cache
        .put(CacheEntry {
            key: "device:expired".to_owned(),
            value: b"expired".to_vec(),
            expires_at_ms: now_ms().saturating_sub(1),
        })
        .await
        .unwrap();

    assert_eq!(
        cache.get("device:meter-a").await.unwrap(),
        Some(b"authorized".to_vec())
    );
    assert_eq!(cache.get("device:expired").await.unwrap(), None);
    drop(cache);

    let reopened = PersistentCache::open(&path).await.unwrap();
    assert_eq!(
        reopened.get("device:meter-a").await.unwrap(),
        Some(b"authorized".to_vec())
    );
    assert_eq!(reopened.get("device:expired").await.unwrap(), None);
}

#[tokio::test]
async fn cache_removes_entries_that_expire_after_persistence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    let cache = PersistentCache::open(&path).await.unwrap();

    cache
        .put(CacheEntry {
            key: "device:expired".to_owned(),
            value: b"expired".to_vec(),
            expires_at_ms: now_ms() + 100,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(cache.remove_expired().await.unwrap(), 1);
    assert_eq!(cache.get("device:expired").await.unwrap(), None);
    drop(cache);

    let reopened = PersistentCache::open(&path).await.unwrap();
    assert_eq!(reopened.get("device:expired").await.unwrap(), None);
}

#[cfg(unix)]
#[tokio::test]
async fn cache_rejects_state_files_that_are_not_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    std::fs::write(&path, []).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

    assert!(matches!(
        PersistentCache::open(&path).await,
        Err(CacheError::InvalidState(_))
    ));
}

#[tokio::test]
async fn cache_rejects_an_existing_schema_without_the_expiry_constraint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    let cache = PersistentCache::open(&path).await.unwrap();
    drop(cache);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "
            DROP TABLE cache_entries;
            CREATE TABLE cache_entries (
                key TEXT PRIMARY KEY NOT NULL CHECK (key <> ''),
                value BLOB NOT NULL,
                expires_at_ms INTEGER NOT NULL
            );
            CREATE INDEX cache_entries_expiry ON cache_entries (expires_at_ms);
            ",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        PersistentCache::open(&path).await,
        Err(CacheError::InvalidState(_))
    ));
}
