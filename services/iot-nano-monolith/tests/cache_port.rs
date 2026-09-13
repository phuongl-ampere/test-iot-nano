use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use iot_nano_monolith::PersistentCache;
use iot_nano_mqttd::{CacheEntry, CacheError, CachePort};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn persistent_cache_port_reopens_live_entries_and_hides_expired_entries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache.sqlite");
    let cache = Arc::new(PersistentCache::open(&path).await.unwrap());
    let port: Arc<dyn CachePort> = cache;

    port.put(CacheEntry {
        key: "device:live".to_owned(),
        value: b"authorized".to_vec(),
        expires_at_ms: now_ms() + 60_000,
    })
    .await
    .unwrap();
    port.put(CacheEntry {
        key: "device:expired".to_owned(),
        value: b"expired".to_vec(),
        expires_at_ms: now_ms().saturating_sub(1),
    })
    .await
    .unwrap();

    assert_eq!(
        port.get("device:live").await.unwrap(),
        Some(b"authorized".to_vec())
    );
    assert_eq!(port.get("device:expired").await.unwrap(), None);
    assert_eq!(port.get("").await, Err(CacheError::EmptyKey));
    assert_eq!(
        port.put(CacheEntry {
            key: String::new(),
            value: b"invalid".to_vec(),
            expires_at_ms: now_ms() + 60_000,
        })
        .await,
        Err(CacheError::EmptyKey)
    );

    drop(port);
    let reopened = Arc::new(PersistentCache::open(&path).await.unwrap());
    let reopened_port: Arc<dyn CachePort> = reopened;

    assert_eq!(
        reopened_port.get("device:live").await.unwrap(),
        Some(b"authorized".to_vec())
    );
    assert_eq!(reopened_port.get("device:expired").await.unwrap(), None);
}
