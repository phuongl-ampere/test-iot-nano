use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

const HOT_CACHE_CAPACITY: usize = 1_024;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub expires_at_ms: u64,
}

#[derive(Clone)]
pub struct PersistentCache {
    state: Arc<Mutex<CacheState>>,
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("cache state I/O failed")]
    Io(#[from] std::io::Error),
    #[error("cache background task failed: {0}")]
    Task(String),
    #[error("cache clock is before the Unix epoch")]
    Clock,
    #[error("cache expiration timestamp is out of range")]
    InvalidExpiration,
    #[error("cache key cannot be empty")]
    EmptyKey,
    #[error("cache state mutex is poisoned")]
    Poisoned,
    #[error("cache state is invalid: {0}")]
    InvalidState(String),
}

struct CacheState {
    connection: Connection,
    hot: HashMap<String, CacheEntry>,
    recency: VecDeque<String>,
}

impl PersistentCache {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        let path = path.as_ref().to_path_buf();
        let state = tokio::task::spawn_blocking(move || CacheState::open(&path))
            .await
            .map_err(|error| CacheError::Task(error.to_string()))??;

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        if key.is_empty() {
            return Err(CacheError::EmptyKey);
        }

        let key = key.to_owned();
        self.with_state(move |state| state.get(&key)).await
    }

    pub async fn put(&self, entry: CacheEntry) -> Result<(), CacheError> {
        if entry.key.is_empty() {
            return Err(CacheError::EmptyKey);
        }
        let expires_at_ms =
            i64::try_from(entry.expires_at_ms).map_err(|_| CacheError::InvalidExpiration)?;

        self.with_state(move |state| state.put(entry, expires_at_ms))
            .await
    }

    pub async fn remove_expired(&self) -> Result<usize, CacheError> {
        self.with_state(CacheState::remove_expired).await
    }

    async fn with_state<T, F>(&self, operation: F) -> Result<T, CacheError>
    where
        T: Send + 'static,
        F: FnOnce(&mut CacheState) -> Result<T, CacheError> + Send + 'static,
    {
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| CacheError::Poisoned)?;
            operation(&mut state)
        })
        .await
        .map_err(|error| CacheError::Task(error.to_string()))?
    }
}

impl CacheState {
    fn open(path: &Path) -> Result<Self, CacheError> {
        prepare_cache_file(path)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.execute_batch(
            "
            PRAGMA journal_mode = DELETE;
            PRAGMA synchronous = FULL;
            CREATE TABLE IF NOT EXISTS cache_entries (
                key TEXT PRIMARY KEY NOT NULL CHECK (key <> ''),
                value BLOB NOT NULL,
                expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms >= 0)
            );
            CREATE INDEX IF NOT EXISTS cache_entries_expiry
                ON cache_entries (expires_at_ms);
            ",
        )?;

        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(CacheError::InvalidState(format!(
                "SQLite quick_check returned {integrity}"
            )));
        }

        let now_ms = current_time_ms()?;
        connection.execute(
            "DELETE FROM cache_entries WHERE expires_at_ms <= ?1",
            params![now_ms],
        )?;
        let has_empty_key: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM cache_entries WHERE key = '')",
            [],
            |row| row.get(0),
        )?;
        if has_empty_key {
            return Err(CacheError::InvalidState(
                "cache entries cannot have empty keys".to_owned(),
            ));
        }

        Ok(Self {
            connection,
            hot: HashMap::new(),
            recency: VecDeque::new(),
        })
    }

    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let now_ms = current_time_ms()?;
        if let Some(entry) = self.hot.get(key) {
            if entry.expires_at_ms > now_ms as u64 {
                let value = entry.value.clone();
                self.touch_hot(key);
                return Ok(Some(value));
            }
            self.remove_hot(key);
        }

        let entry = self
            .connection
            .query_row(
                "
                SELECT value, expires_at_ms
                FROM cache_entries
                WHERE key = ?1
                ",
                params![key],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match entry {
            Some((value, expires_at_ms)) if expires_at_ms > now_ms => {
                self.insert_hot(CacheEntry {
                    key: key.to_owned(),
                    value: value.clone(),
                    expires_at_ms: expires_at_ms as u64,
                });
                Ok(Some(value))
            }
            Some(_) => {
                self.connection.execute(
                    "DELETE FROM cache_entries WHERE key = ?1 AND expires_at_ms <= ?2",
                    params![key, now_ms],
                )?;
                Ok(None)
            }
            None => Ok(None),
        }
    }

    fn put(&mut self, entry: CacheEntry, expires_at_ms: i64) -> Result<(), CacheError> {
        let now_ms = current_time_ms()?;
        if expires_at_ms <= now_ms {
            self.remove_hot(&entry.key);
            self.connection.execute(
                "DELETE FROM cache_entries WHERE key = ?1",
                params![entry.key],
            )?;
            return Ok(());
        }

        self.connection.execute(
            "
            INSERT INTO cache_entries (key, value, expires_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                expires_at_ms = excluded.expires_at_ms
            ",
            params![entry.key, entry.value, expires_at_ms],
        )?;
        self.insert_hot(entry);
        Ok(())
    }

    fn remove_expired(&mut self) -> Result<usize, CacheError> {
        let now_ms = current_time_ms()?;
        self.hot
            .retain(|_, entry| entry.expires_at_ms > now_ms as u64);
        self.recency.retain(|key| self.hot.contains_key(key));
        Ok(self.connection.execute(
            "DELETE FROM cache_entries WHERE expires_at_ms <= ?1",
            params![now_ms],
        )?)
    }

    fn insert_hot(&mut self, entry: CacheEntry) {
        let key = entry.key.clone();
        self.hot.insert(key.clone(), entry);
        self.touch_hot(&key);
        while self.hot.len() > HOT_CACHE_CAPACITY {
            let least_recently_used = self
                .recency
                .pop_front()
                .expect("a non-empty hot cache has an access-order entry");
            self.hot.remove(&least_recently_used);
        }
    }

    fn touch_hot(&mut self, key: &str) {
        if let Some(position) = self
            .recency
            .iter()
            .position(|existing_key| existing_key == key)
        {
            self.recency.remove(position);
        }
        self.recency.push_back(key.to_owned());
    }

    fn remove_hot(&mut self, key: &str) {
        self.hot.remove(key);
        if let Some(position) = self
            .recency
            .iter()
            .position(|existing_key| existing_key == key)
        {
            self.recency.remove(position);
        }
    }
}

fn current_time_ms() -> Result<i64, CacheError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CacheError::Clock)?;
    i64::try_from(duration.as_millis()).map_err(|_| CacheError::InvalidExpiration)
}

fn prepare_cache_file(path: &Path) -> Result<(), CacheError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(CacheError::InvalidState(format!(
                    "cache path is not a regular file: {}",
                    path.display()
                )));
            }
            ensure_cache_file_owner_only(path, &metadata)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_cache_file(path)?,
        Err(error) => return Err(CacheError::Io(error)),
    }
    Ok(())
}

fn create_cache_file(path: &Path) -> Result<(), CacheError> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    drop(options.open(path)?);
    Ok(())
}

#[cfg(unix)]
fn ensure_cache_file_owner_only(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<(), CacheError> {
    use std::os::unix::fs::MetadataExt;

    let current_uid = rustix::process::geteuid().as_raw();
    let mode = metadata.mode() & 0o777;
    if metadata.uid() != current_uid || mode != 0o600 {
        return Err(CacheError::InvalidState(format!(
            "cache file must be owned by the service user with mode 0600: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_cache_file_owner_only(
    _path: &Path,
    _metadata: &std::fs::Metadata,
) -> Result<(), CacheError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::{CacheEntry, PersistentCache};

    fn future_expiration() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000
    }

    #[tokio::test]
    async fn hot_cache_evicts_the_least_recently_used_entry_at_capacity() {
        let directory = tempdir().unwrap();
        let cache = PersistentCache::open(directory.path().join("cache.sqlite"))
            .await
            .unwrap();

        for index in 0..1_024 {
            cache
                .put(CacheEntry {
                    key: format!("device:{index}"),
                    value: vec![index as u8],
                    expires_at_ms: future_expiration(),
                })
                .await
                .unwrap();
        }
        assert_eq!(cache.get("device:0").await.unwrap(), Some(vec![0]));
        cache
            .put(CacheEntry {
                key: "device:overflow".to_owned(),
                value: b"overflow".to_vec(),
                expires_at_ms: future_expiration(),
            })
            .await
            .unwrap();

        let state = cache.state.lock().unwrap();
        assert_eq!(state.hot.len(), 1_024);
        assert!(state.hot.contains_key("device:0"));
        assert!(!state.hot.contains_key("device:1"));
        assert!(state.hot.contains_key("device:overflow"));
    }
}
