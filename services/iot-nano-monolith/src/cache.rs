use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    path::{Path, PathBuf},
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
    _state_file: File,
    connection: Connection,
    hot: HashMap<String, CacheEntry>,
    recency: VecDeque<String>,
}

impl PersistentCache {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CacheError> {
        let path = path.as_ref().to_path_buf();
        let state = tokio::task::spawn_blocking(move || CacheState::open_path(path))
            .await
            .map_err(|error| CacheError::Task(error.to_string()))??;

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub(crate) async fn open_file(file: File, path: PathBuf) -> Result<Self, CacheError> {
        let state = tokio::task::spawn_blocking(move || CacheState::open_file(file, path))
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
    fn open_path(path: PathBuf) -> Result<Self, CacheError> {
        let path = canonical_cache_path(&path)?;
        let file = open_cache_file(&path)?;
        Self::open_file(file, path)
    }

    fn open_file(file: File, path: PathBuf) -> Result<Self, CacheError> {
        validate_cache_file(&file)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
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

        validate_cache_schema(&connection)?;

        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(CacheError::InvalidState(format!(
                "SQLite quick_check returned {integrity}"
            )));
        }

        validate_cache_entries(&connection)?;
        let now_ms = current_time_ms()?;
        connection.execute(
            "DELETE FROM cache_entries WHERE expires_at_ms <= ?1",
            params![now_ms],
        )?;

        Ok(Self {
            _state_file: file,
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

fn canonical_cache_path(path: &Path) -> Result<PathBuf, CacheError> {
    let file_name = path
        .file_name()
        .ok_or_else(|| CacheError::InvalidState("cache path must name a file".to_owned()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    Ok(std::fs::canonicalize(parent)?.join(file_name))
}

fn validate_cache_schema(connection: &Connection) -> Result<(), CacheError> {
    let schema: Option<String> = connection
        .query_row(
            "
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'cache_entries'
            ",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let schema = schema
        .ok_or_else(|| CacheError::InvalidState("cache_entries table is missing".to_owned()))?;
    let normalized = schema
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    for required in [
        "keytextprimarykeynotnullcheck(key<>'')",
        "valueblobnotnull",
        "expires_at_msintegernotnullcheck(expires_at_ms>=0)",
    ] {
        if !normalized.contains(required) {
            return Err(CacheError::InvalidState(
                "cache_entries schema does not enforce the required invariants".to_owned(),
            ));
        }
    }

    let columns = connection
        .prepare("PRAGMA table_info(cache_entries)")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if columns
        != [
            ("key".to_owned(), "TEXT".to_owned(), 1, 1),
            ("value".to_owned(), "BLOB".to_owned(), 1, 0),
            ("expires_at_ms".to_owned(), "INTEGER".to_owned(), 1, 0),
        ]
    {
        return Err(CacheError::InvalidState(
            "cache_entries columns are invalid".to_owned(),
        ));
    }

    let has_expiry_index: bool = connection.query_row(
        "
        SELECT EXISTS(
            SELECT 1
            FROM sqlite_master
            WHERE type = 'index'
              AND name = 'cache_entries_expiry'
              AND tbl_name = 'cache_entries'
        )
        ",
        [],
        |row| row.get(0),
    )?;
    if !has_expiry_index {
        return Err(CacheError::InvalidState(
            "cache expiration index is missing".to_owned(),
        ));
    }
    Ok(())
}

fn validate_cache_entries(connection: &Connection) -> Result<(), CacheError> {
    let has_invalid_entry: bool = connection.query_row(
        "
        SELECT EXISTS(
            SELECT 1
            FROM cache_entries
            WHERE key = ''
               OR typeof(key) <> 'text'
               OR typeof(value) <> 'blob'
               OR typeof(expires_at_ms) <> 'integer'
               OR expires_at_ms < 0
        )
        ",
        [],
        |row| row.get(0),
    )?;
    if has_invalid_entry {
        return Err(CacheError::InvalidState(
            "cache entries violate the required invariants".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_cache_file(path: &Path) -> Result<File, CacheError> {
    use rustix::{
        fs::{Mode, OFlags, fchmod, openat},
        io::Errno,
    };

    let (file, created) = loop {
        match openat(
            rustix::fs::CWD,
            path,
            OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(file) => break (file, false),
            Err(Errno::NOENT) => match openat(
                rustix::fs::CWD,
                path,
                OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::from(0o600),
            ) {
                Ok(file) => break (file, true),
                Err(Errno::EXIST) => continue,
                Err(error) => return Err(CacheError::Io(error.into())),
            },
            Err(error) => return Err(CacheError::Io(error.into())),
        }
    };
    if created {
        fchmod(&file, Mode::from(0o600)).map_err(std::io::Error::from)?;
    }
    let file = File::from(file);
    validate_cache_file(&file)?;
    Ok(file)
}

#[cfg(not(unix))]
fn open_cache_file(path: &Path) -> Result<File, CacheError> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    validate_cache_file(&file)?;
    Ok(file)
}

fn validate_cache_file(file: &File) -> Result<(), CacheError> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(CacheError::InvalidState(
            "cache path is not a regular file".to_owned(),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let current_uid = rustix::process::geteuid().as_raw();
        let mode = metadata.mode() & 0o777;
        if metadata.uid() != current_uid || mode != 0o600 || metadata.nlink() != 1 {
            return Err(CacheError::InvalidState(
                "cache file must be owner-only with no additional hard links".to_owned(),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use tempfile::tempdir;

    use super::{CacheEntry, PersistentCache, canonical_cache_path};

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

    #[cfg(unix)]
    #[test]
    fn canonical_cache_path_resolves_only_the_parent_directory() {
        let directory = tempdir().unwrap();
        let parent = directory.path().join("cache-parent");
        let target = directory.path().join("cache-target.sqlite");
        std::fs::create_dir(&parent).unwrap();
        std::fs::write(&target, b"unrelated").unwrap();
        let cache_path = parent.join("cache.sqlite");
        std::os::unix::fs::symlink(&target, &cache_path).unwrap();

        assert_eq!(
            canonical_cache_path(&cache_path).unwrap(),
            std::fs::canonicalize(&parent).unwrap().join("cache.sqlite")
        );
        assert_ne!(
            canonical_cache_path(&cache_path).unwrap(),
            PathBuf::from(std::fs::canonicalize(&target).unwrap())
        );
    }
}
