use std::path::Path;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use rumqttd::{
    BrokerStorage, BrokerStorageState, InboundQos2CommitResult, InboundQos2CompletionResult,
    InboundQos2JournalState, InboundQos2PrepareResult, RetentionPolicy, StorageError,
    StoredPublish, StoredSession,
};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug)]
pub struct SqliteStorage {
    connection: Mutex<Connection>,
    next_lease_token: AtomicU64,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let connection = Connection::open(path).map_err(sqlite_error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "busy_timeout", 5_000)
            .map_err(sqlite_error)?;
        connection
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS retained (
                    topic TEXT PRIMARY KEY NOT NULL,
                    value BLOB NOT NULL,
                    stored_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    client_id TEXT PRIMARY KEY NOT NULL,
                    value BLOB NOT NULL,
                    stored_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS offline_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    value BLOB NOT NULL,
                    stored_at_ms INTEGER NOT NULL,
                    lease_until_ms INTEGER,
                    lease_token TEXT
                );
                CREATE INDEX IF NOT EXISTS offline_queue_client_id
                    ON offline_queue(client_id, id);
                CREATE TABLE IF NOT EXISTS inbound_qos (
                    client_id TEXT NOT NULL,
                    packet_id INTEGER NOT NULL,
                    qos INTEGER NOT NULL,
                    value BLOB NOT NULL,
                    stored_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (client_id, packet_id)
                );
                CREATE TABLE IF NOT EXISTS inbound_qos2_journal (
                    client_id TEXT NOT NULL,
                    packet_id INTEGER NOT NULL,
                    qos INTEGER NOT NULL,
                    value BLOB NOT NULL,
                    stored_at_ms INTEGER NOT NULL,
                    committed INTEGER NOT NULL DEFAULT 0,
                    state INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (client_id, packet_id)
                );
                ",
            )
            .map_err(sqlite_error)?;
        let journal_columns = connection
            .prepare("PRAGMA table_info(inbound_qos2_journal)")
            .map_err(sqlite_error)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        let has_committed_column = journal_columns.iter().any(|name| name == "committed");
        if !has_committed_column {
            connection
                .execute(
                    "ALTER TABLE inbound_qos2_journal
                     ADD COLUMN committed INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .map_err(sqlite_error)?;
        }
        if !journal_columns.iter().any(|name| name == "state") {
            connection
                .execute(
                    "ALTER TABLE inbound_qos2_journal
                     ADD COLUMN state INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .map_err(sqlite_error)?;
            if has_committed_column {
                connection
                    .execute(
                        "UPDATE inbound_qos2_journal
                         SET state = CASE WHEN committed = 1 THEN 2 ELSE 0 END",
                        [],
                    )
                    .map_err(sqlite_error)?;
            }
        }
        Ok(Self {
            connection: Mutex::new(connection),
            next_lease_token: AtomicU64::new(1),
        })
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        self.connection
            .lock()
            .map_err(|_| StorageError::new("sqlite connection mutex poisoned"))
    }
}

impl BrokerStorage for SqliteStorage {
    fn load(&self, now_ms: u64) -> Result<BrokerStorageState, StorageError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let mut retained_statement = transaction
            .prepare("SELECT topic, value, stored_at_ms FROM retained")
            .map_err(sqlite_error)?;
        let retained = retained_statement
            .query_map([], |row| {
                let topic: String = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                Ok((topic, value, row.get::<_, u64>(2)?))
            })
            .map_err(sqlite_error)?
            .map(|row| {
                let (topic, value, stored_at_ms) = row.map_err(sqlite_error)?;
                let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
                if publish.topic_string() != topic
                    || publish.stored_at_ms != stored_at_ms
                    || !publish.is_retained()
                    || publish.payload_is_empty()
                {
                    return Err(StorageError::new(
                        "retained row topic does not match serialized publish topic",
                    ));
                }
                Ok((topic, publish))
            })
            .collect::<Result<_, StorageError>>()?;

        drop(retained_statement);
        let mut session_statement = transaction
            .prepare("SELECT client_id, value, stored_at_ms FROM sessions")
            .map_err(sqlite_error)?;
        let sessions: Vec<StoredSession> = session_statement
            .query_map([], |row| {
                let client_id: String = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                Ok((client_id, value, row.get::<_, u64>(2)?))
            })
            .map_err(sqlite_error)?
            .map(|row| {
                let (client_id, value, stored_at_ms) = row.map_err(sqlite_error)?;
                let session: StoredSession = serde_json::from_slice(&value).map_err(json_error)?;
                if session.client_id != client_id || session.stored_at_ms != stored_at_ms {
                    return Err(StorageError::new(
                        "session row client ID does not match serialized session",
                    ));
                }
                if session.tracker.id != session.client_id {
                    return Err(StorageError::new(
                        "session tracker ID does not match session client ID",
                    ));
                }
                if session
                    .tracker
                    .data_requests
                    .iter()
                    .any(|request| !session.subscriptions.contains(&request.filter))
                {
                    return Err(StorageError::new(
                        "session tracker filter is absent from subscriptions",
                    ));
                }
                if session.inflight.iter().any(|inflight| {
                    inflight.filter.as_ref().is_some_and(|filter| {
                        session
                            .tracker
                            .data_requests
                            .iter()
                            .find(|request| request.filter == *filter)
                            .is_none_or(|request| request.filter_idx != inflight.filter_idx)
                    })
                }) {
                    return Err(StorageError::new(
                        "session inflight filter or index is absent from tracker",
                    ));
                }
                if session.inflight.iter().any(|inflight| {
                    inflight.pkid == 0
                        || inflight.publish.packet_id() != inflight.pkid
                        || !(1..=2).contains(&inflight.publish.qos_level())
                        || inflight.stored_at_ms > now_ms
                        || (inflight.filter.is_none()
                            && (inflight.cursor.is_some() || inflight.offline_lease_id.is_none()))
                }) {
                    return Err(StorageError::new(
                        "session inflight packet, QoS, cursor, or lease identity is invalid",
                    ));
                }
                let phase_ids = session
                    .outbound_qos2
                    .iter()
                    .map(|phase| phase.packet_id)
                    .collect::<std::collections::HashSet<_>>();
                let pending_ids = session
                    .unacked_pubrels
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>();
                let phase_leases = session
                    .outbound_qos2
                    .iter()
                    .filter_map(|phase| {
                        phase
                            .offline_lease_id
                            .map(|lease_id| (phase.packet_id, lease_id))
                    })
                    .collect::<std::collections::HashMap<_, _>>();
                let stored_leases = session
                    .qos2_leases
                    .iter()
                    .copied()
                    .collect::<std::collections::HashMap<_, _>>();
                if phase_ids.len() != session.outbound_qos2.len()
                    || phase_ids != pending_ids
                    || phase_leases.len() != session.qos2_leases.len()
                    || phase_leases != stored_leases
                    || session.outbound_qos2.iter().any(|phase| {
                        phase.packet_id == 0
                            || phase.stored_at_ms > now_ms
                            || phase.offline_lease_id
                                != session
                                    .qos2_leases
                                    .iter()
                                    .find(|(packet_id, _)| *packet_id == phase.packet_id)
                                    .map(|(_, lease_id)| *lease_id)
                    })
                {
                    return Err(StorageError::new(
                        "session outbound QoS2 phase or lease reference is invalid",
                    ));
                }
                Ok(session)
            })
            .collect::<Result<_, StorageError>>()?;
        drop(session_statement);
        let session_clients = sessions
            .iter()
            .map(|session| session.client_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut offline_statement = transaction
            .prepare("SELECT client_id, value, stored_at_ms FROM offline_queue")
            .map_err(sqlite_error)?;
        offline_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            })
            .map_err(sqlite_error)?
            .try_for_each(|row| {
                let (client_id, value, stored_at_ms) = row.map_err(sqlite_error)?;
                let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
                if !session_clients.contains(client_id.as_str())
                    || stored_at_ms > now_ms
                    || publish.stored_at_ms != stored_at_ms
                {
                    return Err(StorageError::new(
                        "offline row client association or timestamp is invalid",
                    ));
                }
                Ok::<_, StorageError>(())
            })?;
        drop(offline_statement);
        let mut inbound_statement = transaction
            .prepare("SELECT client_id, packet_id, qos, value, stored_at_ms FROM inbound_qos")
            .map_err(sqlite_error)?;
        inbound_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u16>(1)?,
                    row.get::<_, u8>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, u64>(4)?,
                ))
            })
            .map_err(sqlite_error)?
            .try_for_each(|row| {
                let (client_id, packet_id, qos, value, stored_at_ms) = row.map_err(sqlite_error)?;
                let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
                if client_id.is_empty()
                    || stored_at_ms > now_ms
                    || publish.stored_at_ms != stored_at_ms
                    || publish.packet_id() != packet_id
                    || publish.qos_level() != qos
                {
                    return Err(StorageError::new(
                        "inbound row identity does not match serialized publish",
                    ));
                }
                Ok::<_, StorageError>(())
            })?;
        drop(inbound_statement);
        let mut journal_statement = transaction
            .prepare(
                "SELECT client_id, packet_id, qos, value, stored_at_ms, state
             FROM inbound_qos2_journal",
            )
            .map_err(sqlite_error)?;
        let inbound_qos2 = journal_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u16>(1)?,
                    row.get::<_, u8>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, u8>(5)?,
                ))
            })
            .map_err(sqlite_error)?
            .try_fold(Vec::new(), |mut committed_rows, row| {
                let (client_id, packet_id, qos, value, stored_at_ms, state) =
                    row.map_err(sqlite_error)?;
                let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
                validate_inbound_qos2_row(
                    &client_id,
                    packet_id,
                    qos,
                    stored_at_ms,
                    &publish,
                    now_ms,
                )?;
                let state = InboundQos2JournalState::from_storage(state)?;
                if state == InboundQos2JournalState::Committed {
                    committed_rows.push(rumqttd::InboundQos2JournalEntry {
                        client_id,
                        packet_id,
                        publish,
                        state,
                    });
                }
                Ok::<_, StorageError>(committed_rows)
            })?;
        drop(journal_statement);
        transaction.commit().map_err(sqlite_error)?;

        Ok(BrokerStorageState {
            retained,
            sessions,
            inbound_qos2,
        })
    }

    fn save_retained(
        &self,
        topic: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let value = serde_json::to_vec(publish).map_err(json_error)?;
        self.locked()?
            .execute(
                "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)
                 ON CONFLICT(topic) DO UPDATE SET value = excluded.value,
                   stored_at_ms = excluded.stored_at_ms",
                params![topic, value, now_ms as i64],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn delete_retained(&self, topic: &str) -> Result<(), StorageError> {
        self.locked()?
            .execute("DELETE FROM retained WHERE topic = ?1", params![topic])
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn commit_inbound(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        if publish.qos_level() == 2 {
            return self
                .prepare_inbound_qos2(client_id, publish, now_ms)
                .map(|_| ());
        }
        let value = serde_json::to_vec(publish).map_err(json_error)?;
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        if publish.is_retained() {
            if publish.payload_is_empty() {
                transaction
                    .execute(
                        "DELETE FROM retained WHERE topic = ?1",
                        params![publish.topic_string()],
                    )
                    .map_err(sqlite_error)?;
            } else {
                transaction
                    .execute(
                        "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)
                         ON CONFLICT(topic) DO UPDATE SET value = excluded.value,
                           stored_at_ms = excluded.stored_at_ms",
                        params![publish.topic_string(), value.clone(), now_ms as i64],
                    )
                    .map_err(sqlite_error)?;
            }
        }
        if publish.qos_level() > 0 {
            transaction
                .execute(
                    "INSERT INTO inbound_qos(client_id, packet_id, qos, value, stored_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(client_id, packet_id) DO UPDATE SET qos = excluded.qos,
                       value = excluded.value, stored_at_ms = excluded.stored_at_ms",
                    params![
                        client_id,
                        publish.packet_id() as i64,
                        publish.qos_level() as i64,
                        value.clone(),
                        now_ms as i64
                    ],
                )
                .map_err(sqlite_error)?;
        }
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }

    fn load_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<StoredPublish>, StorageError> {
        let connection = self.locked()?;
        let value = connection
            .query_row(
                "SELECT value FROM inbound_qos
                 WHERE client_id = ?1 AND packet_id = ?2 AND qos = 2",
                params![client_id, packet_id as i64],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        value
            .map(|value| serde_json::from_slice(&value).map_err(json_error))
            .transpose()
    }

    fn complete_inbound(&self, client_id: &str, packet_id: u16) -> Result<(), StorageError> {
        self.locked()?
            .execute(
                "DELETE FROM inbound_qos WHERE client_id = ?1 AND packet_id = ?2",
                params![client_id, packet_id as i64],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn prepare_inbound_qos2(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<InboundQos2PrepareResult, StorageError> {
        if client_id.is_empty() || publish.qos_level() != 2 || publish.packet_id() == 0 {
            return Err(StorageError::new(
                "inbound QoS2 journal entry has invalid identity",
            ));
        }
        let value = serde_json::to_vec(publish).map_err(json_error)?;
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let existing = transaction
            .query_row(
                "SELECT qos, value, stored_at_ms, state
                 FROM inbound_qos2_journal
                 WHERE client_id = ?1 AND packet_id = ?2",
                params![client_id, publish.packet_id() as i64],
                |row| {
                    Ok((
                        row.get::<_, u8>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, u8>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?;

        let (result, persist) = if let Some((qos, existing_value, stored_at_ms, state)) = existing {
            let existing_publish: StoredPublish =
                serde_json::from_slice(&existing_value).map_err(json_error)?;
            validate_inbound_qos2_row(
                client_id,
                publish.packet_id(),
                qos,
                stored_at_ms,
                &existing_publish,
                now_ms,
            )?;
            let state = InboundQos2JournalState::from_storage(state)?;
            if !publish.is_duplicate() && state == InboundQos2JournalState::Completed {
                transaction
                    .execute(
                        "UPDATE inbound_qos2_journal
                         SET value = ?1, stored_at_ms = ?2, state = ?3
                        WHERE client_id = ?4 AND packet_id = ?5",
                        params![
                            value.clone(),
                            publish.stored_at_ms as i64,
                            InboundQos2JournalState::Pending as u8,
                            client_id,
                            publish.packet_id() as i64
                        ],
                    )
                    .map_err(sqlite_error)?;
                (
                    InboundQos2PrepareResult::NewPending {
                        publish: publish.clone(),
                    },
                    true,
                )
            } else {
                if !existing_publish.has_same_inbound_qos2_identity(publish) {
                    return Err(StorageError::new(
                        "mismatched inbound QoS2 duplicate for client and packet ID",
                    ));
                }
                if publish.is_duplicate() {
                    (
                        match state {
                            InboundQos2JournalState::Pending => {
                                InboundQos2PrepareResult::ExistingPending {
                                    publish: existing_publish,
                                }
                            }
                            InboundQos2JournalState::Committed => {
                                InboundQos2PrepareResult::ExistingCommitted {
                                    publish: existing_publish,
                                }
                            }
                            InboundQos2JournalState::Completed => {
                                InboundQos2PrepareResult::ExistingCompleted {
                                    publish: existing_publish,
                                }
                            }
                        },
                        false,
                    )
                } else {
                    return Err(StorageError::new("inbound QoS2 packet ID is still active"));
                }
            }
        } else {
            transaction
                .execute(
                    "INSERT INTO inbound_qos2_journal(
                        client_id, packet_id, qos, value, stored_at_ms, state
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        client_id,
                        publish.packet_id() as i64,
                        publish.qos_level() as i64,
                        value.clone(),
                        publish.stored_at_ms as i64,
                        InboundQos2JournalState::Pending as u8
                    ],
                )
                .map_err(sqlite_error)?;
            (
                InboundQos2PrepareResult::NewPending {
                    publish: publish.clone(),
                },
                true,
            )
        };
        if persist {
            persist_inbound_qos2_prepare(&transaction, client_id, publish, &value)?;
        }
        transaction.commit().map_err(sqlite_error)?;
        Ok(result)
    }

    fn commit_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
        now_ms: u64,
    ) -> Result<InboundQos2CommitResult, StorageError> {
        if client_id.is_empty() || packet_id == 0 {
            return Err(StorageError::new(
                "inbound QoS2 journal entry has invalid identity",
            ));
        }
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let (qos, value, stored_at_ms, state) = transaction
            .query_row(
                "SELECT qos, value, stored_at_ms, state
                 FROM inbound_qos2_journal
                 WHERE client_id = ?1 AND packet_id = ?2",
                params![client_id, packet_id as i64],
                |row| {
                    Ok((
                        row.get::<_, u8>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, u8>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?
            .ok_or_else(|| {
                StorageError::new("inbound QoS2 journal entry is missing before commit")
            })?;
        let existing_publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
        validate_inbound_qos2_row(
            client_id,
            packet_id,
            qos,
            stored_at_ms,
            &existing_publish,
            now_ms,
        )?;
        let state = InboundQos2JournalState::from_storage(state)?;
        let result = match state {
            InboundQos2JournalState::Pending => {
                transaction
                    .execute(
                        "UPDATE inbound_qos2_journal
                     SET state = ?1
                     WHERE client_id = ?2 AND packet_id = ?3",
                        params![
                            InboundQos2JournalState::Committed as u8,
                            client_id,
                            packet_id as i64
                        ],
                    )
                    .map_err(sqlite_error)?;
                persist_retained_effect(&transaction, &existing_publish, &value)?;
                InboundQos2CommitResult::AppendRequired {
                    publish: existing_publish,
                }
            }
            InboundQos2JournalState::Committed => InboundQos2CommitResult::ExistingCommitted {
                publish: existing_publish,
            },
            InboundQos2JournalState::Completed => InboundQos2CommitResult::ExistingCompleted {
                publish: existing_publish,
            },
        };
        transaction.commit().map_err(sqlite_error)?;
        Ok(result)
    }

    fn complete_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<InboundQos2CompletionResult, StorageError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let (value, state) = transaction
            .query_row(
                "SELECT value, state FROM inbound_qos2_journal
                 WHERE client_id = ?1 AND packet_id = ?2",
                params![client_id, packet_id as i64],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u8>(1)?)),
            )
            .optional()
            .map_err(sqlite_error)?
            .ok_or_else(|| {
                StorageError::new("inbound QoS2 journal entry is missing before completion")
            })?;
        let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
        let result = match InboundQos2JournalState::from_storage(state)? {
            InboundQos2JournalState::Pending => {
                return Err(StorageError::new(
                    "inbound QoS2 journal entry is pending before completion",
                ));
            }
            InboundQos2JournalState::Committed => {
                transaction
                    .execute(
                        "UPDATE inbound_qos2_journal SET state = ?1
                         WHERE client_id = ?2 AND packet_id = ?3",
                        params![
                            InboundQos2JournalState::Completed as u8,
                            client_id,
                            packet_id as i64
                        ],
                    )
                    .map_err(sqlite_error)?;
                InboundQos2CompletionResult::Completed { publish }
            }
            InboundQos2JournalState::Completed => {
                InboundQos2CompletionResult::ExistingCompleted { publish }
            }
        };
        transaction.commit().map_err(sqlite_error)?;
        Ok(result)
    }

    fn save_session(&self, session: &StoredSession, now_ms: u64) -> Result<(), StorageError> {
        let value = serde_json::to_vec(session).map_err(json_error)?;
        self.locked()?
            .execute(
                "INSERT INTO sessions(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)
                 ON CONFLICT(client_id) DO UPDATE SET value = excluded.value,
                   stored_at_ms = excluded.stored_at_ms",
                params![session.client_id, value, now_ms as i64],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn delete_session(&self, client_id: &str) -> Result<(), StorageError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM sessions WHERE client_id = ?1",
                params![client_id],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM offline_queue WHERE client_id = ?1",
                params![client_id],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM inbound_qos WHERE client_id = ?1",
                params![client_id],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM inbound_qos2_journal WHERE client_id = ?1",
                params![client_id],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }

    fn enqueue_offline(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<(), StorageError> {
        if now_ms.saturating_sub(publish.stored_at_ms) >= policy.offline_ttl_ms
            || publish.message_expired(now_ms)
        {
            return Ok(());
        }
        let value = serde_json::to_vec(publish).map_err(json_error)?;
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let mut statement = transaction
            .prepare("SELECT value FROM offline_queue WHERE client_id = ?1")
            .map_err(sqlite_error)?;
        let duplicate = statement
            .query_map(params![client_id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(sqlite_error)?
            .try_fold(false, |_, row| {
                let stored: StoredPublish =
                    serde_json::from_slice(&row.map_err(sqlite_error)?).map_err(json_error)?;
                Ok::<_, StorageError>(stored.has_same_offline_identity(publish))
            })?;
        drop(statement);
        if duplicate {
            return Ok(());
        }
        transaction
            .execute(
                "INSERT INTO offline_queue(client_id, value, stored_at_ms)
                 VALUES (?1, ?2, ?3)",
                params![client_id, value, now_ms as i64],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM offline_queue
                 WHERE client_id = ?1 AND id NOT IN (
                    SELECT id FROM offline_queue WHERE client_id = ?1
                    ORDER BY id DESC LIMIT ?2
                 )",
                params![client_id, policy.max_offline_messages as i64],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }

    fn lease_offline(
        &self,
        client_id: &str,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<Vec<rumqttd::LeasedOffline>, StorageError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        let lease_until_ms = now_ms.saturating_add(policy.offline_lease_ms) as i64;
        let lease_token = self
            .next_lease_token
            .fetch_add(1, Ordering::Relaxed)
            .to_string();
        transaction
            .execute(
                "UPDATE offline_queue
                 SET lease_until_ms = ?1, lease_token = ?2
                 WHERE client_id = ?3
                   AND stored_at_ms > ?4
                   AND (lease_until_ms IS NULL OR lease_until_ms <= ?5)",
                params![
                    lease_until_ms,
                    lease_token,
                    client_id,
                    expiry_cutoff(now_ms, policy.offline_ttl_ms),
                    now_ms as i64
                ],
            )
            .map_err(sqlite_error)?;
        let mut statement = transaction
            .prepare(
                "SELECT id, value FROM offline_queue
                 WHERE client_id = ?1 AND lease_token = ?2 ORDER BY id",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map(params![client_id, lease_token], |row| {
                let id: u64 = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                Ok((id, value))
            })
            .map_err(sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        drop(statement);
        transaction.commit().map_err(sqlite_error)?;
        rows.into_iter()
            .map(|(lease_id, value)| {
                serde_json::from_slice(&value)
                    .map(|publish| rumqttd::LeasedOffline { lease_id, publish })
                    .map_err(json_error)
            })
            .collect()
    }

    fn acknowledge_offline(&self, client_id: &str, lease_id: u64) -> Result<(), StorageError> {
        self.locked()?
            .execute(
                "DELETE FROM offline_queue WHERE client_id = ?1 AND id = ?2",
                params![client_id, lease_id as i64],
            )
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn prune(&self, now_ms: u64, policy: RetentionPolicy) -> Result<(), StorageError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction().map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM retained WHERE stored_at_ms <= ?1",
                params![expiry_cutoff(now_ms, policy.retained_ttl_ms)],
            )
            .map_err(sqlite_error)?;
        let mut statement = transaction
            .prepare("SELECT id, value FROM offline_queue")
            .map_err(sqlite_error)?;
        let expired = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(sqlite_error)?
            .map(|row| {
                let (id, value) = row.map_err(sqlite_error)?;
                let publish: StoredPublish = serde_json::from_slice(&value).map_err(json_error)?;
                Ok::<_, StorageError>((id, publish.message_expired(now_ms)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for (id, expired) in expired {
            if expired {
                transaction
                    .execute("DELETE FROM offline_queue WHERE id = ?1", params![id])
                    .map_err(sqlite_error)?;
            }
        }
        transaction
            .execute(
                "DELETE FROM sessions WHERE stored_at_ms <= ?1",
                params![expiry_cutoff(now_ms, policy.session_ttl_ms)],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM offline_queue
                 WHERE stored_at_ms <= ?1
                    OR client_id NOT IN (SELECT client_id FROM sessions)",
                params![expiry_cutoff(now_ms, policy.offline_ttl_ms)],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM inbound_qos WHERE stored_at_ms <= ?1",
                params![expiry_cutoff(now_ms, policy.session_ttl_ms)],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM inbound_qos2_journal WHERE stored_at_ms <= ?1",
                params![expiry_cutoff(now_ms, policy.session_ttl_ms)],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(())
    }
}

fn persist_inbound_qos2_prepare(
    transaction: &rusqlite::Transaction<'_>,
    client_id: &str,
    publish: &StoredPublish,
    value: &[u8],
) -> Result<(), StorageError> {
    transaction
        .execute(
            "INSERT INTO inbound_qos(client_id, packet_id, qos, value, stored_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(client_id, packet_id) DO UPDATE SET qos = excluded.qos,
               value = excluded.value, stored_at_ms = excluded.stored_at_ms",
            params![
                client_id,
                publish.packet_id() as i64,
                publish.qos_level() as i64,
                value,
                publish.stored_at_ms as i64
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn persist_retained_effect(
    transaction: &rusqlite::Transaction<'_>,
    publish: &StoredPublish,
    value: &[u8],
) -> Result<(), StorageError> {
    if publish.is_retained() {
        if publish.payload_is_empty() {
            transaction
                .execute(
                    "DELETE FROM retained WHERE topic = ?1",
                    params![publish.topic_string()],
                )
                .map_err(sqlite_error)?;
        } else {
            transaction
                .execute(
                    "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)
                     ON CONFLICT(topic) DO UPDATE SET value = excluded.value,
                       stored_at_ms = excluded.stored_at_ms",
                    params![publish.topic_string(), value, publish.stored_at_ms as i64],
                )
                .map_err(sqlite_error)?;
        }
    }
    Ok(())
}

fn validate_inbound_qos2_row(
    client_id: &str,
    packet_id: u16,
    qos: u8,
    stored_at_ms: u64,
    publish: &StoredPublish,
    now_ms: u64,
) -> Result<(), StorageError> {
    if client_id.is_empty()
        || packet_id == 0
        || qos != 2
        || stored_at_ms > now_ms
        || publish.stored_at_ms != stored_at_ms
        || publish.packet_id() != packet_id
        || publish.qos_level() != 2
    {
        return Err(StorageError::new(
            "inbound QoS2 journal row identity does not match serialized publish",
        ));
    }
    Ok(())
}

fn expiry_cutoff(now_ms: u64, ttl_ms: u64) -> i64 {
    now_ms.saturating_sub(ttl_ms) as i64
}

fn sqlite_error(error: rusqlite::Error) -> StorageError {
    StorageError::new(error.to_string())
}

fn json_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttd::{BrokerStorage, ConnectionEvents, StoredPublish, StoredSession, Tracker};
    use tempfile::tempdir;

    #[test]
    fn sqlite_uses_wal_and_prunes_with_supplied_clock() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let publish = StoredPublish {
            publish: rumqttd::protocol::Publish::new(
                String::from("topic"),
                String::from("payload"),
                true,
            ),
            properties: None,
            stored_at_ms: 0,
        };
        storage.save_retained("topic", &publish, 0).unwrap();
        storage
            .prune(
                11,
                RetentionPolicy {
                    retained_ttl_ms: 10,
                    offline_lease_ms: 1,
                    ..RetentionPolicy::default()
                },
            )
            .unwrap();
        assert!(storage.load(11).unwrap().retained.is_empty());
        let connection = storage.connection.lock().unwrap();
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_lowercase(), "wal");
    }

    #[test]
    fn sqlite_load_rejects_malformed_retained_rows() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let publish = StoredPublish {
            publish: rumqttd::protocol::Publish::new(
                String::from("actual/topic"),
                String::from("payload"),
                true,
            ),
            properties: None,
            stored_at_ms: 0,
        };
        storage
            .connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                params!["wrong/topic", serde_json::to_vec(&publish).unwrap(), 0_i64],
            )
            .unwrap();
        assert!(storage.load(0).is_err());
    }

    #[test]
    fn sqlite_load_rejects_malformed_offline_and_inbound_rows() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        storage
            .connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO offline_queue(client_id, value, stored_at_ms) VALUES (?1, ?2, ?3)",
                params!["client", b"not-json", 0_i64],
            )
            .unwrap();
        assert!(storage.load(0).is_err());
    }

    #[test]
    fn sqlite_same_timestamp_lease_operations_do_not_overlap() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let publish = StoredPublish {
            publish: rumqttd::protocol::Publish::new(
                String::from("lease/topic"),
                String::from("payload"),
                false,
            ),
            properties: None,
            stored_at_ms: 10,
        };
        let policy = RetentionPolicy {
            offline_ttl_ms: 100,
            offline_lease_ms: 100,
            ..RetentionPolicy::default()
        };
        storage
            .enqueue_offline("client", &publish, 10, policy)
            .unwrap();
        let first = storage.lease_offline("client", 10, policy).unwrap();
        let second = storage.lease_offline("client", 10, policy).unwrap();
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }

    #[test]
    fn sqlite_offline_dedup_and_prune_honor_mqtt_message_expiry() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let policy = RetentionPolicy {
            offline_ttl_ms: 10_000,
            offline_lease_ms: 10,
            ..RetentionPolicy::default()
        };
        let session = StoredSession {
            client_id: "client".into(),
            tracker: Tracker::new("client".into()),
            subscriptions: vec![],
            unacked_pubrels: vec![],
            inflight: vec![],
            qos2_leases: vec![],
            outbound_qos2: vec![],
            qos2_publishes: vec![],
            metrics: ConnectionEvents::default(),
            stored_at_ms: 1,
        };
        storage.save_session(&session, 1).unwrap();

        let mut first = StoredPublish {
            publish: rumqttd::protocol::Publish::new(
                String::from("lease/topic"),
                String::from("payload"),
                false,
            ),
            properties: None,
            stored_at_ms: 1,
        };
        let mut retransmit = first.clone();
        retransmit.stored_at_ms = 2;
        storage
            .enqueue_offline("client", &first, 1, policy)
            .unwrap();
        storage
            .enqueue_offline("client", &retransmit, 2, policy)
            .unwrap();
        let count: i64 = storage
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM offline_queue", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        first.properties = Some(rumqttd::protocol::PublishProperties {
            message_expiry_interval: Some(1),
            ..Default::default()
        });
        first.stored_at_ms = 0;
        storage
            .enqueue_offline("client", &first, 0, policy)
            .unwrap();
        storage.prune(1_000, policy).unwrap();
        let count: i64 = storage
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM offline_queue", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn sqlite_inbound_qos2_prepare_survives_reopen_without_overwrite() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("broker.sqlite");
        let storage = SqliteStorage::open(&database).unwrap();
        let original = qos2_publish("original", 10, 7);
        let mut duplicate = original.clone();
        let mut serialized = serde_json::to_value(&duplicate.publish).unwrap();
        serialized["dup"] = serde_json::json!(true);
        duplicate.publish = serde_json::from_value(serialized).unwrap();
        duplicate.stored_at_ms = 20;

        assert!(matches!(
            storage
                .prepare_inbound_qos2("client", &original, 10)
                .unwrap(),
            rumqttd::InboundQos2PrepareResult::NewPending { .. }
        ));
        drop(storage);

        let reopened = SqliteStorage::open(&database).unwrap();
        assert_eq!(
            reopened
                .prepare_inbound_qos2("client", &duplicate, 20)
                .unwrap(),
            rumqttd::InboundQos2PrepareResult::ExistingPending {
                publish: original.clone()
            }
        );
    }

    #[test]
    fn sqlite_inbound_qos2_rejects_mismatched_duplicate() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let original = qos2_publish("original", 10, 7);
        let mismatch = qos2_publish("different", 20, 7);

        storage
            .prepare_inbound_qos2("client", &original, 10)
            .unwrap();
        let error = storage
            .prepare_inbound_qos2("client", &mismatch, 20)
            .unwrap_err();

        assert!(error.message.contains("mismatched inbound QoS2 duplicate"));
    }

    #[test]
    fn sqlite_inbound_qos2_completion_is_idempotent_and_prune_removes_expired_rows() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let publish = qos2_publish("payload", 0, 7);

        storage.prepare_inbound_qos2("client", &publish, 0).unwrap();
        assert!(matches!(
            storage.commit_inbound_qos2("client", 7, 0).unwrap(),
            rumqttd::InboundQos2CommitResult::AppendRequired { .. }
        ));
        storage.complete_inbound_qos2("client", 7).unwrap();
        storage.complete_inbound_qos2("client", 7).unwrap();
        assert_eq!(
            storage.commit_inbound_qos2("client", 7, 0).unwrap(),
            rumqttd::InboundQos2CommitResult::ExistingCompleted {
                publish: publish.clone()
            }
        );

        storage
            .prune(
                11,
                RetentionPolicy {
                    session_ttl_ms: 10,
                    ..RetentionPolicy::default()
                },
            )
            .unwrap();
        assert!(matches!(
            storage
                .prepare_inbound_qos2("client", &publish, 11)
                .unwrap(),
            rumqttd::InboundQos2PrepareResult::NewPending { .. }
        ));
    }

    #[test]
    fn sqlite_inbound_qos2_state_machine_reuses_completed_packet_ids_at_ttl_boundary() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let policy = RetentionPolicy {
            session_ttl_ms: 10,
            ..RetentionPolicy::default()
        };
        let first = qos2_publish("first", 0, 7);
        let second = qos2_publish("second", 0, 7);

        assert!(matches!(
            storage.prepare_inbound_qos2("client", &first, 0).unwrap(),
            rumqttd::InboundQos2PrepareResult::NewPending { .. }
        ));
        assert!(matches!(
            storage.commit_inbound_qos2("client", 7, 1).unwrap(),
            rumqttd::InboundQos2CommitResult::AppendRequired { .. }
        ));
        assert!(matches!(
            storage.complete_inbound_qos2("client", 7).unwrap(),
            rumqttd::InboundQos2CompletionResult::Completed { .. }
        ));
        assert!(matches!(
            storage.prepare_inbound_qos2("client", &second, 1).unwrap(),
            rumqttd::InboundQos2PrepareResult::NewPending { publish }
                if publish == second
        ));

        assert!(matches!(
            storage.commit_inbound_qos2("client", 7, 1).unwrap(),
            rumqttd::InboundQos2CommitResult::AppendRequired { publish }
                if publish == second
        ));
        storage.complete_inbound_qos2("client", 7).unwrap();
        storage.prune(10, policy).unwrap();
        assert!(matches!(
            storage.prepare_inbound_qos2("client", &second, 10).unwrap(),
            rumqttd::InboundQos2PrepareResult::NewPending { .. }
        ));
    }

    #[test]
    fn sqlite_load_rejects_malformed_inbound_qos2_journal_rows() {
        let directory = tempdir().unwrap();
        let storage = SqliteStorage::open(directory.path().join("broker.sqlite")).unwrap();
        let publish = qos2_publish("payload", 10, 7);
        storage
            .connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO inbound_qos2_journal(
                    client_id, packet_id, qos, value, stored_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "client",
                    7_i64,
                    1_i64,
                    serde_json::to_vec(&publish).unwrap(),
                    10_i64
                ],
            )
            .unwrap();

        assert!(storage.load(10).is_err());
    }

    fn qos2_publish(payload: &str, stored_at_ms: u64, packet_id: u16) -> StoredPublish {
        let mut publish = StoredPublish {
            publish: rumqttd::protocol::Publish::new(
                String::from("journal/topic"),
                String::from(payload),
                false,
            ),
            properties: None,
            stored_at_ms,
        };
        let mut serialized = serde_json::to_value(&publish.publish).unwrap();
        serialized["qos"] = serde_json::json!("ExactlyOnce");
        serialized["pkid"] = serde_json::json!(packet_id);
        publish.publish = serde_json::from_value(serialized).unwrap();
        publish
    }
}
