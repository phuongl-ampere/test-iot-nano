use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};

use serde::{Deserialize, Serialize};

use crate::protocol::{Publish, PublishProperties};
use crate::router::{ConnectionEvents, Tracker};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub retained_ttl_ms: u64,
    pub session_ttl_ms: u64,
    pub offline_ttl_ms: u64,
    pub max_offline_messages: usize,
    pub offline_lease_ms: u64,
    pub prune_interval_ms: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retained_ttl_ms: 24 * 60 * 60 * 1_000,
            session_ttl_ms: 24 * 60 * 60 * 1_000,
            offline_ttl_ms: 24 * 60 * 60 * 1_000,
            max_offline_messages: 1_000,
            offline_lease_ms: 30_000,
            prune_interval_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPublish {
    pub publish: Publish,
    pub properties: Option<PublishProperties>,
    pub stored_at_ms: u64,
}

impl StoredPublish {
    pub fn qos_level(&self) -> u8 {
        self.publish.qos_level()
    }

    pub fn packet_id(&self) -> u16 {
        self.publish.packet_id()
    }

    pub fn is_retained(&self) -> bool {
        self.publish.is_retained()
    }

    pub fn payload_is_empty(&self) -> bool {
        self.publish.payload_is_empty()
    }

    pub fn topic_string(&self) -> String {
        self.publish.topic_string()
    }

    pub fn message_expired(&self, now_ms: u64) -> bool {
        self.properties
            .as_ref()
            .and_then(|properties| properties.message_expiry_interval)
            .is_some_and(|seconds| {
                now_ms.saturating_sub(self.stored_at_ms) >= u64::from(seconds) * 1_000
            })
    }

    pub fn properties_for_delivery(&self, now_ms: u64) -> Option<PublishProperties> {
        let mut properties = self.properties.clone();
        if let Some(expiry) = properties
            .as_mut()
            .and_then(|properties| properties.message_expiry_interval.as_mut())
        {
            let elapsed_seconds = now_ms.saturating_sub(self.stored_at_ms) / 1_000;
            *expiry = expiry.saturating_sub(u32::try_from(elapsed_seconds).unwrap_or(u32::MAX));
        }
        properties
    }

    pub fn has_same_offline_identity(&self, other: &StoredPublish) -> bool {
        let mut publish = self.publish.clone();
        let mut other_publish = other.publish.clone();
        publish.dup = false;
        other_publish.dup = false;
        publish == other_publish && self.properties == other.properties
    }

    pub fn has_same_inbound_qos2_identity(&self, other: &StoredPublish) -> bool {
        same_inbound_qos2_identity(self, other)
    }

    pub fn is_duplicate(&self) -> bool {
        self.publish.dup
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredInflight {
    pub publish: Publish,
    pub properties: Option<PublishProperties>,
    pub stored_at_ms: u64,
    pub pkid: u16,
    pub cursor: Option<(u64, u64)>,
    pub filter_idx: usize,
    pub filter: Option<String>,
    pub offline_lease_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeasedOffline {
    pub lease_id: u64,
    pub publish: StoredPublish,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboundQos2Phase {
    pub packet_id: u16,
    pub offline_lease_id: Option<u64>,
    pub stored_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSession {
    pub client_id: String,
    pub tracker: Tracker,
    pub subscriptions: Vec<String>,
    pub unacked_pubrels: Vec<u16>,
    pub inflight: Vec<StoredInflight>,
    pub qos2_leases: Vec<(u16, u64)>,
    pub outbound_qos2: Vec<OutboundQos2Phase>,
    pub qos2_publishes: Vec<StoredPublish>,
    pub metrics: ConnectionEvents,
    pub stored_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerStorageState {
    pub retained: HashMap<String, StoredPublish>,
    pub sessions: Vec<StoredSession>,
    pub inbound_qos2: Vec<InboundQos2JournalEntry>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("broker storage error: {message}")]
pub struct StorageError {
    pub message: String,
}

impl StorageError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum InboundQos2JournalState {
    Pending = 0,
    Committed = 1,
    Completed = 2,
}

impl InboundQos2JournalState {
    pub fn from_storage(value: u8) -> Result<Self, StorageError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Committed),
            2 => Ok(Self::Completed),
            _ => Err(StorageError::new("inbound QoS2 journal state is invalid")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundQos2PrepareResult {
    NewPending { publish: StoredPublish },
    ExistingPending { publish: StoredPublish },
    ExistingCommitted { publish: StoredPublish },
    ExistingCompleted { publish: StoredPublish },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundQos2CommitResult {
    AppendRequired { publish: StoredPublish },
    ExistingCommitted { publish: StoredPublish },
    ExistingCompleted { publish: StoredPublish },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundQos2CompletionResult {
    Completed { publish: StoredPublish },
    ExistingCompleted { publish: StoredPublish },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundQos2JournalEntry {
    pub client_id: String,
    pub packet_id: u16,
    pub publish: StoredPublish,
    pub state: InboundQos2JournalState,
}

pub trait BrokerStorage: Send + Sync + std::fmt::Debug {
    fn load(&self, now_ms: u64) -> Result<BrokerStorageState, StorageError>;
    fn save_retained(
        &self,
        topic: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<(), StorageError>;
    fn delete_retained(&self, topic: &str) -> Result<(), StorageError>;
    fn commit_inbound(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<(), StorageError>;
    fn load_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<StoredPublish>, StorageError>;
    fn complete_inbound(&self, client_id: &str, packet_id: u16) -> Result<(), StorageError>;
    fn prepare_inbound_qos2(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<InboundQos2PrepareResult, StorageError>;
    fn complete_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<InboundQos2CompletionResult, StorageError>;
    fn commit_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
        now_ms: u64,
    ) -> Result<InboundQos2CommitResult, StorageError>;
    fn save_session(&self, session: &StoredSession, now_ms: u64) -> Result<(), StorageError>;
    fn delete_session(&self, client_id: &str) -> Result<(), StorageError>;
    fn enqueue_offline(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<(), StorageError>;
    fn lease_offline(
        &self,
        client_id: &str,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<Vec<LeasedOffline>, StorageError>;
    fn acknowledge_offline(&self, client_id: &str, lease_id: u64) -> Result<(), StorageError>;
    fn prune(&self, now_ms: u64, policy: RetentionPolicy) -> Result<(), StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryStorage {
    state: Mutex<MemoryState>,
    fail_writes: AtomicBool,
    fail_on_write: AtomicUsize,
    write_attempts: AtomicUsize,
}

#[derive(Debug, Default)]
struct MemoryState {
    retained: HashMap<String, StoredPublish>,
    sessions: HashMap<String, StoredSession>,
    offline: HashMap<String, Vec<OfflineRecord>>,
    next_lease_id: u64,
    inbound: HashMap<(String, u16), StoredPublish>,
    inbound_qos2_journal: HashMap<(String, u16), InboundQos2Record>,
}

#[derive(Debug, Clone)]
struct OfflineRecord {
    lease_id: u64,
    leased_until_ms: Option<u64>,
    publish: StoredPublish,
}

#[derive(Debug, Clone)]
struct InboundQos2Record {
    publish: StoredPublish,
    state: InboundQos2JournalState,
}

fn apply_inbound_effects(state: &mut MemoryState, client_id: &str, publish: &StoredPublish) {
    apply_retained_effect(&mut state.retained, publish);
    if publish.qos_level() > 0 {
        state
            .inbound
            .insert((client_id.to_owned(), publish.packet_id()), publish.clone());
    }
}

fn apply_retained_effect(retained: &mut HashMap<String, StoredPublish>, publish: &StoredPublish) {
    if publish.is_retained() {
        if publish.payload_is_empty() {
            retained.remove(&publish.topic_string());
        } else {
            retained.insert(publish.topic_string(), publish.clone());
        }
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }

    pub fn fail_on_write(&self, write_number: usize) {
        self.write_attempts.store(0, Ordering::SeqCst);
        self.fail_on_write.store(write_number, Ordering::SeqCst);
    }

    fn check_write(&self) -> Result<(), StorageError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(StorageError::new("injected storage write failure"));
        }
        let target = self.fail_on_write.load(Ordering::SeqCst);
        if target != 0 {
            let attempt = self.write_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == target {
                self.fail_on_write.store(0, Ordering::SeqCst);
                return Err(StorageError::new("injected storage write failure"));
            }
        }
        Ok(())
    }

    fn prepare_inbound_qos2(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        _now_ms: u64,
    ) -> Result<InboundQos2PrepareResult, StorageError> {
        if publish.qos_level() != 2 || publish.packet_id() == 0 || client_id.is_empty() {
            return Err(StorageError::new(
                "inbound QoS2 journal entry has invalid identity",
            ));
        }
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        let key = (client_id.to_owned(), publish.packet_id());
        if let Some(existing) = state.inbound_qos2_journal.get(&key) {
            if !publish.is_duplicate() && existing.state == InboundQos2JournalState::Completed {
                state.inbound_qos2_journal.insert(
                    key,
                    InboundQos2Record {
                        publish: publish.clone(),
                        state: InboundQos2JournalState::Pending,
                    },
                );
                state
                    .inbound
                    .insert((client_id.to_owned(), publish.packet_id()), publish.clone());
                return Ok(InboundQos2PrepareResult::NewPending {
                    publish: publish.clone(),
                });
            }
            if !same_inbound_qos2_identity(&existing.publish, publish) {
                return Err(StorageError::new(
                    "mismatched inbound QoS2 duplicate for client and packet ID",
                ));
            }
            if publish.is_duplicate() {
                return Ok(match existing.state {
                    InboundQos2JournalState::Pending => InboundQos2PrepareResult::ExistingPending {
                        publish: existing.publish.clone(),
                    },
                    InboundQos2JournalState::Committed => {
                        InboundQos2PrepareResult::ExistingCommitted {
                            publish: existing.publish.clone(),
                        }
                    }
                    InboundQos2JournalState::Completed => {
                        InboundQos2PrepareResult::ExistingCompleted {
                            publish: existing.publish.clone(),
                        }
                    }
                });
            }
            return Err(StorageError::new("inbound QoS2 packet ID is still active"));
        }
        state.inbound_qos2_journal.insert(
            key,
            InboundQos2Record {
                publish: publish.clone(),
                state: InboundQos2JournalState::Pending,
            },
        );
        state
            .inbound
            .insert((client_id.to_owned(), publish.packet_id()), publish.clone());
        Ok(InboundQos2PrepareResult::NewPending {
            publish: publish.clone(),
        })
    }

    fn complete_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<InboundQos2CompletionResult, StorageError> {
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        let Some(record) = state
            .inbound_qos2_journal
            .get_mut(&(client_id.to_owned(), packet_id))
        else {
            return Err(StorageError::new(
                "inbound QoS2 journal entry is missing before completion",
            ));
        };
        match record.state {
            InboundQos2JournalState::Pending => Err(StorageError::new(
                "inbound QoS2 journal entry is pending before completion",
            )),
            InboundQos2JournalState::Committed => {
                record.state = InboundQos2JournalState::Completed;
                Ok(InboundQos2CompletionResult::Completed {
                    publish: record.publish.clone(),
                })
            }
            InboundQos2JournalState::Completed => {
                Ok(InboundQos2CompletionResult::ExistingCompleted {
                    publish: record.publish.clone(),
                })
            }
        }
    }

    fn commit_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
        _now_ms: u64,
    ) -> Result<InboundQos2CommitResult, StorageError> {
        if packet_id == 0 || client_id.is_empty() {
            return Err(StorageError::new(
                "inbound QoS2 journal entry has invalid identity",
            ));
        }
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        let Some(record) = state
            .inbound_qos2_journal
            .get_mut(&(client_id.to_owned(), packet_id))
        else {
            return Err(StorageError::new(
                "inbound QoS2 journal entry is missing before commit",
            ));
        };
        match record.state {
            InboundQos2JournalState::Pending => {
                record.state = InboundQos2JournalState::Committed;
                let publish = record.publish.clone();
                apply_retained_effect(&mut state.retained, &publish);
                Ok(InboundQos2CommitResult::AppendRequired { publish })
            }
            InboundQos2JournalState::Committed => Ok(InboundQos2CommitResult::ExistingCommitted {
                publish: record.publish.clone(),
            }),
            InboundQos2JournalState::Completed => Ok(InboundQos2CommitResult::ExistingCompleted {
                publish: record.publish.clone(),
            }),
        }
    }
}

impl BrokerStorage for MemoryStorage {
    fn load(&self, now_ms: u64) -> Result<BrokerStorageState, StorageError> {
        let _ = now_ms;
        let state = self.state.lock().unwrap();
        Ok(BrokerStorageState {
            retained: state.retained.clone(),
            sessions: state.sessions.values().cloned().collect(),
            inbound_qos2: state
                .inbound_qos2_journal
                .iter()
                .filter(|(_, record)| record.state == InboundQos2JournalState::Committed)
                .map(|((client_id, packet_id), record)| InboundQos2JournalEntry {
                    client_id: client_id.clone(),
                    packet_id: *packet_id,
                    publish: record.publish.clone(),
                    state: record.state,
                })
                .collect(),
        })
    }

    fn save_retained(
        &self,
        topic: &str,
        publish: &StoredPublish,
        _now_ms: u64,
    ) -> Result<(), StorageError> {
        self.check_write()?;
        self.state
            .lock()
            .unwrap()
            .retained
            .insert(topic.to_owned(), publish.clone());
        Ok(())
    }

    fn delete_retained(&self, topic: &str) -> Result<(), StorageError> {
        self.check_write()?;
        self.state.lock().unwrap().retained.remove(topic);
        Ok(())
    }

    fn commit_inbound(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        if publish.qos_level() == 2 {
            return MemoryStorage::prepare_inbound_qos2(self, client_id, publish, now_ms)
                .map(|_| ());
        }
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        apply_inbound_effects(&mut state, client_id, publish);
        Ok(())
    }

    fn load_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<StoredPublish>, StorageError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .inbound
            .get(&(client_id.to_owned(), packet_id))
            .filter(|publish| publish.qos_level() == 2)
            .cloned())
    }

    fn complete_inbound(&self, client_id: &str, packet_id: u16) -> Result<(), StorageError> {
        self.check_write()?;
        self.state
            .lock()
            .unwrap()
            .inbound
            .remove(&(client_id.to_owned(), packet_id));
        Ok(())
    }

    fn prepare_inbound_qos2(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
    ) -> Result<InboundQos2PrepareResult, StorageError> {
        MemoryStorage::prepare_inbound_qos2(self, client_id, publish, now_ms)
    }

    fn complete_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<InboundQos2CompletionResult, StorageError> {
        MemoryStorage::complete_inbound_qos2(self, client_id, packet_id)
    }

    fn commit_inbound_qos2(
        &self,
        client_id: &str,
        packet_id: u16,
        now_ms: u64,
    ) -> Result<InboundQos2CommitResult, StorageError> {
        MemoryStorage::commit_inbound_qos2(self, client_id, packet_id, now_ms)
    }

    fn save_session(&self, session: &StoredSession, _now_ms: u64) -> Result<(), StorageError> {
        self.check_write()?;
        self.state
            .lock()
            .unwrap()
            .sessions
            .insert(session.client_id.clone(), session.clone());
        Ok(())
    }

    fn delete_session(&self, client_id: &str) -> Result<(), StorageError> {
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        state.sessions.remove(client_id);
        state.offline.remove(client_id);
        state
            .inbound
            .retain(|(stored_client_id, _), _| stored_client_id != client_id);
        state
            .inbound_qos2_journal
            .retain(|(stored_client_id, _), _| stored_client_id != client_id);
        Ok(())
    }

    fn enqueue_offline(
        &self,
        client_id: &str,
        publish: &StoredPublish,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<(), StorageError> {
        self.check_write()?;
        if now_ms.saturating_sub(publish.stored_at_ms) >= policy.offline_ttl_ms
            || publish.message_expired(now_ms)
        {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        let duplicate = state.offline.get(client_id).is_some_and(|queue| {
            queue
                .iter()
                .any(|record| record.publish.has_same_offline_identity(publish))
        });
        if duplicate {
            return Ok(());
        }
        let lease_id = state.next_lease_id;
        state.next_lease_id += 1;
        let queue = state.offline.entry(client_id.to_owned()).or_default();
        queue.push(OfflineRecord {
            lease_id,
            leased_until_ms: None,
            publish: publish.clone(),
        });
        if queue.len() > policy.max_offline_messages {
            let excess = queue.len() - policy.max_offline_messages;
            queue.drain(..excess);
        }
        Ok(())
    }

    fn lease_offline(
        &self,
        client_id: &str,
        now_ms: u64,
        policy: RetentionPolicy,
    ) -> Result<Vec<LeasedOffline>, StorageError> {
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        let Some(queue) = state.offline.get_mut(client_id) else {
            return Ok(Vec::new());
        };
        Ok(queue
            .iter_mut()
            .filter(|record| {
                now_ms.saturating_sub(record.publish.stored_at_ms) < policy.offline_ttl_ms
                    && !record.publish.message_expired(now_ms)
                    && record.leased_until_ms.is_none_or(|until| until <= now_ms)
            })
            .map(|record| {
                record.leased_until_ms = Some(now_ms.saturating_add(policy.offline_lease_ms));
                LeasedOffline {
                    lease_id: record.lease_id,
                    publish: record.publish.clone(),
                }
            })
            .collect())
    }

    fn acknowledge_offline(&self, client_id: &str, lease_id: u64) -> Result<(), StorageError> {
        self.check_write()?;
        if let Some(queue) = self.state.lock().unwrap().offline.get_mut(client_id) {
            queue.retain(|record| record.lease_id != lease_id);
        }
        Ok(())
    }

    fn prune(&self, now_ms: u64, policy: RetentionPolicy) -> Result<(), StorageError> {
        self.check_write()?;
        let mut state = self.state.lock().unwrap();
        state
            .retained
            .retain(|_, value| now_ms.saturating_sub(value.stored_at_ms) < policy.retained_ttl_ms);
        state
            .sessions
            .retain(|_, value| now_ms.saturating_sub(value.stored_at_ms) < policy.session_ttl_ms);
        let session_ids = state
            .sessions
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        state.offline.retain(|client_id, queue| {
            queue.retain(|record| {
                now_ms.saturating_sub(record.publish.stored_at_ms) < policy.offline_ttl_ms
                    && !record.publish.message_expired(now_ms)
            });
            session_ids.contains(client_id) && !queue.is_empty()
        });
        state
            .inbound
            .retain(|_, value| now_ms.saturating_sub(value.stored_at_ms) < policy.session_ttl_ms);
        state.inbound_qos2_journal.retain(|_, value| {
            now_ms.saturating_sub(value.publish.stored_at_ms) < policy.session_ttl_ms
        });
        Ok(())
    }
}

fn same_inbound_qos2_identity(left: &StoredPublish, right: &StoredPublish) -> bool {
    let mut left_publish = left.publish.clone();
    let mut right_publish = right.publish.clone();
    left_publish.dup = false;
    right_publish.dup = false;
    left_publish == right_publish && left.properties == right.properties
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Publish;
    use bytes::Bytes;

    fn retained(payload: &[u8], stored_at_ms: u64) -> StoredPublish {
        StoredPublish {
            publish: Publish::new(
                Bytes::from_static(b"state/topic"),
                Bytes::copy_from_slice(payload),
                true,
            ),
            properties: None,
            stored_at_ms,
        }
    }

    #[test]
    fn memory_storage_prunes_retained_sessions_and_offline_messages_by_supplied_clock() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            retained_ttl_ms: 10,
            session_ttl_ms: 10,
            offline_ttl_ms: 10,
            max_offline_messages: 10,
            offline_lease_ms: 1,
            prune_interval_ms: 1,
        };
        let session = StoredSession {
            client_id: "client".into(),
            tracker: Tracker::new("client".into()),
            subscriptions: vec!["state/#".into()],
            unacked_pubrels: vec![],
            inflight: vec![],
            qos2_leases: vec![],
            outbound_qos2: vec![],
            qos2_publishes: vec![],
            metrics: ConnectionEvents::default(),
            stored_at_ms: 0,
        };

        store
            .save_retained("state/topic", &retained(b"old", 0), 0)
            .unwrap();
        store.save_session(&session, 0).unwrap();
        store
            .enqueue_offline("client", &retained(b"old", 0), 0, policy)
            .unwrap();
        store.prune(11, policy).unwrap();

        let state = store.load(11).unwrap();
        assert!(state.retained.is_empty());
        assert!(state.sessions.is_empty());
        assert!(store
            .lease_offline("client", 11, policy)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn offline_leases_redeliver_only_after_expiry_and_delete_on_acknowledgement() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            retained_ttl_ms: 100,
            session_ttl_ms: 100,
            offline_ttl_ms: 100,
            max_offline_messages: 10,
            offline_lease_ms: 10,
            prune_interval_ms: 1,
        };
        store
            .enqueue_offline("client", &retained(b"payload", 0), 0, policy)
            .unwrap();

        let first = store.lease_offline("client", 0, policy).unwrap();
        assert_eq!(first.len(), 1);
        assert!(store.lease_offline("client", 1, policy).unwrap().is_empty());
        let redelivery = store.lease_offline("client", 10, policy).unwrap();
        assert_eq!(redelivery.len(), 1);
        assert_eq!(redelivery[0].lease_id, first[0].lease_id);
        store
            .acknowledge_offline("client", redelivery[0].lease_id)
            .unwrap();
        assert!(store
            .lease_offline("client", 20, policy)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn offline_dedup_ignores_stored_time_and_dup() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            offline_ttl_ms: 100,
            offline_lease_ms: 10,
            ..RetentionPolicy::default()
        };
        let mut first = retained(b"payload", 1);
        first.publish.retain = false;
        first.publish.qos = crate::protocol::QoS::AtLeastOnce;
        first.publish.pkid = 7;
        let mut retransmit = first.clone();
        retransmit.stored_at_ms = 2;
        retransmit.publish.dup = true;

        store.enqueue_offline("client", &first, 1, policy).unwrap();
        store
            .enqueue_offline("client", &retransmit, 2, policy)
            .unwrap();

        assert_eq!(store.lease_offline("client", 2, policy).unwrap().len(), 1);
    }

    #[test]
    fn offline_enqueue_and_prune_drop_mqtt_expired_messages() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            offline_ttl_ms: 10_000,
            offline_lease_ms: 10,
            ..RetentionPolicy::default()
        };
        let mut expired = retained(b"expired", 0);
        expired.publish.retain = false;
        expired.publish.qos = crate::protocol::QoS::AtLeastOnce;
        expired.properties = Some(crate::protocol::PublishProperties {
            message_expiry_interval: Some(1),
            ..Default::default()
        });
        store
            .enqueue_offline("client", &expired, 1_000, policy)
            .unwrap();
        assert!(store
            .lease_offline("client", 1_000, policy)
            .unwrap()
            .is_empty());

        store
            .enqueue_offline("client", &expired, 0, policy)
            .unwrap();
        store.prune(1_000, policy).unwrap();
        assert!(store
            .lease_offline("client", 1_000, policy)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn memory_inbound_qos2_prepare_returns_existing_without_overwrite() {
        let store = MemoryStorage::new();
        let original = qos2_publish(b"original", 10, 7);
        let mut duplicate = original.clone();
        duplicate.publish.dup = true;
        duplicate.stored_at_ms = 20;

        assert!(matches!(
            store.prepare_inbound_qos2("client", &original, 10).unwrap(),
            InboundQos2PrepareResult::NewPending { .. }
        ));
        assert_eq!(
            store
                .prepare_inbound_qos2("client", &duplicate, 20)
                .unwrap(),
            InboundQos2PrepareResult::ExistingPending {
                publish: original.clone()
            }
        );
    }

    #[test]
    fn memory_inbound_qos2_rejects_mismatched_duplicate() {
        let store = MemoryStorage::new();
        let original = qos2_publish(b"original", 10, 7);
        let mismatch = qos2_publish(b"different", 20, 7);

        store.prepare_inbound_qos2("client", &original, 10).unwrap();
        let error = store
            .prepare_inbound_qos2("client", &mismatch, 20)
            .unwrap_err();

        assert!(error.message.contains("mismatched inbound QoS2 duplicate"));
    }

    #[test]
    fn memory_inbound_qos2_completion_is_idempotent_and_prune_removes_expired_rows() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            session_ttl_ms: 10,
            ..RetentionPolicy::default()
        };
        let publish = qos2_publish(b"payload", 0, 7);

        store.prepare_inbound_qos2("client", &publish, 0).unwrap();
        assert!(matches!(
            store.commit_inbound_qos2("client", 7, 0).unwrap(),
            InboundQos2CommitResult::AppendRequired { .. }
        ));
        store.complete_inbound_qos2("client", 7).unwrap();
        store.complete_inbound_qos2("client", 7).unwrap();
        assert_eq!(
            store.commit_inbound_qos2("client", 7, 0).unwrap(),
            InboundQos2CommitResult::ExistingCompleted {
                publish: publish.clone()
            }
        );

        store.prune(11, policy).unwrap();
        assert!(matches!(
            store.prepare_inbound_qos2("client", &publish, 11).unwrap(),
            InboundQos2PrepareResult::NewPending { .. }
        ));
    }

    #[test]
    fn memory_inbound_qos2_state_machine_reuses_completed_packet_ids_at_ttl_boundary() {
        let store = MemoryStorage::new();
        let policy = RetentionPolicy {
            session_ttl_ms: 10,
            ..RetentionPolicy::default()
        };
        let first = qos2_publish(b"first", 0, 7);
        let mut duplicate = first.clone();
        duplicate.publish.dup = true;
        duplicate.stored_at_ms = 1;

        assert_eq!(
            store.prepare_inbound_qos2("client", &first, 0).unwrap(),
            InboundQos2PrepareResult::NewPending {
                publish: first.clone()
            }
        );
        assert_eq!(
            store.prepare_inbound_qos2("client", &duplicate, 1).unwrap(),
            InboundQos2PrepareResult::ExistingPending {
                publish: first.clone()
            }
        );
        assert_eq!(
            store.commit_inbound_qos2("client", 7, 1).unwrap(),
            InboundQos2CommitResult::AppendRequired {
                publish: first.clone()
            }
        );
        assert_eq!(
            store.complete_inbound_qos2("client", 7).unwrap(),
            InboundQos2CompletionResult::Completed {
                publish: first.clone()
            }
        );
        assert_eq!(
            store.prepare_inbound_qos2("client", &duplicate, 1).unwrap(),
            InboundQos2PrepareResult::ExistingCompleted {
                publish: first.clone()
            }
        );

        let second = qos2_publish(b"second", 0, 7);
        assert_eq!(
            store.prepare_inbound_qos2("client", &second, 1).unwrap(),
            InboundQos2PrepareResult::NewPending {
                publish: second.clone()
            }
        );
        assert_eq!(
            store.commit_inbound_qos2("client", 7, 1).unwrap(),
            InboundQos2CommitResult::AppendRequired {
                publish: second.clone()
            }
        );
        store.complete_inbound_qos2("client", 7).unwrap();

        store.prune(10, policy).unwrap();
        assert!(matches!(
            store.prepare_inbound_qos2("client", &second, 10).unwrap(),
            InboundQos2PrepareResult::NewPending { .. }
        ));
    }

    #[test]
    fn memory_pending_inbound_qos2_does_not_mutate_retained_state_before_commit() {
        let store = MemoryStorage::new();
        let mut publish = qos2_publish(b"retained", 0, 7);
        publish.publish.retain = true;

        store.prepare_inbound_qos2("client", &publish, 0).unwrap();
        assert!(store.load(0).unwrap().retained.is_empty());

        store.commit_inbound_qos2("client", 7, 0).unwrap();
        assert_eq!(
            store.load(0).unwrap().retained.get("state/topic").cloned(),
            Some(publish)
        );
    }

    fn qos2_publish(payload: &'static [u8], stored_at_ms: u64, packet_id: u16) -> StoredPublish {
        let mut publish = retained(payload, stored_at_ms);
        publish.publish.retain = false;
        publish.publish.qos = crate::protocol::QoS::ExactlyOnce;
        publish.publish.pkid = packet_id;
        publish
    }
}
