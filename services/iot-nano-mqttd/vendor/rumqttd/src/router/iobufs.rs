use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use flume::{Receiver, Sender};
use parking_lot::Mutex;
use tracing::warn;

use crate::{
    protocol::Packet,
    router::{FilterIdx, MAX_CHANNEL_CAPACITY},
    Cursor, Notification, StoredInflight,
};

use super::{Forward, IncomingMeter, OutgoingMeter};

const MAX_INFLIGHT: usize = 100;
const MAX_PKID: u16 = MAX_INFLIGHT as u16;
const MAX_COMPLETED_PUBCOMPS: usize = MAX_INFLIGHT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PubCompRegistration {
    Completed,
    Duplicate,
    Unknown,
}

#[derive(Debug)]
pub struct Incoming {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    /// Recv buffer
    pub(crate) buffer: Arc<Mutex<VecDeque<Packet>>>,
    /// incoming metrics
    pub(crate) meter: IncomingMeter,
}

impl Incoming {
    #[inline]
    pub(crate) fn new(client_id: String) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CHANNEL_CAPACITY))),
            meter: Default::default(),
            client_id,
        }
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Packet>>> {
        self.buffer.clone()
    }

    #[inline]
    pub(crate) fn exchange(&mut self, mut v: VecDeque<Packet>) -> VecDeque<Packet> {
        std::mem::swap(&mut v, &mut self.buffer.lock());
        v
    }
}

#[derive(Debug)]
pub struct Outgoing {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    /// Send buffer
    pub(crate) data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    /// Handle which is given to router to allow router to communicate with this connection
    pub(crate) handle: Sender<()>,
    /// The buffer to keep track of inflight packets.
    inflight_buffer: VecDeque<InflightEntry>,
    /// PubRels waiting for PubComp
    pub(crate) unacked_pubrels: VecDeque<u16>,
    completed_pubcomps: VecDeque<u16>,
    offline_qos2_leases: HashMap<u16, u64>,
    outbound_qos2_stored_at: HashMap<u16, u64>,
    /// Last packet id
    last_pkid: u16,
    /// Metrics of outgoing messages of this connection
    pub(crate) meter: OutgoingMeter,
}

#[derive(Debug, Clone)]
struct InflightEntry {
    pkid: u16,
    filter_idx: FilterIdx,
    cursor: Option<Cursor>,
    publish: crate::protocol::Publish,
    properties: Option<crate::protocol::PublishProperties>,
    stored_at_ms: u64,
    filter: Option<String>,
    offline_lease_id: Option<u64>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

impl Outgoing {
    #[inline]
    pub(crate) fn new(client_id: String) -> (Self, Receiver<()>) {
        let (handle, rx) = flume::bounded(MAX_CHANNEL_CAPACITY);
        let data_buffer = VecDeque::with_capacity(MAX_CHANNEL_CAPACITY);
        let inflight_buffer = VecDeque::with_capacity(MAX_INFLIGHT);
        let unacked_pubrels = VecDeque::with_capacity(MAX_INFLIGHT);
        let completed_pubcomps = VecDeque::with_capacity(MAX_COMPLETED_PUBCOMPS);

        // Ensure that there won't be any new allocations
        assert!(MAX_INFLIGHT <= inflight_buffer.capacity());
        assert!(MAX_CHANNEL_CAPACITY <= data_buffer.capacity());

        let outgoing = Self {
            client_id,
            data_buffer: Arc::new(Mutex::new(data_buffer)),
            inflight_buffer,
            unacked_pubrels,
            completed_pubcomps,
            offline_qos2_leases: HashMap::new(),
            outbound_qos2_stored_at: HashMap::new(),
            handle,
            last_pkid: 0,
            meter: Default::default(),
        };

        (outgoing, rx)
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Notification>>> {
        self.data_buffer.clone()
    }

    pub fn free_slots(&self) -> usize {
        MAX_INFLIGHT - self.inflight_buffer.len()
    }

    pub fn push_notification(&mut self, notification: Notification) -> usize {
        let mut buffer = self.data_buffer.lock();
        buffer.push_back(notification);
        buffer.len()
    }

    /// Push packets to the outgoing buffer.
    pub fn push_forwards(
        &mut self,
        publishes: impl Iterator<Item = Forward>,
        qos: u8,
        filter_idx: usize,
        filter: Option<String>,
    ) -> (usize, usize) {
        let mut buffer = self.data_buffer.lock();
        let publishes = publishes;

        if qos == 0 {
            for p in publishes {
                self.meter.publish_count += 1;
                buffer.push_back(Notification::Forward(p));
                // self.meter.total_size += p.len();
            }

            // self.meter.update_data_rate(total_size);
            let buffer_count = buffer.len();
            let inflight_count = self.inflight_buffer.len();
            return (buffer_count, inflight_count);
        }

        for mut p in publishes {
            // Index and pkid of current outgoing packet
            self.last_pkid += 1;
            p.publish.pkid = self.last_pkid;

            self.inflight_buffer.push_back(InflightEntry {
                pkid: self.last_pkid,
                filter_idx,
                cursor: p.cursor,
                publish: p.publish.clone(),
                properties: p.properties.clone(),
                stored_at_ms: now_ms(),
                filter: filter.clone(),
                offline_lease_id: None,
            });

            // Place max pkid packet at index 0
            if self.last_pkid == MAX_PKID {
                self.last_pkid = 0;
            }

            self.meter.publish_count += 1;
            self.meter.total_size += p.publish.topic.len() + p.publish.payload.len();
            buffer.push_back(Notification::Forward(p));
        }

        let buffer_count = buffer.len();
        let inflight_count = self.inflight_buffer.len();

        if inflight_count > MAX_INFLIGHT {
            warn!(
                "More inflight publishes than max allowed, inflight count = {}, max allowed = {}",
                inflight_count, MAX_INFLIGHT
            );
        }

        (buffer_count, inflight_count)
    }

    // Returns (unsolicited, outoforder) flags
    // Return: Out of order or unsolicited acks
    pub fn register_ack(&mut self, pkid: u16) -> Option<(Option<u64>, u64)> {
        self.inflight_buffer
            .iter()
            .position(|entry| entry.pkid == pkid)
            .and_then(|index| self.inflight_buffer.remove(index))
            .map(|entry| (entry.offline_lease_id, entry.stored_at_ms))
    }

    pub fn register_pubrec(&mut self, pkid: u16, stored_at_ms: u64) {
        self.completed_pubcomps
            .retain(|completed| *completed != pkid);
        if !self.unacked_pubrels.contains(&pkid) {
            self.unacked_pubrels.push_back(pkid);
            self.outbound_qos2_stored_at.insert(pkid, stored_at_ms);
        }
    }

    pub fn has_unacked_pubrel(&self, pkid: u16) -> bool {
        self.unacked_pubrels.contains(&pkid)
    }

    pub fn remember_qos2_lease(&mut self, pkid: u16, lease_id: u64) {
        self.offline_qos2_leases.insert(pkid, lease_id);
    }

    pub fn take_qos2_lease(&mut self, pkid: u16) -> Option<u64> {
        self.offline_qos2_leases.remove(&pkid)
    }

    pub(crate) fn snapshot_qos2_leases(&self) -> Vec<(u16, u64)> {
        self.offline_qos2_leases
            .iter()
            .map(|(pkid, lease_id)| (*pkid, *lease_id))
            .collect()
    }

    pub(crate) fn snapshot_outbound_qos2(&self) -> Vec<crate::OutboundQos2Phase> {
        self.unacked_pubrels
            .iter()
            .map(|packet_id| crate::OutboundQos2Phase {
                packet_id: *packet_id,
                offline_lease_id: self.offline_qos2_leases.get(packet_id).copied(),
                stored_at_ms: self
                    .outbound_qos2_stored_at
                    .get(packet_id)
                    .copied()
                    .unwrap_or_else(now_ms),
            })
            .collect()
    }

    pub(crate) fn restore_outbound_qos2(&mut self, phases: Vec<crate::OutboundQos2Phase>) {
        self.unacked_pubrels = phases.iter().map(|phase| phase.packet_id).collect();
        self.outbound_qos2_stored_at.extend(
            phases
                .iter()
                .map(|phase| (phase.packet_id, phase.stored_at_ms)),
        );
        self.offline_qos2_leases.extend(
            phases
                .into_iter()
                .filter_map(|phase| phase.offline_lease_id.map(|lease| (phase.packet_id, lease))),
        );
    }

    pub(crate) fn restore_qos2_leases(&mut self, leases: Vec<(u16, u64)>) {
        self.offline_qos2_leases.extend(leases);
    }

    pub(crate) fn register_pubcomp(&mut self, pkid: u16) -> PubCompRegistration {
        let completed = self
            .unacked_pubrels
            .iter()
            .position(|id| *id == pkid)
            .and_then(|index| self.unacked_pubrels.remove(index))
            .map(|_| {
                self.outbound_qos2_stored_at.remove(&pkid);
            });
        if completed.is_some() {
            if self.completed_pubcomps.len() == MAX_COMPLETED_PUBCOMPS {
                self.completed_pubcomps.pop_front();
            }
            self.completed_pubcomps.push_back(pkid);
            PubCompRegistration::Completed
        } else if self.completed_pubcomps.contains(&pkid) {
            PubCompRegistration::Duplicate
        } else {
            PubCompRegistration::Unknown
        }
    }

    // Here we are assuming that the first unique filter_idx we find while iterating will have the
    // least corresponding cursor because of the way we insert into the inflight_buffer
    pub fn retransmission_map(&self) -> HashMap<FilterIdx, Cursor> {
        let mut o = HashMap::new();
        for entry in self.inflight_buffer.iter() {
            // if cursor in None, it means it was a retained publish
            if !o.contains_key(&entry.filter_idx) && entry.cursor.is_some() {
                o.insert(entry.filter_idx, entry.cursor.unwrap());
            }
        }

        o
    }

    pub(crate) fn snapshot_inflight(&self) -> Vec<StoredInflight> {
        self.inflight_buffer
            .iter()
            .map(|entry| StoredInflight {
                publish: entry.publish.clone(),
                properties: entry.properties.clone(),
                stored_at_ms: entry.stored_at_ms,
                pkid: entry.pkid,
                cursor: entry.cursor,
                filter_idx: entry.filter_idx,
                filter: entry.filter.clone(),
                offline_lease_id: entry.offline_lease_id,
            })
            .collect()
    }

    pub(crate) fn restore_inflight(&mut self, entries: Vec<StoredInflight>) {
        let mut buffer = self.data_buffer.lock();
        let now = now_ms();
        for entry in entries {
            let stored = crate::StoredPublish {
                publish: entry.publish.clone(),
                properties: entry.properties.clone(),
                stored_at_ms: entry.stored_at_ms,
            };
            if stored.message_expired(now) {
                continue;
            }
            self.last_pkid = entry.pkid;
            let mut publish = entry.publish;
            publish.dup = true;
            self.inflight_buffer.push_back(InflightEntry {
                pkid: entry.pkid,
                filter_idx: entry.filter_idx,
                cursor: entry.cursor,
                publish: publish.clone(),
                properties: entry.properties.clone(),
                stored_at_ms: entry.stored_at_ms,
                filter: entry.filter,
                offline_lease_id: entry.offline_lease_id,
            });
            buffer.push_back(Notification::Forward(crate::router::Forward {
                cursor: entry.cursor,
                size: 0,
                publish,
                properties: stored.properties_for_delivery(now),
            }));
        }
        self.handle.try_send(()).ok();
    }

    pub(crate) fn push_offline(&mut self, publishes: Vec<crate::LeasedOffline>) {
        for leased in publishes {
            let mut stored = leased.publish;
            let delivery_properties = stored.properties_for_delivery(now_ms());
            if stored.publish.qos == crate::protocol::QoS::AtMostOnce {
                self.data_buffer
                    .lock()
                    .push_back(Notification::Forward(crate::router::Forward {
                        cursor: None,
                        size: 0,
                        publish: stored.publish,
                        properties: delivery_properties,
                    }));
            } else {
                let qos = stored.publish.qos as u8;
                self.last_pkid += 1;
                stored.publish.pkid = self.last_pkid;
                self.inflight_buffer.push_back(InflightEntry {
                    pkid: self.last_pkid,
                    filter_idx: 0,
                    cursor: None,
                    publish: stored.publish.clone(),
                    properties: stored.properties,
                    stored_at_ms: stored.stored_at_ms,
                    filter: None,
                    offline_lease_id: Some(leased.lease_id),
                });
                self.meter.publish_count += 1;
                self.meter.total_size += stored.publish.topic.len() + stored.publish.payload.len();
                self.data_buffer
                    .lock()
                    .push_back(Notification::Forward(crate::router::Forward {
                        cursor: None,
                        size: 0,
                        publish: stored.publish,
                        properties: delivery_properties,
                    }));
                if self.last_pkid == MAX_PKID {
                    self.last_pkid = 0;
                }
                debug_assert_eq!(qos, self.inflight_buffer.back().unwrap().publish.qos as u8);
            }
        }
        self.handle.try_send(()).ok();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn retransmission_map_is_calculated_accurately() {
        let (mut outgoing, _) = Outgoing::new("retransmission-test".to_string());
        let mut result = HashMap::new();

        result.insert(0, (0, 8));
        result.insert(1, (0, 1));
        result.insert(2, (1, 1));
        result.insert(3, (1, 0));

        let buf = vec![
            (1, 0, Some((0, 8))),
            (1, 0, Some((0, 10))),
            (1, 1, Some((0, 1))),
            (3, 1, Some((0, 4))),
            (2, 2, Some((1, 1))),
            (1, 2, Some((2, 6))),
            (1, 2, Some((2, 1))),
            (1, 3, Some((1, 0))),
            (1, 3, Some((1, 1))),
            (1, 3, Some((1, 3))),
            (1, 3, Some((1, 3))),
        ]
        .into_iter()
        .map(|(pkid, filter_idx, cursor)| InflightEntry {
            pkid,
            filter_idx,
            cursor,
            publish: crate::protocol::Publish::default(),
            properties: None,
            stored_at_ms: 0,
            filter: None,
            offline_lease_id: None,
        });

        outgoing.inflight_buffer.extend(buf);
        assert_eq!(outgoing.retransmission_map(), result);
    }

    // use super::{Outgoing, MAX_INFLIGHT};
    // use crate::protocol::{Publish, QoS};
    // use crate::router::Forward;
    // use crate::Notification;
    //
    // fn publishes(count: usize) -> impl Iterator<Item = Forward> {
    //     (1..=count).map(|v| {
    //         let publish = Publish {
    //             dup: false,
    //             retain: false,
    //             pkid: 0,
    //             qos: QoS::AtLeastOnce,
    //             topic: "hello/world".into(),
    //             payload: vec![1, 2, 3].into(),
    //         };
    //
    //         Forward {
    //             cursor: (0, v as u64),
    //             publish,
    //             size: 0,
    //         }
    //     })
    // }
    //
    // #[test]
    // fn inflight_ring_buffer_pushes_correctly() {
    //     let count = MAX_INFLIGHT as usize;
    //     let (mut outgoing, _rx) = Outgoing::new("hello".to_owned());
    //     outgoing.push_forwards(publishes(count), 1, 3);
    //
    //     // Index 1 = (0, 1), Index 99 = (0, 99), Index 0 = (0, 100)
    //     assert_eq!(outgoing.inflight_buffer[0].unwrap().1, (0, count as u64));
    //     for i in 1..count {
    //         assert_eq!(outgoing.inflight_buffer[i].unwrap().1, (0, i as u64));
    //     }
    //
    //     // Outgoing publish pkids are as expected
    //     for (i, o) in outgoing.data_buffer.lock().iter().enumerate() {
    //         let pkid = match &o {
    //             Notification::Forward(f) => f.publish.pkid,
    //             _ => unreachable!(),
    //         };
    //
    //         assert_eq!(pkid, i as u16 + 1);
    //     }
    // }
    //
    // #[test]
    // fn inflight_ring_buffer_pops_correctly() {
    //     let (mut outgoing, _rx) = Outgoing::new("hello".to_owned());
    //     outgoing.push_forwards(publishes(MAX_INFLIGHT as usize), 1, 3);
    //
    //     for pkid in 1..=MAX_INFLIGHT {
    //         let (unsolicited, outoforder) = outgoing.register_ack(pkid);
    //         assert_eq!(unsolicited, false);
    //         assert_eq!(outoforder, false);
    //     }
    // }

    #[test]
    fn completed_pubcomp_tombstones_are_bounded_and_allow_packet_id_reuse() {
        let (mut outgoing, _rx) = Outgoing::new("client".to_owned());
        outgoing.register_pubrec(7, 0);
        assert_eq!(outgoing.register_pubcomp(7), PubCompRegistration::Completed);
        assert_eq!(outgoing.register_pubcomp(7), PubCompRegistration::Duplicate);

        outgoing.register_pubrec(7, 1);
        assert_eq!(outgoing.register_pubcomp(7), PubCompRegistration::Completed);

        for packet_id in 8..=(MAX_COMPLETED_PUBCOMPS as u16 + 8) {
            outgoing.register_pubrec(packet_id, 0);
            assert_eq!(
                outgoing.register_pubcomp(packet_id),
                PubCompRegistration::Completed
            );
        }
        assert!(outgoing.completed_pubcomps.len() <= MAX_COMPLETED_PUBCOMPS);
    }
}
