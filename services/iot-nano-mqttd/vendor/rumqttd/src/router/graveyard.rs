use std::collections::{HashMap, HashSet, VecDeque};

use super::{
    scheduler::{PauseReason, Tracker},
    ConnectionEvents,
};
use crate::StoredSession;

pub struct Graveyard {
    connections: HashMap<String, SavedState>,
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connections: HashMap::new(),
        }
    }

    /// Add a new connection.
    /// Return tracker of previous connection if connection id already exists
    pub fn retrieve(&mut self, id: &str) -> Option<SavedState> {
        self.connections.remove(id)
    }

    pub fn has_inflight_offline_lease(&self, id: &str) -> bool {
        self.connections.get(id).is_some_and(|saved| {
            saved.session_state.as_ref().is_some_and(|session| {
                session
                    .inflight
                    .iter()
                    .any(|inflight| inflight.offline_lease_id.is_some())
                    || session
                        .outbound_qos2
                        .iter()
                        .any(|phase| phase.offline_lease_id.is_some())
            })
        })
    }

    /// Save connection tracker
    pub fn save_state(
        &mut self,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        metrics: ConnectionEvents,
        unacked_pubrels: VecDeque<u16>,
        inflight: Vec<crate::StoredInflight>,
        qos2_leases: Vec<(u16, u64)>,
        outbound_qos2: Vec<crate::OutboundQos2Phase>,
        qos2_publishes: Vec<crate::StoredPublish>,
    ) {
        tracker.pause(PauseReason::Busy);
        let id = tracker.id.clone();

        let session_state = SessionState {
            tracker,
            subscriptions,
            unacked_pubrels,
            inflight,
            qos2_leases,
            outbound_qos2,
            qos2_publishes,
        };

        self.connections.insert(
            id,
            SavedState {
                session_state: Some(session_state),
                metrics,
            },
        );
    }

    pub fn restore(&mut self, session: StoredSession) {
        let mut tracker = session.tracker;
        tracker.pause(PauseReason::Busy);
        self.connections.insert(
            session.client_id,
            SavedState {
                session_state: Some(SessionState {
                    tracker,
                    subscriptions: session.subscriptions.into_iter().collect(),
                    unacked_pubrels: session.unacked_pubrels.into_iter().collect(),
                    inflight: session.inflight,
                    qos2_leases: session.qos2_leases,
                    outbound_qos2: session.outbound_qos2,
                    qos2_publishes: session.qos2_publishes,
                }),
                metrics: session.metrics,
            },
        );
    }

    pub fn offline_subscriptions(&self) -> Vec<(String, Vec<(String, u8)>)> {
        self.connections
            .iter()
            .filter_map(|(client_id, saved)| {
                saved.session_state.as_ref().map(|session| {
                    (
                        client_id.clone(),
                        session
                            .tracker
                            .data_requests
                            .iter()
                            .map(|request| (request.filter.clone(), request.qos))
                            .collect(),
                    )
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub session_state: Option<SessionState>,
    pub metrics: ConnectionEvents,
}

#[derive(Debug)]
pub struct SessionState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    // used for pubrel in qos2
    pub unacked_pubrels: VecDeque<u16>,
    pub inflight: Vec<crate::StoredInflight>,
    pub qos2_leases: Vec<(u16, u64)>,
    pub outbound_qos2: Vec<crate::OutboundQos2Phase>,
    pub qos2_publishes: Vec<crate::StoredPublish>,
}
