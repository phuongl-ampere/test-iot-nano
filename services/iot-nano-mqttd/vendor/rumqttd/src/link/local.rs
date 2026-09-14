use crate::protocol::{
    Filter, LastWill, LastWillProperties, Packet, Publish, QoS, RetainForwardRule, Subscribe,
    Unsubscribe,
};
use crate::router::Ack;
use crate::router::{
    iobufs::{Incoming, Outgoing},
    Connection, Event, ManagedLinkState, ManagedRouterLink, Notification, ShadowRequest,
};
use crate::ConnectionId;
use bytes::Bytes;
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Unexpected router message")]
    NotConnectionAck,
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel timeout recv error")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
    #[error("an asynchronous local link requires a managed router link")]
    ManagedRouterLinkRequired,
    #[error("a managed router link must use the asynchronous local-link builder")]
    ManagedRouterLinkRequiresAsync,
}

struct LinkSetup {
    router_tx: Sender<(ConnectionId, Event)>,
    event: Event,
    link_rx: Receiver<()>,
    outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    incoming_data_buffer: Arc<Mutex<VecDeque<Packet>>>,
    managed_cleanup: Option<ManagedLinkCleanup>,
}

#[derive(Debug)]
struct ManagedLinkCleanup {
    link_state: Arc<ManagedLinkState>,
    cancellation_tx: UnboundedSender<Arc<ManagedLinkState>>,
}

impl Drop for ManagedLinkCleanup {
    fn drop(&mut self) {
        self.link_state.cancel();
        let _ = self.cancellation_tx.send(self.link_state.clone());
    }
}

// used to build LinkTx and LinkRx
pub struct LinkBuilder<'a> {
    tenant_id: Option<String>,
    client_id: &'a str,
    router_tx: Sender<(ConnectionId, Event)>,
    // true by default
    clean_session: bool,
    last_will: Option<LastWill>,
    last_will_properties: Option<LastWillProperties>,
    // false by default
    dynamic_filters: bool,
    // default to 0, indicating to not use topic alias
    topic_alias_max: u16,
    managed_link: Option<ManagedRouterLink>,
}

impl<'a> LinkBuilder<'a> {
    pub fn new(client_id: &'a str, router_tx: Sender<(ConnectionId, Event)>) -> Self {
        LinkBuilder {
            client_id,
            router_tx,
            tenant_id: None,
            clean_session: true,
            last_will: None,
            last_will_properties: None,
            dynamic_filters: false,
            topic_alias_max: 0,
            managed_link: None,
        }
    }

    /// Builds a local link whose lifecycle is owned by `ManagedRouter`.
    pub fn new_managed(client_id: &'a str, managed_link: ManagedRouterLink) -> Self {
        let router_tx = managed_link.router_tx();
        let mut builder = Self::new(client_id, router_tx);
        builder.managed_link = Some(managed_link);
        builder
    }

    pub fn tenant_id(mut self, tenant_id: Option<String>) -> Self {
        self.tenant_id = tenant_id;
        self
    }

    pub fn last_will(mut self, last_will: Option<LastWill>) -> Self {
        self.last_will = last_will;
        self
    }

    pub fn last_will_properties(
        mut self,
        last_will_properties: Option<LastWillProperties>,
    ) -> Self {
        self.last_will_properties = last_will_properties;
        self
    }

    pub fn topic_alias_max(mut self, max: u16) -> Self {
        self.topic_alias_max = max;
        self
    }

    pub fn clean_session(mut self, clean: bool) -> Self {
        self.clean_session = clean;
        self
    }

    pub fn dynamic_filters(mut self, dynamic_filters: bool) -> Self {
        self.dynamic_filters = dynamic_filters;
        self
    }

    pub fn build(self) -> Result<(LinkTx, LinkRx, Notification), LinkError> {
        if self.managed_link.is_some() {
            return Err(LinkError::ManagedRouterLinkRequiresAsync);
        }
        let LinkSetup {
            router_tx,
            event,
            link_rx,
            outgoing_data_buffer,
            incoming_data_buffer,
            managed_cleanup,
        } = self.prepare(None);
        router_tx.send((0, event))?;
        link_rx.recv()?;

        finish_link(
            router_tx,
            link_rx,
            outgoing_data_buffer,
            incoming_data_buffer,
            managed_cleanup,
        )
    }

    /// Connects to the router without blocking the caller's Tokio worker.
    ///
    /// The returned future is owned by its caller. Its managed cleanup guard
    /// remains with the returned `LinkRx`, so dropping either the pending
    /// future or that receiver reports cancellation to the parent router
    /// without spawning or detaching a task.
    pub async fn build_async(self) -> Result<(LinkTx, LinkRx, Notification), LinkError> {
        let managed_link = self
            .managed_link
            .clone()
            .ok_or(LinkError::ManagedRouterLinkRequired)?;
        let LinkSetup {
            router_tx,
            event,
            link_rx,
            outgoing_data_buffer,
            incoming_data_buffer,
            managed_cleanup,
        } = self.prepare(Some(&managed_link));
        router_tx.send_async((0, event)).await?;
        link_rx.recv_async().await?;

        finish_link(
            router_tx,
            link_rx,
            outgoing_data_buffer,
            incoming_data_buffer,
            managed_cleanup,
        )
    }

    fn prepare(self, managed_link: Option<&ManagedRouterLink>) -> LinkSetup {
        // Connect to router
        // Local connections to the router shall have access to all subscriptions
        let mut connection = Connection::new(
            self.tenant_id,
            self.client_id.to_owned(),
            self.clean_session,
            self.dynamic_filters,
        );

        connection
            .last_will(self.last_will, self.last_will_properties)
            .topic_alias_max(self.topic_alias_max);
        let incoming = Incoming::new(connection.client_id.to_owned());
        let (outgoing, link_rx) = Outgoing::new(connection.client_id.to_owned());
        let outgoing_data_buffer = outgoing.buffer();
        let incoming_data_buffer = incoming.buffer();

        let (event, managed_cleanup) = match managed_link {
            Some(managed_link) => {
                let (link_state, cancellation_tx) = managed_link.new_link_state();
                (
                    Event::ManagedConnect {
                        connection,
                        incoming,
                        outgoing,
                        link_state: link_state.clone(),
                    },
                    Some(ManagedLinkCleanup {
                        link_state,
                        cancellation_tx,
                    }),
                )
            }
            None => (
                Event::Connect {
                    connection,
                    incoming,
                    outgoing,
                },
                None,
            ),
        };

        LinkSetup {
            router_tx: self.router_tx,
            event,
            link_rx,
            outgoing_data_buffer,
            incoming_data_buffer,
            managed_cleanup,
        }
    }
}

fn finish_link(
    router_tx: Sender<(ConnectionId, Event)>,
    link_rx: Receiver<()>,
    outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    incoming_data_buffer: Arc<Mutex<VecDeque<Packet>>>,
    managed_cleanup: Option<ManagedLinkCleanup>,
) -> Result<(LinkTx, LinkRx, Notification), LinkError> {
    let notification = outgoing_data_buffer.lock().pop_front().unwrap();

    // Right now link identifies failure with dropped rx in router,
    // which is probably ok. We need this here to get id assigned by router
    let id = match notification {
        Notification::DeviceAck(Ack::ConnAck(id, ..)) => id,
        _message => return Err(LinkError::NotConnectionAck),
    };

    let tx = LinkTx::new(id, router_tx.clone(), incoming_data_buffer);
    let rx = LinkRx::new_with_managed_cleanup(
        id,
        router_tx,
        link_rx,
        outgoing_data_buffer,
        managed_cleanup,
    );
    Ok((tx, rx, notification))
}

pub struct LinkTx {
    pub(crate) connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    recv_buffer: Arc<Mutex<VecDeque<Packet>>>,
}

impl LinkTx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        recv_buffer: Arc<Mutex<VecDeque<Packet>>>,
    ) -> LinkTx {
        LinkTx {
            connection_id,
            router_tx,
            recv_buffer,
        }
    }

    pub fn buffer(&self) -> MutexGuard<RawMutex, VecDeque<Packet>> {
        self.recv_buffer.lock()
    }

    /// Send raw device data
    fn push(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send((self.connection_id, Event::DeviceData))?;

        Ok(len)
    }

    /// Send raw device data
    pub async fn send(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send((self.connection_id, Event::DeviceData))?;

        Ok(len)
    }

    fn try_push(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .try_send((self.connection_id, Event::DeviceData))?;
        Ok(len)
    }

    pub(crate) async fn notify(&mut self) -> Result<(), LinkError> {
        self.router_tx
            .send_async((self.connection_id, Event::DeviceData))
            .await?;

        Ok(())
    }

    /// Sends a MQTT Publish to the router
    pub fn publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<Bytes>,
        V: Into<Bytes>,
    {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            pkid: 0,
            payload: payload.into(),
        };

        let len = self.push(Packet::Publish(publish, None))?;
        Ok(len)
    }

    /// Sends a MQTT Publish to the router
    pub fn try_publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<Bytes>,
        V: Into<Bytes>,
    {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            pkid: 0,
            payload: payload.into(),
        };

        let len = self.try_push(Packet::Publish(publish, None))?;
        // TODO: Remote item in buffer after failure and write unittest
        Ok(len)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let filters = vec![Filter {
            path: filter.into(),
            qos: QoS::AtMostOnce,
            nolocal: false,
            preserve_retain: false,
            retain_forward_rule: RetainForwardRule::Never,
        }];

        let subscribe = Subscribe { pkid: 0, filters };

        let len = self.push(Packet::Subscribe(subscribe, None))?;
        Ok(len)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let filters = vec![Filter {
            path: filter.into(),
            qos: QoS::AtMostOnce,
            nolocal: false,
            preserve_retain: false,
            retain_forward_rule: RetainForwardRule::Never,
        }];

        let subscribe = Subscribe { pkid: 0, filters };

        let len = self.try_push(Packet::Subscribe(subscribe, None))?;
        Ok(len)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn unsubscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let unsubscribe = Unsubscribe {
            pkid: 0,
            filters: vec![filter.into()],
        };

        let len = self.push(Packet::Unsubscribe(unsubscribe, None))?;
        Ok(len)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let unsubscribe = Unsubscribe {
            pkid: 0,
            filters: vec![filter.into()],
        };

        let len = self.try_push(Packet::Unsubscribe(unsubscribe, None))?;
        Ok(len)
    }

    /// Request to get device shadow
    pub fn shadow<S: Into<String>>(&mut self, filter: S) -> Result<(), LinkError> {
        let message = Event::Shadow(ShadowRequest {
            filter: filter.into(),
        });

        self.router_tx.try_send((self.connection_id, message))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LinkRx {
    connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    router_rx: Receiver<()>,
    send_buffer: Arc<Mutex<VecDeque<Notification>>>,
    cache: VecDeque<Notification>,
    _managed_cleanup: Option<ManagedLinkCleanup>,
}

impl LinkRx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        router_rx: Receiver<()>,
        outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    ) -> LinkRx {
        Self::new_with_managed_cleanup(
            connection_id,
            router_tx,
            router_rx,
            outgoing_data_buffer,
            None,
        )
    }

    fn new_with_managed_cleanup(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        router_rx: Receiver<()>,
        outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
        managed_cleanup: Option<ManagedLinkCleanup>,
    ) -> LinkRx {
        LinkRx {
            connection_id,
            router_tx,
            router_rx,
            send_buffer: outgoing_data_buffer,
            cache: VecDeque::with_capacity(100),
            _managed_cleanup: managed_cleanup,
        }
    }

    pub fn id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn recv(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv()?;
                // Collect 'all' the data in the buffer after a notification.
                // Notification means fresh data which isn't previously collected
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv_deadline(deadline)?;
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv_async().await?;
                // Collect 'all' the data in the buffer after a notification.
                // Notification means fresh data which isn't previously collected
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub(crate) async fn exchange(
        &mut self,
        notifications: &mut VecDeque<Notification>,
    ) -> Result<(), LinkError> {
        self.router_rx.recv_async().await?;
        mem::swap(&mut *self.send_buffer.lock(), notifications);
        Ok(())
    }

    pub fn ready(&self) -> Result<(), LinkError> {
        self.router_tx.send((self.connection_id, Event::Ready))?;
        Ok(())
    }

    pub async fn wake(&self) -> Result<(), LinkError> {
        self.router_tx
            .send_async((self.connection_id, Event::Ready))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{LinkBuilder, LinkError, LinkTx};
    use flume::bounded;
    use parking_lot::Mutex;
    use std::{collections::VecDeque, sync::Arc, thread};

    #[test]
    fn push_sends_all_data_and_notifications_to_router() {
        let (router_tx, router_rx) = bounded(10);
        let mut buffers = Vec::new();
        const CONNECTIONS: usize = 1000;
        const MESSAGES_PER_CONNECTION: usize = 100;

        for i in 0..CONNECTIONS {
            let buffer = Arc::new(Mutex::new(VecDeque::new()));
            let mut link_tx = LinkTx::new(i, router_tx.clone(), buffer.clone());
            buffers.push(buffer);
            thread::spawn(move || {
                for _ in 0..MESSAGES_PER_CONNECTION {
                    link_tx.publish("hello/world", vec![1, 2, 3]).unwrap();
                }
            });
        }

        // Router should receive notifications from all the connections
        for _ in 0..CONNECTIONS * MESSAGES_PER_CONNECTION {
            let _v = router_rx.recv().unwrap();
        }

        // Every connection has expected number of messages
        for item in buffers.iter().take(CONNECTIONS) {
            assert_eq!(item.lock().len(), MESSAGES_PER_CONNECTION);
        }

        // TODO: Write a similar test to benchmark buffer vs channels
    }

    #[tokio::test]
    async fn async_builder_requires_a_managed_router_link() {
        let (router_tx, _router_rx) = bounded(1);

        assert!(matches!(
            LinkBuilder::new("not-managed", router_tx)
                .build_async()
                .await,
            Err(LinkError::ManagedRouterLinkRequired)
        ));
    }
}
