use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::link::network;
use crate::link::network::Network;
use crate::local::LinkBuilder;
use crate::protocol::{ConnAck, Connect, ConnectReturnCode, Login, Packet, Protocol};
use crate::router::{Connection, Event, Notification};
use crate::{
    AuthorizationAction, AuthorizationHandler, AuthorizationRequest, ConnectionId,
    ConnectionSettings,
};

use flume::{RecvError, SendError, Sender, TrySendError};
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::time::error::Elapsed;
use tokio::{select, sync::watch, time};
use tracing::{trace, Span};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Zero keep alive")]
    ZeroKeepAlive,
    #[error("Not connect packet")]
    NotConnectPacket(Packet),
    #[error("Network {0}")]
    Network(#[from] network::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Got new session, disconnecting old one")]
    SessionEnd,
    #[error("Persistent session requires valid client id")]
    InvalidClientId,
    #[error("Unexpected router message")]
    NotConnectionAck,
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Authentication error")]
    InvalidAuth,
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
    #[error("Authorization denied")]
    AuthorizationDenied,
    #[error("Shutdown")]
    Shutdown,
}

/// Orchestrates between Router and Network.
pub struct RemoteLink<P> {
    connect: Connect,
    pub(crate) connection_id: ConnectionId,
    network: Network<P>,
    link_tx: LinkTx,
    link_rx: LinkRx,
    notifications: VecDeque<Notification>,
    pub(crate) will_delay_interval: u32,
    authorization_client_id: String,
    authorization_handler: Option<AuthorizationHandler>,
    authenticated_username: Option<String>,
    authenticated_principal: Option<String>,
    topic_aliases: HashMap<u16, String>,
}

impl<P: Protocol> RemoteLink<P> {
    pub async fn new(
        router_tx: Sender<(ConnectionId, Event)>,
        tenant_id: Option<String>,
        mut network: Network<P>,
        connect_packet: Packet,
        dynamic_filters: bool,
        assigned_client_id: Option<String>,
        authorization_client_id: String,
        authenticated_principal: Option<String>,
        authorization_handler: Option<AuthorizationHandler>,
    ) -> Result<RemoteLink<P>, Error> {
        let authenticated_username = connect_packet_login(&connect_packet);
        let Packet::Connect(connect, props, lastwill, lastwill_props, _) = connect_packet else {
            return Err(Error::NotConnectPacket(connect_packet));
        };

        // Register this connection with the router. Router replys with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let client_id = assigned_client_id.as_ref().unwrap_or(&connect.client_id);
        let clean_session = connect.clean_session;

        let topic_alias_max = props.as_ref().and_then(|p| p.topic_alias_max);
        let session_expiry = props
            .as_ref()
            .and_then(|p| p.session_expiry_interval)
            .unwrap_or(0);

        let delay_interval = lastwill_props
            .as_ref()
            .and_then(|f| f.delay_interval)
            .unwrap_or(0);

        // The Server delays publishing the Client’s Will Message until
        // the Will Delay Interval has passed or the Session ends, whichever happens first
        let will_delay_interval = min(session_expiry, delay_interval);

        let (link_tx, link_rx, notification) = LinkBuilder::new(client_id, router_tx)
            .tenant_id(tenant_id)
            .clean_session(clean_session)
            .last_will(lastwill)
            .last_will_properties(lastwill_props)
            .dynamic_filters(dynamic_filters)
            .topic_alias_max(topic_alias_max.unwrap_or(0))
            .build()?;

        let id = link_rx.id();
        Span::current().record("connection_id", id);

        if let Some(mut packet) = notification.into() {
            if let Packet::ConnAck(_ack, props) = &mut packet {
                let mut new_props = props.clone().unwrap_or_default();
                new_props.assigned_client_identifier = assigned_client_id;
                *props = Some(new_props);
                network.write(packet).await?;
            }
        }

        Ok(RemoteLink {
            connect,
            connection_id: id,
            network,
            link_tx,
            link_rx,
            notifications: VecDeque::with_capacity(100),
            will_delay_interval,
            authorization_client_id,
            authorization_handler,
            authenticated_username,
            authenticated_principal,
            topic_aliases: HashMap::new(),
        })
    }

    pub async fn start(&mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), Error> {
        self.network.set_keepalive(self.connect.keep_alive);

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.read() => {
                    let packet = o?;
                    let mut packets = VecDeque::new();
                    packets.push_back(packet);
                    self.network.readv(&mut packets)?;
                    if self.authorization_handler.is_some() {
                        self.resolve_topic_aliases_for_authorization(&mut packets)?;
                    }
                    let authorization_handler = self.authorization_handler.clone();
                    let client_id = self.authorization_client_id.clone();
                    let authenticated_username = self.authenticated_username.clone();
                    let authenticated_principal = self.authenticated_principal.clone();
                    authorize_batch(
                        authorization_handler.as_ref(),
                        &client_id,
                        authenticated_username.as_deref(),
                        authenticated_principal.as_deref(),
                        &packets,
                    )
                    .await?;
                    let len = {
                        let mut buffer = self.link_tx.buffer();
                        buffer.extend(packets);
                        buffer.len()
                    };

                    trace!("Packets read from network, count = {}", len);
                    self.link_tx.notify().await?;
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                o = self.link_rx.exchange(&mut self.notifications) => {
                    o?;
                    let mut packets = VecDeque::new();
                    let mut unscheduled = false;

                    for notif in self.notifications.drain(..) {
                        if let Some(packet) = notif.into() {
                            packets.push_back(packet);
                        } else {
                            unscheduled = true;
                        }

                    }
                    self.network.writev(packets).await?;
                    if unscheduled {
                        self.link_rx.wake().await?;
                    }
                }
                _ = shutdown.changed() => return Ok(()),
            }
        }
    }

    fn resolve_topic_aliases_for_authorization(
        &mut self,
        packets: &mut VecDeque<Packet>,
    ) -> Result<(), Error> {
        for packet in packets {
            let Packet::Publish(publish, properties) = packet else {
                continue;
            };
            let Some(alias) = properties
                .as_ref()
                .and_then(|properties| properties.topic_alias)
            else {
                continue;
            };
            if alias == 0 || alias > crate::MAX_TOPIC_ALIAS {
                return Err(Error::AuthorizationDenied);
            }
            if publish.topic.is_empty() {
                publish.topic = self
                    .topic_aliases
                    .get(&alias)
                    .ok_or(Error::AuthorizationDenied)?
                    .clone()
                    .into();
            } else {
                let topic =
                    std::str::from_utf8(&publish.topic).map_err(|_| Error::AuthorizationDenied)?;
                self.topic_aliases.insert(alias, topic.to_owned());
            }
        }
        Ok(())
    }
}

async fn authorize_packet(
    handler: Option<&AuthorizationHandler>,
    client_id: &str,
    authenticated_username: Option<&str>,
    authenticated_principal: Option<&str>,
    packet: &Packet,
) -> Result<(), Error> {
    let Some(handler) = handler else {
        return Ok(());
    };

    match packet {
        Packet::Publish(publish, _) => {
            if publish.topic.is_empty() {
                return Err(Error::AuthorizationDenied);
            }
            authorize(
                handler,
                client_id,
                authenticated_username,
                authenticated_principal,
                AuthorizationAction::Publish,
                Some(String::from_utf8_lossy(&publish.topic).into_owned()),
                None,
            )
            .await
        }
        Packet::Subscribe(subscribe, _) => {
            for filter in &subscribe.filters {
                authorize(
                    handler,
                    client_id,
                    authenticated_username,
                    authenticated_principal,
                    AuthorizationAction::Subscribe,
                    None,
                    Some(filter.path.clone()),
                )
                .await?;
            }
            return Ok(());
        }
        _ => Ok(()),
    }
}

async fn authorize_batch(
    handler: Option<&AuthorizationHandler>,
    client_id: &str,
    authenticated_username: Option<&str>,
    authenticated_principal: Option<&str>,
    packets: &VecDeque<Packet>,
) -> Result<(), Error> {
    for packet in packets {
        authorize_packet(
            handler,
            client_id,
            authenticated_username,
            authenticated_principal,
            packet,
        )
        .await?;
    }
    Ok(())
}

async fn authorize(
    handler: &AuthorizationHandler,
    client_id: &str,
    authenticated_username: Option<&str>,
    authenticated_principal: Option<&str>,
    action: AuthorizationAction,
    topic: Option<String>,
    topic_filter: Option<String>,
) -> Result<(), Error> {
    let request = AuthorizationRequest {
        client_id: client_id.to_owned(),
        username: authenticated_username.map(str::to_owned),
        principal: authenticated_principal.map(str::to_owned),
        action,
        topic,
        topic_filter,
    };
    if handler(request).await {
        Ok(())
    } else {
        Err(Error::AuthorizationDenied)
    }
}

pub(crate) struct MqttConnectResult {
    pub(crate) packet: Packet,
    pub(crate) assigned_client_id: Option<String>,
    pub(crate) effective_client_id: String,
}

/// Read MQTT connect packet from network and verify it.
/// Authentication and checks are done here.
#[allow(dead_code)]
pub async fn mqtt_connect<P>(
    config: Arc<ConnectionSettings>,
    network: &mut Network<P>,
) -> Result<Packet, Error>
where
    P: Protocol,
{
    let (_shutdown, mut receiver) = watch::channel(false);
    mqtt_connect_with_identity(config, network, None, &mut receiver)
        .await
        .map(|result| result.packet)
}

pub(crate) async fn mqtt_connect_with_identity<P>(
    config: Arc<ConnectionSettings>,
    network: &mut Network<P>,
    tenant_id: Option<&str>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<MqttConnectResult, Error>
where
    P: Protocol,
{
    // Wait for MQTT connect packet and error out if it's not received in time to prevent
    // DOS attacks by filling total connections that the server can handle with idle open
    // connections which results in server rejecting new connections
    let connection_timeout_ms = config.connection_timeout_ms.into();
    let packet = tokio::select! {
        result = time::timeout(Duration::from_millis(connection_timeout_ms), async {
            let packet = network.read().await?;
            Ok::<_, network::Error>(packet)
        }) => result??,
        _ = shutdown.changed() => return Err(Error::Shutdown),
    };

    let (connect, _props, login) = match packet {
        Packet::Connect(ref connect, ref props, _, _, ref login) => (connect, props, login),
        packet => return Err(Error::NotConnectPacket(packet)),
    };

    Span::current().record("client_id", &connect.client_id);

    // When keep_alive feature is disabled client can live forever, which is not good in
    // distributed broker context so currenlty we don't allow it.
    if connect.keep_alive == 0 {
        return Err(Error::ZeroKeepAlive);
    }

    let empty_client_id = connect.client_id.is_empty();
    let clean_session = connect.clean_session;

    if empty_client_id && !clean_session {
        let ack = ConnAck {
            session_present: false,
            code: ConnectReturnCode::ClientIdentifierNotValid,
        };

        let packet = Packet::ConnAck(ack, None);
        network.write(packet).await?;

        return Err(Error::InvalidClientId);
    }

    let mut client_id = connect.client_id.clone();
    let mut assigned_client_id = None;
    if client_id.is_empty() {
        let uuid = Uuid::new_v4().simple();
        client_id = format!("rumqtt-{uuid}");
        assigned_client_id = Some(client_id.clone());
    }
    let effective_client_id = Connection::effective_client_id(tenant_id, &client_id);

    handle_auth(config, login.as_ref(), &effective_client_id).await?;

    Ok(MqttConnectResult {
        packet,
        assigned_client_id,
        effective_client_id,
    })
}

pub(crate) async fn authorize_connect(
    handler: Option<&AuthorizationHandler>,
    client_id: &str,
    login: Option<&Login>,
    principal: Option<String>,
) -> Result<(), Error> {
    let Some(handler) = handler else {
        return Ok(());
    };

    let request = AuthorizationRequest {
        client_id: client_id.to_owned(),
        username: login.map(|login| login.username.clone()),
        principal,
        action: AuthorizationAction::Connect,
        topic: None,
        topic_filter: None,
    };
    if handler(request).await {
        Ok(())
    } else {
        Err(Error::AuthorizationDenied)
    }
}

pub(crate) fn authenticated_principal(
    config: &ConnectionSettings,
    login: Option<&Login>,
) -> Option<String> {
    if config.auth.is_some() || config.external_auth.is_some() {
        login.map(|login| login.username.clone())
    } else {
        None
    }
}

fn connect_packet_login(packet: &Packet) -> Option<String> {
    match packet {
        Packet::Connect(_, _, _, _, login) => login.as_ref().map(|login| login.username.clone()),
        _ => None,
    }
}

async fn handle_auth(
    config: Arc<ConnectionSettings>,
    login: Option<&Login>,
    client_id: &str,
) -> Result<(), Error> {
    if config.auth.is_none() && config.external_auth.is_none() {
        return Ok(());
    }

    // if authentication is configured and connect packet doesn't have login details
    // return an error
    let Some(login) = login else {
        return Err(Error::InvalidAuth);
    };

    let username = &login.username;
    let password = &login.password;

    if let Some(auth) = &config.external_auth {
        if !auth(
            client_id.to_owned(),
            username.to_owned(),
            password.to_owned(),
        )
        .await
        {
            return Err(Error::InvalidAuth);
        }

        return Ok(());
    }

    if let Some(pairs) = &config.auth {
        if let Some(stored_password) = pairs.get(username) {
            if stored_password.as_bytes().ct_eq(password.as_bytes()).into() {
                return Ok(());
            }
        }

        return Err(Error::InvalidAuth);
    }

    Err(Error::InvalidAuth)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use tokio::io::AsyncWriteExt;

    use crate::{
        link::network::Network,
        protocol::{v4::V4, Login},
        AuthorizationAction, AuthorizationHandler, AuthorizationRequest, ConnectionSettings,
    };

    use super::{authenticated_principal, authorize_batch, authorize_connect, handle_auth};

    fn config() -> ConnectionSettings {
        ConnectionSettings {
            connection_timeout_ms: 0,
            max_payload_size: 0,
            max_inflight_count: 0,
            auth: None,
            external_auth: None,
            authorization_handler: None,
            dynamic_filters: false,
        }
    }

    fn login() -> Login {
        Login {
            username: "u".to_owned(),
            password: "p".to_owned(),
        }
    }

    fn capture_handler(captured: Arc<Mutex<Option<AuthorizationRequest>>>) -> AuthorizationHandler {
        Arc::new(move |request| {
            *captured.lock().unwrap() = Some(request);
            Box::pin(async { true })
        })
    }

    fn publish_packet(topic: &str, payload: &[u8]) -> Vec<u8> {
        let remaining = 2 + topic.len() + payload.len();
        let mut packet = vec![0x30, remaining as u8];
        packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        packet.extend_from_slice(topic.as_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    #[tokio::test]
    async fn username_without_configured_auth_is_not_an_authenticated_principal() {
        let captured = Arc::new(Mutex::new(None));
        let login = login();
        let cfg = config();
        let principal = authenticated_principal(&cfg, Some(&login));

        authorize_connect(
            Some(&capture_handler(Arc::clone(&captured))),
            "client",
            Some(&login),
            principal,
        )
        .await
        .unwrap();

        let request = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.action, AuthorizationAction::Connect);
        assert_eq!(request.username.as_deref(), Some("u"));
        assert_eq!(request.principal, None);
    }

    #[tokio::test]
    async fn validated_static_auth_promotes_username_to_authenticated_principal() {
        let captured = Arc::new(Mutex::new(None));
        let login = login();
        let mut raw_config = config();
        raw_config
            .auth
            .get_or_insert_with(HashMap::new)
            .insert(login.username.clone(), login.password.clone());
        let cfg = Arc::new(raw_config);
        handle_auth(Arc::clone(&cfg), Some(&login), "client")
            .await
            .unwrap();

        authorize_connect(
            Some(&capture_handler(Arc::clone(&captured))),
            "client",
            Some(&login),
            authenticated_principal(&cfg, Some(&login)),
        )
        .await
        .unwrap();

        let request = captured.lock().unwrap().clone().unwrap();
        assert_eq!(request.principal.as_deref(), Some("u"));
    }

    #[tokio::test]
    async fn readv_batch_is_staged_atomically_when_second_packet_is_denied() {
        let (mut client, server) = tokio::io::duplex(4096);
        let mut bytes = publish_packet("batch-allowed", b"allowed");
        bytes.extend(publish_packet("batch-denied", b"denied"));
        client.write_all(&bytes).await.unwrap();

        let mut network = Network::new(Box::new(server), 1024, 10, V4);
        let first = network.read().await.unwrap();
        let mut packets = VecDeque::from([first]);
        assert_eq!(network.readv(&mut packets).unwrap(), 2);

        let handler: AuthorizationHandler = Arc::new(|request| {
            Box::pin(async move { request.topic.as_deref() != Some("batch-denied") })
        });
        let result = authorize_batch(Some(&handler), "client", None, None, &packets).await;

        let mut forwarded = VecDeque::new();
        let mut notifications = 0;
        if result.is_ok() {
            forwarded.extend(packets.iter().cloned());
            notifications += 1;
        }

        assert!(result.is_err());
        assert_eq!(packets.len(), 2);
        assert!(forwarded.is_empty());
        assert_eq!(notifications, 0);
    }

    #[tokio::test]
    async fn no_login_no_auth() {
        let cfg = Arc::new(config());
        let r = handle_auth(cfg, None, "").await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn some_login_no_auth() {
        let cfg = Arc::new(config());
        let login = login();
        let r = handle_auth(cfg, Some(&login), "").await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_matches_static_auth() {
        let login = login();
        let mut map = HashMap::<String, String>::new();
        map.insert(login.username.clone(), login.password.clone());

        let mut cfg = config();
        cfg.auth = Some(map);

        let r = handle_auth(Arc::new(cfg), Some(&login), "").await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_fails_static_no_external() {
        let login = login();
        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let mut cfg = config();
        cfg.auth = Some(map);

        let r = handle_auth(Arc::new(cfg), Some(&login), "").await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn login_fails_static_matches_external() {
        let login = login();

        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let dynamic = |_: String, _: String, _: String| async { true };

        let mut cfg = config();
        cfg.auth = Some(map);
        cfg.set_auth_handler(dynamic);

        let r = handle_auth(Arc::new(cfg), Some(&login), "").await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn login_fails_static_fails_external() {
        let login = login();

        let mut map = HashMap::<String, String>::new();
        map.insert("wrong".to_owned(), "wrong".to_owned());

        let dynamic = |_: String, _: String, _: String| async { false };

        let mut cfg = config();
        cfg.auth = Some(map);
        cfg.set_auth_handler(dynamic);

        let r = handle_auth(Arc::new(cfg), Some(&login), "").await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn external_auth_clousre_or_fnptr_type_check_or_fail_compile() {
        let closure = |_: String, _: String, _: String| async { false };
        async fn fnptr(_: String, _: String, _: String) -> bool {
            true
        }

        let mut cfg = config();
        cfg.set_auth_handler(closure);
        cfg.set_auth_handler(fnptr);
    }
}
