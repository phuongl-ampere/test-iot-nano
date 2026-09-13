use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

use chrono::{Duration, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use iot_core::RpcRequest;
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, CommandResponseError, CommandResponsePort,
    DeviceAuthenticator, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, LocalDeviceAuthenticator, LocalRpcResponseForwarder,
    LocalStreamUplinkForwarder, MqttdDeviceTransport, RpcResponseForwarder, RpcSessionRouter,
    SessionError, SessionRegistration, TransportAuthRequest, TransportRpcResponse, TransportUplink,
    UplinkForwarder,
};
use iot_nano_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
    HeartbeatRequest, PartitionId, StreamError, StreamMessage, StreamPort,
};
use rumqttc::{
    Connect as V311Connect, ConnectReturnCode as V311ConnectReturnCode, Packet as V311Packet,
    Publish as V311Publish, QoS,
    mqttbytes::v4::Codec as V311Codec,
    v5::mqttbytes::{
        QoS as V5QoS,
        v5::{
            Codec as V5Codec, Connect as V5Connect, ConnectReturnCode as V5ConnectReturnCode,
            Login as V5Login, Packet as V5Packet, PubAck as V5PubAck, Publish as V5Publish,
        },
    },
};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
    sync::{Mutex, Notify, mpsc},
};
use tokio_util::codec::Framed;
use uuid::Uuid;

#[derive(Clone, Default)]
struct RecordingAuthorization {
    authenticated: Arc<Mutex<Vec<TransportAuthRequest>>>,
    sessions: Arc<Mutex<Vec<AuthenticatedDevice>>>,
}

impl DeviceAuthorizationPort for RecordingAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        let authenticated = Arc::clone(&self.authenticated);
        Box::pin(async move {
            authenticated.lock().await.push(request);
            Ok(device())
        })
    }

    fn authorize_session(
        &self,
        authenticated: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            sessions.lock().await.push(authenticated);
            Ok(())
        })
    }

    fn authorize_gateway_uplink(
        &self,
        _request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async { Err(AuthorizationError::Denied) })
    }
}

#[derive(Clone)]
struct GatewayAuthorizationStub {
    result: Result<GatewayAuthorization, AuthorizationError>,
    requests: Arc<Mutex<Vec<GatewayAuthorizationRequest>>>,
}

impl GatewayAuthorizationStub {
    fn new(result: Result<GatewayAuthorization, AuthorizationError>) -> Self {
        Self {
            result,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl DeviceAuthorizationPort for GatewayAuthorizationStub {
    fn authenticate(
        &self,
        _request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async { Ok(gateway_device()) })
    }

    fn authorize_session(
        &self,
        _authenticated: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn authorize_gateway_uplink(
        &self,
        request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        let requests = Arc::clone(&self.requests);
        let result = self.result.clone();
        Box::pin(async move {
            requests.lock().await.push(request);
            result
        })
    }
}

#[derive(Clone)]
struct BlockingStream {
    append_started: Arc<Notify>,
    release_append: Arc<Notify>,
    appended: Arc<AtomicUsize>,
    attempted: Arc<AtomicUsize>,
    failure: bool,
}

impl BlockingStream {
    fn new() -> Self {
        Self::with_failure(false)
    }

    fn failing() -> Self {
        Self::with_failure(true)
    }

    fn with_failure(failure: bool) -> Self {
        Self {
            append_started: Arc::new(Notify::new()),
            release_append: Arc::new(Notify::new()),
            appended: Arc::new(AtomicUsize::new(0)),
            attempted: Arc::new(AtomicUsize::new(0)),
            failure,
        }
    }
}

impl StreamPort for BlockingStream {
    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        let append_started = Arc::clone(&self.append_started);
        let release_append = Arc::clone(&self.release_append);
        let appended = Arc::clone(&self.appended);
        let attempted = Arc::clone(&self.attempted);
        let failure = self.failure;
        Box::pin(async move {
            attempted.fetch_add(1, Ordering::SeqCst);
            append_started.notify_one();
            if failure {
                return Err(StreamError::CapacityExceeded {
                    max_bytes: 1,
                    current_bytes: 1,
                    requested_bytes: 1,
                });
            }
            release_append.notified().await;
            appended.fetch_add(1, Ordering::SeqCst);
            Ok(AppendReceipt {
                partition: PartitionId::new(0),
                offset: 0,
            })
        })
    }

    fn claim(
        &self,
        _request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn acknowledge(
        &self,
        _request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn heartbeat(
        &self,
        _request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(async {
            Ok(GroupAssignment {
                generation: 1,
                partitions: vec![PartitionId::new(0)],
            })
        })
    }

    fn drain(
        &self,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct WriteObservedSocket {
    inner: DuplexStream,
    writes: mpsc::UnboundedSender<Vec<u8>>,
}

impl WriteObservedSocket {
    fn new(inner: DuplexStream, writes: mpsc::UnboundedSender<Vec<u8>>) -> Self {
        Self { inner, writes }
    }
}

impl AsyncRead for WriteObservedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for WriteObservedSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(context, buffer) {
            Poll::Ready(Ok(written)) => {
                this.writes
                    .send(buffer[..written].to_vec())
                    .expect("the test must retain its server-write receiver");
                Poll::Ready(Ok(written))
            }
            result => result,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

#[derive(Clone, Default)]
struct RecordingResponses {
    responses: Arc<Mutex<Vec<TransportRpcResponse>>>,
}

impl CommandResponsePort for RecordingResponses {
    fn record_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>> {
        let responses = Arc::clone(&self.responses);
        Box::pin(async move {
            responses.lock().await.push(response);
            Ok(())
        })
    }
}

#[tokio::test]
async fn local_device_authorization_uses_the_typed_port_without_http() {
    let authorization = Arc::new(RecordingAuthorization::default());
    let authenticator = LocalDeviceAuthenticator::new(authorization.clone());

    let authenticated = authenticator
        .authenticate(TransportAuthRequest {
            client_id: "device-client".to_owned(),
            username: "iotd_device_token".to_owned(),
            password: String::new(),
        })
        .await
        .unwrap();
    authenticator
        .authorize_session(authenticated.clone())
        .await
        .unwrap();

    assert_eq!(authorization.authenticated.lock().await.len(), 1);
    assert_eq!(
        authorization.sessions.lock().await[0].device_id,
        authenticated.device_id
    );
}

#[tokio::test]
async fn local_mqtt_connect_revalidates_the_authenticated_session() {
    let authorization = Arc::new(RecordingAuthorization::default());
    let transport = MqttdDeviceTransport::with_local_ports(
        authorization.clone(),
        Arc::new(BlockingStream::new()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let server_task = tokio::spawn(async move { transport.serve_connection(server).await });
    let mut client = Framed::new(client, v311_codec());

    connect_v311(&mut client).await;
    assert_eq!(authorization.sessions.lock().await.len(), 1);

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn local_uplink_returns_only_after_durable_stream_append() {
    let authorization = Arc::new(RecordingAuthorization::default());
    let stream = BlockingStream::new();
    let uplink = LocalStreamUplinkForwarder::new(authorization, Arc::new(stream.clone()));
    let forwarding = tokio::spawn(async move { uplink.forward("unused", direct_uplink()).await });

    stream.append_started.notified().await;
    assert!(!forwarding.is_finished());
    assert_eq!(stream.appended.load(Ordering::SeqCst), 0);

    stream.release_append.notify_one();
    forwarding.await.unwrap().unwrap();
    assert_eq!(stream.appended.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn local_gateway_uplink_authorizes_and_appends_exactly_once() {
    let authorization = Arc::new(GatewayAuthorizationStub::new(Ok(
        matching_gateway_authorization(),
    )));
    let stream = BlockingStream::new();
    let uplink = LocalStreamUplinkForwarder::new(authorization.clone(), Arc::new(stream.clone()));
    let forwarding = tokio::spawn(async move { uplink.forward("unused", gateway_uplink()).await });

    stream.append_started.notified().await;
    assert_eq!(stream.appended.load(Ordering::SeqCst), 0);
    stream.release_append.notify_one();
    forwarding.await.unwrap().unwrap();

    assert_eq!(stream.appended.load(Ordering::SeqCst), 1);
    assert_eq!(
        authorization.requests.lock().await.as_slice(),
        &[GatewayAuthorizationRequest {
            gateway_device_id: "gateway-a".to_owned(),
            token_id: gateway_device().token_id,
            child_device_id: Some("child-a".to_owned()),
            topic: "v1/gateways/me/telemetry".to_owned(),
            event_kind: "child_telemetry".to_owned(),
        }]
    );
}

#[tokio::test]
async fn local_gateway_uplink_denial_does_not_append() {
    let authorization = Arc::new(GatewayAuthorizationStub::new(Err(
        AuthorizationError::Denied,
    )));
    let stream = BlockingStream::new();
    let uplink = LocalStreamUplinkForwarder::new(authorization.clone(), Arc::new(stream.clone()));

    assert!(matches!(
        uplink
            .forward("unused", gateway_uplink())
            .await
            .unwrap_err(),
        iot_nano_mqttd::TransportError::Unauthorized
    ));
    assert_eq!(authorization.requests.lock().await.len(), 1);
    assert_eq!(stream.appended.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_gateway_uplink_unavailability_does_not_append() {
    let authorization = Arc::new(GatewayAuthorizationStub::new(Err(
        AuthorizationError::Unavailable("core offline".to_owned()),
    )));
    let stream = BlockingStream::new();
    let uplink = LocalStreamUplinkForwarder::new(authorization.clone(), Arc::new(stream.clone()));

    assert!(matches!(
        uplink.forward("unused", gateway_uplink()).await.unwrap_err(),
        iot_nano_mqttd::TransportError::AuthorizationUnavailable(reason) if reason == "core offline"
    ));
    assert_eq!(authorization.requests.lock().await.len(), 1);
    assert_eq!(stream.appended.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_gateway_uplink_identity_mismatches_do_not_append() {
    let mut mismatches = Vec::new();

    let mut authorization = matching_gateway_authorization();
    authorization.gateway_device_id = "other-gateway".to_owned();
    mismatches.push(authorization);

    let mut authorization = matching_gateway_authorization();
    authorization.token_id = Uuid::now_v7();
    mismatches.push(authorization);

    let mut authorization = matching_gateway_authorization();
    authorization.topic = "v1/gateways/me/connect".to_owned();
    mismatches.push(authorization);

    let mut authorization = matching_gateway_authorization();
    authorization.event_kind = "connect".to_owned();
    mismatches.push(authorization);

    let mut authorization = matching_gateway_authorization();
    authorization.child_device_id = Some("other-child".to_owned());
    mismatches.push(authorization);

    for authorization in mismatches {
        let authorization = Arc::new(GatewayAuthorizationStub::new(Ok(authorization)));
        let stream = BlockingStream::new();
        let uplink =
            LocalStreamUplinkForwarder::new(authorization.clone(), Arc::new(stream.clone()));

        assert!(matches!(
            uplink
                .forward("unused", gateway_uplink())
                .await
                .unwrap_err(),
            iot_nano_mqttd::TransportError::Unauthorized
        ));
        assert_eq!(authorization.requests.lock().await.len(), 1);
        assert_eq!(stream.appended.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn local_rpc_response_forwarder_uses_the_typed_port_without_http() {
    let responses = Arc::new(RecordingResponses::default());
    let forwarder = LocalRpcResponseForwarder::new(responses.clone());
    let response = TransportRpcResponse {
        command_id: Uuid::now_v7(),
        device_id: "device-a".to_owned(),
        token_id: Uuid::now_v7(),
        response: json!({"ok": true}),
    };

    forwarder.forward_response(response.clone()).await.unwrap();
    assert_eq!(responses.responses.lock().await.as_slice(), &[response]);
}

#[test]
fn device_transport_composes_the_local_typed_ports() {
    let authorization = Arc::new(RecordingAuthorization::default());
    let stream = Arc::new(BlockingStream::new());
    let responses = Arc::new(RecordingResponses::default());

    let _transport = MqttdDeviceTransport::with_local_ports(authorization, stream, responses);
}

#[tokio::test]
async fn injected_router_routes_a_request_to_only_the_connected_transport_session() {
    let injected_router = RpcSessionRouter::default();
    let transport = MqttdDeviceTransport::with_local_ports_and_router(
        injected_router.clone(),
        Arc::new(RecordingAuthorization::default()),
        Arc::new(BlockingStream::new()),
        Arc::new(RecordingResponses::default()),
    );
    let mut device = injected_router
        .register(SessionRegistration {
            token_id: device().token_id,
            device_id: "device-a".to_owned(),
            client_id: "local-client".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let mut other_device = injected_router
        .register(SessionRegistration {
            token_id: Uuid::now_v7(),
            device_id: "device-b".to_owned(),
            client_id: "other-client".to_owned(),
            connection_id: "connection-b".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let issued_at = Utc::now();
    let request = RpcRequest::new(
        Uuid::now_v7(),
        "sample_now",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();

    let publish = tokio::spawn({
        let transport = transport.clone();
        async move {
            transport
                .router()
                .publish_to_device("device-a", request)
                .await
        }
    });
    let command = device.recv().await.unwrap();

    assert_eq!(command.request.method, "sample_now");
    assert!(other_device.try_recv().is_err());
    assert!(!publish.is_finished());
    command.acknowledge_published().unwrap();
    assert!(publish.await.unwrap().is_ok());
}

#[tokio::test]
async fn legacy_local_ports_wrapper_owns_a_usable_default_router() {
    let transport = MqttdDeviceTransport::with_local_ports(
        Arc::new(RecordingAuthorization::default()),
        Arc::new(BlockingStream::new()),
        Arc::new(RecordingResponses::default()),
    );
    let mut device = transport
        .router()
        .register(SessionRegistration {
            token_id: device().token_id,
            device_id: "device-a".to_owned(),
            client_id: "local-client".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let issued_at = Utc::now();
    let request = RpcRequest::new(
        Uuid::now_v7(),
        "reboot",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();

    let publish = tokio::spawn({
        let router = transport.router();
        async move { router.publish_to_device("device-a", request).await }
    });
    let command = device.recv().await.unwrap();

    assert_eq!(command.request.method, "reboot");
    assert!(!publish.is_finished());
    command.acknowledge_published().unwrap();
    assert!(publish.await.unwrap().is_ok());
}

#[tokio::test]
async fn revocation_invalidates_a_command_queued_before_device_delivery() {
    let router = RpcSessionRouter::default();
    let token_id = device().token_id;
    let mut device = router
        .register(SessionRegistration {
            token_id,
            device_id: "device-a".to_owned(),
            client_id: "local-client".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;
    let issued_at = Utc::now();
    let request = RpcRequest::new(
        Uuid::now_v7(),
        "reboot",
        json!({}),
        issued_at,
        issued_at + Duration::seconds(30),
    )
    .unwrap();

    let publish = tokio::spawn({
        let router = router.clone();
        async move { router.publish_to_device("device-a", request).await }
    });
    let command = device.recv().await.unwrap();

    assert!(router.revoke_session("device-a", token_id).await);
    assert!(command.acquire_delivery_lease().await.is_none());
    drop(command);
    assert!(matches!(
        publish.await.unwrap(),
        Err(SessionError::PublicationWaiterUnavailable)
    ));
}

#[tokio::test]
async fn revoked_token_cannot_register_a_session_after_revocation() {
    let router = RpcSessionRouter::default();
    let token_id = device().token_id;

    assert!(!router.revoke_session("device-a", token_id).await);
    let _receiver = router
        .register(SessionRegistration {
            token_id,
            device_id: "device-a".to_owned(),
            client_id: "local-client".to_owned(),
            connection_id: "connection-a".to_owned(),
            is_gateway: false,
            connected_at: Utc::now(),
        })
        .await;

    assert!(router.active_snapshot("device-a").await.is_none());
}

#[tokio::test]
async fn revoked_mqtt311_connection_cannot_append_or_ack_telemetry() {
    let router = RpcSessionRouter::default();
    let transport = MqttdDeviceTransport::with_local_ports_and_router(
        router.clone(),
        Arc::new(RecordingAuthorization::default()),
        Arc::new(BlockingStream::failing()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v311_codec());

    connect_v311(&mut client).await;
    drain_server_writes(&mut server_writes);
    assert!(!router.revoke_session("device-a", device().token_id).await);
    client
        .send(V311Packet::Publish(v311_telemetry_publish(71)))
        .await
        .unwrap();

    assert!(matches!(
        server_task.await.unwrap(),
        Err(iot_nano_mqttd::TransportError::Unauthorized)
    ));
    assert_no_server_writes(&mut server_writes);
}

#[tokio::test]
async fn revoked_mqtt5_connection_cannot_append_or_ack_telemetry() {
    let router = RpcSessionRouter::default();
    let transport = MqttdDeviceTransport::with_local_ports_and_router(
        router.clone(),
        Arc::new(RecordingAuthorization::default()),
        Arc::new(BlockingStream::failing()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_v5_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v5_codec());

    connect_v5(&mut client).await;
    drain_server_writes(&mut server_writes);
    assert!(!router.revoke_session("device-a", device().token_id).await);
    client
        .send(V5Packet::Publish(v5_telemetry_publish(72)))
        .await
        .unwrap();

    assert!(matches!(
        server_task.await.unwrap(),
        Err(iot_nano_mqttd::TransportError::Unauthorized)
    ));
    assert_no_server_writes(&mut server_writes);
}

#[tokio::test]
async fn local_mqtt311_qos1_puback_waits_for_stream_append_release() {
    let stream = BlockingStream::new();
    let transport = MqttdDeviceTransport::with_local_ports(
        Arc::new(RecordingAuthorization::default()),
        Arc::new(stream.clone()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v311_codec());

    connect_v311(&mut client).await;
    drain_server_writes(&mut server_writes);
    client
        .send(V311Packet::Publish(v311_telemetry_publish(41)))
        .await
        .unwrap();

    stream.append_started.notified().await;
    assert_no_server_writes(&mut server_writes);
    assert!(!server_task.is_finished());

    stream.release_append.notify_one();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        V311Packet::PubAck(ack) if ack.pkid == 41
    ));

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn local_mqtt311_qos1_append_failure_never_writes_puback() {
    let stream = BlockingStream::failing();
    let transport = MqttdDeviceTransport::with_local_ports(
        Arc::new(RecordingAuthorization::default()),
        Arc::new(stream.clone()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v311_codec());

    connect_v311(&mut client).await;
    drain_server_writes(&mut server_writes);
    client
        .send(V311Packet::Publish(v311_telemetry_publish(42)))
        .await
        .unwrap();

    assert!(matches!(
        server_task.await.unwrap(),
        Err(iot_nano_mqttd::TransportError::StreamAppendFailed(_))
    ));
    assert_eq!(stream.attempted.load(Ordering::SeqCst), 1);
    assert_no_server_writes(&mut server_writes);
}

#[tokio::test]
async fn local_mqtt5_qos1_puback_waits_for_stream_append_release() {
    let stream = BlockingStream::new();
    let transport = MqttdDeviceTransport::with_local_ports(
        Arc::new(RecordingAuthorization::default()),
        Arc::new(stream.clone()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_v5_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v5_codec());

    connect_v5(&mut client).await;
    drain_server_writes(&mut server_writes);
    client
        .send(V5Packet::Publish(v5_telemetry_publish(51)))
        .await
        .unwrap();

    stream.append_started.notified().await;
    assert_no_server_writes(&mut server_writes);
    assert!(!server_task.is_finished());

    stream.release_append.notify_one();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        V5Packet::PubAck(V5PubAck { pkid: 51, .. })
    ));

    drop(client);
    assert!(server_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn local_mqtt5_qos1_append_failure_never_writes_puback() {
    let stream = BlockingStream::failing();
    let transport = MqttdDeviceTransport::with_local_ports(
        Arc::new(RecordingAuthorization::default()),
        Arc::new(stream.clone()),
        Arc::new(RecordingResponses::default()),
    );
    let (server, client) = duplex(8 * 1024);
    let (write_sender, mut server_writes) = mpsc::unbounded_channel();
    let server_task = tokio::spawn(async move {
        transport
            .serve_v5_connection(WriteObservedSocket::new(server, write_sender))
            .await
    });
    let mut client = Framed::new(client, v5_codec());

    connect_v5(&mut client).await;
    drain_server_writes(&mut server_writes);
    client
        .send(V5Packet::Publish(v5_telemetry_publish(52)))
        .await
        .unwrap();

    assert!(matches!(
        server_task.await.unwrap(),
        Err(iot_nano_mqttd::TransportError::StreamAppendFailed(_))
    ));
    assert_eq!(stream.attempted.load(Ordering::SeqCst), 1);
    assert_no_server_writes(&mut server_writes);
}

#[test]
fn local_port_implementations_contain_no_http_boundary() {
    let source =
        std::fs::read_to_string(format!("{}/src/ports.rs", env!("CARGO_MANIFEST_DIR"))).unwrap();
    for forbidden in ["reqwest", "/internal/", "x-iot-nano-"] {
        assert!(
            !source.contains(forbidden),
            "local port implementation contains {forbidden:?}"
        );
    }
}

fn device() -> AuthenticatedDevice {
    AuthenticatedDevice {
        token_id: Uuid::parse_str("019a5114-0674-7bd7-8486-50b6ebbd7245").unwrap(),
        device_id: "device-a".to_owned(),
        is_gateway: false,
    }
}

fn gateway_device() -> AuthenticatedDevice {
    AuthenticatedDevice {
        token_id: Uuid::parse_str("019a5114-0674-7bd7-8486-50b6ebbd7245").unwrap(),
        device_id: "gateway-a".to_owned(),
        is_gateway: true,
    }
}

fn matching_gateway_authorization() -> GatewayAuthorization {
    GatewayAuthorization {
        gateway_device_id: "gateway-a".to_owned(),
        token_id: gateway_device().token_id,
        child_device_id: Some("child-a".to_owned()),
        topic: "v1/gateways/me/telemetry".to_owned(),
        event_kind: "child_telemetry".to_owned(),
    }
}

fn direct_uplink() -> TransportUplink {
    TransportUplink {
        device: device(),
        topic: "v1/devices/me/telemetry".to_owned(),
        payload: serde_json::to_vec(&json!({
            "schema_version": 1,
            "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
            "sequence": 1,
            "event_at": "2026-09-10T08:00:00Z",
            "measurements": {"temperature_c": 26.4},
        }))
        .unwrap(),
        qos: QoS::AtLeastOnce,
        received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
    }
}

fn gateway_uplink() -> TransportUplink {
    TransportUplink {
        device: gateway_device(),
        topic: "v1/gateways/me/telemetry".to_owned(),
        payload: serde_json::to_vec(&json!({
            "kind": "child_telemetry",
            "schema_version": 1,
            "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
            "sequence": 1,
            "event_at": "2026-09-10T08:00:00Z",
            "child_device_id": "child-a",
            "measurements": {"temperature_c": 26.4},
        }))
        .unwrap(),
        qos: QoS::AtLeastOnce,
        received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
    }
}

fn v311_codec() -> V311Codec {
    V311Codec {
        max_incoming_size: 1024 * 1024,
        max_outgoing_size: 1024 * 1024,
    }
}

fn v5_codec() -> V5Codec {
    V5Codec {
        max_incoming_size: Some(1024 * 1024),
        max_outgoing_size: Some(1024 * 1024),
    }
}

async fn connect_v311(client: &mut Framed<DuplexStream, V311Codec>) {
    let mut connect = V311Connect::new("local-v311-client");
    connect.set_login("iotd_local_device_token", "");
    client.send(V311Packet::Connect(connect)).await.unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        V311Packet::ConnAck(ack) if ack.code == V311ConnectReturnCode::Success
    ));
}

async fn connect_v5(client: &mut Framed<DuplexStream, V5Codec>) {
    client
        .send(V5Packet::Connect(
            V5Connect {
                keep_alive: 60,
                client_id: "local-v5-client".to_owned(),
                clean_start: true,
                properties: None,
            },
            None,
            Some(V5Login::new("iotd_local_device_token", "")),
        ))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        V5Packet::ConnAck(ack) if ack.code == V5ConnectReturnCode::Success
    ));
}

fn v311_telemetry_publish(packet_id: u16) -> V311Publish {
    let mut publish = V311Publish::new(
        "v1/devices/me/telemetry",
        QoS::AtLeastOnce,
        direct_uplink().payload,
    );
    publish.pkid = packet_id;
    publish
}

fn v5_telemetry_publish(packet_id: u16) -> V5Publish {
    let mut publish = V5Publish::new(
        "v1/devices/me/telemetry",
        V5QoS::AtLeastOnce,
        direct_uplink().payload,
        None,
    );
    publish.pkid = packet_id;
    publish
}

fn drain_server_writes(writes: &mut mpsc::UnboundedReceiver<Vec<u8>>) {
    while writes.try_recv().is_ok() {}
}

fn assert_no_server_writes(writes: &mut mpsc::UnboundedReceiver<Vec<u8>>) {
    match writes.try_recv() {
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {}
        Ok(bytes) => panic!("server wrote MQTT bytes before the append outcome: {bytes:?}"),
    }
}
