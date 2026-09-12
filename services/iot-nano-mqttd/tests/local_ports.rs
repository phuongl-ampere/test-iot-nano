use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use chrono::{TimeZone, Utc};
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, CommandResponseError, CommandResponsePort,
    DeviceAuthenticator, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, LocalDeviceAuthenticator, LocalRpcResponseForwarder,
    LocalStreamUplinkForwarder, MqttdDeviceTransport, RpcResponseForwarder, TransportAuthRequest,
    TransportRpcResponse, TransportUplink, UplinkForwarder,
};
use iot_nano_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
    HeartbeatRequest, PartitionId, StreamError, StreamMessage, StreamPort,
};
use rumqttc::QoS;
use serde_json::json;
use tokio::sync::{Mutex, Notify};
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
struct BlockingStream {
    append_started: Arc<Notify>,
    release_append: Arc<Notify>,
    appended: Arc<AtomicUsize>,
}

impl BlockingStream {
    fn new() -> Self {
        Self {
            append_started: Arc::new(Notify::new()),
            release_append: Arc::new(Notify::new()),
            appended: Arc::new(AtomicUsize::new(0)),
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
        Box::pin(async move {
            append_started.notify_one();
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
