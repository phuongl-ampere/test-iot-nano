use std::{future::Future, sync::Arc};

use chrono::{DateTime, Utc};
use iot_core::TelemetryEvent;
use iot_stream::{StreamError, StreamPort, TelemetryMessage};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Accepted,
    Rejected,
}

#[derive(Debug, Error)]
pub enum MqttConsumerError {
    #[error(transparent)]
    Stream(#[from] StreamError),
}

#[derive(Clone)]
pub struct MqttStreamProducer {
    tenant_id: uuid::Uuid,
    stream: Arc<dyn StreamPort>,
}

impl MqttStreamProducer {
    pub fn new(tenant_id: uuid::Uuid, stream: Arc<dyn StreamPort>) -> Self {
        Self { tenant_id, stream }
    }

    pub async fn ingest(
        &self,
        topic: &str,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<IngestOutcome, MqttConsumerError> {
        let event = match serde_json::from_slice::<TelemetryEvent>(payload) {
            Ok(event) => event,
            Err(_) => return Ok(IngestOutcome::Rejected),
        };

        self.ingest_event(topic, payload.to_vec(), event, received_at)
            .await
    }

    pub async fn ingest_event(
        &self,
        topic: &str,
        payload: Vec<u8>,
        event: TelemetryEvent,
        received_at: DateTime<Utc>,
    ) -> Result<IngestOutcome, MqttConsumerError> {
        let message = TelemetryMessage {
            tenant_id: self.tenant_id,
            topic: topic.to_owned(),
            payload,
            event,
            received_at,
        };
        match self.stream.append(message.into()).await {
            Ok(_) => Ok(IngestOutcome::Accepted),
            Err(StreamError::InvalidTelemetry(_)) => Ok(IngestOutcome::Rejected),
            Err(error) => Err(MqttConsumerError::Stream(error)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MqttRuntimeConfig {
    pub tenant_id: uuid::Uuid,
    pub client_id: String,
    pub broker_host: String,
    pub broker_port: u16,
}

#[derive(Debug, Error)]
pub enum MqttRuntimeError {
    #[error(transparent)]
    Client(#[from] rumqttc::ClientError),
    #[error(transparent)]
    Connection(#[from] rumqttc::ConnectionError),
    #[error(transparent)]
    Consumer(#[from] MqttConsumerError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

async fn ingest_before_ack<F>(
    producer: &MqttStreamProducer,
    topic: &str,
    payload: &[u8],
    received_at: DateTime<Utc>,
    acknowledgement: F,
) -> Result<IngestOutcome, MqttRuntimeError>
where
    F: Future<Output = Result<(), rumqttc::ClientError>>,
{
    let outcome = producer.ingest(topic, payload, received_at).await?;
    acknowledgement.await?;
    Ok(outcome)
}

pub struct MqttRuntime {
    client: AsyncClient,
    event_loop: EventLoop,
    producer: MqttStreamProducer,
    subscribed: bool,
}

impl MqttRuntime {
    pub fn new(config: MqttRuntimeConfig, stream: Arc<dyn StreamPort>) -> Self {
        let mut options =
            MqttOptions::new(config.client_id, config.broker_host, config.broker_port);
        options.set_clean_session(false);
        options.set_manual_acks(true);
        let (client, event_loop) = AsyncClient::new(options, 100);

        Self {
            client,
            event_loop,
            producer: MqttStreamProducer::new(config.tenant_id, stream),
            subscribed: false,
        }
    }

    pub async fn subscribe(&self) -> Result<(), MqttRuntimeError> {
        self.client
            .subscribe("iot/v1/devices/+/telemetry", QoS::AtLeastOnce)
            .await?;
        Ok(())
    }

    pub fn is_subscribed(&self) -> bool {
        self.subscribed
    }

    pub async fn poll_once(
        &mut self,
        received_at: DateTime<Utc>,
    ) -> Result<Option<IngestOutcome>, MqttRuntimeError> {
        match self.event_loop.poll().await? {
            Event::Incoming(Packet::SubAck(_)) => {
                self.subscribed = true;
                Ok(None)
            }
            Event::Incoming(Packet::Publish(publish)) => {
                let outcome = ingest_before_ack(
                    &self.producer,
                    &publish.topic,
                    &publish.payload,
                    received_at,
                    self.client.ack(&publish),
                )
                .await?;
                Ok(Some(outcome))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Instant,
    };

    use chrono::{TimeZone, Utc};
    use iot_core::TelemetryEvent;
    use iot_stream::{
        AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
        HeartbeatRequest, PartitionId, StreamError, StreamMessage, StreamPort,
    };
    use serde_json::json;
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::{IngestOutcome, MqttStreamProducer, ingest_before_ack};

    #[derive(Clone)]
    struct BlockingAppendStream {
        append_started: Arc<Notify>,
        release_append: Arc<Notify>,
        append_count: Arc<AtomicUsize>,
    }

    impl BlockingAppendStream {
        fn new() -> Self {
            Self {
                append_started: Arc::new(Notify::new()),
                release_append: Arc::new(Notify::new()),
                append_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl StreamPort for BlockingAppendStream {
        fn append(
            &self,
            _message: StreamMessage,
        ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
            let append_started = Arc::clone(&self.append_started);
            let release_append = Arc::clone(&self.release_append);
            let append_count = Arc::clone(&self.append_count);
            Box::pin(async move {
                append_started.notify_one();
                release_append.notified().await;
                append_count.fetch_add(1, Ordering::SeqCst);
                Ok(AppendReceipt {
                    partition: PartitionId::new(0),
                    offset: 0,
                })
            })
        }

        fn claim(
            &self,
            _request: ClaimRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>>
        {
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
        ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>>
        {
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

    #[tokio::test]
    async fn durable_append_completes_before_acknowledgement_starts() {
        let stream = BlockingAppendStream::new();
        let producer = MqttStreamProducer::new(Arc::new(stream.clone()));
        let acknowledged = Arc::new(AtomicBool::new(false));
        let now = Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap();
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: "esp-000123".to_owned(),
            boot_id: Uuid::new_v4(),
            sequence: 1,
            event_at: now,
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
            gateway_device_id: None,
        };
        let payload = serde_json::to_vec(&event).unwrap();
        let acknowledged_by_task = Arc::clone(&acknowledged);
        let task = tokio::spawn(async move {
            ingest_before_ack(
                &producer,
                "iot/v1/devices/esp-000123/telemetry",
                &payload,
                now,
                async move {
                    acknowledged_by_task.store(true, Ordering::SeqCst);
                    Ok::<(), rumqttc::ClientError>(())
                },
            )
            .await
        });

        stream.append_started.notified().await;
        assert!(!acknowledged.load(Ordering::SeqCst));
        assert_eq!(stream.append_count.load(Ordering::SeqCst), 0);

        stream.release_append.notify_one();
        assert_eq!(task.await.unwrap().unwrap(), IngestOutcome::Accepted);
        assert_eq!(stream.append_count.load(Ordering::SeqCst), 1);
        assert!(acknowledged.load(Ordering::SeqCst));
    }
}
