use std::sync::Arc;

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
    stream: Arc<dyn StreamPort>,
}

impl MqttStreamProducer {
    pub fn new(stream: Arc<dyn StreamPort>) -> Self {
        Self { stream }
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
            producer: MqttStreamProducer::new(stream),
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
                let outcome = self
                    .producer
                    .ingest(&publish.topic, &publish.payload, received_at)
                    .await?;
                self.client.ack(&publish).await?;
                Ok(Some(outcome))
            }
            _ => Ok(None),
        }
    }
}
