use chrono::{DateTime, Utc};
use iot_storage::{
    GatewayIngestEventKind, GatewayIngestRepository, GatewayIngestRequest, PlatformStoreError,
    TelemetryRepository,
};
use iot_stream::{GatewayEventKind, GatewayMessage, StreamError, StreamMessage};
use thiserror::Error;

use crate::{ClaimedBatch, CoreStreamConsumer};

pub struct PlatformTelemetryWriter<S> {
    store: S,
    batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushResult {
    pub read: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub committed_partitions: usize,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    Platform(#[from] PlatformStoreError),
}

impl<S> PlatformTelemetryWriter<S>
where
    S: TelemetryRepository + GatewayIngestRepository,
{
    pub fn new(store: S, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_once(
        &self,
        consumer: &CoreStreamConsumer,
        _now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.claim(self.batch_size).await?;
        let mut result = self.flush_batch(&batch).await?;
        let committed_partitions = batch.committed_partitions();
        consumer.acknowledge(&batch).await?;
        result.committed_partitions = committed_partitions;
        Ok(result)
    }

    async fn flush_batch(&self, batch: &ClaimedBatch) -> Result<FlushResult, WriterError> {
        if batch.is_empty() {
            return Ok(empty_flush_result());
        }

        let mut inserted = 0;
        let mut telemetry_records = 0;
        for record in batch.records() {
            match &record.message {
                StreamMessage::Gateway(message) => {
                    if message.telemetry_event.is_some() {
                        telemetry_records += 1;
                    }
                    let result = self
                        .store
                        .ingest_gateway(gateway_ingest_request(message))
                        .await?;
                    if message.telemetry_event.is_some() {
                        inserted += usize::from(result.telemetry_inserted);
                    }
                }
                StreamMessage::Telemetry(message) => {
                    telemetry_records += 1;
                    inserted += usize::from(
                        self.store
                            .write_telemetry(
                                message.tenant_id,
                                &message.event,
                                message.received_at,
                                &message.topic,
                            )
                            .await?,
                    );
                }
            }
        }

        Ok(FlushResult {
            read: batch.records().len(),
            inserted,
            duplicates: telemetry_records - inserted,
            committed_partitions: 0,
        })
    }
}

fn gateway_ingest_request(message: &GatewayMessage) -> GatewayIngestRequest {
    GatewayIngestRequest {
        tenant_id: message.tenant_id,
        gateway_device_id: message.gateway_event.gateway_device_id.clone(),
        child_device_id: message.gateway_event.child_device_id.clone(),
        event_kind: match message.gateway_event.event_kind {
            GatewayEventKind::Connect => GatewayIngestEventKind::Connect,
            GatewayEventKind::Disconnect => GatewayIngestEventKind::Disconnect,
            GatewayEventKind::Heartbeat => GatewayIngestEventKind::Heartbeat,
            GatewayEventKind::ChildTelemetry => GatewayIngestEventKind::ChildTelemetry,
        },
        event_at: message.gateway_event.event_at,
        idempotency_key: message.gateway_event.idempotency_key.clone(),
        telemetry_event: message.telemetry_event.clone(),
        topic: message.topic.clone(),
        received_at: message.received_at,
    }
}

fn empty_flush_result() -> FlushResult {
    FlushResult {
        read: 0,
        inserted: 0,
        duplicates: 0,
        committed_partitions: 0,
    }
}
