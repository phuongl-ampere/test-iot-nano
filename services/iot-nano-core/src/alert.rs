use chrono::{DateTime, Utc};
use iot_storage::{AlertEvaluationEvent, AlertEvaluationRepository, PlatformStoreError};
use iot_stream::{ClaimedRecord, StreamError};
use thiserror::Error;

use crate::CoreStreamConsumer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlertFlushResult {
    pub read: usize,
    pub evaluated: usize,
    pub opened: usize,
    pub resolved: usize,
    pub reminders: usize,
}

#[derive(Debug, Error)]
pub enum AlertError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    Store(#[from] PlatformStoreError),
}

pub struct PlatformAlertEvaluator<S> {
    store: S,
    batch_size: usize,
}

impl<S> PlatformAlertEvaluator<S>
where
    S: AlertEvaluationRepository,
{
    pub fn new(store: S, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_event_rules(
        &self,
        consumer: &CoreStreamConsumer,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let batch = consumer.claim(self.batch_size).await?;
        if batch.is_empty() {
            return Ok(empty_alert_flush_result());
        }

        let events = batch
            .records()
            .iter()
            .filter_map(platform_alert_event)
            .collect::<Vec<_>>();
        let evaluated = self.store.evaluate_alert_events(&events, now).await?;
        let result = AlertFlushResult {
            read: batch.records().len(),
            evaluated: evaluated.evaluated,
            opened: evaluated.opened,
            resolved: evaluated.resolved,
            reminders: evaluated.reminders,
        };
        consumer.acknowledge(&batch).await?;
        Ok(result)
    }

    pub async fn flush_window_rules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let evaluated = self.store.evaluate_alert_windows(now).await?;
        Ok(AlertFlushResult {
            read: 0,
            evaluated: evaluated.evaluated,
            opened: evaluated.opened,
            resolved: evaluated.resolved,
            reminders: evaluated.reminders,
        })
    }
}

fn platform_alert_event(record: &ClaimedRecord) -> Option<AlertEvaluationEvent> {
    let (event, received_at, _) = record.message.telemetry_parts()?;
    Some(AlertEvaluationEvent {
        event_at: event.event_at,
        received_at,
        tenant_id: record.message.tenant_id(),
        device_id: event.device_id.clone(),
        boot_id: event.boot_id,
        sequence: event.sequence,
        measurements: event.measurements.clone(),
    })
}

fn empty_alert_flush_result() -> AlertFlushResult {
    AlertFlushResult {
        read: 0,
        evaluated: 0,
        opened: 0,
        resolved: 0,
        reminders: 0,
    }
}
