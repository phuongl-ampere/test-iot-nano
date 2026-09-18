use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use iot_core::TelemetryEvent;

use crate::PlatformStoreError;

pub trait TelemetryRepository: Send + Sync {
    fn write_telemetry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TelemetryAggregate {
    pub average: f64,
    pub sample_count: u64,
}

pub trait TelemetryAggregateRepository: Send + Sync {
    fn average_metric<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        metric_key: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TelemetryAggregate>, PlatformStoreError>> + Send + 'a,
        >,
    >;
}
