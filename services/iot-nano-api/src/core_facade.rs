use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreTelemetryBucket,
    CoreTelemetryPoint,
};

#[derive(Debug, Clone)]
pub struct CoreTelemetryQuery {
    pub device_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket: CoreTelemetryBucket,
}

#[derive(Debug, Error)]
pub enum CoreFacadeError {
    #[error("core command was not found")]
    NotFound,
    #[error("core command service returned status {0}")]
    Rejected(u16),
    #[error("core command service is unavailable")]
    Unavailable,
}

pub trait CoreFacade: Send + Sync {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>>;

    fn get_command(
        &self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>>;

    fn record_command_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>>;

    fn telemetry(
        &self,
        query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>;
}
