use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use iot_core::RpcMode;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{CoreFacade, CoreFacadeError, CoreTelemetryQuery};

const CORE_SECRET_HEADER: &str = "x-iot-nano-api-core-secret";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize)]
pub struct CoreCommandCreateRequest {
    pub id: Uuid,
    pub device_id: String,
    pub method: String,
    pub params: serde_json::Value,
    pub mode: RpcMode,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoreCommandResponseRequest {
    pub command_id: Uuid,
    pub device_id: String,
    pub response: serde_json::Value,
    pub responded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreCommandRecord {
    pub id: Uuid,
    pub device_id: String,
    pub state: String,
    pub expires_at: DateTime<Utc>,
    pub mode: RpcMode,
    pub response: Option<serde_json::Value>,
    pub responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreTelemetryBucket {
    Raw,
    FiveMinutes,
    OneHour,
}

impl CoreTelemetryBucket {
    fn as_query_value(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::FiveMinutes => "5m",
            Self::OneHour => "1h",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreTelemetryPoint {
    pub at: DateTime<Utc>,
    pub temperature_c: Option<f64>,
    pub humidity_pct: Option<f64>,
    pub event_count: i64,
}

#[derive(Debug, Error)]
pub enum CoreClientError {
    #[error("core command client configuration is invalid: {0}")]
    Configuration(String),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("core command service returned status {0}")]
    Rejected(u16),
    #[error("core command was not found")]
    NotFound,
}

#[derive(Clone)]
pub struct CoreClient {
    client: Client,
    base_url: String,
    commands_url: String,
    secret: Arc<str>,
}

impl CoreClient {
    pub fn new(
        core_base_url: impl AsRef<str>,
        secret: impl AsRef<str>,
    ) -> Result<Self, CoreClientError> {
        let secret = secret.as_ref();
        if secret.len() < 32
            || !secret.is_ascii()
            || secret.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(CoreClientError::Configuration(
                "core secret must be at least 32 ASCII non-whitespace characters".to_owned(),
            ));
        }
        let commands_url = format!(
            "{}/internal/commands",
            core_base_url.as_ref().trim_end_matches('/')
        );
        reqwest::Url::parse(&commands_url)
            .map_err(|error| CoreClientError::Configuration(error.to_string()))?;
        Ok(Self {
            client: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .map_err(|error| CoreClientError::Configuration(error.to_string()))?,
            base_url: core_base_url.as_ref().trim_end_matches('/').to_owned(),
            commands_url,
            secret: Arc::from(secret),
        })
    }

    pub async fn create(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Result<CoreCommandRecord, CoreClientError> {
        let response = self
            .client
            .post(&self.commands_url)
            .header(CORE_SECRET_HEADER, self.secret.as_ref())
            .json(&request)
            .send()
            .await?;
        if response.status() == StatusCode::ACCEPTED {
            response.json().await.map_err(Into::into)
        } else {
            Err(status_error(response.status()))
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<CoreCommandRecord, CoreClientError> {
        let response = self
            .client
            .get(format!("{}/{}", self.commands_url, id))
            .header(CORE_SECRET_HEADER, self.secret.as_ref())
            .send()
            .await?;
        if response.status() == StatusCode::OK {
            response.json().await.map_err(Into::into)
        } else {
            Err(status_error(response.status()))
        }
    }

    pub async fn record_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Result<(), CoreClientError> {
        let response = self
            .client
            .post(format!("{}/response", self.commands_url))
            .header(CORE_SECRET_HEADER, self.secret.as_ref())
            .json(&request)
            .send()
            .await?;
        if response.status() == StatusCode::NO_CONTENT {
            Ok(())
        } else {
            Err(status_error(response.status()))
        }
    }

    pub async fn telemetry(
        &self,
        device_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket: CoreTelemetryBucket,
    ) -> Result<Vec<CoreTelemetryPoint>, CoreClientError> {
        let response = self
            .client
            .get(format!(
                "{}/internal/telemetry/devices/{}",
                self.base_url, device_id
            ))
            .header(CORE_SECRET_HEADER, self.secret.as_ref())
            .query(&[
                ("from", from.to_rfc3339()),
                ("to", to.to_rfc3339()),
                ("bucket", bucket.as_query_value().to_owned()),
            ])
            .send()
            .await?;
        if response.status() == StatusCode::OK {
            response.json().await.map_err(Into::into)
        } else {
            Err(status_error(response.status()))
        }
    }
}

impl CoreFacade for CoreClient {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async move {
            CoreClient::create(self, request)
                .await
                .map_err(core_facade_error)
        })
    }

    fn get_command(
        &self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async move { CoreClient::get(self, id).await.map_err(core_facade_error) })
    }

    fn record_command_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>> {
        Box::pin(async move {
            CoreClient::record_response(self, request)
                .await
                .map_err(core_facade_error)
        })
    }

    fn telemetry(
        &self,
        query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        Box::pin(async move {
            CoreClient::telemetry(self, &query.device_id, query.from, query.to, query.bucket)
                .await
                .map_err(core_facade_error)
        })
    }
}

fn core_facade_error(error: CoreClientError) -> CoreFacadeError {
    match error {
        CoreClientError::NotFound => CoreFacadeError::NotFound,
        CoreClientError::Rejected(status) => CoreFacadeError::Rejected(status),
        CoreClientError::Configuration(_) | CoreClientError::Request(_) => {
            CoreFacadeError::Unavailable
        }
    }
}

fn status_error(status: StatusCode) -> CoreClientError {
    if status == StatusCode::NOT_FOUND {
        CoreClientError::NotFound
    } else {
        CoreClientError::Rejected(status.as_u16())
    }
}
