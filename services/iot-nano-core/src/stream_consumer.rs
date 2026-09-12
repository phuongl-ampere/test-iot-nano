use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use iot_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment, GroupStart,
    HeartbeatRequest, StreamError, StreamMessage, StreamPort, StreamRecord,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CORE_STREAM_SECRET_HEADER: &str = "x-iot-nano-core-stream-secret";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum HttpStreamConsumerError {
    #[error("stream consumer configuration is invalid: {0}")]
    Configuration(String),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("stream service returned status {0}")]
    Rejected(u16),
}

#[derive(Clone)]
pub struct HttpStreamConsumer {
    client: Client,
    claim_url: String,
    ack_url: String,
    secret: Arc<str>,
    group: String,
    member_id: String,
}

#[derive(Serialize)]
struct LegacyClaimRequest<'a> {
    member_id: &'a str,
    start: &'static str,
    limit: usize,
}

#[derive(Deserialize)]
struct LegacyClaimResponse {
    generation: u64,
    records: Vec<StreamRecord>,
}

#[derive(Serialize)]
struct LegacyAcknowledgeRequest<'a> {
    member_id: &'a str,
    generation: u64,
    commits: Vec<LegacyCommitRequest>,
}

#[derive(Serialize)]
struct LegacyCommitRequest {
    partition: u16,
    next_offset: u64,
}

impl HttpStreamConsumer {
    pub fn new(
        stream_base_url: impl AsRef<str>,
        secret: impl AsRef<str>,
        group: impl AsRef<str>,
        member_id: impl AsRef<str>,
    ) -> Result<Self, HttpStreamConsumerError> {
        let group = group.as_ref();
        let member_id = member_id.as_ref();
        validate_identifier("group", group)?;
        validate_identifier("member ID", member_id)?;
        let secret = secret.as_ref();
        if secret.len() < 32
            || !secret.is_ascii()
            || secret.bytes().any(|value| value.is_ascii_whitespace())
        {
            return Err(HttpStreamConsumerError::Configuration(
                "stream secret must be at least 32 ASCII non-whitespace characters".to_owned(),
            ));
        }
        let base_url = stream_base_url.as_ref().trim_end_matches('/');
        let claim_url = format!("{base_url}/internal/groups/{group}/claim");
        let ack_url = format!("{base_url}/internal/groups/{group}/ack");
        reqwest::Url::parse(&claim_url)
            .map_err(|error| HttpStreamConsumerError::Configuration(error.to_string()))?;
        reqwest::Url::parse(&ack_url)
            .map_err(|error| HttpStreamConsumerError::Configuration(error.to_string()))?;

        Ok(Self {
            client: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .map_err(|error| HttpStreamConsumerError::Configuration(error.to_string()))?,
            claim_url,
            ack_url,
            secret: Arc::from(secret),
            group: group.to_owned(),
            member_id: member_id.to_owned(),
        })
    }

    async fn send_claim(
        &self,
        request: &ClaimRequest,
    ) -> Result<LegacyClaimResponse, HttpStreamConsumerError> {
        self.validate_member(&request.group, &request.member_id)?;
        let start = match request.start {
            GroupStart::Earliest => "earliest",
            GroupStart::Latest => "latest",
        };
        let response = self
            .client
            .post(&self.claim_url)
            .header(CORE_STREAM_SECRET_HEADER, self.secret.as_ref())
            .json(&LegacyClaimRequest {
                member_id: &request.member_id,
                start,
                limit: request.limit,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(HttpStreamConsumerError::Rejected(
                response.status().as_u16(),
            ));
        }
        response
            .json::<LegacyClaimResponse>()
            .await
            .map_err(Into::into)
    }

    async fn send_acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Result<(), HttpStreamConsumerError> {
        self.validate_member(&request.group, &request.member_id)?;
        if request.commits.is_empty() {
            return Ok(());
        }
        let response = self
            .client
            .post(&self.ack_url)
            .header(CORE_STREAM_SECRET_HEADER, self.secret.as_ref())
            .json(&LegacyAcknowledgeRequest {
                member_id: &request.member_id,
                generation: request.generation,
                commits: request
                    .commits
                    .into_iter()
                    .map(|commit| LegacyCommitRequest {
                        partition: commit.partition.get(),
                        next_offset: commit.next_offset,
                    })
                    .collect(),
            })
            .send()
            .await?;
        if response.status() == StatusCode::NO_CONTENT {
            Ok(())
        } else {
            Err(HttpStreamConsumerError::Rejected(
                response.status().as_u16(),
            ))
        }
    }

    fn validate_member(&self, group: &str, member_id: &str) -> Result<(), HttpStreamConsumerError> {
        if group == self.group && member_id == self.member_id {
            Ok(())
        } else {
            Err(HttpStreamConsumerError::Configuration(
                "legacy stream adapter only supports its configured group and member ID".to_owned(),
            ))
        }
    }
}

impl StreamPort for HttpStreamConsumer {
    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(async {
            Err(StreamError::InvalidConfig(
                "legacy HTTP stream adapter is consume-only".to_owned(),
            ))
        })
    }

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        Box::pin(async move {
            let response = self.send_claim(&request).await.map_err(stream_error)?;
            Ok(response
                .records
                .into_iter()
                .map(|record| ClaimedRecord {
                    partition: record.partition,
                    offset: record.offset,
                    message: record.message,
                    generation: response.generation,
                })
                .collect())
        })
    }

    fn acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async move { self.send_acknowledge(request).await.map_err(stream_error) })
    }

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(async move {
            let response = self
                .send_claim(&ClaimRequest {
                    group: request.group,
                    member_id: request.member_id,
                    start: GroupStart::Earliest,
                    limit: 0,
                })
                .await
                .map_err(stream_error)?;
            Ok(GroupAssignment {
                generation: response.generation,
                partitions: response
                    .records
                    .into_iter()
                    .map(|record| record.partition)
                    .collect(),
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

fn stream_error(error: HttpStreamConsumerError) -> StreamError {
    StreamError::InvalidConfig(format!("legacy HTTP stream adapter: {error}"))
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), HttpStreamConsumerError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(HttpStreamConsumerError::Configuration(format!(
            "{kind} must contain only ASCII letters, digits, '-' or '_'"
        )));
    }
    Ok(())
}
