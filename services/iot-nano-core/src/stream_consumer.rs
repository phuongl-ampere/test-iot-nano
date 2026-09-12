use std::{sync::Arc, time::Duration};

use iot_stream::{PartitionCommit, PartitionId, PollBatch, StreamRecord};
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
    member_id: String,
}

#[derive(Serialize)]
struct ClaimRequest<'a> {
    member_id: &'a str,
    start: &'static str,
    limit: usize,
}

#[derive(Deserialize)]
struct ClaimResponse {
    generation: u64,
    records: Vec<StreamRecord>,
    commits: Vec<CommitResponse>,
}

#[derive(Deserialize)]
struct CommitResponse {
    partition: u16,
    next_offset: u64,
}

#[derive(Serialize)]
struct AckRequest<'a> {
    member_id: &'a str,
    generation: u64,
    commits: Vec<CommitRequest>,
}

#[derive(Serialize)]
struct CommitRequest {
    partition: u16,
    next_offset: u64,
}

impl HttpStreamConsumer {
    pub fn new(
        stream_base_url: &str,
        secret: impl AsRef<str>,
        group: &str,
        member_id: &str,
    ) -> Result<Self, HttpStreamConsumerError> {
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
        let base_url = stream_base_url.trim_end_matches('/');
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
            member_id: member_id.to_owned(),
        })
    }

    pub async fn claim(&self, limit: usize) -> Result<PollBatch, HttpStreamConsumerError> {
        let response = self
            .client
            .post(&self.claim_url)
            .header(CORE_STREAM_SECRET_HEADER, self.secret.as_ref())
            .json(&ClaimRequest {
                member_id: &self.member_id,
                start: "earliest",
                limit,
            })
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(HttpStreamConsumerError::Rejected(
                response.status().as_u16(),
            ));
        }
        let response = response.json::<ClaimResponse>().await?;
        Ok(PollBatch {
            generation: response.generation,
            records: response.records,
            commits: response
                .commits
                .into_iter()
                .map(|commit| PartitionCommit {
                    partition: PartitionId::new(commit.partition),
                    next_offset: commit.next_offset,
                })
                .collect(),
        })
    }

    pub async fn acknowledge(&self, batch: &PollBatch) -> Result<(), HttpStreamConsumerError> {
        if batch.commits.is_empty() {
            return Ok(());
        }
        let response = self
            .client
            .post(&self.ack_url)
            .header(CORE_STREAM_SECRET_HEADER, self.secret.as_ref())
            .json(&AckRequest {
                member_id: &self.member_id,
                generation: batch.generation,
                commits: batch
                    .commits
                    .iter()
                    .map(|commit| CommitRequest {
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

    pub async fn heartbeat(&self) -> Result<(), HttpStreamConsumerError> {
        self.claim(0).await.map(|_| ())
    }
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
