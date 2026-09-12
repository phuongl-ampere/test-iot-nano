use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    AppendedRecord, GatewayMessage, GroupStart, LocalStream, PartitionCommit, PartitionId,
    PollBatch, StreamMessage, TelemetryMessage,
};

#[derive(Clone)]
pub struct StreamHttpState {
    stream: LocalStream,
    mqttd_secret: Arc<str>,
    core_secret: Arc<str>,
}

impl StreamHttpState {
    pub fn new(
        stream: LocalStream,
        mqttd_secret: impl AsRef<str>,
        core_secret: impl AsRef<str>,
    ) -> Self {
        Self {
            stream,
            mqttd_secret: Arc::from(mqttd_secret.as_ref()),
            core_secret: Arc::from(core_secret.as_ref()),
        }
    }
}

#[derive(Debug, Serialize)]
struct AppendResponse {
    partition: u16,
    offset: u64,
}

#[derive(Debug, Deserialize)]
struct ClaimRequest {
    member_id: String,
    #[serde(default)]
    start: ClaimStart,
    #[serde(default = "default_claim_limit")]
    limit: usize,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClaimStart {
    #[default]
    Earliest,
    Latest,
}

#[derive(Debug, Serialize)]
struct ClaimResponse {
    generation: u64,
    records: Vec<crate::StreamRecord>,
    commits: Vec<CommitResponse>,
}

#[derive(Debug, Serialize)]
struct CommitResponse {
    partition: u16,
    next_offset: u64,
}

#[derive(Debug, Deserialize)]
struct AckRequest {
    member_id: String,
    generation: u64,
    commits: Vec<CommitRequest>,
}

#[derive(Debug, Deserialize)]
struct CommitRequest {
    partition: u16,
    next_offset: u64,
}

pub fn router(state: StreamHttpState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/internal/streams/telemetry/append", post(append_telemetry))
        .route("/internal/streams/gateway/append", post(append_gateway))
        .route("/internal/groups/{group}/claim", post(claim_group))
        .route("/internal/groups/{group}/ack", post(ack_group))
        .with_state(state)
}

async fn healthz() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok\n")
}

async fn append_telemetry(
    State(state): State<StreamHttpState>,
    headers: HeaderMap,
    Json(message): Json<TelemetryMessage>,
) -> Result<Json<AppendResponse>, StatusCode> {
    authorize_mqttd(&state, &headers)?;
    let AppendedRecord { partition, offset } = state
        .stream
        .append(StreamMessage::Telemetry(message))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(AppendResponse {
        partition: partition.get(),
        offset,
    }))
}

async fn append_gateway(
    State(state): State<StreamHttpState>,
    headers: HeaderMap,
    Json(message): Json<GatewayMessage>,
) -> Result<Json<AppendResponse>, StatusCode> {
    authorize_mqttd(&state, &headers)?;
    let AppendedRecord { partition, offset } = state
        .stream
        .append(StreamMessage::Gateway(message))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(AppendResponse {
        partition: partition.get(),
        offset,
    }))
}

fn authorize_mqttd(state: &StreamHttpState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let supplied = headers
        .get("x-iot-nano-mqttd-stream-secret")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    constant_time_equal(state.mqttd_secret.as_bytes(), supplied.as_bytes())
        .then_some(())
        .ok_or(StatusCode::UNAUTHORIZED)
}

fn authorize_core(state: &StreamHttpState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let supplied = headers
        .get("x-iot-nano-core-stream-secret")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    constant_time_equal(state.core_secret.as_bytes(), supplied.as_bytes())
        .then_some(())
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn claim_group(
    State(state): State<StreamHttpState>,
    Path(group): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ClaimRequest>,
) -> Result<Json<ClaimResponse>, StatusCode> {
    authorize_core(&state, &headers)?;
    let start = match request.start {
        ClaimStart::Earliest => GroupStart::Earliest,
        ClaimStart::Latest => GroupStart::Latest,
    };
    let mut consumer = state
        .stream
        .join_group(&group, &request.member_id, start, Utc::now())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    consumer
        .heartbeat(Utc::now())
        .map_err(|_| StatusCode::CONFLICT)?;
    let batch = consumer
        .poll(request.limit, Utc::now())
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok(Json(ClaimResponse {
        generation: batch.generation,
        records: batch.records,
        commits: batch
            .commits
            .into_iter()
            .map(|commit| CommitResponse {
                partition: commit.partition.get(),
                next_offset: commit.next_offset,
            })
            .collect(),
    }))
}

async fn ack_group(
    State(state): State<StreamHttpState>,
    Path(group): Path<String>,
    headers: HeaderMap,
    Json(request): Json<AckRequest>,
) -> Result<StatusCode, StatusCode> {
    authorize_core(&state, &headers)?;
    let mut consumer = state
        .stream
        .join_group(&group, &request.member_id, GroupStart::Earliest, Utc::now())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    consumer
        .heartbeat(Utc::now())
        .map_err(|_| StatusCode::CONFLICT)?;
    let batch = PollBatch {
        generation: request.generation,
        records: Vec::new(),
        commits: request
            .commits
            .into_iter()
            .map(|commit| PartitionCommit {
                partition: PartitionId::new(commit.partition),
                next_offset: commit.next_offset,
            })
            .collect(),
    };
    consumer
        .commit(batch, Utc::now())
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok(StatusCode::NO_CONTENT)
}

fn default_claim_limit() -> usize {
    100
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
