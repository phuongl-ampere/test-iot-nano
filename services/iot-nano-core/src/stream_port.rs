use std::sync::Arc;

use iot_stream::{
    AcknowledgeRequest, ClaimRequest, ClaimedRecord, GroupStart, HeartbeatRequest, StreamError,
    StreamPort,
};

#[derive(Clone)]
pub struct CoreStreamConsumer {
    stream: Arc<dyn StreamPort>,
    group: String,
    member_id: String,
    start: GroupStart,
}

#[derive(Debug, Clone)]
pub struct ClaimedBatch {
    records: Vec<ClaimedRecord>,
}

impl ClaimedBatch {
    pub fn records(&self) -> &[ClaimedRecord] {
        &self.records
    }

    pub fn committed_partitions(&self) -> usize {
        AcknowledgeRequest::from_claims("", "", &self.records)
            .commits
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl CoreStreamConsumer {
    pub fn new(
        stream: Arc<dyn StreamPort>,
        group: impl Into<String>,
        member_id: impl Into<String>,
    ) -> Self {
        Self {
            stream,
            group: group.into(),
            member_id: member_id.into(),
            start: GroupStart::Earliest,
        }
    }

    pub fn with_start(mut self, start: GroupStart) -> Self {
        self.start = start;
        self
    }

    pub async fn claim(&self, limit: usize) -> Result<ClaimedBatch, StreamError> {
        let records = self
            .stream
            .claim(ClaimRequest {
                group: self.group.clone(),
                member_id: self.member_id.clone(),
                start: self.start,
                limit,
            })
            .await?;
        Ok(ClaimedBatch { records })
    }

    pub async fn acknowledge(&self, batch: &ClaimedBatch) -> Result<(), StreamError> {
        if batch.is_empty() {
            return Ok(());
        }
        self.stream
            .acknowledge(AcknowledgeRequest::from_claims(
                &self.group,
                &self.member_id,
                batch.records(),
            ))
            .await
    }

    pub async fn heartbeat(&self) -> Result<(), StreamError> {
        match self
            .stream
            .heartbeat(HeartbeatRequest::new(
                self.group.clone(),
                self.member_id.clone(),
            ))
            .await
        {
            Ok(_) => Ok(()),
            Err(StreamError::GroupMemberNotFound { .. } | StreamError::LeaseExpired { .. }) => {
                self.claim(0).await.map(|_| ())
            }
            Err(error) => Err(error),
        }
    }
}
