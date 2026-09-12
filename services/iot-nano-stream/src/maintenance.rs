use crate::{LocalStream, RetentionResult, StreamError};

pub async fn enforce_retention(stream: LocalStream) -> Result<RetentionResult, StreamError> {
    stream.enforce_retention(chrono::Utc::now()).await
}
