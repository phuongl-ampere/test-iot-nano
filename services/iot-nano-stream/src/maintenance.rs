use crate::{LocalStream, RetentionResult, StreamError};

pub async fn enforce_retention(stream: LocalStream) -> Result<RetentionResult, StreamError> {
    tokio::task::spawn_blocking(move || stream.enforce_retention(chrono::Utc::now()))
        .await
        .map_err(|error| StreamError::Io(std::io::Error::other(error.to_string())))?
}
