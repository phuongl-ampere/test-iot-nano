use std::{future::Future, path::PathBuf, pin::Pin, process::Stdio};

use iot_core::{SystemConfiguration, SystemConfigurationUpdate};
use thiserror::Error;
use tokio::{io::AsyncWriteExt, process::Command};

pub trait SystemConfigurationService: Send + Sync {
    fn read(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    >;
    fn apply(
        &self,
        update: SystemConfigurationUpdate,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    >;
}

#[derive(Debug, Error)]
pub enum SystemConfigurationServiceError {
    #[error("system configuration is invalid: {0}")]
    Validation(String),
    #[error("system configuration helper is unavailable: {0}")]
    Unavailable(String),
    #[error("system configuration helper returned invalid JSON")]
    Serialization(#[source] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct HelperSystemConfigurationService {
    helper_path: PathBuf,
    use_sudo: bool,
}

impl HelperSystemConfigurationService {
    pub fn new(helper_path: PathBuf) -> Self {
        Self {
            helper_path,
            use_sudo: true,
        }
    }

    pub fn direct(helper_path: PathBuf) -> Self {
        Self {
            helper_path,
            use_sudo: false,
        }
    }

    async fn invoke(
        &self,
        command: &str,
        input: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, SystemConfigurationServiceError> {
        let mut process = if self.use_sudo {
            let mut process = Command::new("sudo");
            process.arg("-n").arg(&self.helper_path);
            process
        } else {
            Command::new(&self.helper_path)
        };
        process
            .arg(command)
            .stdin(
                input
                    .is_some()
                    .then_some(Stdio::piped())
                    .unwrap_or_else(Stdio::null),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = process
            .spawn()
            .map_err(|error| SystemConfigurationServiceError::Unavailable(error.to_string()))?;

        if let Some(input) = input {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                SystemConfigurationServiceError::Unavailable(
                    "helper stdin was unavailable".to_owned(),
                )
            })?;
            stdin
                .write_all(&input)
                .await
                .map_err(|error| SystemConfigurationServiceError::Unavailable(error.to_string()))?;
            stdin
                .shutdown()
                .await
                .map_err(|error| SystemConfigurationServiceError::Unavailable(error.to_string()))?;
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|error| SystemConfigurationServiceError::Unavailable(error.to_string()))?;
        if output.status.success() {
            return Ok(output.stdout);
        }

        let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if output.status.code() == Some(2) {
            Err(SystemConfigurationServiceError::Validation(message))
        } else {
            Err(SystemConfigurationServiceError::Unavailable(message))
        }
    }
}

impl SystemConfigurationService for HelperSystemConfigurationService {
    fn read(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let output = self.invoke("read", None).await?;
            serde_json::from_slice(&output).map_err(SystemConfigurationServiceError::Serialization)
        })
    }

    fn apply(
        &self,
        update: SystemConfigurationUpdate,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let input = serde_json::to_vec(&update)
                .map_err(|error| SystemConfigurationServiceError::Unavailable(error.to_string()))?;
            let output = self.invoke("apply", Some(input)).await?;
            serde_json::from_slice(&output).map_err(SystemConfigurationServiceError::Serialization)
        })
    }
}
