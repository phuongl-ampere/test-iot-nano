use std::{io, time::Instant};

use clap::Parser;
use iot_nano_monolith::{MonolithConfig, MonolithRuntime};

#[derive(Debug, Parser)]
#[command(name = "iot-nano-monolith")]
struct Arguments {
    #[arg(long, conflicts_with = "migrate_only")]
    config_check: bool,
    #[arg(long)]
    migrate_only: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = Arguments::parse();
    let configuration = MonolithConfig::from_env()?;

    if arguments.config_check {
        return Ok(());
    }

    if arguments.migrate_only {
        return Err(io::Error::other(
            "migrations are not available until platform storage is wired",
        )
        .into());
    }

    let mut runtime = MonolithRuntime::start(configuration.clone()).await?;
    let cancellation = runtime.cancellation_token();
    let failure = runtime.failure_token();
    let child_failed = tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            false
        }
        _ = shutdown_signal() => false,
        _ = cancellation.cancelled() => false,
        _ = failure.cancelled() => true,
    };
    runtime
        .shutdown(Instant::now() + configuration.shutdown_deadline)
        .await?;
    runtime_exit_result(child_failed)?;
    Ok(())
}

fn runtime_exit_result(child_failed: bool) -> io::Result<()> {
    if child_failed {
        Err(io::Error::other("a monolith runtime child failed"))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler must install");
    terminate.recv().await;
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use super::runtime_exit_result;

    #[test]
    fn child_failure_requires_a_nonzero_process_exit() {
        assert!(runtime_exit_result(false).is_ok());
        assert!(runtime_exit_result(true).is_err());
    }
}
