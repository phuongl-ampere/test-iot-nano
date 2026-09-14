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
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = shutdown_signal() => {}
    }
    runtime
        .shutdown(Instant::now() + configuration.shutdown_deadline)
        .await?;
    Ok(())
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
