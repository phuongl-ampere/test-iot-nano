use std::{
    collections::BTreeMap, env, error::Error, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
};

use axum::{Router, response::IntoResponse, routing::get};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::Parser;
use iot_core::{DatabaseStorage, IngestTuning, StorageConfiguration};
use iot_ingest::{
    AlertEvaluator, CommandDispatcher, EmailSender, HttpTransportRpcClient, IngestMetrics,
    MqttRuntime, MqttRuntimeConfig, NotificationDispatcher, ReloadingSmtpEmailSender, SmtpConfig,
    SqliteAlertEvaluator, SqliteCommandDispatcher, SqliteNotificationDispatcher,
    SqliteTelemetryWriter, SqliteTokenWebhookIngress, TelemetryWriter, TokenWebhookIngress,
    TransportRpcClient, WriterError, migrate, sqlite_webhook_router_with_transport_secret,
    webhook_router_with_transport_secret,
};
use iot_storage::SqliteStore;
use iot_stream::{GroupStart, LocalStream, StreamConfig, StreamConsumer};
use sqlx::{PgPool, Row};
use tokio::{
    task::JoinSet,
    time::{MissedTickBehavior, interval, sleep},
};

type TaskResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Debug, Parser)]
#[command(about = "Durably ingest MQTT telemetry into TimescaleDB")]
struct Arguments {
    #[arg(long, env = "MQTT_BROKER_HOST", default_value = "127.0.0.1")]
    broker_host: String,
    #[arg(long, env = "MQTT_BROKER_PORT", default_value_t = 1883)]
    broker_port: u16,
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(
        long,
        env = "IOT_STREAM_DIR",
        default_value = "/var/lib/iot-ingest/stream"
    )]
    stream_dir: PathBuf,
    #[arg(
        long,
        env = "IOT_INGEST_HEALTH_ADDRESS",
        default_value = "127.0.0.1:8081"
    )]
    health_address: SocketAddr,
    #[arg(long, env = "IOT_STREAM_PARTITIONS", default_value_t = 8)]
    stream_partitions: u16,
    #[arg(
        long,
        env = "IOT_STREAM_SEGMENT_BYTES",
        default_value_t = 128 * 1024 * 1024
    )]
    stream_segment_bytes: u64,
    #[arg(
        long,
        env = "IOT_STREAM_RETENTION_BYTES",
        default_value_t = 2 * 1024 * 1024 * 1024
    )]
    stream_retention_bytes: u64,
    #[arg(
        long,
        env = "IOT_STREAM_RETENTION_SECONDS",
        default_value_t = 24 * 60 * 60
    )]
    stream_retention_seconds: u64,
    #[arg(
        long,
        env = "IOT_STREAM_MAX_RECORD_BYTES",
        default_value_t = 1024 * 1024
    )]
    stream_max_record_bytes: usize,
    #[arg(long, env = "IOT_WRITER_GROUP", default_value = "timescaledb-writer")]
    writer_group: String,
    #[arg(long, env = "IOT_ALERT_GROUP", default_value = "alert-evaluator")]
    alert_group: String,
    #[arg(long, env = "IOT_WRITER_MEMBER_ID")]
    writer_member_id: Option<String>,
    #[arg(long, env = "IOT_WRITER_BATCH_SIZE", default_value_t = 1_000)]
    writer_batch_size: u64,
    #[arg(long, env = "IOT_ALERT_BATCH_SIZE", default_value_t = 250)]
    alert_batch_size: u64,
    #[arg(long, env = "IOT_NOTIFICATION_BATCH_SIZE", default_value_t = 10)]
    notification_batch_size: u64,
    #[arg(long, env = "IOT_COMMAND_BATCH_SIZE", default_value_t = 10)]
    command_batch_size: u32,
    #[arg(long, env = "IOT_WRITER_FLUSH_SECONDS", default_value_t = 1)]
    writer_flush_seconds: u64,
    #[arg(
        long,
        env = "IOT_ALERT_EVENT_INTERVAL_MILLISECONDS",
        default_value_t = 250
    )]
    alert_event_interval_milliseconds: u64,
    #[arg(long, env = "IOT_ALERT_WINDOW_INTERVAL_SECONDS", default_value_t = 60)]
    alert_window_interval_seconds: u64,
    #[arg(long, env = "IOT_NOTIFICATION_INTERVAL_SECONDS", default_value_t = 1)]
    notification_interval_seconds: u64,
    #[arg(long, env = "IOT_COMMAND_INTERVAL_SECONDS", default_value_t = 1)]
    command_interval_seconds: u64,
    #[arg(long, env = "IOT_RETENTION_INTERVAL_SECONDS", default_value_t = 60)]
    retention_interval_seconds: u64,
    #[arg(long, env = "IOT_SQLITE_RAW_RETENTION_DAYS", default_value_t = 30)]
    sqlite_raw_retention_days: u64,
    #[arg(long, env = "IOT_SQLITE_ROLLUP_RETENTION_DAYS", default_value_t = 365)]
    sqlite_rollup_retention_days: u64,
    #[arg(
        long,
        env = "IOT_SQLITE_MAINTENANCE_BATCH_SIZE",
        default_value_t = 1_000
    )]
    sqlite_maintenance_batch_size: u64,
    #[arg(long, env = "IOT_NOTIFICATION_LEASE_SECONDS", default_value_t = 30)]
    notification_lease_seconds: u64,
    #[arg(long, env = "IOT_NOTIFICATION_RETRY_BASE_SECONDS", default_value_t = 1)]
    notification_retry_base_seconds: u64,
    #[arg(
        long,
        env = "IOT_NOTIFICATION_RETRY_MAX_SECONDS",
        default_value_t = 3_600
    )]
    notification_retry_max_seconds: u64,
    #[arg(
        long,
        env = "IOT_SMTP_CONFIG_PATH",
        default_value = "/etc/rush-iot-nano/smtp.env"
    )]
    smtp_config_path: PathBuf,
    #[arg(long, env = "IOT_NANOMQ_WEBHOOK_SECRET")]
    nanomq_webhook_secret: String,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_URL")]
    mqtt_transport_url: String,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_SECRET")]
    mqtt_transport_secret: String,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET")]
    mqtt_transport_ingest_webhook_secret: String,
    #[arg(
        long,
        env = "IOT_NANOMQ_WEBHOOK_INBOX_DIR",
        default_value = "/var/lib/iot-ingest/webhook-inbox"
    )]
    nanomq_webhook_inbox_dir: PathBuf,
    #[arg(long, env = "IOT_LEGACY_MQTT_INGRESS", default_value_t = false)]
    legacy_mqtt_ingress: bool,
}

impl Arguments {
    fn tuning(&self) -> IngestTuning {
        IngestTuning {
            retention_bytes: self.stream_retention_bytes,
            retention_seconds: self.stream_retention_seconds,
            segment_bytes: self.stream_segment_bytes,
            max_record_bytes: self.stream_max_record_bytes as u64,
            writer_batch_size: self.writer_batch_size,
            alert_batch_size: self.alert_batch_size,
            notification_batch_size: self.notification_batch_size,
            writer_flush_seconds: self.writer_flush_seconds,
            alert_event_interval_milliseconds: self.alert_event_interval_milliseconds,
            alert_window_interval_seconds: self.alert_window_interval_seconds,
            notification_interval_seconds: self.notification_interval_seconds,
            retention_interval_seconds: self.retention_interval_seconds,
            notification_lease_seconds: self.notification_lease_seconds,
            notification_retry_base_seconds: self.notification_retry_base_seconds,
            notification_retry_max_seconds: self.notification_retry_max_seconds,
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.tuning()
            .validate()
            .map_err(|error| error.to_string())?;
        for (name, value) in [
            ("IOT_WRITER_BATCH_SIZE", self.writer_batch_size),
            ("IOT_ALERT_BATCH_SIZE", self.alert_batch_size),
            ("IOT_NOTIFICATION_BATCH_SIZE", self.notification_batch_size),
        ] {
            if usize::try_from(value).is_err() {
                return Err(format!("{name} does not fit this platform"));
            }
        }
        if self.command_batch_size == 0 {
            return Err("IOT_COMMAND_BATCH_SIZE must be positive".to_owned());
        }
        if self.command_interval_seconds == 0 {
            return Err("IOT_COMMAND_INTERVAL_SECONDS must be positive".to_owned());
        }
        if self.nanomq_webhook_secret.len() < 32
            || !self.nanomq_webhook_secret.is_ascii()
            || self
                .nanomq_webhook_secret
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
        {
            return Err(
                "IOT_NANOMQ_WEBHOOK_SECRET must be at least 32 ASCII non-whitespace characters"
                    .to_owned(),
            );
        }
        if self.mqtt_transport_secret.len() < 32
            || !self.mqtt_transport_secret.is_ascii()
            || self
                .mqtt_transport_secret
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
        {
            return Err(
                "IOT_MQTT_TRANSPORT_SECRET must be at least 32 ASCII non-whitespace characters"
                    .to_owned(),
            );
        }
        if self.mqtt_transport_ingest_webhook_secret.len() < 32
            || !self.mqtt_transport_ingest_webhook_secret.is_ascii()
            || self
                .mqtt_transport_ingest_webhook_secret
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
        {
            return Err(
                "IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET must be at least 32 ASCII non-whitespace characters"
                    .to_owned(),
            );
        }
        for (name, value) in [
            (
                "IOT_SQLITE_RAW_RETENTION_DAYS",
                self.sqlite_raw_retention_days,
            ),
            (
                "IOT_SQLITE_ROLLUP_RETENTION_DAYS",
                self.sqlite_rollup_retention_days,
            ),
            (
                "IOT_SQLITE_MAINTENANCE_BATCH_SIZE",
                self.sqlite_maintenance_batch_size,
            ),
        ] {
            if value == 0 || i64::try_from(value).is_err() {
                return Err(format!("{name} must be a positive signed 64-bit integer"));
            }
        }
        Ok(())
    }
}

fn storage_configuration(
    arguments: &Arguments,
    mut values: BTreeMap<String, String>,
) -> Result<StorageConfiguration, String> {
    if let Some(database_url) = &arguments.database_url {
        values.insert("DATABASE_URL".to_owned(), database_url.clone());
    }
    StorageConfiguration::from_values(&values).map_err(|error| error.to_string())
}

fn sqlite_retention_cutoffs(
    raw_retention_days: u64,
    rollup_retention_days: u64,
    now: DateTime<Utc>,
) -> Result<(String, String), String> {
    let raw_days = i64::try_from(raw_retention_days)
        .map_err(|_| "IOT_SQLITE_RAW_RETENTION_DAYS does not fit i64".to_owned())?;
    let rollup_days = i64::try_from(rollup_retention_days)
        .map_err(|_| "IOT_SQLITE_ROLLUP_RETENTION_DAYS does not fit i64".to_owned())?;
    let raw_before = now
        .checked_sub_signed(ChronoDuration::days(raw_days))
        .ok_or_else(|| "IOT_SQLITE_RAW_RETENTION_DAYS is out of range".to_owned())?;
    let rollup_before = now
        .checked_sub_signed(ChronoDuration::days(rollup_days))
        .ok_or_else(|| "IOT_SQLITE_ROLLUP_RETENTION_DAYS is out of range".to_owned())?;
    Ok((raw_before.to_rfc3339(), rollup_before.to_rfc3339()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = Arguments::parse();
    arguments.validate().map_err(std::io::Error::other)?;
    let storage = storage_configuration(&arguments, BTreeMap::from_iter(env::vars()))
        .map_err(std::io::Error::other)?;
    let tuning = arguments.tuning();
    let metrics = Arc::new(IngestMetrics::default());

    let stream = LocalStream::open(
        arguments.stream_dir,
        StreamConfig {
            partition_count: arguments.stream_partitions,
            segment_max_bytes: arguments.stream_segment_bytes,
            retention_max_bytes: arguments.stream_retention_bytes,
            retention_max_age: Duration::from_secs(arguments.stream_retention_seconds),
            max_record_bytes: arguments.stream_max_record_bytes,
            index_stride: 128,
        },
    )?;
    let writer_member_id = arguments.writer_member_id.unwrap_or_else(default_member_id);
    let writer_consumer = stream.join_group(
        &arguments.writer_group,
        &writer_member_id,
        GroupStart::Earliest,
        Utc::now(),
    )?;
    let alert_consumer = stream.join_group(
        &arguments.alert_group,
        &format!("{writer_member_id}-alerts"),
        GroupStart::Earliest,
        Utc::now(),
    )?;
    let runtime = arguments.legacy_mqtt_ingress.then(|| {
        MqttRuntime::new(
            MqttRuntimeConfig {
                client_id: "iot-ingest-legacy".to_owned(),
                broker_host: arguments.broker_host,
                broker_port: arguments.broker_port,
            },
            stream.clone(),
        )
    });
    if let Some(runtime) = &runtime {
        runtime.subscribe().await?;
    }
    let smtp_fallback = SmtpConfig::from_env()?;
    let command_transport = HttpTransportRpcClient::new(
        &arguments.mqtt_transport_url,
        &arguments.mqtt_transport_secret,
    )
    .map_err(std::io::Error::other)?;

    let mut tasks = JoinSet::new();
    if let Some(runtime) = runtime {
        tasks.spawn(run_mqtt_ingress(runtime, Arc::clone(&metrics)));
    }
    match storage.storage {
        DatabaseStorage::Timescale => {
            let pool = PgPool::connect(
                storage
                    .database_url
                    .as_deref()
                    .ok_or("DATABASE_URL is required for Timescale storage")?,
            )
            .await?;
            migrate(&pool).await?;
            let webhook_ingress = TokenWebhookIngress::new(
                pool.clone(),
                stream.clone(),
                &arguments.nanomq_webhook_secret,
                &arguments.nanomq_webhook_inbox_dir,
                Arc::clone(&metrics),
            )
            .map_err(std::io::Error::other)?;
            start_http_server(
                arguments.health_address,
                Arc::clone(&metrics),
                webhook_router_with_transport_secret(
                    webhook_ingress,
                    &arguments.mqtt_transport_ingest_webhook_secret,
                ),
            )
            .await?;

            let writer = TelemetryWriter::new(
                pool.clone(),
                usize::try_from(tuning.writer_batch_size).expect("validated writer batch size"),
            );
            let evaluator = AlertEvaluator::new(
                pool.clone(),
                usize::try_from(tuning.alert_batch_size).expect("validated alert batch size"),
            );
            tasks.spawn(run_telemetry_writer(
                writer,
                writer_consumer,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.writer_flush_seconds),
            ));
            tasks.spawn(run_alert_evaluator(
                evaluator.clone(),
                alert_consumer,
                Arc::clone(&metrics),
                Duration::from_millis(tuning.alert_event_interval_milliseconds),
            ));
            tasks.spawn(run_window_evaluator(
                evaluator,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.alert_window_interval_seconds),
            ));
            tasks.spawn(run_retention(
                stream.clone(),
                Arc::clone(&metrics),
                Duration::from_secs(tuning.retention_interval_seconds),
            ));
            tasks.spawn(run_metrics_refresh(
                stream.clone(),
                pool.clone(),
                Arc::clone(&metrics),
            ));

            let dispatcher = NotificationDispatcher::new(
                pool.clone(),
                ReloadingSmtpEmailSender::new(arguments.smtp_config_path, smtp_fallback),
                usize::try_from(tuning.notification_batch_size)
                    .expect("validated notification batch size"),
            )
            .with_timeout(Duration::from_secs(300))
            .with_delivery_policy(
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_lease_seconds)
                        .expect("validated notification lease duration"),
                ),
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_retry_base_seconds)
                        .expect("validated notification retry base"),
                ),
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_retry_max_seconds)
                        .expect("validated notification retry maximum"),
                ),
            );
            tasks.spawn(run_notification_dispatcher(
                dispatcher,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.notification_interval_seconds),
            ));
            tasks.spawn(run_command_dispatcher(
                CommandDispatcher::new(
                    pool,
                    command_transport.clone(),
                    arguments.command_batch_size,
                ),
                Duration::from_secs(arguments.command_interval_seconds),
            ));
        }
        DatabaseStorage::Sqlite => {
            let store = SqliteStore::open(&storage).await?;
            let webhook_ingress = SqliteTokenWebhookIngress::new(
                store.clone(),
                stream.clone(),
                &arguments.nanomq_webhook_secret,
                &arguments.nanomq_webhook_inbox_dir,
                Arc::clone(&metrics),
            )
            .map_err(std::io::Error::other)?;
            start_http_server(
                arguments.health_address,
                Arc::clone(&metrics),
                sqlite_webhook_router_with_transport_secret(
                    webhook_ingress,
                    &arguments.mqtt_transport_ingest_webhook_secret,
                ),
            )
            .await?;

            let writer = SqliteTelemetryWriter::new(
                store.clone(),
                usize::try_from(tuning.writer_batch_size).expect("validated writer batch size"),
            );
            let evaluator = SqliteAlertEvaluator::new(
                store.clone(),
                usize::try_from(tuning.alert_batch_size).expect("validated alert batch size"),
            );
            tasks.spawn(run_sqlite_telemetry_writer(
                writer,
                writer_consumer,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.writer_flush_seconds),
            ));
            tasks.spawn(run_sqlite_alert_evaluator(
                evaluator.clone(),
                alert_consumer,
                Arc::clone(&metrics),
                Duration::from_millis(tuning.alert_event_interval_milliseconds),
            ));
            tasks.spawn(run_sqlite_window_evaluator(
                evaluator,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.alert_window_interval_seconds),
            ));
            tasks.spawn(run_retention(
                stream.clone(),
                Arc::clone(&metrics),
                Duration::from_secs(tuning.retention_interval_seconds),
            ));
            tasks.spawn(run_sqlite_metrics_refresh(
                stream.clone(),
                store.clone(),
                Arc::clone(&metrics),
            ));
            tasks.spawn(run_sqlite_data_retention(
                store.clone(),
                Arc::clone(&metrics),
                arguments.sqlite_raw_retention_days,
                arguments.sqlite_rollup_retention_days,
                arguments.sqlite_maintenance_batch_size,
                Duration::from_secs(tuning.retention_interval_seconds),
            ));

            let dispatcher = SqliteNotificationDispatcher::new(
                store.pool().clone(),
                ReloadingSmtpEmailSender::new(arguments.smtp_config_path, smtp_fallback),
                usize::try_from(tuning.notification_batch_size)
                    .expect("validated notification batch size"),
            )
            .with_delivery_policy(
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_lease_seconds)
                        .expect("validated notification lease duration"),
                ),
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_retry_base_seconds)
                        .expect("validated notification retry base"),
                ),
                ChronoDuration::seconds(
                    i64::try_from(tuning.notification_retry_max_seconds)
                        .expect("validated notification retry maximum"),
                ),
            );
            tasks.spawn(run_sqlite_notification_dispatcher(
                dispatcher,
                Arc::clone(&metrics),
                Duration::from_secs(tuning.notification_interval_seconds),
            ));
            tasks.spawn(run_sqlite_command_dispatcher(
                SqliteCommandDispatcher::new(
                    store,
                    command_transport.clone(),
                    arguments.command_batch_size,
                ),
                Duration::from_secs(arguments.command_interval_seconds),
            ));
        }
    }

    while let Some(task) = tasks.join_next().await {
        task??;
    }

    Ok(())
}

fn default_member_id() -> String {
    format!(
        "{}-{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "iot-ingest".to_owned()),
        std::process::id()
    )
}

async fn run_mqtt_ingress(mut runtime: MqttRuntime, metrics: Arc<IngestMetrics>) -> TaskResult {
    loop {
        match runtime.poll_once(Utc::now()).await {
            Ok(Some(outcome)) => metrics.record_outcome(outcome),
            Ok(None) => {}
            Err(error) => {
                metrics.record_stream_failure();
                eprintln!("legacy MQTT ingestion error: {error}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_telemetry_writer(
    writer: TelemetryWriter,
    mut consumer: StreamConsumer,
    metrics: Arc<IngestMetrics>,
    flush_interval: Duration,
) -> TaskResult {
    let mut flush_tick = interval(flush_interval);
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat_tick = interval(Duration::from_secs(10));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = flush_tick.tick() => {
                match writer.flush_once(&mut consumer, Utc::now()).await {
                    Ok(_) => {}
                    Err(WriterError::Database(error)) => {
                        metrics.record_database_failure();
                        eprintln!("TimescaleDB flush error: {error}");
                    }
                    Err(WriterError::Sqlite(error)) => {
                        metrics.record_database_failure();
                        eprintln!("SQLite flush error: {error}");
                    }
                    Err(WriterError::Stream(error)) => {
                        metrics.record_stream_failure();
                        eprintln!("stream writer error: {error}");
                    }
                }
            }
            _ = heartbeat_tick.tick() => {
                if let Err(error) = consumer.heartbeat(Utc::now()) {
                    metrics.record_stream_failure();
                    eprintln!("telemetry writer heartbeat error: {error}");
                }
            }
        }
        update_group_metrics(&metrics, &consumer);
    }
}

async fn run_sqlite_telemetry_writer(
    writer: SqliteTelemetryWriter,
    mut consumer: StreamConsumer,
    metrics: Arc<IngestMetrics>,
    flush_interval: Duration,
) -> TaskResult {
    let mut flush_tick = interval(flush_interval);
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat_tick = interval(Duration::from_secs(10));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = flush_tick.tick() => {
                match writer.flush_once(&mut consumer, Utc::now()).await {
                    Ok(_) => {}
                    Err(WriterError::Database(error) | WriterError::Sqlite(iot_storage::SqliteStoreError::Database(error))) => {
                        metrics.record_database_failure();
                        eprintln!("SQLite flush error: {error}");
                    }
                    Err(WriterError::Sqlite(error)) => {
                        metrics.record_database_failure();
                        eprintln!("SQLite flush error: {error}");
                    }
                    Err(WriterError::Stream(error)) => {
                        metrics.record_stream_failure();
                        eprintln!("stream writer error: {error}");
                    }
                }
            }
            _ = heartbeat_tick.tick() => {
                if let Err(error) = consumer.heartbeat(Utc::now()) {
                    metrics.record_stream_failure();
                    eprintln!("telemetry writer heartbeat error: {error}");
                }
            }
        }
        update_group_metrics(&metrics, &consumer);
    }
}

async fn run_alert_evaluator(
    evaluator: AlertEvaluator,
    mut consumer: StreamConsumer,
    metrics: Arc<IngestMetrics>,
    evaluation_interval: Duration,
) -> TaskResult {
    let mut evaluation_tick = interval(evaluation_interval);
    evaluation_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat_tick = interval(Duration::from_secs(10));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = evaluation_tick.tick() => {
                if let Err(error) = evaluator.flush_event_rules(&mut consumer, Utc::now()).await {
                    metrics.record_alert_failure();
                    eprintln!("alert event evaluation error: {error}");
                }
            }
            _ = heartbeat_tick.tick() => {
                if let Err(error) = consumer.heartbeat(Utc::now()) {
                    metrics.record_alert_failure();
                    eprintln!("alert evaluator heartbeat error: {error}");
                }
            }
        }
        update_group_metrics(&metrics, &consumer);
    }
}

async fn run_sqlite_alert_evaluator(
    evaluator: SqliteAlertEvaluator,
    mut consumer: StreamConsumer,
    metrics: Arc<IngestMetrics>,
    evaluation_interval: Duration,
) -> TaskResult {
    let mut evaluation_tick = interval(evaluation_interval);
    evaluation_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut heartbeat_tick = interval(Duration::from_secs(10));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = evaluation_tick.tick() => {
                if let Err(error) = evaluator.flush_event_rules(&mut consumer, Utc::now()).await {
                    metrics.record_alert_failure();
                    eprintln!("SQLite alert event evaluation error: {error}");
                }
            }
            _ = heartbeat_tick.tick() => {
                if let Err(error) = consumer.heartbeat(Utc::now()) {
                    metrics.record_alert_failure();
                    eprintln!("alert evaluator heartbeat error: {error}");
                }
            }
        }
        update_group_metrics(&metrics, &consumer);
    }
}

async fn run_window_evaluator(
    evaluator: AlertEvaluator,
    metrics: Arc<IngestMetrics>,
    evaluation_interval: Duration,
) -> TaskResult {
    let mut tick = interval(evaluation_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Err(error) = evaluator.flush_window_rules(Utc::now()).await {
            metrics.record_alert_failure();
            eprintln!("alert window evaluation error: {error}");
        }
    }
}

async fn run_sqlite_window_evaluator(
    evaluator: SqliteAlertEvaluator,
    metrics: Arc<IngestMetrics>,
    evaluation_interval: Duration,
) -> TaskResult {
    let mut tick = interval(evaluation_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Err(error) = evaluator.flush_window_rules(Utc::now()).await {
            metrics.record_alert_failure();
            eprintln!("SQLite alert window evaluation error: {error}");
        }
    }
}

async fn run_notification_dispatcher<S>(
    dispatcher: NotificationDispatcher<S>,
    metrics: Arc<IngestMetrics>,
    dispatch_interval: Duration,
) -> TaskResult
where
    S: EmailSender + 'static,
{
    let mut tick = interval(dispatch_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        match dispatcher.dispatch_once(Utc::now()).await {
            Ok(result) => {
                metrics.record_notification_failures(result.retried);
            }
            Err(error) => {
                metrics.record_notification_failure();
                eprintln!("notification dispatch error: {error}");
            }
        }
    }
}

async fn run_sqlite_notification_dispatcher<S>(
    dispatcher: SqliteNotificationDispatcher<S>,
    metrics: Arc<IngestMetrics>,
    dispatch_interval: Duration,
) -> TaskResult
where
    S: EmailSender + 'static,
{
    let mut tick = interval(dispatch_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        match dispatcher.dispatch_once(Utc::now()).await {
            Ok(result) => {
                metrics.record_notification_failures(result.retried);
            }
            Err(error) => {
                metrics.record_notification_failure();
                eprintln!("SQLite notification dispatch error: {error}");
            }
        }
    }
}

async fn run_command_dispatcher<C>(
    dispatcher: CommandDispatcher<C>,
    dispatch_interval: Duration,
) -> TaskResult
where
    C: TransportRpcClient + 'static,
{
    let mut tick = interval(dispatch_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Err(error) = dispatcher.dispatch_once(Utc::now()).await {
            eprintln!("command dispatch error: {error}");
        }
    }
}

async fn run_sqlite_command_dispatcher<C>(
    dispatcher: SqliteCommandDispatcher<C>,
    dispatch_interval: Duration,
) -> TaskResult
where
    C: TransportRpcClient + 'static,
{
    let mut tick = interval(dispatch_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Err(error) = dispatcher.dispatch_once(Utc::now()).await {
            eprintln!("SQLite command dispatch error: {error}");
        }
    }
}

async fn run_retention(
    stream: LocalStream,
    metrics: Arc<IngestMetrics>,
    retention_interval: Duration,
) -> TaskResult {
    let mut tick = interval(retention_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let stream_for_retention = stream.clone();
        match tokio::task::spawn_blocking(move || {
            stream_for_retention.enforce_retention(Utc::now())
        })
        .await
        {
            Ok(Ok(result)) => {
                if result.deleted_segments > 0 {
                    eprintln!(
                        "stream retention deleted {} segments and {} bytes",
                        result.deleted_segments, result.deleted_bytes
                    );
                }
            }
            Ok(Err(error)) => {
                metrics.record_stream_failure();
                eprintln!("stream retention error: {error}");
            }
            Err(error) => {
                metrics.record_stream_failure();
                eprintln!("stream retention task error: {error}");
            }
        }
    }
}

async fn run_sqlite_data_retention(
    store: SqliteStore,
    metrics: Arc<IngestMetrics>,
    raw_retention_days: u64,
    rollup_retention_days: u64,
    batch_size: u64,
    retention_interval: Duration,
) -> TaskResult {
    let mut tick = interval(retention_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let (raw_before, rollup_before) =
            match sqlite_retention_cutoffs(raw_retention_days, rollup_retention_days, Utc::now()) {
                Ok(cutoffs) => cutoffs,
                Err(error) => {
                    metrics.record_database_failure();
                    eprintln!("SQLite retention cutoff error: {error}");
                    continue;
                }
            };
        match store
            .enforce_retention(&raw_before, &rollup_before, batch_size)
            .await
        {
            Ok(result)
                if result.raw_rows > 0
                    || result.rollup_rows > 0
                    || result.event_evaluation_rows > 0
                    || result.notification_rows > 0
                    || result.resolved_incident_rows > 0 =>
            {
                eprintln!(
                    "SQLite retention deleted {} raw, {} rollup, {} alert dedup, {} sent notification, and {} resolved incident rows",
                    result.raw_rows,
                    result.rollup_rows,
                    result.event_evaluation_rows,
                    result.notification_rows,
                    result.resolved_incident_rows,
                );
            }
            Ok(_) => {}
            Err(error) => {
                metrics.record_database_failure();
                eprintln!("SQLite retention error: {error}");
            }
        }
    }
}

async fn run_metrics_refresh(
    stream: LocalStream,
    pool: PgPool,
    metrics: Arc<IngestMetrics>,
) -> TaskResult {
    let mut tick = interval(Duration::from_secs(10));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let stream_for_stats = stream.clone();
        match tokio::task::spawn_blocking(move || stream_for_stats.stats()).await {
            Ok(Ok(stats)) => metrics.update_stream(stats),
            Ok(Err(error)) => {
                metrics.record_stream_failure();
                eprintln!("stream metrics error: {error}");
            }
            Err(error) => {
                metrics.record_stream_failure();
                eprintln!("stream metrics task error: {error}");
            }
        }

        match sqlx::query(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'open') AS open_incidents,
                (SELECT COUNT(*) FROM notification_outbox WHERE state = 'pending') AS pending_outbox
             FROM alert_incidents",
        )
        .fetch_one(&pool)
        .await
        {
            Ok(row) => metrics.update_alert_state(
                row.get::<i64, _>("open_incidents").max(0) as u64,
                row.get::<i64, _>("pending_outbox").max(0) as u64,
            ),
            Err(error) => {
                metrics.record_alert_failure();
                eprintln!("alert metrics error: {error}");
            }
        }
    }
}

async fn run_sqlite_metrics_refresh(
    stream: LocalStream,
    store: SqliteStore,
    metrics: Arc<IngestMetrics>,
) -> TaskResult {
    let mut tick = interval(Duration::from_secs(10));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let stream_for_stats = stream.clone();
        match tokio::task::spawn_blocking(move || stream_for_stats.stats()).await {
            Ok(Ok(stats)) => metrics.update_stream(stats),
            Ok(Err(error)) => {
                metrics.record_stream_failure();
                eprintln!("stream metrics error: {error}");
            }
            Err(error) => {
                metrics.record_stream_failure();
                eprintln!("stream metrics task error: {error}");
            }
        }

        match sqlx::query(
            "SELECT
                (SELECT COUNT(*) FROM alert_incidents WHERE status = 'open') AS open_incidents,
                (SELECT COUNT(*) FROM notification_outbox WHERE state = 'pending') AS pending_outbox",
        )
        .fetch_one(store.pool())
        .await
        {
            Ok(row) => metrics.update_alert_state(
                row.get::<i64, _>("open_incidents").max(0) as u64,
                row.get::<i64, _>("pending_outbox").max(0) as u64,
            ),
            Err(error) => {
                metrics.record_alert_failure();
                eprintln!("SQLite alert metrics error: {error}");
            }
        }
    }
}

fn update_group_metrics(metrics: &IngestMetrics, consumer: &StreamConsumer) {
    match consumer.group_stats() {
        Ok(stats) => metrics.update_group(stats),
        Err(error) => {
            metrics.record_stream_failure();
            eprintln!("stream group metrics error: {error}");
        }
    }
}

async fn start_http_server(
    address: SocketAddr,
    metrics: Arc<IngestMetrics>,
    app: Router,
) -> Result<(), std::io::Error> {
    let metrics_for_handler = Arc::clone(&metrics);
    let app = app.route("/healthz", get(healthz)).route(
        "/metrics",
        get(move || {
            let metrics = Arc::clone(&metrics_for_handler);
            async move { metrics.render_prometheus() }
        }),
    );
    let listener = tokio::net::TcpListener::bind(address).await?;

    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("health server error: {error}");
        }
    });

    Ok(())
}

async fn healthz() -> impl IntoResponse {
    "ok\n"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use clap::Parser;
    use iot_core::DatabaseStorage;

    use super::{Arguments, sqlite_retention_cutoffs, storage_configuration};

    #[test]
    fn stream_retention_defaults_to_one_day_and_two_gibibytes() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--database-url",
            "postgres://iot:iot@127.0.0.1:54329/iot",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();

        assert_eq!(arguments.stream_retention_seconds, 24 * 60 * 60);
        assert_eq!(arguments.stream_retention_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(arguments.alert_group, "alert-evaluator");
    }

    #[test]
    fn sqlite_startup_does_not_require_a_database_url_argument() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();

        assert!(arguments.database_url.is_none());
    }

    #[test]
    fn sqlite_startup_resolves_the_sqlite_storage_configuration() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();
        let storage = storage_configuration(
            &arguments,
            BTreeMap::from([
                ("IOT_DATABASE_STORAGE".to_owned(), "sqlite".to_owned()),
                (
                    "IOT_SQLITE_PATH".to_owned(),
                    "/var/lib/rush-iot-nano/rush.db".to_owned(),
                ),
            ]),
        )
        .unwrap();

        assert_eq!(storage.storage, DatabaseStorage::Sqlite);
        assert!(storage.database_url.is_none());
    }

    #[test]
    fn sqlite_maintenance_defaults_keep_raw_and_rollups_for_expected_periods() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();

        assert_eq!(arguments.sqlite_raw_retention_days, 30);
        assert_eq!(arguments.sqlite_rollup_retention_days, 365);
        assert_eq!(arguments.sqlite_maintenance_batch_size, 1_000);
    }

    #[test]
    fn sqlite_retention_cutoffs_follow_the_configured_day_windows() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
            "--sqlite-raw-retention-days",
            "7",
            "--sqlite-rollup-retention-days",
            "90",
        ])
        .unwrap();
        let now = Utc.with_ymd_and_hms(2026, 9, 7, 12, 0, 0).unwrap();
        let (raw_before, rollup_before) = sqlite_retention_cutoffs(
            arguments.sqlite_raw_retention_days,
            arguments.sqlite_rollup_retention_days,
            now,
        )
        .unwrap();

        assert_eq!(raw_before, "2026-08-31T12:00:00+00:00");
        assert_eq!(rollup_before, "2026-06-09T12:00:00+00:00");
    }

    #[test]
    fn operational_tuning_has_safe_defaults_and_rejects_invalid_retry_ranges() {
        let defaults = Arguments::try_parse_from([
            "iot-ingest",
            "--database-url",
            "postgres://iot:iot@127.0.0.1:54329/iot",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();

        assert_eq!(defaults.writer_batch_size, 1_000);
        assert_eq!(defaults.alert_batch_size, 250);
        assert_eq!(defaults.notification_batch_size, 10);
        assert_eq!(defaults.notification_lease_seconds, 30);
        assert_eq!(defaults.notification_retry_base_seconds, 1);
        assert_eq!(defaults.notification_retry_max_seconds, 3_600);
        assert!(defaults.validate().is_ok());

        let invalid = Arguments::try_parse_from([
            "iot-ingest",
            "--database-url",
            "postgres://iot:iot@127.0.0.1:54329/iot",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "test-mqtt-transport-secret-must-have-32-bytes",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
            "--notification-retry-base-seconds",
            "60",
            "--notification-retry-max-seconds",
            "30",
        ])
        .unwrap();

        assert!(invalid.validate().is_err());
    }

    #[test]
    fn transport_secret_matches_the_transport_service_policy() {
        let arguments = Arguments::try_parse_from([
            "iot-ingest",
            "--database-url",
            "postgres://iot:iot@127.0.0.1:54329/iot",
            "--nanomq-webhook-secret",
            "test-nanomq-webhook-secret-must-have-32-bytes",
            "--mqtt-transport-url",
            "http://127.0.0.1:8082",
            "--mqtt-transport-secret",
            "short-secret",
            "--mqtt-transport-ingest-webhook-secret",
            "test-transport-ingest-webhook-secret-must-have-32-bytes",
        ])
        .unwrap();

        assert!(arguments.validate().is_err());
    }
}
