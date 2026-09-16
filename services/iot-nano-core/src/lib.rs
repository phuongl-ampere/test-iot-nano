#![forbid(unsafe_code)]

mod alert;
mod command;
mod control;
mod metrics;
mod mqtt;
mod notification;
mod runtime;
mod storage;
mod stream_port;
mod writer;

pub use alert::{
    AlertError, AlertEvaluator, AlertFlushResult, PlatformAlertEvaluator, SqliteAlertEvaluator,
};
pub use command::{
    CommandDispatchResult, CommandDispatcher, CommandError, CommandTransport,
    CommandTransport as TransportRpcClient, CommandTransportError, PlatformCommandDispatcher,
    SqliteCommandDispatcher, TransportRpcPublishRequest,
};
pub use control::{CoreControlState, core_control_router};
pub use metrics::IngestMetrics;
pub use mqtt::{
    IngestOutcome, MqttConsumerError, MqttRuntime, MqttRuntimeConfig, MqttRuntimeError,
    MqttStreamProducer,
};
pub use notification::{
    EmailSender, NotificationDispatchResult, NotificationDispatcher, NotificationError,
    PlatformNotificationDispatcher, ReloadingSmtpEmailSender, SmtpConfig, SmtpConfigInput,
    SmtpEmailSender, SqliteNotificationDispatcher, load_live_smtp_config,
};
pub use runtime::{CoreRuntime, CoreRuntimeConfig, CoreRuntimeError, CoreRuntimeWorkerError};
pub use storage::{
    CommandOutboxRecord, CommandOutboxState, CoreSqliteStore, CoreSqliteStoreError,
    NewCommandOutboxEntry, RetentionResult,
};
pub use stream_port::{ClaimedBatch, CoreStreamConsumer};
pub use writer::{
    FlushResult, PlatformTelemetryWriter, SqliteTelemetryWriter, TelemetryWriter, WriterError,
    connect_core_database, migrate,
};
