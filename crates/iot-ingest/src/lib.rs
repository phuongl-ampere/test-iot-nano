#![forbid(unsafe_code)]

mod alert;
mod metrics;
mod mqtt;
mod notification;
mod webhook;
mod writer;

pub use alert::{AlertError, AlertEvaluator, AlertFlushResult, SqliteAlertEvaluator};
pub use metrics::IngestMetrics;
pub use mqtt::{
    IngestOutcome, MqttConsumerError, MqttRuntime, MqttRuntimeConfig, MqttRuntimeError,
    MqttStreamProducer,
};
pub use notification::{
    EmailSender, NotificationDispatchResult, NotificationDispatcher, NotificationError,
    ReloadingSmtpEmailSender, SmtpConfig, SmtpConfigInput, SmtpEmailSender,
    SqliteNotificationDispatcher, load_live_smtp_config,
};
pub use webhook::{
    SqliteTokenWebhookIngress, TokenWebhookIngress, WebhookError, sqlite_webhook_router,
    webhook_router,
};
pub use writer::{FlushResult, SqliteTelemetryWriter, TelemetryWriter, WriterError, migrate};
