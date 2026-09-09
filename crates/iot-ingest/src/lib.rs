#![forbid(unsafe_code)]

mod alert;
mod command;
mod metrics;
mod mqtt;
mod notification;
mod webhook;
mod writer;

pub use alert::{AlertError, AlertEvaluator, AlertFlushResult, SqliteAlertEvaluator};
pub use command::{
    CommandDispatchResult, CommandDispatcher, CommandError, HttpTransportRpcClient,
    SqliteCommandDispatcher, TransportRpcClient, TransportRpcClientError,
    TransportRpcPublishRequest,
};
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
    sqlite_webhook_router_with_transport_secret, webhook_router,
    webhook_router_with_transport_secret,
};
pub use writer::{FlushResult, SqliteTelemetryWriter, TelemetryWriter, WriterError, migrate};
