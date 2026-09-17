#![forbid(unsafe_code)]

mod alert;
mod command;
mod metrics;
mod mqtt;
mod notification;
mod runtime;
mod stream_port;
mod writer;

pub use alert::{AlertError, AlertFlushResult, PlatformAlertEvaluator};
pub use command::{
    CommandDispatchResult, CommandError, CommandTransport, CommandTransport as TransportRpcClient,
    CommandTransportError, PlatformCommandDispatcher, TransportRpcPublishRequest,
};
pub use metrics::IngestMetrics;
pub use mqtt::{
    IngestOutcome, MqttConsumerError, MqttRuntime, MqttRuntimeConfig, MqttRuntimeError,
    MqttStreamProducer,
};
pub use notification::{
    EmailSender, NotificationDispatchResult, NotificationError, PlatformNotificationDispatcher,
    ReloadingSmtpEmailSender, SmtpConfig, SmtpConfigInput, SmtpEmailSender, load_live_smtp_config,
};
pub use runtime::{CoreRuntime, CoreRuntimeConfig, CoreRuntimeError, CoreRuntimeWorkerError};
pub use stream_port::{ClaimedBatch, CoreStreamConsumer};
pub use writer::{FlushResult, PlatformTelemetryWriter, WriterError};
