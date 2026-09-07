use std::collections::BTreeMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use iot_stream::{GroupStats, StreamStats};

use crate::IngestOutcome;

#[derive(Default)]
pub struct IngestMetrics {
    accepted: AtomicU64,
    rejected: AtomicU64,
    database_failures: AtomicU64,
    stream_failures: AtomicU64,
    alert_failures: AtomicU64,
    notification_failures: AtomicU64,
    alert_open_incidents: AtomicU64,
    notification_outbox_pending: AtomicU64,
    stream: Mutex<Option<StreamStats>>,
    groups: Mutex<BTreeMap<String, GroupStats>>,
}

impl IngestMetrics {
    pub fn record_outcome(&self, outcome: IngestOutcome) {
        match outcome {
            IngestOutcome::Accepted => {
                self.accepted.fetch_add(1, Ordering::Relaxed);
            }
            IngestOutcome::Rejected => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_database_failure(&self) {
        self.database_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stream_failure(&self) {
        self.stream_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_alert_failure(&self) {
        self.alert_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_notification_failure(&self) {
        self.notification_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_notification_failures(&self, count: usize) {
        self.notification_failures
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub fn update_stream(&self, stats: StreamStats) {
        *self.stream.lock().expect("stream metrics mutex poisoned") = Some(stats);
    }

    pub fn update_group(&self, stats: GroupStats) {
        self.groups
            .lock()
            .expect("group metrics mutex poisoned")
            .insert(stats.group.clone(), stats);
    }

    pub fn update_alert_state(&self, open_incidents: u64, pending_outbox: u64) {
        self.alert_open_incidents
            .store(open_incidents, Ordering::Relaxed);
        self.notification_outbox_pending
            .store(pending_outbox, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut rendered = format!(
            concat!(
                "iot_ingest_accepted_total {}\n",
                "iot_ingest_rejected_total {}\n",
                "iot_ingest_database_failures_total {}\n",
                "iot_ingest_stream_failures_total {}\n",
                "iot_ingest_alert_failures_total {}\n",
                "iot_ingest_notification_failures_total {}\n",
                "iot_ingest_alert_open_incidents {}\n",
                "iot_ingest_notification_outbox_pending {}\n"
            ),
            self.accepted.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
            self.database_failures.load(Ordering::Relaxed),
            self.stream_failures.load(Ordering::Relaxed),
            self.alert_failures.load(Ordering::Relaxed),
            self.notification_failures.load(Ordering::Relaxed),
            self.alert_open_incidents.load(Ordering::Relaxed),
            self.notification_outbox_pending.load(Ordering::Relaxed),
        );

        if let Some(stream) = self
            .stream
            .lock()
            .expect("stream metrics mutex poisoned")
            .as_ref()
        {
            rendered.push_str(&format!(
                "iot_ingest_stream_log_bytes {}\n",
                stream.total_bytes
            ));
            for partition in &stream.partitions {
                rendered.push_str(&format!(
                    "iot_ingest_stream_earliest_offset{{partition=\"{}\"}} {}\n",
                    partition.partition.get(),
                    partition.earliest_offset
                ));
                rendered.push_str(&format!(
                    "iot_ingest_stream_high_watermark{{partition=\"{}\"}} {}\n",
                    partition.partition.get(),
                    partition.next_offset
                ));
            }
        }

        for group in self
            .groups
            .lock()
            .expect("group metrics mutex poisoned")
            .values()
        {
            for partition in &group.partitions {
                rendered.push_str(&format!(
                    "iot_ingest_stream_group_committed_offset{{group=\"{}\",partition=\"{}\"}} {}\n",
                    group.group,
                    partition.partition.get(),
                    partition.committed_next_offset
                ));
                rendered.push_str(&format!(
                    "iot_ingest_stream_group_lag{{group=\"{}\",partition=\"{}\"}} {}\n",
                    group.group,
                    partition.partition.get(),
                    partition.lag
                ));
            }
            rendered.push_str(&format!(
                "iot_ingest_stream_group_generation{{group=\"{}\"}} {}\n",
                group.group, group.generation
            ));
        }

        rendered
    }
}
