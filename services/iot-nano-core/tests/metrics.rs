use iot_nano_core::{IngestMetrics, IngestOutcome};
use iot_stream::{GroupPartitionStats, GroupStats, PartitionId, PartitionStats, StreamStats};

#[test]
fn metrics_render_partition_watermarks_and_writer_group_lag() {
    let metrics = IngestMetrics::default();
    metrics.record_outcome(IngestOutcome::Accepted);
    metrics.record_outcome(IngestOutcome::Rejected);
    metrics.record_database_failure();
    metrics.record_stream_failure();
    metrics.record_alert_failure();
    metrics.record_notification_failure();
    metrics.update_alert_state(3, 7);
    metrics.update_stream(StreamStats {
        total_bytes: 4_096,
        partitions: vec![PartitionStats {
            partition: PartitionId::new(0),
            earliest_offset: 4,
            next_offset: 12,
            bytes: 4_096,
        }],
    });
    metrics.update_group(GroupStats {
        group: "timescaledb-writer".to_owned(),
        generation: 3,
        partitions: vec![GroupPartitionStats {
            partition: PartitionId::new(0),
            committed_next_offset: 8,
            high_watermark: 12,
            lag: 4,
        }],
    });

    let rendered = metrics.render_prometheus();

    assert!(rendered.contains("iot_ingest_accepted_total 1"));
    assert!(rendered.contains("iot_ingest_rejected_total 1"));
    assert!(rendered.contains("iot_ingest_database_failures_total 1"));
    assert!(rendered.contains("iot_ingest_stream_failures_total 1"));
    assert!(rendered.contains("iot_ingest_alert_failures_total 1"));
    assert!(rendered.contains("iot_ingest_notification_failures_total 1"));
    assert!(rendered.contains("iot_ingest_alert_open_incidents 3"));
    assert!(rendered.contains("iot_ingest_notification_outbox_pending 7"));
    assert!(rendered.contains("iot_ingest_stream_log_bytes 4096"));
    assert!(rendered.contains("iot_ingest_stream_earliest_offset{partition=\"0\"} 4"));
    assert!(rendered.contains("iot_ingest_stream_high_watermark{partition=\"0\"} 12"));
    assert!(rendered.contains(
        "iot_ingest_stream_group_committed_offset{group=\"timescaledb-writer\",partition=\"0\"} 8"
    ));
    assert!(
        rendered.contains(
            "iot_ingest_stream_group_lag{group=\"timescaledb-writer\",partition=\"0\"} 4"
        )
    );
    assert!(
        rendered.contains("iot_ingest_stream_group_generation{group=\"timescaledb-writer\"} 3")
    );
}

#[test]
fn metrics_render_writer_and_alert_group_lag() {
    let metrics = IngestMetrics::default();
    metrics.update_group(GroupStats {
        group: "timescaledb-writer".to_owned(),
        generation: 1,
        partitions: vec![GroupPartitionStats {
            partition: PartitionId::new(0),
            committed_next_offset: 10,
            high_watermark: 12,
            lag: 2,
        }],
    });
    metrics.update_group(GroupStats {
        group: "alert-evaluator".to_owned(),
        generation: 1,
        partitions: vec![GroupPartitionStats {
            partition: PartitionId::new(0),
            committed_next_offset: 8,
            high_watermark: 12,
            lag: 4,
        }],
    });

    let rendered = metrics.render_prometheus();

    assert!(
        rendered.contains(
            "iot_ingest_stream_group_lag{group=\"timescaledb-writer\",partition=\"0\"} 2"
        )
    );
    assert!(
        rendered
            .contains("iot_ingest_stream_group_lag{group=\"alert-evaluator\",partition=\"0\"} 4")
    );
}
