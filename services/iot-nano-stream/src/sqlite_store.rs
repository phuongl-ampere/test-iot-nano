use std::{
    fs,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment, GroupStart,
    HeartbeatRequest, Offset, PartitionCommit, PartitionId, PartitionStats, RetentionResult,
    StreamConfig, StreamError, StreamMessage, StreamStats, group::validate_identifier,
    record::decode_message, segment,
};

const APPLICATION_ID: i32 = 0x4953_5453;

pub(crate) struct SqliteStore {
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub(crate) fn open(config: &StreamConfig) -> Result<Self, StreamError> {
        if let Some(parent) = config.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&config.path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(
            None,
            "busy_timeout",
            duration_millis(config.busy_timeout, "busy_timeout")?,
        )?;
        validate_database_owner(&connection)?;
        migrate(&connection, config)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn append(
        &self,
        config: &StreamConfig,
        message: StreamMessage,
    ) -> Result<AppendReceipt, StreamError> {
        message.validate()?;
        let payload_json = serde_json::to_string(&message)?;
        if payload_json.len() > config.max_record_bytes {
            return Err(StreamError::RecordTooLarge {
                encoded_bytes: payload_json.len(),
                max_bytes: config.max_record_bytes,
            });
        }
        let payload_bytes =
            u64::try_from(payload_json.len()).map_err(|_| StreamError::RecordTooLarge {
                encoded_bytes: payload_json.len(),
                max_bytes: config.max_record_bytes,
            })?;
        let idempotency_key = message.idempotency_key();
        let partition = segment::partition_for(message.partition_key(), config);
        let created_at = message.received_at();

        let mut connection = self.locked()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = idempotency_receipt(&transaction, &idempotency_key)? {
            transaction.commit()?;
            return Ok(receipt);
        }
        enforce_retention_locked(
            &transaction,
            config,
            chrono::Utc::now().timestamp_millis(),
            payload_bytes,
            true,
        )?;
        let offset = partition_next_offset(&transaction, partition)?;
        let next_offset = offset.checked_add(1).ok_or(StreamError::OffsetOverflow)?;
        transaction.execute(
            "INSERT INTO stream_records(
                partition, offset, payload_json, payload_bytes, created_at, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                i64::from(partition.get()),
                to_i64(offset)?,
                payload_json,
                to_i64(payload_bytes)?,
                created_at.to_rfc3339(),
                created_at.timestamp_millis(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO stream_idempotency(idempotency_key, partition, offset)
             VALUES (?1, ?2, ?3)",
            params![idempotency_key, i64::from(partition.get()), to_i64(offset)?],
        )?;
        transaction.execute(
            "UPDATE stream_partitions SET next_offset = ?1 WHERE partition = ?2",
            params![to_i64(next_offset)?, i64::from(partition.get())],
        )?;
        transaction.commit()?;
        Ok(AppendReceipt { partition, offset })
    }

    pub(crate) fn claim(
        &self,
        config: &StreamConfig,
        request: ClaimRequest,
    ) -> Result<Vec<ClaimedRecord>, StreamError> {
        validate_identifier("name", &request.group)?;
        validate_identifier("member ID", &request.member_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        let lease_until = expires_at(now, config.lease_duration, "lease_duration")?;
        let mut connection = self.locked()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let assignment = join_group(
            &transaction,
            config,
            &request.group,
            &request.member_id,
            request.start,
            now,
            lease_until,
        )?;
        let mut records = Vec::with_capacity(request.limit);
        for partition in assignment.partitions {
            if records.len() == request.limit {
                break;
            }
            let committed_offset = group_offset(&transaction, &request.group, partition)?;
            let (earliest, high_watermark) = partition_bounds(&transaction, partition)?;
            if committed_offset < earliest {
                return Err(StreamError::OffsetOutOfRange {
                    partition,
                    requested: committed_offset,
                    earliest,
                });
            }
            if committed_offset > high_watermark {
                return Err(StreamError::CorruptStore(format!(
                    "group {:?} is beyond partition {} high watermark",
                    request.group,
                    partition.get()
                )));
            }
            let mut statement = transaction.prepare(
                "SELECT offset, payload_json
                 FROM stream_records
                 WHERE partition = ?1 AND offset >= ?2
                 ORDER BY offset
                 LIMIT ?3",
            )?;
            let remaining = request.limit - records.len();
            let rows = statement.query_map(
                params![
                    i64::from(partition.get()),
                    to_i64(committed_offset)?,
                    usize_to_i64(remaining)?,
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )?;
            let mut partition_records = Vec::new();
            for row in rows {
                let (offset, payload_json) = row?;
                partition_records.push(ClaimedRecord {
                    partition,
                    offset: from_i64(offset)?,
                    message: decode_message(&payload_json)?,
                    generation: assignment.generation,
                });
            }
            drop(statement);
            if let Some(last) = partition_records.last() {
                let claimed_next = last
                    .offset
                    .checked_add(1)
                    .ok_or(StreamError::OffsetOverflow)?;
                transaction.execute(
                    "UPDATE stream_group_leases
                     SET inflight_until_ms = ?1, inflight_next_offset = ?2
                     WHERE group_name = ?3 AND partition = ?4
                       AND lease_owner = ?5 AND generation = ?6",
                    params![
                        lease_until,
                        to_i64(claimed_next)?,
                        &request.group,
                        i64::from(partition.get()),
                        &request.member_id,
                        to_i64(assignment.generation)?,
                    ],
                )?;
            }
            records.extend(partition_records);
        }
        transaction.commit()?;
        Ok(records)
    }

    pub(crate) fn acknowledge(
        &self,
        config: &StreamConfig,
        request: AcknowledgeRequest,
    ) -> Result<(), StreamError> {
        validate_identifier("name", &request.group)?;
        validate_identifier("member ID", &request.member_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        let mut connection = self.locked()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let generation = get_generation(&transaction, &request.group)?.ok_or_else(|| {
            StreamError::GroupMemberNotFound {
                group: request.group.clone(),
                member_id: request.member_id.clone(),
            }
        })?;
        if generation != request.generation {
            return Err(StreamError::StaleGeneration);
        }
        active_member(&transaction, &request.group, &request.member_id, now)?;
        for PartitionCommit {
            partition,
            next_offset,
        } in request.commits
        {
            if partition.get() >= config.partitions {
                return Err(StreamError::InvalidPartition {
                    partition: partition.get(),
                });
            }
            let lease = get_lease(&transaction, &request.group, partition)?.ok_or_else(|| {
                StreamError::GroupMemberNotFound {
                    group: request.group.clone(),
                    member_id: request.member_id.clone(),
                }
            })?;
            if lease.owner != request.member_id || lease.generation != generation {
                return Err(StreamError::StaleGeneration);
            }
            if lease.until_ms <= now {
                return Err(StreamError::LeaseExpired {
                    group: request.group.clone(),
                    member_id: request.member_id.clone(),
                });
            }
            let current = group_offset(&transaction, &request.group, partition)?;
            let (_, high_watermark) = partition_bounds(&transaction, partition)?;
            let Some(inflight_next_offset) = lease.inflight_next_offset else {
                return Err(StreamError::NoInflightClaim {
                    group: request.group.clone(),
                    member_id: request.member_id.clone(),
                    partition,
                });
            };
            if lease
                .inflight_until_ms
                .is_none_or(|until_ms| until_ms <= now)
            {
                return Err(StreamError::NoInflightClaim {
                    group: request.group.clone(),
                    member_id: request.member_id.clone(),
                    partition,
                });
            }
            let maximum_committable_offset = high_watermark.min(inflight_next_offset);
            if next_offset < current || next_offset > maximum_committable_offset {
                return Err(StreamError::InvalidCommit {
                    partition,
                    current,
                    requested: next_offset,
                    high_watermark: maximum_committable_offset,
                });
            }
            transaction.execute(
                "UPDATE stream_group_offsets
                 SET committed_offset = ?1
                 WHERE group_name = ?2 AND partition = ?3",
                params![
                    to_i64(next_offset)?,
                    &request.group,
                    i64::from(partition.get())
                ],
            )?;
            transaction.execute(
                "UPDATE stream_group_leases
                 SET inflight_until_ms = CASE
                         WHEN inflight_next_offset IS NOT NULL
                              AND ?1 = inflight_next_offset
                         THEN NULL ELSE inflight_until_ms END,
                     inflight_next_offset = CASE
                         WHEN inflight_next_offset IS NOT NULL
                              AND ?1 = inflight_next_offset
                         THEN NULL ELSE inflight_next_offset END
                 WHERE group_name = ?2 AND partition = ?3",
                params![
                    to_i64(next_offset)?,
                    &request.group,
                    i64::from(partition.get())
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn heartbeat(
        &self,
        config: &StreamConfig,
        request: HeartbeatRequest,
    ) -> Result<GroupAssignment, StreamError> {
        validate_identifier("name", &request.group)?;
        validate_identifier("member ID", &request.member_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        let lease_until = expires_at(now, config.lease_duration, "lease_duration")?;
        let mut connection = self.locked()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let generation = get_generation(&transaction, &request.group)?.ok_or_else(|| {
            StreamError::GroupMemberNotFound {
                group: request.group.clone(),
                member_id: request.member_id.clone(),
            }
        })?;
        active_member(&transaction, &request.group, &request.member_id, now)?;
        transaction.execute(
            "UPDATE stream_group_members SET expires_at_ms = ?1
             WHERE group_name = ?2 AND member_id = ?3",
            params![lease_until, &request.group, &request.member_id],
        )?;
        transaction.execute(
            "UPDATE stream_group_leases
             SET lease_until_ms = ?1,
                 inflight_until_ms = CASE
                     WHEN inflight_until_ms IS NULL THEN NULL ELSE ?1 END
             WHERE group_name = ?2 AND lease_owner = ?3",
            params![lease_until, &request.group, &request.member_id],
        )?;
        let partitions = owned_partitions(&transaction, &request.group, &request.member_id)?;
        transaction.commit()?;
        Ok(GroupAssignment {
            generation,
            partitions,
        })
    }

    pub(crate) fn enforce_retention(
        &self,
        config: &StreamConfig,
        now_ms: i64,
    ) -> Result<RetentionResult, StreamError> {
        let mut connection = self.locked()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = enforce_retention_locked(&transaction, config, now_ms, 0, true)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn stats(&self, config: &StreamConfig) -> Result<StreamStats, StreamError> {
        let connection = self.locked()?;
        let mut total_bytes = 0_u64;
        let mut partitions = Vec::with_capacity(usize::from(config.partitions));
        for value in 0..config.partitions {
            let partition = PartitionId::new(value);
            let (earliest_offset, next_offset) = partition_bounds(&connection, partition)?;
            let bytes = connection.query_row(
                "SELECT COALESCE(SUM(payload_bytes), 0)
                 FROM stream_records WHERE partition = ?1",
                params![i64::from(value)],
                |row| row.get::<_, i64>(0),
            )?;
            let bytes = from_i64(bytes)?;
            total_bytes = total_bytes
                .checked_add(bytes)
                .ok_or(StreamError::OffsetOverflow)?;
            partitions.push(PartitionStats {
                partition,
                earliest_offset,
                next_offset,
                bytes,
            });
        }
        Ok(StreamStats {
            total_bytes,
            partitions,
        })
    }

    pub(crate) fn inflight_count(&self, now_ms: i64) -> Result<u64, StreamError> {
        let connection = self.locked()?;
        let count = connection.query_row(
            "SELECT COUNT(*) FROM stream_group_leases
             WHERE inflight_until_ms IS NOT NULL AND inflight_until_ms > ?1",
            params![now_ms],
            |row| row.get::<_, i64>(0),
        )?;
        from_i64(count)
    }

    pub(crate) fn locked(&self) -> Result<MutexGuard<'_, Connection>, StreamError> {
        self.connection
            .lock()
            .map_err(|_| StreamError::LockPoisoned)
    }
}

fn idempotency_receipt(
    transaction: &Transaction<'_>,
    idempotency_key: &str,
) -> Result<Option<AppendReceipt>, StreamError> {
    let row: Option<(i64, i64)> = transaction
        .query_row(
            "SELECT partition, offset FROM stream_idempotency WHERE idempotency_key = ?1",
            params![idempotency_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(partition, offset)| {
        Ok(AppendReceipt {
            partition: partition_from_i64(partition)?,
            offset: from_i64(offset)?,
        })
    })
    .transpose()
}

fn partition_next_offset(
    connection: &Connection,
    partition: PartitionId,
) -> Result<Offset, StreamError> {
    let offset = connection.query_row(
        "SELECT next_offset FROM stream_partitions WHERE partition = ?1",
        params![i64::from(partition.get())],
        |row| row.get::<_, i64>(0),
    )?;
    from_i64(offset)
}

fn partition_bounds(
    connection: &Connection,
    partition: PartitionId,
) -> Result<(Offset, Offset), StreamError> {
    let next_offset = partition_next_offset(connection, partition)?;
    let earliest: Option<i64> = connection.query_row(
        "SELECT MIN(offset) FROM stream_records WHERE partition = ?1",
        params![i64::from(partition.get())],
        |row| row.get(0),
    )?;
    Ok((earliest.map_or(Ok(next_offset), from_i64)?, next_offset))
}

fn enforce_retention_locked(
    transaction: &Transaction<'_>,
    config: &StreamConfig,
    now_ms: i64,
    requested_bytes: u64,
    apply_age_retention: bool,
) -> Result<RetentionResult, StreamError> {
    if requested_bytes > config.retention_max_bytes {
        let current_bytes = total_bytes(transaction)?;
        return Err(StreamError::CapacityExceeded {
            max_bytes: config.retention_max_bytes,
            current_bytes,
            requested_bytes,
        });
    }
    let mut result = RetentionResult {
        deleted_records: 0,
        deleted_bytes: 0,
    };
    if apply_age_retention {
        let cutoff = now_ms
            .checked_sub(duration_millis(
                config.retention_max_age,
                "retention_max_age",
            )?)
            .ok_or(StreamError::OffsetOverflow)?;
        loop {
            let record: Option<(i64, i64, i64)> = transaction
                .query_row(
                    "SELECT partition, offset, payload_bytes
                     FROM stream_records
                     WHERE created_at_ms < ?1
                     ORDER BY created_at_ms, partition, offset
                     LIMIT 1",
                    params![cutoff],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((partition, offset, bytes)) = record else {
                break;
            };
            delete_record(transaction, partition, offset, bytes, &mut result)?;
        }
    }
    let mut current_bytes = total_bytes(transaction)?;
    if requested_bytes > 0 {
        if current_bytes
            .checked_add(requested_bytes)
            .is_none_or(|total| total > config.retention_max_bytes)
        {
            return Err(StreamError::CapacityExceeded {
                max_bytes: config.retention_max_bytes,
                current_bytes,
                requested_bytes,
            });
        }
        return Ok(result);
    }
    while current_bytes > config.retention_max_bytes {
        let record: Option<(i64, i64, i64)> = transaction
            .query_row(
                "SELECT partition, offset, payload_bytes
                 FROM stream_records
                 ORDER BY created_at_ms, partition, offset
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((partition, offset, bytes)) = record else {
            return Err(StreamError::CapacityExceeded {
                max_bytes: config.retention_max_bytes,
                current_bytes,
                requested_bytes,
            });
        };
        delete_record(transaction, partition, offset, bytes, &mut result)?;
        current_bytes = current_bytes
            .checked_sub(from_i64(bytes)?)
            .ok_or(StreamError::OffsetOverflow)?;
    }
    Ok(result)
}

fn delete_record(
    transaction: &Transaction<'_>,
    partition: i64,
    offset: i64,
    bytes: i64,
    result: &mut RetentionResult,
) -> Result<(), StreamError> {
    transaction.execute(
        "DELETE FROM stream_records WHERE partition = ?1 AND offset = ?2",
        params![partition, offset],
    )?;
    result.deleted_records = result
        .deleted_records
        .checked_add(1)
        .ok_or(StreamError::OffsetOverflow)?;
    result.deleted_bytes = result
        .deleted_bytes
        .checked_add(from_i64(bytes)?)
        .ok_or(StreamError::OffsetOverflow)?;
    Ok(())
}

fn total_bytes(connection: &Connection) -> Result<u64, StreamError> {
    let bytes = connection.query_row(
        "SELECT COALESCE(SUM(payload_bytes), 0) FROM stream_records",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    from_i64(bytes)
}

#[derive(Debug)]
struct Lease {
    owner: String,
    until_ms: i64,
    generation: u64,
    inflight_until_ms: Option<i64>,
    inflight_next_offset: Option<Offset>,
}

fn join_group(
    transaction: &Transaction<'_>,
    config: &StreamConfig,
    group: &str,
    member_id: &str,
    start: GroupStart,
    now: i64,
    lease_until: i64,
) -> Result<GroupAssignment, StreamError> {
    let created = transaction.execute(
        "INSERT OR IGNORE INTO stream_groups(group_name, generation) VALUES (?1, 0)",
        params![group],
    )? > 0;
    if created {
        for partition in 0..config.partitions {
            let partition = PartitionId::new(partition);
            let (earliest, latest) = partition_bounds(transaction, partition)?;
            let offset = match start {
                GroupStart::Earliest => earliest,
                GroupStart::Latest => latest,
            };
            transaction.execute(
                "INSERT INTO stream_group_offsets(group_name, partition, committed_offset)
                 VALUES (?1, ?2, ?3)",
                params![group, i64::from(partition.get()), to_i64(offset)?],
            )?;
        }
    }

    let expired_members = transaction.execute(
        "DELETE FROM stream_group_members
         WHERE group_name = ?1 AND expires_at_ms <= ?2",
        params![group, now],
    )?;
    let existing_member: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM stream_group_members
             WHERE group_name = ?1 AND member_id = ?2",
            params![group, member_id],
            |row| row.get(0),
        )
        .optional()?;
    transaction.execute(
        "INSERT INTO stream_group_members(group_name, member_id, expires_at_ms)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(group_name, member_id)
         DO UPDATE SET expires_at_ms = excluded.expires_at_ms",
        params![group, member_id, lease_until],
    )?;

    let membership_changed = created || expired_members > 0 || existing_member.is_none();
    let mut generation = get_generation(transaction, group)?
        .ok_or_else(|| StreamError::CorruptStore("group disappeared while joining".to_owned()))?;
    if membership_changed {
        generation = generation
            .checked_add(1)
            .ok_or(StreamError::OffsetOverflow)?;
        transaction.execute(
            "UPDATE stream_groups SET generation = ?1 WHERE group_name = ?2",
            params![to_i64(generation)?, group],
        )?;
        let mut statement = transaction.prepare(
            "SELECT member_id FROM stream_group_members
             WHERE group_name = ?1 ORDER BY member_id",
        )?;
        let members = statement
            .query_map(params![group], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        if members.is_empty() {
            return Err(StreamError::CorruptStore(
                "consumer group has no active members".to_owned(),
            ));
        }
        for partition in 0..config.partitions {
            let owner = &members[usize::from(partition) % members.len()];
            transaction.execute(
                "INSERT INTO stream_group_leases(
                    group_name, partition, lease_owner, lease_until_ms, generation,
                    inflight_until_ms, inflight_next_offset
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)
                 ON CONFLICT(group_name, partition) DO UPDATE SET
                    lease_owner = excluded.lease_owner,
                    lease_until_ms = excluded.lease_until_ms,
                    generation = excluded.generation,
                    inflight_until_ms = NULL,
                    inflight_next_offset = NULL",
                params![
                    group,
                    i64::from(partition),
                    owner,
                    lease_until,
                    to_i64(generation)?,
                ],
            )?;
        }
    } else {
        transaction.execute(
            "UPDATE stream_group_leases SET lease_until_ms = ?1
             WHERE group_name = ?2 AND lease_owner = ?3",
            params![lease_until, group, member_id],
        )?;
    }
    Ok(GroupAssignment {
        generation,
        partitions: owned_partitions(transaction, group, member_id)?,
    })
}

fn get_generation(connection: &Connection, group: &str) -> Result<Option<u64>, StreamError> {
    let value: Option<i64> = connection
        .query_row(
            "SELECT generation FROM stream_groups WHERE group_name = ?1",
            params![group],
            |row| row.get(0),
        )
        .optional()?;
    value.map(from_i64).transpose()
}

fn active_member(
    connection: &Connection,
    group: &str,
    member_id: &str,
    now: i64,
) -> Result<(), StreamError> {
    let expiry: Option<i64> = connection
        .query_row(
            "SELECT expires_at_ms FROM stream_group_members
             WHERE group_name = ?1 AND member_id = ?2",
            params![group, member_id],
            |row| row.get(0),
        )
        .optional()?;
    match expiry {
        None => Err(StreamError::GroupMemberNotFound {
            group: group.to_owned(),
            member_id: member_id.to_owned(),
        }),
        Some(expiry) if expiry <= now => Err(StreamError::LeaseExpired {
            group: group.to_owned(),
            member_id: member_id.to_owned(),
        }),
        Some(_) => Ok(()),
    }
}

fn owned_partitions(
    connection: &Connection,
    group: &str,
    member_id: &str,
) -> Result<Vec<PartitionId>, StreamError> {
    let mut statement = connection.prepare(
        "SELECT partition FROM stream_group_leases
         WHERE group_name = ?1 AND lease_owner = ?2
         ORDER BY partition",
    )?;
    statement
        .query_map(params![group, member_id], |row| row.get::<_, i64>(0))?
        .map(|row| partition_from_i64(row?))
        .collect()
}

fn group_offset(
    connection: &Connection,
    group: &str,
    partition: PartitionId,
) -> Result<Offset, StreamError> {
    let offset = connection.query_row(
        "SELECT committed_offset FROM stream_group_offsets
         WHERE group_name = ?1 AND partition = ?2",
        params![group, i64::from(partition.get())],
        |row| row.get::<_, i64>(0),
    )?;
    from_i64(offset)
}

fn get_lease(
    connection: &Connection,
    group: &str,
    partition: PartitionId,
) -> Result<Option<Lease>, StreamError> {
    let row: Option<(String, i64, i64, Option<i64>, Option<i64>)> = connection
        .query_row(
            "SELECT lease_owner, lease_until_ms, generation, inflight_until_ms,
                    inflight_next_offset
             FROM stream_group_leases
             WHERE group_name = ?1 AND partition = ?2",
            params![group, i64::from(partition.get())],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(owner, until_ms, generation, inflight_until_ms, inflight_next_offset)| {
            Ok(Lease {
                owner,
                until_ms,
                generation: from_i64(generation)?,
                inflight_until_ms,
                inflight_next_offset: inflight_next_offset.map(from_i64).transpose()?,
            })
        },
    )
    .transpose()
}

fn validate_database_owner(connection: &Connection) -> Result<(), StreamError> {
    let application_id: i32 =
        connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    if application_id != 0 && application_id != APPLICATION_ID {
        return Err(StreamError::ForeignDatabase { application_id });
    }
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'stream_%' AND name NOT LIKE 'sqlite_%'",
    )?;
    let foreign_table = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .next()
        .transpose()?;
    if let Some(table) = foreign_table {
        return Err(StreamError::ForeignTable { table });
    }
    if application_id == 0 {
        connection.pragma_update(None, "application_id", APPLICATION_ID)?;
    }
    Ok(())
}

fn migrate(connection: &Connection, config: &StreamConfig) -> Result<(), StreamError> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS stream_metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stream_partitions (
            partition INTEGER PRIMARY KEY NOT NULL,
            next_offset INTEGER NOT NULL CHECK (next_offset >= 0)
        );
        CREATE TABLE IF NOT EXISTS stream_records (
            partition INTEGER NOT NULL,
            offset INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            payload_bytes INTEGER NOT NULL CHECK (payload_bytes >= 0),
            created_at TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            PRIMARY KEY (partition, offset),
            FOREIGN KEY (partition) REFERENCES stream_partitions(partition)
        );
        CREATE TABLE IF NOT EXISTS stream_idempotency (
            idempotency_key TEXT PRIMARY KEY NOT NULL,
            partition INTEGER NOT NULL,
            offset INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stream_groups (
            group_name TEXT PRIMARY KEY NOT NULL,
            generation INTEGER NOT NULL CHECK (generation >= 0)
        );
        CREATE TABLE IF NOT EXISTS stream_group_members (
            group_name TEXT NOT NULL,
            member_id TEXT NOT NULL,
            expires_at_ms INTEGER NOT NULL,
            PRIMARY KEY (group_name, member_id),
            FOREIGN KEY (group_name) REFERENCES stream_groups(group_name) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS stream_group_offsets (
            group_name TEXT NOT NULL,
            partition INTEGER NOT NULL,
            committed_offset INTEGER NOT NULL CHECK (committed_offset >= 0),
            PRIMARY KEY (group_name, partition),
            FOREIGN KEY (group_name) REFERENCES stream_groups(group_name) ON DELETE CASCADE,
            FOREIGN KEY (partition) REFERENCES stream_partitions(partition)
        );
        CREATE TABLE IF NOT EXISTS stream_group_leases (
            group_name TEXT NOT NULL,
            partition INTEGER NOT NULL,
            lease_owner TEXT NOT NULL,
            lease_until_ms INTEGER NOT NULL,
            generation INTEGER NOT NULL,
            inflight_until_ms INTEGER,
            inflight_next_offset INTEGER,
            PRIMARY KEY (group_name, partition),
            FOREIGN KEY (group_name, partition)
                REFERENCES stream_group_offsets(group_name, partition) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS stream_records_created_at
            ON stream_records(created_at_ms, partition, offset);
        CREATE INDEX IF NOT EXISTS stream_group_leases_inflight
            ON stream_group_leases(inflight_until_ms);
        ",
    )?;
    migrate_idempotency_tombstones(&transaction)?;
    let stored_partitions: Option<String> = transaction
        .query_row(
            "SELECT value FROM stream_metadata WHERE key = 'partition_count'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    match stored_partitions {
        Some(value) if value == config.partitions.to_string() => {}
        Some(value) => {
            return Err(StreamError::InvalidConfig(format!(
                "stream has {value} partitions but configuration requests {}",
                config.partitions
            )));
        }
        None => {
            transaction.execute(
                "INSERT INTO stream_metadata(key, value) VALUES ('partition_count', ?1)",
                params![config.partitions.to_string()],
            )?;
            for partition in 0..config.partitions {
                transaction.execute(
                    "INSERT INTO stream_partitions(partition, next_offset) VALUES (?1, 0)",
                    params![i64::from(partition)],
                )?;
            }
        }
    }
    transaction.commit()?;
    Ok(())
}

fn migrate_idempotency_tombstones(transaction: &Transaction<'_>) -> Result<(), StreamError> {
    let mut statement = transaction.prepare("PRAGMA foreign_key_list(stream_idempotency)")?;
    let references_records = statement
        .query_map([], |row| row.get::<_, String>(2))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|table| table == "stream_records");
    drop(statement);
    if !references_records {
        return Ok(());
    }

    transaction.execute_batch(
        "
        CREATE TABLE stream_idempotency_tombstones (
            idempotency_key TEXT PRIMARY KEY NOT NULL,
            partition INTEGER NOT NULL,
            offset INTEGER NOT NULL
        );
        INSERT INTO stream_idempotency_tombstones(idempotency_key, partition, offset)
            SELECT idempotency_key, partition, offset FROM stream_idempotency;
        DROP TABLE stream_idempotency;
        ALTER TABLE stream_idempotency_tombstones RENAME TO stream_idempotency;
        ",
    )?;
    Ok(())
}

fn duration_millis(duration: Duration, name: &str) -> Result<i64, StreamError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| StreamError::InvalidConfig(format!("{name} is too large")))
}

fn expires_at(now: i64, duration: Duration, name: &str) -> Result<i64, StreamError> {
    now.checked_add(duration_millis(duration, name)?)
        .ok_or(StreamError::OffsetOverflow)
}

fn to_i64(value: u64) -> Result<i64, StreamError> {
    i64::try_from(value).map_err(|_| StreamError::OffsetOverflow)
}

fn usize_to_i64(value: usize) -> Result<i64, StreamError> {
    i64::try_from(value).map_err(|_| StreamError::OffsetOverflow)
}

fn from_i64(value: i64) -> Result<u64, StreamError> {
    u64::try_from(value)
        .map_err(|_| StreamError::CorruptStore("sqlite value must be non-negative".to_owned()))
}

fn partition_from_i64(value: i64) -> Result<PartitionId, StreamError> {
    let value = u16::try_from(value)
        .map_err(|_| StreamError::CorruptStore("invalid persisted partition".to_owned()))?;
    Ok(PartitionId::new(value))
}
