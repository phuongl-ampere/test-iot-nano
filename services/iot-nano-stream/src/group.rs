use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::{LocalStream, Offset, PartitionId, StreamError, StreamRecord};

const GROUP_FORMAT_VERSION: u16 = 1;
const LEASE_DURATION: Duration = Duration::minutes(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStart {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignment {
    pub generation: u64,
    pub partitions: Vec<PartitionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCommit {
    pub partition: PartitionId,
    pub next_offset: Offset,
}

#[derive(Debug, Clone)]
pub struct PollBatch {
    pub generation: u64,
    pub records: Vec<StreamRecord>,
    pub commits: Vec<PartitionCommit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPartitionStats {
    pub partition: PartitionId,
    pub committed_next_offset: Offset,
    pub high_watermark: Offset,
    pub lag: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStats {
    pub group: String,
    pub generation: u64,
    pub partitions: Vec<GroupPartitionStats>,
}

impl GroupStats {
    pub fn total_lag(&self) -> u64 {
        self.partitions.iter().map(|partition| partition.lag).sum()
    }

    pub fn committed_offset(&self, partition: PartitionId) -> Option<Offset> {
        self.partitions
            .iter()
            .find(|stats| stats.partition == partition)
            .map(|stats| stats.committed_next_offset)
    }
}

#[derive(Debug, Clone)]
pub struct StreamConsumer {
    stream: LocalStream,
    group: String,
    member_id: String,
    directory: PathBuf,
    generation: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupState {
    format_version: u16,
    generation: u64,
    members: BTreeMap<String, MemberState>,
    assignments: BTreeMap<u16, PartitionLease>,
    next_offsets: BTreeMap<u16, Offset>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MemberState {
    heartbeat_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartitionLease {
    member_id: String,
    expires_at: DateTime<Utc>,
}

pub(crate) fn join_group(
    stream: LocalStream,
    group: &str,
    member_id: &str,
    start: GroupStart,
    now: DateTime<Utc>,
) -> Result<StreamConsumer, StreamError> {
    validate_identifier("name", group)?;
    validate_identifier("member ID", member_id)?;
    let directory = stream.root().join("groups").join(group);
    fs::create_dir_all(&directory)?;

    let generation = mutate_state(&stream, &directory, start, |state| {
        let mut membership_changed = prune_expired_members(state, now);
        membership_changed |= state
            .members
            .insert(member_id.to_owned(), MemberState { heartbeat_at: now })
            .is_none();
        refresh_assignments(
            state,
            member_id,
            now,
            stream.partition_count(),
            membership_changed,
        );
        Ok(state.generation)
    })?;

    Ok(StreamConsumer {
        stream,
        group: group.to_owned(),
        member_id: member_id.to_owned(),
        directory,
        generation,
    })
}

impl StreamConsumer {
    pub fn heartbeat(&mut self, now: DateTime<Utc>) -> Result<GroupAssignment, StreamError> {
        let assignment = mutate_state(
            &self.stream,
            &self.directory,
            GroupStart::Earliest,
            |state| {
                let member = state.members.get(&self.member_id).ok_or_else(|| {
                    StreamError::GroupMemberNotFound {
                        group: self.group.clone(),
                        member_id: self.member_id.clone(),
                    }
                })?;
                if member.heartbeat_at + LEASE_DURATION <= now {
                    return Err(StreamError::LeaseExpired {
                        group: self.group.clone(),
                        member_id: self.member_id.clone(),
                    });
                }

                state
                    .members
                    .insert(self.member_id.clone(), MemberState { heartbeat_at: now });
                let membership_changed = prune_expired_members(state, now);
                refresh_assignments(
                    state,
                    &self.member_id,
                    now,
                    self.stream.partition_count(),
                    membership_changed,
                );
                Ok(GroupAssignment {
                    generation: state.generation,
                    partitions: partitions_for_member(state, &self.member_id),
                })
            },
        )?;
        self.generation = assignment.generation;
        Ok(assignment)
    }

    pub fn poll(&self, limit: usize, now: DateTime<Utc>) -> Result<PollBatch, StreamError> {
        if limit == 0 {
            return Ok(PollBatch {
                generation: self.generation,
                records: Vec::new(),
                commits: Vec::new(),
            });
        }

        let (generation, assignments, offsets) =
            read_state(&self.directory, |state| self.current_assignment(state, now))?;
        if generation != self.generation {
            return Err(StreamError::StaleGeneration);
        }

        let mut records = Vec::with_capacity(limit);
        let mut commits = Vec::new();
        for partition in assignments {
            if records.len() == limit {
                break;
            }
            let requested = offsets.get(&partition.get()).copied().ok_or_else(|| {
                StreamError::CorruptSegment {
                    path: self.directory.join("state.json"),
                    reason: format!("missing offset for partition {}", partition.get()),
                }
            })?;
            let (earliest, high_watermark) = self.stream.partition_bounds(partition)?;
            if requested < earliest {
                return Err(StreamError::OffsetOutOfRange {
                    partition,
                    requested,
                    earliest,
                });
            }
            if requested > high_watermark {
                return Err(StreamError::CorruptSegment {
                    path: self.directory.join("state.json"),
                    reason: format!(
                        "committed offset {requested} exceeds high watermark {high_watermark}"
                    ),
                });
            }

            let partition_records =
                self.stream
                    .read_partition(partition, requested, limit - records.len())?;
            if let Some(last) = partition_records.last() {
                commits.push(PartitionCommit {
                    partition,
                    next_offset: last.offset.checked_add(1).ok_or_else(|| {
                        StreamError::CorruptSegment {
                            path: self.directory.join("state.json"),
                            reason: "record offset overflowed u64".to_owned(),
                        }
                    })?,
                });
            }
            records.extend(partition_records);
        }

        Ok(PollBatch {
            generation,
            records,
            commits,
        })
    }

    pub fn commit(&self, batch: PollBatch, now: DateTime<Utc>) -> Result<(), StreamError> {
        if batch.generation != self.generation {
            return Err(StreamError::StaleGeneration);
        }

        mutate_state(
            &self.stream,
            &self.directory,
            GroupStart::Earliest,
            |state| {
                self.current_assignment(state, now)?;
                if state.generation != batch.generation {
                    return Err(StreamError::StaleGeneration);
                }

                for commit in batch.commits {
                    let current = state
                        .next_offsets
                        .get(&commit.partition.get())
                        .copied()
                        .ok_or_else(|| StreamError::CorruptSegment {
                            path: self.directory.join("state.json"),
                            reason: format!(
                                "missing offset for partition {}",
                                commit.partition.get()
                            ),
                        })?;
                    let (_, high_watermark) = self.stream.partition_bounds(commit.partition)?;
                    if commit.next_offset < current || commit.next_offset > high_watermark {
                        return Err(StreamError::CorruptSegment {
                            path: self.directory.join("state.json"),
                            reason: format!(
                                "invalid committed next offset {} for partition {}",
                                commit.next_offset,
                                commit.partition.get()
                            ),
                        });
                    }
                    state
                        .next_offsets
                        .insert(commit.partition.get(), commit.next_offset);
                }
                Ok(())
            },
        )
    }

    pub fn group_stats(&self) -> Result<GroupStats, StreamError> {
        let (generation, offsets) = read_state(&self.directory, |state| {
            Ok((state.generation, state.next_offsets.clone()))
        })?;
        let mut partitions = Vec::with_capacity(usize::from(self.stream.partition_count()));
        for partition in 0..self.stream.partition_count() {
            let partition = PartitionId::new(partition);
            let committed_next_offset =
                offsets.get(&partition.get()).copied().ok_or_else(|| {
                    StreamError::CorruptSegment {
                        path: self.directory.join("state.json"),
                        reason: format!("missing offset for partition {}", partition.get()),
                    }
                })?;
            let (_, high_watermark) = self.stream.partition_bounds(partition)?;
            partitions.push(GroupPartitionStats {
                partition,
                committed_next_offset,
                high_watermark,
                lag: high_watermark.saturating_sub(committed_next_offset),
            });
        }

        Ok(GroupStats {
            group: self.group.clone(),
            generation,
            partitions,
        })
    }

    fn current_assignment(
        &self,
        state: &GroupState,
        now: DateTime<Utc>,
    ) -> Result<(u64, Vec<PartitionId>, BTreeMap<u16, Offset>), StreamError> {
        let member =
            state
                .members
                .get(&self.member_id)
                .ok_or_else(|| StreamError::GroupMemberNotFound {
                    group: self.group.clone(),
                    member_id: self.member_id.clone(),
                })?;
        if member.heartbeat_at + LEASE_DURATION <= now {
            return Err(StreamError::LeaseExpired {
                group: self.group.clone(),
                member_id: self.member_id.clone(),
            });
        }
        if state.generation != self.generation {
            return Err(StreamError::StaleGeneration);
        }
        let partitions = partitions_for_member(state, &self.member_id);
        for partition in &partitions {
            let lease = state.assignments.get(&partition.get()).ok_or_else(|| {
                StreamError::CorruptSegment {
                    path: self.directory.join("state.json"),
                    reason: format!("missing lease for partition {}", partition.get()),
                }
            })?;
            if lease.expires_at <= now {
                return Err(StreamError::LeaseExpired {
                    group: self.group.clone(),
                    member_id: self.member_id.clone(),
                });
            }
        }
        Ok((state.generation, partitions, state.next_offsets.clone()))
    }
}

fn prune_expired_members(state: &mut GroupState, now: DateTime<Utc>) -> bool {
    let before = state.members.len();
    state
        .members
        .retain(|_, member| member.heartbeat_at + LEASE_DURATION > now);
    before != state.members.len()
}

fn refresh_assignments(
    state: &mut GroupState,
    member_id: &str,
    now: DateTime<Utc>,
    partition_count: u16,
    membership_changed: bool,
) {
    let members = state.members.keys().cloned().collect::<Vec<_>>();
    let expected = (0..partition_count)
        .map(|partition| {
            (
                partition,
                members[usize::from(partition) % members.len()].clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let assignments_match = state.assignments.len() == expected.len()
        && expected.iter().all(|(partition, owner)| {
            state
                .assignments
                .get(partition)
                .is_some_and(|lease| lease.member_id == *owner && lease.expires_at > now)
        });

    if membership_changed || !assignments_match {
        state.generation = state.generation.saturating_add(1);
        state.assignments = expected
            .into_iter()
            .map(|(partition, owner)| {
                (
                    partition,
                    PartitionLease {
                        member_id: owner,
                        expires_at: now + LEASE_DURATION,
                    },
                )
            })
            .collect();
    } else {
        for lease in state
            .assignments
            .values_mut()
            .filter(|lease| lease.member_id == member_id)
        {
            lease.expires_at = now + LEASE_DURATION;
        }
    }
}

fn partitions_for_member(state: &GroupState, member_id: &str) -> Vec<PartitionId> {
    state
        .assignments
        .iter()
        .filter_map(|(partition, lease)| {
            (lease.member_id == member_id).then_some(PartitionId::new(*partition))
        })
        .collect()
}

fn mutate_state<T>(
    stream: &LocalStream,
    directory: &Path,
    start: GroupStart,
    operation: impl FnOnce(&mut GroupState) -> Result<T, StreamError>,
) -> Result<T, StreamError> {
    let lock = open_lock(directory)?;
    FileExt::lock_exclusive(&lock)?;
    let result = (|| -> Result<T, StreamError> {
        let mut state = load_or_create_state(stream, directory, start)?;
        let output = operation(&mut state)?;
        write_state(directory, &state)?;
        Ok(output)
    })();
    let unlock_result = FileExt::unlock(&lock);
    match (result, unlock_result) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(StreamError::Io(error)),
    }
}

fn read_state<T>(
    directory: &Path,
    operation: impl FnOnce(&GroupState) -> Result<T, StreamError>,
) -> Result<T, StreamError> {
    let lock = open_lock(directory)?;
    FileExt::lock_exclusive(&lock)?;
    let result = (|| -> Result<T, StreamError> {
        let state = load_state(directory)?;
        operation(&state)
    })();
    let unlock_result = FileExt::unlock(&lock);
    match (result, unlock_result) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(StreamError::Io(error)),
    }
}

fn open_lock(directory: &Path) -> Result<File, StreamError> {
    fs::create_dir_all(directory)?;
    Ok(OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(directory.join("state.lock"))?)
}

fn load_or_create_state(
    stream: &LocalStream,
    directory: &Path,
    start: GroupStart,
) -> Result<GroupState, StreamError> {
    let path = directory.join("state.json");
    if path.exists() {
        return load_state(directory);
    }

    let mut next_offsets = BTreeMap::new();
    for partition in 0..stream.partition_count() {
        let (earliest, latest) = stream.partition_bounds(PartitionId::new(partition))?;
        next_offsets.insert(
            partition,
            match start {
                GroupStart::Earliest => earliest,
                GroupStart::Latest => latest,
            },
        );
    }

    Ok(GroupState {
        format_version: GROUP_FORMAT_VERSION,
        generation: 0,
        members: BTreeMap::new(),
        assignments: BTreeMap::new(),
        next_offsets,
    })
}

fn load_state(directory: &Path) -> Result<GroupState, StreamError> {
    let path = directory.join("state.json");
    let state = serde_json::from_slice::<GroupState>(&fs::read(&path)?)?;
    if state.format_version != GROUP_FORMAT_VERSION {
        return Err(StreamError::InvalidConfig(format!(
            "consumer group state has unsupported format version {}",
            state.format_version
        )));
    }
    Ok(state)
}

fn write_state(directory: &Path, state: &GroupState) -> Result<(), StreamError> {
    let temp_path = directory.join("state.json.tmp");
    let final_path = directory.join("state.json");
    let payload = serde_json::to_vec(state)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    file.write_all(&payload)?;
    file.sync_all()?;
    fs::rename(temp_path, final_path)?;
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> Result<(), StreamError> {
    let file = File::open(directory)?;
    match file.sync_all() {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(StreamError::Io(error)),
    }
}

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), StreamError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Ok(());
    }

    Err(StreamError::InvalidGroupIdentifier {
        kind,
        value: value.to_owned(),
    })
}
