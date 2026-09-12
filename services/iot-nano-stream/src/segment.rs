use std::{
    cmp,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AppendedRecord, Offset, PartitionId, StreamConfig, StreamError, StreamRecord,
    record::decode_message,
};

const MANIFEST_FILE: &str = "manifest.json";
const FORMAT_VERSION: u16 = 1;
const FRAME_HEADER_BYTES: u64 = 8;
const INDEX_ENTRY_BYTES: u64 = 16;

pub(crate) const fn frame_header_bytes() -> u64 {
    FRAME_HEADER_BYTES
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    format_version: u16,
    partition_count: u16,
}

#[derive(Debug, Clone)]
struct SegmentMeta {
    base_offset: Offset,
    next_offset: Offset,
    log_path: PathBuf,
    index_path: PathBuf,
    bytes: u64,
    newest_received_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub(crate) struct PartitionLog {
    partition: PartitionId,
    directory: PathBuf,
    segments: Vec<SegmentMeta>,
    next_offset: Offset,
    max_record_bytes: usize,
}

#[derive(Debug)]
struct Frame {
    message: crate::StreamMessage,
    bytes: u64,
}

#[derive(Debug)]
struct Scan {
    bytes: u64,
    next_offset: Offset,
    index_entries: Vec<(Offset, u64)>,
    newest_received_at: Option<DateTime<Utc>>,
}

enum FrameReadError {
    Truncated,
    Corrupt(String),
    Io(io::Error),
}

pub(crate) fn initialize_root(root: &Path, partition_count: u16) -> Result<(), StreamError> {
    fs::create_dir_all(root)?;
    let manifest_path = root.join(MANIFEST_FILE);
    if manifest_path.exists() {
        let manifest = serde_json::from_slice::<Manifest>(&fs::read(&manifest_path)?)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(StreamError::InvalidConfig(format!(
                "stream format version {} is not supported",
                manifest.format_version
            )));
        }
        if manifest.partition_count != partition_count {
            return Err(StreamError::InvalidConfig(format!(
                "stream has {} partitions but configuration requests {}",
                manifest.partition_count, partition_count
            )));
        }
        return Ok(());
    }

    write_json_atomically(
        root,
        MANIFEST_FILE,
        &Manifest {
            format_version: FORMAT_VERSION,
            partition_count,
        },
    )
}

impl PartitionLog {
    pub(crate) fn open(
        root: &Path,
        partition: PartitionId,
        config: &StreamConfig,
    ) -> Result<Self, StreamError> {
        let directory = root
            .join("partitions")
            .join(format!("{:04}", partition.get()));
        fs::create_dir_all(&directory)?;
        let mut log_paths = log_paths(&directory)?;
        if log_paths.is_empty() {
            create_segment(&directory, 0)?;
            log_paths.push((0, log_path(&directory, 0)));
        }

        let mut segments = Vec::with_capacity(log_paths.len());
        let mut expected_base = None;
        for (index, (base_offset, path)) in log_paths.iter().enumerate() {
            if let Some(expected_base) = expected_base {
                if *base_offset != expected_base {
                    return Err(StreamError::CorruptSegment {
                        path: path.clone(),
                        reason: format!(
                            "base offset {} does not follow prior offset {}",
                            base_offset, expected_base
                        ),
                    });
                }
            }

            let is_active = index + 1 == log_paths.len();
            let scan = scan_segment(path, *base_offset, config, is_active)?;
            let index_path = index_path(&directory, *base_offset);
            write_index(&index_path, &scan.index_entries)?;
            expected_base = Some(scan.next_offset);
            segments.push(SegmentMeta {
                base_offset: *base_offset,
                next_offset: scan.next_offset,
                log_path: path.clone(),
                index_path,
                bytes: scan.bytes,
                newest_received_at: scan.newest_received_at,
            });
        }

        let next_offset = segments
            .last()
            .map(|segment| segment.next_offset)
            .unwrap_or(0);

        Ok(Self {
            partition,
            directory,
            segments,
            next_offset,
            max_record_bytes: config.max_record_bytes,
        })
    }

    pub(crate) fn append(
        &mut self,
        _root: &Path,
        config: &StreamConfig,
        payload: Vec<u8>,
        received_at: DateTime<Utc>,
    ) -> Result<AppendedRecord, StreamError> {
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| StreamError::RecordTooLarge {
                encoded_bytes: payload.len(),
                max_bytes: u32::MAX as usize,
            })?;
        let frame_bytes = FRAME_HEADER_BYTES
            .checked_add(u64::from(payload_length))
            .ok_or_else(|| StreamError::Io(io::Error::other("frame length overflow")))?;

        let active = self
            .segments
            .last()
            .expect("partition log always has an active segment");
        if active.bytes > 0
            && active
                .bytes
                .checked_add(frame_bytes)
                .is_none_or(|bytes| bytes > config.segment_max_bytes)
        {
            create_segment(&self.directory, self.next_offset)?;
            self.segments.push(SegmentMeta {
                base_offset: self.next_offset,
                next_offset: self.next_offset,
                log_path: log_path(&self.directory, self.next_offset),
                index_path: index_path(&self.directory, self.next_offset),
                bytes: 0,
                newest_received_at: None,
            });
        }

        let offset = self.next_offset;
        let active = self
            .segments
            .last_mut()
            .expect("partition log always has an active segment");
        let position = active.bytes;
        let mut log = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&active.log_path)?;
        log.write_all(&payload_length.to_le_bytes())?;
        log.write_all(&crc32fast::hash(&payload).to_le_bytes())?;
        log.write_all(&payload)?;
        log.sync_data()?;

        active.bytes = active
            .bytes
            .checked_add(frame_bytes)
            .ok_or_else(|| StreamError::Io(io::Error::other("segment length overflow")))?;
        active.next_offset = active
            .next_offset
            .checked_add(1)
            .ok_or_else(|| StreamError::Io(io::Error::other("offset overflow")))?;
        active.newest_received_at = Some(
            active
                .newest_received_at
                .map_or(received_at, |previous| previous.max(received_at)),
        );
        self.next_offset = active.next_offset;

        if offset % config.index_stride == 0 {
            let mut index = OpenOptions::new().append(true).open(&active.index_path)?;
            index.write_all(&offset.to_le_bytes())?;
            index.write_all(&position.to_le_bytes())?;
            index.sync_data()?;
        }

        Ok(AppendedRecord {
            partition: self.partition,
            offset,
        })
    }

    pub(crate) fn read(
        &self,
        offset: Offset,
        limit: usize,
    ) -> Result<Vec<StreamRecord>, StreamError> {
        let mut records = Vec::with_capacity(limit);
        for segment in &self.segments {
            if records.len() == limit || segment.next_offset <= offset {
                continue;
            }
            let requested = cmp::max(offset, segment.base_offset);
            read_segment(
                self.partition,
                segment,
                requested,
                limit - records.len(),
                self.max_record_bytes,
                &mut records,
            )?;
        }

        Ok(records)
    }

    pub(crate) fn bounds(&self) -> (Offset, Offset) {
        (
            self.segments
                .first()
                .map(|segment| segment.base_offset)
                .unwrap_or(self.next_offset),
            self.next_offset,
        )
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.segments.iter().map(|segment| segment.bytes).sum()
    }

    pub(crate) fn oldest_closed_segment(&self) -> Option<(usize, DateTime<Utc>)> {
        self.segments
            .iter()
            .take(self.segments.len().saturating_sub(1))
            .enumerate()
            .filter_map(|(index, segment)| segment.newest_received_at.map(|at| (index, at)))
            .min_by_key(|(_, at)| *at)
    }

    pub(crate) fn remove_closed_segment(&mut self, index: usize) -> Result<u64, StreamError> {
        if index >= self.segments.len().saturating_sub(1) {
            return Err(StreamError::Io(io::Error::other(
                "cannot remove the active stream segment",
            )));
        }

        let segment = self.segments.remove(index);
        fs::remove_file(segment.log_path)?;
        fs::remove_file(segment.index_path)?;
        sync_directory(&self.directory)?;
        Ok(segment.bytes)
    }
}

fn log_paths(directory: &Path) -> Result<Vec<(Offset, PathBuf)>, StreamError> {
    let mut logs = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let is_log = path.extension().is_some_and(|extension| extension == "log");
            if !is_log {
                return None;
            }
            let base = path.file_stem()?.to_str()?.parse::<Offset>().ok()?;
            Some((base, path))
        })
        .collect::<Vec<_>>();
    logs.sort_by_key(|(base, _)| *base);
    Ok(logs)
}

fn create_segment(directory: &Path, base_offset: Offset) -> Result<(), StreamError> {
    let log = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(log_path(directory, base_offset))?;
    log.sync_all()?;
    let index = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(index_path(directory, base_offset))?;
    index.sync_all()?;
    sync_directory(directory)
}

fn scan_segment(
    path: &Path,
    base_offset: Offset,
    config: &StreamConfig,
    is_active: bool,
) -> Result<Scan, StreamError> {
    let mut file = OpenOptions::new().read(true).write(is_active).open(path)?;
    let mut position = 0_u64;
    let mut offset = base_offset;
    let mut index_entries = Vec::new();
    let mut newest_received_at = None;

    loop {
        let frame_position = position;
        match read_frame(&mut file, config.max_record_bytes) {
            Ok(None) => break,
            Ok(Some(frame)) => {
                if offset % config.index_stride == 0 {
                    index_entries.push((offset, frame_position));
                }
                newest_received_at = Some(
                    newest_received_at
                        .map_or(frame.message.received_at(), |previous: DateTime<Utc>| {
                            previous.max(frame.message.received_at())
                        }),
                );
                position = position.checked_add(frame.bytes).ok_or_else(|| {
                    StreamError::CorruptSegment {
                        path: path.to_path_buf(),
                        reason: "frame positions overflowed u64".to_owned(),
                    }
                })?;
                offset = offset
                    .checked_add(1)
                    .ok_or_else(|| StreamError::CorruptSegment {
                        path: path.to_path_buf(),
                        reason: "offset overflowed u64".to_owned(),
                    })?;
            }
            Err(FrameReadError::Truncated) if is_active => {
                file.set_len(frame_position)?;
                file.sync_all()?;
                break;
            }
            Err(error) => return Err(frame_error(path, error)),
        }
    }

    Ok(Scan {
        bytes: position,
        next_offset: offset,
        index_entries,
        newest_received_at,
    })
}

fn read_segment(
    partition: PartitionId,
    segment: &SegmentMeta,
    requested: Offset,
    limit: usize,
    max_record_bytes: usize,
    records: &mut Vec<StreamRecord>,
) -> Result<(), StreamError> {
    let (mut offset, position) = index_position(segment, requested)?;
    let mut log = File::open(&segment.log_path)?;
    log.seek(SeekFrom::Start(position))?;
    let initial_len = records.len();

    while offset < segment.next_offset && records.len() - initial_len < limit {
        let frame = match read_frame(&mut log, max_record_bytes) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(StreamError::CorruptSegment {
                    path: segment.log_path.clone(),
                    reason: "segment ended before its recorded next offset".to_owned(),
                });
            }
            Err(error) => return Err(frame_error(&segment.log_path, error)),
        };
        if offset >= requested {
            records.push(StreamRecord {
                partition,
                offset,
                message: frame.message,
            });
        }
        offset = offset
            .checked_add(1)
            .ok_or_else(|| StreamError::CorruptSegment {
                path: segment.log_path.clone(),
                reason: "offset overflowed u64".to_owned(),
            })?;
    }

    Ok(())
}

fn index_position(segment: &SegmentMeta, requested: Offset) -> Result<(Offset, u64), StreamError> {
    let mut index = File::open(&segment.index_path)?;
    let mut result = (segment.base_offset, 0_u64);
    loop {
        let mut bytes = [0_u8; INDEX_ENTRY_BYTES as usize];
        match read_exact_or_eof(&mut index, &mut bytes) {
            Ok(false) => break,
            Ok(true) => {
                let offset = u64::from_le_bytes(bytes[..8].try_into().expect("slice length"));
                let position = u64::from_le_bytes(bytes[8..].try_into().expect("slice length"));
                if offset > requested {
                    break;
                }
                result = (offset, position);
            }
            Err(FrameReadError::Truncated) => {
                return Err(StreamError::CorruptSegment {
                    path: segment.index_path.clone(),
                    reason: "sparse index has a partial entry".to_owned(),
                });
            }
            Err(error) => return Err(frame_error(&segment.index_path, error)),
        }
    }

    Ok(result)
}

fn read_frame(file: &mut File, max_record_bytes: usize) -> Result<Option<Frame>, FrameReadError> {
    let mut length = [0_u8; 4];
    if !read_exact_or_eof(file, &mut length)? {
        return Ok(None);
    }
    let payload_length = usize::try_from(u32::from_le_bytes(length))
        .map_err(|_| FrameReadError::Corrupt("payload length does not fit usize".to_owned()))?;
    if payload_length > max_record_bytes {
        return Err(FrameReadError::Corrupt(format!(
            "payload length {payload_length} exceeds {max_record_bytes}"
        )));
    }

    let mut expected_crc = [0_u8; 4];
    read_exact_required(file, &mut expected_crc)?;
    let mut payload = vec![0_u8; payload_length];
    if !payload.is_empty() {
        read_exact_required(file, &mut payload)?;
    }
    let actual_crc = crc32fast::hash(&payload);
    if actual_crc != u32::from_le_bytes(expected_crc) {
        return Err(FrameReadError::Corrupt(
            "payload CRC32 does not match".to_owned(),
        ));
    }

    let message = decode_message(&payload)
        .map_err(|error| FrameReadError::Corrupt(format!("invalid telemetry record: {error}")))?;
    let bytes = FRAME_HEADER_BYTES
        .checked_add(u64::try_from(payload_length).expect("u32 fits u64"))
        .ok_or_else(|| FrameReadError::Corrupt("frame byte count overflowed u64".to_owned()))?;
    Ok(Some(Frame { message, bytes }))
}

fn read_exact_or_eof(file: &mut File, buffer: &mut [u8]) -> Result<bool, FrameReadError> {
    let first = file.read(buffer).map_err(FrameReadError::Io)?;
    if first == 0 {
        return Ok(false);
    }
    file.read_exact(&mut buffer[first..]).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            FrameReadError::Truncated
        } else {
            FrameReadError::Io(error)
        }
    })?;
    Ok(true)
}

fn read_exact_required(file: &mut File, buffer: &mut [u8]) -> Result<(), FrameReadError> {
    if read_exact_or_eof(file, buffer)? {
        Ok(())
    } else {
        Err(FrameReadError::Truncated)
    }
}

fn write_index(path: &Path, entries: &[(Offset, u64)]) -> Result<(), StreamError> {
    let temp_path = path.with_extension("idx.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    for (offset, position) in entries {
        file.write_all(&offset.to_le_bytes())?;
        file.write_all(&position.to_le_bytes())?;
    }
    file.sync_all()?;
    fs::rename(temp_path, path)?;
    sync_directory(
        path.parent()
            .expect("index file path always has a parent directory"),
    )
}

fn write_json_atomically<T: Serialize>(
    directory: &Path,
    name: &str,
    value: &T,
) -> Result<(), StreamError> {
    let final_path = directory.join(name);
    let temp_path = directory.join(format!("{name}.tmp"));
    let payload = serde_json::to_vec(value)?;
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
                ErrorKind::InvalidInput | ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(StreamError::Io(error)),
    }
}

fn frame_error(path: &Path, error: FrameReadError) -> StreamError {
    match error {
        FrameReadError::Truncated => StreamError::CorruptSegment {
            path: path.to_path_buf(),
            reason: "truncated frame".to_owned(),
        },
        FrameReadError::Corrupt(reason) => StreamError::CorruptSegment {
            path: path.to_path_buf(),
            reason,
        },
        FrameReadError::Io(error) => StreamError::Io(error),
    }
}

fn log_path(directory: &Path, base_offset: Offset) -> PathBuf {
    directory.join(format!("{base_offset:020}.log"))
}

fn index_path(directory: &Path, base_offset: Offset) -> PathBuf {
    directory.join(format!("{base_offset:020}.idx"))
}
