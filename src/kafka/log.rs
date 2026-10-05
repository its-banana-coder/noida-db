//! The partition log's lifecycle: topic configs (and their validation),
//! segments, time/size retention, `DeleteRecords`, and log compaction.
//!
//! Segments here are bookkeeping only (a base offset and a creation time):
//! the batches stay in `PartitionState::record_batches`, and a segment owns
//! the batches whose base offset falls in `[base, next segment's base)`.
//! They exist because Kafka's retention and compaction work segment by
//! segment — retention deletes whole segments, and compaction never touches
//! the active (last) one — and clients that tune `segment.ms`/`segment.bytes`
//! in tests rely on exactly that granularity.

use super::engine::PartitionState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A 64-bit integer with a lower bound.
    Long(i64),
    /// A 32-bit integer within `[min, max]`.
    Int(i64, i64),
    /// A double within `[min, max]`.
    Double(f64, f64),
    Bool,
    /// One of these strings.
    OneOf(&'static [&'static str]),
    /// A comma-separated list whose items are each one of these (any item if
    /// empty).
    ListOf(&'static [&'static str]),
    Str,
}

/// Kafka 3.8's topic-level configs and their defaults (as `DescribeConfigs`
/// reports them for a topic with no overrides).
const TOPIC_CONFIGS: &[(&str, &str, Kind)] = &[
    ("cleanup.policy", "delete", Kind::ListOf(&["compact", "delete"])),
    ("compression.gzip.level", "-1", Kind::Int(-1, 9)),
    ("compression.lz4.level", "9", Kind::Int(1, 17)),
    (
        "compression.type",
        "producer",
        Kind::OneOf(&["uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"]),
    ),
    ("compression.zstd.level", "3", Kind::Int(-131072, 22)),
    ("delete.retention.ms", "86400000", Kind::Long(0)),
    ("file.delete.delay.ms", "60000", Kind::Long(0)),
    ("flush.messages", "9223372036854775807", Kind::Long(1)),
    ("flush.ms", "9223372036854775807", Kind::Long(0)),
    ("follower.replication.throttled.replicas", "", Kind::ListOf(&[])),
    ("index.interval.bytes", "4096", Kind::Int(0, i32::MAX as i64)),
    ("leader.replication.throttled.replicas", "", Kind::ListOf(&[])),
    ("local.retention.bytes", "-2", Kind::Long(-2)),
    ("local.retention.ms", "-2", Kind::Long(-2)),
    ("max.compaction.lag.ms", "9223372036854775807", Kind::Long(1)),
    ("max.message.bytes", "1048588", Kind::Int(0, i32::MAX as i64)),
    ("message.downconversion.enable", "true", Kind::Bool),
    ("message.format.version", "3.0-IV1", Kind::Str),
    ("message.timestamp.after.max.ms", "9223372036854775807", Kind::Long(0)),
    ("message.timestamp.before.max.ms", "9223372036854775807", Kind::Long(0)),
    ("message.timestamp.difference.max.ms", "9223372036854775807", Kind::Long(0)),
    ("message.timestamp.type", "CreateTime", Kind::OneOf(&["CreateTime", "LogAppendTime"])),
    ("min.cleanable.dirty.ratio", "0.5", Kind::Double(0.0, 1.0)),
    ("min.compaction.lag.ms", "0", Kind::Long(0)),
    ("min.insync.replicas", "1", Kind::Int(1, i32::MAX as i64)),
    ("preallocate", "false", Kind::Bool),
    ("remote.storage.enable", "false", Kind::Bool),
    ("retention.bytes", "-1", Kind::Long(i64::MIN)),
    ("retention.ms", "604800000", Kind::Long(-1)),
    // 14 bytes: Kafka's smallest possible (v0) record.
    ("segment.bytes", "1073741824", Kind::Int(14, i32::MAX as i64)),
    ("segment.index.bytes", "10485760", Kind::Int(4, i32::MAX as i64)),
    ("segment.jitter.ms", "0", Kind::Long(0)),
    ("segment.ms", "604800000", Kind::Long(1)),
    ("unclean.leader.election.enable", "false", Kind::Bool),
];

fn lookup(name: &str) -> Option<&'static (&'static str, &'static str, Kind)> {
    TOPIC_CONFIGS.iter().find(|(n, _, _)| *n == name)
}

/// Every topic config name with its default value, in Kafka's order.
pub fn topic_config_defaults() -> impl Iterator<Item = (&'static str, &'static str)> {
    TOPIC_CONFIGS.iter().map(|(n, d, _)| (*n, *d))
}

/// Whether `name` is a list-typed config (the only kind `APPEND`/`SUBTRACT`
/// work on).
pub fn is_list_config(name: &str) -> bool {
    matches!(lookup(name), Some((_, _, Kind::ListOf(_))))
}

/// Checks one topic config the way the broker does, with Kafka's message
/// for the `INVALID_CONFIG` error.
pub fn validate_topic_config(name: &str, value: &str) -> Result<(), String> {
    let Some(&(_, _, kind)) = lookup(name) else {
        return Err(format!("Unknown topic config name: {name}"));
    };
    let bad = |why: String| Err(format!("Invalid value {value} for configuration {name}: {why}"));
    let v = value.trim();
    match kind {
        Kind::Long(min) => match v.parse::<i64>() {
            Err(_) => bad("Not a number of type LONG".into()),
            Ok(n) if n < min => bad(format!("Value must be at least {min}")),
            Ok(_) => Ok(()),
        },
        Kind::Int(min, max) => match v.parse::<i32>() {
            Err(_) => bad("Not a number of type INT".into()),
            Ok(n) if (n as i64) < min => bad(format!("Value must be at least {min}")),
            Ok(n) if (n as i64) > max => bad(format!("Value must be no more than {max}")),
            Ok(_) => Ok(()),
        },
        Kind::Double(min, max) => match v.parse::<f64>() {
            Err(_) => bad("Not a number of type DOUBLE".into()),
            Ok(n) if n < min => bad(format!("Value must be at least {min:?}")),
            Ok(n) if n > max => bad(format!("Value must be no more than {max:?}")),
            Ok(_) => Ok(()),
        },
        Kind::Bool => {
            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
                Ok(())
            } else {
                bad("Expected value to be either true or false".into())
            }
        }
        Kind::OneOf(allowed) => {
            if allowed.contains(&v) {
                Ok(())
            } else {
                bad(format!("String must be one of: {}", allowed.join(", ")))
            }
        }
        Kind::ListOf(allowed) => {
            if allowed.is_empty() {
                return Ok(());
            }
            for item in split_list(v) {
                if !allowed.contains(&item) {
                    return bad(format!("String must be one of: {}", allowed.join(", ")));
                }
            }
            Ok(())
        }
        Kind::Str => Ok(()),
    }
}

/// A list config's items, as Kafka splits them (comma-separated, trimmed,
/// empty items dropped).
pub fn split_list(v: &str) -> impl Iterator<Item = &str> {
    v.split(',').map(str::trim).filter(|s| !s.is_empty())
}

/// Kafka's topic-name rules (`org.apache.kafka.common.internals.Topic`).
pub fn validate_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Topic name is illegal, it can't be empty".into());
    }
    if name == "." || name == ".." {
        return Err("Topic name cannot be \".\" or \"..\"".into());
    }
    if name.len() > 249 {
        return Err(format!(
            "Topic name is illegal, it can't be longer than 249 characters, topic name: {name}"
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-') {
        return Err(format!(
            "Topic name \"{name}\" is illegal, it contains a character other than ASCII \
             alphanumerics, '.', '_' and '-'"
        ));
    }
    Ok(())
}

/// The configs the log itself acts on, resolved against the defaults.
#[derive(Clone, Debug)]
pub struct LogConfig {
    pub delete: bool,
    pub compact: bool,
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub segment_bytes: i64,
    pub segment_ms: i64,
    pub delete_retention_ms: i64,
    pub min_compaction_lag_ms: i64,
    pub min_cleanable_dirty_ratio: f64,
    pub log_append_time: bool,
    pub max_message_bytes: i64,
}

impl LogConfig {
    pub fn of(configs: &HashMap<String, String>) -> Self {
        let get = |name: &str| -> String {
            configs
                .get(name)
                .cloned()
                .or_else(|| lookup(name).map(|(_, d, _)| d.to_string()))
                .unwrap_or_default()
        };
        let long = |name: &str| -> i64 {
            get(name)
                .trim()
                .parse()
                .unwrap_or_else(|_| lookup(name).and_then(|(_, d, _)| d.parse().ok()).unwrap_or(0))
        };
        let policy = get("cleanup.policy");
        LogConfig {
            delete: split_list(&policy).any(|p| p == "delete"),
            compact: split_list(&policy).any(|p| p == "compact"),
            retention_ms: long("retention.ms"),
            retention_bytes: long("retention.bytes"),
            segment_bytes: long("segment.bytes"),
            segment_ms: long("segment.ms"),
            delete_retention_ms: long("delete.retention.ms"),
            min_compaction_lag_ms: long("min.compaction.lag.ms"),
            min_cleanable_dirty_ratio: get("min.cleanable.dirty.ratio")
                .trim()
                .parse()
                .unwrap_or(0.5),
            log_append_time: get("message.timestamp.type") == "LogAppendTime",
            max_message_bytes: long("max.message.bytes"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Segment {
    pub base_offset: i64,
    pub created_ms: i64,
}

/// A v2 batch's last offset (`baseOffset + lastOffsetDelta`).
pub fn batch_last_offset(base: i64, bytes: &[u8]) -> i64 {
    if bytes.len() >= 27 && bytes[16] == 2 {
        base + i32::from_be_bytes(bytes[23..27].try_into().unwrap()) as i64
    } else {
        base
    }
}

/// A v2 batch's `maxTimestamp`.
fn batch_max_timestamp(bytes: &[u8]) -> i64 {
    if bytes.len() >= 43 && bytes[16] == 2 {
        i64::from_be_bytes(bytes[35..43].try_into().unwrap())
    } else {
        -1
    }
}

/// A v2 batch's attributes word.
fn batch_attributes(bytes: &[u8]) -> i16 {
    if bytes.len() >= 23 && bytes[16] == 2 {
        i16::from_be_bytes(bytes[21..23].try_into().unwrap())
    } else {
        0
    }
}

/// Turns on a v2 batch's LogAppendTime attribute (bit 3) and re-seals its
/// CRC — kafka-protocol's encoder always writes CreateTime.
pub fn set_log_append_time_flag(bytes: &mut [u8]) {
    if bytes.len() < 61 || bytes[16] != 2 {
        return;
    }
    let attrs = batch_attributes(bytes) | 0x08;
    bytes[21..23].copy_from_slice(&attrs.to_be_bytes());
    let crc = crc32c::crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
}

impl PartitionState {
    fn ensure_segment(&mut self, now: i64) {
        if self.segments.is_empty() {
            let base = self.record_batches.first().map_or(self.log_start_offset, |(b, _)| *b);
            self.segments
                .push(Segment { base_offset: base.min(self.log_start_offset), created_ms: now });
        }
    }

    /// Appends one batch of `count` records at the high watermark, rolling a
    /// new active segment first when `cfg`'s size or age limit says so.
    /// Returns the batch's base offset.
    pub fn append_batch(
        &mut self,
        mut bytes: Vec<u8>,
        count: i64,
        now: i64,
        cfg: Option<&LogConfig>,
    ) -> i64 {
        self.ensure_segment(now);
        if let Some(cfg) = cfg {
            let active = self.segments.last().unwrap();
            let active_bytes: i64 = self
                .record_batches
                .iter()
                .rev()
                .take_while(|(b, _)| *b >= active.base_offset)
                .map(|(_, b)| b.len() as i64)
                .sum();
            if active_bytes > 0
                && (active_bytes + bytes.len() as i64 > cfg.segment_bytes
                    || now - active.created_ms >= cfg.segment_ms)
            {
                self.segments.push(Segment { base_offset: self.high_watermark, created_ms: now });
            }
        }
        let base = self.high_watermark;
        if bytes.len() >= 8 && bytes.get(16) == Some(&2) {
            bytes[0..8].copy_from_slice(&base.to_be_bytes());
        }
        self.record_batches.push((base, bytes));
        self.high_watermark += count;
        base
    }

    /// The bytes this partition's log holds.
    pub fn size_bytes(&self) -> i64 {
        self.record_batches.iter().map(|(_, b)| b.len() as i64).sum()
    }

    /// Moves the log start forward to `new_start`, dropping every batch
    /// that ends before it and every segment that no longer holds anything
    /// at or past it. A batch straddling `new_start` stays (as in Kafka,
    /// fetches return it whole and clients skip the records before their
    /// fetch offset).
    pub fn truncate_front(&mut self, new_start: i64, now: i64) {
        if new_start <= self.log_start_offset {
            return;
        }
        self.ensure_segment(now);
        self.log_start_offset = new_start;
        self.record_batches.retain(|(b, bytes)| batch_last_offset(*b, bytes) >= new_start);
        if self.record_batches.is_empty() {
            self.segments = vec![Segment { base_offset: new_start, created_ms: now }];
        } else {
            let keep = self.segments.iter().rposition(|s| s.base_offset <= new_start).unwrap_or(0);
            self.segments.drain(..keep);
        }
        self.aborted_txns.retain(|&(_, first)| first >= new_start);
        self.tombstone_horizons.retain(|&off, _| off >= new_start);
        self.clean_offset = self.clean_offset.max(new_start);
    }

    /// `[lo, hi)` offsets and byte size of each segment (the active one
    /// ends at the high watermark).
    fn segment_spans(&self) -> Vec<(i64, i64, i64, i64)> {
        let n = self.segments.len();
        (0..n)
            .map(|i| {
                let lo = self.segments[i].base_offset;
                let hi =
                    if i + 1 < n { self.segments[i + 1].base_offset } else { self.high_watermark };
                let in_seg =
                    self.record_batches.iter().filter(|(b, _)| *b >= lo && (i + 1 == n || *b < hi));
                let size = in_seg.clone().map(|(_, b)| b.len() as i64).sum();
                // A segment with no record timestamps ages from its creation
                // (Kafka falls back to the segment file's mtime).
                let max_ts = in_seg
                    .map(|(_, b)| batch_max_timestamp(b))
                    .max()
                    .filter(|&ts| ts >= 0)
                    .unwrap_or(self.segments[i].created_ms);
                (lo, hi, size, max_ts)
            })
            .collect()
    }

    /// `cleanup.policy=delete`: deletes the oldest segments while they're
    /// past `retention.ms` (by their newest record's timestamp) or while the
    /// log is over `retention.bytes` without them. The active segment goes
    /// too when it qualifies, as in Kafka (which then rolls a fresh one).
    pub fn apply_retention(&mut self, cfg: &LogConfig, now: i64) -> bool {
        if !cfg.delete || (cfg.retention_ms < 0 && cfg.retention_bytes < 0) {
            return false;
        }
        self.ensure_segment(now);
        let spans = self.segment_spans();
        let mut over = self.size_bytes() - cfg.retention_bytes;
        let mut upto = None;
        for (i, &(_, hi, size, max_ts)) in spans.iter().enumerate() {
            let last = i + 1 == spans.len();
            if size == 0 && last {
                break;
            }
            let by_time = cfg.retention_ms >= 0 && (size == 0 || now - max_ts > cfg.retention_ms);
            let by_size = cfg.retention_bytes >= 0 && over - size >= 0;
            if !(by_time || by_size) {
                break;
            }
            over -= size;
            upto = Some(hi);
        }
        match upto {
            Some(hi) if hi > self.log_start_offset => {
                self.truncate_front(hi, now);
                true
            }
            _ => false,
        }
    }

    /// `cleanup.policy=compact`: keeps only the newest record per key in the
    /// closed segments, Kafka's way — the active segment, open
    /// transactions, and segments younger than `min.compaction.lag.ms` are
    /// never cleaned; a tombstone survives its first clean and is dropped
    /// by the first clean after `delete.retention.ms`; offsets never change.
    /// A clean starts once the uncleaned share of the closed segments
    /// reaches `min.cleanable.dirty.ratio`.
    /// Transactional and control batches are left as they are.
    pub fn compact(&mut self, cfg: &LogConfig, now: i64) -> bool {
        use kafka_protocol::records::{
            Compression, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        };
        if !cfg.compact {
            return false;
        }
        self.ensure_segment(now);
        let spans = self.segment_spans();
        let mut uncleanable = self.segments.last().unwrap().base_offset;
        if let Some(&open) = self.active_txns.values().min() {
            uncleanable = uncleanable.min(open);
        }
        if cfg.min_compaction_lag_ms > 0
            && let Some(&(lo, ..)) = spans
                .iter()
                .find(|&&(_, _, size, max_ts)| size > 0 && max_ts > now - cfg.min_compaction_lag_ms)
        {
            uncleanable = uncleanable.min(lo);
        }
        let first_dirty = self.clean_offset.max(self.log_start_offset);
        let (mut clean, mut dirty) = (0i64, 0i64);
        for (b, bytes) in &self.record_batches {
            if *b >= uncleanable {
                break;
            }
            if *b < first_dirty {
                clean += bytes.len() as i64;
            } else {
                dirty += bytes.len() as i64;
            }
        }
        let ratio_due =
            dirty > 0 && dirty as f64 / (clean + dirty) as f64 >= cfg.min_cleanable_dirty_ratio;
        // An expired tombstone alone doesn't start a clean (nor does it in
        // Kafka, as observed against 3.8): it goes on the next clean that
        // new dirty data triggers.
        if !ratio_due {
            return false;
        }

        let decode = |bytes: &[u8]| {
            let mut buf = bytes::Bytes::copy_from_slice(bytes);
            RecordBatchDecoder::decode(&mut buf).ok()
        };
        let plain = |bytes: &[u8]| batch_attributes(bytes) & 0x30 == 0;

        let mut latest: HashMap<bytes::Bytes, i64> = HashMap::new();
        for (b, bytes) in &self.record_batches {
            if *b >= uncleanable {
                break;
            }
            if *b < first_dirty || !plain(bytes) {
                continue;
            }
            for r in decode(bytes).map(|s| s.records).unwrap_or_default() {
                if let (Some(k), true) = (r.key, r.offset < uncleanable) {
                    latest.insert(k, r.offset);
                }
            }
        }

        let mut changed = false;
        let batches = std::mem::take(&mut self.record_batches);
        for (b, bytes) in batches {
            if b >= uncleanable || !plain(&bytes) {
                self.record_batches.push((b, bytes));
                continue;
            }
            let Some(set) = decode(&bytes) else {
                self.record_batches.push((b, bytes));
                continue;
            };
            let total = set.records.len();
            let mut kept = Vec::with_capacity(total);
            for r in set.records {
                if let Some(k) = &r.key {
                    if latest.get(k).is_some_and(|&l| l > r.offset) {
                        self.tombstone_horizons.remove(&r.offset);
                        continue;
                    }
                    if r.value.is_none() {
                        match self.tombstone_horizons.get(&r.offset) {
                            Some(&h) if h <= now => {
                                self.tombstone_horizons.remove(&r.offset);
                                continue;
                            }
                            Some(_) => {}
                            None => {
                                self.tombstone_horizons
                                    .insert(r.offset, now.saturating_add(cfg.delete_retention_ms));
                            }
                        }
                    }
                }
                kept.push(r);
            }
            if kept.len() == total {
                self.record_batches.push((b, bytes));
                continue;
            }
            changed = true;
            if kept.is_empty() {
                continue;
            }
            // Non-idempotent batches carry base sequence -1; keep it that
            // way after re-encoding (the encoder derives it from the first
            // record's sequence).
            if kept[0].producer_id < 0 {
                let first = kept[0].offset;
                for r in &mut kept {
                    r.sequence = -1i32.wrapping_add((r.offset - first) as i32);
                }
            }
            let log_append = batch_attributes(&bytes) & 0x08 != 0;
            let mut buf = bytes::BytesMut::new();
            let options = RecordEncodeOptions { version: 2, compression: Compression::None };
            if RecordBatchEncoder::encode(&mut buf, kept.iter(), &options).is_ok() {
                let mut out = buf.to_vec();
                if log_append {
                    set_log_append_time_flag(&mut out);
                }
                self.record_batches.push((kept[0].offset, out));
            }
        }
        // A cleaned segment can end up empty; drop it (never the active one).
        let keep_seg: Vec<bool> = (0..self.segments.len())
            .map(|i| match self.segments.get(i + 1) {
                None => true,
                Some(next) => {
                    let (lo, hi) = (self.segments[i].base_offset, next.base_offset);
                    self.record_batches.iter().any(|(b, _)| *b >= lo && *b < hi)
                }
            })
            .collect();
        let mut it = keep_seg.into_iter();
        self.segments.retain(|_| it.next().unwrap());
        self.clean_offset = self.clean_offset.max(uncleanable);
        changed
    }
}
