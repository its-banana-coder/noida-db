//! In-process engine tests for Kafka.
//! Follows the Redis module pattern: one file per area, real error codes, clock injection, no networking.

mod admin;
mod coordinator;
mod idempotence;
mod log;
mod offsets;
mod produce_fetch;
mod topics;
mod transactions;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::engine::Engine;

pub const START_MS: u64 = 1_700_000_000_000;

/// An engine with a controllable clock.
pub struct T {
    pub engine: Engine,
    now: Arc<AtomicU64>,
}

impl T {
    pub fn new() -> Self {
        let now = Arc::new(AtomicU64::new(START_MS));
        let clock = now.clone();
        let engine = Engine::with_clock(
            "127.0.0.1".to_string(),
            9092,
            Arc::new(move || clock.load(Ordering::SeqCst)),
        );
        Self { engine, now }
    }

    pub fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}

/// A real v2 record batch holding `values` (kafka-protocol's own encoder,
/// so the CRC and lengths are right — the engine rejects anything else, as
/// a broker does). `producer_id` -1 means a plain, non-idempotent producer.
pub fn record_batch(
    values: &[&[u8]],
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Vec<u8> {
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: base_sequence + i as i32,
            timestamp: START_MS as i64,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(v)),
            headers: Default::default(),
        })
        .collect();
    let mut buf = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &options).unwrap();
    buf.to_vec()
}

/// The record values in a fetch response's `records`.
pub fn record_values(records: Option<&bytes::Bytes>) -> Vec<Vec<u8>> {
    let Some(bytes) = records else { return Vec::new() };
    let mut buf = bytes.clone();
    kafka_protocol::records::RecordBatchDecoder::decode_all(&mut buf)
        .unwrap()
        .into_iter()
        .flat_map(|set| set.records)
        .filter_map(|r| r.value.map(|v| v.to_vec()))
        .collect()
}
