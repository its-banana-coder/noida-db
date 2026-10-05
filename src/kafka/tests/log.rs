//! Retention, DeleteRecords, compaction and topic-config validation. The
//! expected behavior in each test was observed against a real Kafka 3.8
//! broker (tests/failure-diff/kafka_edges.py runs the same scenarios
//! against both).

use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use super::{START_MS, T};

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

fn create(t: &T, topic: &str, configs: &[(&str, &str)]) -> (i16, Option<String>) {
    let mut req = CreateTopicsRequest::default();
    let mut ct = CreatableTopic::default();
    ct.name = TopicName(s(topic));
    ct.num_partitions = 1;
    ct.replication_factor = 1;
    for (k, v) in configs {
        let mut c = CreatableTopicConfig::default();
        c.name = s(k);
        c.value = Some(s(v));
        ct.configs.push(c);
    }
    req.topics.push(ct);
    let res = t.engine.handle_create_topics(&req, 5);
    let r = &res.topics[0];
    (r.error_code, r.error_message.as_ref().map(|m| m.to_string()))
}

fn batch(records: &[(Option<&str>, Option<&str>)], ts: i64) -> Vec<u8> {
    let records: Vec<Record> = records
        .iter()
        .enumerate()
        .map(|(i, (k, v))| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            // offset - sequence must be constant within a batch, or the
            // encoder splits it; this keeps the base sequence at -1.
            sequence: i as i32 - 1,
            timestamp: ts,
            key: k.map(|k| bytes::Bytes::from(k.to_string())),
            value: v.map(|v| bytes::Bytes::from(v.to_string())),
            headers: Default::default(),
        })
        .collect();
    let mut buf = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &options).unwrap();
    buf.to_vec()
}

/// Produces one batch; returns (error_code, base_offset).
fn produce_batch(t: &T, topic: &str, records: &[(Option<&str>, Option<&str>)]) -> (i16, i64) {
    let ts = t.engine.now_ms();
    let mut req = ProduceRequest::default();
    req.acks = -1;
    let mut td = TopicProduceData::default();
    td.name = TopicName(s(topic));
    let mut pd = PartitionProduceData::default();
    pd.index = 0;
    pd.records = Some(bytes::Bytes::from(batch(records, ts)));
    td.partition_data.push(pd);
    req.topic_data.push(td);
    let res = t.engine.handle_produce(&req, 8);
    let p = &res.responses[0].partition_responses[0];
    (p.error_code, p.base_offset)
}

fn produce(t: &T, topic: &str, key: Option<&str>, value: Option<&str>) -> i64 {
    let (code, off) = produce_batch(t, topic, &[(key, value)]);
    assert_eq!(code, 0);
    off
}

/// (error_code, [(offset, key, value)]) from `offset` to the end.
#[allow(clippy::type_complexity)]
fn fetch(t: &T, topic: &str, offset: i64) -> (i16, Vec<(i64, Option<String>, Option<String>)>) {
    let mut req = FetchRequest::default();
    let mut ft = FetchTopic::default();
    ft.topic = TopicName(s(topic));
    let mut fp = FetchPartition::default();
    fp.partition = 0;
    fp.fetch_offset = offset;
    ft.partitions.push(fp);
    req.topics.push(ft);
    let res = t.engine.handle_fetch(&req, 11);
    let p = &res.responses[0].partitions[0];
    let mut out = Vec::new();
    if let Some(bytes) = &p.records {
        let mut buf = bytes.clone();
        for set in RecordBatchDecoder::decode_all(&mut buf).unwrap() {
            for r in set.records {
                if r.offset >= offset {
                    let st =
                        |b: Option<bytes::Bytes>| b.map(|b| String::from_utf8(b.to_vec()).unwrap());
                    out.push((r.offset, st(r.key), st(r.value)));
                }
            }
        }
    }
    (p.error_code, out)
}

fn earliest(t: &T, topic: &str) -> i64 {
    let mut req = ListOffsetsRequest::default();
    let mut lt = ListOffsetsTopic::default();
    lt.name = TopicName(s(topic));
    let mut lp = ListOffsetsPartition::default();
    lp.partition_index = 0;
    lp.timestamp = -2;
    lt.partitions.push(lp);
    req.topics.push(lt);
    t.engine.handle_list_offsets(&req, 5).topics[0].partitions[0].offset
}

fn delete_records(t: &T, topic: &str, offset: i64) -> (i16, i64) {
    let mut req = DeleteRecordsRequest::default();
    let mut dt = DeleteRecordsTopic::default();
    dt.name = TopicName(s(topic));
    let mut dp = DeleteRecordsPartition::default();
    dp.partition_index = 0;
    dp.offset = offset;
    dt.partitions.push(dp);
    req.topics.push(dt);
    let res = t.engine.handle_delete_records(&req, 2);
    let p = &res.topics[0].partitions[0];
    (p.error_code, p.low_watermark)
}

fn offsets(records: &[(i64, Option<String>, Option<String>)]) -> Vec<i64> {
    records.iter().map(|r| r.0).collect()
}

#[test]
fn delete_records_moves_the_log_start() {
    let t = T::new();
    create(&t, "dr", &[]);
    for i in 0..10 {
        produce(&t, "dr", Some("k"), Some(&i.to_string()));
    }
    assert_eq!(delete_records(&t, "dr", 3), (0, 3));
    assert_eq!(earliest(&t, "dr"), 3);
    // Below the start: nothing changes, the current start is reported.
    assert_eq!(delete_records(&t, "dr", 1), (0, 3));
    // Past the end.
    assert_eq!(delete_records(&t, "dr", 11).0, 1); // OFFSET_OUT_OF_RANGE
    // Fetching below the start is out of range, at the start works.
    assert_eq!(fetch(&t, "dr", 2).0, 1);
    assert_eq!(offsets(&fetch(&t, "dr", 3).1), vec![3, 4, 5, 6, 7, 8, 9]);
    // -1 means the high watermark.
    assert_eq!(delete_records(&t, "dr", -1), (0, 10));
    assert_eq!(earliest(&t, "dr"), 10);
    assert_eq!(produce(&t, "dr", Some("k"), Some("after")), 10);
    assert_eq!(offsets(&fetch(&t, "dr", 10).1), vec![10]);
    assert_eq!(delete_records(&t, "missing", 0).0, 3);
}

#[test]
fn delete_records_mid_batch_keeps_the_batch() {
    let t = T::new();
    create(&t, "drb", &[]);
    let batch: Vec<_> = ["0", "1", "2", "3", "4", "5"].iter().map(|v| (None, Some(*v))).collect();
    produce_batch(&t, "drb", &batch);
    assert_eq!(delete_records(&t, "drb", 4), (0, 4));
    assert_eq!(offsets(&fetch(&t, "drb", 4).1), vec![4, 5]);
}

#[test]
fn delete_records_refuses_compact_only_topics() {
    let t = T::new();
    create(&t, "c", &[("cleanup.policy", "compact")]);
    create(&t, "cd", &[("cleanup.policy", "compact,delete")]);
    produce(&t, "c", Some("k"), Some("v"));
    produce(&t, "cd", Some("k"), Some("v"));
    assert_eq!(delete_records(&t, "c", 1).0, 44); // POLICY_VIOLATION
    assert_eq!(delete_records(&t, "cd", 1), (0, 1));
}

#[test]
fn retention_ms_deletes_expired_segments_including_the_active_one() {
    let t = T::new();
    create(&t, "r", &[("retention.ms", "1000")]);
    for i in 0..5 {
        produce(&t, "r", None, Some(&i.to_string()));
    }
    t.engine.run_log_cleaner();
    assert_eq!(earliest(&t, "r"), 0);
    t.advance(1500);
    t.engine.run_log_cleaner();
    assert_eq!(earliest(&t, "r"), 5);
    assert_eq!(produce(&t, "r", None, Some("new")), 5);
    assert_eq!(offsets(&fetch(&t, "r", 5).1), vec![5]);
}

#[test]
fn retention_bytes_deletes_whole_closed_segments() {
    let t = T::new();
    create(&t, "rb", &[("retention.bytes", "300"), ("segment.bytes", "200")]);
    for _ in 0..12 {
        produce(&t, "rb", None, Some(&"x".repeat(60)));
    }
    t.engine.run_log_cleaner();
    let start = earliest(&t, "rb");
    assert!(start > 0 && start < 12, "start {start}");
    // Retention never takes the log below retention.bytes.
    let (_, rest) = fetch(&t, "rb", start);
    assert_eq!(rest.len() as i64, 12 - start);
}

#[test]
fn infinite_retention_keeps_everything() {
    let t = T::new();
    create(&t, "inf", &[("retention.ms", "-1")]);
    produce(&t, "inf", None, Some("x"));
    t.advance(10 * 365 * 24 * 3600 * 1000);
    t.engine.run_log_cleaner();
    assert_eq!(earliest(&t, "inf"), 0);
}

#[test]
fn compaction_keeps_latest_per_key_and_expires_tombstones() {
    let t = T::new();
    create(
        &t,
        "cmp",
        &[
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("delete.retention.ms", "100"),
        ],
    );
    for (k, v) in [
        ("a", Some("1")),
        ("b", Some("1")),
        ("a", Some("2")),
        ("c", Some("1")),
        ("b", None),
        ("a", Some("3")),
        ("d", Some("1")),
        ("c", Some("2")),
    ] {
        produce(&t, "cmp", Some(k), v);
    }
    // The active segment is never cleaned.
    t.engine.run_log_cleaner();
    assert_eq!(fetch(&t, "cmp", 0).1.len(), 8);

    t.advance(300);
    produce(&t, "cmp", Some("e"), Some("1")); // rolls the segment
    t.engine.run_log_cleaner();
    // The tombstone survives the first clean.
    assert_eq!(offsets(&fetch(&t, "cmp", 0).1), vec![4, 5, 6, 7, 8]);
    assert_eq!(fetch(&t, "cmp", 0).1[0], (4, Some("b".into()), None));
    // Offsets don't move and the log start doesn't either.
    assert_eq!(earliest(&t, "cmp"), 0);
    // Fetching a compacted-away offset continues at the next record.
    assert_eq!(offsets(&fetch(&t, "cmp", 2).1), vec![4, 5, 6, 7, 8]);

    // Past delete.retention.ms an expired tombstone alone doesn't start a
    // clean; the next clean (new dirty data) drops it.
    t.advance(300);
    t.engine.run_log_cleaner();
    assert_eq!(offsets(&fetch(&t, "cmp", 0).1), vec![4, 5, 6, 7, 8]);
    produce(&t, "cmp", Some("f"), Some("1"));
    t.engine.run_log_cleaner();
    assert_eq!(offsets(&fetch(&t, "cmp", 0).1), vec![5, 6, 7, 8, 9]);
}

#[test]
fn min_compaction_lag_holds_back_young_segments() {
    let t = T::new();
    create(
        &t,
        "lag",
        &[
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "3600000"),
        ],
    );
    produce(&t, "lag", Some("a"), Some("1"));
    produce(&t, "lag", Some("a"), Some("2"));
    t.advance(300);
    produce(&t, "lag", Some("b"), Some("1"));
    t.engine.run_log_cleaner();
    assert_eq!(offsets(&fetch(&t, "lag", 0).1), vec![0, 1, 2]);
    t.advance(3_600_000);
    produce(&t, "lag", Some("c"), Some("1"));
    t.engine.run_log_cleaner();
    assert_eq!(offsets(&fetch(&t, "lag", 0).1), vec![1, 2, 3]);
}

#[test]
fn compacted_topics_reject_keyless_records() {
    let t = T::new();
    create(&t, "nk", &[("cleanup.policy", "compact")]);
    assert_eq!(produce_batch(&t, "nk", &[(None, Some("v"))]).0, 87); // INVALID_RECORD
    assert_eq!(produce_batch(&t, "nk", &[(Some("k"), Some("v"))]), (0, 0));
    // A tombstone has a key.
    assert_eq!(produce_batch(&t, "nk", &[(Some("k"), None)]), (0, 1));
}

#[test]
fn max_message_bytes_is_enforced() {
    let t = T::new();
    create(&t, "mm", &[("max.message.bytes", "200")]);
    assert_eq!(produce_batch(&t, "mm", &[(None, Some(&"x".repeat(50)))]).0, 0);
    assert_eq!(produce_batch(&t, "mm", &[(None, Some(&"y".repeat(500)))]).0, 10);
}

#[test]
fn topic_configs_and_names_are_validated() {
    let t = T::new();
    for cfg in [
        ("no.such.config", "1"),
        ("retention.ms", "abc"),
        ("retention.ms", "-5"),
        ("cleanup.policy", "bogus"),
        ("min.cleanable.dirty.ratio", "2"),
        ("segment.bytes", "10"),
        ("segment.ms", "0"),
        ("message.timestamp.type", "Nope"),
        ("compression.type", "brotli"),
        ("preallocate", "yes"),
        ("min.insync.replicas", "0"),
    ] {
        assert_eq!(create(&t, "v", &[cfg]).0, 40, "{cfg:?}"); // INVALID_CONFIG
    }
    let (_, msg) = create(&t, "v", &[("retention.ms", "abc")]);
    assert_eq!(
        msg.as_deref(),
        Some("Invalid value abc for configuration retention.ms: Not a number of type LONG")
    );
    for cfg in [
        ("retention.ms", "-1"),
        ("cleanup.policy", "delete,compact"),
        ("cleanup.policy", ""),
        ("retention.bytes", "-2"),
        ("message.timestamp.type", "LogAppendTime"),
    ] {
        assert_eq!(create(&t, &format!("ok-{}", cfg.0), &[cfg]).0, 0, "{cfg:?}");
        t.engine.handle_delete_topics(
            &{
                let mut r = kafka_protocol::messages::DeleteTopicsRequest::default();
                r.topic_names.push(TopicName(s(&format!("ok-{}", cfg.0))));
                r
            },
            4,
        );
    }
    for name in ["bad/name", "..", ".", &"a".repeat(250), "has space"] {
        assert_eq!(create(&t, name, &[]).0, 17, "{name}"); // INVALID_TOPIC_EXCEPTION
    }
    assert_eq!(create(&t, "ok.name_-1", &[]).0, 0);
}

#[test]
fn incremental_alter_appends_and_subtracts_list_configs() {
    let t = T::new();
    create(&t, "ia", &[]);
    let alter = |name: &str, value: Option<&str>, op: i8| {
        let mut req = IncrementalAlterConfigsRequest::default();
        let mut res = AlterConfigsResource::default();
        res.resource_type = 2;
        res.resource_name = s("ia");
        let mut c = AlterableConfig::default();
        c.name = s(name);
        c.value = value.map(s);
        c.config_operation = op;
        res.configs.push(c);
        req.resources.push(res);
        t.engine.handle_incremental_alter_configs(&req, 1).responses[0].error_code
    };
    let policy = || {
        let mut req = kafka_protocol::messages::DescribeConfigsRequest::default();
        let mut r =
            kafka_protocol::messages::describe_configs_request::DescribeConfigsResource::default();
        r.resource_type = 2;
        r.resource_name = s("ia");
        r.configuration_keys = Some(vec![s("cleanup.policy")]);
        req.resources.push(r);
        let res = t.engine.handle_describe_configs(&req, 1);
        res.results[0].configs[0].value.as_ref().unwrap().to_string()
    };
    assert_eq!(alter("cleanup.policy", Some("compact"), 2), 0); // APPEND
    assert_eq!(policy(), "delete,compact");
    assert_eq!(alter("cleanup.policy", Some("compact"), 2), 0);
    assert_eq!(policy(), "delete,compact");
    assert_eq!(alter("cleanup.policy", Some("delete"), 3), 0); // SUBTRACT
    assert_eq!(policy(), "compact");
    assert_eq!(alter("cleanup.policy", Some("nope"), 2), 40);
    assert_eq!(alter("retention.ms", Some("5"), 2), 40); // not a list
    assert_eq!(alter("retention.ms", Some("abc"), 0), 40);
    assert_eq!(alter("no.such", Some("1"), 0), 40);
    assert_eq!(alter("cleanup.policy", Some("compact"), 3), 0);
    assert_eq!(policy(), "");
    assert_eq!(alter("cleanup.policy", None, 1), 0); // DELETE: back to the default
    assert_eq!(policy(), "delete");
}

#[test]
fn list_offsets_by_time_skips_deleted_records() {
    let t = T::new();
    create(&t, "lo", &[]);
    for i in 0..5 {
        produce(&t, "lo", None, Some(&i.to_string()));
        t.advance(1000);
    }
    delete_records(&t, "lo", 3);
    let mut req = ListOffsetsRequest::default();
    let mut lt = ListOffsetsTopic::default();
    lt.name = TopicName(s("lo"));
    let mut lp = ListOffsetsPartition::default();
    lp.partition_index = 0;
    lp.timestamp = START_MS as i64;
    lt.partitions.push(lp);
    req.topics.push(lt);
    let res = t.engine.handle_list_offsets(&req, 5);
    assert_eq!(res.topics[0].partitions[0].offset, 3);
}
