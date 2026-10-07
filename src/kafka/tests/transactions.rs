use kafka_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::describe_transactions_request::DescribeTransactionsRequest;
use kafka_protocol::messages::end_txn_request::EndTxnRequest;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::list_transactions_request::ListTransactionsRequest;
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{GroupId, InitProducerIdRequest, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use super::T;

/// A real v2 record batch (via the same `RecordBatchEncoder` the engine
/// itself uses for control batches) carrying one transactional data
/// record — real byte-for-byte encoding, not hand-rolled offsets, so it
/// round-trips through a real decoder like `RecordBatchDecoder::decode_all`
/// the way a real producer's bytes would.
fn make_transactional_batch(producer_id: i64, producer_epoch: i16, base_sequence: i32) -> Vec<u8> {
    let record = Record {
        transactional: true,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id,
        producer_epoch,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: base_sequence,
        timestamp: 0,
        key: None,
        value: Some(bytes::Bytes::from_static(b"payload")),
        headers: Default::default(),
    };
    let mut buf = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, std::iter::once(&record), &options)
        .expect("encoding a single uncompressed transactional record cannot fail");
    buf.to_vec()
}

fn fetch(
    t: &T,
    topic: &str,
    offset: i64,
    isolation_level: i8,
) -> kafka_protocol::messages::fetch_response::PartitionData {
    let mut req = FetchRequest::default();
    req.isolation_level = isolation_level;
    let mut ft = FetchTopic::default();
    ft.topic = TopicName::from(StrBytes::from_string(topic.to_string()));
    let mut fp = FetchPartition::default();
    fp.partition = 0;
    fp.fetch_offset = offset;
    ft.partitions.push(fp);
    req.topics.push(ft);
    let resp = t.engine.handle_fetch(&req, 11);
    resp.responses[0].partitions[0].clone()
}

/// `PartitionData::default()`'s `records` field is `Some(Bytes::new())`,
/// not `None` — so "no records visible" can't be checked by
/// `Option::is_none()`, and can't be checked by raw byte length either
/// now that a fetch response may legitimately carry a non-empty control
/// batch (e.g. an EndTxn marker) with zero actual application records —
/// a real client's own decoder sees exactly this and correctly reports
/// no records to the application, filtering control records out first.
fn has_records(part: &kafka_protocol::messages::fetch_response::PartitionData) -> bool {
    let Some(bytes) = &part.records else { return false };
    if bytes.is_empty() {
        return false;
    }
    let mut buf = bytes.clone();
    let Ok(sets) = kafka_protocol::records::RecordBatchDecoder::decode_all(&mut buf) else {
        return false;
    };
    sets.iter().any(|s| s.records.iter().any(|r| !r.control))
}

#[test]
fn test_transaction_lifecycle() {
    let t = T::new();
    t.create_topic("orders", 2);
    let mut init = InitProducerIdRequest::default();
    init.transactional_id = Some(TransactionalId(StrBytes::from_static_str("my-tx")));
    init.transaction_timeout_ms = 60_000;
    let init_resp = t.engine.handle_init_producer_id(&init, 1);
    assert_eq!(init_resp.error_code, 0);
    assert_eq!(describe_txn(&t, "my-tx"), (0, "Empty".to_string(), vec![]));

    // 1. AddPartitionsToTxn
    let mut add_parts_req = AddPartitionsToTxnRequest::default();
    add_parts_req.v3_and_below_transactional_id =
        TransactionalId(StrBytes::from_static_str("my-tx"));
    let mut txn_topic = AddPartitionsToTxnTopic::default();
    txn_topic.name = TopicName::from(StrBytes::from_static_str("orders"));
    txn_topic.partitions = vec![0, 1];
    add_parts_req.v3_and_below_topics = vec![txn_topic];

    let add_parts_resp = t.engine.handle_add_partitions_to_txn(&add_parts_req, 1);
    let results = &add_parts_resp.results_by_topic_v3_and_below[0].results_by_partition;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].partition_error_code, 0);
    assert_eq!(results[1].partition_error_code, 0);

    // 2. AddOffsetsToTxn
    let mut add_offs_req = AddOffsetsToTxnRequest::default();
    add_offs_req.transactional_id = TransactionalId(StrBytes::from_static_str("my-tx"));
    add_offs_req.group_id = GroupId(StrBytes::from_static_str("my-consumer-group"));

    let add_offs_resp = t.engine.handle_add_offsets_to_txn(&add_offs_req, 1);
    assert_eq!(add_offs_resp.error_code, 0);

    // 3. TxnOffsetCommit
    let mut txn_commit_req = TxnOffsetCommitRequest::default();
    txn_commit_req.transactional_id = TransactionalId(StrBytes::from_static_str("my-tx"));
    txn_commit_req.group_id = GroupId(StrBytes::from_static_str("my-consumer-group"));
    let mut tc_topic = TxnOffsetCommitRequestTopic::default();
    tc_topic.name = TopicName::from(StrBytes::from_static_str("orders"));
    let mut tc_part = TxnOffsetCommitRequestPartition::default();
    tc_part.partition_index = 0;
    tc_part.committed_offset = 99;
    tc_topic.partitions.push(tc_part);
    txn_commit_req.topics.push(tc_topic);

    let txn_commit_resp = t.engine.handle_txn_offset_commit(&txn_commit_req, 1);
    assert_eq!(txn_commit_resp.topics[0].partitions[0].error_code, 0);

    // Verify offset was recorded
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = GroupId(StrBytes::from_static_str("my-consumer-group"));
    let mut fetch_topic = OffsetFetchRequestTopic::default();
    fetch_topic.name = TopicName::from(StrBytes::from_static_str("orders"));
    fetch_topic.partition_indexes.push(0);
    fetch_req.topics = Some(vec![fetch_topic]);
    // Not visible until the transaction commits.
    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, -1);
    assert_eq!(
        describe_txn(&t, "my-tx"),
        (0, "Ongoing".to_string(), vec![("orders".to_string(), vec![0, 1])])
    );

    // 4. EndTxn (commit)
    let mut end_commit_req = EndTxnRequest::default();
    end_commit_req.transactional_id = TransactionalId(StrBytes::from_static_str("my-tx"));
    end_commit_req.producer_id = init_resp.producer_id;
    end_commit_req.producer_epoch = init_resp.producer_epoch;
    end_commit_req.committed = true;
    let end_commit_resp = t.engine.handle_end_txn(&end_commit_req, 1);
    assert_eq!(end_commit_resp.error_code, 0);
    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 99);
    assert_eq!(describe_txn(&t, "my-tx"), (0, "CompleteCommit".to_string(), vec![]));

    // 5. EndTxn (abort)
    let mut end_abort_req = EndTxnRequest::default();
    end_abort_req.transactional_id = TransactionalId(StrBytes::from_static_str("my-tx"));
    end_abort_req.committed = false;
    let end_abort_resp = t.engine.handle_end_txn(&end_abort_req, 1);
    assert_eq!(end_abort_resp.error_code, 0);

    // 6. DescribeTransactions & ListTransactions
    let mut desc_tx_req = DescribeTransactionsRequest::default();
    desc_tx_req.transactional_ids.push(TransactionalId(StrBytes::from_static_str("my-tx")));
    let desc_tx_resp = t.engine.handle_describe_transactions(&desc_tx_req, 0);
    assert_eq!(desc_tx_resp.transaction_states[0].error_code, 0);
    assert_eq!(desc_tx_resp.transaction_states[0].transactional_id.as_str(), "my-tx");
    assert_eq!(describe_txn(&t, "never-used").0, 105); // TRANSACTIONAL_ID_NOT_FOUND

    let list_tx_req = ListTransactionsRequest::default();
    let list_tx_resp = t.engine.handle_list_transactions(&list_tx_req, 0);
    assert_eq!(list_tx_resp.error_code, 0);
    let listed: Vec<_> = list_tx_resp
        .transaction_states
        .iter()
        .map(|s| (s.transactional_id.as_str().to_string(), s.transaction_state.to_string()))
        .collect();
    assert_eq!(listed, vec![("my-tx".to_string(), "CompleteAbort".to_string())]);
    let mut ongoing_only = ListTransactionsRequest::default();
    ongoing_only.state_filters.push(StrBytes::from_static_str("Ongoing"));
    assert!(t.engine.handle_list_transactions(&ongoing_only, 0).transaction_states.is_empty());
}

fn describe_txn(t: &T, tid: &str) -> (i16, String, Vec<(String, Vec<i32>)>) {
    let mut req = DescribeTransactionsRequest::default();
    req.transactional_ids.push(TransactionalId(StrBytes::from_string(tid.to_string())));
    let res = t.engine.handle_describe_transactions(&req, 0);
    let s = &res.transaction_states[0];
    let topics =
        s.topics.iter().map(|td| (td.topic.as_str().to_string(), td.partitions.clone())).collect();
    (s.error_code, s.transaction_state.to_string(), topics)
}

fn txn_offset_commit(t: &T, tid: &str, pid: i64, epoch: i16, group: &str, offset: i64) -> i16 {
    let mut req = TxnOffsetCommitRequest::default();
    req.transactional_id = TransactionalId(StrBytes::from_string(tid.to_string()));
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.producer_id = kafka_protocol::messages::ProducerId(pid);
    req.producer_epoch = epoch;
    let mut tp = TxnOffsetCommitRequestTopic::default();
    tp.name = TopicName::from(StrBytes::from_static_str("src"));
    let mut part = TxnOffsetCommitRequestPartition::default();
    part.partition_index = 0;
    part.committed_offset = offset;
    tp.partitions.push(part);
    req.topics.push(tp);
    t.engine.handle_txn_offset_commit(&req, 3).topics[0].partitions[0].error_code
}

fn committed(t: &T, group: &str) -> i64 {
    let mut req = OffsetFetchRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    let mut ft = OffsetFetchRequestTopic::default();
    ft.name = TopicName::from(StrBytes::from_static_str("src"));
    ft.partition_indexes.push(0);
    req.topics = Some(vec![ft]);
    t.engine.handle_offset_fetch(&req, 7).topics[0].partitions[0].committed_offset
}

/// send_offsets_to_transaction + abort must leave the group's offsets
/// alone (found: the aborted offsets were committed immediately).
#[test]
fn test_txn_offsets_discarded_on_abort() {
    let t = T::new();
    t.create_topic("src", 1);
    let mut init = InitProducerIdRequest::default();
    init.transactional_id = Some(TransactionalId(StrBytes::from_static_str("ab-tx")));
    init.transaction_timeout_ms = 60_000;
    let r = t.engine.handle_init_producer_id(&init, 1);
    let (pid, epoch) = (r.producer_id.0, r.producer_epoch);
    assert_eq!(txn_offset_commit(&t, "ab-tx", pid, epoch, "g", 5), 0);
    let mut end = EndTxnRequest::default();
    end.transactional_id = TransactionalId(StrBytes::from_static_str("ab-tx"));
    end.producer_id = kafka_protocol::messages::ProducerId(pid);
    end.producer_epoch = epoch;
    end.committed = false;
    assert_eq!(t.engine.handle_end_txn(&end, 3).error_code, 0);
    assert_eq!(committed(&t, "g"), -1);
    assert_eq!(describe_txn(&t, "ab-tx").1, "CompleteAbort");
}

/// A second InitProducerId for the same transactional.id aborts the first
/// instance's open transaction: its records never become visible to
/// read_committed and its offsets are dropped (found: they were committed).
#[test]
fn test_reinit_aborts_open_transaction() {
    let t = T::new();
    t.create_topic("src", 1);
    t.create_topic("out", 1);
    let mut init = InitProducerIdRequest::default();
    init.transactional_id = Some(TransactionalId(StrBytes::from_static_str("z-tx")));
    init.transaction_timeout_ms = 60_000;
    let r1 = t.engine.handle_init_producer_id(&init, 1);
    let (pid, e1) = (r1.producer_id.0, r1.producer_epoch);

    let mut add = AddPartitionsToTxnRequest::default();
    add.v3_and_below_transactional_id = TransactionalId(StrBytes::from_static_str("z-tx"));
    add.v3_and_below_producer_id = kafka_protocol::messages::ProducerId(pid);
    add.v3_and_below_producer_epoch = e1;
    let mut at = AddPartitionsToTxnTopic::default();
    at.name = TopicName::from(StrBytes::from_static_str("out"));
    at.partitions = vec![0];
    add.v3_and_below_topics = vec![at];
    t.engine.handle_add_partitions_to_txn(&add, 3);
    let mut prod = ProduceRequest::default();
    prod.transactional_id = Some(TransactionalId(StrBytes::from_static_str("z-tx")));
    let mut td = TopicProduceData::default();
    td.name = TopicName::from(StrBytes::from_static_str("out"));
    let mut pd = PartitionProduceData::default();
    pd.index = 0;
    pd.records = Some(bytes::Bytes::from(make_transactional_batch(pid, e1, 0)));
    td.partition_data.push(pd);
    prod.topic_data.push(td);
    assert_eq!(t.engine.handle_produce(&prod, 9).responses[0].partition_responses[0].error_code, 0);
    assert_eq!(txn_offset_commit(&t, "z-tx", pid, e1, "g", 7), 0);

    let r2 = t.engine.handle_init_producer_id(&init, 1);
    assert_eq!(r2.producer_id.0, pid);
    assert!(r2.producer_epoch > e1);
    assert_eq!(committed(&t, "g"), -1);
    assert_eq!(describe_txn(&t, "z-tx"), (0, "Empty".to_string(), vec![]));
    // The zombie can no longer commit offsets.
    assert_eq!(txn_offset_commit(&t, "z-tx", pid, e1, "g", 8), 47);

    // read_committed sees nothing: the record was aborted.
    let mut fetch = FetchRequest::default();
    fetch.isolation_level = 1;
    let mut ft = FetchTopic::default();
    ft.topic = TopicName::from(StrBytes::from_static_str("out"));
    let mut fp = FetchPartition::default();
    fp.partition = 0;
    fp.partition_max_bytes = 1 << 20;
    ft.partitions.push(fp);
    fetch.topics.push(ft);
    let res = t.engine.handle_fetch(&fetch, 11);
    let p = &res.responses[0].partitions[0];
    assert_eq!(p.last_stable_offset, p.high_watermark);
    let aborted = p.aborted_transactions.as_ref().map(|a| a.len()).unwrap_or(0);
    assert_eq!(aborted, 1, "the open transaction was aborted");
}

/// The whole point of this feature: a read_committed consumer must never
/// see an aborted transaction's records, and must see a committed
/// transaction's records once (and only once) EndTxn(commit) has run.
#[test]
fn read_committed_hides_aborted_records_and_reveals_committed_ones() {
    let t = T::new();

    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    // --- Producer A: writes one record, then ABORTS. ---
    let init_a = t.engine.handle_init_producer_id(
        &{
            let mut r = InitProducerIdRequest::default();
            r.transactional_id = Some(TransactionalId(StrBytes::from_static_str("tx-a")));
            r.transaction_timeout_ms = 60_000;
            r
        },
        4,
    );
    let pid_a = init_a.producer_id.0;

    let mut add_parts_a = AddPartitionsToTxnRequest::default();
    add_parts_a.v3_and_below_transactional_id = TransactionalId(StrBytes::from_static_str("tx-a"));
    add_parts_a.v3_and_below_producer_id = init_a.producer_id;
    add_parts_a.v3_and_below_producer_epoch = init_a.producer_epoch;
    let mut txn_topic_a = AddPartitionsToTxnTopic::default();
    txn_topic_a.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
    txn_topic_a.partitions = vec![0];
    add_parts_a.v3_and_below_topics = vec![txn_topic_a];
    let add_parts_a_resp = t.engine.handle_add_partitions_to_txn(&add_parts_a, 1);
    assert_eq!(
        add_parts_a_resp.results_by_topic_v3_and_below[0].results_by_partition[0]
            .partition_error_code,
        0
    );

    let mut produce_a = ProduceRequest::default();
    let mut topic_data_a = TopicProduceData::default();
    topic_data_a.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
    let mut part_data_a = PartitionProduceData::default();
    part_data_a.index = 0;
    part_data_a.records =
        Some(bytes::Bytes::from(make_transactional_batch(pid_a, init_a.producer_epoch, 0)));
    topic_data_a.partition_data.push(part_data_a);
    produce_a.topic_data.push(topic_data_a);
    let produce_a_resp = t.engine.handle_produce(&produce_a, 8);
    assert_eq!(produce_a_resp.responses[0].partition_responses[0].error_code, 0);

    // read_uncommitted sees it immediately (it's already in the log).
    let ru_before_abort = fetch(&t, "txn-topic", 0, 0);
    assert!(has_records(&ru_before_abort), "read_uncommitted sees uncommitted records");

    // read_committed does NOT see it yet — the LSO can't be past it while
    // the transaction is still open.
    let rc_before_abort = fetch(&t, "txn-topic", 0, 1);
    assert!(
        !has_records(&rc_before_abort),
        "read_committed must not see a record from a still-open transaction"
    );

    let mut end_a = EndTxnRequest::default();
    end_a.transactional_id = TransactionalId(StrBytes::from_static_str("tx-a"));
    end_a.producer_id = init_a.producer_id;
    end_a.producer_epoch = init_a.producer_epoch;
    end_a.committed = false; // abort
    let end_a_resp = t.engine.handle_end_txn(&end_a, 1);
    assert_eq!(end_a_resp.error_code, 0);

    // After the abort, LSO has advanced past the control batch, but the
    // aborted record must still never surface to a read_committed fetch.
    let rc_after_abort = fetch(&t, "txn-topic", 0, 1);
    assert!(
        !has_records(&rc_after_abort),
        "read_committed must never see an aborted transaction's records, even after EndTxn"
    );
    assert!(
        rc_after_abort.aborted_transactions.is_some(),
        "the fetch response should list the aborted transaction"
    );

    // --- Producer B: writes one record, then COMMITS. ---
    let init_b = t.engine.handle_init_producer_id(
        &{
            let mut r = InitProducerIdRequest::default();
            r.transactional_id = Some(TransactionalId(StrBytes::from_static_str("tx-b")));
            r.transaction_timeout_ms = 60_000;
            r
        },
        4,
    );
    let pid_b = init_b.producer_id.0;

    let mut add_parts_b = AddPartitionsToTxnRequest::default();
    add_parts_b.v3_and_below_transactional_id = TransactionalId(StrBytes::from_static_str("tx-b"));
    add_parts_b.v3_and_below_producer_id = init_b.producer_id;
    add_parts_b.v3_and_below_producer_epoch = init_b.producer_epoch;
    let mut txn_topic_b = AddPartitionsToTxnTopic::default();
    txn_topic_b.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
    txn_topic_b.partitions = vec![0];
    add_parts_b.v3_and_below_topics = vec![txn_topic_b];
    t.engine.handle_add_partitions_to_txn(&add_parts_b, 1);

    let committed_offset_start = fetch(&t, "txn-topic", 0, 1).high_watermark;

    let mut produce_b = ProduceRequest::default();
    let mut topic_data_b = TopicProduceData::default();
    topic_data_b.name = TopicName::from(StrBytes::from_static_str("txn-topic"));
    let mut part_data_b = PartitionProduceData::default();
    part_data_b.index = 0;
    part_data_b.records =
        Some(bytes::Bytes::from(make_transactional_batch(pid_b, init_b.producer_epoch, 0)));
    topic_data_b.partition_data.push(part_data_b);
    produce_b.topic_data.push(topic_data_b);
    t.engine.handle_produce(&produce_b, 8);

    let rc_before_commit = fetch(&t, "txn-topic", committed_offset_start, 1);
    assert!(
        !has_records(&rc_before_commit),
        "read_committed must not see producer B's record before it commits"
    );

    let mut end_b = EndTxnRequest::default();
    end_b.transactional_id = TransactionalId(StrBytes::from_static_str("tx-b"));
    end_b.producer_id = init_b.producer_id;
    end_b.producer_epoch = init_b.producer_epoch;
    end_b.committed = true;
    t.engine.handle_end_txn(&end_b, 1);

    let rc_after_commit = fetch(&t, "txn-topic", committed_offset_start, 1);
    assert!(
        has_records(&rc_after_commit),
        "read_committed must see producer B's record once its transaction has committed"
    );
}

/// A stale producer epoch (a zombie instance superseded by a newer
/// InitProducerId for the same transactional.id) must be fenced, not
/// silently accepted.
#[test]
fn stale_producer_epoch_is_fenced() {
    let t = T::new();

    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("fence-topic"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    let mut init_req = InitProducerIdRequest::default();
    init_req.transactional_id = Some(TransactionalId(StrBytes::from_static_str("tx-zombie")));
    init_req.transaction_timeout_ms = 60_000;
    let init1 = t.engine.handle_init_producer_id(&init_req, 4);
    assert_eq!(init1.producer_epoch, 0);

    // A second InitProducerId for the same transactional.id (a new
    // instance of the same producer, e.g. after a restart) bumps the
    // epoch and fences the first instance.
    let init2 = t.engine.handle_init_producer_id(&init_req, 4);
    assert_eq!(init2.producer_id, init1.producer_id);
    assert_eq!(init2.producer_epoch, 1);

    // The old (zombie) instance's Produce, still using epoch 0, must be
    // rejected now that epoch 1 is current for this producer_id.
    let mut produce = ProduceRequest::default();
    let mut topic_data = TopicProduceData::default();
    topic_data.name = TopicName::from(StrBytes::from_static_str("fence-topic"));
    let mut part_data = PartitionProduceData::default();
    part_data.index = 0;
    part_data.records =
        Some(bytes::Bytes::from(make_transactional_batch(init1.producer_id.0, 0, 0)));
    topic_data.partition_data.push(part_data);
    produce.topic_data.push(topic_data);
    let resp = t.engine.handle_produce(&produce, 8);
    assert_eq!(resp.responses[0].partition_responses[0].error_code, 47); // INVALID_PRODUCER_EPOCH
}
