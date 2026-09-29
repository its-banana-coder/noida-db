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

use super::T;

/// A synthetic v2 record batch carrying one record, with the
/// isTransactional attribute bit set (bit 4, 0x0010) — see Kafka's
/// RecordBatch wire format.
fn make_transactional_batch(producer_id: i64, producer_epoch: i16, base_sequence: i32) -> Vec<u8> {
    let mut buf = vec![0u8; 61];
    buf[16] = 2; // magic v2
    buf[21..23].copy_from_slice(&0x0010i16.to_be_bytes()); // attributes: isTransactional
    buf[23..27].copy_from_slice(&0i32.to_be_bytes()); // last_offset_delta: 1 record
    buf[43..51].copy_from_slice(&producer_id.to_be_bytes());
    buf[51..53].copy_from_slice(&producer_epoch.to_be_bytes());
    buf[53..57].copy_from_slice(&base_sequence.to_be_bytes());
    buf.extend_from_slice(b"payload");
    buf
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
/// not `None` — so "no records visible" has to be checked by byte length,
/// not `Option::is_none()`.
fn has_records(part: &kafka_protocol::messages::fetch_response::PartitionData) -> bool {
    part.records.as_ref().map(|b| !b.is_empty()).unwrap_or(false)
}

#[test]
fn test_transaction_lifecycle() {
    let t = T::new();

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
    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 99);

    // 4. EndTxn (commit)
    let mut end_commit_req = EndTxnRequest::default();
    end_commit_req.transactional_id = TransactionalId(StrBytes::from_static_str("my-tx"));
    end_commit_req.committed = true;
    let end_commit_resp = t.engine.handle_end_txn(&end_commit_req, 1);
    assert_eq!(end_commit_resp.error_code, 0);

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

    let list_tx_req = ListTransactionsRequest::default();
    let list_tx_resp = t.engine.handle_list_transactions(&list_tx_req, 0);
    assert_eq!(list_tx_resp.error_code, 0);
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
