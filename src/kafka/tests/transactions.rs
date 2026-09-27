use kafka_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
};
use kafka_protocol::messages::describe_transactions_request::DescribeTransactionsRequest;
use kafka_protocol::messages::end_txn_request::EndTxnRequest;
use kafka_protocol::messages::list_transactions_request::ListTransactionsRequest;
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{GroupId, TopicName, TransactionalId};
use kafka_protocol::protocol::StrBytes;

use super::T;

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
