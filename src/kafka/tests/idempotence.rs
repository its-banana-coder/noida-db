use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::{InitProducerIdRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::T;

fn make_v2_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_count: i32,
) -> Vec<u8> {
    let mut buf = vec![0u8; 61];
    // base_offset = 0
    buf[0..8].copy_from_slice(&0i64.to_be_bytes());
    // batch_length = 50
    buf[8..12].copy_from_slice(&50i32.to_be_bytes());
    // partition_leader_epoch = 0
    buf[12..16].copy_from_slice(&0i32.to_be_bytes());
    // magic = 2
    buf[16] = 2;
    // crc = 0
    buf[17..21].copy_from_slice(&0u32.to_be_bytes());
    // attributes = 0
    buf[21..23].copy_from_slice(&0i16.to_be_bytes());
    // last_offset_delta = records_count - 1
    let delta = (records_count - 1).max(0);
    buf[23..27].copy_from_slice(&delta.to_be_bytes());
    // first_timestamp = 0
    buf[27..35].copy_from_slice(&0i64.to_be_bytes());
    // max_timestamp = 0
    buf[35..43].copy_from_slice(&0i64.to_be_bytes());
    // producer_id
    buf[43..51].copy_from_slice(&producer_id.to_be_bytes());
    // producer_epoch
    buf[51..53].copy_from_slice(&producer_epoch.to_be_bytes());
    // base_sequence
    buf[53..57].copy_from_slice(&base_sequence.to_be_bytes());
    // records_count
    buf[57..61].copy_from_slice(&records_count.to_be_bytes());
    // append arbitrary payload
    buf.extend_from_slice(b"sample-record-payload");
    buf
}

#[test]
fn test_init_producer_id() {
    let t = T::new();

    let req1 = InitProducerIdRequest::default();
    let resp1 = t.engine.handle_init_producer_id(&req1, 4);
    assert_eq!(resp1.error_code, 0);
    assert!(resp1.producer_id.0 >= 1000);
    assert_eq!(resp1.producer_epoch, 0);

    let req2 = InitProducerIdRequest::default();
    let resp2 = t.engine.handle_init_producer_id(&req2, 4);
    assert_eq!(resp2.error_code, 0);
    assert_eq!(resp2.producer_id.0, resp1.producer_id.0 + 1);
}

#[test]
fn test_idempotent_producer_sequence_and_deduplication() {
    let t = T::new();

    // Create topic
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("idempotent-topic"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    let pid = 1001i64;
    let epoch = 0i16;

    // 1. Send first batch with sequence 0
    let batch0 = make_v2_batch(pid, epoch, 0, 1);
    let mut prod_req0 = ProduceRequest::default();
    let mut td0 = TopicProduceData::default();
    td0.name = TopicName::from(StrBytes::from_static_str("idempotent-topic"));
    let mut pd0 = PartitionProduceData::default();
    pd0.index = 0;
    pd0.records = Some(bytes::Bytes::from(batch0.clone()));
    td0.partition_data.push(pd0);
    prod_req0.topic_data.push(td0);

    let resp0 = t.engine.handle_produce(&prod_req0, 8);
    let p_res0 = &resp0.responses[0].partition_responses[0];
    assert_eq!(p_res0.error_code, 0);
    assert_eq!(p_res0.base_offset, 0);

    // 2. Resend the exact same batch with sequence 0 (duplicate retry)
    // Broker should acknowledge without re-appending!
    let resp0_dup = t.engine.handle_produce(&prod_req0, 8);
    let p_res0_dup = &resp0_dup.responses[0].partition_responses[0];
    assert_eq!(p_res0_dup.error_code, 0);
    assert_eq!(p_res0_dup.base_offset, 0); // same base offset returned

    // 3. Send batch with sequence 1 -> succeeds with offset 1
    let batch1 = make_v2_batch(pid, epoch, 1, 1);
    let mut prod_req1 = ProduceRequest::default();
    let mut td1 = TopicProduceData::default();
    td1.name = TopicName::from(StrBytes::from_static_str("idempotent-topic"));
    let mut pd1 = PartitionProduceData::default();
    pd1.index = 0;
    pd1.records = Some(bytes::Bytes::from(batch1));
    td1.partition_data.push(pd1);
    prod_req1.topic_data.push(td1);

    let resp1 = t.engine.handle_produce(&prod_req1, 8);
    let p_res1 = &resp1.responses[0].partition_responses[0];
    assert_eq!(p_res1.error_code, 0);
    assert_eq!(p_res1.base_offset, 1);

    // 4. Send batch with sequence 5 (gap: expected 2) -> OUT_OF_ORDER_SEQUENCE_NUMBER (45)
    let batch5 = make_v2_batch(pid, epoch, 5, 1);
    let mut prod_req5 = ProduceRequest::default();
    let mut td5 = TopicProduceData::default();
    td5.name = TopicName::from(StrBytes::from_static_str("idempotent-topic"));
    let mut pd5 = PartitionProduceData::default();
    pd5.index = 0;
    pd5.records = Some(bytes::Bytes::from(batch5));
    td5.partition_data.push(pd5);
    prod_req5.topic_data.push(td5);

    let resp5 = t.engine.handle_produce(&prod_req5, 8);
    let p_res5 = &resp5.responses[0].partition_responses[0];
    assert_eq!(p_res5.error_code, 45); // OUT_OF_ORDER_SEQUENCE_NUMBER
}

#[test]
fn test_init_producer_id_transactional() {
    let t = T::new();

    // Test with transactional_id
    let mut req_txn1 = InitProducerIdRequest::default();
    req_txn1.transactional_id = Some(kafka_protocol::messages::TransactionalId(
        kafka_protocol::protocol::StrBytes::from_static_str("tx1"),
    ));
    let resp_txn1 = t.engine.handle_init_producer_id(&req_txn1, 4);
    assert_eq!(resp_txn1.error_code, 0);
    assert_eq!(resp_txn1.producer_epoch, 0);
    let txn_pid = resp_txn1.producer_id.0;

    let resp_txn2 = t.engine.handle_init_producer_id(&req_txn1, 4);
    assert_eq!(resp_txn2.error_code, 0);
    assert_eq!(resp_txn2.producer_id.0, txn_pid);
    assert_eq!(resp_txn2.producer_epoch, 1);
}
