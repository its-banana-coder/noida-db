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
    let values: Vec<&[u8]> =
        (0..records_count).map(|_| b"sample-record-payload".as_ref()).collect();
    super::record_batch(&values, producer_id, producer_epoch, base_sequence)
}

#[test]
fn test_init_producer_id() {
    let t = T::new();

    // kafka-protocol's default transactional_id is Some(""), which a broker
    // rejects; a non-transactional producer sends null.
    let mut req1 = InitProducerIdRequest::default();
    req1.transactional_id = None;
    let resp1 = t.engine.handle_init_producer_id(&req1, 4);
    assert_eq!(resp1.error_code, 0);
    assert!(resp1.producer_id.0 >= 1000);
    assert_eq!(resp1.producer_epoch, 0);

    let req2 = req1.clone();
    let resp2 = t.engine.handle_init_producer_id(&req2, 4);
    assert_eq!(resp2.error_code, 0);
    assert_eq!(resp2.producer_id.0, resp1.producer_id.0 + 1);

    // An empty transactional id, a producer id without an epoch, and a
    // transactional timeout out of range are all refused.
    let empty = InitProducerIdRequest::default();
    assert_eq!(t.engine.handle_init_producer_id(&empty, 4).error_code, 42);
    let mut half = req1.clone();
    half.producer_id = kafka_protocol::messages::ProducerId(5);
    assert_eq!(t.engine.handle_init_producer_id(&half, 4).error_code, 42);
    for timeout in [0, -5, 900_001] {
        let mut tx = InitProducerIdRequest::default();
        tx.transactional_id =
            Some(kafka_protocol::messages::TransactionalId(StrBytes::from_static_str("tx")));
        tx.transaction_timeout_ms = timeout;
        assert_eq!(t.engine.handle_init_producer_id(&tx, 4).error_code, 50, "{timeout}");
    }
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
