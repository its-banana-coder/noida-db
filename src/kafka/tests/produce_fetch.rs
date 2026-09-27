use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::protocol::StrBytes;

use super::{START_MS, T};

#[test]
fn test_produce_and_fetch_happy_path() {
    let t = T::new();

    // Create topic
    let mut req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("logs"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    req.topics.push(topic);
    t.engine.handle_create_topics(&req, 5);

    // Produce first record batch
    let mut prod_req1 = ProduceRequest::default();
    let mut topic_data1 = TopicProduceData::default();
    topic_data1.name = TopicName::from(StrBytes::from_static_str("logs"));
    let mut part_data1 = PartitionProduceData::default();
    part_data1.index = 0;
    part_data1.records = Some(bytes::Bytes::from("msg-1-payload"));
    topic_data1.partition_data.push(part_data1);
    prod_req1.topic_data.push(topic_data1);

    let prod_resp1 = t.engine.handle_produce(&prod_req1, 8);
    let p_res1 = &prod_resp1.responses[0].partition_responses[0];
    assert_eq!(p_res1.error_code, 0);
    assert_eq!(p_res1.base_offset, 0);
    assert_eq!(p_res1.log_append_time_ms, START_MS as i64);

    // Advance clock by 500ms and produce second record
    t.advance(500);

    let mut prod_req2 = ProduceRequest::default();
    let mut topic_data2 = TopicProduceData::default();
    topic_data2.name = TopicName::from(StrBytes::from_static_str("logs"));
    let mut part_data2 = PartitionProduceData::default();
    part_data2.index = 0;
    part_data2.records = Some(bytes::Bytes::from("msg-2-payload"));
    topic_data2.partition_data.push(part_data2);
    prod_req2.topic_data.push(topic_data2);

    let prod_resp2 = t.engine.handle_produce(&prod_req2, 8);
    let p_res2 = &prod_resp2.responses[0].partition_responses[0];
    assert_eq!(p_res2.error_code, 0);
    assert_eq!(p_res2.base_offset, 1);
    assert_eq!(p_res2.log_append_time_ms, (START_MS + 500) as i64);

    // Fetch offset 0
    let mut fetch_req1 = FetchRequest::default();
    let mut fetch_topic1 = FetchTopic::default();
    fetch_topic1.topic = TopicName::from(StrBytes::from_static_str("logs"));
    let mut fetch_part1 = FetchPartition::default();
    fetch_part1.partition = 0;
    fetch_part1.fetch_offset = 0;
    fetch_topic1.partitions.push(fetch_part1);
    fetch_req1.topics.push(fetch_topic1);

    let fetch_resp1 = t.engine.handle_fetch(&fetch_req1, 11);
    let f_part1 = &fetch_resp1.responses[0].partitions[0];
    assert_eq!(f_part1.error_code, 0);
    assert_eq!(f_part1.high_watermark, 2);
    assert_eq!(f_part1.records.as_deref(), Some(b"msg-1-payload".as_ref()));

    // Fetch offset 1
    let mut fetch_req2 = FetchRequest::default();
    let mut fetch_topic2 = FetchTopic::default();
    fetch_topic2.topic = TopicName::from(StrBytes::from_static_str("logs"));
    let mut fetch_part2 = FetchPartition::default();
    fetch_part2.partition = 0;
    fetch_part2.fetch_offset = 1;
    fetch_topic2.partitions.push(fetch_part2);
    fetch_req2.topics.push(fetch_topic2);

    let fetch_resp2 = t.engine.handle_fetch(&fetch_req2, 11);
    let f_part2 = &fetch_resp2.responses[0].partitions[0];
    assert_eq!(f_part2.error_code, 0);
    assert_eq!(f_part2.high_watermark, 2);
    assert_eq!(f_part2.records.as_deref(), Some(b"msg-2-payload".as_ref()));
}

#[test]
fn test_produce_unknown_topic_or_partition() {
    let t = T::new();

    // Unknown topic
    let mut prod_req = ProduceRequest::default();
    let mut topic_data = TopicProduceData::default();
    topic_data.name = TopicName::from(StrBytes::from_static_str("unknown-topic"));
    let mut part_data = PartitionProduceData::default();
    part_data.index = 0;
    part_data.records = Some(bytes::Bytes::from("hello"));
    topic_data.partition_data.push(part_data);
    prod_req.topic_data.push(topic_data);

    let prod_resp = t.engine.handle_produce(&prod_req, 8);
    assert_eq!(prod_resp.responses[0].partition_responses[0].error_code, 3); // UNKNOWN_TOPIC_OR_PARTITION

    // Create topic with 1 partition, produce to partition 99
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("one-part"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    let mut prod_req2 = ProduceRequest::default();
    let mut topic_data2 = TopicProduceData::default();
    topic_data2.name = TopicName::from(StrBytes::from_static_str("one-part"));
    let mut part_data2 = PartitionProduceData::default();
    part_data2.index = 99;
    part_data2.records = Some(bytes::Bytes::from("hello"));
    topic_data2.partition_data.push(part_data2);
    prod_req2.topic_data.push(topic_data2);

    let prod_resp2 = t.engine.handle_produce(&prod_req2, 8);
    assert_eq!(prod_resp2.responses[0].partition_responses[0].error_code, 3);
}

#[test]
fn test_fetch_error_cases() {
    let t = T::new();

    // Create topic with 1 partition
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("fetch-err"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    // Fetch from unknown topic -> UNKNOWN_TOPIC_OR_PARTITION (3)
    let mut fetch_req_unknown = FetchRequest::default();
    let mut fetch_topic_unknown = FetchTopic::default();
    fetch_topic_unknown.topic = TopicName::from(StrBytes::from_static_str("nonexistent"));
    let mut fetch_part = FetchPartition::default();
    fetch_part.partition = 0;
    fetch_part.fetch_offset = 0;
    fetch_topic_unknown.partitions.push(fetch_part);
    fetch_req_unknown.topics.push(fetch_topic_unknown);

    let resp_unknown = t.engine.handle_fetch(&fetch_req_unknown, 11);
    assert_eq!(resp_unknown.responses[0].partitions[0].error_code, 3);

    // Fetch offset out of range (> high watermark 0) -> OFFSET_OUT_OF_RANGE (1)
    let mut fetch_req_oor = FetchRequest::default();
    let mut fetch_topic_oor = FetchTopic::default();
    fetch_topic_oor.topic = TopicName::from(StrBytes::from_static_str("fetch-err"));
    let mut fetch_part_oor = FetchPartition::default();
    fetch_part_oor.partition = 0;
    fetch_part_oor.fetch_offset = 5; // high_watermark is 0
    fetch_topic_oor.partitions.push(fetch_part_oor);
    fetch_req_oor.topics.push(fetch_topic_oor);

    let resp_oor = t.engine.handle_fetch(&fetch_req_oor, 11);
    assert_eq!(resp_oor.responses[0].partitions[0].error_code, 1);
}

#[test]
fn test_list_offsets() {
    let t = T::new();

    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("offsets-test"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    // Produce 3 records
    for i in 0..3 {
        let mut prod_req = ProduceRequest::default();
        let mut topic_data = TopicProduceData::default();
        topic_data.name = TopicName::from(StrBytes::from_static_str("offsets-test"));
        let mut part_data = PartitionProduceData::default();
        part_data.index = 0;
        part_data.records = Some(bytes::Bytes::from(format!("msg-{}", i)));
        topic_data.partition_data.push(part_data);
        prod_req.topic_data.push(topic_data);
        t.engine.handle_produce(&prod_req, 8);
    }

    // ListOffsets earliest (-2)
    let mut lo_earliest = ListOffsetsRequest::default();
    let mut lot_earliest = ListOffsetsTopic::default();
    lot_earliest.name = TopicName::from(StrBytes::from_static_str("offsets-test"));
    let mut lop_earliest = ListOffsetsPartition::default();
    lop_earliest.partition_index = 0;
    lop_earliest.timestamp = -2;
    lot_earliest.partitions.push(lop_earliest);
    lo_earliest.topics.push(lot_earliest);

    let lo_resp_earliest = t.engine.handle_list_offsets(&lo_earliest, 6);
    assert_eq!(lo_resp_earliest.topics[0].partitions[0].error_code, 0);
    assert_eq!(lo_resp_earliest.topics[0].partitions[0].offset, 0);

    // ListOffsets latest (-1)
    let mut lo_latest = ListOffsetsRequest::default();
    let mut lot_latest = ListOffsetsTopic::default();
    lot_latest.name = TopicName::from(StrBytes::from_static_str("offsets-test"));
    let mut lop_latest = ListOffsetsPartition::default();
    lop_latest.partition_index = 0;
    lop_latest.timestamp = -1;
    lot_latest.partitions.push(lop_latest);
    lo_latest.topics.push(lot_latest);

    let lo_resp_latest = t.engine.handle_list_offsets(&lo_latest, 6);
    assert_eq!(lo_resp_latest.topics[0].partitions[0].error_code, 0);
    assert_eq!(lo_resp_latest.topics[0].partitions[0].offset, 3);

    // ListOffsets unknown topic -> 3
    let mut lo_unknown = ListOffsetsRequest::default();
    let mut lot_unknown = ListOffsetsTopic::default();
    lot_unknown.name = TopicName::from(StrBytes::from_static_str("unknown-topic"));
    let mut lop_unknown = ListOffsetsPartition::default();
    lop_unknown.partition_index = 0;
    lot_unknown.partitions.push(lop_unknown);
    lo_unknown.topics.push(lot_unknown);

    let lo_resp_unknown = t.engine.handle_list_offsets(&lo_unknown, 6);
    assert_eq!(lo_resp_unknown.topics[0].partitions[0].error_code, 3);
}
