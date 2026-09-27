use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use kafka_protocol::protocol::StrBytes;

use super::T;

#[test]
fn test_create_topics_happy_path() {
    let t = T::new();

    let mut req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("orders"));
    topic.num_partitions = 3;
    topic.replication_factor = 1;
    req.topics.push(topic);

    let resp = t.engine.handle_create_topics(&req, 5);
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].name.as_str(), "orders");

    // Verify metadata shows 3 partitions
    let mut meta_req = MetadataRequest::default();
    let mut mt = MetadataRequestTopic::default();
    mt.name = Some(TopicName::from(StrBytes::from_static_str("orders")));
    meta_req.topics = Some(vec![mt]);

    let meta_resp = t.engine.handle_metadata(&meta_req, 9);
    assert_eq!(meta_resp.topics.len(), 1);
    assert_eq!(meta_resp.topics[0].error_code, 0);
    assert_eq!(meta_resp.topics[0].partitions.len(), 3);
    assert!(!meta_resp.topics[0].is_internal);
}

#[test]
fn test_create_topics_already_exists() {
    let t = T::new();

    let mut req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("events"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    req.topics.push(topic);

    let resp1 = t.engine.handle_create_topics(&req, 5);
    assert_eq!(resp1.topics[0].error_code, 0);

    // Duplicate creation returns TOPIC_ALREADY_EXISTS (36)
    let resp2 = t.engine.handle_create_topics(&req, 5);
    assert_eq!(resp2.topics[0].error_code, 36);
}

#[test]
fn test_create_topics_invalid_partitions_and_rf() {
    let t = T::new();

    let mut req = CreateTopicsRequest::default();

    // Invalid partitions (<= 0) -> INVALID_PARTITIONS (37)
    let mut topic1 = CreatableTopic::default();
    topic1.name = TopicName::from(StrBytes::from_static_str("bad-part"));
    topic1.num_partitions = 0;
    topic1.replication_factor = 1;
    req.topics.push(topic1);

    // Replication factor > 1 -> INVALID_REPLICATION_FACTOR (38)
    let mut topic2 = CreatableTopic::default();
    topic2.name = TopicName::from(StrBytes::from_static_str("bad-rf"));
    topic2.num_partitions = 1;
    topic2.replication_factor = 3;
    req.topics.push(topic2);

    // Empty topic name -> INVALID_TOPIC_EXCEPTION (17)
    let mut topic3 = CreatableTopic::default();
    topic3.name = TopicName::from(StrBytes::from_static_str(""));
    topic3.num_partitions = 1;
    topic3.replication_factor = 1;
    req.topics.push(topic3);

    let resp = t.engine.handle_create_topics(&req, 5);
    assert_eq!(resp.topics[0].error_code, 37);
    assert_eq!(resp.topics[1].error_code, 38);
    assert_eq!(resp.topics[2].error_code, 17);
}

#[test]
fn test_create_topics_validate_only() {
    let t = T::new();

    let mut req = CreateTopicsRequest::default();
    req.validate_only = true;
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("dry-run-topic"));
    topic.num_partitions = 2;
    topic.replication_factor = 1;
    req.topics.push(topic);

    let resp = t.engine.handle_create_topics(&req, 5);
    assert_eq!(resp.topics[0].error_code, 0);

    // Topic should NOT exist in metadata with allow_auto_topic_creation: false
    let mut meta_req = MetadataRequest::default();
    meta_req.allow_auto_topic_creation = false;
    let mut mt = MetadataRequestTopic::default();
    mt.name = Some(TopicName::from(StrBytes::from_static_str("dry-run-topic")));
    meta_req.topics = Some(vec![mt]);

    let meta_resp = t.engine.handle_metadata(&meta_req, 9);
    assert_eq!(meta_resp.topics[0].error_code, 3); // UNKNOWN_TOPIC_OR_PARTITION
}

#[test]
fn test_delete_topics() {
    let t = T::new();

    // Create topic
    let mut req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("to-delete"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    req.topics.push(topic);
    t.engine.handle_create_topics(&req, 5);

    // Delete existing topic -> 0
    let mut del_req = DeleteTopicsRequest::default();
    del_req.topic_names.push(TopicName::from(StrBytes::from_static_str("to-delete")));
    let del_resp = t.engine.handle_delete_topics(&del_req, 4);
    assert_eq!(del_resp.responses[0].error_code, 0);

    // Delete nonexistent topic -> 3 (UNKNOWN_TOPIC_OR_PARTITION)
    let mut del_unknown_req = DeleteTopicsRequest::default();
    del_unknown_req.topic_names.push(TopicName::from(StrBytes::from_static_str("nonexistent")));
    let del_unknown_resp = t.engine.handle_delete_topics(&del_unknown_req, 4);
    assert_eq!(del_unknown_resp.responses[0].error_code, 3);
}

#[test]
fn test_create_partitions() {
    let t = T::new();

    // Create topic with 2 partitions
    let mut req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("partition-growth"));
    topic.num_partitions = 2;
    topic.replication_factor = 1;
    req.topics.push(topic);
    t.engine.handle_create_topics(&req, 5);

    // Expand to 5 partitions -> 0
    let mut cp_req = CreatePartitionsRequest::default();
    let mut cp_topic = CreatePartitionsTopic::default();
    cp_topic.name = TopicName::from(StrBytes::from_static_str("partition-growth"));
    cp_topic.count = 5;
    cp_req.topics.push(cp_topic);
    let cp_resp = t.engine.handle_create_partitions(&cp_req, 2);
    assert_eq!(cp_resp.results[0].error_code, 0);

    // Try to decrease or keep same partition count (5 -> 3) -> INVALID_PARTITIONS (37)
    let mut cp_req_invalid = CreatePartitionsRequest::default();
    let mut cp_topic_invalid = CreatePartitionsTopic::default();
    cp_topic_invalid.name = TopicName::from(StrBytes::from_static_str("partition-growth"));
    cp_topic_invalid.count = 3;
    cp_req_invalid.topics.push(cp_topic_invalid);
    let cp_resp_invalid = t.engine.handle_create_partitions(&cp_req_invalid, 2);
    assert_eq!(cp_resp_invalid.results[0].error_code, 37);

    // Unknown topic -> UNKNOWN_TOPIC_OR_PARTITION (3)
    let mut cp_req_unknown = CreatePartitionsRequest::default();
    let mut cp_topic_unknown = CreatePartitionsTopic::default();
    cp_topic_unknown.name = TopicName::from(StrBytes::from_static_str("missing-topic"));
    cp_topic_unknown.count = 5;
    cp_req_unknown.topics.push(cp_topic_unknown);
    let cp_resp_unknown = t.engine.handle_create_partitions(&cp_req_unknown, 2);
    assert_eq!(cp_resp_unknown.results[0].error_code, 3);
}

#[test]
fn test_metadata_internal_topics_and_auto_creation() {
    let t = T::new();

    // Query all topics (empty topic list)
    let meta_req = MetadataRequest::default();
    let meta_resp = t.engine.handle_metadata(&meta_req, 9);

    let consumer_offsets = meta_resp
        .topics
        .iter()
        .find(|topic| topic.name.as_ref().map(|n| n.as_str()) == Some("__consumer_offsets"))
        .expect("__consumer_offsets must exist in metadata");
    assert!(consumer_offsets.is_internal);
    assert_eq!(consumer_offsets.partitions.len(), 50);

    let txn_state = meta_resp
        .topics
        .iter()
        .find(|topic| topic.name.as_ref().map(|n| n.as_str()) == Some("__transaction_state"))
        .expect("__transaction_state must exist in metadata");
    assert!(txn_state.is_internal);
    assert_eq!(txn_state.partitions.len(), 50);

    // Auto topic creation enabled
    let mut auto_meta_req = MetadataRequest::default();
    auto_meta_req.allow_auto_topic_creation = true;
    let mut auto_topic = MetadataRequestTopic::default();
    auto_topic.name = Some(TopicName::from(StrBytes::from_static_str("auto-created")));
    auto_meta_req.topics = Some(vec![auto_topic]);

    let auto_meta_resp = t.engine.handle_metadata(&auto_meta_req, 9);
    assert_eq!(auto_meta_resp.topics[0].error_code, 0);
    assert_eq!(auto_meta_resp.topics[0].partitions.len(), 1);
    assert!(!auto_meta_resp.topics[0].is_internal);

    // Auto topic creation disabled for missing topic
    let mut no_auto_req = MetadataRequest::default();
    no_auto_req.allow_auto_topic_creation = false;
    let mut missing_topic = MetadataRequestTopic::default();
    missing_topic.name = Some(TopicName::from(StrBytes::from_static_str("does-not-exist")));
    no_auto_req.topics = Some(vec![missing_topic]);

    let no_auto_resp = t.engine.handle_metadata(&no_auto_req, 9);
    assert_eq!(no_auto_resp.topics[0].error_code, 3);
}
