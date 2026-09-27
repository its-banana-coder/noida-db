use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_delete_request::{
    OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::protocol::{Encodable, StrBytes};

use super::T;

#[test]
fn test_offset_commit_and_fetch() {
    let t = T::new();

    // Commit offset 42
    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = GroupId(StrBytes::from_static_str("commit-grp"));
    let mut topic_req = OffsetCommitRequestTopic::default();
    topic_req.name = TopicName::from(StrBytes::from_static_str("my-topic"));
    let mut part_req = OffsetCommitRequestPartition::default();
    part_req.partition_index = 0;
    part_req.committed_offset = 42;
    topic_req.partitions.push(part_req);
    commit_req.topics.push(topic_req);

    let commit_resp = t.engine.handle_offset_commit(&commit_req, 8);
    assert_eq!(commit_resp.topics[0].partitions[0].error_code, 0);

    // Fetch offset
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = GroupId(StrBytes::from_static_str("commit-grp"));
    let mut fetch_topic = OffsetFetchRequestTopic::default();
    fetch_topic.name = TopicName::from(StrBytes::from_static_str("my-topic"));
    fetch_topic.partition_indexes.push(0);
    fetch_topic.partition_indexes.push(1); // uncommitted partition
    fetch_req.topics = Some(vec![fetch_topic]);

    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    let parts = &fetch_resp.topics[0].partitions;
    assert_eq!(parts[0].partition_index, 0);
    assert_eq!(parts[0].committed_offset, 42);
    assert_eq!(parts[0].error_code, 0);

    // Uncommitted partition returns -1
    assert_eq!(parts[1].partition_index, 1);
    assert_eq!(parts[1].committed_offset, -1);
    assert_eq!(parts[1].error_code, 0);
}

#[test]
fn test_offset_commit_generation_check() {
    let t = T::new();

    // Join and sync group
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("gen-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;
    let gen_id = step2.generation_id;

    let mut sync_req = SyncGroupRequest::default();
    sync_req.group_id = GroupId(StrBytes::from_static_str("gen-grp"));
    sync_req.member_id = member_id.clone();
    sync_req.generation_id = gen_id;
    let mut assign = SyncGroupRequestAssignment::default();
    assign.member_id = member_id.clone();
    assign.assignment = bytes::Bytes::from("p0");
    sync_req.assignments.push(assign);
    t.engine.handle_sync_group(&sync_req, 3);

    // Commit with matching generation -> 0
    let mut valid_commit = OffsetCommitRequest::default();
    valid_commit.group_id = GroupId(StrBytes::from_static_str("gen-grp"));
    valid_commit.member_id = member_id.clone();
    valid_commit.generation_id_or_member_epoch = gen_id;
    let mut vt = OffsetCommitRequestTopic::default();
    vt.name = TopicName::from(StrBytes::from_static_str("t"));
    let mut vp = OffsetCommitRequestPartition::default();
    vp.partition_index = 0;
    vp.committed_offset = 10;
    vt.partitions.push(vp);
    valid_commit.topics.push(vt);

    let valid_resp = t.engine.handle_offset_commit(&valid_commit, 8);
    assert_eq!(valid_resp.topics[0].partitions[0].error_code, 0);

    // Commit with illegal generation -> ILLEGAL_GENERATION (22)
    let mut bad_commit = OffsetCommitRequest::default();
    bad_commit.group_id = GroupId(StrBytes::from_static_str("gen-grp"));
    bad_commit.member_id = member_id;
    bad_commit.generation_id_or_member_epoch = gen_id + 99;
    let mut bt = OffsetCommitRequestTopic::default();
    bt.name = TopicName::from(StrBytes::from_static_str("t"));
    let mut bp = OffsetCommitRequestPartition::default();
    bp.partition_index = 0;
    bp.committed_offset = 20;
    bt.partitions.push(bp);
    bad_commit.topics.push(bt);

    let bad_resp = t.engine.handle_offset_commit(&bad_commit, 8);
    assert_eq!(bad_resp.topics[0].partitions[0].error_code, 22);
}

#[test]
fn test_offset_delete() {
    let t = T::new();

    // First commit an offset
    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = GroupId(StrBytes::from_static_str("del-grp"));
    let mut topic_req = OffsetCommitRequestTopic::default();
    topic_req.name = TopicName::from(StrBytes::from_static_str("topic-a"));
    let mut part_req = OffsetCommitRequestPartition::default();
    part_req.partition_index = 0;
    part_req.committed_offset = 50;
    topic_req.partitions.push(part_req);
    commit_req.topics.push(topic_req);
    t.engine.handle_offset_commit(&commit_req, 8);

    // Create group record by joining so group exists
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("del-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id;
    t.engine.handle_join_group(&join_req, 5);

    // Delete offset
    let mut del_req = OffsetDeleteRequest::default();
    del_req.group_id = GroupId(StrBytes::from_static_str("del-grp"));
    let mut dt = OffsetDeleteRequestTopic::default();
    dt.name = TopicName::from(StrBytes::from_static_str("topic-a"));
    let mut dp = OffsetDeleteRequestPartition::default();
    dp.partition_index = 0;
    dt.partitions.push(dp);
    del_req.topics.push(dt);

    let del_resp = t.engine.handle_offset_delete(&del_req, 0);
    assert_eq!(del_resp.error_code, 0);

    // Fetch now returns -1
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = GroupId(StrBytes::from_static_str("del-grp"));
    let mut fetch_topic = OffsetFetchRequestTopic::default();
    fetch_topic.name = TopicName::from(StrBytes::from_static_str("topic-a"));
    fetch_topic.partition_indexes.push(0);
    fetch_req.topics = Some(vec![fetch_topic]);
    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, -1);

    // OffsetDelete on unknown group -> GROUP_ID_NOT_FOUND (69)
    let mut del_unknown = OffsetDeleteRequest::default();
    del_unknown.group_id = GroupId(StrBytes::from_static_str("nonexistent-group"));
    let del_unknown_resp = t.engine.handle_offset_delete(&del_unknown, 0);
    assert_eq!(del_unknown_resp.error_code, 69);
}

#[test]
fn test_offset_fetch_v8() {
    let t = T::new();

    // Commit offset 100 for v8-grp
    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = GroupId(StrBytes::from_static_str("v8-grp"));
    let mut topic_req = OffsetCommitRequestTopic::default();
    topic_req.name = TopicName::from(StrBytes::from_static_str("v8-topic"));
    let mut part_req = OffsetCommitRequestPartition::default();
    part_req.partition_index = 0;
    part_req.committed_offset = 100;
    topic_req.partitions.push(part_req);
    commit_req.topics.push(topic_req);
    let commit_resp = t.engine.handle_offset_commit(&commit_req, 8);
    assert_eq!(commit_resp.topics[0].partitions[0].error_code, 0);

    // Fetch offset using v8 format (groups array)
    let mut fetch_req = OffsetFetchRequest::default();
    let mut group_req = OffsetFetchRequestGroup::default();
    group_req.group_id = GroupId(StrBytes::from_static_str("v8-grp"));
    let mut topic_req = OffsetFetchRequestTopics::default();
    topic_req.name = TopicName::from(StrBytes::from_static_str("v8-topic"));
    topic_req.partition_indexes.push(0);
    topic_req.partition_indexes.push(1);
    group_req.topics = Some(vec![topic_req]);
    fetch_req.groups.push(group_req);

    let fetch_resp = t.engine.handle_offset_fetch(&fetch_req, 8);
    assert_eq!(fetch_resp.groups.len(), 1);
    assert_eq!(fetch_resp.groups[0].group_id.as_str(), "v8-grp");
    assert_eq!(fetch_resp.groups[0].topics.len(), 1);
    let parts = &fetch_resp.groups[0].topics[0].partitions;
    assert_eq!(parts[0].partition_index, 0);
    assert_eq!(parts[0].committed_offset, 100);
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(parts[1].partition_index, 1);
    assert_eq!(parts[1].committed_offset, -1);
    assert_eq!(parts[1].error_code, 0);

    // Verify OffsetFetchResponse v8 encodes without error
    let mut buf = bytes::BytesMut::new();
    fetch_resp.encode(&mut buf, 8).expect("v8 response encoding");
}
