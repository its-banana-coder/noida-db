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
    t.create_topic("my-topic", 2);

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
    t.create_topic("t", 2);

    // Join and sync group
    let mut join_req = JoinGroupRequest::default();
    join_req.protocols.push(
        kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default()
            .with_name(StrBytes::from_static_str("range")),
    );
    join_req.session_timeout_ms = 10_000; // within group.min/max.session.timeout.ms
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
    t.create_topic("topic-a", 2);

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
    join_req.protocols.push(
        kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default()
            .with_name(StrBytes::from_static_str("range")),
    );
    join_req.session_timeout_ms = 10_000; // within group.min/max.session.timeout.ms
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
    t.create_topic("v8-topic", 2);

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

fn commit(t: &T, group: &str, topic: &str, partition: i32, offset: i64, meta: Option<&str>) -> i16 {
    let mut req = OffsetCommitRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    req.generation_id_or_member_epoch = -1;
    let mut tr = OffsetCommitRequestTopic::default();
    tr.name = TopicName::from(StrBytes::from_string(topic.to_string()));
    let mut pr = OffsetCommitRequestPartition::default();
    pr.partition_index = partition;
    pr.committed_offset = offset;
    pr.committed_metadata = meta.map(|m| StrBytes::from_string(m.to_string()));
    tr.partitions.push(pr);
    req.topics.push(tr);
    t.engine.handle_offset_commit(&req, 8).topics[0].partitions[0].error_code
}

fn fetch(t: &T, group: &str, topic: &str, partition: i32) -> (i64, String) {
    let mut req = OffsetFetchRequest::default();
    req.group_id = GroupId(StrBytes::from_string(group.to_string()));
    let mut ft = OffsetFetchRequestTopic::default();
    ft.name = TopicName::from(StrBytes::from_string(topic.to_string()));
    ft.partition_indexes.push(partition);
    req.topics = Some(vec![ft]);
    let res = t.engine.handle_offset_fetch(&req, 7);
    let p = &res.topics[0].partitions[0];
    (p.committed_offset, p.metadata.as_deref().unwrap_or("<null>").to_string())
}

/// Kafka 3.8 refuses commits for a topic or partition that doesn't exist.
#[test]
fn test_offset_commit_unknown_topic_or_partition() {
    let t = T::new();
    t.create_topic("known", 1);
    assert_eq!(commit(&t, "g", "known", 0, 5, None), 0);
    assert_eq!(commit(&t, "g", "known", 3, 5, None), 3);
    assert_eq!(commit(&t, "g", "missing", 0, 5, None), 3);
    assert_eq!(fetch(&t, "g", "known", 3).0, -1);
}

/// The metadata string committed with an offset comes back on fetch.
#[test]
fn test_offset_commit_metadata_round_trip() {
    let t = T::new();
    t.create_topic("meta", 1);
    assert_eq!(commit(&t, "g", "meta", 0, 1, Some("meta-1")), 0);
    assert_eq!(fetch(&t, "g", "meta", 0), (1, "meta-1".to_string()));
    assert_eq!(commit(&t, "g", "meta", 0, 2, None), 0);
    assert_eq!(fetch(&t, "g", "meta", 0), (2, String::new()));
}

fn list_groups(t: &T, states: &[&str]) -> Vec<(String, String, String)> {
    use kafka_protocol::messages::ListGroupsRequest;
    let mut req = ListGroupsRequest::default();
    req.states_filter = states.iter().map(|s| StrBytes::from_string(s.to_string())).collect();
    let mut v: Vec<_> = t
        .engine
        .handle_list_groups(&req, 4)
        .groups
        .iter()
        .map(|g| {
            (
                g.group_id.as_str().to_string(),
                g.group_state.to_string(),
                g.protocol_type.to_string(),
            )
        })
        .collect();
    v.sort();
    v
}

fn describe_group(t: &T, group: &str) -> (String, String, String, usize) {
    use kafka_protocol::messages::DescribeGroupsRequest;
    let mut req = DescribeGroupsRequest::default();
    req.groups.push(GroupId(StrBytes::from_string(group.to_string())));
    let res = t.engine.handle_describe_groups(&req, 5);
    let g = &res.groups[0];
    (
        g.group_state.to_string(),
        g.protocol_type.to_string(),
        g.protocol_data.to_string(),
        g.members.len(),
    )
}

fn delete_group(t: &T, group: &str) -> i16 {
    use kafka_protocol::messages::DeleteGroupsRequest;
    let mut req = DeleteGroupsRequest::default();
    req.groups_names.push(GroupId(StrBytes::from_string(group.to_string())));
    t.engine.handle_delete_groups(&req, 2).results[0].error_code
}

/// A group only used through manual assignment + commits (no members ever)
/// is, to Kafka, an Empty group with no protocol type: listed, described,
/// deletable once -- a second delete is GROUP_ID_NOT_FOUND.
#[test]
fn test_offsets_only_group_is_visible_and_deletable_once() {
    let t = T::new();
    t.create_topic("oo", 1);
    assert_eq!(commit(&t, "simple", "oo", 0, 1, None), 0);
    assert_eq!(list_groups(&t, &[]), vec![("simple".into(), "Empty".into(), String::new())]);
    assert_eq!(list_groups(&t, &["Stable"]), vec![]);
    assert_eq!(list_groups(&t, &["empty"]).len(), 1);
    assert_eq!(describe_group(&t, "simple"), ("Empty".into(), String::new(), String::new(), 0));
    assert_eq!(delete_group(&t, "simple"), 0);
    assert_eq!(delete_group(&t, "simple"), 69);
    assert_eq!(delete_group(&t, "never-existed"), 69);
    assert_eq!(fetch(&t, "simple", "oo", 0).0, -1);
    assert_eq!(list_groups(&t, &[]), vec![]);
    assert_eq!(describe_group(&t, "simple").0, "Dead");
}

/// Once its last member leaves, a consumer group is Empty with no chosen
/// protocol; deleting it twice fails the second time.
#[test]
fn test_empty_group_after_leave_has_no_protocol() {
    use kafka_protocol::messages::LeaveGroupRequest;
    use kafka_protocol::messages::leave_group_request::MemberIdentity;
    let t = T::new();
    let mut join_req = JoinGroupRequest::default();
    join_req.protocols.push(
        kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default()
            .with_name(StrBytes::from_static_str("range")),
    );
    join_req.session_timeout_ms = 10_000;
    join_req.group_id = GroupId(StrBytes::from_static_str("lg"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);

    let mut leave = LeaveGroupRequest::default();
    leave.group_id = GroupId(StrBytes::from_static_str("lg"));
    leave.members.push(MemberIdentity::default().with_member_id(step2.member_id.clone()));
    t.engine.handle_leave_group(&leave, 3);
    assert_eq!(describe_group(&t, "lg"), ("Empty".into(), "consumer".into(), String::new(), 0));
    assert_eq!(list_groups(&t, &[]), vec![("lg".into(), "Empty".into(), "consumer".into())]);
    assert_eq!(delete_group(&t, "lg"), 0);
    assert_eq!(delete_group(&t, "lg"), 69);

    // The deleted group's id can be used again by a new consumer.
    join_req.member_id = StrBytes::default();
    let again = t.engine.handle_join_group(&join_req, 5);
    assert_eq!(again.error_code, 79); // MEMBER_ID_REQUIRED, then a normal join
    assert!(again.member_id.starts_with("noida-client-"));
    join_req.member_id = again.member_id.clone();
    assert_eq!(t.engine.handle_join_group(&join_req, 5).error_code, 0);
}
