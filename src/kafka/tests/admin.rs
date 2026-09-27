use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::delete_groups_request::DeleteGroupsRequest;
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use kafka_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use kafka_protocol::messages::describe_log_dirs_request::DescribeLogDirsRequest;
use kafka_protocol::messages::describe_producers_request::DescribeProducersRequest;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrAlterConfigsResource, AlterableConfig as IncrAlterableConfig,
    IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::offset_for_leader_epoch_request::{
    OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use kafka_protocol::messages::{
    DescribeClusterRequest, GroupId, SaslAuthenticateRequest, SaslHandshakeRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use super::T;

#[test]
fn test_describe_and_list_and_delete_groups() {
    let t = T::new();

    // 1. Join group to make it active (Stable)
    let mut join_req = JoinGroupRequest::default();
    join_req.group_id = GroupId(StrBytes::from_static_str("admin-grp"));
    join_req.protocol_type = StrBytes::from_static_str("consumer");
    let step1 = t.engine.handle_join_group(&join_req, 5);
    join_req.member_id = step1.member_id.clone();
    let step2 = t.engine.handle_join_group(&join_req, 5);
    let member_id = step2.member_id;

    let mut sync_req = SyncGroupRequest::default();
    sync_req.group_id = GroupId(StrBytes::from_static_str("admin-grp"));
    sync_req.member_id = member_id.clone();
    sync_req.generation_id = step2.generation_id;
    let mut assign = SyncGroupRequestAssignment::default();
    assign.member_id = member_id.clone();
    assign.assignment = bytes::Bytes::from("assignment");
    sync_req.assignments.push(assign);
    t.engine.handle_sync_group(&sync_req, 3);

    // ListGroups lists the group
    let list_req = ListGroupsRequest::default();
    let list_resp = t.engine.handle_list_groups(&list_req, 4);
    assert_eq!(list_resp.error_code, 0);
    assert!(list_resp.groups.iter().any(|g| g.group_id.as_str() == "admin-grp"));

    // DescribeGroups returns Stable state and member details
    let mut desc_req = DescribeGroupsRequest::default();
    desc_req.groups.push(GroupId(StrBytes::from_static_str("admin-grp")));
    let desc_resp = t.engine.handle_describe_groups(&desc_req, 5);
    assert_eq!(desc_resp.groups[0].error_code, 0);
    assert_eq!(desc_resp.groups[0].group_state.as_str(), "Stable");
    assert_eq!(desc_resp.groups[0].members.len(), 1);

    // Try deleting active group -> NON_EMPTY_GROUP (68)
    let mut del_grp_req = DeleteGroupsRequest::default();
    del_grp_req.groups_names.push(GroupId(StrBytes::from_static_str("admin-grp")));
    let del_resp_active = t.engine.handle_delete_groups(&del_grp_req, 2);
    assert_eq!(del_resp_active.results[0].error_code, 68);

    // Try deleting nonexistent group -> GROUP_ID_NOT_FOUND (69)
    let mut del_nonexistent = DeleteGroupsRequest::default();
    del_nonexistent.groups_names.push(GroupId(StrBytes::from_static_str("ghost-grp")));
    let del_resp_ghost = t.engine.handle_delete_groups(&del_nonexistent, 2);
    assert_eq!(del_resp_ghost.results[0].error_code, 69);
}

#[test]
fn test_configs_management() {
    let t = T::new();

    // Create topic
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("config-topic"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    // DescribeConfigs for topic
    let mut desc_req = DescribeConfigsRequest::default();
    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2; // Topic
    resource.resource_name = StrBytes::from_static_str("config-topic");
    desc_req.resources.push(resource);

    let desc_resp = t.engine.handle_describe_configs(&desc_req, 4);
    assert_eq!(desc_resp.results[0].error_code, 0);
    let cleanup = desc_resp.results[0]
        .configs
        .iter()
        .find(|c| c.name.as_str() == "cleanup.policy")
        .expect("cleanup.policy config exists");
    assert_eq!(cleanup.value.as_ref().map(|s| s.as_str()), Some("delete"));

    // DescribeConfigs for unknown topic -> UNKNOWN_TOPIC_OR_PARTITION (3)
    let mut desc_req_bad = DescribeConfigsRequest::default();
    let mut resource_bad = DescribeConfigsResource::default();
    resource_bad.resource_type = 2;
    resource_bad.resource_name = StrBytes::from_static_str("missing-topic");
    desc_req_bad.resources.push(resource_bad);

    let desc_resp_bad = t.engine.handle_describe_configs(&desc_req_bad, 4);
    assert_eq!(desc_resp_bad.results[0].error_code, 3);

    // AlterConfigs topic config to compact
    let mut alter_req = AlterConfigsRequest::default();
    let mut alter_res = AlterConfigsResource::default();
    alter_res.resource_type = 2;
    alter_res.resource_name = StrBytes::from_static_str("config-topic");
    let mut conf_entry = AlterableConfig::default();
    conf_entry.name = StrBytes::from_static_str("cleanup.policy");
    conf_entry.value = Some(StrBytes::from_static_str("compact"));
    alter_res.configs.push(conf_entry);
    alter_req.resources.push(alter_res);

    let alter_resp = t.engine.handle_alter_configs(&alter_req, 2);
    assert_eq!(alter_resp.responses[0].error_code, 0);

    // Verify DescribeConfigs reflects the altered value "compact"
    let desc_resp2 = t.engine.handle_describe_configs(&desc_req, 4);
    let cleanup2 =
        desc_resp2.results[0].configs.iter().find(|c| c.name.as_str() == "cleanup.policy").unwrap();
    assert_eq!(cleanup2.value.as_ref().map(|s| s.as_str()), Some("compact"));

    // IncrementalAlterConfigs: SET retention.ms
    let mut incr_req = IncrementalAlterConfigsRequest::default();
    let mut incr_res = IncrAlterConfigsResource::default();
    incr_res.resource_type = 2;
    incr_res.resource_name = StrBytes::from_static_str("config-topic");
    let mut incr_entry = IncrAlterableConfig::default();
    incr_entry.name = StrBytes::from_static_str("retention.ms");
    incr_entry.config_operation = 0; // SET
    incr_entry.value = Some(StrBytes::from_static_str("123456"));
    incr_res.configs.push(incr_entry);
    incr_req.resources.push(incr_res);

    let incr_resp = t.engine.handle_incremental_alter_configs(&incr_req, 1);
    assert_eq!(incr_resp.responses[0].error_code, 0);

    let desc_resp3 = t.engine.handle_describe_configs(&desc_req, 4);
    let ret =
        desc_resp3.results[0].configs.iter().find(|c| c.name.as_str() == "retention.ms").unwrap();
    assert_eq!(ret.value.as_ref().map(|s| s.as_str()), Some("123456"));
}

#[test]
fn test_describe_cluster_and_offset_for_leader_epoch() {
    let t = T::new();

    // Create topic and produce 2 records
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("cluster-test"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    t.engine.handle_create_topics(&create_req, 5);

    let mut prod_req = ProduceRequest::default();
    let mut td = TopicProduceData::default();
    td.name = TopicName::from(StrBytes::from_static_str("cluster-test"));
    let mut pd = PartitionProduceData::default();
    pd.index = 0;
    pd.records = Some(bytes::Bytes::from("hello"));
    td.partition_data.push(pd);
    prod_req.topic_data.push(td);
    t.engine.handle_produce(&prod_req, 8);

    // DescribeCluster
    let dc_req = DescribeClusterRequest::default();
    let dc_resp = t.engine.handle_describe_cluster(&dc_req, 0);
    assert_eq!(dc_resp.error_code, 0);
    assert_eq!(dc_resp.controller_id.0, 1);
    assert!(!dc_resp.brokers.is_empty());

    // OffsetForLeaderEpoch
    let mut ofle_req = OffsetForLeaderEpochRequest::default();
    let mut ofle_topic = OffsetForLeaderTopic::default();
    ofle_topic.topic = TopicName::from(StrBytes::from_static_str("cluster-test"));
    let mut ofle_part = OffsetForLeaderPartition::default();
    ofle_part.partition = 0;
    ofle_part.leader_epoch = 0;
    ofle_topic.partitions.push(ofle_part);
    ofle_req.topics.push(ofle_topic);

    let ofle_resp = t.engine.handle_offset_for_leader_epoch(&ofle_req, 4);
    assert_eq!(ofle_resp.topics[0].partitions[0].error_code, 0);
    assert_eq!(ofle_resp.topics[0].partitions[0].leader_epoch, 0);
    assert_eq!(ofle_resp.topics[0].partitions[0].end_offset, 1);
}

#[test]
fn test_delete_records_and_log_dirs_and_producers_and_sasl() {
    let t = T::new();

    // DeleteRecords
    let mut dr_req = DeleteRecordsRequest::default();
    let mut dr_topic = DeleteRecordsTopic::default();
    dr_topic.name = TopicName::from(StrBytes::from_static_str("del-rec"));
    let mut dr_part = DeleteRecordsPartition::default();
    dr_part.partition_index = 0;
    dr_part.offset = 10;
    dr_topic.partitions.push(dr_part);
    dr_req.topics.push(dr_topic);

    let dr_resp = t.engine.handle_delete_records(&dr_req, 2);
    assert_eq!(dr_resp.topics[0].partitions[0].error_code, 0);
    assert_eq!(dr_resp.topics[0].partitions[0].low_watermark, 10);

    // DescribeLogDirs
    let log_req = DescribeLogDirsRequest::default();
    let log_resp = t.engine.handle_describe_log_dirs(&log_req, 2);
    assert_eq!(log_resp.results[0].error_code, 0);

    // DescribeProducers
    let mut dp_req = DescribeProducersRequest::default();
    let mut dp_topic =
        kafka_protocol::messages::describe_producers_request::TopicRequest::default();
    dp_topic.name = TopicName::from(StrBytes::from_static_str("t"));
    dp_topic.partition_indexes.push(0);
    dp_req.topics.push(dp_topic);

    let dp_resp = t.engine.handle_describe_producers(&dp_req, 0);
    assert_eq!(dp_resp.topics[0].partitions[0].error_code, 0);

    // SaslHandshake & SaslAuthenticate
    let mut sasl_req = SaslHandshakeRequest::default();
    sasl_req.mechanism = StrBytes::from_static_str("PLAIN");
    let sasl_resp = t.engine.handle_sasl_handshake(&sasl_req, 1);
    assert_eq!(sasl_resp.error_code, 0);
    assert!(sasl_resp.mechanisms.iter().any(|m| m.as_str() == "PLAIN"));

    let auth_req = SaslAuthenticateRequest::default();
    let auth_resp = t.engine.handle_sasl_authenticate(&auth_req, 2);
    assert_eq!(auth_resp.error_code, 0);
}
