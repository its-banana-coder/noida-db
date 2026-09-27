use bytes::BytesMut;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    FetchRequest, FetchResponse, InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest,
    ListOffsetsResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::io::{Read, Write};
use std::net::TcpStream;

fn send_request<Req: Encodable, Resp: Decodable>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    req: &Req,
) -> Resp {
    let mut header = RequestHeader::default();
    header.request_api_key = api_key as i16;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;
    header.client_id = client_id.map(|s| StrBytes::from_string(s.to_string()));

    let header_version = api_key.request_header_version(api_version);

    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version).unwrap();
    req.encode(&mut buf, api_version).unwrap();

    let len = (buf.len() as u32).to_be_bytes();
    stream.write_all(&len).unwrap();
    stream.write_all(&buf).unwrap();
    stream.flush().unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).unwrap();

    let mut bytes_mut = BytesMut::from(&resp_buf[..]);
    let resp_header_ver = api_key.response_header_version(api_version);
    let _resp_header = ResponseHeader::decode(&mut bytes_mut, resp_header_ver).unwrap();

    Resp::decode(&mut bytes_mut, api_version).unwrap()
}

#[test]
fn test_kafka_milestone_1_and_2() {
    let addr = noida::kafka::spawn("127.0.0.1:0").unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();

    // 1. ApiVersions
    let api_ver_req = ApiVersionsRequest::default();
    let api_ver_resp: ApiVersionsResponse =
        send_request(&mut stream, ApiKey::ApiVersions, 3, 1, Some("test-client"), &api_ver_req);
    assert_eq!(api_ver_resp.error_code, 0);
    assert!(!api_ver_resp.api_keys.is_empty());

    // 2. CreateTopics
    let mut create_req = CreateTopicsRequest::default();
    let mut creatable_topic = CreatableTopic::default();
    let topic_name = TopicName::from(StrBytes::from_string("milestone2-topic".to_string()));
    creatable_topic.name = topic_name.clone();
    creatable_topic.num_partitions = 1;
    creatable_topic.replication_factor = 1;
    create_req.topics.push(creatable_topic);

    let create_resp: CreateTopicsResponse =
        send_request(&mut stream, ApiKey::CreateTopics, 5, 2, Some("test-client"), &create_req);
    let created_topic = create_resp.topics.iter().find(|t| t.name == topic_name).unwrap();
    assert_eq!(created_topic.error_code, 0);

    // 3. Metadata
    let mut meta_req = MetadataRequest::default();
    let mut topic_req = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
    topic_req.name = Some(topic_name.clone());
    meta_req.topics = Some(vec![topic_req]);

    let meta_resp: MetadataResponse =
        send_request(&mut stream, ApiKey::Metadata, 9, 3, Some("test-client"), &meta_req);
    assert_eq!(meta_resp.brokers.len(), 1);

    // 4. InitProducerId
    let init_pid_req = InitProducerIdRequest::default();
    let init_pid_resp: InitProducerIdResponse =
        send_request(&mut stream, ApiKey::InitProducerId, 4, 4, Some("test-client"), &init_pid_req);
    assert_eq!(init_pid_resp.error_code, 0);
    assert!(init_pid_resp.producer_id.0 >= 1000);

    // 5. Produce
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 1000;
    let mut topic_prod = TopicProduceData::default();
    topic_prod.name = topic_name.clone();
    let mut part_prod = PartitionProduceData::default();
    part_prod.index = 0;
    part_prod.records = Some(bytes::Bytes::from("hello kafka record batch payload"));
    topic_prod.partition_data.push(part_prod);
    produce_req.topic_data.push(topic_prod);

    let produce_resp: ProduceResponse =
        send_request(&mut stream, ApiKey::Produce, 8, 5, Some("test-client"), &produce_req);
    let prod_topic = produce_resp.responses.iter().find(|t| t.name == topic_name).unwrap();
    let prod_part = &prod_topic.partition_responses[0];
    assert_eq!(prod_part.error_code, 0);
    assert_eq!(prod_part.base_offset, 0);

    // 6. ListOffsets
    let mut list_offsets_req = ListOffsetsRequest::default();
    let mut list_topic = ListOffsetsTopic::default();
    list_topic.name = topic_name.clone();
    let mut list_part = ListOffsetsPartition::default();
    list_part.partition_index = 0;
    list_part.timestamp = -1; // Latest
    list_topic.partitions.push(list_part);
    list_offsets_req.topics.push(list_topic);

    let list_offsets_resp: ListOffsetsResponse = send_request(
        &mut stream,
        ApiKey::ListOffsets,
        6,
        6,
        Some("test-client"),
        &list_offsets_req,
    );
    let offset_part = &list_offsets_resp.topics[0].partitions[0];
    assert_eq!(offset_part.error_code, 0);
    assert_eq!(offset_part.offset, 1);

    // 7. Fetch
    let mut fetch_req = FetchRequest::default();
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = topic_name.clone();
    let mut fetch_part = FetchPartition::default();
    fetch_part.partition = 0;
    fetch_part.fetch_offset = 0;
    fetch_topic.partitions.push(fetch_part);
    fetch_req.topics.push(fetch_topic);

    let fetch_resp: FetchResponse =
        send_request(&mut stream, ApiKey::Fetch, 11, 7, Some("test-client"), &fetch_req);
    let fetched_part = &fetch_resp.responses[0].partitions[0];
    assert_eq!(fetched_part.error_code, 0);
    assert_eq!(fetched_part.high_watermark, 1);
    assert!(fetched_part.records.is_some());

    // 8. CreatePartitions (increase from 1 to 3 partitions)
    let mut create_part_req = kafka_protocol::messages::CreatePartitionsRequest::default();
    let mut cp_topic =
        kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic::default();
    cp_topic.name = topic_name.clone();
    cp_topic.count = 3;
    create_part_req.topics.push(cp_topic);

    let create_part_resp: kafka_protocol::messages::CreatePartitionsResponse = send_request(
        &mut stream,
        ApiKey::CreatePartitions,
        2,
        8,
        Some("test-client"),
        &create_part_req,
    );
    assert_eq!(create_part_resp.results[0].error_code, 0);

    // 9. DeleteTopics
    let mut del_topics_req = kafka_protocol::messages::DeleteTopicsRequest::default();
    del_topics_req.topic_names.push(topic_name.clone());

    let del_topics_resp: kafka_protocol::messages::DeleteTopicsResponse =
        send_request(&mut stream, ApiKey::DeleteTopics, 4, 9, Some("test-client"), &del_topics_req);
    assert_eq!(del_topics_resp.responses[0].error_code, 0);

    // 10. FindCoordinator
    let mut find_coord_req = kafka_protocol::messages::FindCoordinatorRequest::default();
    find_coord_req.key = StrBytes::from_string("test-consumer-group".to_string());

    let find_coord_resp: kafka_protocol::messages::FindCoordinatorResponse = send_request(
        &mut stream,
        ApiKey::FindCoordinator,
        3,
        10,
        Some("test-client"),
        &find_coord_req,
    );
    assert_eq!(find_coord_resp.error_code, 0);

    // 11. JoinGroup
    let mut join_req = kafka_protocol::messages::JoinGroupRequest::default();
    join_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    join_req.protocol_type = StrBytes::from_string("consumer".to_string());

    let join_resp: kafka_protocol::messages::JoinGroupResponse =
        send_request(&mut stream, ApiKey::JoinGroup, 5, 11, Some("test-client"), &join_req);
    assert_eq!(join_resp.error_code, 0);

    // 12. SyncGroup
    let mut sync_req = kafka_protocol::messages::SyncGroupRequest::default();
    sync_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    sync_req.member_id = join_resp.member_id;

    let sync_resp: kafka_protocol::messages::SyncGroupResponse =
        send_request(&mut stream, ApiKey::SyncGroup, 3, 12, Some("test-client"), &sync_req);
    assert_eq!(sync_resp.error_code, 0);

    // 13. OffsetCommit & OffsetFetch
    let mut commit_req = kafka_protocol::messages::OffsetCommitRequest::default();
    commit_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    let mut oc_topic =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default();
    oc_topic.name = topic_name.clone();
    let mut oc_part =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default();
    oc_part.partition_index = 0;
    oc_part.committed_offset = 42;
    oc_topic.partitions.push(oc_part);
    commit_req.topics.push(oc_topic);

    let commit_resp: kafka_protocol::messages::OffsetCommitResponse =
        send_request(&mut stream, ApiKey::OffsetCommit, 5, 13, Some("test-client"), &commit_req);
    assert_eq!(commit_resp.topics[0].partitions[0].error_code, 0);

    let mut fetch_off_req = kafka_protocol::messages::OffsetFetchRequest::default();
    fetch_off_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    let mut of_topic =
        kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default();
    of_topic.name = topic_name.clone();
    of_topic.partition_indexes.push(0);
    fetch_off_req.topics = Some(vec![of_topic]);

    let fetch_off_resp: kafka_protocol::messages::OffsetFetchResponse =
        send_request(&mut stream, ApiKey::OffsetFetch, 5, 14, Some("test-client"), &fetch_off_req);
    assert_eq!(fetch_off_resp.topics[0].partitions[0].committed_offset, 42);

    // 14. ListGroups & DescribeGroups
    let list_groups_req = kafka_protocol::messages::ListGroupsRequest::default();
    let list_groups_resp: kafka_protocol::messages::ListGroupsResponse =
        send_request(&mut stream, ApiKey::ListGroups, 2, 15, Some("test-client"), &list_groups_req);
    assert_eq!(list_groups_resp.error_code, 0);

    let mut desc_groups_req = kafka_protocol::messages::DescribeGroupsRequest::default();
    desc_groups_req.groups.push(kafka_protocol::messages::GroupId(StrBytes::from_string(
        "test-consumer-group".to_string(),
    )));
    let desc_groups_resp: kafka_protocol::messages::DescribeGroupsResponse = send_request(
        &mut stream,
        ApiKey::DescribeGroups,
        3,
        16,
        Some("test-client"),
        &desc_groups_req,
    );
    assert_eq!(desc_groups_resp.groups[0].error_code, 0);

    // 15. DescribeConfigs & DescribeCluster
    let mut desc_cfg_req = kafka_protocol::messages::DescribeConfigsRequest::default();
    let mut cfg_res =
        kafka_protocol::messages::describe_configs_request::DescribeConfigsResource::default();
    cfg_res.resource_type = 2; // Topic
    cfg_res.resource_name = StrBytes::from_string("milestone2-topic".to_string());
    desc_cfg_req.resources.push(cfg_res);
    let desc_cfg_resp: kafka_protocol::messages::DescribeConfigsResponse = send_request(
        &mut stream,
        ApiKey::DescribeConfigs,
        2,
        17,
        Some("test-client"),
        &desc_cfg_req,
    );
    assert_eq!(desc_cfg_resp.results[0].error_code, 0);

    let desc_cluster_req = kafka_protocol::messages::DescribeClusterRequest::default();
    let desc_cluster_resp: kafka_protocol::messages::DescribeClusterResponse = send_request(
        &mut stream,
        ApiKey::DescribeCluster,
        0,
        18,
        Some("test-client"),
        &desc_cluster_req,
    );
    assert_eq!(desc_cluster_resp.error_code, 0);

    // 16. Transactions (AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn)
    let mut add_parts_txn_req = kafka_protocol::messages::AddPartitionsToTxnRequest::default();
    add_parts_txn_req.v3_and_below_transactional_id =
        kafka_protocol::messages::TransactionalId(StrBytes::from_string("tx-1".to_string()));
    let mut txn_topic =
        kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic::default();
    txn_topic.name = topic_name.clone();
    txn_topic.partitions.push(0);
    add_parts_txn_req.v3_and_below_topics = vec![txn_topic];

    let add_parts_txn_resp: kafka_protocol::messages::AddPartitionsToTxnResponse = send_request(
        &mut stream,
        ApiKey::AddPartitionsToTxn,
        1,
        19,
        Some("test-client"),
        &add_parts_txn_req,
    );
    assert_eq!(
        add_parts_txn_resp.results_by_topic_v3_and_below[0].results_by_partition[0]
            .partition_error_code,
        0
    );

    let mut add_offs_txn_req = kafka_protocol::messages::AddOffsetsToTxnRequest::default();
    add_offs_txn_req.transactional_id =
        kafka_protocol::messages::TransactionalId(StrBytes::from_string("tx-1".to_string()));
    add_offs_txn_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));

    let add_offs_txn_resp: kafka_protocol::messages::AddOffsetsToTxnResponse = send_request(
        &mut stream,
        ApiKey::AddOffsetsToTxn,
        1,
        20,
        Some("test-client"),
        &add_offs_txn_req,
    );
    assert_eq!(add_offs_txn_resp.error_code, 0);

    let mut txn_commit_req = kafka_protocol::messages::TxnOffsetCommitRequest::default();
    txn_commit_req.transactional_id =
        kafka_protocol::messages::TransactionalId(StrBytes::from_string("tx-1".to_string()));
    txn_commit_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    let mut toc_topic =
        kafka_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequestTopic::default();
    toc_topic.name = topic_name.clone();
    let mut toc_part = kafka_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequestPartition::default();
    toc_part.partition_index = 0;
    toc_part.committed_offset = 100;
    toc_topic.partitions.push(toc_part);
    txn_commit_req.topics.push(toc_topic);

    let txn_commit_resp: kafka_protocol::messages::TxnOffsetCommitResponse = send_request(
        &mut stream,
        ApiKey::TxnOffsetCommit,
        1,
        21,
        Some("test-client"),
        &txn_commit_req,
    );
    assert_eq!(txn_commit_resp.topics[0].partitions[0].error_code, 0);

    let mut end_txn_req = kafka_protocol::messages::EndTxnRequest::default();
    end_txn_req.transactional_id =
        kafka_protocol::messages::TransactionalId(StrBytes::from_string("tx-1".to_string()));
    end_txn_req.committed = true;

    let end_txn_resp: kafka_protocol::messages::EndTxnResponse =
        send_request(&mut stream, ApiKey::EndTxn, 1, 22, Some("test-client"), &end_txn_req);
    assert_eq!(end_txn_resp.error_code, 0);

    // 17. DeleteRecords & OffsetDelete
    let mut del_recs_req = kafka_protocol::messages::DeleteRecordsRequest::default();
    let mut dr_topic =
        kafka_protocol::messages::delete_records_request::DeleteRecordsTopic::default();
    dr_topic.name = topic_name.clone();
    let mut dr_part =
        kafka_protocol::messages::delete_records_request::DeleteRecordsPartition::default();
    dr_part.partition_index = 0;
    dr_part.offset = 0;
    dr_topic.partitions.push(dr_part);
    del_recs_req.topics = vec![dr_topic];

    let del_recs_resp: kafka_protocol::messages::DeleteRecordsResponse =
        send_request(&mut stream, ApiKey::DeleteRecords, 2, 23, Some("test-client"), &del_recs_req);
    assert_eq!(del_recs_resp.topics[0].partitions[0].error_code, 0);

    // 18. LogDirs & SASL Handshake
    let desc_log_dirs_req = kafka_protocol::messages::DescribeLogDirsRequest::default();
    let desc_log_dirs_resp: kafka_protocol::messages::DescribeLogDirsResponse = send_request(
        &mut stream,
        ApiKey::DescribeLogDirs,
        1,
        24,
        Some("test-client"),
        &desc_log_dirs_req,
    );
    assert_eq!(desc_log_dirs_resp.results[0].error_code, 0);

    let mut sasl_hs_req = kafka_protocol::messages::SaslHandshakeRequest::default();
    sasl_hs_req.mechanism = StrBytes::from_string("PLAIN".to_string());
    let sasl_hs_resp: kafka_protocol::messages::SaslHandshakeResponse =
        send_request(&mut stream, ApiKey::SaslHandshake, 0, 25, Some("test-client"), &sasl_hs_req);
    assert_eq!(sasl_hs_resp.error_code, 0);
}
