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
    assert_eq!(join_resp.error_code, 79); // MEMBER_ID_REQUIRED
    assert!(!join_resp.member_id.is_empty());

    // Complete join with assigned member_id
    join_req.member_id = join_resp.member_id.clone();
    let join_resp: kafka_protocol::messages::JoinGroupResponse =
        send_request(&mut stream, ApiKey::JoinGroup, 5, 12, Some("test-client"), &join_req);
    assert_eq!(join_resp.error_code, 0);

    // 12. SyncGroup
    let mut sync_req = kafka_protocol::messages::SyncGroupRequest::default();
    sync_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    sync_req.member_id = join_resp.member_id.clone();
    sync_req.generation_id = join_resp.generation_id;

    let sync_resp: kafka_protocol::messages::SyncGroupResponse =
        send_request(&mut stream, ApiKey::SyncGroup, 3, 12, Some("test-client"), &sync_req);
    assert_eq!(sync_resp.error_code, 0);

    // 13. OffsetCommit & OffsetFetch
    let mut commit_req = kafka_protocol::messages::OffsetCommitRequest::default();
    commit_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_string("test-consumer-group".to_string()));
    commit_req.member_id = join_resp.member_id.clone();
    commit_req.generation_id_or_member_epoch = join_resp.generation_id;
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
    cfg_res.resource_name = StrBytes::from_string("__consumer_offsets".to_string());
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

fn make_v2_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_count: i32,
) -> Vec<u8> {
    let mut buf = vec![0u8; 61];
    buf[0..8].copy_from_slice(&0i64.to_be_bytes());
    buf[8..12].copy_from_slice(&50i32.to_be_bytes());
    buf[12..16].copy_from_slice(&0i32.to_be_bytes());
    buf[16] = 2; // magic 2
    buf[17..21].copy_from_slice(&0u32.to_be_bytes());
    buf[21..23].copy_from_slice(&0i16.to_be_bytes());
    let delta = (records_count - 1).max(0);
    buf[23..27].copy_from_slice(&delta.to_be_bytes());
    buf[27..35].copy_from_slice(&0i64.to_be_bytes());
    buf[35..43].copy_from_slice(&0i64.to_be_bytes());
    buf[43..51].copy_from_slice(&producer_id.to_be_bytes());
    buf[51..53].copy_from_slice(&producer_epoch.to_be_bytes());
    buf[53..57].copy_from_slice(&base_sequence.to_be_bytes());
    buf[57..61].copy_from_slice(&records_count.to_be_bytes());
    buf.extend_from_slice(b"sample-payload");
    buf
}

#[test]
fn test_kafka_idempotent_producer_network() {
    let addr = noida::kafka::spawn("127.0.0.1:0").unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();

    // 1. Create topic
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = TopicName::from(StrBytes::from_static_str("idemp-net"));
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    let create_resp: CreateTopicsResponse =
        send_request(&mut stream, ApiKey::CreateTopics, 5, 1, Some("idemp-client"), &create_req);
    assert_eq!(create_resp.topics[0].error_code, 0);

    // 2. InitProducerId
    let init_req = InitProducerIdRequest::default();
    let init_resp: InitProducerIdResponse =
        send_request(&mut stream, ApiKey::InitProducerId, 4, 2, Some("idemp-client"), &init_req);
    assert_eq!(init_resp.error_code, 0);
    let pid = init_resp.producer_id.0;
    let epoch = init_resp.producer_epoch;

    // 3. Produce sequence 0
    let b0 = make_v2_batch(pid, epoch, 0, 1);
    let mut p_req0 = ProduceRequest::default();
    let mut td0 = TopicProduceData::default();
    td0.name = TopicName::from(StrBytes::from_static_str("idemp-net"));
    let mut pd0 = PartitionProduceData::default();
    pd0.index = 0;
    pd0.records = Some(bytes::Bytes::from(b0.clone()));
    td0.partition_data.push(pd0);
    p_req0.topic_data.push(td0);

    let p_resp0: ProduceResponse =
        send_request(&mut stream, ApiKey::Produce, 8, 3, Some("idemp-client"), &p_req0);
    assert_eq!(p_resp0.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(p_resp0.responses[0].partition_responses[0].base_offset, 0);

    // 4. Duplicate produce with sequence 0
    let p_resp0_dup: ProduceResponse =
        send_request(&mut stream, ApiKey::Produce, 8, 4, Some("idemp-client"), &p_req0);
    assert_eq!(p_resp0_dup.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(p_resp0_dup.responses[0].partition_responses[0].base_offset, 0);

    // 5. Sequence 1
    let b1 = make_v2_batch(pid, epoch, 1, 1);
    let mut p_req1 = ProduceRequest::default();
    let mut td1 = TopicProduceData::default();
    td1.name = TopicName::from(StrBytes::from_static_str("idemp-net"));
    let mut pd1 = PartitionProduceData::default();
    pd1.index = 0;
    pd1.records = Some(bytes::Bytes::from(b1));
    td1.partition_data.push(pd1);
    p_req1.topic_data.push(td1);

    let p_resp1: ProduceResponse =
        send_request(&mut stream, ApiKey::Produce, 8, 5, Some("idemp-client"), &p_req1);
    assert_eq!(p_resp1.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(p_resp1.responses[0].partition_responses[0].base_offset, 1);

    // 6. Sequence gap (sequence 10) -> OUT_OF_ORDER_SEQUENCE_NUMBER (45)
    let b10 = make_v2_batch(pid, epoch, 10, 1);
    let mut p_req10 = ProduceRequest::default();
    let mut td10 = TopicProduceData::default();
    td10.name = TopicName::from(StrBytes::from_static_str("idemp-net"));
    let mut pd10 = PartitionProduceData::default();
    pd10.index = 0;
    pd10.records = Some(bytes::Bytes::from(b10));
    td10.partition_data.push(pd10);
    p_req10.topic_data.push(td10);

    let p_resp10: ProduceResponse =
        send_request(&mut stream, ApiKey::Produce, 8, 6, Some("idemp-client"), &p_req10);
    assert_eq!(p_resp10.responses[0].partition_responses[0].error_code, 45);
}

#[test]
fn test_kafka_consumer_group_scenario_b_rebalance() {
    let addr = noida::kafka::spawn("127.0.0.1:0").unwrap();
    let mut s1 = TcpStream::connect(addr).unwrap();
    let mut s2 = TcpStream::connect(addr).unwrap();

    let topic_name = TopicName::from(StrBytes::from_static_str("scen-b-topic"));

    // 1. Create 3-partition topic
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = topic_name.clone();
    topic.num_partitions = 3;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    let create_resp: CreateTopicsResponse =
        send_request(&mut s1, ApiKey::CreateTopics, 5, 1, Some("client1"), &create_req);
    assert_eq!(create_resp.topics[0].error_code, 0);

    // 2. Consumer 1 joins
    let mut c1_join = kafka_protocol::messages::JoinGroupRequest::default();
    c1_join.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    c1_join.protocol_type = StrBytes::from_static_str("consumer");
    let mut proto =
        kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default();
    proto.name = StrBytes::from_static_str("range");
    proto.metadata = bytes::Bytes::from("c1-metadata");
    c1_join.protocols.push(proto);

    let c1_j1: kafka_protocol::messages::JoinGroupResponse =
        send_request(&mut s1, ApiKey::JoinGroup, 5, 2, Some("c1"), &c1_join);
    assert_eq!(c1_j1.error_code, 79); // MEMBER_ID_REQUIRED

    c1_join.member_id = c1_j1.member_id.clone();
    let c1_j2: kafka_protocol::messages::JoinGroupResponse =
        send_request(&mut s1, ApiKey::JoinGroup, 5, 3, Some("c1"), &c1_join);
    assert_eq!(c1_j2.error_code, 0);
    assert_eq!(c1_j2.generation_id, 1);
    let m1_id = c1_j2.member_id;

    // 3. Consumer 1 syncs (receives all 3 partitions [0, 1, 2])
    let mut c1_sync = kafka_protocol::messages::SyncGroupRequest::default();
    c1_sync.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    c1_sync.member_id = m1_id.clone();
    c1_sync.generation_id = 1;
    let mut assign =
        kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment::default();
    assign.member_id = m1_id.clone();
    assign.assignment = bytes::Bytes::from("p0,p1,p2");
    c1_sync.assignments.push(assign);

    let c1_s: kafka_protocol::messages::SyncGroupResponse =
        send_request(&mut s1, ApiKey::SyncGroup, 3, 4, Some("c1"), &c1_sync);
    assert_eq!(c1_s.error_code, 0);
    assert_eq!(c1_s.assignment.as_ref(), b"p0,p1,p2");

    // 4. Consumer 1 commits offsets for partitions 0, 1, 2
    let mut oc_req = kafka_protocol::messages::OffsetCommitRequest::default();
    oc_req.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    oc_req.member_id = m1_id.clone();
    oc_req.generation_id_or_member_epoch = 1;
    let mut oc_top =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default();
    oc_top.name = topic_name.clone();
    for p in 0..3 {
        let mut ocp =
            kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default(
            );
        ocp.partition_index = p;
        ocp.committed_offset = 100 + p as i64 * 50;
        oc_top.partitions.push(ocp);
    }
    oc_req.topics.push(oc_top);

    let oc_resp: kafka_protocol::messages::OffsetCommitResponse =
        send_request(&mut s1, ApiKey::OffsetCommit, 5, 5, Some("c1"), &oc_req);
    assert_eq!(oc_resp.topics[0].partitions[0].error_code, 0);

    // 5. Consumer 2 joins the group (triggers rebalance)
    let mut c2_join = kafka_protocol::messages::JoinGroupRequest::default();
    c2_join.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    c2_join.protocol_type = StrBytes::from_static_str("consumer");
    let mut proto2 =
        kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol::default();
    proto2.name = StrBytes::from_static_str("range");
    proto2.metadata = bytes::Bytes::from("c2-metadata");
    c2_join.protocols.push(proto2);

    let c2_j1: kafka_protocol::messages::JoinGroupResponse =
        send_request(&mut s2, ApiKey::JoinGroup, 5, 6, Some("c2"), &c2_join);
    assert_eq!(c2_j1.error_code, 79); // MEMBER_ID_REQUIRED

    c2_join.member_id = c2_j1.member_id.clone();
    let _ = send_request::<_, kafka_protocol::messages::JoinGroupResponse>(
        &mut s2,
        ApiKey::JoinGroup,
        5,
        7,
        Some("c2"),
        &c2_join,
    );

    // 6. Consumer 1 heartbeats with old generation -> notices rebalance
    let mut c1_hb = kafka_protocol::messages::HeartbeatRequest::default();
    c1_hb.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    c1_hb.member_id = m1_id.clone();
    c1_hb.generation_id = 1;
    let hb_resp: kafka_protocol::messages::HeartbeatResponse =
        send_request(&mut s1, ApiKey::Heartbeat, 4, 8, Some("c1"), &c1_hb);
    assert!(hb_resp.error_code == 22 || hb_resp.error_code == 27);

    // 7. Consumer 1 leaves the group
    let mut leave_req = kafka_protocol::messages::LeaveGroupRequest::default();
    leave_req.group_id =
        kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    leave_req.member_id = m1_id;
    let leave_resp: kafka_protocol::messages::LeaveGroupResponse =
        send_request(&mut s1, ApiKey::LeaveGroup, 2, 9, Some("c1"), &leave_req);
    assert_eq!(leave_resp.error_code, 0);

    // 8. Consumer 2 fetches committed offsets and resumes consumption
    let mut of_req = kafka_protocol::messages::OffsetFetchRequest::default();
    of_req.group_id = kafka_protocol::messages::GroupId(StrBytes::from_static_str("scen-b-group"));
    let mut of_top =
        kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default();
    of_top.name = topic_name;
    of_top.partition_indexes = vec![0, 1, 2];
    of_req.topics = Some(vec![of_top]);

    let of_resp: kafka_protocol::messages::OffsetFetchResponse =
        send_request(&mut s2, ApiKey::OffsetFetch, 5, 10, Some("c2"), &of_req);
    let parts = &of_resp.topics[0].partitions;
    assert_eq!(parts[0].committed_offset, 100);
    assert_eq!(parts[1].committed_offset, 150);
    assert_eq!(parts[2].committed_offset, 200);
}

#[test]
fn test_kafka_admin_describe_and_delete() {
    let addr = noida::kafka::spawn("127.0.0.1:0").unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();

    let topic_name = TopicName::from(StrBytes::from_static_str("admin-net-topic"));

    // 1. Create topic with 2 partitions
    let mut create_req = CreateTopicsRequest::default();
    let mut topic = CreatableTopic::default();
    topic.name = topic_name.clone();
    topic.num_partitions = 2;
    topic.replication_factor = 1;
    create_req.topics.push(topic);
    let create_resp: CreateTopicsResponse =
        send_request(&mut stream, ApiKey::CreateTopics, 5, 1, Some("admin"), &create_req);
    assert_eq!(create_resp.topics[0].error_code, 0);

    // 2. DescribeConfigs
    let mut desc_cfg_req = kafka_protocol::messages::DescribeConfigsRequest::default();
    let mut res =
        kafka_protocol::messages::describe_configs_request::DescribeConfigsResource::default();
    res.resource_type = 2; // Topic
    res.resource_name = StrBytes::from_static_str("admin-net-topic");
    desc_cfg_req.resources.push(res);
    let desc_cfg_resp: kafka_protocol::messages::DescribeConfigsResponse =
        send_request(&mut stream, ApiKey::DescribeConfigs, 4, 2, Some("admin"), &desc_cfg_req);
    assert_eq!(desc_cfg_resp.results[0].error_code, 0);

    // 3. AlterConfigs
    let mut alter_req = kafka_protocol::messages::AlterConfigsRequest::default();
    let mut a_res =
        kafka_protocol::messages::alter_configs_request::AlterConfigsResource::default();
    a_res.resource_type = 2;
    a_res.resource_name = StrBytes::from_static_str("admin-net-topic");
    let mut cfg_entry = kafka_protocol::messages::alter_configs_request::AlterableConfig::default();
    cfg_entry.name = StrBytes::from_static_str("cleanup.policy");
    cfg_entry.value = Some(StrBytes::from_static_str("compact"));
    a_res.configs.push(cfg_entry);
    alter_req.resources.push(a_res);
    let alter_resp: kafka_protocol::messages::AlterConfigsResponse =
        send_request(&mut stream, ApiKey::AlterConfigs, 2, 3, Some("admin"), &alter_req);
    assert_eq!(alter_resp.responses[0].error_code, 0);

    // 4. CreatePartitions (expand 2 -> 4)
    let mut cp_req = kafka_protocol::messages::CreatePartitionsRequest::default();
    let mut cp_topic =
        kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic::default();
    cp_topic.name = topic_name.clone();
    cp_topic.count = 4;
    cp_req.topics.push(cp_topic);
    let cp_resp: kafka_protocol::messages::CreatePartitionsResponse =
        send_request(&mut stream, ApiKey::CreatePartitions, 2, 4, Some("admin"), &cp_req);
    assert_eq!(cp_resp.results[0].error_code, 0);

    // 5. DeleteTopics
    let mut del_req = kafka_protocol::messages::DeleteTopicsRequest::default();
    del_req.topic_names.push(topic_name);
    let del_resp: kafka_protocol::messages::DeleteTopicsResponse =
        send_request(&mut stream, ApiKey::DeleteTopics, 4, 5, Some("admin"), &del_req);
    assert_eq!(del_resp.responses[0].error_code, 0);
}
