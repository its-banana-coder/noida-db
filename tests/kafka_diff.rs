use bytes::BytesMut;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, GroupId, HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest,
    InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::env;
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
fn test_kafka_diff() {
    let ref_env = env::var("NOIDA_KAFKA_REF");
    let ref_addr = match ref_env {
        Ok(addr) if !addr.is_empty() => addr,
        _ => {
            println!("SKIPPED (no reference Kafka broker set in NOIDA_KAFKA_REF)");
            return;
        }
    };

    println!("Running Kafka diff tests against reference broker at {}", ref_addr);

    let mut ref_stream = TcpStream::connect(&ref_addr).expect("connect to reference broker");
    let noida_addr = noida::kafka::spawn("127.0.0.1:0").expect("spawn noida kafka");
    let mut noida_stream = TcpStream::connect(noida_addr).expect("connect to noida broker");

    let mut compared_count = 0;
    let mut cid = 1;

    // 1. ApiVersions at v0, v1, v2, v3
    for v in 0..=3 {
        // v3 names the client; both brokers reject an empty name (42).
        let mut req = ApiVersionsRequest::default();
        req.client_software_name = StrBytes::from_static_str("noida-diff");
        req.client_software_version = StrBytes::from_static_str("1.0");
        let ref_resp: ApiVersionsResponse =
            send_request(&mut ref_stream, ApiKey::ApiVersions, v, cid, None, &req);
        let noida_resp: ApiVersionsResponse =
            send_request(&mut noida_stream, ApiKey::ApiVersions, v, cid, None, &req);
        cid += 1;

        assert_eq!(ref_resp.error_code, noida_resp.error_code);
        compared_count += 1;
    }

    // 2. CreateTopics happy path & error cases
    // Unique per run: the reference broker keeps state between runs.
    let run_id =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    let grp = format!("diff-grp-{run_id}");
    let diff_topic = TopicName::from(StrBytes::from_string(format!("diff-topic-{run_id}")));
    let mut create_req = CreateTopicsRequest::default();
    let mut ct = CreatableTopic::default();
    ct.name = diff_topic.clone();
    ct.num_partitions = 1;
    ct.replication_factor = 1;
    create_req.topics.push(ct);

    let ref_ct: CreateTopicsResponse =
        send_request(&mut ref_stream, ApiKey::CreateTopics, 5, cid, Some("diff"), &create_req);
    let noida_ct: CreateTopicsResponse =
        send_request(&mut noida_stream, ApiKey::CreateTopics, 5, cid, Some("diff"), &create_req);
    cid += 1;
    assert_eq!(ref_ct.topics[0].error_code, noida_ct.topics[0].error_code);
    compared_count += 1;

    // Duplicate topic -> TOPIC_ALREADY_EXISTS (36)
    let ref_dup: CreateTopicsResponse =
        send_request(&mut ref_stream, ApiKey::CreateTopics, 5, cid, Some("diff"), &create_req);
    let noida_dup: CreateTopicsResponse =
        send_request(&mut noida_stream, ApiKey::CreateTopics, 5, cid, Some("diff"), &create_req);
    cid += 1;
    assert_eq!(ref_dup.topics[0].error_code, 36);
    assert_eq!(ref_dup.topics[0].error_code, noida_dup.topics[0].error_code);
    compared_count += 1;

    // 3. Metadata with and without auto-create
    let mut meta_req = MetadataRequest::default();
    let mut mt = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
    mt.name = Some(diff_topic.clone());
    meta_req.topics = Some(vec![mt]);

    let ref_meta: MetadataResponse =
        send_request(&mut ref_stream, ApiKey::Metadata, 9, cid, Some("diff"), &meta_req);
    let noida_meta: MetadataResponse =
        send_request(&mut noida_stream, ApiKey::Metadata, 9, cid, Some("diff"), &meta_req);
    cid += 1;
    assert_eq!(ref_meta.topics[0].error_code, noida_meta.topics[0].error_code);
    assert_eq!(ref_meta.topics[0].partitions.len(), noida_meta.topics[0].partitions.len());
    compared_count += 1;

    // 4. Produce and Fetch
    let mut prod_req = ProduceRequest::default();
    prod_req.acks = 1;
    let mut t_prod = TopicProduceData::default();
    t_prod.name = diff_topic.clone();
    let mut p_prod = PartitionProduceData::default();
    p_prod.index = 0;
    p_prod.records = Some(bytes::Bytes::from("diff test record content"));
    t_prod.partition_data.push(p_prod);
    prod_req.topic_data.push(t_prod);

    let ref_p: ProduceResponse =
        send_request(&mut ref_stream, ApiKey::Produce, 8, cid, Some("diff"), &prod_req);
    let noida_p: ProduceResponse =
        send_request(&mut noida_stream, ApiKey::Produce, 8, cid, Some("diff"), &prod_req);
    cid += 1;
    assert_eq!(
        ref_p.responses[0].partition_responses[0].error_code,
        noida_p.responses[0].partition_responses[0].error_code
    );
    // Bytes that aren't a record batch: CORRUPT_MESSAGE on both.
    assert_eq!(ref_p.responses[0].partition_responses[0].error_code, 2);
    compared_count += 1;

    // A real batch.
    prod_req.topic_data[0].partition_data[0].records =
        Some(bytes::Bytes::from(record_batch(b"diff test record content")));
    let ref_p: ProduceResponse =
        send_request(&mut ref_stream, ApiKey::Produce, 8, cid, Some("diff"), &prod_req);
    let noida_p: ProduceResponse =
        send_request(&mut noida_stream, ApiKey::Produce, 8, cid, Some("diff"), &prod_req);
    cid += 1;
    for field in [
        |p: &ProduceResponse| p.responses[0].partition_responses[0].error_code as i64,
        |p: &ProduceResponse| p.responses[0].partition_responses[0].base_offset,
        |p: &ProduceResponse| p.responses[0].partition_responses[0].log_append_time_ms,
        |p: &ProduceResponse| p.responses[0].partition_responses[0].log_start_offset,
    ] {
        assert_eq!(field(&ref_p), field(&noida_p));
    }
    compared_count += 1;

    // Fetch offset 0
    let mut fetch_req = FetchRequest::default();
    let mut f_topic = FetchTopic::default();
    f_topic.topic = diff_topic.clone();
    let mut f_part = FetchPartition::default();
    f_part.partition = 0;
    f_part.fetch_offset = 0;
    f_topic.partitions.push(f_part);
    fetch_req.topics.push(f_topic);

    let ref_f: FetchResponse =
        send_request(&mut ref_stream, ApiKey::Fetch, 11, cid, Some("diff"), &fetch_req);
    let noida_f: FetchResponse =
        send_request(&mut noida_stream, ApiKey::Fetch, 11, cid, Some("diff"), &fetch_req);
    cid += 1;
    assert_eq!(
        ref_f.responses[0].partitions[0].error_code,
        noida_f.responses[0].partitions[0].error_code
    );
    assert_eq!(
        ref_f.responses[0].partitions[0].high_watermark,
        noida_f.responses[0].partitions[0].high_watermark
    );
    compared_count += 1;

    // Fetch offset out of range -> OFFSET_OUT_OF_RANGE (1)
    let mut fetch_oor = FetchRequest::default();
    let mut f_topic_oor = FetchTopic::default();
    f_topic_oor.topic = diff_topic.clone();
    let mut f_part_oor = FetchPartition::default();
    f_part_oor.partition = 0;
    f_part_oor.fetch_offset = 999;
    f_topic_oor.partitions.push(f_part_oor);
    fetch_oor.topics.push(f_topic_oor);

    let ref_foor: FetchResponse =
        send_request(&mut ref_stream, ApiKey::Fetch, 11, cid, Some("diff"), &fetch_oor);
    let noida_foor: FetchResponse =
        send_request(&mut noida_stream, ApiKey::Fetch, 11, cid, Some("diff"), &fetch_oor);
    cid += 1;
    assert_eq!(ref_foor.responses[0].partitions[0].error_code, 1);
    assert_eq!(
        ref_foor.responses[0].partitions[0].error_code,
        noida_foor.responses[0].partitions[0].error_code
    );
    compared_count += 1;

    // 5. ListOffsets
    let mut lo_req = ListOffsetsRequest::default();
    let mut lo_t = ListOffsetsTopic::default();
    lo_t.name = diff_topic.clone();
    let mut lo_p = ListOffsetsPartition::default();
    lo_p.partition_index = 0;
    lo_p.timestamp = -1; // latest
    lo_t.partitions.push(lo_p);
    lo_req.topics.push(lo_t);

    let ref_lo: ListOffsetsResponse =
        send_request(&mut ref_stream, ApiKey::ListOffsets, 6, cid, Some("diff"), &lo_req);
    let noida_lo: ListOffsetsResponse =
        send_request(&mut noida_stream, ApiKey::ListOffsets, 6, cid, Some("diff"), &lo_req);
    cid += 1;
    assert_eq!(
        ref_lo.topics[0].partitions[0].error_code,
        noida_lo.topics[0].partitions[0].error_code
    );
    assert_eq!(ref_lo.topics[0].partitions[0].offset, noida_lo.topics[0].partitions[0].offset);
    compared_count += 1;

    // 6. InitProducerId
    // The crate's default (an empty transactional id) is INVALID_REQUEST;
    // then a plain idempotent producer (null id).
    let mut init_pid_req = InitProducerIdRequest::default();
    for tx_id in [Some(Default::default()), None] {
        init_pid_req.transactional_id = tx_id;
        let ref_pid: InitProducerIdResponse = send_request(
            &mut ref_stream,
            ApiKey::InitProducerId,
            4,
            cid,
            Some("diff"),
            &init_pid_req,
        );
        let noida_pid: InitProducerIdResponse = send_request(
            &mut noida_stream,
            ApiKey::InitProducerId,
            4,
            cid,
            Some("diff"),
            &init_pid_req,
        );
        cid += 1;
        assert_eq!(ref_pid.error_code, noida_pid.error_code);
    }
    let ref_pid: InitProducerIdResponse =
        send_request(&mut ref_stream, ApiKey::InitProducerId, 4, cid, Some("diff"), &init_pid_req);
    let noida_pid: InitProducerIdResponse = send_request(
        &mut noida_stream,
        ApiKey::InitProducerId,
        4,
        cid,
        Some("diff"),
        &init_pid_req,
    );
    cid += 1;
    assert_eq!(ref_pid.error_code, noida_pid.error_code);
    compared_count += 1;

    // 7. FindCoordinator
    let mut fc_req = FindCoordinatorRequest::default();
    fc_req.key = StrBytes::from_string(grp.clone());
    let ref_fc: FindCoordinatorResponse =
        send_request(&mut ref_stream, ApiKey::FindCoordinator, 3, cid, Some("diff"), &fc_req);
    let noida_fc: FindCoordinatorResponse =
        send_request(&mut noida_stream, ApiKey::FindCoordinator, 3, cid, Some("diff"), &fc_req);
    cid += 1;
    assert_eq!(ref_fc.error_code, noida_fc.error_code);
    compared_count += 1;

    // 8. JoinGroup round trip (MEMBER_ID_REQUIRED 79)
    let mut j_req = JoinGroupRequest::default();
    j_req.group_id = GroupId(StrBytes::from_string(grp.clone()));
    j_req.protocol_type = StrBytes::from_static_str("consumer");
    let mut j_proto = JoinGroupRequestProtocol::default();
    j_proto.name = StrBytes::from_static_str("range");
    // A real ConsumerProtocolSubscription v0 (no topics, null user data).
    j_proto.metadata = bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]);
    j_req.protocols.push(j_proto);

    // Request checks: session timeout (the crate's default 0 is out of
    // range), empty group id, no protocols.
    let mut bad_cases = Vec::new();
    bad_cases.push(j_req.clone());
    j_req.session_timeout_ms = 10_000;
    j_req.rebalance_timeout_ms = 10_000;
    let mut no_group = j_req.clone();
    no_group.group_id = GroupId(StrBytes::from_static_str(""));
    bad_cases.push(no_group);
    let mut no_protocols = j_req.clone();
    no_protocols.protocols.clear();
    bad_cases.push(no_protocols);
    let mut too_long = j_req.clone();
    too_long.session_timeout_ms = 1_800_001;
    bad_cases.push(too_long);
    for bad in &bad_cases {
        let r: JoinGroupResponse =
            send_request(&mut ref_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), bad);
        let n: JoinGroupResponse =
            send_request(&mut noida_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), bad);
        cid += 1;
        assert_eq!(r.error_code, n.error_code, "JoinGroup {bad:?}");
        compared_count += 1;
    }

    let ref_j1: JoinGroupResponse =
        send_request(&mut ref_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), &j_req);
    let noida_j1: JoinGroupResponse =
        send_request(&mut noida_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), &j_req);
    cid += 1;
    assert_eq!(ref_j1.error_code, 79);
    assert_eq!(ref_j1.error_code, noida_j1.error_code);
    compared_count += 1;

    // Second join with assigned ID
    let mut ref_j_rejoin = j_req.clone();
    ref_j_rejoin.member_id = ref_j1.member_id.clone();
    let ref_j2: JoinGroupResponse =
        send_request(&mut ref_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), &ref_j_rejoin);

    let mut noida_j_rejoin = j_req.clone();
    noida_j_rejoin.member_id = noida_j1.member_id.clone();
    let noida_j2: JoinGroupResponse =
        send_request(&mut noida_stream, ApiKey::JoinGroup, 5, cid, Some("diff"), &noida_j_rejoin);
    cid += 1;
    assert_eq!(ref_j2.error_code, noida_j2.error_code);
    assert_eq!(ref_j2.generation_id, noida_j2.generation_id);
    compared_count += 1;

    // 9. SyncGroup
    let mut ref_sync = SyncGroupRequest::default();
    ref_sync.group_id = GroupId(StrBytes::from_string(grp.clone()));
    ref_sync.member_id = ref_j2.member_id.clone();
    ref_sync.generation_id = ref_j2.generation_id;
    let mut ref_assign = SyncGroupRequestAssignment::default();
    ref_assign.member_id = ref_j2.member_id.clone();
    ref_assign.assignment = bytes::Bytes::from("assignment");
    ref_sync.assignments.push(ref_assign);

    let mut noida_sync = SyncGroupRequest::default();
    noida_sync.group_id = GroupId(StrBytes::from_string(grp.clone()));
    noida_sync.member_id = noida_j2.member_id.clone();
    noida_sync.generation_id = noida_j2.generation_id;
    let mut noida_assign = SyncGroupRequestAssignment::default();
    noida_assign.member_id = noida_j2.member_id.clone();
    noida_assign.assignment = bytes::Bytes::from("assignment");
    noida_sync.assignments.push(noida_assign);

    let ref_s: SyncGroupResponse =
        send_request(&mut ref_stream, ApiKey::SyncGroup, 3, cid, Some("diff"), &ref_sync);
    let noida_s: SyncGroupResponse =
        send_request(&mut noida_stream, ApiKey::SyncGroup, 3, cid, Some("diff"), &noida_sync);
    cid += 1;
    assert_eq!(ref_s.error_code, noida_s.error_code);
    compared_count += 1;

    // 10. Heartbeat
    let mut ref_hb = HeartbeatRequest::default();
    ref_hb.group_id = GroupId(StrBytes::from_string(grp.clone()));
    ref_hb.member_id = ref_j2.member_id.clone();
    ref_hb.generation_id = ref_j2.generation_id;

    let mut noida_hb = HeartbeatRequest::default();
    noida_hb.group_id = GroupId(StrBytes::from_string(grp.clone()));
    noida_hb.member_id = noida_j2.member_id.clone();
    noida_hb.generation_id = noida_j2.generation_id;

    let ref_hb_resp: HeartbeatResponse =
        send_request(&mut ref_stream, ApiKey::Heartbeat, 4, cid, Some("diff"), &ref_hb);
    let noida_hb_resp: HeartbeatResponse =
        send_request(&mut noida_stream, ApiKey::Heartbeat, 4, cid, Some("diff"), &noida_hb);
    cid += 1;
    assert_eq!(ref_hb_resp.error_code, noida_hb_resp.error_code);
    compared_count += 1;

    // Heartbeat illegal generation -> ILLEGAL_GENERATION (22)
    ref_hb.generation_id = 999;
    noida_hb.generation_id = 999;
    let ref_hb_err: HeartbeatResponse =
        send_request(&mut ref_stream, ApiKey::Heartbeat, 4, cid, Some("diff"), &ref_hb);
    let noida_hb_err: HeartbeatResponse =
        send_request(&mut noida_stream, ApiKey::Heartbeat, 4, cid, Some("diff"), &noida_hb);
    cid += 1;
    assert_eq!(ref_hb_err.error_code, 22);
    assert_eq!(ref_hb_err.error_code, noida_hb_err.error_code);
    compared_count += 1;

    // 11. OffsetCommit and OffsetFetch
    let mut oc_req = OffsetCommitRequest::default();
    oc_req.group_id = GroupId(StrBytes::from_string(grp.clone()));
    let mut oc_t = OffsetCommitRequestTopic::default();
    oc_t.name = diff_topic.clone();
    let mut oc_p = OffsetCommitRequestPartition::default();
    oc_p.partition_index = 0;
    oc_p.committed_offset = 123;
    oc_t.partitions.push(oc_p);
    oc_req.topics.push(oc_t);

    let ref_oc: OffsetCommitResponse =
        send_request(&mut ref_stream, ApiKey::OffsetCommit, 5, cid, Some("diff"), &oc_req);
    let noida_oc: OffsetCommitResponse =
        send_request(&mut noida_stream, ApiKey::OffsetCommit, 5, cid, Some("diff"), &oc_req);
    cid += 1;
    assert_eq!(
        ref_oc.topics[0].partitions[0].error_code,
        noida_oc.topics[0].partitions[0].error_code
    );
    compared_count += 1;

    // OffsetFetch
    let mut of_req = OffsetFetchRequest::default();
    of_req.group_id = GroupId(StrBytes::from_string(grp.clone()));
    let mut of_t = OffsetFetchRequestTopic::default();
    of_t.name = diff_topic.clone();
    of_t.partition_indexes.push(0);
    of_req.topics = Some(vec![of_t]);

    let ref_of: OffsetFetchResponse =
        send_request(&mut ref_stream, ApiKey::OffsetFetch, 5, cid, Some("diff"), &of_req);
    let noida_of: OffsetFetchResponse =
        send_request(&mut noida_stream, ApiKey::OffsetFetch, 5, cid, Some("diff"), &of_req);
    cid += 1;
    assert_eq!(
        ref_of.topics[0].partitions[0].committed_offset,
        noida_of.topics[0].partitions[0].committed_offset
    );
    compared_count += 1;

    // 12. LeaveGroup
    let mut leave_ref = LeaveGroupRequest::default();
    leave_ref.group_id = GroupId(StrBytes::from_string(grp.clone()));
    leave_ref.member_id = ref_j2.member_id;

    let mut leave_noida = LeaveGroupRequest::default();
    leave_noida.group_id = GroupId(StrBytes::from_string(grp.clone()));
    leave_noida.member_id = noida_j2.member_id;

    let ref_l: LeaveGroupResponse =
        send_request(&mut ref_stream, ApiKey::LeaveGroup, 2, cid, Some("diff"), &leave_ref);
    let noida_l: LeaveGroupResponse =
        send_request(&mut noida_stream, ApiKey::LeaveGroup, 2, cid, Some("diff"), &leave_noida);
    cid += 1;
    assert_eq!(ref_l.error_code, noida_l.error_code);
    compared_count += 1;

    // 13. DeleteTopics
    let mut del_req = DeleteTopicsRequest::default();
    // The crate's default timeout (0) gets REQUEST_TIMED_OUT from a real
    // broker, whose controller can't finish the delete within it.
    del_req.timeout_ms = 30_000;
    del_req.topic_names.push(diff_topic);
    let ref_del: DeleteTopicsResponse =
        send_request(&mut ref_stream, ApiKey::DeleteTopics, 4, cid, Some("diff"), &del_req);
    let noida_del: DeleteTopicsResponse =
        send_request(&mut noida_stream, ApiKey::DeleteTopics, 4, cid, Some("diff"), &del_req);
    assert_eq!(ref_del.responses[0].error_code, noida_del.responses[0].error_code);
    compared_count += 1;

    println!(
        "Successfully compared {} response pairs against reference Kafka broker",
        compared_count
    );
}

/// A one-record v2 batch, encoded by kafka-protocol (valid CRC and lengths).
fn record_batch(value: &[u8]) -> Vec<u8> {
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    let record = Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        key: None,
        value: Some(bytes::Bytes::copy_from_slice(value)),
        headers: Default::default(),
    };
    let mut buf = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, std::iter::once(&record), &options).unwrap();
    buf.to_vec()
}
