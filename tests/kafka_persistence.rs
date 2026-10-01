//! Real on-disk persistence for Kafka: save on a clean "shutdown" (simulated
//! directly here via the save closure `spawn_persistent_for_test` returns),
//! load back into a fresh server, and confirm topics, records, and committed
//! consumer offsets survive a restart.

#[cfg(feature = "kafka")]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{
        ApiKey, CreateTopicsRequest, CreateTopicsResponse, FetchRequest, FetchResponse, GroupId,
        OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
        ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use noida::kafka::spawn_persistent_for_test;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};

    fn send_request<Req: Encodable, Resp: Decodable>(
        stream: &mut TcpStream,
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        req: &Req,
    ) -> Resp {
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_string("test-client".to_string()));

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

    fn connect(addr: SocketAddr) -> TcpStream {
        TcpStream::connect(addr).expect("connects to kafka")
    }

    fn create_topic(stream: &mut TcpStream, topic: &str, partitions: i32) -> CreateTopicsResponse {
        let mut req = CreateTopicsRequest::default();
        let mut t = CreatableTopic::default();
        let topic_name = TopicName(StrBytes::from_string(topic.to_string()));
        t.name = topic_name;
        t.num_partitions = partitions;
        t.replication_factor = 1;
        req.topics.push(t);
        send_request(stream, ApiKey::CreateTopics, 5, 1, &req)
    }

    /// Produce raw bytes as a record batch payload (no encoding — the
    /// engine stores whatever bytes are sent; this matches how the existing
    /// kafka_client.rs tests work).
    fn produce(
        stream: &mut TcpStream,
        topic: &str,
        partition: i32,
        payload: &[u8],
    ) -> ProduceResponse {
        let mut req = ProduceRequest::default();
        req.acks = 1;
        req.timeout_ms = 1000;
        let mut tdata = TopicProduceData::default();
        tdata.name = TopicName(StrBytes::from_string(topic.to_string()));
        let mut pdata = PartitionProduceData::default();
        pdata.index = partition;
        pdata.records = Some(bytes::Bytes::copy_from_slice(payload));
        tdata.partition_data.push(pdata);
        req.topic_data.push(tdata);
        send_request(stream, ApiKey::Produce, 8, 2, &req)
    }

    fn commit_offset(
        stream: &mut TcpStream,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> OffsetCommitResponse {
        let mut req = OffsetCommitRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
        let mut t = OffsetCommitRequestTopic::default();
        t.name = TopicName(StrBytes::from_string(topic.to_string()));
        let mut p = OffsetCommitRequestPartition::default();
        p.partition_index = partition;
        p.committed_offset = offset;
        t.partitions.push(p);
        req.topics.push(t);
        send_request(stream, ApiKey::OffsetCommit, 5, 3, &req)
    }

    fn fetch_committed_offset(
        stream: &mut TcpStream,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> i64 {
        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group_id.to_string()));
        let mut t = OffsetFetchRequestTopic::default();
        t.name = TopicName(StrBytes::from_string(topic.to_string()));
        t.partition_indexes.push(partition);
        req.topics = Some(vec![t]);
        let resp: OffsetFetchResponse = send_request(stream, ApiKey::OffsetFetch, 5, 4, &req);
        resp.topics
            .first()
            .and_then(|t| t.partitions.first())
            .map(|p| p.committed_offset)
            .unwrap_or(-1)
    }

    fn fetch_records_bytes(
        stream: &mut TcpStream,
        topic: &str,
        partition: i32,
    ) -> Option<bytes::Bytes> {
        let mut req = FetchRequest::default();
        req.max_wait_ms = 100;
        req.min_bytes = 0;
        let mut ft = FetchTopic::default();
        ft.topic = TopicName(StrBytes::from_string(topic.to_string()));
        let mut fp = FetchPartition::default();
        fp.partition = partition;
        fp.fetch_offset = 0;
        ft.partitions.push(fp);
        req.topics.push(ft);
        let resp: FetchResponse = send_request(stream, ApiKey::Fetch, 11, 5, &req);
        resp.responses
            .first()
            .and_then(|tr| tr.partitions.first())
            .and_then(|pr| pr.records.clone())
    }

    #[test]
    fn persists_topics_and_consumer_offsets_across_a_restart() {
        let dir = std::env::temp_dir().join(format!("noida-kafka-persist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let (addr1, save1) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        {
            let mut stream = connect(addr1);

            // Create a topic with 2 partitions.
            let create_resp = create_topic(&mut stream, "test-topic", 2);
            assert_eq!(create_resp.topics[0].error_code, 0);

            // Produce one record batch to each partition.
            let prod0 = produce(&mut stream, "test-topic", 0, b"batch-payload-0");
            assert_eq!(
                prod0.responses[0].partition_responses[0].error_code, 0,
                "produce to partition 0 must succeed"
            );
            let prod1 = produce(&mut stream, "test-topic", 1, b"batch-payload-1");
            assert_eq!(
                prod1.responses[0].partition_responses[0].error_code, 0,
                "produce to partition 1 must succeed"
            );

            // Commit consumer group offsets.
            let c0 = commit_offset(&mut stream, "test-group", "test-topic", 0, 1);
            assert_eq!(c0.topics[0].partitions[0].error_code, 0, "offset commit p0");
            let c1 = commit_offset(&mut stream, "test-group", "test-topic", 1, 1);
            assert_eq!(c1.topics[0].partitions[0].error_code, 0, "offset commit p1");
        }

        // Trigger save (not a real SIGTERM — see tests/redis_persistence.rs).
        save1();
        assert!(dir.join("kafka.json").exists(), "kafka.json must be written after save");

        // Start a second server against the same dir.
        let (addr2, _save2) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        let mut stream = connect(addr2);

        // ---- Records survived ----
        let rec0 = fetch_records_bytes(&mut stream, "test-topic", 0);
        assert!(rec0.is_some(), "partition 0 must have records after restart");
        let rec0_bytes = rec0.unwrap();
        assert!(
            rec0_bytes.windows(b"batch-payload-0".len()).any(|w| w == b"batch-payload-0"),
            "partition 0 payload must survive"
        );

        let rec1 = fetch_records_bytes(&mut stream, "test-topic", 1);
        assert!(rec1.is_some(), "partition 1 must have records after restart");

        // ---- Committed consumer offsets survived ----
        let offset0 = fetch_committed_offset(&mut stream, "test-group", "test-topic", 0);
        assert_eq!(offset0, 1, "committed offset for partition 0 must survive restart");

        let offset1 = fetch_committed_offset(&mut stream, "test-group", "test-topic", 1);
        assert_eq!(offset1, 1, "committed offset for partition 1 must survive restart");
    }

    #[test]
    fn snapshot_round_trip_with_producer_seqs() {
        // Fast, network-free test: builds EngineState directly, exercises
        // to_snapshot -> serde_json -> from_snapshot, and checks producer_seqs
        // round-trips correctly (the tuple-key map that can't be derived directly).
        use noida::kafka::engine::{EngineState, PartitionState, TopicState};
        use std::collections::HashMap;

        let mut state = EngineState::new("127.0.0.1".to_string(), 9999);
        // Add a topic with a partition that has producer_seqs entries.
        let mut part = PartitionState::new(0, 1);
        part.producer_seqs.insert((1001i64, 0i16), (5i32, 0i64));
        part.producer_seqs.insert((1002i64, 1i16), (3i32, 10i64));
        let mut topic = TopicState {
            name: "my-topic".to_string(),
            is_internal: false,
            partitions: HashMap::new(),
            configs: HashMap::new(),
        };
        topic.partitions.insert(0, part);
        state.topics.insert("my-topic".to_string(), topic);
        state.committed_offsets.insert(("g1".to_string(), "my-topic".to_string(), 0), 42);

        let snapshot = state.to_snapshot();
        let bytes = serde_json::to_vec(&snapshot).expect("must serialize");
        let snapshot2: noida::kafka::engine::Snapshot =
            serde_json::from_slice(&bytes).expect("must deserialize");
        let state2 = EngineState::from_snapshot(snapshot2);

        let prod_seqs = &state2.topics["my-topic"].partitions[&0].producer_seqs;
        assert_eq!(prod_seqs.get(&(1001, 0)), Some(&(5, 0)));
        assert_eq!(prod_seqs.get(&(1002, 1)), Some(&(3, 10)));

        let off = state2.committed_offsets.get(&("g1".to_string(), "my-topic".to_string(), 0));
        assert_eq!(off, Some(&42));
    }

    #[test]
    fn resolve_open_transactions_for_shutdown_aborts_in_flight() {
        use noida::kafka::engine::{EngineState, PartitionState, TopicState};
        use std::collections::HashMap;

        let mut state = EngineState::new("127.0.0.1".to_string(), 9999);
        let mut part = PartitionState::new(0, 1);
        // Simulate an open transaction.
        part.active_txns.insert(9999i64, 0i64); // producer_id -> first_offset
        let initial_hwm = part.high_watermark;
        let mut topic = TopicState {
            name: "txn-topic".to_string(),
            is_internal: false,
            partitions: HashMap::new(),
            configs: HashMap::new(),
        };
        topic.partitions.insert(0, part);
        state.topics.insert("txn-topic".to_string(), topic);

        state.resolve_open_transactions_for_shutdown();

        let part = &state.topics["txn-topic"].partitions[&0];
        // active_txns must be empty after resolution.
        assert!(part.active_txns.is_empty(), "active_txns must be cleared");
        // aborted_txns must contain our transaction.
        assert!(
            part.aborted_txns.iter().any(|&(pid, _)| pid == 9999),
            "aborted_txns must record the aborted transaction"
        );
        // high_watermark must have advanced by 1 (the abort control batch).
        assert_eq!(
            part.high_watermark,
            initial_hwm + 1,
            "high_watermark must advance by 1 for the abort control batch"
        );

        // A no-op call (no open transactions) must not add spurious batches.
        let hwm_before = part.high_watermark;
        state.resolve_open_transactions_for_shutdown();
        let part_after = &state.topics["txn-topic"].partitions[&0];
        assert_eq!(
            part_after.high_watermark, hwm_before,
            "resolve_open_transactions_for_shutdown with no open txns must be a no-op"
        );
    }
}
