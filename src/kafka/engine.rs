//! Engine and state machine for Kafka implementation.

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, FetchRequest,
    FetchResponse, InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest,
    ListOffsetsResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
    ProducerId,
};
use kafka_protocol::protocol::StrBytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PartitionState {
    pub id: i32,
    pub leader: i32,
    pub record_batches: Vec<Vec<u8>>,
    pub high_watermark: i64,
    pub producer_seqs: HashMap<(i64, i16), i32>, // (producer_id, epoch) -> last_sequence
}

impl PartitionState {
    pub fn new(id: i32, leader: i32) -> Self {
        Self {
            id,
            leader,
            record_batches: Vec::new(),
            high_watermark: 0,
            producer_seqs: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicState {
    pub name: String,
    pub partitions: HashMap<i32, PartitionState>,
}

#[derive(Debug, Default)]
pub struct EngineState {
    pub topics: HashMap<String, TopicState>,
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
    pub next_producer_id: i64,
}

impl EngineState {
    pub fn new(host: String, port: i32) -> Self {
        Self {
            topics: HashMap::new(),
            broker_id: 1,
            host,
            port,
            // Stable base64 UUID style string for cluster id
            cluster_id: "MkU3OEVBNTctOEUyRi00".to_string(),
            next_producer_id: 1000,
        }
    }

    pub fn handle_api_versions(&self, _version: i16) -> ApiVersionsResponse {
        let mut res = ApiVersionsResponse::default();
        // Only advertise API keys that noida actually handles
        let supported: &[(ApiKey, i16, i16)] = &[
            (ApiKey::Produce, 0, 9),
            (ApiKey::Fetch, 0, 13),
            (ApiKey::ListOffsets, 0, 8),
            (ApiKey::Metadata, 0, 12),
            (ApiKey::ApiVersions, 0, 3),
            (ApiKey::CreateTopics, 0, 7),
            (ApiKey::DeleteTopics, 0, 6),
            (ApiKey::CreatePartitions, 0, 3),
            (ApiKey::InitProducerId, 0, 4),
        ];

        for &(api_key, min_version, max_version) in supported {
            let mut v = ApiVersion::default();
            v.api_key = api_key as i16;
            v.min_version = min_version;
            v.max_version = max_version;
            res.api_keys.push(v);
        }

        res.error_code = 0;
        res
    }

    pub fn handle_metadata(&mut self, req: &MetadataRequest, _version: i16) -> MetadataResponse {
        let mut res = MetadataResponse::default();

        let mut broker = MetadataResponseBroker::default();
        broker.node_id = kafka_protocol::messages::BrokerId(self.broker_id);
        broker.host = StrBytes::from_string(self.host.clone());
        broker.port = self.port;
        res.brokers.push(broker);

        res.cluster_id = Some(StrBytes::from_string(self.cluster_id.clone()));
        res.controller_id = kafka_protocol::messages::BrokerId(self.broker_id);

        let topic_names_to_query: Vec<String> = match &req.topics {
            Some(topics) if !topics.is_empty() => topics
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.as_str().to_string()))
                .collect(),
            _ => self.topics.keys().cloned().collect(), // List all topics if empty/none
        };

        for topic_name in topic_names_to_query {
            let mut topic_res = MetadataResponseTopic::default();
            topic_res.name = Some(kafka_protocol::messages::TopicName::from(
                StrBytes::from_string(topic_name.clone()),
            ));

            if let Some(state) = self.topics.get(&topic_name) {
                topic_res.error_code = 0;
                for (p_id, part_state) in &state.partitions {
                    let mut part_res = MetadataResponsePartition::default();
                    part_res.partition_index = *p_id;
                    part_res.leader_id = kafka_protocol::messages::BrokerId(part_state.leader);
                    part_res.replica_nodes =
                        vec![kafka_protocol::messages::BrokerId(part_state.leader)];
                    part_res.isr_nodes =
                        vec![kafka_protocol::messages::BrokerId(part_state.leader)];
                    topic_res.partitions.push(part_res);
                }
            } else if req.allow_auto_topic_creation {
                let mut topic_state =
                    TopicState { name: topic_name.clone(), partitions: HashMap::new() };
                topic_state.partitions.insert(0, PartitionState::new(0, self.broker_id));

                let mut part_res = MetadataResponsePartition::default();
                part_res.partition_index = 0;
                part_res.leader_id = kafka_protocol::messages::BrokerId(self.broker_id);
                part_res.replica_nodes = vec![kafka_protocol::messages::BrokerId(self.broker_id)];
                part_res.isr_nodes = vec![kafka_protocol::messages::BrokerId(self.broker_id)];
                topic_res.partitions.push(part_res);

                self.topics.insert(topic_name, topic_state);
                topic_res.error_code = 0;
            } else {
                topic_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_create_topics(
        &mut self,
        req: &CreateTopicsRequest,
        _version: i16,
    ) -> CreateTopicsResponse {
        let mut res = CreateTopicsResponse::default();

        for topic in &req.topics {
            let mut topic_res = CreatableTopicResult::default();
            topic_res.name = topic.name.clone();

            let topic_name_str = topic.name.as_str();

            if topic_name_str.is_empty() {
                topic_res.error_code = 37; // INVALID_TOPIC_EXCEPTION
            } else if topic.replication_factor > 1 {
                topic_res.error_code = 38; // INVALID_REPLICATION_FACTOR
            } else if self.topics.contains_key(topic_name_str) {
                topic_res.error_code = 36; // TOPIC_ALREADY_EXISTS
            } else {
                let num_partitions =
                    if topic.num_partitions > 0 { topic.num_partitions } else { 1 };
                let mut topic_state =
                    TopicState { name: topic_name_str.to_string(), partitions: HashMap::new() };
                for p in 0..num_partitions {
                    topic_state.partitions.insert(p, PartitionState::new(p, self.broker_id));
                }
                self.topics.insert(topic_name_str.to_string(), topic_state);
                topic_res.error_code = 0;
                topic_res.num_partitions = num_partitions;
                topic_res.replication_factor = 1;
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_delete_topics(
        &mut self,
        req: &kafka_protocol::messages::DeleteTopicsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DeleteTopicsResponse {
        use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
        let mut res = kafka_protocol::messages::DeleteTopicsResponse::default();

        for topic_name_bytes in &req.topic_names {
            let topic_name_str = topic_name_bytes.as_str();
            let mut topic_res = DeletableTopicResult::default();
            topic_res.name = Some(topic_name_bytes.clone());

            if self.topics.remove(topic_name_str).is_some() {
                topic_res.error_code = 0;
            } else {
                topic_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
            }
            res.responses.push(topic_res);
        }

        res
    }

    pub fn handle_create_partitions(
        &mut self,
        req: &kafka_protocol::messages::CreatePartitionsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::CreatePartitionsResponse {
        use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
        let mut res = kafka_protocol::messages::CreatePartitionsResponse::default();

        for topic_partition_data in &req.topics {
            let topic_name_str = topic_partition_data.name.as_str();
            let mut topic_res = CreatePartitionsTopicResult::default();
            topic_res.name = topic_partition_data.name.clone();

            if let Some(topic_state) = self.topics.get_mut(topic_name_str) {
                let current_count = topic_state.partitions.len() as i32;
                let new_count = topic_partition_data.count;

                if new_count < current_count {
                    topic_res.error_code = 37; // INVALID_PARTITIONS
                } else if new_count == current_count {
                    topic_res.error_code = 0;
                } else {
                    for p in current_count..new_count {
                        topic_state.partitions.insert(p, PartitionState::new(p, self.broker_id));
                    }
                    topic_res.error_code = 0;
                }
            } else {
                topic_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
            }

            res.results.push(topic_res);
        }

        res
    }

    pub fn handle_init_producer_id(
        &mut self,
        _req: &InitProducerIdRequest,
        _version: i16,
    ) -> InitProducerIdResponse {
        let pid = self.next_producer_id;
        self.next_producer_id += 1;

        let mut res = InitProducerIdResponse::default();
        res.error_code = 0;
        res.producer_id = ProducerId(pid);
        res.producer_epoch = 0;
        res
    }

    pub fn handle_produce(&mut self, req: &ProduceRequest, _version: i16) -> ProduceResponse {
        let mut res = ProduceResponse::default();
        let now =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;

        for topic in &req.topic_data {
            let mut topic_res = TopicProduceResponse::default();
            topic_res.name = topic.name.clone();

            let topic_name = topic.name.as_str();
            for partition in &topic.partition_data {
                let mut part_res = PartitionProduceResponse::default();
                part_res.index = partition.index;

                if let Some(topic_state) = self.topics.get_mut(topic_name) {
                    if let Some(part_state) = topic_state.partitions.get_mut(&partition.index) {
                        if let Some(records) = &partition.records {
                            let base_offset = part_state.high_watermark;
                            part_state.record_batches.push(records.to_vec());
                            part_state.high_watermark += 1;

                            part_res.error_code = 0;
                            part_res.base_offset = base_offset;
                            part_res.log_append_time_ms = now;
                        } else {
                            part_res.error_code = 0;
                            part_res.base_offset = part_state.high_watermark;
                            part_res.log_append_time_ms = now;
                        }
                    } else {
                        part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                } else {
                    part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }

                topic_res.partition_responses.push(part_res);
            }

            res.responses.push(topic_res);
        }

        res
    }

    pub fn handle_fetch(&self, req: &FetchRequest, _version: i16) -> FetchResponse {
        let mut res = FetchResponse::default();

        for topic in &req.topics {
            let mut topic_res = FetchableTopicResponse::default();
            topic_res.topic = topic.topic.clone();

            let topic_name = topic.topic.as_str();
            for partition in &topic.partitions {
                let mut part_res = PartitionData::default();
                part_res.partition_index = partition.partition;

                if let Some(topic_state) = self.topics.get(topic_name) {
                    if let Some(part_state) = topic_state.partitions.get(&partition.partition) {
                        part_res.error_code = 0;
                        part_res.high_watermark = part_state.high_watermark;

                        let fetch_offset = partition.fetch_offset;
                        if fetch_offset < 0 || fetch_offset > part_state.high_watermark {
                            part_res.error_code = 1; // OFFSET_OUT_OF_RANGE
                        } else if fetch_offset < part_state.high_watermark {
                            let idx = fetch_offset as usize;
                            if idx < part_state.record_batches.len() {
                                part_res.records = Some(bytes::Bytes::from(
                                    part_state.record_batches[idx].clone(),
                                ));
                            }
                        }
                    } else {
                        part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                } else {
                    part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }

                topic_res.partitions.push(part_res);
            }

            res.responses.push(topic_res);
        }

        res
    }

    pub fn handle_list_offsets(
        &self,
        req: &ListOffsetsRequest,
        _version: i16,
    ) -> ListOffsetsResponse {
        let mut res = ListOffsetsResponse::default();

        for topic in &req.topics {
            let mut topic_res = ListOffsetsTopicResponse::default();
            topic_res.name = topic.name.clone();

            let topic_name = topic.name.as_str();
            for partition in &topic.partitions {
                let mut part_res = ListOffsetsPartitionResponse::default();
                part_res.partition_index = partition.partition_index;

                if let Some(topic_state) = self.topics.get(topic_name) {
                    if let Some(part_state) =
                        topic_state.partitions.get(&partition.partition_index)
                    {
                        part_res.error_code = 0;
                        if partition.timestamp == -2 {
                            // Earliest
                            part_res.offset = 0;
                        } else {
                            // Latest (-1) / Max timestamp (-3) / default
                            part_res.offset = part_state.high_watermark;
                        }
                    } else {
                        part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                } else {
                    part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }

                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }
}

#[derive(Clone, Debug, Default)]
pub struct Engine {
    state: Arc<Mutex<EngineState>>,
}

impl Engine {
    pub fn new(host: String, port: i32) -> Self {
        Self { state: Arc::new(Mutex::new(EngineState::new(host, port))) }
    }

    pub fn handle_api_versions(&self, version: i16) -> ApiVersionsResponse {
        self.state.lock().unwrap().handle_api_versions(version)
    }

    pub fn handle_metadata(&self, req: &MetadataRequest, version: i16) -> MetadataResponse {
        self.state.lock().unwrap().handle_metadata(req, version)
    }

    pub fn handle_create_topics(
        &self,
        req: &CreateTopicsRequest,
        version: i16,
    ) -> CreateTopicsResponse {
        self.state.lock().unwrap().handle_create_topics(req, version)
    }

    pub fn handle_init_producer_id(
        &self,
        req: &InitProducerIdRequest,
        version: i16,
    ) -> InitProducerIdResponse {
        self.state.lock().unwrap().handle_init_producer_id(req, version)
    }

    pub fn handle_produce(&self, req: &ProduceRequest, version: i16) -> ProduceResponse {
        self.state.lock().unwrap().handle_produce(req, version)
    }

    pub fn handle_fetch(&self, req: &FetchRequest, version: i16) -> FetchResponse {
        self.state.lock().unwrap().handle_fetch(req, version)
    }

    pub fn handle_list_offsets(
        &self,
        req: &ListOffsetsRequest,
        version: i16,
    ) -> ListOffsetsResponse {
        self.state.lock().unwrap().handle_list_offsets(req, version)
    }

    pub fn handle_delete_topics(
        &self,
        req: &kafka_protocol::messages::DeleteTopicsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DeleteTopicsResponse {
        self.state.lock().unwrap().handle_delete_topics(req, version)
    }

    pub fn handle_create_partitions(
        &self,
        req: &kafka_protocol::messages::CreatePartitionsRequest,
        version: i16,
    ) -> kafka_protocol::messages::CreatePartitionsResponse {
        self.state.lock().unwrap().handle_create_partitions(req, version)
    }
}
