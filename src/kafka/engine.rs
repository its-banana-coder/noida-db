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
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
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
    // (group_id, topic_name, partition) -> offset
    pub committed_offsets: HashMap<(String, String, i32), i64>,
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
            committed_offsets: HashMap::new(),
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
            (ApiKey::OffsetCommit, 0, 8),
            (ApiKey::OffsetFetch, 0, 8),
            (ApiKey::FindCoordinator, 0, 4),
            (ApiKey::JoinGroup, 0, 7),
            (ApiKey::Heartbeat, 0, 4),
            (ApiKey::LeaveGroup, 0, 5),
            (ApiKey::SyncGroup, 0, 5),
            (ApiKey::DescribeGroups, 0, 5),
            (ApiKey::ListGroups, 0, 4),
            (ApiKey::DeleteGroups, 0, 2),
            (ApiKey::DescribeConfigs, 0, 4),
            (ApiKey::AlterConfigs, 0, 2),
            (ApiKey::IncrementalAlterConfigs, 0, 1),
            (ApiKey::DescribeCluster, 0, 0),
            (ApiKey::OffsetForLeaderEpoch, 0, 4),
            (ApiKey::DeleteRecords, 0, 2),
            (ApiKey::OffsetDelete, 0, 0),
            (ApiKey::AddPartitionsToTxn, 0, 3),
            (ApiKey::AddOffsetsToTxn, 0, 3),
            (ApiKey::EndTxn, 0, 3),
            (ApiKey::TxnOffsetCommit, 0, 3),
            (ApiKey::DescribeTransactions, 0, 0),
            (ApiKey::ListTransactions, 0, 0),
            (ApiKey::DescribeProducers, 0, 0),
            (ApiKey::DescribeLogDirs, 0, 2),
            (ApiKey::SaslHandshake, 0, 1),
            (ApiKey::SaslAuthenticate, 0, 2),
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
                    if let Some(part_state) = topic_state.partitions.get(&partition.partition_index)
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

    pub fn handle_find_coordinator(
        &self,
        _req: &kafka_protocol::messages::FindCoordinatorRequest,
        _version: i16,
    ) -> kafka_protocol::messages::FindCoordinatorResponse {
        let mut res = kafka_protocol::messages::FindCoordinatorResponse::default();
        res.node_id = kafka_protocol::messages::BrokerId(self.broker_id);
        res.host = StrBytes::from_string(self.host.clone());
        res.port = self.port;
        res.error_code = 0;
        res
    }

    pub fn handle_join_group(
        &self,
        req: &kafka_protocol::messages::JoinGroupRequest,
        _version: i16,
    ) -> kafka_protocol::messages::JoinGroupResponse {
        use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
        let mut res = kafka_protocol::messages::JoinGroupResponse::default();

        let member_id = if req.member_id.is_empty() {
            format!("noida-client-{}", uuid_simple())
        } else {
            req.member_id.as_str().to_string()
        };

        res.error_code = 0;
        res.generation_id = 1;
        res.protocol_name = req.protocols.first().map(|p| p.name.clone());
        res.leader = StrBytes::from_string(member_id.clone());
        res.member_id = StrBytes::from_string(member_id.clone());

        let mut member = JoinGroupResponseMember::default();
        member.member_id = StrBytes::from_string(member_id);
        if let Some(first_protocol) = req.protocols.first() {
            member.metadata = first_protocol.metadata.clone();
        }
        res.members.push(member);

        res
    }

    pub fn handle_sync_group(
        &self,
        req: &kafka_protocol::messages::SyncGroupRequest,
        _version: i16,
    ) -> kafka_protocol::messages::SyncGroupResponse {
        let mut res = kafka_protocol::messages::SyncGroupResponse::default();
        res.error_code = 0;

        if let Some(assignment) = req.assignments.first() {
            res.assignment = assignment.assignment.clone();
        }

        res
    }

    pub fn handle_heartbeat(
        &self,
        _req: &kafka_protocol::messages::HeartbeatRequest,
        _version: i16,
    ) -> kafka_protocol::messages::HeartbeatResponse {
        let mut res = kafka_protocol::messages::HeartbeatResponse::default();
        res.error_code = 0;
        res
    }

    pub fn handle_leave_group(
        &self,
        _req: &kafka_protocol::messages::LeaveGroupRequest,
        _version: i16,
    ) -> kafka_protocol::messages::LeaveGroupResponse {
        let mut res = kafka_protocol::messages::LeaveGroupResponse::default();
        res.error_code = 0;
        res
    }

    pub fn handle_offset_commit(
        &mut self,
        req: &kafka_protocol::messages::OffsetCommitRequest,
        _version: i16,
    ) -> kafka_protocol::messages::OffsetCommitResponse {
        use kafka_protocol::messages::offset_commit_response::{
            OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        };
        let mut res = kafka_protocol::messages::OffsetCommitResponse::default();
        let group_id = req.group_id.as_str().to_string();

        for topic in &req.topics {
            let mut topic_res = OffsetCommitResponseTopic::default();
            topic_res.name = topic.name.clone();
            let topic_name = topic.name.as_str().to_string();

            for part in &topic.partitions {
                let mut part_res = OffsetCommitResponsePartition::default();
                part_res.partition_index = part.partition_index;

                self.committed_offsets.insert(
                    (group_id.clone(), topic_name.clone(), part.partition_index),
                    part.committed_offset,
                );

                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_offset_fetch(
        &self,
        req: &kafka_protocol::messages::OffsetFetchRequest,
        _version: i16,
    ) -> kafka_protocol::messages::OffsetFetchResponse {
        use kafka_protocol::messages::offset_fetch_response::{
            OffsetFetchResponsePartition, OffsetFetchResponseTopic,
        };
        let mut res = kafka_protocol::messages::OffsetFetchResponse::default();
        let group_id = req.group_id.as_str().to_string();

        if let Some(topics) = &req.topics {
            for topic in topics {
                let mut topic_res = OffsetFetchResponseTopic::default();
                topic_res.name = topic.name.clone();
                let topic_name = topic.name.as_str().to_string();

                for &partition_index in &topic.partition_indexes {
                    let mut part_res = OffsetFetchResponsePartition::default();
                    part_res.partition_index = partition_index;

                    if let Some(&offset) = self.committed_offsets.get(&(
                        group_id.clone(),
                        topic_name.clone(),
                        partition_index,
                    )) {
                        part_res.committed_offset = offset;
                        part_res.error_code = 0;
                    } else {
                        part_res.committed_offset = -1;
                        part_res.error_code = 0;
                    }

                    topic_res.partitions.push(part_res);
                }

                res.topics.push(topic_res);
            }
        }

        res
    }

    pub fn handle_describe_groups(
        &self,
        req: &kafka_protocol::messages::DescribeGroupsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeGroupsResponse {
        use kafka_protocol::messages::describe_groups_response::DescribedGroup;
        let mut res = kafka_protocol::messages::DescribeGroupsResponse::default();

        for group_id in &req.groups {
            let mut group = DescribedGroup::default();
            group.group_id = group_id.clone();
            group.group_state = StrBytes::from_string("Stable".to_string());
            group.protocol_type = StrBytes::from_string("consumer".to_string());
            group.protocol_data = StrBytes::from_string("range".to_string());
            group.error_code = 0;
            res.groups.push(group);
        }

        res
    }

    pub fn handle_list_groups(
        &self,
        _req: &kafka_protocol::messages::ListGroupsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::ListGroupsResponse {
        use kafka_protocol::messages::list_groups_response::ListedGroup;
        let mut res = kafka_protocol::messages::ListGroupsResponse::default();

        let mut groups_set = std::collections::HashSet::new();
        for (g, _, _) in self.committed_offsets.keys() {
            groups_set.insert(g.clone());
        }

        for g in groups_set {
            let mut group = ListedGroup::default();
            group.group_id = kafka_protocol::messages::GroupId(StrBytes::from_string(g));
            group.protocol_type = StrBytes::from_string("consumer".to_string());
            res.groups.push(group);
        }

        res.error_code = 0;
        res
    }

    pub fn handle_delete_groups(
        &mut self,
        req: &kafka_protocol::messages::DeleteGroupsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DeleteGroupsResponse {
        use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
        let mut res = kafka_protocol::messages::DeleteGroupsResponse::default();

        for group_id in &req.groups_names {
            let gid_str = group_id.as_str().to_string();
            let mut result = DeletableGroupResult::default();
            result.group_id = group_id.clone();

            self.committed_offsets.retain(|(g, _, _), _| g != &gid_str);
            result.error_code = 0;
            res.results.push(result);
        }

        res
    }

    pub fn handle_describe_configs(
        &self,
        req: &kafka_protocol::messages::DescribeConfigsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeConfigsResponse {
        use kafka_protocol::messages::describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResult,
        };
        let mut res = kafka_protocol::messages::DescribeConfigsResponse::default();

        for resource in &req.resources {
            let mut result = DescribeConfigsResult::default();
            result.resource_type = resource.resource_type;
            result.resource_name = resource.resource_name.clone();
            result.error_code = 0;

            // Provide common default topic/broker configs
            let configs: &[(&str, &str)] = match resource.resource_type {
                2 => &[
                    // Topic
                    ("cleanup.policy", "delete"),
                    ("retention.ms", "604800000"),
                    ("segment.bytes", "1073741824"),
                ],
                _ => &[
                    // Broker/other
                    ("auto.create.topics.enable", "true"),
                    ("num.partitions", "1"),
                    ("default.replication.factor", "1"),
                ],
            };

            for &(k, v) in configs {
                let mut conf = DescribeConfigsResourceResult::default();
                conf.name = StrBytes::from_string(k.to_string());
                conf.value = Some(StrBytes::from_string(v.to_string()));
                conf.read_only = false;
                result.configs.push(conf);
            }

            res.results.push(result);
        }

        res
    }

    pub fn handle_describe_cluster(
        &self,
        _req: &kafka_protocol::messages::DescribeClusterRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeClusterResponse {
        use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
        let mut res = kafka_protocol::messages::DescribeClusterResponse::default();

        res.cluster_id = StrBytes::from_string(self.cluster_id.clone());
        res.controller_id = kafka_protocol::messages::BrokerId(self.broker_id);

        let mut broker = DescribeClusterBroker::default();
        broker.broker_id = kafka_protocol::messages::BrokerId(self.broker_id);
        broker.host = StrBytes::from_string(self.host.clone());
        broker.port = self.port;
        res.brokers.push(broker);

        res.error_code = 0;
        res
    }

    pub fn handle_offset_for_leader_epoch(
        &self,
        req: &kafka_protocol::messages::OffsetForLeaderEpochRequest,
        _version: i16,
    ) -> kafka_protocol::messages::OffsetForLeaderEpochResponse {
        use kafka_protocol::messages::offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderTopicResult,
        };
        let mut res = kafka_protocol::messages::OffsetForLeaderEpochResponse::default();

        for topic in &req.topics {
            let mut topic_res = OffsetForLeaderTopicResult::default();
            topic_res.topic = topic.topic.clone();

            let topic_name = topic.topic.as_str();
            for part in &topic.partitions {
                let mut part_res = EpochEndOffset::default();
                part_res.partition = part.partition;

                if let Some(t_state) = self.topics.get(topic_name) {
                    if let Some(p_state) = t_state.partitions.get(&part.partition) {
                        part_res.error_code = 0;
                        part_res.leader_epoch = 0;
                        part_res.end_offset = p_state.high_watermark;
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

    pub fn handle_add_partitions_to_txn(
        &self,
        req: &kafka_protocol::messages::AddPartitionsToTxnRequest,
        _version: i16,
    ) -> kafka_protocol::messages::AddPartitionsToTxnResponse {
        use kafka_protocol::messages::add_partitions_to_txn_response::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
        };
        let mut res = kafka_protocol::messages::AddPartitionsToTxnResponse::default();

        for topic in &req.v3_and_below_topics {
            let mut topic_res = AddPartitionsToTxnTopicResult::default();
            topic_res.name = topic.name.clone();

            for &p_id in &topic.partitions {
                let mut part_res = AddPartitionsToTxnPartitionResult::default();
                part_res.partition_index = p_id;
                part_res.partition_error_code = 0;
                topic_res.results_by_partition.push(part_res);
            }

            res.results_by_topic_v3_and_below.push(topic_res);
        }

        res
    }

    pub fn handle_add_offsets_to_txn(
        &self,
        _req: &kafka_protocol::messages::AddOffsetsToTxnRequest,
        _version: i16,
    ) -> kafka_protocol::messages::AddOffsetsToTxnResponse {
        let mut res = kafka_protocol::messages::AddOffsetsToTxnResponse::default();
        res.error_code = 0;
        res
    }

    pub fn handle_end_txn(
        &self,
        _req: &kafka_protocol::messages::EndTxnRequest,
        _version: i16,
    ) -> kafka_protocol::messages::EndTxnResponse {
        let mut res = kafka_protocol::messages::EndTxnResponse::default();
        res.error_code = 0;
        res
    }

    pub fn handle_txn_offset_commit(
        &mut self,
        req: &kafka_protocol::messages::TxnOffsetCommitRequest,
        _version: i16,
    ) -> kafka_protocol::messages::TxnOffsetCommitResponse {
        use kafka_protocol::messages::txn_offset_commit_response::{
            TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        };
        let mut res = kafka_protocol::messages::TxnOffsetCommitResponse::default();
        let group_id = req.group_id.as_str().to_string();

        for topic in &req.topics {
            let mut topic_res = TxnOffsetCommitResponseTopic::default();
            topic_res.name = topic.name.clone();
            let topic_name = topic.name.as_str().to_string();

            for part in &topic.partitions {
                let mut part_res = TxnOffsetCommitResponsePartition::default();
                part_res.partition_index = part.partition_index;

                self.committed_offsets.insert(
                    (group_id.clone(), topic_name.clone(), part.partition_index),
                    part.committed_offset,
                );

                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_alter_configs(
        &self,
        req: &kafka_protocol::messages::AlterConfigsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::AlterConfigsResponse {
        use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;
        let mut res = kafka_protocol::messages::AlterConfigsResponse::default();

        for resource in &req.resources {
            let mut resource_res = AlterConfigsResourceResponse::default();
            resource_res.resource_type = resource.resource_type;
            resource_res.resource_name = resource.resource_name.clone();
            resource_res.error_code = 0;
            res.responses.push(resource_res);
        }

        res
    }

    pub fn handle_incremental_alter_configs(
        &self,
        req: &kafka_protocol::messages::IncrementalAlterConfigsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::IncrementalAlterConfigsResponse {
        use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
        let mut res = kafka_protocol::messages::IncrementalAlterConfigsResponse::default();

        for resource in &req.resources {
            let mut resource_res = AlterConfigsResourceResponse::default();
            resource_res.resource_type = resource.resource_type;
            resource_res.resource_name = resource.resource_name.clone();
            resource_res.error_code = 0;
            res.responses.push(resource_res);
        }

        res
    }

    pub fn handle_delete_records(
        &mut self,
        req: &kafka_protocol::messages::DeleteRecordsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DeleteRecordsResponse {
        use kafka_protocol::messages::delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
        };
        let mut res = kafka_protocol::messages::DeleteRecordsResponse::default();

        for topic in &req.topics {
            let mut topic_res = DeleteRecordsTopicResult::default();
            topic_res.name = topic.name.clone();

            for part in &topic.partitions {
                let mut part_res = DeleteRecordsPartitionResult::default();
                part_res.partition_index = part.partition_index;
                part_res.low_watermark = part.offset;
                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_offset_delete(
        &mut self,
        req: &kafka_protocol::messages::OffsetDeleteRequest,
        _version: i16,
    ) -> kafka_protocol::messages::OffsetDeleteResponse {
        use kafka_protocol::messages::offset_delete_response::{
            OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
        };
        let mut res = kafka_protocol::messages::OffsetDeleteResponse::default();
        let group_id = req.group_id.as_str().to_string();

        for topic in &req.topics {
            let mut topic_res = OffsetDeleteResponseTopic::default();
            topic_res.name = topic.name.clone();
            let topic_name = topic.name.as_str().to_string();

            for part in &topic.partitions {
                let mut part_res = OffsetDeleteResponsePartition::default();
                part_res.partition_index = part.partition_index;
                self.committed_offsets.remove(&(
                    group_id.clone(),
                    topic_name.clone(),
                    part.partition_index,
                ));
                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res.error_code = 0;
        res
    }

    pub fn handle_describe_transactions(
        &self,
        req: &kafka_protocol::messages::DescribeTransactionsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeTransactionsResponse {
        use kafka_protocol::messages::describe_transactions_response::TransactionState;
        let mut res = kafka_protocol::messages::DescribeTransactionsResponse::default();

        for tx_id in &req.transactional_ids {
            let mut tx_state = TransactionState::default();
            tx_state.transactional_id = tx_id.clone();
            tx_state.transaction_state = StrBytes::from_string("CompleteCommit".to_string());
            tx_state.error_code = 0;
            res.transaction_states.push(tx_state);
        }

        res
    }

    pub fn handle_list_transactions(
        &self,
        _req: &kafka_protocol::messages::ListTransactionsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::ListTransactionsResponse {
        let mut res = kafka_protocol::messages::ListTransactionsResponse::default();
        res.error_code = 0;
        res
    }

    pub fn handle_describe_producers(
        &self,
        req: &kafka_protocol::messages::DescribeProducersRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeProducersResponse {
        use kafka_protocol::messages::describe_producers_response::{
            PartitionResponse, TopicResponse,
        };
        let mut res = kafka_protocol::messages::DescribeProducersResponse::default();

        for topic in &req.topics {
            let mut topic_res = TopicResponse::default();
            topic_res.name = topic.name.clone();

            for &p_id in &topic.partition_indexes {
                let mut part_res = PartitionResponse::default();
                part_res.partition_index = p_id;
                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_describe_log_dirs(
        &self,
        _req: &kafka_protocol::messages::DescribeLogDirsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeLogDirsResponse {
        use kafka_protocol::messages::describe_log_dirs_response::{
            DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
        };
        let mut res = kafka_protocol::messages::DescribeLogDirsResponse::default();

        let mut log_dir = DescribeLogDirsResult::default();
        log_dir.log_dir = StrBytes::from_string("/tmp/noida-kafka-logs".to_string());
        log_dir.error_code = 0;

        for (topic_name, topic_state) in &self.topics {
            let mut t_dir = DescribeLogDirsTopic::default();
            t_dir.name =
                kafka_protocol::messages::TopicName(StrBytes::from_string(topic_name.clone()));

            for &p_id in topic_state.partitions.keys() {
                let mut p_dir = DescribeLogDirsPartition::default();
                p_dir.partition_index = p_id;
                p_dir.partition_size = 1024;
                p_dir.offset_lag = 0;
                p_dir.is_future_key = false;
                t_dir.partitions.push(p_dir);
            }

            log_dir.topics.push(t_dir);
        }

        res.results.push(log_dir);
        res
    }

    pub fn handle_sasl_handshake(
        &self,
        _req: &kafka_protocol::messages::SaslHandshakeRequest,
        _version: i16,
    ) -> kafka_protocol::messages::SaslHandshakeResponse {
        let mut res = kafka_protocol::messages::SaslHandshakeResponse::default();
        res.error_code = 0;
        res.mechanisms.push(StrBytes::from_string("PLAIN".to_string()));
        res
    }

    pub fn handle_sasl_authenticate(
        &self,
        _req: &kafka_protocol::messages::SaslAuthenticateRequest,
        _version: i16,
    ) -> kafka_protocol::messages::SaslAuthenticateResponse {
        let mut res = kafka_protocol::messages::SaslAuthenticateResponse::default();
        res.error_code = 0;
        res
    }
}

fn uuid_simple() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
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

    pub fn handle_find_coordinator(
        &self,
        req: &kafka_protocol::messages::FindCoordinatorRequest,
        version: i16,
    ) -> kafka_protocol::messages::FindCoordinatorResponse {
        self.state.lock().unwrap().handle_find_coordinator(req, version)
    }

    pub fn handle_join_group(
        &self,
        req: &kafka_protocol::messages::JoinGroupRequest,
        version: i16,
    ) -> kafka_protocol::messages::JoinGroupResponse {
        self.state.lock().unwrap().handle_join_group(req, version)
    }

    pub fn handle_sync_group(
        &self,
        req: &kafka_protocol::messages::SyncGroupRequest,
        version: i16,
    ) -> kafka_protocol::messages::SyncGroupResponse {
        self.state.lock().unwrap().handle_sync_group(req, version)
    }

    pub fn handle_heartbeat(
        &self,
        req: &kafka_protocol::messages::HeartbeatRequest,
        version: i16,
    ) -> kafka_protocol::messages::HeartbeatResponse {
        self.state.lock().unwrap().handle_heartbeat(req, version)
    }

    pub fn handle_leave_group(
        &self,
        req: &kafka_protocol::messages::LeaveGroupRequest,
        version: i16,
    ) -> kafka_protocol::messages::LeaveGroupResponse {
        self.state.lock().unwrap().handle_leave_group(req, version)
    }

    pub fn handle_offset_commit(
        &self,
        req: &kafka_protocol::messages::OffsetCommitRequest,
        version: i16,
    ) -> kafka_protocol::messages::OffsetCommitResponse {
        self.state.lock().unwrap().handle_offset_commit(req, version)
    }

    pub fn handle_offset_fetch(
        &self,
        req: &kafka_protocol::messages::OffsetFetchRequest,
        version: i16,
    ) -> kafka_protocol::messages::OffsetFetchResponse {
        self.state.lock().unwrap().handle_offset_fetch(req, version)
    }

    pub fn handle_describe_groups(
        &self,
        req: &kafka_protocol::messages::DescribeGroupsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeGroupsResponse {
        self.state.lock().unwrap().handle_describe_groups(req, version)
    }

    pub fn handle_list_groups(
        &self,
        req: &kafka_protocol::messages::ListGroupsRequest,
        version: i16,
    ) -> kafka_protocol::messages::ListGroupsResponse {
        self.state.lock().unwrap().handle_list_groups(req, version)
    }

    pub fn handle_delete_groups(
        &self,
        req: &kafka_protocol::messages::DeleteGroupsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DeleteGroupsResponse {
        self.state.lock().unwrap().handle_delete_groups(req, version)
    }

    pub fn handle_describe_configs(
        &self,
        req: &kafka_protocol::messages::DescribeConfigsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeConfigsResponse {
        self.state.lock().unwrap().handle_describe_configs(req, version)
    }

    pub fn handle_describe_cluster(
        &self,
        req: &kafka_protocol::messages::DescribeClusterRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeClusterResponse {
        self.state.lock().unwrap().handle_describe_cluster(req, version)
    }

    pub fn handle_offset_for_leader_epoch(
        &self,
        req: &kafka_protocol::messages::OffsetForLeaderEpochRequest,
        version: i16,
    ) -> kafka_protocol::messages::OffsetForLeaderEpochResponse {
        self.state.lock().unwrap().handle_offset_for_leader_epoch(req, version)
    }

    pub fn handle_add_partitions_to_txn(
        &self,
        req: &kafka_protocol::messages::AddPartitionsToTxnRequest,
        version: i16,
    ) -> kafka_protocol::messages::AddPartitionsToTxnResponse {
        self.state.lock().unwrap().handle_add_partitions_to_txn(req, version)
    }

    pub fn handle_add_offsets_to_txn(
        &self,
        req: &kafka_protocol::messages::AddOffsetsToTxnRequest,
        version: i16,
    ) -> kafka_protocol::messages::AddOffsetsToTxnResponse {
        self.state.lock().unwrap().handle_add_offsets_to_txn(req, version)
    }

    pub fn handle_end_txn(
        &self,
        req: &kafka_protocol::messages::EndTxnRequest,
        version: i16,
    ) -> kafka_protocol::messages::EndTxnResponse {
        self.state.lock().unwrap().handle_end_txn(req, version)
    }

    pub fn handle_txn_offset_commit(
        &self,
        req: &kafka_protocol::messages::TxnOffsetCommitRequest,
        version: i16,
    ) -> kafka_protocol::messages::TxnOffsetCommitResponse {
        self.state.lock().unwrap().handle_txn_offset_commit(req, version)
    }

    pub fn handle_alter_configs(
        &self,
        req: &kafka_protocol::messages::AlterConfigsRequest,
        version: i16,
    ) -> kafka_protocol::messages::AlterConfigsResponse {
        self.state.lock().unwrap().handle_alter_configs(req, version)
    }

    pub fn handle_incremental_alter_configs(
        &self,
        req: &kafka_protocol::messages::IncrementalAlterConfigsRequest,
        version: i16,
    ) -> kafka_protocol::messages::IncrementalAlterConfigsResponse {
        self.state.lock().unwrap().handle_incremental_alter_configs(req, version)
    }

    pub fn handle_delete_records(
        &self,
        req: &kafka_protocol::messages::DeleteRecordsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DeleteRecordsResponse {
        self.state.lock().unwrap().handle_delete_records(req, version)
    }

    pub fn handle_offset_delete(
        &self,
        req: &kafka_protocol::messages::OffsetDeleteRequest,
        version: i16,
    ) -> kafka_protocol::messages::OffsetDeleteResponse {
        self.state.lock().unwrap().handle_offset_delete(req, version)
    }

    pub fn handle_describe_transactions(
        &self,
        req: &kafka_protocol::messages::DescribeTransactionsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeTransactionsResponse {
        self.state.lock().unwrap().handle_describe_transactions(req, version)
    }

    pub fn handle_list_transactions(
        &self,
        req: &kafka_protocol::messages::ListTransactionsRequest,
        version: i16,
    ) -> kafka_protocol::messages::ListTransactionsResponse {
        self.state.lock().unwrap().handle_list_transactions(req, version)
    }

    pub fn handle_describe_producers(
        &self,
        req: &kafka_protocol::messages::DescribeProducersRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeProducersResponse {
        self.state.lock().unwrap().handle_describe_producers(req, version)
    }

    pub fn handle_describe_log_dirs(
        &self,
        req: &kafka_protocol::messages::DescribeLogDirsRequest,
        version: i16,
    ) -> kafka_protocol::messages::DescribeLogDirsResponse {
        self.state.lock().unwrap().handle_describe_log_dirs(req, version)
    }

    pub fn handle_sasl_handshake(
        &self,
        req: &kafka_protocol::messages::SaslHandshakeRequest,
        version: i16,
    ) -> kafka_protocol::messages::SaslHandshakeResponse {
        self.state.lock().unwrap().handle_sasl_handshake(req, version)
    }

    pub fn handle_sasl_authenticate(
        &self,
        req: &kafka_protocol::messages::SaslAuthenticateRequest,
        version: i16,
    ) -> kafka_protocol::messages::SaslAuthenticateResponse {
        self.state.lock().unwrap().handle_sasl_authenticate(req, version)
    }
}
