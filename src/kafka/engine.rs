//! Engine and state machine for Kafka implementation.

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
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
    ProducerId, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PartitionState {
    pub id: i32,
    pub leader: i32,
    // (base_offset, batch_bytes) — batches don't hold one record each, so
    // fetching by offset has to find the batch that *contains* the
    // requested offset, not treat this vec as if it were indexed by offset.
    pub record_batches: Vec<(i64, Vec<u8>)>,
    pub high_watermark: i64,
    // (producer_id, epoch) -> (last_sequence, base_offset)
    pub producer_seqs: HashMap<(i64, i16), (i32, i64)>,
    // producer_id -> first_offset of its still-open transaction on this
    // partition. Populated when a transactional batch is produced,
    // cleared on EndTxn. Used to compute the last-stable-offset (LSO): a
    // read_committed fetch never sees past the earliest still-open
    // transaction.
    pub active_txns: HashMap<i64, i64>,
    // (producer_id, first_offset) for every aborted transaction whose
    // first_offset is still >= the log's earliest retained offset —
    // reported to read_committed fetchers via FetchResponse's
    // aborted_transactions so they know to skip those records even
    // though they're physically still in the log.
    pub aborted_txns: Vec<(i64, i64)>,
    // The earliest offset still readable: moved forward by retention and
    // DeleteRecords, never by compaction (see log.rs).
    pub log_start_offset: i64,
    pub segments: Vec<super::log::Segment>,
    // Tombstone offset -> when the cleaner may drop it (set on the first
    // clean that sees it, `delete.retention.ms` later).
    pub tombstone_horizons: std::collections::BTreeMap<i64, i64>,
    // Everything below this offset has already been compacted.
    pub clean_offset: i64,
}

impl PartitionState {
    pub fn new(id: i32, leader: i32) -> Self {
        Self {
            id,
            leader,
            record_batches: Vec::new(),
            high_watermark: 0,
            producer_seqs: HashMap::new(),
            active_txns: HashMap::new(),
            aborted_txns: Vec::new(),
            log_start_offset: 0,
            segments: Vec::new(),
            tombstone_horizons: Default::default(),
            clean_offset: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicState {
    pub name: String,
    pub is_internal: bool,
    pub partitions: HashMap<i32, PartitionState>,
    pub configs: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupLifecycleState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl GroupLifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub last_heartbeat_ms: i64,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct GroupState {
    pub group_id: String,
    pub state: GroupLifecycleState,
    pub protocol_type: String,
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub members: HashMap<String, GroupMember>,
    pub pending_member_ids: HashSet<String>,
    pub awaiting_members: HashMap<String, Vec<(String, Vec<u8>)>>,
    pub assignments: HashMap<String, Vec<u8>>,
    pub rebalance_start_ms: i64,
    /// A freshly formed group's first round waits until this (wall-clock)
    /// instant for more members, like Kafka's
    /// `group.initial.rebalance.delay.ms` -- so consumers started together
    /// land in one round instead of racing.
    pub initial_until: Option<std::time::Instant>,
}

/// noida-db's `group.initial.rebalance.delay.ms` (Kafka's default is 3s).
const INITIAL_REBALANCE_DELAY: std::time::Duration = std::time::Duration::from_millis(300);

impl GroupState {
    /// Completes a rebalance once every member has (re)joined: the round
    /// moves to CompletingRebalance and a protocol all members support is
    /// chosen (the leader's first such). Called on join *and* on leave --
    /// found via testing before a public release: a member leaving while
    /// everyone else had already rejoined never completed the round, so
    /// the rest sat in JoinGroup until the rebalance timeout (5 minutes).
    pub fn try_complete_join(&mut self) {
        if self.initial_until.is_some_and(|t| std::time::Instant::now() < t) {
            return;
        }
        self.initial_until = None;
        if self.state != GroupLifecycleState::PreparingRebalance
            || self.members.is_empty()
            || !self.members.keys().all(|m| self.awaiting_members.contains_key(m))
        {
            return;
        }
        self.awaiting_members.retain(|m, _| self.members.contains_key(m));
        self.state = GroupLifecycleState::CompletingRebalance;
        if self.leader_id.as_ref().is_none_or(|l| !self.members.contains_key(l)) {
            self.leader_id = self.awaiting_members.keys().next().cloned();
        }
        if let Some(leader_id) = &self.leader_id
            && let Some(leader_protos) = self.awaiting_members.get(leader_id)
        {
            for (proto_name, _) in leader_protos {
                let supported_by_all = self
                    .awaiting_members
                    .values()
                    .all(|protos| protos.iter().any(|(p, _)| p == proto_name));
                if supported_by_all {
                    self.protocol_name = Some(proto_name.clone());
                    break;
                }
            }
        }
    }

    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: GroupLifecycleState::Empty,
            protocol_type: String::new(),
            protocol_name: None,
            generation_id: 0,
            leader_id: None,
            members: HashMap::new(),
            pending_member_ids: HashSet::new(),
            awaiting_members: HashMap::new(),
            assignments: HashMap::new(),
            rebalance_start_ms: 0,
            initial_until: None,
        }
    }
}

pub struct EngineState {
    pub topics: HashMap<String, TopicState>,
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
    pub next_producer_id: i64,
    pub next_member_counter: u64,
    // (group_id, topic_name, partition) -> offset
    pub committed_offsets: HashMap<(String, String, i32), i64>,
    // (group_id, topic_name, partition) -> the metadata string committed
    // with that offset (absent = "").
    pub offset_metadata: HashMap<(String, String, i32), String>,
    pub groups: HashMap<String, GroupState>,
    pub broker_configs: HashMap<String, String>,
    pub clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    // producer_id -> its current epoch. Set by InitProducerId, checked by
    // every Produce/AddPartitionsToTxn/EndTxn carrying that producer_id: a
    // lower epoch means a zombie/superseded producer instance and gets
    // fenced (INVALID_PRODUCER_EPOCH) rather than accepted.
    pub producer_epochs: HashMap<i64, i16>,
    // transactional.id -> (producer_id, current epoch).
    pub txn_producers: HashMap<String, (i64, i16)>,
    // transactional.id -> (topic, partition) set added via
    // AddPartitionsToTxn for the transaction currently in progress.
    pub txn_partitions: HashMap<String, HashSet<(String, i32)>>,
}

impl std::fmt::Debug for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineState")
            .field("topics", &self.topics)
            .field("broker_id", &self.broker_id)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("cluster_id", &self.cluster_id)
            .field("next_producer_id", &self.next_producer_id)
            .field("committed_offsets", &self.committed_offsets)
            .field("groups", &self.groups)
            .finish()
    }
}

/// Builds a v2 record batch containing one control record — the commit or
/// abort marker real Kafka appends to every partition a transaction
/// touched, once it ends. Uses kafka_protocol's own encoder rather than
/// hand-writing the batch header/CRC/varint-length record framing, since
/// a real client's decoder validates all of that strictly.
fn encode_control_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_offset: i64,
    committed: bool,
    now_ms: i64,
) -> Vec<u8> {
    // EndTxnMarker (KIP-98): a 2-byte version followed by the 4-byte
    // coordinator epoch. The control record's key holds the marker type
    // (0 = abort, 1 = commit) the same way; noida-db has no real
    // transaction-coordinator epoch to report, so 0 stands in for it —
    // nothing here reads it back.
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes());
    key.extend_from_slice(&(if committed { 1i16 } else { 0i16 }).to_be_bytes());
    let mut value = Vec::with_capacity(6);
    value.extend_from_slice(&0i16.to_be_bytes());
    value.extend_from_slice(&0i32.to_be_bytes());

    let record = Record {
        transactional: true,
        control: true,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id,
        producer_epoch,
        timestamp_type: TimestampType::Creation,
        offset: base_offset,
        sequence: -1,
        timestamp: now_ms,
        key: Some(bytes::Bytes::from(key)),
        value: Some(bytes::Bytes::from(value)),
        headers: Default::default(),
    };

    let mut buf = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: Compression::None };
    RecordBatchEncoder::encode(&mut buf, std::iter::once(&record), &options)
        .expect("encoding a single uncompressed control record cannot fail");
    buf.to_vec()
}

impl EngineState {
    pub fn new(host: String, port: i32) -> Self {
        let mut state = Self {
            topics: HashMap::new(),
            broker_id: 1,
            host,
            port,
            // Stable base64 UUID style string for cluster id
            cluster_id: "MkU3OEVBNTctOEUyRi00".to_string(),
            next_producer_id: 1000,
            next_member_counter: 1,
            committed_offsets: HashMap::new(),
            offset_metadata: HashMap::new(),
            groups: HashMap::new(),
            broker_configs: HashMap::new(),
            clock: None,
            producer_epochs: HashMap::new(),
            txn_producers: HashMap::new(),
            txn_partitions: HashMap::new(),
        };

        // Pre-create internal topics as a real KRaft broker does
        let mut consumer_offsets = TopicState {
            name: "__consumer_offsets".to_string(),
            is_internal: true,
            partitions: HashMap::new(),
            configs: HashMap::new(),
        };
        for p in 0..50 {
            consumer_offsets.partitions.insert(p, PartitionState::new(p, state.broker_id));
        }
        state.topics.insert("__consumer_offsets".to_string(), consumer_offsets);

        let mut txn_state = TopicState {
            name: "__transaction_state".to_string(),
            is_internal: true,
            partitions: HashMap::new(),
            configs: HashMap::new(),
        };
        for p in 0..50 {
            txn_state.partitions.insert(p, PartitionState::new(p, state.broker_id));
        }
        state.topics.insert("__transaction_state".to_string(), txn_state);

        state
    }

    pub fn now_ms(&self) -> i64 {
        if let Some(ref clock) = self.clock {
            clock() as i64
        } else {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
        }
    }

    pub fn handle_api_versions(&self, _version: i16) -> ApiVersionsResponse {
        let mut res = ApiVersionsResponse::default();
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
            _ => self.topics.keys().cloned().collect(),
        };

        for topic_name in topic_names_to_query {
            let mut topic_res = MetadataResponseTopic::default();
            topic_res.name = Some(kafka_protocol::messages::TopicName::from(
                StrBytes::from_string(topic_name.clone()),
            ));

            if let Some(state) = self.topics.get(&topic_name) {
                topic_res.error_code = 0;
                topic_res.is_internal = state.is_internal;
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
                let mut topic_state = TopicState {
                    name: topic_name.clone(),
                    is_internal: false,
                    partitions: HashMap::new(),
                    configs: HashMap::new(),
                };
                topic_state.partitions.insert(0, PartitionState::new(0, self.broker_id));

                let mut part_res = MetadataResponsePartition::default();
                part_res.partition_index = 0;
                part_res.leader_id = kafka_protocol::messages::BrokerId(self.broker_id);
                part_res.replica_nodes = vec![kafka_protocol::messages::BrokerId(self.broker_id)];
                part_res.isr_nodes = vec![kafka_protocol::messages::BrokerId(self.broker_id)];
                topic_res.partitions.push(part_res);
                topic_res.is_internal = false;

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
            let config_error = topic.configs.iter().find_map(|c| {
                let value = c.value.as_ref().map_or("", |v| v.as_str());
                super::log::validate_topic_config(c.name.as_str(), value).err()
            });

            if let Err(msg) = super::log::validate_topic_name(topic_name_str) {
                topic_res.error_code = 17; // INVALID_TOPIC_EXCEPTION
                topic_res.error_message = Some(StrBytes::from_string(msg));
            } else if let Some(msg) = config_error {
                topic_res.error_code = 40; // INVALID_CONFIG
                topic_res.error_message = Some(StrBytes::from_string(msg));
            } else if topic.num_partitions <= 0 {
                topic_res.error_code = 37; // INVALID_PARTITIONS
            } else if topic.replication_factor > 1 {
                topic_res.error_code = 38; // INVALID_REPLICATION_FACTOR
            } else if self.topics.contains_key(topic_name_str) {
                topic_res.error_code = 36; // TOPIC_ALREADY_EXISTS
            } else {
                topic_res.error_code = 0;
                if !req.validate_only {
                    let mut topic_state = TopicState {
                        name: topic_name_str.to_string(),
                        is_internal: false,
                        partitions: HashMap::new(),
                        configs: HashMap::new(),
                    };
                    for p in 0..topic.num_partitions {
                        topic_state.partitions.insert(p, PartitionState::new(p, self.broker_id));
                    }
                    for conf in &topic.configs {
                        if let Some(val) = &conf.value {
                            topic_state.configs.insert(conf.name.to_string(), val.to_string());
                        }
                    }
                    self.topics.insert(topic_name_str.to_string(), topic_state);
                }
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

                if new_count <= current_count {
                    // Partitions can only be increased
                    topic_res.error_code = 37; // INVALID_PARTITIONS
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
        req: &InitProducerIdRequest,
        _version: i16,
    ) -> InitProducerIdResponse {
        let mut res = InitProducerIdResponse::default();

        // As a broker validates it (observed against Kafka 3.8): a
        // transactional.id is null or non-empty ("" is INVALID_REQUEST, not
        // "no id" — clients send null), a transactional producer's timeout
        // is within (0, transaction.max.timeout.ms = 15 minutes], and a
        // producer id and epoch are given together or not at all.
        let tx_id = req.transactional_id.as_ref().map(|t| t.as_str().to_string());
        if tx_id.as_deref() == Some("") || (req.producer_id.0 == -1) != (req.producer_epoch == -1) {
            res.error_code = 42; // INVALID_REQUEST
            res.producer_id = ProducerId(-1);
            res.producer_epoch = -1;
            return res;
        }
        if tx_id.is_some() && !(1..=900_000).contains(&req.transaction_timeout_ms) {
            res.error_code = 50; // INVALID_TRANSACTION_TIMEOUT
            res.producer_id = ProducerId(-1);
            res.producer_epoch = -1;
            return res;
        }
        let (pid, epoch) = match &tx_id {
            Some(tid) => {
                // A new InitProducerId for a known transactional.id fences
                // the previous producer instance (a "zombie" from a
                // restarted/duplicate app) by bumping the epoch — any
                // Produce/AddPartitionsToTxn/EndTxn still arriving with the
                // old epoch gets rejected once producer_epochs is updated.
                if let Some(&(pid, epoch)) = self.txn_producers.get(tid) {
                    (pid, epoch.wrapping_add(1))
                } else {
                    let pid = self.next_producer_id;
                    self.next_producer_id += 1;
                    (pid, 0)
                }
            }
            None => {
                let pid = self.next_producer_id;
                self.next_producer_id += 1;
                (pid, 0)
            }
        };

        if let Some(tid) = tx_id {
            self.txn_producers.insert(tid, (pid, epoch));
        }
        self.producer_epochs.insert(pid, epoch);

        res.error_code = 0;
        res.producer_id = ProducerId(pid);
        res.producer_epoch = epoch;
        res
    }

    pub fn run_log_cleaner(&mut self) {
        let now = self.now_ms();
        for topic in self.topics.values_mut() {
            if topic.is_internal {
                continue;
            }
            let cfg = super::log::LogConfig::of(&topic.configs);
            for part in topic.partitions.values_mut() {
                part.compact(&cfg, now);
                part.apply_retention(&cfg, now);
            }
        }
    }

    pub fn handle_produce(&mut self, req: &ProduceRequest, _version: i16) -> ProduceResponse {
        let mut res = ProduceResponse::default();
        let now = self.now_ms();

        for topic in &req.topic_data {
            let mut topic_res = TopicProduceResponse::default();
            topic_res.name = topic.name.clone();

            let topic_name = topic.name.as_str();
            for partition in &topic.partition_data {
                let mut part_res = PartitionProduceResponse::default();
                part_res.index = partition.index;

                if let Some(topic_state) = self.topics.get_mut(topic_name) {
                    let cfg = super::log::LogConfig::of(&topic_state.configs);
                    if let Some(part_state) = topic_state.partitions.get_mut(&partition.index) {
                        if let Some(records) = &partition.records {
                            // Like a broker, accept only well-formed v2
                            // record batches: one batch whose length field
                            // covers the payload exactly, with a valid CRC.
                            if !is_valid_v2_batch(records) {
                                part_res.error_code = 2; // CORRUPT_MESSAGE
                                part_res.base_offset = -1;
                                part_res.log_append_time_ms = -1;
                                topic_res.partition_responses.push(part_res);
                                continue;
                            }
                            let is_batch_v2 = true;
                            if records.len() as i64 > cfg.max_message_bytes {
                                part_res.error_code = 10; // MESSAGE_TOO_LARGE
                                topic_res.partition_responses.push(part_res);
                                continue;
                            }
                            // A compacted topic keeps the latest record per
                            // key, so a record without one is rejected.
                            if cfg.compact && is_batch_v2 {
                                let mut buf = records.clone();
                                let keyless =
                                    kafka_protocol::records::RecordBatchDecoder::decode(&mut buf)
                                        .map(|set| {
                                            set.records
                                                .iter()
                                                .position(|r| r.key.is_none() && !r.control)
                                        })
                                        .unwrap_or(None);
                                if let Some(index) = keyless {
                                    use kafka_protocol::messages::produce_response::BatchIndexAndErrorMessage;
                                    let msg = format!(
                                        "Compacted topic cannot accept message without key in \
                                         topic partition {}-{}.",
                                        topic_name, partition.index
                                    );
                                    part_res.error_code = 87; // INVALID_RECORD
                                    part_res.error_message =
                                        Some(StrBytes::from_string(msg.clone()));
                                    let mut rec_err = BatchIndexAndErrorMessage::default();
                                    rec_err.batch_index = index as i32;
                                    rec_err.batch_index_error_message =
                                        Some(StrBytes::from_string(msg));
                                    part_res.record_errors.push(rec_err);
                                    part_res.base_offset = -1;
                                    part_res.log_append_time_ms = -1;
                                    topic_res.partition_responses.push(part_res);
                                    continue;
                                }
                            }
                            // `lastOffsetDelta` = number of records in the batch minus
                            // one; every batch (idempotent or not) needs it to advance
                            // the high watermark by the right amount, not just to track
                            // idempotent producer sequences.
                            let last_offset_delta_v2 = if is_batch_v2 {
                                i32::from_be_bytes(records[23..27].try_into().unwrap())
                            } else {
                                -1
                            };
                            if is_batch_v2 {
                                let attributes =
                                    i16::from_be_bytes(records[21..23].try_into().unwrap());
                                // Attributes bit 4: isTransactional (see
                                // Kafka's RecordBatch wire format).
                                let is_transactional = attributes & 0x0010 != 0;
                                let producer_id =
                                    i64::from_be_bytes(records[43..51].try_into().unwrap());
                                let producer_epoch =
                                    i16::from_be_bytes(records[51..53].try_into().unwrap());
                                let base_sequence =
                                    i32::from_be_bytes(records[53..57].try_into().unwrap());
                                let last_offset_delta = last_offset_delta_v2;

                                if producer_id >= 0 {
                                    // A stale/zombie producer instance:
                                    // InitProducerId bumped the epoch for
                                    // this producer_id (a newer instance
                                    // took over), so this older-epoch
                                    // Produce is rejected rather than
                                    // silently accepted.
                                    if let Some(&expected_epoch) =
                                        self.producer_epochs.get(&producer_id)
                                        && producer_epoch < expected_epoch
                                    {
                                        part_res.error_code = 47; // INVALID_PRODUCER_EPOCH
                                        topic_res.partition_responses.push(part_res);
                                        continue;
                                    }

                                    if let Some(&(last_seq, prev_base_offset)) =
                                        part_state.producer_seqs.get(&(producer_id, producer_epoch))
                                    {
                                        if base_sequence <= last_seq {
                                            // Duplicate batch acknowledged without re-append
                                            part_res.error_code = 0;
                                            part_res.base_offset = prev_base_offset;
                                            part_res.log_append_time_ms =
                                                if cfg.log_append_time { now } else { -1 };
                                            topic_res.partition_responses.push(part_res);
                                            continue;
                                        } else if base_sequence > last_seq + 1 {
                                            // Gap in sequence numbers
                                            part_res.error_code = 45; // OUT_OF_ORDER_SEQUENCE_NUMBER
                                            topic_res.partition_responses.push(part_res);
                                            continue;
                                        }
                                    } else if base_sequence != 0 {
                                        // First sequence must be 0
                                        part_res.error_code = 45; // OUT_OF_ORDER_SEQUENCE_NUMBER
                                        topic_res.partition_responses.push(part_res);
                                        continue;
                                    }

                                    let last_seq = base_sequence + last_offset_delta;
                                    part_state.producer_seqs.insert(
                                        (producer_id, producer_epoch),
                                        (last_seq, part_state.high_watermark),
                                    );

                                    // First transactional batch from this
                                    // producer on this partition: remember
                                    // where its (still uncommitted) records
                                    // start, so a read_committed fetch's
                                    // last-stable-offset can't pass it
                                    // until EndTxn clears this entry.
                                    if is_transactional {
                                        part_state
                                            .active_txns
                                            .entry(producer_id)
                                            .or_insert(part_state.high_watermark);
                                    }
                                }
                            }

                            // A record batch (v2) carries `last_offset_delta` =
                            // number of records in it minus one; every record
                            // in the batch gets its own offset, so the log's
                            // high watermark must advance by the record count,
                            // not by one per Produce call. Getting this wrong
                            // silently drops every record in a batch after the
                            // first whenever a producer batches more than one
                            // record per partition (the common case, not just
                            // an idempotent-producer edge case).
                            let delta = if is_batch_v2 && last_offset_delta_v2 >= 0 {
                                last_offset_delta_v2 as i64 + 1
                            } else {
                                1
                            };
                            let mut batch_bytes = records.to_vec();
                            // `message.timestamp.type=LogAppendTime`: the
                            // broker stamps every record with its own clock.
                            if cfg.log_append_time && is_batch_v2 {
                                batch_bytes =
                                    stamp_log_append_time(&batch_bytes, now).unwrap_or(batch_bytes);
                            }
                            let base_offset =
                                part_state.append_batch(batch_bytes, delta, now, Some(&cfg));

                            part_res.error_code = 0;
                            part_res.base_offset = base_offset;
                            part_res.log_append_time_ms =
                                if cfg.log_append_time { now } else { -1 };
                            part_res.log_start_offset = part_state.log_start_offset;
                        } else {
                            part_res.error_code = 0;
                            part_res.base_offset = part_state.high_watermark;
                            part_res.log_append_time_ms = -1;
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
        use kafka_protocol::messages::fetch_response::AbortedTransaction;

        let mut res = FetchResponse::default();
        // isolation_level: 0 = read_uncommitted (default, sees everything
        // immediately), 1 = read_committed (never sees past the earliest
        // still-open transaction).
        let read_committed = req.isolation_level == 1;

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
                        part_res.log_start_offset = part_state.log_start_offset;

                        // Last-stable-offset: the high watermark, capped to
                        // just before the earliest still-open transaction
                        // on this partition (if any). A read_uncommitted
                        // fetch ignores this and reads straight to the
                        // high watermark, same as before this existed.
                        let lso = part_state
                            .active_txns
                            .values()
                            .copied()
                            .min()
                            .map_or(part_state.high_watermark, |min_open| {
                                min_open.min(part_state.high_watermark)
                            });
                        let visible_up_to =
                            if read_committed { lso } else { part_state.high_watermark };
                        part_res.last_stable_offset = lso;

                        let fetch_offset = partition.fetch_offset;
                        if fetch_offset < part_state.log_start_offset
                            || fetch_offset > part_state.high_watermark
                        {
                            part_res.error_code = 1; // OFFSET_OUT_OF_RANGE
                        } else if fetch_offset < visible_up_to {
                            // Batches aren't one record each: find the batch
                            // whose base_offset covers fetch_offset (the
                            // last one starting at or before it), then
                            // concatenate it and every following batch up to
                            // visible_up_to — real Kafka's Fetch response is
                            // as many whole batches as fit, not just one, and
                            // returning only one made every poll() need one
                            // extra round-trip per batch, which a real
                            // client's default poll budget doesn't always
                            // afford (this is what made the transactional
                            // producer integration test flaky/incomplete,
                            // not a transactions bug: see docs/specs/kafka.md).
                            // The first batch that still has records at or
                            // past fetch_offset — compaction leaves gaps, so
                            // the batch covering it may be gone and the next
                            // one is where the log continues.
                            let start = part_state.record_batches.iter().position(|(base, b)| {
                                super::log::batch_last_offset(*base, b) >= fetch_offset
                            });
                            if let Some(start) = start {
                                let mut out = Vec::new();
                                for (base, batch) in &part_state.record_batches[start..] {
                                    if *base >= visible_up_to {
                                        break;
                                    }
                                    // Real Kafka includes an aborted batch's
                                    // bytes in the fetch response too,
                                    // relying on the client to discard it
                                    // using aborted_transactions below. This
                                    // engine doesn't replicate that
                                    // client-side bookkeeping, so it
                                    // simplifies to: a read_committed fetch
                                    // never serves a batch whose base_offset
                                    // is a known aborted transaction's start.
                                    let is_this_batch_aborted = read_committed
                                        && part_state
                                            .aborted_txns
                                            .iter()
                                            .any(|&(_, first_offset)| first_offset == *base);
                                    if !is_this_batch_aborted {
                                        out.extend_from_slice(batch);
                                    }
                                }
                                if !out.is_empty() {
                                    part_res.records = Some(bytes::Bytes::from(out));
                                }
                            }
                        }

                        if read_committed {
                            let aborted: Vec<AbortedTransaction> = part_state
                                .aborted_txns
                                .iter()
                                .filter(|&&(_, first_offset)| first_offset < lso)
                                .map(|&(producer_id, first_offset)| {
                                    let mut at = AbortedTransaction::default();
                                    at.producer_id = ProducerId(producer_id);
                                    at.first_offset = first_offset;
                                    at
                                })
                                .collect();
                            if !aborted.is_empty() {
                                part_res.aborted_transactions = Some(aborted);
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
                        part_res.timestamp = -1;
                        // A read_committed client's "latest" is the last
                        // stable offset, never past an open transaction.
                        let lso = part_state
                            .active_txns
                            .values()
                            .copied()
                            .min()
                            .map_or(part_state.high_watermark, |m| {
                                m.min(part_state.high_watermark)
                            });
                        let end =
                            if req.isolation_level == 1 { lso } else { part_state.high_watermark };
                        match partition.timestamp {
                            -2 => part_res.offset = part_state.log_start_offset,
                            -1 => part_res.offset = end,
                            // A timestamp (or -3, the record with the largest
                            // timestamp). Found via testing before a public
                            // release: every timestamp lookup used to answer
                            // with the end offset, so `offsetsForTimes` and
                            // "replay from 10 minutes ago" skipped everything.
                            ts => {
                                let (offset, found_ts) = offset_for_timestamp(part_state, ts, end);
                                part_res.offset = offset;
                                part_res.timestamp = found_ts;
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

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_find_coordinator(
        &self,
        req: &kafka_protocol::messages::FindCoordinatorRequest,
        version: i16,
    ) -> kafka_protocol::messages::FindCoordinatorResponse {
        let mut res = kafka_protocol::messages::FindCoordinatorResponse::default();

        if version <= 3 {
            res.node_id = kafka_protocol::messages::BrokerId(self.broker_id);
            res.host = StrBytes::from_string(self.host.clone());
            res.port = self.port;
            res.error_code = 0;
        } else {
            let keys = if !req.coordinator_keys.is_empty() {
                req.coordinator_keys.clone()
            } else {
                vec![req.key.clone()]
            };

            for k in keys {
                let mut coord = Coordinator::default();
                coord.key = k;
                coord.node_id = kafka_protocol::messages::BrokerId(self.broker_id);
                coord.host = StrBytes::from_string(self.host.clone());
                coord.port = self.port;
                coord.error_code = 0;
                res.coordinators.push(coord);
            }
        }

        res
    }

    /// `client_id` / `client_host` are the joining connection's, as Kafka
    /// reports them for each member (and prefixes member ids with the
    /// client id).
    pub fn handle_join_group(
        &mut self,
        req: &kafka_protocol::messages::JoinGroupRequest,
        version: i16,
        client_id: &str,
        client_host: &str,
    ) -> kafka_protocol::messages::JoinGroupResponse {
        let mut res = kafka_protocol::messages::JoinGroupResponse::default();
        let group_id = req.group_id.as_str().to_string();

        // The coordinator's request checks, in Kafka's order: a group id,
        // a session timeout within group.min/max.session.timeout.ms
        // (6s..30min by default), and at least one protocol of a named type.
        let reject = |code: i16| {
            let mut res = kafka_protocol::messages::JoinGroupResponse::default();
            res.error_code = code;
            res.generation_id = -1;
            res.member_id = req.member_id.clone();
            res
        };
        if group_id.is_empty() {
            return reject(24); // INVALID_GROUP_ID
        }
        if !(6_000..=1_800_000).contains(&req.session_timeout_ms) {
            return reject(26); // INVALID_SESSION_TIMEOUT
        }
        if req.protocol_type.is_empty() || req.protocols.is_empty() {
            return reject(23); // INCONSISTENT_GROUP_PROTOCOL
        }

        let now = self.now_ms();
        let group = self
            .groups
            .entry(group_id.clone())
            .or_insert_with(|| GroupState::new(group_id.clone()));

        // A deleted group's id is free again: joining it starts a new
        // group, as on Kafka.
        if group.state == GroupLifecycleState::Dead {
            *group = GroupState::new(group_id.clone());
        }

        // KIP-394: dynamic member join with empty member_id returns MEMBER_ID_REQUIRED (79)
        // for API version >= 4. For v0-v3, coordinator assigns member_id in the first JoinGroup.
        if version >= 4 && req.member_id.is_empty() {
            let assigned_id = new_member_id(client_id, &mut self.next_member_counter);
            group.pending_member_ids.insert(assigned_id.clone());

            res.error_code = 79; // MEMBER_ID_REQUIRED
            res.generation_id = -1;
            res.member_id = StrBytes::from_string(assigned_id);
            res.leader = StrBytes::from_static_str("");
            return res;
        }

        let m_id = if req.member_id.is_empty() {
            new_member_id(client_id, &mut self.next_member_counter)
        } else {
            req.member_id.as_str().to_string()
        };

        let is_known = req.member_id.is_empty()
            || group.members.contains_key(&m_id)
            || group.pending_member_ids.remove(&m_id);

        if !is_known {
            res.error_code = 25; // UNKNOWN_MEMBER_ID
            return res;
        }

        let req_proto_type = req.protocol_type.as_str().to_string();
        if !group.protocol_type.is_empty() && group.protocol_type != req_proto_type {
            res.error_code = 23; // INCONSISTENT_GROUP_PROTOCOL
            return res;
        }
        if group.protocol_type.is_empty() {
            group.protocol_type = req_proto_type.clone();
        }

        let protocols_vec: Vec<(String, Vec<u8>)> = req
            .protocols
            .iter()
            .map(|p| (p.name.as_str().to_string(), p.metadata.to_vec()))
            .collect();

        let member = GroupMember {
            member_id: m_id.clone(),
            group_instance_id: req.group_instance_id.as_ref().map(|s| s.as_str().to_string()),
            client_id: client_id.to_string(),
            client_host: client_host.to_string(),
            session_timeout_ms: req.session_timeout_ms,
            rebalance_timeout_ms: req.rebalance_timeout_ms,
            protocol_type: req_proto_type,
            protocols: protocols_vec.clone(),
            last_heartbeat_ms: now,
            assignment: Vec::new(),
        };

        // A member that's new to the group (or whose subscription changed)
        // arriving while a round is completing must start a new round, as
        // Kafka's coordinator does. Found via testing before a public
        // release: it used to be folded into the round already completing,
        // whose leader had computed assignments without it -- so two
        // consumers starting together often left one with every partition
        // and the other with none, indefinitely.
        let changed = group.members.get(&m_id).is_none_or(|m| m.protocols != protocols_vec);
        if group.state == GroupLifecycleState::CompletingRebalance && changed {
            group.state = GroupLifecycleState::PreparingRebalance;
            group.generation_id += 1;
            group.rebalance_start_ms = now;
            group.assignments.clear();
            group.awaiting_members.clear();
        }

        group.members.insert(m_id.clone(), member);
        group.awaiting_members.insert(m_id.clone(), protocols_vec);

        // State transition: trigger rebalance if Empty or Stable
        if group.state == GroupLifecycleState::Empty || group.state == GroupLifecycleState::Stable {
            if group.state == GroupLifecycleState::Empty {
                group.initial_until = Some(std::time::Instant::now() + INITIAL_REBALANCE_DELAY);
            }
            group.state = GroupLifecycleState::PreparingRebalance;
            group.generation_id += 1;
            group.rebalance_start_ms = now;
            group.assignments.clear();
        }

        if group.leader_id.is_none()
            || !group.members.contains_key(group.leader_id.as_ref().unwrap())
        {
            group.leader_id = Some(m_id.clone());
        }

        // Complete rebalance when all known members have joined
        group.try_complete_join();
        if group.protocol_name.is_none() && group.state == GroupLifecycleState::CompletingRebalance
        {
            group.protocol_name = req.protocols.first().map(|p| p.name.as_str().to_string());
        }

        res.error_code = 0;
        res.generation_id = group.generation_id;
        res.protocol_name = group.protocol_name.as_ref().map(|s| StrBytes::from_string(s.clone()));
        res.leader = StrBytes::from_string(group.leader_id.clone().unwrap_or_default());
        res.member_id = StrBytes::from_string(m_id.clone());

        // Leader gets the full list of members and their protocol metadata; followers get empty
        if Some(&m_id) == group.leader_id.as_ref() {
            for (member_id_str, protos) in &group.awaiting_members {
                let mut member_res = JoinGroupResponseMember::default();
                member_res.member_id = StrBytes::from_string(member_id_str.clone());
                let meta = protos
                    .iter()
                    .find(|(n, _)| Some(n) == group.protocol_name.as_ref())
                    .map(|(_, m)| bytes::Bytes::copy_from_slice(m))
                    .unwrap_or_default();
                member_res.metadata = meta;
                res.members.push(member_res);
            }
        }

        let _ = version;
        res
    }

    pub fn handle_sync_group(
        &mut self,
        req: &kafka_protocol::messages::SyncGroupRequest,
        _version: i16,
    ) -> kafka_protocol::messages::SyncGroupResponse {
        let mut res = kafka_protocol::messages::SyncGroupResponse::default();
        let group_id = req.group_id.as_str().to_string();
        let m_id = req.member_id.as_str().to_string();

        let group = match self.groups.get_mut(&group_id) {
            Some(g) if g.state != GroupLifecycleState::Dead => g,
            _ => {
                res.error_code = 25; // UNKNOWN_MEMBER_ID
                return res;
            }
        };

        if !group.members.contains_key(&m_id) {
            res.error_code = 25; // UNKNOWN_MEMBER_ID
            return res;
        }

        if group.state == GroupLifecycleState::PreparingRebalance {
            res.error_code = 27; // REBALANCE_IN_PROGRESS
            return res;
        }

        if req.generation_id != group.generation_id {
            res.error_code = 22; // ILLEGAL_GENERATION
            return res;
        }

        // If leader sends assignments, store them and transition to Stable
        if !req.assignments.is_empty() {
            for assignment in &req.assignments {
                group.assignments.insert(
                    assignment.member_id.as_str().to_string(),
                    assignment.assignment.to_vec(),
                );
            }
            group.state = GroupLifecycleState::Stable;
            group.awaiting_members.clear();
        } else if group.leader_id.as_ref() == Some(&m_id) && req.assignments.is_empty() {
            group.state = GroupLifecycleState::Stable;
            group.awaiting_members.clear();
        }

        if let Some(assignment) = group.assignments.get(&m_id) {
            res.assignment = bytes::Bytes::copy_from_slice(assignment);
        } else if let Some(m) = group.members.get(&m_id)
            && !m.assignment.is_empty()
        {
            res.assignment = bytes::Bytes::copy_from_slice(&m.assignment);
        }

        res.error_code = 0;
        res
    }

    pub fn handle_heartbeat(
        &mut self,
        req: &kafka_protocol::messages::HeartbeatRequest,
        _version: i16,
    ) -> kafka_protocol::messages::HeartbeatResponse {
        let mut res = kafka_protocol::messages::HeartbeatResponse::default();
        let group_id = req.group_id.as_str().to_string();
        let m_id = req.member_id.as_str().to_string();

        let now = self.now_ms();
        let group = match self.groups.get_mut(&group_id) {
            Some(g) if g.state != GroupLifecycleState::Dead => g,
            _ => {
                res.error_code = 25; // UNKNOWN_MEMBER_ID
                return res;
            }
        };

        if !group.members.contains_key(&m_id) {
            res.error_code = 25; // UNKNOWN_MEMBER_ID
            return res;
        }

        if group.state == GroupLifecycleState::PreparingRebalance {
            res.error_code = 27; // REBALANCE_IN_PROGRESS
            return res;
        }

        if req.generation_id != group.generation_id {
            res.error_code = 22; // ILLEGAL_GENERATION
            return res;
        }

        // Check session timeout
        let member = group.members.get_mut(&m_id).unwrap();
        if now - member.last_heartbeat_ms > member.session_timeout_ms as i64 {
            group.members.remove(&m_id);
            if group.members.is_empty() {
                group.state = GroupLifecycleState::Empty;
            } else {
                group.state = GroupLifecycleState::PreparingRebalance;
                group.generation_id += 1;
            }
            res.error_code = 25; // UNKNOWN_MEMBER_ID
            return res;
        }

        member.last_heartbeat_ms = now;
        res.error_code = 0;
        res
    }

    pub fn handle_leave_group(
        &mut self,
        req: &kafka_protocol::messages::LeaveGroupRequest,
        version: i16,
    ) -> kafka_protocol::messages::LeaveGroupResponse {
        use kafka_protocol::messages::leave_group_response::MemberResponse;
        let mut res = kafka_protocol::messages::LeaveGroupResponse::default();
        let group_id = req.group_id.as_str().to_string();

        let group = match self.groups.get_mut(&group_id) {
            Some(g) if g.state != GroupLifecycleState::Dead => g,
            _ => {
                res.error_code = 25; // UNKNOWN_MEMBER_ID
                return res;
            }
        };

        let members_to_leave: Vec<String> = if version >= 3 && !req.members.is_empty() {
            req.members.iter().map(|m| m.member_id.as_str().to_string()).collect()
        } else {
            vec![req.member_id.as_str().to_string()]
        };

        for m_id in members_to_leave {
            if group.members.remove(&m_id).is_some() {
                group.assignments.remove(&m_id);
                group.awaiting_members.remove(&m_id);

                if version >= 3 {
                    let mut m_resp = MemberResponse::default();
                    m_resp.member_id = StrBytes::from_string(m_id.clone());
                    m_resp.error_code = 0;
                    res.members.push(m_resp);
                }
            } else {
                if version >= 3 {
                    let mut m_resp = MemberResponse::default();
                    m_resp.member_id = StrBytes::from_string(m_id.clone());
                    m_resp.error_code = 25; // UNKNOWN_MEMBER_ID
                    res.members.push(m_resp);
                }
                res.error_code = 25;
            }
        }

        if group.members.is_empty() {
            group.state = GroupLifecycleState::Empty;
            group.leader_id = None;
        } else {
            // Elect new leader if needed and trigger rebalance
            if group.leader_id.as_ref().is_none_or(|l| !group.members.contains_key(l)) {
                group.leader_id = group.members.keys().next().cloned();
            }
            if group.state != GroupLifecycleState::PreparingRebalance {
                // A new round: everyone rejoins.
                group.awaiting_members.clear();
                group.assignments.clear();
            }
            group.state = GroupLifecycleState::PreparingRebalance;
            group.generation_id += 1;
            group.try_complete_join();
        }

        res
    }

    fn set_offset_metadata(&mut self, key: (String, String, i32), metadata: Option<&str>) {
        match metadata {
            Some(m) if !m.is_empty() => {
                self.offset_metadata.insert(key, m.to_string());
            }
            _ => {
                self.offset_metadata.remove(&key);
            }
        }
    }

    fn offset_metadata_of(&self, key: &(String, String, i32)) -> StrBytes {
        StrBytes::from_string(self.offset_metadata.get(key).cloned().unwrap_or_default())
    }

    /// A group Kafka still knows about: one with live state, or -- like a
    /// group only ever used through manual assignment and commits, or one
    /// whose membership a restart dropped -- one that only has committed
    /// offsets, which Kafka reports as an Empty group with no protocol.
    fn group_is_live(&self, group_id: &str) -> bool {
        self.groups.get(group_id).is_some_and(|g| g.state != GroupLifecycleState::Dead)
    }

    fn group_has_offsets(&self, group_id: &str) -> bool {
        self.committed_offsets.keys().any(|(g, _, _)| g == group_id)
    }

    fn drop_group_offsets(&mut self, group_id: &str) {
        self.committed_offsets.retain(|(g, _, _), _| g != group_id);
        self.offset_metadata.retain(|(g, _, _), _| g != group_id);
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

        // Kafka's GroupCoordinator.validateOffsetCommit: a commit naming
        // a member or generation must come from a current member of the
        // current generation; an anonymous one (generation -1, no member,
        // as an admin client or a consumer with manual assignment sends)
        // only goes into a group with no members.
        let generation = req.generation_id_or_member_epoch;
        let m_id = req.member_id.as_str();
        let group = self.groups.get(&group_id).filter(|g| g.state != GroupLifecycleState::Dead);
        let err = if group_id.is_empty() {
            24 // INVALID_GROUP_ID
        } else {
            match group {
                None if generation < 0 => 0,
                None => 22, // ILLEGAL_GENERATION
                Some(g)
                    if generation >= 0 || !m_id.is_empty() || req.group_instance_id.is_some() =>
                {
                    if !g.members.contains_key(m_id) {
                        25 // UNKNOWN_MEMBER_ID
                    } else if generation != g.generation_id {
                        22 // ILLEGAL_GENERATION
                    } else if g.state == GroupLifecycleState::CompletingRebalance {
                        27 // REBALANCE_IN_PROGRESS
                    } else {
                        0
                    }
                }
                Some(g) if !g.members.is_empty() => 25, // UNKNOWN_MEMBER_ID
                Some(_) => 0,
            }
        };

        for topic in &req.topics {
            let mut topic_res = OffsetCommitResponseTopic::default();
            topic_res.name = topic.name.clone();
            let topic_name = topic.name.as_str().to_string();

            for part in &topic.partitions {
                let mut part_res = OffsetCommitResponsePartition::default();
                part_res.partition_index = part.partition_index;

                // Kafka rejects a commit for a partition that doesn't exist.
                let known = self
                    .topics
                    .get(&topic_name)
                    .is_some_and(|t| t.partitions.contains_key(&part.partition_index));
                if err != 0 {
                    part_res.error_code = err;
                } else if !known {
                    part_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                } else {
                    let key = (group_id.clone(), topic_name.clone(), part.partition_index);
                    self.committed_offsets.insert(key.clone(), part.committed_offset);
                    self.set_offset_metadata(key, part.committed_metadata.as_deref());
                    part_res.error_code = 0;
                }

                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_offset_fetch(
        &self,
        req: &kafka_protocol::messages::OffsetFetchRequest,
        version: i16,
    ) -> kafka_protocol::messages::OffsetFetchResponse {
        use kafka_protocol::messages::offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        };
        let mut res = kafka_protocol::messages::OffsetFetchResponse::default();

        if version >= 8 && (!req.groups.is_empty() || req.group_id.is_empty()) {
            for group_req in &req.groups {
                let group_id = group_req.group_id.as_str().to_string();
                let mut group_res = OffsetFetchResponseGroup::default();
                group_res.group_id = group_req.group_id.clone();
                group_res.error_code = 0;

                if let Some(topics) = &group_req.topics {
                    for topic_req in topics {
                        let mut topic_res = OffsetFetchResponseTopics::default();
                        topic_res.name = topic_req.name.clone();
                        let topic_name = topic_req.name.as_str().to_string();

                        for &partition_index in &topic_req.partition_indexes {
                            let mut part_res = OffsetFetchResponsePartitions::default();
                            part_res.partition_index = partition_index;

                            let key = (group_id.clone(), topic_name.clone(), partition_index);
                            if let Some(&offset) = self.committed_offsets.get(&key) {
                                part_res.committed_offset = offset;
                                part_res.metadata = Some(self.offset_metadata_of(&key));
                                part_res.error_code = 0;
                            } else {
                                part_res.committed_offset = -1;
                                part_res.error_code = 0;
                            }

                            topic_res.partitions.push(part_res);
                        }

                        group_res.topics.push(topic_res);
                    }
                } else {
                    let mut topics_map: std::collections::BTreeMap<String, Vec<(i32, i64)>> =
                        std::collections::BTreeMap::new();
                    for ((g, t, p), &off) in &self.committed_offsets {
                        if g == &group_id {
                            topics_map.entry(t.clone()).or_default().push((*p, off));
                        }
                    }
                    for (t_name, mut parts) in topics_map {
                        parts.sort_by_key(|(p, _)| *p);
                        let mut topic_res = OffsetFetchResponseTopics::default();
                        topic_res.name = TopicName::from(StrBytes::from_string(t_name.clone()));
                        for (partition_index, offset) in parts {
                            let mut part_res = OffsetFetchResponsePartitions::default();
                            part_res.partition_index = partition_index;
                            part_res.committed_offset = offset;
                            part_res.metadata = Some(self.offset_metadata_of(&(
                                group_id.clone(),
                                t_name.clone(),
                                partition_index,
                            )));
                            part_res.error_code = 0;
                            topic_res.partitions.push(part_res);
                        }
                        group_res.topics.push(topic_res);
                    }
                }

                res.groups.push(group_res);
            }
        } else {
            let group_id = req.group_id.as_str().to_string();

            if let Some(topics) = &req.topics {
                for topic in topics {
                    let mut topic_res = OffsetFetchResponseTopic::default();
                    topic_res.name = topic.name.clone();
                    let topic_name = topic.name.as_str().to_string();

                    for &partition_index in &topic.partition_indexes {
                        let mut part_res = OffsetFetchResponsePartition::default();
                        part_res.partition_index = partition_index;

                        let key = (group_id.clone(), topic_name.clone(), partition_index);
                        if let Some(&offset) = self.committed_offsets.get(&key) {
                            part_res.committed_offset = offset;
                            part_res.metadata = Some(self.offset_metadata_of(&key));
                            part_res.error_code = 0;
                        } else {
                            // Offset uncommitted: offset -1 with error 0
                            part_res.committed_offset = -1;
                            part_res.error_code = 0;
                        }

                        topic_res.partitions.push(part_res);
                    }

                    res.topics.push(topic_res);
                }
            } else {
                let mut topics_map: std::collections::BTreeMap<String, Vec<(i32, i64)>> =
                    std::collections::BTreeMap::new();
                for ((g, t, p), &off) in &self.committed_offsets {
                    if g == &group_id {
                        topics_map.entry(t.clone()).or_default().push((*p, off));
                    }
                }
                for (t_name, mut parts) in topics_map {
                    parts.sort_by_key(|(p, _)| *p);
                    let mut topic_res = OffsetFetchResponseTopic::default();
                    topic_res.name = TopicName::from(StrBytes::from_string(t_name.clone()));
                    for (partition_index, offset) in parts {
                        let mut part_res = OffsetFetchResponsePartition::default();
                        part_res.partition_index = partition_index;
                        part_res.committed_offset = offset;
                        part_res.metadata = Some(self.offset_metadata_of(&(
                            group_id.clone(),
                            t_name.clone(),
                            partition_index,
                        )));
                        part_res.error_code = 0;
                        topic_res.partitions.push(part_res);
                    }
                    res.topics.push(topic_res);
                }
            }
        }

        res
    }

    pub fn handle_describe_groups(
        &self,
        req: &kafka_protocol::messages::DescribeGroupsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::DescribeGroupsResponse {
        use kafka_protocol::messages::describe_groups_response::{
            DescribedGroup, DescribedGroupMember,
        };
        let mut res = kafka_protocol::messages::DescribeGroupsResponse::default();

        for group_id in &req.groups {
            let gid_str = group_id.as_str();
            let mut desc = DescribedGroup::default();
            desc.group_id = group_id.clone();

            if let Some(group) = self.groups.get(gid_str).filter(|_| self.group_is_live(gid_str)) {
                desc.error_code = 0;
                desc.group_state = StrBytes::from_string(group.state.as_str().to_string());
                desc.protocol_type = StrBytes::from_string(group.protocol_type.clone());
                // A group with no members has no chosen protocol (Kafka
                // clears it when the last member leaves).
                if !group.members.is_empty() {
                    desc.protocol_data =
                        StrBytes::from_string(group.protocol_name.clone().unwrap_or_default());
                }

                for (m_id, m) in &group.members {
                    let mut dm = DescribedGroupMember::default();
                    dm.member_id = StrBytes::from_string(m_id.clone());
                    dm.client_id = StrBytes::from_string(m.client_id.clone());
                    dm.client_host = StrBytes::from_string(m.client_host.clone());

                    if let Some(proto_name) = &group.protocol_name
                        && let Some((_, meta)) = m.protocols.iter().find(|(p, _)| p == proto_name)
                    {
                        dm.member_metadata = bytes::Bytes::copy_from_slice(meta);
                    }
                    if let Some(assignment) = group.assignments.get(m_id) {
                        dm.member_assignment = bytes::Bytes::copy_from_slice(assignment);
                    }

                    desc.members.push(dm);
                }
            } else if self.group_has_offsets(gid_str) {
                desc.error_code = 0;
                desc.group_state = StrBytes::from_static_str("Empty");
            } else {
                desc.error_code = 0;
                desc.group_state = StrBytes::from_static_str("Dead");
            }

            res.groups.push(desc);
        }

        res
    }

    pub fn handle_list_groups(
        &self,
        req: &kafka_protocol::messages::ListGroupsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::ListGroupsResponse {
        use kafka_protocol::messages::list_groups_response::ListedGroup;
        let mut res = kafka_protocol::messages::ListGroupsResponse::default();
        res.error_code = 0;

        let mut listed: std::collections::BTreeMap<String, (String, &'static str)> =
            std::collections::BTreeMap::new();
        for (gid, group) in &self.groups {
            if group.state != GroupLifecycleState::Dead {
                listed.insert(gid.clone(), (group.protocol_type.clone(), group.state.as_str()));
            }
        }
        for (g, _, _) in self.committed_offsets.keys() {
            if !listed.contains_key(g) {
                listed.insert(g.clone(), (String::new(), "Empty"));
            }
        }
        for (gid, (protocol_type, state)) in listed {
            // v4+: only groups in one of the requested states.
            if !req.states_filter.is_empty()
                && !req.states_filter.iter().any(|f| f.as_str().eq_ignore_ascii_case(state))
            {
                continue;
            }
            let mut lg = ListedGroup::default();
            lg.group_id = kafka_protocol::messages::GroupId(StrBytes::from_string(gid));
            lg.protocol_type = StrBytes::from_string(protocol_type);
            lg.group_state = StrBytes::from_static_str(state);
            res.groups.push(lg);
        }

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

            let live = self.group_is_live(&gid_str);
            if live && self.groups[&gid_str].state != GroupLifecycleState::Empty {
                result.error_code = 68; // NON_EMPTY_GROUP
            } else if live || self.group_has_offsets(&gid_str) {
                if let Some(group) = self.groups.get_mut(&gid_str) {
                    group.state = GroupLifecycleState::Dead;
                }
                self.drop_group_offsets(&gid_str);
                result.error_code = 0;
            } else {
                // Never existed, or already deleted.
                result.error_code = 69; // GROUP_ID_NOT_FOUND
            }

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

            // `ConfigSource` (org.apache.kafka.clients.admin.ConfigEntry):
            // real clients (kafka-topics.sh's admin client included) reject
            // an id outside the enum, and the crate's own default (-1) is
            // exactly that — every config entry needs a real source.
            const DYNAMIC_TOPIC_CONFIG: i8 = 1;
            const STATIC_BROKER_CONFIG: i8 = 4;
            const DEFAULT_CONFIG: i8 = 5;

            if resource.resource_type == 2 {
                // Topic
                let topic_name = resource.resource_name.as_str();
                if let Some(topic_state) = self.topics.get(topic_name) {
                    result.error_code = 0;

                    let mut configs: Vec<(String, String, i8)> =
                        super::log::topic_config_defaults()
                            .map(|(k, d)| match topic_state.configs.get(k) {
                                Some(v) => (k.to_string(), v.clone(), DYNAMIC_TOPIC_CONFIG),
                                None => (k.to_string(), d.to_string(), DEFAULT_CONFIG),
                            })
                            .collect();
                    // Only the keys asked for, when the request names any.
                    if let Some(keys) =
                        resource.configuration_keys.as_ref().filter(|k| !k.is_empty())
                    {
                        configs.retain(|(k, _, _)| keys.iter().any(|q| q.as_str() == k));
                    }

                    for (k, v, source) in configs {
                        let mut conf = DescribeConfigsResourceResult::default();
                        conf.name = StrBytes::from_string(k);
                        conf.value = Some(StrBytes::from_string(v));
                        conf.read_only = false;
                        conf.config_source = source;
                        result.configs.push(conf);
                    }
                } else {
                    result.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
            } else {
                // Broker / cluster
                result.error_code = 0;
                let configs: &[(&str, &str)] = &[
                    ("auto.create.topics.enable", "true"),
                    ("num.partitions", "1"),
                    ("default.replication.factor", "1"),
                ];

                for &(k, v) in configs {
                    let mut conf = DescribeConfigsResourceResult::default();
                    conf.name = StrBytes::from_string(k.to_string());
                    let overridden = self.broker_configs.get(k);
                    let val = overridden.map(|s| s.as_str()).unwrap_or(v);
                    conf.value = Some(StrBytes::from_string(val.to_string()));
                    conf.read_only = false;
                    conf.config_source =
                        if overridden.is_some() { STATIC_BROKER_CONFIG } else { DEFAULT_CONFIG };
                    result.configs.push(conf);
                }
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
            for partition in &topic.partitions {
                let mut part_res = EpochEndOffset::default();
                part_res.partition = partition.partition;

                if let Some(topic_state) = self.topics.get(topic_name) {
                    if let Some(p_state) = topic_state.partitions.get(&partition.partition) {
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
        &mut self,
        req: &kafka_protocol::messages::AddPartitionsToTxnRequest,
        _version: i16,
    ) -> kafka_protocol::messages::AddPartitionsToTxnResponse {
        use kafka_protocol::messages::add_partitions_to_txn_response::{
            AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
        };
        let mut res = kafka_protocol::messages::AddPartitionsToTxnResponse::default();

        let tx_id = req.v3_and_below_transactional_id.as_str().to_string();
        let producer_id = req.v3_and_below_producer_id.0;
        let producer_epoch = req.v3_and_below_producer_epoch;
        let is_fenced = self
            .producer_epochs
            .get(&producer_id)
            .is_some_and(|&expected| producer_epoch < expected);

        for topic in &req.v3_and_below_topics {
            let mut topic_res = AddPartitionsToTxnTopicResult::default();
            topic_res.name = topic.name.clone();

            for &p_id in &topic.partitions {
                let mut part_res = AddPartitionsToTxnPartitionResult::default();
                part_res.partition_index = p_id;
                if is_fenced {
                    part_res.partition_error_code = 47; // INVALID_PRODUCER_EPOCH
                } else {
                    part_res.partition_error_code = 0;
                    self.txn_partitions
                        .entry(tx_id.clone())
                        .or_default()
                        .insert((topic.name.as_str().to_string(), p_id));
                }
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
        &mut self,
        req: &kafka_protocol::messages::EndTxnRequest,
        _version: i16,
    ) -> kafka_protocol::messages::EndTxnResponse {
        let mut res = kafka_protocol::messages::EndTxnResponse::default();
        let tx_id = req.transactional_id.as_str().to_string();
        let producer_id = req.producer_id.0;
        let producer_epoch = req.producer_epoch;

        if self.producer_epochs.get(&producer_id).is_some_and(|&expected| producer_epoch < expected)
        {
            res.error_code = 47; // INVALID_PRODUCER_EPOCH
            return res;
        }

        if let Some(partitions) = self.txn_partitions.remove(&tx_id) {
            let now = self.now_ms();
            for (topic_name, p_id) in partitions {
                let Some(topic_state) = self.topics.get_mut(&topic_name) else { continue };
                let Some(part_state) = topic_state.partitions.get_mut(&p_id) else { continue };

                // Only append a control batch if the transaction was genuinely active on this
                // partition. If it was already force-aborted at shutdown, active_txns is empty
                // and a commit must be rejected with INVALID_TXN_STATE.
                if let Some(first_offset) = part_state.active_txns.remove(&producer_id) {
                    if !req.committed {
                        part_state.aborted_txns.push((producer_id, first_offset));
                    }
                    let base_offset = part_state.high_watermark;
                    let control_batch = encode_control_batch(
                        producer_id,
                        producer_epoch,
                        base_offset,
                        req.committed,
                        now,
                    );
                    part_state.append_batch(control_batch, 1, now, None);
                } else if req.committed
                    && part_state.aborted_txns.iter().any(|&(pid, _)| pid == producer_id)
                {
                    // This transaction was already force-aborted (e.g. by shutdown resolution).
                    // A subsequent commit cannot succeed.
                    res.error_code = 48; // INVALID_TXN_STATE
                    return res;
                }
            }
        }

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

                let key = (group_id.clone(), topic_name.clone(), part.partition_index);
                self.committed_offsets.insert(key.clone(), part.committed_offset);
                self.set_offset_metadata(key, part.committed_metadata.as_deref());

                part_res.error_code = 0;
                topic_res.partitions.push(part_res);
            }

            res.topics.push(topic_res);
        }

        res
    }

    pub fn handle_alter_configs(
        &mut self,
        req: &kafka_protocol::messages::AlterConfigsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::AlterConfigsResponse {
        use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;
        let mut res = kafka_protocol::messages::AlterConfigsResponse::default();

        for resource in &req.resources {
            let mut resource_res = AlterConfigsResourceResponse::default();
            resource_res.resource_type = resource.resource_type;
            resource_res.resource_name = resource.resource_name.clone();

            if resource.resource_type == 2 {
                let topic_name = resource.resource_name.as_str();
                if let Some(topic_state) = self.topics.get_mut(topic_name) {
                    // AlterConfigs (unlike the incremental one) replaces the
                    // topic's whole set of overrides: anything not in the
                    // request goes back to its default.
                    let invalid = resource.configs.iter().find_map(|e| {
                        let value = e.value.as_ref().map_or("", |v| v.as_str());
                        super::log::validate_topic_config(e.name.as_str(), value).err()
                    });
                    if let Some(msg) = invalid {
                        resource_res.error_code = 40; // INVALID_CONFIG
                        resource_res.error_message = Some(StrBytes::from_string(msg));
                    } else {
                        if !req.validate_only {
                            topic_state.configs = resource
                                .configs
                                .iter()
                                .filter_map(|e| {
                                    e.value.as_ref().map(|v| (e.name.to_string(), v.to_string()))
                                })
                                .collect();
                        }
                        resource_res.error_code = 0;
                    }
                } else {
                    resource_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
            } else {
                for entry in &resource.configs {
                    if let Some(val) = &entry.value {
                        self.broker_configs.insert(entry.name.to_string(), val.to_string());
                    }
                }
                resource_res.error_code = 0;
            }

            res.responses.push(resource_res);
        }

        res
    }

    pub fn handle_incremental_alter_configs(
        &mut self,
        req: &kafka_protocol::messages::IncrementalAlterConfigsRequest,
        _version: i16,
    ) -> kafka_protocol::messages::IncrementalAlterConfigsResponse {
        use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
        let mut res = kafka_protocol::messages::IncrementalAlterConfigsResponse::default();

        for resource in &req.resources {
            let mut resource_res = AlterConfigsResourceResponse::default();
            resource_res.resource_type = resource.resource_type;
            resource_res.resource_name = resource.resource_name.clone();

            if resource.resource_type == 2 {
                let topic_name = resource.resource_name.as_str();
                if let Some(topic_state) = self.topics.get_mut(topic_name) {
                    // Applied to a copy and committed only if every entry
                    // is valid: a request either changes all it names or
                    // nothing.
                    let mut configs = topic_state.configs.clone();
                    let mut error = None;
                    for entry in &resource.configs {
                        let name = entry.name.as_str();
                        let value = entry.value.as_ref().map_or("", |v| v.as_str());
                        let current = configs.get(name).cloned().or_else(|| {
                            super::log::topic_config_defaults()
                                .find(|(n, _)| *n == name)
                                .map(|(_, d)| d.to_string())
                        });
                        let new_value = match entry.config_operation {
                            0 => Some(value.to_string()), // SET
                            1 => None,                    // DELETE
                            op @ (2 | 3) => {
                                // APPEND / SUBTRACT, list configs only.
                                if !super::log::is_list_config(name) {
                                    error = Some(format!(
                                        "Config value append is not allowed for config key: {name}"
                                    ));
                                    break;
                                }
                                let mut items: Vec<String> =
                                    super::log::split_list(current.as_deref().unwrap_or(""))
                                        .map(str::to_string)
                                        .collect();
                                for item in super::log::split_list(value) {
                                    if op == 2 {
                                        if !items.iter().any(|i| i == item) {
                                            items.push(item.to_string());
                                        }
                                    } else {
                                        items.retain(|i| i != item);
                                    }
                                }
                                Some(items.join(","))
                            }
                            _ => {
                                error = Some(format!("Unknown config operation for {name}"));
                                break;
                            }
                        };
                        match new_value {
                            Some(v) => {
                                if let Err(msg) = super::log::validate_topic_config(name, &v) {
                                    error = Some(msg);
                                    break;
                                }
                                configs.insert(name.to_string(), v);
                            }
                            None => {
                                if let Err(msg) = super::log::validate_topic_config(name, "")
                                    && msg.starts_with("Unknown")
                                {
                                    error = Some(msg);
                                    break;
                                }
                                configs.remove(name);
                            }
                        }
                    }
                    if let Some(msg) = error {
                        resource_res.error_code = 40; // INVALID_CONFIG
                        resource_res.error_message = Some(StrBytes::from_string(msg));
                    } else {
                        if !req.validate_only {
                            topic_state.configs = configs;
                        }
                        resource_res.error_code = 0;
                    }
                } else {
                    resource_res.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
            } else {
                for entry in &resource.configs {
                    if entry.config_operation == 0 {
                        if let Some(val) = &entry.value {
                            self.broker_configs.insert(entry.name.to_string(), val.to_string());
                        }
                    } else if entry.config_operation == 1 {
                        self.broker_configs.remove(entry.name.as_str());
                    }
                }
                resource_res.error_code = 0;
            }

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
        let now = self.now_ms();

        for topic in &req.topics {
            let mut topic_res = DeleteRecordsTopicResult::default();
            topic_res.name = topic.name.clone();
            let topic_state = self.topics.get_mut(topic.name.as_str());
            let cfg = topic_state.as_ref().map(|t| super::log::LogConfig::of(&t.configs));
            let mut topic_state = topic_state;

            for part in &topic.partitions {
                let mut part_res = DeleteRecordsPartitionResult::default();
                part_res.partition_index = part.partition_index;
                part_res.low_watermark = -1;
                let part_state = topic_state
                    .as_deref_mut()
                    .and_then(|t| t.partitions.get_mut(&part.partition_index));
                match (part_state, &cfg) {
                    (Some(ps), Some(cfg)) => {
                        // -1 means "up to the high watermark".
                        let offset =
                            if part.offset == -1 { ps.high_watermark } else { part.offset };
                        if !cfg.delete {
                            part_res.error_code = 44; // POLICY_VIOLATION
                        } else if offset < 0 || offset > ps.high_watermark {
                            part_res.error_code = 1; // OFFSET_OUT_OF_RANGE
                        } else {
                            ps.truncate_front(offset, now);
                            part_res.low_watermark = ps.log_start_offset;
                        }
                    }
                    _ => part_res.error_code = 3, // UNKNOWN_TOPIC_OR_PARTITION
                }
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

        if !self.group_is_live(&group_id) && !self.group_has_offsets(&group_id) {
            res.error_code = 69; // GROUP_ID_NOT_FOUND
            return res;
        }

        for topic in &req.topics {
            let mut topic_res = OffsetDeleteResponseTopic::default();
            topic_res.name = topic.name.clone();
            let topic_name = topic.name.as_str().to_string();

            for part in &topic.partitions {
                let mut part_res = OffsetDeleteResponsePartition::default();
                part_res.partition_index = part.partition_index;
                let key = (group_id.clone(), topic_name.clone(), part.partition_index);
                self.committed_offsets.remove(&key);
                self.offset_metadata.remove(&key);
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
            tx_state.transaction_state = StrBytes::from_static_str("CompleteCommit");
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
        log_dir.log_dir = StrBytes::from_static_str("/tmp/noida-kafka-logs");
        log_dir.error_code = 0;

        for (topic_name, topic_state) in &self.topics {
            let mut t_dir = DescribeLogDirsTopic::default();
            t_dir.name =
                kafka_protocol::messages::TopicName(StrBytes::from_string(topic_name.clone()));

            for (&p_id, p_state) in &topic_state.partitions {
                let mut p_dir = DescribeLogDirsPartition::default();
                p_dir.partition_index = p_id;
                p_dir.partition_size = p_state.size_bytes();
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
        res.mechanisms.push(StrBytes::from_static_str("PLAIN"));
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

/// A member id the way Kafka mints one: `<client.id>-<uuid>`.
fn new_member_id(client_id: &str, counter: &mut u64) -> String {
    *counter += 1;
    let x = uuid_simple().wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (*counter as u128) << 64;
    let h = format!("{x:032x}");
    format!("{client_id}-{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

#[derive(Clone, Debug)]
pub struct Engine {
    state: Arc<Mutex<EngineState>>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new("127.0.0.1".to_string(), 9092)
    }
}

impl Engine {
    pub fn new(host: String, port: i32) -> Self {
        Self { state: Arc::new(Mutex::new(EngineState::new(host, port))) }
    }

    pub fn with_clock(host: String, port: i32, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        let mut state = EngineState::new(host, port);
        state.clock = Some(clock);
        Self { state: Arc::new(Mutex::new(state)) }
    }

    pub fn handle_api_versions(&self, version: i16) -> ApiVersionsResponse {
        self.state.lock().unwrap().handle_api_versions(version)
    }

    pub fn now_ms(&self) -> i64 {
        self.state.lock().unwrap().now_ms()
    }

    /// One pass of compaction and retention over every partition.
    pub fn run_log_cleaner(&self) {
        self.state.lock().unwrap().run_log_cleaner();
    }

    /// Runs the log cleaner once a second in the background (a broker
    /// checks every 5 minutes by default; a dev server shouldn't make
    /// anyone wait that long to see retention work). Stops once the engine
    /// is dropped.
    pub fn spawn_log_cleaner(&self) {
        let weak = Arc::downgrade(&self.state);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let Some(state) = weak.upgrade() else { break };
                state.lock().unwrap().run_log_cleaner();
            }
        });
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
        self.handle_join_group_from(req, version, "noida-client", "/127.0.0.1")
    }

    /// JoinGroup from a connection whose client id and address are known.
    pub fn handle_join_group_from(
        &self,
        req: &kafka_protocol::messages::JoinGroupRequest,
        version: i16,
        client_id: &str,
        client_host: &str,
    ) -> kafka_protocol::messages::JoinGroupResponse {
        let mut resp =
            self.state.lock().unwrap().handle_join_group(req, version, client_id, client_host);

        // An old-version JoinGroup (kafka-go) arrives with an empty member
        // id and is assigned one in this same request; it waits for the
        // round to complete like any other join.
        if resp.error_code != 0 {
            return resp;
        }
        let member_id = resp.member_id.clone();

        let group_id = req.group_id.as_str().to_string();
        loop {
            let state = self.state.lock().unwrap();
            let group = match state.groups.get(&group_id) {
                Some(g) => g,
                None => {
                    resp.error_code = 25; // UNKNOWN_MEMBER_ID
                    return resp;
                }
            };

            if group.state == GroupLifecycleState::CompletingRebalance
                || group.state == GroupLifecycleState::Stable
                || group.state == GroupLifecycleState::Dead
            {
                let mut final_resp = kafka_protocol::messages::JoinGroupResponse::default();
                final_resp.generation_id = group.generation_id;
                final_resp.protocol_type = Some(kafka_protocol::protocol::StrBytes::from_string(
                    group.protocol_type.clone(),
                ));
                final_resp.protocol_name = Some(kafka_protocol::protocol::StrBytes::from_string(
                    group.protocol_name.clone().unwrap_or_default(),
                ));
                final_resp.leader = kafka_protocol::protocol::StrBytes::from_string(
                    group.leader_id.clone().unwrap_or_default(),
                );
                final_resp.member_id = member_id.clone();
                if final_resp.leader == member_id {
                    let mut mems = Vec::new();
                    for (m_id, protos) in &group.awaiting_members {
                        let mut mem = kafka_protocol::messages::join_group_response::JoinGroupResponseMember::default();
                        mem.member_id =
                            kafka_protocol::protocol::StrBytes::from_string(m_id.clone());
                        mem.group_instance_id = None;
                        mem.metadata = protos
                            .iter()
                            .find(|(n, _)| Some(n) == group.protocol_name.as_ref())
                            .map(|(_, m)| bytes::Bytes::copy_from_slice(m))
                            .unwrap_or_default();
                        mems.push(mem);
                    }
                    final_resp.members = mems;
                }
                return final_resp;
            }

            let now = state.now_ms();
            let max_timeout =
                group.members.values().map(|m| m.rebalance_timeout_ms).max().unwrap_or(0);
            if group.rebalance_start_ms > 0 && now - group.rebalance_start_ms >= max_timeout as i64
            {
                drop(state);
                let mut st = self.state.lock().unwrap();
                if let Some(g) = st.groups.get_mut(&group_id)
                    && g.state == GroupLifecycleState::PreparingRebalance
                {
                    g.state = GroupLifecycleState::CompletingRebalance;
                }
                continue;
            }

            drop(state);
            if let Some(g) = self.state.lock().unwrap().groups.get_mut(&group_id) {
                g.try_complete_join();
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    pub fn handle_sync_group(
        &self,
        req: &kafka_protocol::messages::SyncGroupRequest,
        version: i16,
    ) -> kafka_protocol::messages::SyncGroupResponse {
        // A follower's SyncGroup waits for the leader's: answered before the
        // leader has sent the round's assignments, it used to get an empty
        // assignment and no error, leaving that consumer with no partitions
        // until the next rebalance (found via testing before a public
        // release). The leader's own SyncGroup (carrying assignments) is
        // answered at once; so is everyone once the group is Stable, and a
        // restarted round answers REBALANCE_IN_PROGRESS so members rejoin.
        let group_id = req.group_id.as_str().to_string();
        let member_id = req.member_id.as_str();
        let started = std::time::Instant::now();
        loop {
            let state = self.state.lock().unwrap();
            let Some(group) = state.groups.get(&group_id) else {
                drop(state);
                return self.state.lock().unwrap().handle_sync_group(req, version);
            };
            let waiting_for_leader = group.state == GroupLifecycleState::CompletingRebalance
                && group.members.contains_key(member_id)
                && req.assignments.is_empty()
                && group.leader_id.as_deref() != Some(member_id)
                && req.generation_id == group.generation_id;
            // A leader that died without leaving is only noticed when its
            // session expires, so don't wait longer than that.
            let timeout_ms = group
                .members
                .get(member_id)
                .map_or(30_000, |m| m.rebalance_timeout_ms.min(m.session_timeout_ms).max(1));
            if !waiting_for_leader {
                drop(state);
                return self.state.lock().unwrap().handle_sync_group(req, version);
            }
            if started.elapsed().as_millis() as i64 >= timeout_ms as i64 {
                let mut resp = kafka_protocol::messages::SyncGroupResponse::default();
                resp.error_code = 27; // REBALANCE_IN_PROGRESS: rejoin
                return resp;
            }
            drop(state);
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
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

    /// Clones out the shared state (resolving any in-flight transactions as
    /// aborted, per spec §5) and serializes it into a `Snapshot` DTO for
    /// on-disk persistence.
    /// The state to save. Open transactions are saved as aborted (they
    /// can't survive a restart), but the running broker keeps them open:
    /// the autosave runs while producers are mid-transaction, and resolving
    /// them in place made their commits fail with INVALID_TXN_STATE.
    pub fn snapshot(&self) -> Snapshot {
        let mut state = self.state.lock().unwrap();
        let open: Vec<(String, i32, PartitionState)> = state
            .topics
            .iter()
            .flat_map(|(topic, ts)| {
                ts.partitions
                    .iter()
                    .filter(|(_, p)| !p.active_txns.is_empty())
                    .map(move |(&id, p)| (topic.clone(), id, p.clone()))
            })
            .collect();
        state.resolve_open_transactions_for_shutdown();
        let snapshot = state.to_snapshot();
        for (topic, id, part) in open {
            if let Some(ts) = state.topics.get_mut(&topic) {
                ts.partitions.insert(id, part);
            }
        }
        snapshot
    }

    /// Builds an `Engine` from a previously saved `Snapshot`, updating `host`
    /// and `port` to the current bind address so client metadata responses
    /// reflect the actual listener rather than the stale address from the snapshot.
    pub fn new_persistent(snapshot: Snapshot, host: String, port: i32) -> Self {
        let mut state = EngineState::from_snapshot(snapshot);
        state.host = host;
        state.port = port;
        Self { state: Arc::new(Mutex::new(state)) }
    }
}

// ---------------------------------------------------------------------------
// Snapshot DTOs — a separate, serde-friendly representation of `EngineState`
// that avoids the `serde_json` tuple-key limitation (see spec §4).
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

/// Snapshot of a single partition's persistent state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub id: i32,
    pub leader: i32,
    pub record_batches: Vec<(i64, Vec<u8>)>,
    pub high_watermark: i64,
    /// `HashMap<(i64, i16), (i32, i64)>` serialized as `Vec` to avoid tuple-key
    /// serde_json limitation (§4).
    pub producer_seqs: Vec<((i64, i16), (i32, i64))>,
    pub aborted_txns: Vec<(i64, i64)>,
    #[serde(default)]
    pub log_start_offset: i64,
    #[serde(default)]
    pub segments: Vec<super::log::Segment>,
    #[serde(default)]
    pub tombstone_horizons: Vec<(i64, i64)>,
    #[serde(default)]
    pub clean_offset: i64,
    // active_txns intentionally absent — resolved to empty at save time (§5).
}

/// Snapshot of a single topic's persistent state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicSnapshot {
    pub name: String,
    pub is_internal: bool,
    /// `HashMap<i32, PartitionState>` serialized as `Vec` for uniformity (§4).
    pub partitions: Vec<(i32, PartitionSnapshot)>,
    pub configs: HashMap<String, String>,
}

/// Full engine snapshot — the on-disk representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    /// `HashMap<String, TopicState>` serialized as `Vec` (§4).
    pub topics: Vec<(String, TopicSnapshot)>,
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
    pub next_producer_id: i64,
    /// `HashMap<(String, String, i32), i64>` serialized as `Vec` — tuple key (§4).
    pub committed_offsets: Vec<((String, String, i32), i64)>,
    /// Metadata committed alongside offsets (absent in older snapshots).
    #[serde(default)]
    pub offset_metadata: Vec<((String, String, i32), String)>,
    pub broker_configs: HashMap<String, String>,
}

impl EngineState {
    /// Resolves every still-open transaction as aborted by appending an abort
    /// control batch to each affected partition, exactly as `EndTxn`'s abort
    /// path already does. Must be called before `to_snapshot()` so the
    /// snapshot doesn't contain open transactions that can never be completed
    /// (all producer connections are gone after a restart). See spec §5.
    pub fn resolve_open_transactions_for_shutdown(&mut self) {
        let now = self.now_ms();
        for topic_state in self.topics.values_mut() {
            for part_state in topic_state.partitions.values_mut() {
                if part_state.active_txns.is_empty() {
                    continue;
                }
                let open: Vec<(i64, i64)> = part_state.active_txns.drain().collect();
                for (producer_id, first_offset) in open {
                    let epoch = self.producer_epochs.get(&producer_id).copied().unwrap_or(0);
                    let base_offset = part_state.high_watermark;
                    let control_batch =
                        encode_control_batch(producer_id, epoch, base_offset, false, now);
                    part_state.append_batch(control_batch, 1, now, None);
                    part_state.aborted_txns.push((producer_id, first_offset));
                }
            }
        }
    }

    /// Converts live state into the serializable `Snapshot` DTO.
    pub fn to_snapshot(&self) -> Snapshot {
        let topics = self
            .topics
            .iter()
            .map(|(name, ts)| {
                let partitions = ts
                    .partitions
                    .iter()
                    .map(|(&pid, ps)| {
                        let producer_seqs: Vec<_> =
                            ps.producer_seqs.iter().map(|(&k, &v)| (k, v)).collect();
                        (
                            pid,
                            PartitionSnapshot {
                                id: ps.id,
                                leader: ps.leader,
                                record_batches: ps.record_batches.clone(),
                                high_watermark: ps.high_watermark,
                                producer_seqs,
                                aborted_txns: ps.aborted_txns.clone(),
                                log_start_offset: ps.log_start_offset,
                                segments: ps.segments.clone(),
                                tombstone_horizons: ps
                                    .tombstone_horizons
                                    .iter()
                                    .map(|(&k, &v)| (k, v))
                                    .collect(),
                                clean_offset: ps.clean_offset,
                            },
                        )
                    })
                    .collect();
                (
                    name.clone(),
                    TopicSnapshot {
                        name: ts.name.clone(),
                        is_internal: ts.is_internal,
                        partitions,
                        configs: ts.configs.clone(),
                    },
                )
            })
            .collect();

        let committed_offsets: Vec<_> =
            self.committed_offsets.iter().map(|(k, &v)| (k.clone(), v)).collect();

        Snapshot {
            topics,
            broker_id: self.broker_id,
            host: self.host.clone(),
            port: self.port,
            cluster_id: self.cluster_id.clone(),
            next_producer_id: self.next_producer_id,
            committed_offsets,
            offset_metadata: self
                .offset_metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            broker_configs: self.broker_configs.clone(),
        }
    }

    /// Rebuilds live state from a `Snapshot` DTO loaded from disk.
    pub fn from_snapshot(s: Snapshot) -> EngineState {
        let mut topics: HashMap<String, TopicState> = s
            .topics
            .into_iter()
            .map(|(name, ts)| {
                let partitions: HashMap<i32, PartitionState> = ts
                    .partitions
                    .into_iter()
                    .map(|(pid, ps)| {
                        let producer_seqs: HashMap<(i64, i16), (i32, i64)> =
                            ps.producer_seqs.into_iter().collect();
                        (
                            pid,
                            PartitionState {
                                id: ps.id,
                                leader: ps.leader,
                                record_batches: ps.record_batches,
                                high_watermark: ps.high_watermark,
                                producer_seqs,
                                active_txns: HashMap::new(), // always empty after save (§5)
                                aborted_txns: ps.aborted_txns,
                                log_start_offset: ps.log_start_offset,
                                segments: ps.segments,
                                tombstone_horizons: ps.tombstone_horizons.into_iter().collect(),
                                clean_offset: ps.clean_offset,
                            },
                        )
                    })
                    .collect();
                (
                    name.clone(),
                    TopicState {
                        name: ts.name,
                        is_internal: ts.is_internal,
                        partitions,
                        configs: ts.configs,
                    },
                )
            })
            .collect();

        // Ensure the __consumer_offsets internal topic always exists (re-create
        // any partitions that might be missing if the snapshot predates it).
        let broker_id = s.broker_id;
        topics.entry("__consumer_offsets".to_string()).or_insert_with(|| {
            let mut t = TopicState {
                name: "__consumer_offsets".to_string(),
                is_internal: true,
                partitions: HashMap::new(),
                configs: HashMap::new(),
            };
            for p in 0..50 {
                t.partitions.insert(p, PartitionState::new(p, broker_id));
            }
            t
        });

        let committed_offsets: HashMap<(String, String, i32), i64> =
            s.committed_offsets.into_iter().collect();

        EngineState {
            topics,
            broker_id: s.broker_id,
            host: s.host,
            port: s.port,
            cluster_id: s.cluster_id,
            next_producer_id: s.next_producer_id,
            next_member_counter: 1, // reset — no old member survives a restart (§3.2)
            committed_offsets,
            offset_metadata: s.offset_metadata.into_iter().collect(),
            groups: HashMap::new(), // membership reset; offsets are in committed_offsets (§3.2)
            broker_configs: s.broker_configs,
            clock: None,
            producer_epochs: HashMap::new(), // fencing state not persisted (§5)
            txn_producers: HashMap::new(),
            txn_partitions: HashMap::new(),
        }
    }
}

/// One complete v2 record batch: a length field matching the payload, magic
/// 2, and a CRC-32C over everything after the CRC field that checks out.
fn is_valid_v2_batch(records: &[u8]) -> bool {
    if records.len() < 61 || records[16] != 2 {
        return false;
    }
    let len = i32::from_be_bytes(records[8..12].try_into().unwrap());
    if len < 0 || len as usize + 12 != records.len() {
        return false;
    }
    let crc = u32::from_be_bytes(records[17..21].try_into().unwrap());
    crc32c::crc32c(&records[21..]) == crc
}

/// Re-encodes a v2 batch with every record's timestamp set to `now` and the
/// LogAppendTime flag on, as a broker does for a LogAppendTime topic.
fn stamp_log_append_time(batch: &[u8], now: i64) -> Option<Vec<u8>> {
    use kafka_protocol::records::RecordBatchDecoder;
    let mut buf = bytes::Bytes::copy_from_slice(batch);
    let mut set = RecordBatchDecoder::decode(&mut buf).ok()?;
    for r in &mut set.records {
        r.timestamp = now;
        r.timestamp_type = TimestampType::LogAppend;
    }
    let mut out = bytes::BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: set.compression };
    RecordBatchEncoder::encode(&mut out, set.records.iter(), &options).ok()?;
    let mut out = out.to_vec();
    super::log::set_log_append_time_flag(&mut out);
    Some(out)
}

/// Kafka's ListOffsets-by-timestamp: the earliest offset (below `end`)
/// whose record timestamp is >= `target`, with that timestamp; for -3
/// (`MAX_TIMESTAMP`), the record with the largest timestamp. `(-1, -1)` when
/// there's no such record. Control records (transaction markers) never
/// match.
fn offset_for_timestamp(part: &PartitionState, target: i64, end: i64) -> (i64, i64) {
    use kafka_protocol::records::RecordBatchDecoder;
    let mut best: Option<(i64, i64)> = None;
    for (base, batch) in &part.record_batches {
        if *base >= end {
            break;
        }
        // The batch's own header carries the offset its records count from.
        let Some(header_base) = batch.get(..8).map(|b| i64::from_be_bytes(b.try_into().unwrap()))
        else {
            continue;
        };
        let mut buf = bytes::Bytes::copy_from_slice(batch);
        let Ok(set) = RecordBatchDecoder::decode(&mut buf) else { continue };
        for r in set.records {
            let offset = base + (r.offset - header_base);
            if r.control || offset >= end || offset < part.log_start_offset {
                continue;
            }
            if target == -3 {
                if best.is_none_or(|(_, t)| r.timestamp > t) {
                    best = Some((offset, r.timestamp));
                }
            } else if r.timestamp >= target {
                return (offset, r.timestamp);
            }
        }
    }
    best.unwrap_or((-1, -1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_leaves_live_transactions_open() {
        // The autosave snapshots while producers are mid-transaction: the
        // saved copy has them aborted, the running broker still has them open.
        let engine = Engine::new("127.0.0.1".into(), 9092);
        {
            let mut st = engine.state.lock().unwrap();
            let mut part = PartitionState::new(0, 1);
            part.active_txns.insert(7, 0);
            let mut topic = TopicState {
                name: "t".into(),
                is_internal: false,
                partitions: HashMap::new(),
                configs: HashMap::new(),
            };
            topic.partitions.insert(0, part);
            st.topics.insert("t".into(), topic);
        }
        let snap = engine.snapshot();
        let (_, saved_topic) = snap.topics.iter().find(|(n, _)| n == "t").unwrap();
        let (_, saved) = &saved_topic.partitions[0];
        assert!(saved.aborted_txns.iter().any(|&(pid, _)| pid == 7), "saved copy aborts it");
        let st = engine.state.lock().unwrap();
        let live = &st.topics["t"].partitions[&0];
        assert_eq!(live.active_txns.get(&7), Some(&0), "live transaction still open");
        assert!(live.aborted_txns.is_empty(), "live state not aborted");
        assert_eq!(live.high_watermark, 0, "no control batch appended to the live log");
    }
}
