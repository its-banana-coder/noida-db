# Kafka: noida spec

- **Module:** `src/kafka/` (stub exists), Cargo feature `kafka`, branch
  `svc/kafka`
- **Port:** 9092, **native Kafka binary protocol** (not HTTP)
- **Target:** Apache Kafka 3.8 in KRaft mode, single broker, node id 1
- **Reference server:** none installed locally (no Docker); CI uses
  `apache/kafka:3.8.0` as a single-node KRaft broker. Env var:
  `NOIDA_KAFKA_REF=host:port`.

## 1. Purpose

Producers, consumers, stream processors and admin tools work unchanged
against noida. The project's users are largely Java/Spring developers, so
the Java `kafka-clients` library is the primary target, followed by
librdkafka-based clients (confluent-kafka-python, node-rdkafka, Go
confluent-kafka-go), kafkajs, franz-go and sarama.

## 2. Protocol requirements

- Framing: 4-byte big-endian size, request header v0–v2 (api key, version,
  correlation id, client id, tagged fields), response header v0/v1.
  Flexible versions (compact types + tagged fields) must be supported
  wherever a client may pick them.
- Use the `kafka-protocol` crate for message encoding/decoding and record
  batches. Justify any other dependency against binary size.
- Version negotiation: **ApiVersions** advertises exactly the (min, max)
  ranges noida implements for each key. A request at an unsupported version
  gets `UNSUPPORTED_VERSION` (35), and ApiVersions itself falls back to v0
  in that case, like a real broker.
- The broker advertises the host:port it's bound to (configurable later as
  `advertised.listeners`); cluster id is a stable random base64 UUID stored
  in the data dir.
- Every error code returned must be one a real broker returns in that
  situation (e.g. `UNKNOWN_TOPIC_OR_PARTITION` 3, `NOT_COORDINATOR` 16,
  `ILLEGAL_GENERATION` 22, `UNKNOWN_MEMBER_ID` 25, `REBALANCE_IN_PROGRESS`
  27, `TOPIC_ALREADY_EXISTS` 36, `INVALID_PARTITIONS` 37,
  `INVALID_REPLICATION_FACTOR` 38, `OFFSET_OUT_OF_RANGE` 1,
  `OUT_OF_ORDER_SEQUENCE_NUMBER` 45, `INVALID_PRODUCER_EPOCH` 47,
  `INVALID_TXN_STATE` 48, `MEMBER_ID_REQUIRED` 79).

## 3. API keys by priority

### P0 (a Java producer and consumer group work end to end)
| Key | API | Notes |
|---|---|---|
| 18 | ApiVersions | incl. client software name/version tagged fields |
| 3 | Metadata | `auto.create.topics.enable=true` behaviour (default 1 partition, RF 1; `num.partitions` configurable), topic ids, `allow_auto_topic_creation` flag honoured |
| 0 | Produce | record batch v2 (magic 2); acks 0/1/-1 all behave as acks=all on one node; returns base offset, log append time when `message.timestamp.type=LogAppendTime`; compression none, gzip, snappy, lz4, zstd (decode and keep as sent) |
| 1 | Fetch | long polling (`max.wait.ms`, `min.bytes`, `max.bytes`, `partition max bytes`), returns whole batches, `OFFSET_OUT_OF_RANGE`, isolation level, fetch sessions (v7+; may always reply with session id 0 = sessionless) |
| 2 | ListOffsets | earliest (-2), latest (-1), by timestamp, max timestamp (-3) |
| 10 | FindCoordinator | group and transaction coordinators = this broker (batched v4+ form) |
| 11 | JoinGroup | classic protocol, member id assignment (MEMBER_ID_REQUIRED round trip), generation ids, protocol selection, static membership (`group.instance.id`) |
| 14 | SyncGroup | leader assignment distribution |
| 12 | Heartbeat | session and rebalance timeouts enforced with the clock |
| 13 | LeaveGroup | incl. batched members (v3+) |
| 8 | OffsetCommit | incl. generation checks, retention ignored |
| 9 | OffsetFetch | single and multi-group forms, `require_stable` |
| 19 | CreateTopics | partitions, replication factor 1 only (else `INVALID_REPLICATION_FACTOR`), configs, `validate_only` |
| 20 | DeleteTopics | by name and topic id |
| 22 | InitProducerId | idempotence is **on by default** in kafka-clients ≥ 3.0, so this is P0 |

Idempotent producer: per (producer id, epoch, partition) sequence tracking,
duplicate batches acknowledged without re-append, gaps rejected with
`OUT_OF_ORDER_SEQUENCE_NUMBER`.

### P1 (Spring Kafka, Kafka Streams, admin tools)
| Key | API |
|---|---|
| 15 / 16 / 42 | DescribeGroups / ListGroups / DeleteGroups |
| 32 / 33 / 44 | DescribeConfigs / AlterConfigs / IncrementalAlterConfigs (topic + broker configs with real default values and sources) |
| 60 | DescribeCluster |
| 37 | CreatePartitions |
| 21 | DeleteRecords |
| 24 / 25 / 26 / 28 | AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit — **transactions** incl. control records, LSO, `read_committed` fetch with aborted transaction lists |
| 65 / 66 | DescribeTransactions / ListTransactions |
| 47 | OffsetDelete |
| 23 | OffsetForLeaderEpoch (epoch 0 everywhere) |
| 29–31 | DescribeAcls (empty) / CreateAcls / DeleteAcls (accept, store, no enforcement) |
| 36 | SaslHandshake + 17 SaslAuthenticate — PLAIN accepted for clients configured with SASL |
| 61 / 71 | DescribeProducers / DescribeLogDirs (plausible values) |

Kafka Streams needs transactions (EOS v2), internal topic creation
(`*-changelog`, `*-repartition`) with configs like `cleanup.policy=compact`,
and DeleteRecords on repartition topics.

### P2
KIP-848 consumer group protocol (ConsumerGroupHeartbeat, 68), log
compaction honouring `cleanup.policy=compact` (keep latest per key,
tombstones), retention by time/bytes, quotas APIs (describe → empty), SCRAM
auth, KRaft controller APIs (answer as a broker without controller
listener).

## 4. Storage and behaviour

- Topics → partitions → append-only logs of record batches, stored as
  received (offset rewriting on append only). One lock is fine.
- In-memory is acceptable for the first milestone. Later: segment files in
  the data dir (`<data>/kafka/<topic>-<p>/`), read on demand, so logs don't
  count against RAM.
- Offsets are per partition, starting at 0; high watermark = log end
  offset; LSO accounts for open transactions.
- Group coordinator state machine: Empty → PreparingRebalance →
  CompletingRebalance → Stable → Dead, with the same timeouts and
  rebalance triggers as the real broker (join timeout =
  `rebalance.timeout.ms`, session expiry, `group.initial.rebalance.delay.ms`
  default 3000 but configurable to 0 for tests).
- Internal topics `__consumer_offsets` and `__transaction_state` appear in
  Metadata (as internal) the way a real broker lists them.

## 5. Client matrix

Scenarios: (a) idempotent producer sends 1000 keyed records to a new topic
(auto-created); (b) a consumer group of 2 consumers splits partitions of a
3-partition topic, one leaves, the other takes over, offsets resume after
restart; (c) AdminClient creates/describes/deletes topics and describes
groups; (d) transactional producer + `read_committed` consumer, one
committed and one aborted transaction; (e) Kafka Streams word count (P1).

| Client | How to run |
|---|---|
| Java `kafka-clients` 3.8 (+ `kafka-streams` for P1) | jars from Maven Central (kafka-clients, slf4j-api, lz4-java, snappy-java, zstd-jni), compiled with `javac`; Java 17 is installed; download jars at test time, don't commit them |
| confluent-kafka-python (librdkafka) | pip (binary wheel) |
| kafkajs | npm |
| Rust (`rskafka` or raw `kafka-protocol` requests) | dev-dependency in `tests/kafka_client.rs` |
| Spring Kafka | P1, CI |
| franz-go / sarama | P2, CI |

Commit test apps under `tests/clients/kafka/` with a runner script.

## 6. Differential tests

`tests/kafka_diff.rs` sends the same request sequences to the real broker
and noida and compares decoded responses field by field, normalizing only
what legitimately differs (cluster id, node host/port, timestamps, member
ids, throttle times). Cover: ApiVersions at each version, Metadata with and
without auto-create, produce/fetch at several versions with each
compression codec, ListOffsets variants, full group join/sync/heartbeat
cycle, error cases for every P0 key. Locally print SKIPPED (no broker); CI
uses the `apache/kafka` service. Print the number of compared responses.

## 7. Non-goals

Multiple brokers and replication (RF > 1 is an error, as on a one-broker
cluster), Kafka Connect, Schema Registry, ksqlDB, MirrorMaker, TLS,
KRaft controller quorum behaviour.

## 8. Milestones

1. ApiVersions + Metadata + CreateTopics; `kafka-topics.sh`-equivalent
   AdminClient calls work.
2. Produce/Fetch/ListOffsets + InitProducerId; Java idempotent producer and
   a manual-assign consumer round-trip.
3. Consumer groups; scenario (b) passes with 2 Java consumers.
4. Diff suite for P0 green in CI; `kafka` in default features.
5. P1: configs, transactions, Kafka Streams.
