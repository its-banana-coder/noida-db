# Compatibility & footprint targets

## The promise

An app that uses these systems in the usual way can point at noida on a
laptop and work without code changes, using the common drivers, ORMs,
migration tools and CLIs.

"100% compatible" means **100% of the compatibility test suite passes**. The
suite is a set of real-client scenarios, each run against both the real server
and noida, with the results compared. Anything outside the suite returns the
real system's own "not supported" error, never a silently wrong result.

## Footprint targets

| | Target |
|---|---|
| Binary on disk | ≤ 40MB (CI enforced) |
| Idle RAM, all services on, empty data | ≤ 30MB |
| Typical dev workload (≈100MB data, dozens of connections) | ≤ 150MB |
| Hard cap | `--max-memory`, default 512MB |

For comparison, the real stack idles at roughly 2.5–4GB: Elasticsearch
1–2GB, Kafka 0.4–1GB, MySQL ~400MB, ClickHouse 0.3–0.8GB, Postgres ~50MB and
Redis ~10MB. Docker Desktop's VM comes on top of that.

How we stay inside the targets:
- **Data lives on disk.** Only a bounded cache is held in RAM. Kafka logs are
  read straight from files.
- **Redis data is in memory, as it is in real Redis.** It counts toward
  `--max-memory`, and when the cap is hit noida follows Redis's `maxmemory`
  rules (eviction policies, then the OOM error).
- **A thread per connection,** with small stacks. Only the stack memory a
  connection actually touches counts, so an idle one costs KBs, not MBs.
- **Services start lazily.** A service that no one has connected to allocates
  nothing beyond its listener.

## Modular

Every service is its own module:
- **Build time:** each service is a Cargo feature (`redis`, `postgres`, ...),
  all on by default. `cargo build --no-default-features --features redis,postgres`
  leaves the other services out of the binary entirely.
- **Run time:** `noida start --only redis,postgres` starts only those. A
  service that is off opens no port and allocates no memory.

## Rules for every system

- **Every command and API works.** Where a feature makes no sense on one local
  node (clustering, replication, sharding), noida replies exactly as a
  standalone real server would. That counts as compatible.
- **No performance analysis.** noida never implements EXPLAIN ANALYZE,
  SLOWLOG, LATENCY, profilers or query statistics. Where clients or tools may
  send these, noida accepts them and returns an empty or minimal reply so
  nothing breaks.
- **Not in scope:** real clustering, replication, high availability,
  performance tuning, plugins and extensions.

## Per system

### Redis (port 6379), target Redis 7.2
- **In scope:** all 242 commands and their subcommands
  (`tests/data/redis-7.2-commands.txt`, tracked by a coverage test), RESP2
  and RESP3, Lua scripting and functions. Cluster, replication and Sentinel
  commands reply as a standalone Redis does.
- **Out of scope:** modules (RedisJSON, RediSearch).

### Postgres (port 5432)
- **In scope:**
  - Protocol: simple and extended query; SCRAM, md5 and trust login;
    declining SSL cleanly.
  - SQL: DDL, DML, joins, subqueries, CTEs, window functions, aggregates,
    transactions, `ON CONFLICT`, `RETURNING`, sequences and identity columns,
    constraints.
  - Types: the common ones, including json/jsonb, arrays, uuid, timestamptz
    and numeric.
  - Catalogs: enough of `pg_catalog` and `information_schema` for Hibernate,
    Flyway, Liquibase, Prisma, Django and Rails to look up the schema.
- **Out of scope (for now):** PL/pgSQL and stored procedures, extensions,
  logical replication. Concurrency is one writer at a time.

### Kafka (port 9092, native binary protocol)
- **In scope:**
  - Every API key a single-node KRaft broker advertises in ApiVersions:
    produce, fetch, offsets, topic and config admin, ACLs.
  - Full consumer groups: join, sync, heartbeat, offset commit and fetch.
  - Idempotent producers, transactions and exactly-once.
  - Spring Kafka and Kafka Streams.
- **Out of scope:** multiple brokers, Kafka Connect, Schema Registry.

### MySQL (port 3306)
- **In scope:**
  - Shares the SQL engine with Postgres, with the MySQL dialect on top:
    backtick quoting, `AUTO_INCREMENT`, `ON DUPLICATE KEY UPDATE`, `SHOW`
    commands, `information_schema`, and MySQL's comparison and type
    conversion rules.
  - Login: `caching_sha2_password` and `mysql_native_password`.
  - Connector/J, Hibernate, Flyway and mysql2 working.
- **Out of scope (for now):** stored procedures, triggers, multiple storage
  engines, replication.

### Elasticsearch (port 9200)
- **In scope:**
  - Document CRUD, `_bulk` and index mappings.
  - `_search` with match, multi_match, term(s), range, bool, exists, prefix
    and wildcard.
  - Sorting, pagination, highlighting.
  - Aggregations: terms, date_histogram, sum/avg/min/max, cardinality.
  - The standard analyzers.
  - The official Java and Python clients working.
- **Out of scope:** Painless scripting, the full set of analyzers,
  percolator, ML. Relevance scores are close but not identical to real
  Elasticsearch; result order usually matches.

### ClickHouse (port 8123)
- **In scope:**
  - The HTTP interface with the TSV, CSV, JSONEachRow and RowBinary formats.
  - The MergeTree table family, including ReplacingMergeTree's
    de-duplication behaviour.
  - The 150–200 most-used functions.
  - The JDBC driver working.
- **Out of scope (for now):** distributed tables, dictionaries, the native TCP
  protocol, the long tail of functions.

### Memcached (port 11211)
- **In scope:** the text and binary protocols, every command, including
  `meta` commands; `stats` returns minimal counters.

### MongoDB (port 27017)
- **In scope:**
  - The OP_MSG wire protocol and handshake (`hello`), SCRAM auth.
  - CRUD, the query and update operators, indexes (unique, TTL), the
    aggregation pipeline, change streams, transactions and GridFS.
  - The official drivers, Spring Data MongoDB and Mongoose working.
- **Out of scope:** sharding, replica sets (noida replies as a single-node
  replica set so transactions and change streams work), `$where` and
  server-side JavaScript.

### RabbitMQ (port 5672, management HTTP 15672)
- **In scope:**
  - AMQP 0-9-1 in full: exchanges (direct, fanout, topic, headers),
    queues, bindings, acks and nacks, prefetch, TTLs, dead-lettering,
    publisher confirms.
  - The parts of the management HTTP API that tools and Spring AMQP use.
- **Out of scope:** clustering, federation and shovel, plugins, streams,
  MQTT and STOMP.

## Build order

1. Redis
2. Postgres
3. Kafka
4. MySQL
5. Elasticsearch
6. ClickHouse
7. Memcached (small; can come earlier since it reuses Redis storage)
8. MongoDB
9. RabbitMQ
