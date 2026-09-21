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
| Idle RAM, all six services on, empty data | ≤ 30MB |
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
- **One async runtime** with a small thread pool, so connections cost KBs of
  RAM, not MBs.
- **Services start lazily.** A service that no one has connected to allocates
  nothing beyond its listener.

## Not in scope for any system

Clustering, replication, high availability, sharding, performance tuning,
plugins and extensions, and the exact output of internal tools such as
`EXPLAIN`.

## Per system

### Redis (port 6379)
- **In scope:** RESP2/RESP3; strings, hashes, lists, sets, sorted sets,
  streams, TTLs, pub/sub, MULTI/EXEC, bitmaps, HyperLogLog, Lua scripting.
- **Out of scope:** Cluster mode, Sentinel, modules (RedisJSON, RediSearch),
  replication commands.

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

### Kafka (port 9092)
- **In scope:**
  - A single broker with Metadata, Produce, Fetch, ListOffsets and
    CreateTopics/DeleteTopics.
  - Full consumer groups: join, sync, heartbeat, offset commit and fetch.
  - Idempotent producers.
  - Spring Kafka and Kafka Streams in at-least-once mode.
- **Out of scope (for now):** transactions and exactly-once, multiple brokers,
  KRaft/ZooKeeper APIs, Kafka Connect, Schema Registry.

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

## Build order

1. Redis
2. Postgres
3. Kafka
4. MySQL
5. Elasticsearch
6. ClickHouse
