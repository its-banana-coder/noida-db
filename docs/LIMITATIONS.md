# What doesn't work

noida-db is a **local development** tool. This page lists what it does not do,
what is not built yet, and where it knowingly differs from the real servers.
Every service PR must update its section: if you find a gap that isn't here,
add it.

Legend: **By design** = will not be built (see the scope filter in
`docs/specs/README.md`). **Not yet** = wanted, not built. **Differs** = built,
but not identical to the real server.

## Status at a glance

| Service | State | Usable with real clients? |
|---|---|---|
| Redis | most commands done (see below) | yes |
| Postgres | wire protocol, catalogs, ORMs (see below) | yes, for the drivers tested |
| Kafka | native binary protocol, topics, consumer groups, transactions, configs | yes |
| MySQL | early scaffolding, not merged | no |
| Memcached, MongoDB, RabbitMQ, Elasticsearch, ClickHouse | specs only (`docs/specs/`) | no |

## By design, for every service

None of this is planned, and none of it is stubbed: these commands and APIs
answer as *unknown*.

- Replication, clustering, sharding, sentinel, high availability, failover.
- Backup, restore, snapshot, migration and bulk-transfer machinery.
- Multi-user security management (users, roles, ACL rules), TLS, auditing,
  quotas, encryption at rest. There is one implicit login with no password.
- Performance analysis and tuning: no `EXPLAIN ANALYZE`, profilers or query
  statistics. Where a developer GUI probes an inspection command, the reply is
  empty or minimal.
- Real clustering behaviour of any kind. noida-db is one process on one machine.

## Redis

The full list, with how it is verified, is in [`src/redis/README.md`](../src/redis/README.md).

Target: Redis 7.2 behaviour, RESP2 and RESP3. Of Redis 7.2's 242 commands,
217 are implemented, 22 are out of scope (below) and 3 are not built yet (as of
this writing; `cargo test --test redis_coverage -- --nocapture` prints the
current count).

**By design (unknown command)**

| Commands | Why |
|---|---|
| `DUMP` `RESTORE` `RESTORE-ASKING` `MIGRATE` | RDB payloads and key migration are production tooling |
| `PSYNC` `SYNC` `REPLCONF` `REPLICAOF` `SLAVEOF` `ROLE` `WAIT` `WAITAOF` `FAILOVER` | replication |
| `SENTINEL` | sentinel |
| `CLUSTER` `ASKING` `READONLY` `READWRITE` | clustering |
| `DEBUG` `SHUTDOWN` `PFDEBUG` `PFSELFTEST` | server internals |
| modules (`MODULE LOAD` and friends), RedisJSON, RediSearch | plugins |
| `ACL SETUSER` `DELUSER` `DRYRUN` `LOAD` `SAVE` | user management |

**Not yet**

- `FUNCTION` `FCALL` `FCALL_RO` (Redis Functions). `EVAL`/`EVALSHA`/`SCRIPT`
  work.
- Keyspace notifications: `notify-keyspace-events` can be set, but no
  `__keyspace@*__` / `__keyevent@*__` messages are published, so apps that
  listen for expired-key events see nothing.
- `maxmemory`, eviction policies and the OOM error. The setting is stored; it
  is not enforced.
- Lua libraries `struct` and `bit`. `cjson`, `cmsgpack` and the `redis` table
  are available.
- Persistence. All data lives in memory and is gone when noida-db stops.
  `SAVE`, `BGSAVE` and `BGREWRITEAOF` succeed but write nothing. `--data-dir`
  is not used by Redis yet.

**Differs**

- `SLOWLOG` and `LATENCY` are always empty. `MEMORY USAGE`/`MEMORY STATS`
  report estimates, not Redis's exact byte counts. `INFO` counters that only
  matter for performance analysis are zero.
- `ACL` reports only the `default` user (`WHOAMI`, `USERS`, `LIST`,
  `GETUSER`, `CAT`, `GENPASS`, `LOG`).
- `INCRBYFLOAT` and `HINCRBYFLOAT` reproduce x86-64 `long double` output
  exactly. Real Redis on ARM prints differently, so results can differ from an
  ARM Redis in the last digits.
- Scripts run on Lua 5.1 (vendored); error positions match Redis 7.2.
- Reply order of unordered collections (`KEYS`, `SMEMBERS` on big sets) can
  differ from Redis, as the real order is an implementation detail.

## Postgres

Target: PostgreSQL 16 behaviour (14 also compared). Verified against real
servers by `tests/postgres_diff.rs` (about 535 results) and by psycopg,
SQLAlchemy, Django, asyncpg, Alembic, node-postgres, Knex, TypeORM, pgx,
GORM and JDBC (`tests/clients/postgres/run.sh`). `COPY` (used by pgx's
`CopyFrom`) is a documented gap, reported as such rather than a failure.
Django's own management commands (`migrate`, including the built-in
`auth`/`admin`/`sessions`/`contenttypes` apps, `makemigrations` for a schema
change, `bulk_create`, joins, aggregates, `F()`/`Q()`, M2M, transactions and
savepoints, introspection) pass end to end. The introspection queries Prisma
and Hibernate send are in the diff tests; `psql`'s `\d`, `\di`, `\dT` and
similar were compared by hand against a real server.

**By design**

- Replication of any kind. A connection with the `replication` startup
  parameter is treated as an ordinary one, so `pg_basebackup` and
  `pg_recvlogical` do not work.
- Roles and privileges are not enforced: `GRANT`, `REVOKE` and
  `CREATE/ALTER ROLE` are accepted so migrations run. There is one login.
- `EXPLAIN ANALYZE`, statistics views and tuning: `EXPLAIN` returns a minimal
  plan; `VACUUM` and `ANALYZE` are accepted and do nothing.

**Not yet**

- PL/pgSQL and stored procedures, extensions, `COPY`.
- Concurrency is one writer at a time.

**Differs**

- Enum values order and compare by label text, not declaration order (`<`,
  `ORDER BY`, `min`/`max`).
- `pg_attribute` has no system columns (`ctid`, `xmin`, ...).
- `server_version` reports 16.4.
- `pg_class`/`pg_index`/`pg_attribute` and friends list only user relations,
  not the indexes and columns of the system catalogs themselves (a query that
  scans all of `pg_index` sees fewer rows than on a real server; one that
  names a user table works the same).

## Kafka

Target: Apache Kafka 3.8 KRaft mode (single-broker, node ID 1). Speaks native Kafka binary protocol on port 9092. Supported: topic DDL (`CreateTopics`, `DeleteTopics`, `CreatePartitions`, `Metadata`), producer/consumer data operations (`Produce`, `Fetch`, `ListOffsets`, `InitProducerId`), consumer group coordinator (`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`), group admin & cluster configs (`DescribeGroups`, `ListGroups`, `DeleteGroups`, `DescribeConfigs`, `AlterConfigs`, `IncrementalAlterConfigs`, `DescribeCluster`, `OffsetForLeaderEpoch`, `DescribeLogDirs`, `SaslHandshake`), and transactions (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`, `DescribeTransactions`).

**By design**
- Multiple brokers, replication factor > 1, Kafka Connect, Schema Registry, ksqlDB, MirrorMaker.

**Not yet**
- Disk segment persistence (records live in-memory).

## MySQL

Early scaffolding only: the real `mysql` CLI cannot run queries yet.

## Numbers we do not claim yet

- The footprint targets in `COMPATIBILITY.md` (idle RAM, binary size, "50x
  less memory than the real stack") are **targets**. The only measured figure
  today is the release binary size, checked in CI.
- Data lives in memory for now. The "bounded cache, data on disk" design in
  `COMPATIBILITY.md` is planned, not built.
