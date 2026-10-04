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
| Kafka | native binary protocol, topics, consumer groups, configs, transactions with real read_committed isolation and producer fencing | yes |
| MySQL | tables with enforced keys, upserts, `ALTER TABLE`, joins, aggregates, JSON/ENUM, ~90 functions, transactions (incl. `autocommit=0`) and savepoints, prepared statements (text and binary protocol), `information_schema`, subqueries, `UNION`/`INTERSECT`/`EXCEPT`, CTEs (incl. recursive), window functions, `INSERT ... SELECT`, `sql_mode` | yes: mysql_async, pymysql, SQLAlchemy, Django, mysql2, Go + GORM, JDBC, WordPress |
| ClickHouse | HTTP interface, `CREATE`/`INSERT`/`SELECT` on `Memory`/`MergeTree`/`ReplacingMergeTree`/`SummingMergeTree` tables with real `FINAL`/`OPTIMIZE` merge semantics, materialized views (`TO` form), `Nullable(...)` columns, `WHERE`/`GROUP BY`/`ORDER BY`/`LIMIT`, ~25 functions, TSV/JSON/JSONEachRow/RowBinary, chunked request bodies, errors (see below) | yes, for these — the official Rust client works end to end |
| MongoDB | OP_MSG wire protocol, CRUD, unique indexes (see below) | yes, for the official Rust driver |
| Elasticsearch | HTTP layer, CRUD/bulk, match/term/range/bool search with BM25, aggregations (see below) | yes, for the query types implemented |

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

Storage is now persistent (on-disk) and is saved to
`<data_dir>/redis.json` upon a clean process exit (SIGINT/SIGTERM), with
no incremental autosave -- see `src/persistence.rs`. All 16 logical
databases, every key's value (strings, hashes, lists, sets, sorted sets,
streams including consumer groups) and its expiry survive a clean
restart; `DUMP`/`RESTORE`/real RDB/AOF file compatibility is still out
of scope (see below).

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
- Keyspace notifications: `notify-keyspace-events` is enforced and
  `__keyspace@*__` / `__keyevent@*__` messages are published for generic
  events (key `del`, `expired`, `expire`, `persist`, `rename_from`/
  `rename_to`, `move_from`/`move_to`, `copy_to`), streams (`xadd`/`xtrim`/
  `xdel`/`xsetid`/`xclaim`/`xautoclaim`/`xgroup-*`), the HyperLogLog commands
  (`pfadd`/`pfmerge`), and ordinary string/hash/list/set/zset mutations
  (`set`/`setnx`/`setex`/`psetex`/`getset`/`getdel`/`append`/`setrange`/
  `setbit`/`incr`/`decr`/`incrby`/`decrby`/`incrbyfloat`, `hset`/`hmset`/
  `hsetnx`/`hdel`/`hincrby`/`hincrbyfloat`, `lpush`/`rpush`/`lpushx`/
  `rpushx`/`lpop`/`rpop`/`lset`/`lrem`/`linsert`/`ltrim`, `sadd`/`srem`/
  `spop`/`smove` (as `srem`+`sadd`, matching real Redis)/`sinterstore`/
  `sunionstore`/`sdiffstore`, `zadd`/`zincrby` (as `zincr`, matching real
  Redis)/`zrem`/`zinterstore`/`zunionstore`/`zdiffstore`/`zrangestore`, and
  the generic `expire`/`pexpire`/`expireat`/`pexpireat`/`persist`/`rename`/
  `renamenx`/`move`/`copy` commands. Not yet wired: `GETEX`/`MSET`/`MSETNX`/
  `ZREMRANGEBYRANK`/`ZREMRANGEBYSCORE`/`ZREMRANGEBYLEX`/`ZPOPMIN`/`ZPOPMAX`/
  `LMOVE`/`RPOPLPUSH`/`LMPOP`/`ZMPOP` and the blocking variants of the
  list/zset pop and move commands - those still don't emit events even
  though their event-type flags are accepted by `CONFIG SET`.
- `maxmemory`, eviction policies and the OOM error. The setting is stored; it
  is not enforced.
- Lua libraries `struct` and `bit`. `cjson`, `cmsgpack` and the `redis` table
  are available.
- Real RDB/AOF file format compatibility (`DUMP`/`RESTORE`, a real RDB
  file another Redis could load) — see the persistence paragraph above:
  data survives a clean restart via this project's own on-disk JSON
  snapshot format, not Redis's actual binary formats.

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

Storage is now persistent (on-disk) and is saved to
`<data_dir>/postgres.json` upon a clean process exit (SIGINT/SIGTERM),
with no incremental autosave -- see `src/persistence.rs`. Everything
transactional (schemas, tables and their rows, sequences, enum types,
domains) survives a clean restart; a hard kill (`kill -9`) loses
whatever changed since the last clean shutdown but never corrupts the
on-disk snapshot.

Target: PostgreSQL 16 behaviour (14 also compared). Verified against real
servers by `tests/postgres_diff.rs` (about 700 results), by an edge-case
differential (`tests/failure-diff/pg_edges.py`: numbers and casts, strings,
dates, arrays and JSON, DML corner cases, `GROUPING SETS`/`ROLLUP`/`CUBE`,
ordered-set aggregates `percentile_cont`/`percentile_disc`/`mode() WITHIN
GROUP`, named `WINDOW`s) and by psycopg,
SQLAlchemy, Django, asyncpg, Alembic, node-postgres, Knex, TypeORM,
Sequelize, pgx, GORM, sqlx, Npgsql and JDBC (`tests/clients/postgres/run.sh`).
Django's own management commands (`migrate`, including the built-in
`auth`/`admin`/`sessions`/`contenttypes` apps, `makemigrations` for a
schema change, `bulk_create`, joins, aggregates, `F()`/`Q()`, M2M,
transactions and savepoints, introspection) pass end to end. Gitea (a real,
unmodified Go application with a ~115-table production schema, using the
xorm ORM) migrates and starts its actual schema successfully, including
xorm's own per-table column-metadata self-check, and real usage against
it (creating a repository via its API, which round-trips a JSON column
through an extended-protocol parameter) works. The introspection queries
Prisma and
Hibernate send are in the diff tests; `psql`'s `\d`, `\di`, `\dT` and
similar were compared by hand against a real server.

`COPY ... FROM/TO STDIN/STDOUT` (text and CSV) works: `pg_dump`/`psql`
restoring a real dump (the standard "seed my dev DB from a snapshot"
workflow), psycopg's and node-postgres's dedicated `copy()`/`copy-from`
APIs, and the `postgres`/`tokio-postgres` Rust crate's `copy_in`/`copy_out`
(over the extended query protocol, which is what that crate actually uses)
all round-trip byte-for-byte against a real server, including nulls,
arrays, jsonb and embedded newlines/tabs/backslashes — see
`tests/postgres_diff.rs`'s `copy_matches_real_postgres`. `COPY` to/from a
server-side file or program, and `FORMAT BINARY` (used by pgx's `CopyFrom`
fast path), are not implemented; a client always has STDIN/STDOUT
alternatives.

Full-text search (`to_tsvector`/`to_tsquery`/`plainto_tsquery`/
`phraseto_tsquery`/`websearch_to_tsquery`, the `@@` match operator,
`ts_rank`, `setweight`, `tsvector || tsvector`) works for the `'english'`
and `'simple'` configs (any other config name runs as `'simple'`).
`to_tsvector`/`to_tsquery`'s canonical text output and `@@`'s boolean
result match a real server exactly, including phrase (`<->`/`<N>`) and
prefix (`:*`) matching, weight labels (`setweight`'s `A`/`B`/`C`/`D`, kept
through `||` concatenation with the right side's positions correctly
shifted), and `websearch_to_tsquery`'s web-search syntax (`"phrases"`,
`word1 OR word2`, `-excluded`); verified against Django's
`django.contrib.postgres.search` (`SearchVector`/`SearchQuery`/
`SearchRank`) and Miniflux's own full-text index (title/content combined
via `setweight`+`||`) end to end. `ts_rank`'s exact number is a documented
approximation (it orders matches sensibly but doesn't reproduce Postgres's
own formula, which weights lexeme importance labels and document length
nothing here tracks); GIN/GiST indexes and `ts_headline` are not
implemented.

`REFRESH MATERIALIZED VIEW [CONCURRENTLY] name [WITH [NO] DATA]` works: a
materialized view keeps its rows from `CREATE`/the last `REFRESH` until
refreshed again (it does not silently re-run its query on every read), and
`WITH NO DATA` unpopulates it — reading an unpopulated one gives the same
error and hint a real server does. `pg_matviews.ispopulated` reflects this.
`CONCURRENTLY` is accepted and has no effect (no locking to avoid; nothing
here blocks readers while refreshing anyway).

Range types (`int4range`/`int8range`/`numrange`/`daterange`/`tsrange`/
`tstzrange`) work: canonical text (discrete ranges always canonicalize to
`[lower,upper)`, continuous ones keep whatever bounds were given; a
lower bound greater than the upper is a real error, equal bounds are
`empty` unless both are inclusive, in which case it's a genuine
single-point range — all matching a real server exactly), the
constructor functions, `@>`/`<@`/`&&`, and `lower`/`upper`/`isempty`.
Not implemented: `lower_inc`/`upper_inc`, the union/difference/
intersection operators (`+`/`-`/`*`), the adjacency and positional
operators (`-|-`, `<<`, `>>`, `&<`, `&>`), multiranges, and exclusion
constraints.

**By design**

- Replication of any kind (streaming, logical, master/slave, primary/replica
  — whatever it's called). A connection with the `replication` startup
  parameter is treated as an ordinary one, so `pg_basebackup` and
  `pg_recvlogical` do not work.
- Roles and privileges are not enforced: `GRANT`, `REVOKE` and
  `CREATE/ALTER ROLE` are accepted so migrations run. There is one login.
- `EXPLAIN ANALYZE`, statistics views and tuning: `EXPLAIN` returns a minimal
  plan; `VACUUM` and `ANALYZE` are accepted and do nothing.

**Not yet**

- PL/pgSQL, stored procedures, `CREATE PROCEDURE`/`CALL` and triggers,
  and extensions. Deliberately deferred: unlike everything else on this
  list, PL/pgSQL is a real procedural language embedded in SQL (its own
  grammar, control flow, exception handling, `NEW`/`OLD` row access) that
  needs its own interpreter wired into the binder/executor, not a bounded
  parse-and-evaluate addition — it's planned as its own dedicated effort
  once the rest of the compatibility work here is done.
- Full-text search: GIN/GiST indexes, `ts_headline`, any text search
  config other than `'english'`/`'simple'`.
- `COPY` to/from a server-side file or program; `FORMAT BINARY`.
- Concurrency is one writer at a time.

- `BETWEEN SYMMETRIC` (the SQL parser rejects it) and the `GROUPING()`
  function.

**Differs**

- Enum values order and compare by label text, not declaration order (`<`,
  `ORDER BY`, `min`/`max`).
- `ctid`/`xmin`/`cmin`/`xmax`/`cmax`/`tableoid` are selectable and listed in
  `pg_attribute`, but there's no MVCC: `xmin`/`cmin`/`xmax`/`cmax` are fixed
  placeholder values, not real transaction/command ids (`ctid`, the scan
  position, and `tableoid` are real).
- `server_version` reports 16.4.
- `pg_class`/`pg_index`/`pg_attribute` and friends list only user relations,
  not the indexes and columns of the system catalogs themselves (a query that
  scans all of `pg_index` sees fewer rows than on a real server; one that
  names a user table works the same).

## Kafka

Target: Apache Kafka 3.8 KRaft mode (single-broker, node ID 1). Speaks native Kafka binary protocol on port 9092. Supported and verified against real clients (kafkajs, confluent-kafka-python, kafka-go, Java kafka-clients, Spring Kafka): topic DDL (`CreateTopics`, `DeleteTopics`, `CreatePartitions`, `Metadata`), producer/consumer data operations (`Produce`, `Fetch`, `ListOffsets`, `InitProducerId`, every compression codec), consumer group coordinator (`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`, multi-consumer rebalance), group admin & cluster configs (`DescribeGroups`, `ListGroups`, `DeleteGroups`, `DescribeConfigs`, `AlterConfigs`, `IncrementalAlterConfigs`, `DescribeCluster`, `OffsetForLeaderEpoch`, `DescribeLogDirs`, `SaslHandshake`).

The transaction APIs (`InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`, `DescribeTransactions`) are functionally real: a `read_committed` fetch never returns a still-open or aborted transaction's records (tracked per-partition via a last-stable-offset and an aborted-transactions list, reported in `FetchResponse` for API version 4+), a `read_uncommitted` fetch (the default) is unaffected, and a stale producer epoch — a zombie instance superseded by a newer `InitProducerId` for the same `transactional.id` — is fenced (`INVALID_PRODUCER_EPOCH`) on `Produce`, `AddPartitionsToTxn` and `EndTxn`. `EndTxn` appends a control batch to every partition the transaction touched, same as real Kafka. Verified by an engine-level test that produces, aborts and commits real transactional record batches and checks both isolation levels see exactly what they should.
 
Storage is now persistent (on-disk) and is saved to `<data_dir>/kafka.json` upon a clean process exit (SIGINT/SIGTERM), with no incremental autosave -- see `src/persistence.rs`. Topics, record batches, committed consumer group offsets, and idempotent producer sequence state survive a clean restart; open in-flight transactions are resolved to aborted at save time, and live consumer group membership is reset (members transparently rejoin on restart). A hard kill (`kill -9`) loses whatever changed since the last clean shutdown but never corrupts the on-disk snapshot.

**Differs**
- Real Kafka's client discards an aborted batch's bytes itself using the `aborted_transactions` list (the bytes are still on the wire either way); this server simplifies to never serving an aborted batch to a `read_committed` fetch in the first place. Same observable behavior for any real consumer, simpler to implement.
- Consumer group membership reset across restarts: real Kafka can, in some configurations, preserve group membership across a graceful controlled shutdown via static group membership (`group.instance.id`); this engine always resets live membership to empty and relies on the normal consumer rejoin flow.

**By design**
- Multiple brokers, replication factor > 1, Kafka Connect, Schema Registry, ksqlDB, MirrorMaker.

**Not yet**
- Real log segment files and retention/compaction policies — data persists as an on-disk JSON snapshot on clean shutdown, not Kafka's log segment format.

## ClickHouse

Target: ClickHouse 24.8 LTS, HTTP interface on port 8123. Through milestone 3
of `docs/specs/clickhouse.md`:

- `GET`/`POST /` with `query` as a URL param or the request body (plain or
  `Transfer-Encoding: chunked`, which the official Rust client's streaming
  `INSERT` uses); `GET /ping` and `GET /replicas_status`.
- `CREATE TABLE [IF NOT EXISTS] [db.]t (col type, ...) ENGINE = Memory |
  MergeTree | ReplacingMergeTree[(ver[, is_deleted])] |
  SummingMergeTree[(col, ...)] [ORDER BY (...)]`, `CREATE MATERIALIZED VIEW
  [IF NOT EXISTS] name TO target AS SELECT ... FROM source` (the `TO` form;
  `target` must already exist), `INSERT INTO ... [(cols)] VALUES (...), ...`
  or `INSERT INTO ... [(cols)] FORMAT RowBinary[WithNames[AndTypes]]` with
  the rows as `<fmt>`-encoded request-body data, `DROP TABLE [IF EXISTS]`,
  `OPTIMIZE TABLE [db.]t [FINAL]`. Column types: `UInt8/16/32/64`,
  `Int8/16/32/64`, `Float32/64`, `String`, `Bool`, and `Nullable(...)` of
  any of these (a `NULL` literal, `isNull`/`isNotNull`/`ifNull`/`coalesce`
  all work for real against `Nullable` columns; `RowBinary`, `TSV`
  (`\N`), and `JSON`/`JSONEachRow` (`null`) all round-trip `NULL`
  correctly). Any other type given with arguments (`Decimal(10,2)`,
  `Array(String)`) parses but is rejected as `NOT_IMPLEMENTED` — never
  silently accepted as something else.
- `SELECT ... [FINAL]` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY` (only
  by a selected column or alias), `LIMIT`, on `system.one`, `numbers(N)`,
  `system.tables`, `system.columns` and real tables. Expressions: arithmetic
  (`+ - * / %`), comparisons, `AND`/`OR`/`NOT`, parentheses, unary minus,
  string/int/float/bool literals, backtick-quoted identifiers
  (`` `weird name` ``). `HAVING` is evaluated against the query's projected
  output (like `ORDER BY`), so it can reference selected group keys, aliases
  and aggregate results, but not a `GROUP BY` key that wasn't also selected.
  `SELECT ... UNION ALL/DISTINCT SELECT ...` (chained, any number of
  branches; the keyword must be spelled out — bare `UNION` is a syntax
  error, matching ClickHouse's own `union_default_mode` requirement):
  branches are concatenated in order; if *any* `UNION DISTINCT` appears in
  the chain, the whole combined result is deduped (a coarser approximation
  of ClickHouse's more granular per-branch semantics — see **Differs**).
- `DESCRIBE TABLE [db.]t` / `DESC [TABLE] [db.]t`: returns `name`, `type`,
  `default_type`, `default_expression`, `comment`, `codec_expression`,
  `ttl_expression` — the same column set and order real ClickHouse uses (and
  the one the official Rust client's validated-insert path deserializes).
  Column defaults/comments/codecs/TTLs aren't modeled, so those four columns
  are always empty strings. `system.columns` populates `database`, `table`,
  `name`, `type`, `position` (1-based), `default_kind`, `default_expression`,
  `data_compressed_bytes`, `data_uncompressed_bytes`, `marks_bytes`,
  `comment`, `is_in_partition_key`, `is_in_sorting_key`, `is_in_primary_key`,
  `is_in_sampling_key`, `compression_codec`, `character_octet_length`,
  `numeric_precision`, `numeric_precision_radix`, `numeric_scale`,
  `datetime_precision` from table metadata — `is_in_sorting_key`/
  `is_in_primary_key` are 1 for columns in the table's `ORDER BY`; byte
  counts, partition-key membership and precision/scale/octet-length columns
  are always 0 (real ClickHouse reports `NULL` for several of these on
  non-numeric columns, which isn't expressible without `Nullable` support).
- `SHOW DATABASES` (the fixed `default`/`system`/`INFORMATION_SCHEMA`/
  `information_schema` union with any database that has a created table),
  `SHOW TABLES [FROM|IN db] [LIKE 'pattern']` (`%`/`_` wildcards, no `\`
  escaping), `SHOW CREATE TABLE [db.]t` (reproduces the canonical multi-line
  `CREATE TABLE db.t (\`col\` Type, ...) ENGINE = ... [ORDER BY ...]
  [SETTINGS index_granularity = 8192]` statement — the settings tail is
  appended for every `*MergeTree` engine, matching ClickHouse's own default;
  not verified byte-for-byte against a real server, see **Differs**),
  `EXISTS [TABLE] [db.]t` (`1`/`0`), `USE db` and `SET name = value[, ...]`
  (both syntax-checked no-ops — sessions and settings aren't modeled, so
  they don't actually change anything; see **Not yet**).
- **`ReplacingMergeTree`/`SummingMergeTree` merge semantics**, applied on
  demand (there are no parts or background merges to apply them
  incrementally — see **Differs**): `SELECT ... FINAL` computes the merged
  view at read time; `OPTIMIZE TABLE ... FINAL` computes it once and
  overwrites the table's rows; a bare `OPTIMIZE` (no `FINAL`) is a no-op.
  `ReplacingMergeTree` keeps, per `ORDER BY` key, the row with the greatest
  `ver` (or the last inserted if there's no `ver`), then drops rows where
  `is_deleted` is true. `SummingMergeTree` sums numeric columns (all of
  them, or just the ones named in `ENGINE = SummingMergeTree(...)`) grouped
  by the `ORDER BY` key.
- **Materialized views (`TO` form)**: every `INSERT` into the source table
  re-runs the view's `SELECT` (`WHERE`/`GROUP BY`/aggregates included) over
  just the newly inserted rows and appends the result to the target table —
  not the whole table, so repeated inserts don't reprocess old data.
- ~25 functions: `version()`, `currentDatabase()`, `hostName()`,
  `timezone()`, `uptime()`, `toString`, `toInt8..64`/`toUInt8..64` (range
  checked), `toFloat32/64`, `length`, `upper`, `lower`, `concat`,
  `substring`, `trim`, `replaceAll`, `abs`, `round`, `floor`, `ceil`,
  `greatest`, `least`, `if`, `ifNull`, `coalesce`, `isNull`/`isNotNull`
  (real: check a value against `Val::Null`, working correctly for
  `Nullable` columns; `ifNull`/`coalesce` return the first non-null
  argument, matching real ClickHouse).
- Aggregates: `count`/`count(*)`, `sum`, `avg`, `min`, `max`, `any`,
  `uniqExact`.
- Output formats: `TabSeparated` (+`WithNames`, +`WithNamesAndTypes`), `CSV`
  (+`WithNames`, +`WithNamesAndTypes`; quotes a field only when it contains
  a comma/quote/newline, doubling internal quotes; `\n` row separator,
  matching ClickHouse's default `output_format_csv_crlf_end_of_line=0`,
  not RFC 4180's CRLF), `JSON`, `JSONEachRow`, `Pretty`/`PrettyCompact`
  (box-drawing text tables: numbers right-aligned, everything else
  left-aligned; both format names render identically here — see
  **Differs**), `RowBinary` (+`WithNames`, +`WithNamesAndTypes`) — all in
  both directions where applicable (`SELECT` output and, for `RowBinary`,
  `INSERT` input).
- Errors in ClickHouse's HTTP body format (`Code: N. DB::Exception: ...`)
  with `X-ClickHouse-Exception-Code` and real error codes/names for unknown
  table/database/function/identifier, syntax errors, table-already-exists,
  type mismatches and out-of-range values.
- **Verified against the official Rust `clickhouse` crate**
  (`tests/clickhouse_official_client.rs`): `fetch`/`fetch_all`/`fetch_one`
  (which require `RowBinaryWithNamesAndTypes`) and a streaming `insert` with
  **validation on** (`.with_validation(true)`, the crate's default) — the
  client issues `DESCRIBE TABLE` first to check the target schema, maps
  struct fields to columns by name (so field order doesn't need to match
  the table's declared column order), and only then streams `RowBinary`
  rows; response compression is disabled
  (`.with_compression(Compression::None)` — ClickHouse's own lz4/zstd block
  compression isn't built).

**Differs**
- Storage is row-at-a-time (`Vec<Vec<Val>>`), not columnar; performance is
  not a goal for local-dev data sizes (see docs/specs/README.md).
- No parts or background merges: `ReplacingMergeTree`/`SummingMergeTree`
  rows sit unmerged until `FINAL`/`OPTIMIZE ... FINAL` asks for the merged
  view, computed over the whole table each time rather than incrementally.
- Arithmetic result types are an approximation of ClickHouse's real
  per-width promotion/overflow rules (e.g. `UInt8 + UInt8` doesn't widen to
  `UInt16`); see `src/clickhouse/types.rs`. `sum()`/`avg()` are always
  `Float64` (real ClickHouse keeps `sum(UInt32)` as `UInt64`); inserting
  that into an integer column rounds rather than erroring, since the
  underlying value is still numerically exact for realistic local-dev
  sums. Float formatting isn't verified byte-for-byte against a real server
  (none is installed here).
- Persistent connections: every response closes the socket
  (`Connection: close`); real clients reconnect cleanly, but this differs
  from ClickHouse's keep-alive default.
- `UNION`'s `DISTINCT`-ness is chain-wide, not per-branch: `SELECT a UNION
  ALL SELECT b UNION DISTINCT SELECT c` dedupes the whole combined result of
  all three branches, where real ClickHouse would only dedupe going into the
  `DISTINCT` step.
- `SHOW CREATE TABLE`'s exact formatting (the `SETTINGS index_granularity =
  8192` tail in particular) isn't verified byte-for-byte against a real
  server (none reachable in this environment) — it's ClickHouse 24.8's
  well-known default, reproduced from memory. `Pretty` and `PrettyCompact`
  render identically (both as the compact grid, no blank spacer rows), and
  their exact spacing/width tie-break rules aren't verified byte-for-byte
  either.

**Not yet (milestones 4–5, tracked in the spec)**
- Joins, window functions, subqueries, views (the non-materialized kind),
  the wider function/type library (`Date`/`DateTime`, `Decimal`, `UUID`,
  `Array`, `Tuple`, `Map` — `Nullable` is done, see above), `WITH`/CTEs,
  parameterized queries, sessions (so `USE`/`SET` are accepted but don't
  change behavior), response compression.
- `system.columns`' byte-count/precision/scale columns are still always 0
  for every column, `Nullable` included — real ClickHouse reports `NULL`
  there for non-numeric columns, which needs those columns to themselves
  be `Nullable` in `system.columns`' own (fixed, non-`Nullable`) schema;
  not done yet.
- `Native` format and the native TCP protocol (port 9000) — needed by
  clickhouse-go and clickhouse-driver (Python), and by the official Rust
  client's `fetch_native`.
- `ALTER TABLE`, non-`SELECT` `system.*` writes, `system.settings`/
  `system.functions`/other `system.*` introspection tables beyond `system.one`/
  `numbers`/`tables`/`columns`.
- The wider client matrix (clickhouse-connect, @clickhouse/client, JDBC) and
  flipping `clickhouse` into `default` — the Rust client is the only one
  verified so far.

**By design**
- `BACKUP`/`RESTORE`, Keeper/ZooKeeper, replicated engines beyond being
  accepted as plain `MergeTree`, distributed tables, `ON CLUSTER` beyond
  being accepted and ignored, user/role/quota management, query profiling
  and `system.query_log`/`trace_log` contents.

## MongoDB

The OP_MSG wire protocol and handshake (`hello`/`ismaster`, `buildInfo`, `ping`), CRUD (`insert`, `find`, `update`, `delete`), `createIndexes` with unique-index enforcement (`E11000` duplicate key errors on insert and on index creation over existing duplicate data), and the aggregation pipeline (`$match`, `$project`, `$sort`, `$limit`, `$skip`, `$unwind`, `$group` with `$sum`, `$avg`, `$min`, `$max`, `$count` accumulators).

Compound query operators are supported for exact-match equality and basic comparisons (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$not`), as well as document field path resolution (dot notation).

Verified against the official MongoDB Rust driver and a differential test against a real `mongod`.

**Not yet**
- Replica sets, sharding, transactions, change streams, GridFS.
- Non-unique/TTL indexes.
- Authentication (SCRAM).

## MySQL

Storage is now persistent (on-disk) and is saved to `<data_dir>/mysql.json` upon a clean process exit (SIGINT/SIGTERM), with no incremental autosave -- see `src/persistence.rs`. All schemas, tables, rows, and schema-level `AUTO_INCREMENT` state survive a clean restart; session-scoped fields (`current_db`, `last_insert_id`, mid-transaction uncommitted state) are deliberately excluded and reset on reconnect. A hard kill (`kill -9`) loses whatever changed since the last clean shutdown but never corrupts the on-disk snapshot.

The database is shared across every connection to the same server (one
`CREATE TABLE`/`INSERT` on one connection is visible from any other,
including a brand new one) — session-local state like `current_db` and
`last_insert_id` is still per-connection. This matches real MySQL and
every other service here, but was a real bug until fixed alongside the
WordPress real-app test: each connection used to get its own fresh,
empty, unshared database, invisible to any test that only ever used one
connection but fatal for a real app making more than one (WordPress's
`wp core install` succeeding on one connection, then `wp db tables` on a
separate one reporting "the site you have requested is not installed").

A database named directly in the connection string (`mysql://user@host/db`,
what every real client does — `mysqli_real_connect()`, PyMySQL,
node-mysql2) is selected from the handshake response itself, not just via
a later explicit `USE`; this was also a real bug until fixed, since every
one of this project's own tests happened to issue an explicit `USE` as an
unnoticed workaround and no real client-shaped test ever exercised the
handshake path until WordPress did.

Comparing or doing arithmetic between a `Text` value and a numeric one
(e.g. a `VARCHAR` column compared against an integer literal) coerces the
string to a number the way real MySQL does (parsing the longest leading
numeric prefix, falling back to 0 for anything else), rather than
hard-erroring — found via a real WordPress REST API request (`WP_Query`'s
revision-count subquery joining `wp_posts.post_parent` against a literal)
that used to fail outright where real MySQL just returns the matching
rows.

Handshake (`mysql_native_password`, any password accepted — no real
credential check yet), `COM_INIT_DB`, and `COM_QUERY` for both
literal-only `SELECT` expressions (numeric/string/NULL literals,
`+ - * /`, integer division formats as a 4-decimal-place string matching
MySQL's `div_precision_increment` default rather than a bare float,
comparisons `= <> < <= > >=` with three-valued NULL logic, `AND`/`OR`,
`[NOT] IN (...)` with the same three-valued NULL semantics, parenthesized/
nested boolean expressions) and real tables: `CREATE TABLE` (`INT`/`TINYINT`/`SMALLINT`/`MEDIUMINT`/
`BIGINT` and their `UNSIGNED` forms/`VARCHAR`/`TEXT`/`MEDIUMTEXT`/
`LONGTEXT`/`FLOAT`/`DOUBLE`/`DECIMAL`/`DATE`/`DATETIME`/`BOOLEAN` columns,
`NOT NULL`/`DEFAULT`/`PRIMARY KEY`/`AUTO_INCREMENT`, including a
table-level `PRIMARY KEY (...)` clause — the form WordPress's own core
schema always uses rather than a column option; each integer type and
its `UNSIGNED` form has MySQL's range, though all are stored as `i64`),
`INSERT INTO ...
VALUES (...), ...`, `SELECT` with `WHERE`, qualified column references
(`table.col`, anywhere an expression is allowed, not just `WHERE`) and
basic `INNER`/`LEFT`/cross `JOIN`, `ORDER BY`/`LIMIT`/`OFFSET` on a
non-aggregated query (both `LIMIT n OFFSET m` and MySQL's own
`LIMIT m, n`; sorting/limiting by a column that isn't in the `SELECT`
list at all works too, matching real MySQL), `UPDATE ... SET ... WHERE
...`, `DELETE FROM ... WHERE ...`,
`SHOW DATABASES`/`SHOW TABLES`/`SHOW COLUMNS FROM`/`SHOW CREATE TABLE`,
`USE`, and `@@`-prefixed session variables the connection setup of real
client libraries (e.g. mysql_async) needs (`version`, `version_comment`,
`max_allowed_packet`, `wait_timeout`, `socket`, `lower_case_table_names`).
A real unsupported statement now gets a real MySQL `ERR` packet
(previously a silent `OK`, indistinguishable from "0 rows, no error").

Result sets report real column names in the wire column-definition
packets, labeled the way MySQL labels them: a column by its name, an
alias by the alias, and any other expression by its source text exactly
as written (`count(*)`, `price * 2`; a string literal by its value). That
is what lets a client fetch a row by column name (PHP's `$row['COUNT(*)']`,
PDO's associative fetch mode, any ORM's row hydration). Prepared-statement
(binary protocol) results carry real column types too: integers, doubles,
DATE/DATETIME/TIME in their binary forms, DECIMAL and JSON as text.

Prepared statements (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`/`RESET`) support
real parameter binding: `EXECUTE`'s null-bitmap and (when sent —
per-statement type codes are cached across executions of the same
statement id so a client that only sends them once still works) typed
parameter values are decoded from the wire per the MySQL binary protocol
and substituted for the `?` placeholders the statement was prepared
with, so re-executing the same prepared plan with different bound values
actually runs against those values. Supported bound-parameter wire types:
`TINY`/`SHORT`/`LONG`/`INT24`/`LONGLONG`/`YEAR` (integers),
`FLOAT`/`DOUBLE`, `DECIMAL`/`VARCHAR`/`VAR_STRING`/`STRING`/`BLOB`/`JSON`/
`BIT` (as text), and `DATE`/`DATETIME`/`TIMESTAMP`/`TIME` in their binary
layouts. Verified with mysql_async, Node's mysql2, Go's database/sql and
JDBC Connector/J (`useServerPrepStmts=true`). Placeholders are numbered by
their position in the SQL text, and `LIMIT ?`/`OFFSET ?` work.

`GROUP BY` and the five aggregate functions `COUNT`/`COUNT(*)`/`SUM`/
`AVG`/`MIN`/`MAX` work, including with no `GROUP BY` clause at all (the
whole result set is then one implicit group, so `SELECT COUNT(*) FROM t`
on an empty table correctly returns `0` rather than no rows). Grouping
compares keys with simple equality (linear scan per row, matching this
project's "simple over performant, local-use only" scale) rather than
hashing, so it doesn't need `Value` to implement `Hash`/`Ord`. `HAVING`
and mixing aggregates with non-aggregated, non-grouped columns in the
same projection (undefined in standard SQL, and MySQL's own behavior
there depends on `ONLY_FULL_GROUP_BY`) are not handled specially.

Transactions: `BEGIN`/`START TRANSACTION`, `COMMIT` and `ROLLBACK`, and
the implicit transactions drivers actually use: with `SET autocommit = 0`
(what pymysql, mysqlclient, SQLAlchemy and Django's `atomic()` send), the
first write opens a transaction that lasts until `COMMIT`/`ROLLBACK`, and
`SET autocommit = 1` commits it. DDL and a second `BEGIN` commit an open
transaction, and a connection that disconnects mid-transaction is rolled
back, all as in MySQL. OK/EOF packets report `SERVER_STATUS_IN_TRANS` and
`SERVER_STATUS_AUTOCOMMIT` truthfully. A rollback undoes only its own
connection's writes: tables nobody else touched get their pre-transaction
contents back, and in a table another connection also wrote meanwhile,
only this transaction's own row changes are reversed. `AUTO_INCREMENT` is
not rolled back (as in MySQL). `SAVEPOINT`, `ROLLBACK TO SAVEPOINT` and
`RELEASE SAVEPOINT` work (Django's nested `atomic()`). There is no
isolation: uncommitted writes are visible to other connections
immediately.

`OK` packets now report a real `affected_rows` count for
`INSERT`/`UPDATE`/`DELETE` (a client's `.affected_rows()` — e.g.
`mysql_async`'s `Conn::affected_rows()` — reflects rows actually
inserted/matched/deleted) and a real `last_insert_id` for an
`AUTO_INCREMENT` insert (a client's `.last_insert_id()` — e.g.
`mysqli_insert_id()`, which WordPress's `$wpdb->insert_id` reads directly
— reflects the id generated for the *first* row of a multi-row `INSERT`,
matching real MySQL; like real MySQL, it's per-statement, reported as
0/`None` on any statement that didn't itself generate one; the
`LAST_INSERT_ID()` SQL function returns the connection's most recent
generated id and persists across later statements, as in MySQL).

Verified against `mysql_async` (`tests/mysql_client.rs`, including the
prepared-statement, `GROUP BY`, transaction, `affected_rows` and
`last_insert_id` behavior above), a 50-query e-commerce differential
suite shared with Postgres and translated to MySQL's own dialect
(`tests/ecommerce_mysql_diff.rs`), and a differential test against a real
MySQL server (`tests/mysql_diff.rs`, `NOIDA_MYSQL_REF=host:port`). CI
runs both against a `mysql:8.0` service (root, empty password), the same
server `tests/failure-diff/` compares failure paths against.

`HAVING`, `GROUP BY` and `ORDER BY` accept `SELECT`-list aliases and
positions (`GROUP BY 1`, `HAVING total > 5`, `ORDER BY 2 DESC`), and
`ORDER BY`/`LIMIT`/`OFFSET`/`DISTINCT` apply after aggregation
(`SELECT COUNT(*) FROM t LIMIT 1` counts the whole table). Aggregates take
`DISTINCT` (`COUNT(DISTINCT x)`), and `GROUP_CONCAT` supports `DISTINCT`,
`ORDER BY` and `SEPARATOR`.

Keys: the `PRIMARY KEY` (including a composite one) and every `UNIQUE`
key (column-level or `UNIQUE KEY name (a, b)`) are enforced, comparing
values the way the default `_ci` collation does, with MySQL's own
`1062 Duplicate entry 'x' for key 't.k'` error. A failing `INSERT`,
`UPDATE` or `DELETE` leaves the table exactly as it was. `INSERT IGNORE`,
`REPLACE`, `INSERT ... ON DUPLICATE KEY UPDATE` (with `VALUES(col)` and
the 8.0 `AS new` row alias) and `INSERT ... SET` work, with MySQL's
affected-row counts (1 per insert, 2 per updated duplicate).
`AUTO_INCREMENT` moves past an explicitly inserted id. `UPDATE`/`DELETE`
honor `ORDER BY ... LIMIT`.

`sql_mode` is per session and starts at the 8.0 default (strict).
`SET [SESSION] sql_mode = '...'` (WordPress sends one) changes how writes
are checked, and `@@sql_mode`/`SHOW VARIABLES` report it. Strict mode:
`NULL` into a `NOT NULL` column is error 1048 (an explicit `NULL` is never
replaced by the column's `DEFAULT`, which only fills omitted columns), an
omitted `NOT NULL` column with no default is 1364, an out-of-range number
is 1264, a non-numeric string into a number is 1366, an invalid date is
1292, an over-long `VARCHAR` is 1406, an unknown `ENUM` value is 1265,
and dividing by zero in a written value is 1365. Without strict mode
these become MySQL's adjusted values instead: clamped to the type's
range, the numeric prefix of a string, the zero date, truncated text,
`''`, the type's implicit default, and `NULL`. Invalid `JSON` is always
3140, and an unknown column is always 1054, even on an empty table.
`ONLY_FULL_GROUP_BY` is enforced (1055, and 1140 without `GROUP BY`) with
MySQL's functional dependencies: a grouped column, a primary key or NOT
NULL unique key in `GROUP BY` covering its table, `col = constant` and
column equalities in `WHERE`/`ON`, `ANY_VALUE()`. Columns of derived tables
and CTEs aren't checked (MySQL sees through them; this doesn't), so it may
accept a query MySQL rejects, never the reverse. `SELECT DISTINCT ...
ORDER BY` a column that isn't selected is 3065. The other modes (`ANSI_QUOTES`,
`PIPES_AS_CONCAT`, `NO_BACKSLASH_ESCAPES`, ...) are reported but have no
effect. A failed `INSERT` still uses up the `AUTO_INCREMENT` ids it took,
as in InnoDB. `DROP DATABASE [IF EXISTS]` works. Every failure path above is
compared step by step against real MySQL 8 in CI (`tests/failure-diff/`).
Column names are case-insensitive. Values are
stored as their column's type: `DECIMAL` is exact, rounded to the
column's scale, and arithmetic and `SUM`/`AVG` on exact values stay
exact (`AVG` adds 4 decimal places, like MySQL).

Column types: integers (`TINYINT` to `BIGINT`, signed and `UNSIGNED`,
with their real ranges; stored as `i64`, so `BIGINT UNSIGNED` above
2^63-1 isn't representable; display widths are ignored), `DECIMAL`/`NUMERIC`, `FLOAT`/
`DOUBLE`/`REAL`, `CHAR`/`VARCHAR`/`TEXT` (all sizes), `BOOLEAN`, `DATE`,
`DATETIME`/`TIMESTAMP` (no time zone conversion; everything is UTC),
`ENUM`, `JSON`, and the `BLOB`/`BINARY` family. `DEFAULT CURRENT_TIMESTAMP`
and `ON UPDATE CURRENT_TIMESTAMP` work.

Functions: `IF`, `IFNULL`, `NULLIF`, `COALESCE`, `GREATEST`/`LEAST`,
`CASE`; `ROUND`, `TRUNCATE`, `ABS`, `SIGN`, `CEIL`, `FLOOR`, `MOD`, `DIV`,
`POW`, `SQRT`; `CONCAT`, `CONCAT_WS`, `UPPER`/`LOWER`, `LENGTH`,
`CHAR_LENGTH`, `SUBSTRING`/`MID`, `LEFT`/`RIGHT`, `LPAD`/`RPAD`, `TRIM`
(all forms), `REPLACE`, `REPEAT`, `REVERSE`, `LOCATE`/`INSTR`/`POSITION`,
`FIELD`, `ELT`, `STRCMP`, `HEX`; `CAST(x AS SIGNED|UNSIGNED|CHAR|DECIMAL|
DOUBLE|DATE|DATETIME|TIME|JSON)`; `NOW()` and its synonyms, `CURDATE()`,
`UTC_TIMESTAMP()`, `DATE`, `TIME`, `YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/
`SECOND`, `DAYOFWEEK`, `WEEKDAY`, `DAYOFYEAR`, `QUARTER`, `LAST_DAY`,
`EXTRACT`, `DATE_FORMAT`, `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`,
`d + INTERVAL n unit`, `DATEDIFF`, `TIMESTAMPDIFF`/`TIMESTAMPADD`,
`UNIX_TIMESTAMP`, `FROM_UNIXTIME`; `JSON_EXTRACT`, `->`, `->>`,
`JSON_UNQUOTE`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_VALID`, `JSON_TYPE`,
`JSON_LENGTH`, `JSON_CONTAINS`, `JSON_CONTAINS_PATH`, `JSON_KEYS`;
`CONVERT_TZ`; `DATABASE()`, `USER()`, `VERSION()`, `CONNECTION_ID()`,
`LAST_INSERT_ID()`, `FOUND_ROWS()`. An unknown function is an error,
never a silent NULL.

DDL and introspection: `CREATE TABLE [IF NOT EXISTS]`, `ALTER TABLE`
(`ADD`/`DROP`/`MODIFY`/`CHANGE`/`RENAME COLUMN`, `RENAME TO`, `ADD`/`DROP`
`UNIQUE`/`PRIMARY KEY`/`INDEX`, `ALTER COLUMN SET`/`DROP DEFAULT`,
`AUTO_INCREMENT =`; foreign keys, enforced (below)), `CREATE [UNIQUE]
INDEX`, `DROP TABLE [IF EXISTS]`, `TRUNCATE`, `CREATE DATABASE`, `DESCRIBE`, `SHOW [FULL] TABLES
[LIKE]`, `SHOW COLUMNS`, `SHOW CREATE TABLE`, `SHOW INDEX`, `SHOW
DATABASES`, `SHOW VARIABLES`/`STATUS`/`COLLATION`/`WARNINGS`/`ENGINES`,
`SET` (`sql_mode` and `autocommit` take effect; other variables are
accepted with no effect), and `information_schema.SCHEMATA`/`TABLES`/
`COLUMNS`/`STATISTICS`/`KEY_COLUMN_USAGE`, which is what ORMs and
migration tools query.

Subqueries: derived tables (`FROM (SELECT ...) AS d (a, b)`; 1248
without an alias), scalar subqueries (NULL with no rows, 1242 with more
than one, 1241 with more than one column), `[NOT] IN (SELECT ...)` with
`IN`'s NULL semantics, `[NOT] EXISTS`, and `op ALL|ANY|SOME (SELECT ...)`,
anywhere an expression is allowed (`SELECT`, `WHERE`, `HAVING`,
`ORDER BY`, `UPDATE ... SET`, `DELETE ... WHERE`), correlated with the
enclosing row. `UNION [ALL]`, `INTERSECT` and `EXCEPT`, with `ORDER BY`
(output column name or position) and `LIMIT`, parenthesized branches,
and 1222 on a column-count mismatch. `WITH` and `WITH RECURSIVE` (a
recursive CTE stops with 3636 after 1000 rounds, MySQL's default
`cte_max_recursion_depth`). `INSERT ... SELECT`. Subqueries are
re-run for every row of the enclosing query (no caching or decorrelation),
so they're as slow as the nested loop they describe. A recursive CTE's
columns are not narrowed to the anchor's types (MySQL would truncate a
string that grows past the anchor's width).

Foreign keys are enforced like InnoDB's: a child row needs its parent
(1452), a referenced parent row can't be deleted or have its key changed
(1451) unless the key says `ON DELETE`/`ON UPDATE` `CASCADE` or `SET
NULL` (cascading through further tables and self-references, up to
MySQL's depth of 15), and `REPLACE` counts as a delete. `SET
FOREIGN_KEY_CHECKS = 0` turns all of it off (as dump files do). DDL
follows: a missing referenced table or column is 1824/3734, `DROP TABLE`
and `TRUNCATE` of a referenced table are 3730/1701, a column a foreign key
uses can't be dropped (1828/1829), renamed columns and tables stay
referenced, and `ALTER TABLE ... ADD/DROP FOREIGN KEY` work, checking
existing rows. Unnamed keys get MySQL's `<table>_ibfk_<n>` names, and
`SHOW CREATE TABLE`, `information_schema.KEY_COLUMN_USAGE`,
`TABLE_CONSTRAINTS` and `REFERENTIAL_CONSTRAINTS` report them the way MySQL
does. Writes a cascade makes are rolled back with the transaction. An
inline column `REFERENCES` is ignored, as in MySQL.

Connections are isolated like InnoDB's: a transaction's uncommitted
writes are invisible to other connections, and a write to a row another
transaction has changed (or locked with `SELECT ... FOR UPDATE`/
`FOR SHARE`/`LOCK IN SHARE MODE`) waits until that transaction ends, error
1205 after `innodb_lock_wait_timeout` (50 s, settable per session), or 1213
when two transactions wait for each other (the one that closes the cycle
is rolled back). `NOWAIT` (3572) and `SKIP LOCKED` (job queues) work, and
an INSERT duplicating another transaction's uncommitted key waits for it.

`UPDATE` reports the rows it changed (the rows it matched when the client
connects with `CLIENT_FOUND_ROWS`, as Django does), and an
`ON UPDATE CURRENT_TIMESTAMP` column only moves when the row changed.

Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`,
`CUME_DIST`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`,
`NTH_VALUE`, and `SUM`/`AVG`/`COUNT`/`MIN`/`MAX` over a window, with
`PARTITION BY`, `ORDER BY` (peers share a rank and the default frame), named
windows (`WINDOW w AS (...)`, `OVER (w ORDER BY ...)`), and `ROWS` frames
(`n PRECEDING`/`FOLLOWING`, `UNBOUNDED`, `CURRENT ROW`) and `RANGE` frames
bounded by `UNBOUNDED` or `CURRENT ROW`; over plain and `GROUP BY` queries
(`RANK() OVER (ORDER BY SUM(x))`). A window function in `WHERE`/`HAVING` is
error 3593, as in MySQL.

Decimal arithmetic keeps MySQL's hidden precision: `1/3` displays as
0.3333 but is 0.333333333 inside, so `1/3*3` is 1.0000 and
`ROUND(169/244*100, 4)` is 69.2623.

**Not yet**
- `RANGE` frames with a numeric or `INTERVAL` offset, `GROUPS` frames,
  `DISTINCT` inside a window aggregate (MySQL doesn't support it either),
  `LATERAL` derived tables, and `REGEXP`.
- Multi-table `UPDATE`/`DELETE` (`UPDATE a JOIN b`).
- Plain `KEY`/`INDEX`/`FULLTEXT` declarations are accepted and ignored;
  nothing is indexed, so a foreign key may reference any column (MySQL
  requires an index there, error 1822).
- `ON UPDATE`/`ON DELETE SET DEFAULT` (InnoDB rejects it too) and checks
  that a foreign key's column types are compatible (3780).
- `REPEATABLE READ` snapshots: every statement reads the latest committed
  data (as `READ COMMITTED` does), so a transaction that reads the same rows
  twice can see another connection's commit in between. Gap and next-key
  locks aren't taken (an INSERT into a range another transaction read
  `FOR UPDATE` doesn't wait), and locking reads over joins or `GROUP BY`
  don't lock.
- Multiple semicolon-separated statements in one `COM_QUERY` (only the
  first is executed).
- JSON path wildcards (`$[*]`, `$**`) and the JSON modification functions
  (`JSON_SET`, `JSON_INSERT`, ...).
- Authentication (every password is currently accepted).

## Elasticsearch

Milestone 1 (index/document CRUD) and the first half of milestone 2
(analysis + `_search`) are implemented. The HTTP server returns the
required `X-Elastic-Product` header and supports index create/get/delete,
mapping and settings endpoints, document get/index/create/update/delete,
`_mget`, bulk indexing/deletion, aliases, and index templates. Storage is
now persistent (on-disk) and is saved to `<data_dir>/elasticsearch.json`
upon a clean process exit (SIGINT/SIGTERM), with no incremental
autosave. Bulk chunked transfer encoding is accepted.

`_search` and `_count` work for `match_all`/`match_none`, `match`,
`match_phrase`, `multi_match`, `term`, `terms`, `range`, `exists`, `prefix`,
`wildcard`, `regexp`, `ids`, `bool` (must/should/filter/must_not,
minimum_should_match) and `constant_score`, across a single index, a
comma-separated list, a `name*` prefix, or `_all`/`*`. `match`,
`match_phrase` and `multi_match` score with BM25 (k1=1.2, b=0.75),
including Lucene's lossy per-document field-length norm encoding, so
ranking and `_score` should match real Elasticsearch for the same data —
verified against a real node in `tests/elasticsearch_diff.rs`'s
`p0_new_query_types_match_real_elasticsearch` (runs against
`NOIDA_ELASTICSEARCH_REF`, e.g. CI's service container) alongside the
engine-level tests in `src/elasticsearch/tests/mod.rs`; everything else
(`term`, `range`, `wildcard`, `regexp`, etc.) uses a constant score, matching
how Elasticsearch's structured queries are evaluated. `from`/`size`, `sort`
(field or `_score`, asc/desc), `_source` filtering
(bool/string/array/includes-excludes), and `min_score` are supported. The
`standard`, `simple`, `whitespace`, `keyword` and `stop` analyzers are
implemented and reachable via `_analyze`; `standard` approximates Lucene's
`StandardTokenizer` for common ASCII/word cases rather than full UAX#29
segmentation. Near-real-time semantics are enforced: a write is visible to
real-time GET immediately but not to search until `_refresh` (or
`refresh=true`/`wait_for` on the write).

`match_phrase` requires the query's analyzed terms to appear in a document
at consecutive positions, in order — matching Lucene's default `slop=0`.
Term position here is just index order in a field's analyzed token list
(nothing in the tokenizer pipeline drops or reorders tokens, so position
tracking didn't need a separate data structure to be correct); an explicit
non-zero `slop` option is accepted but currently has no effect (treated as
0). `multi_match` implements the `best_fields` type (Elasticsearch's
default): the query is matched against every listed field independently and
a document's score is its single highest-scoring field (`field^boost`
per-field boosting and `operator: "and"` are both honored) — `most_fields`,
`cross_fields`, `phrase` and `phrase_prefix` multi_match types are not
implemented. `wildcard` (`*`/`?` glob) and `regexp` are constant-score
(unboosted 1.0 by default, same as `term`/`prefix`) and match against a
field's index terms — the raw value for `keyword` fields, analyzed tokens
for `text` fields; `regexp` anchors the whole term the way Elasticsearch
does (the pattern must match start to end, not just find a substring) and
uses `regex-lite`'s syntax, which is close to but not a byte-for-byte match
of Lucene's own regexp dialect (e.g. no `~` complement operator).

Aggregations are implemented: `terms`, `range`, `histogram`, `filter`,
`filters`, `missing` (bucket aggregations, with recursive sub-aggregations
via a nested `aggs`), and `avg`/`sum`/`min`/`max`/`stats`/`value_count`/
`cardinality`/`top_hits` (metric aggregations) — see
`src/elasticsearch/search.rs`.

`term`/`terms`/`bool`/`range`/`exists` queries, `sort`/pagination/`_source`
filtering, the aggregations above, `_update`/`_bulk`/delete, and alias
search (including a zero-downtime alias switch — repointing an alias at a
different index takes effect immediately for both `_search` and every
document-level operation, `GET`/`PUT`/`_bulk`/`_update`/`_mget` included,
not just `_search`) are all verified against a real Elasticsearch node in
`tests/elasticsearch_diff.rs`. Optimistic concurrency
(`?if_seq_no=...&if_primary_term=...` on a document write) is enforced: a
write against a stale `seq_no` gets a real 409 version conflict, also
verified against a real node.

Array fields match per element (`"tags": ["a", "b"]` for `term`,
`terms`, aggregations, `exists`), and multi-field sub-fields
(`"fields": {"raw": {"type": "keyword"}}`, or dynamic `.keyword`) are
queryable, sortable and aggregatable. Dynamic mapping detects ISO dates.
Composable index templates apply to new indices, including ones
auto-created on first write. `_delete_by_query` and `_update_by_query`
(without a script) work on the refreshed view. `_source`,
`_source_includes` and `_source_excludes` are honored as URL parameters
on `_search`, GET and `_mget`, and `_mget` takes the `ids` shorthand.

Not yet built: `query_string`, `simple_query_string`, `search_after`,
scroll/PIT, nested field mappings and nested queries, highlighting,
date-math ranges (`now-1d/d`), `date_histogram`, painless scripts
(`_update` / `_update_by_query` with a `script`), gzip, `_cat`/`_cluster` endpoints, and exact
Elasticsearch error/response parity for every path. This is not ready to
replace Elasticsearch for application workflows that search with more
than the query types above (aggregating is well covered).

## Numbers we do not claim yet

- The footprint targets in `COMPATIBILITY.md` (idle RAM, binary size, "50x
  less memory than the real stack") are **targets**. The only measured figure
  today is the release binary size, checked in CI.
- Data lives in memory for now. The "bounded cache, data on disk" design in
  `COMPATIBILITY.md` is planned, not built.
