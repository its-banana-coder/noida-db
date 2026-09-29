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
| Kafka | native binary protocol, topics, consumer groups, configs, transaction APIs wired but not yet fenced/isolated (see below) | yes |
| MySQL | early scaffolding, not merged | no |
| ClickHouse | HTTP interface, `CREATE`/`INSERT`/`SELECT` on `Memory`/`MergeTree`/`ReplacingMergeTree`/`SummingMergeTree` tables with real `FINAL`/`OPTIMIZE` merge semantics, materialized views (`TO` form), `WHERE`/`GROUP BY`/`ORDER BY`/`LIMIT`, ~25 functions, TSV/JSON/JSONEachRow/RowBinary, chunked request bodies, errors (see below) | yes, for these — the official Rust client works end to end |
| Memcached | text protocol: set/add/replace/append/prepend/cas/get/gets/gat/gats/delete/incr/decr/touch/flush_all/stats/version/verbosity/quit | yes |
| MongoDB, RabbitMQ, Elasticsearch | specs only (`docs/specs/`) | no |

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
servers by `tests/postgres_diff.rs` (about 665 results) and by psycopg,
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

- `CREATE DATABASE`/`DROP DATABASE`: there's one database per data dir
  (named whatever the client connects to first), so a client that expects
  to provision its own database as part of setup (Gitea's own `gitea
  migrate`, for one) needs to be pointed at an existing database name
  instead (e.g. the default `postgres`).
- Some `information_schema.columns`/`pg_attrdef` default-value text
  doesn't always match a real server's exact formatting (e.g. boolean
  literal case, or a numeric column default reported as empty instead of
  its value) — cosmetic in most cases, but an ORM that compares its own
  expected schema against the live one column-by-column (xorm, which
  Gitea uses, does) may log a spurious mismatch warning for it.
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

The transaction APIs (`AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`, `DescribeTransactions`) are wired on the wire and always answer success, but are **not functionally real yet**: a transactional producer isn't fenced by a newer one using the same `transactional.id`, and a `read_committed` consumer sees aborted records as if they were committed (there's no per-partition staging, last-stable-offset, or control-record filtering). Confirmed against a real transactional Java `kafka-clients` producer and Spring Kafka's `KafkaTemplate`/`TransactionTemplate`. See the Roadmap in `docs/specs/kafka.md` for what real support needs.

**By design**
- Multiple brokers, replication factor > 1, Kafka Connect, Schema Registry, ksqlDB, MirrorMaker.

**Not yet**
- Disk segment persistence (records live in-memory).
- Real transactional isolation and producer fencing (see above).

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
  `Int8/16/32/64`, `Float32/64`, `String`, `Bool`. A type given with
  arguments (`Nullable(String)`, `Decimal(10,2)`) parses but is rejected as
  `NOT_IMPLEMENTED` — never silently accepted as something else.
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
  (always false/true — no `Nullable` type yet, so nothing is ever null).
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
  `Array`, `Tuple`, `Map`, `Nullable`), `WITH`/CTEs, parameterized queries,
  sessions (so `USE`/`SET` are accepted but don't change behavior),
  response compression.
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

## MySQL

Early scaffolding only: the real `mysql` CLI cannot run queries yet.

## Numbers we do not claim yet

- The footprint targets in `COMPATIBILITY.md` (idle RAM, binary size, "50x
  less memory than the real stack") are **targets**. The only measured figure
  today is the release binary size, checked in CI.
- Data lives in memory for now. The "bounded cache, data on disk" design in
  `COMPATIBILITY.md` is planned, not built.
