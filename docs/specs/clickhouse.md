# ClickHouse: noida spec

- **Module:** `src/clickhouse/`, Cargo feature `clickhouse`, branch
  `svc/clickhouse`
- **Ports:** 8123 (HTTP interface), 9000 (native TCP protocol)
- **Target:** ClickHouse 24.8 LTS, server version reported as `24.8.4.13`
- **Reference server:** not installed locally (no Docker). CI uses
  `clickhouse/clickhouse-server:24.8`. Env vars:
  `NOIDA_CLICKHOUSE_REF=http://host:8123`, `NOIDA_CLICKHOUSE_NATIVE_REF=
  host:9000`. Default user `default`, no password.

## 1. Purpose

Analytics code written against ClickHouse (clickhouse-connect/Python,
@clickhouse/client/Node, clickhouse-java/JDBC, clickhouse-go (native),
clickhouse-rs, Grafana-style dashboards, dbt-clickhouse) works unchanged,
at local data sizes. ClickHouse's SQL dialect differs a lot from Postgres:
it gets its own parser/analyzer on top of noida's shared storage and
expression machinery where that's reusable (coordinate with the SQL engine
from `svc/postgres`), but columnar and aggregate-heavy semantics are
ClickHouse-specific.

## 2. HTTP interface (P0)

- `GET/POST /` with the query in the `query` parameter and/or the body
  (INSERT data follows the query in the body); `GET /ping` → `Ok.\n`;
  `GET /replicas_status` → `Ok.\n`; `/play` and `/dashboard` are non-goals.
- Auth via `X-ClickHouse-User`/`X-ClickHouse-Key`, `user`/`password` params
  or basic auth. Database via `database` param or `X-ClickHouse-Database`.
- Settings as URL params (e.g. `max_result_rows`, `output_format_*`,
  `date_time_output_format`, `session_id`, `session_timeout`,
  `wait_end_of_query`, `send_progress_in_http_headers` (accept),
  `default_format`, `param_<name>` query parameters with `{name:Type}`
  placeholders).
- Response headers: `X-ClickHouse-Query-Id`, `X-ClickHouse-Format`,
  `X-ClickHouse-Timezone`, `X-ClickHouse-Server-Display-Name`,
  `X-ClickHouse-Summary` (JSON with read/written rows and bytes; elapsed
  can be 0), `Content-Type` per format.
- Sessions (`session_id`) keep temporary tables and SET settings.
- Errors: HTTP 500 (or 404/403 etc. as ClickHouse picks), header
  `X-ClickHouse-Exception-Code`, body in ClickHouse's format, e.g.
  `Code: 60. DB::Exception: Unknown table expression identifier 'x' in
  scope SELECT * FROM x. (UNKNOWN_TABLE) (version 24.8.4.13 (official
  build))\n`. Use the real error codes and names (47 UNKNOWN_IDENTIFIER, 60
  UNKNOWN_TABLE, 62 SYNTAX_ERROR, 81 UNKNOWN_DATABASE, 57
  TABLE_ALREADY_EXISTS, 46 UNKNOWN_FUNCTION, 386 NO_COMMON_TYPE, 53
  TYPE_MISMATCH, 27 CANNOT_PARSE_INPUT_ASSERTION_FAILED, 48 NOT_IMPLEMENTED…).
- Compression: HTTP `Accept-Encoding: gzip`/`zstd` (P1), ClickHouse's own
  `compress=1` block compression (P1; needed by some clients).

## 3. Formats (input and output)

P0: `TabSeparated` (+`WithNames`, `WithNamesAndTypes`, `Raw`), `CSV`
(+`WithNames`, `WithNamesAndTypes`), `JSON`, `JSONCompact`, `JSONEachRow`,
`JSONCompactEachRow` (+WithNames/Types), `JSONStrings`/`JSONCompactStrings`,
`RowBinary` (+`WithNames`, `WithNamesAndTypes` — JDBC and clickhouse-connect
use them), `Values`, `Pretty`/`PrettyCompact` (text tables for humans),
`Null`, `Native` (clickhouse-connect and the native protocol). P1:
`Parquet`/`Arrow` (only if a small dependency is found), `TSKV`,
`CustomSeparated`, `Markdown`, `XML`. Exact escaping, number formatting
(e.g. Float64 printing), date/time formatting and 64-bit integers quoted in
JSON by default (`output_format_json_quote_64bit_integers=1`) must match.

## 4. Native TCP protocol (P1, P0 for Go users)

Client Hello / Server Hello (revision negotiation around 54467), Query,
Data blocks (Native columnar format), Progress, ProfileInfo,
Totals/Extremes, EndOfStream, Exception, Ping/Pong, Cancel,
TableColumns, LZ4 compression of blocks with ClickHouse's CityHash128
checksums. Required by clickhouse-go (default protocol), clickhouse-driver
(Python), and `clickhouse-client` CLI.

## 5. SQL

### Types (P0)
`UInt8/16/32/64/128/256`, `Int8…Int256`, `Float32/64`, `Bool`,
`Decimal(P,S)`/`Decimal32/64/128/256`, `String`, `FixedString(N)`, `UUID`,
`Date`, `Date32`, `DateTime([tz])`, `DateTime64(p[, tz])`, `Enum8/16`,
`LowCardinality(T)` (semantic no-op), `Nullable(T)`, `Array(T)`,
`Tuple(...)` (named and unnamed), `Map(K, V)`, `Nested` (flattened into
arrays as ClickHouse does), `IPv4`, `IPv6`, `JSON`/`Object('json')` (P2).
ClickHouse arithmetic rules: result types of operations (e.g. `UInt8 +
UInt8 → UInt16`), integer overflow wrapping, division always Float64,
`intDiv`, NULL handling only for Nullable columns, default values for
non-nullable columns.

### Statements (P0)
- `CREATE DATABASE`, `CREATE TABLE … ENGINE = …` with engines `MergeTree`,
  `ReplacingMergeTree([ver[, is_deleted]])`, `SummingMergeTree`,
  `AggregatingMergeTree` (P1), `CollapsingMergeTree`/
  `VersionedCollapsingMergeTree` (P1), `Memory`, `Log`/`TinyLog`/
  `StripeLog`, `Null`; clauses `ORDER BY`, `PRIMARY KEY`, `PARTITION BY`,
  `SAMPLE BY` (accepted), `TTL` (row TTL applied periodically; column TTL
  P1), `SETTINGS` (accepted); column `DEFAULT`, `MATERIALIZED`, `ALIAS`,
  `CODEC(...)` (accepted), `COMMENT`. `CREATE TABLE … AS …`, `IF NOT
  EXISTS`, `CREATE TEMPORARY TABLE`, `DROP`, `TRUNCATE`, `RENAME`,
  `EXCHANGE TABLES`, `DETACH/ATTACH` (P1).
- **Merge semantics:** inserts create parts; ReplacingMergeTree,
  SummingMergeTree and Collapsing engines apply their logic when parts merge.
  noida merges on `OPTIMIZE TABLE … [FINAL]` and in the background, and
  `SELECT … FINAL` applies the merge logic at read time. Because a real
  server's background merge timing is nondeterministic, tests compare results
  after `OPTIMIZE … FINAL` or with `FINAL`, which are deterministic.
- `INSERT INTO t [(cols)] VALUES …`, `INSERT … FORMAT <fmt>` + data,
  `INSERT … SELECT`, `INSERT INTO FUNCTION` (P2); async inserts accepted.
- `SELECT` with `WITH` (expressions and CTEs), `DISTINCT [ON]`, `FROM`
  tables/subqueries/table functions (`numbers(N)`, `numbers_mt`, `zeros`,
  `generateRandom` (P1), `values(...)`, `null`, `file`/`url`/`s3` →
  non-goal), `FINAL`, `SAMPLE` (accepted, returns all rows), `ARRAY JOIN`/
  `LEFT ARRAY JOIN`, joins (`INNER/LEFT/RIGHT/FULL/CROSS`, `ANY`/`ALL`/
  `SEMI`/`ANTI`/`ASOF` (P1), `USING`/`ON`), `PREWHERE`, `WHERE`,
  `GROUP BY` (`WITH ROLLUP`, `WITH CUBE`, `WITH TOTALS`, `GROUPING SETS`),
  `HAVING`, `QUALIFY` (P1), `ORDER BY … WITH FILL` (P1) and `NULLS
  FIRST/LAST`, `LIMIT n BY expr`, `LIMIT`/`OFFSET`, `UNION ALL/DISTINCT`,
  `INTERSECT`/`EXCEPT`, window functions, `SETTINGS` clause, `FORMAT` clause.
- `ALTER TABLE … ADD/DROP/MODIFY/RENAME/COMMENT COLUMN`, `ALTER TABLE …
  UPDATE/DELETE WHERE` (mutations, applied synchronously; `mutations_sync`
  accepted), lightweight `DELETE FROM … WHERE`, `ALTER … DROP/DETACH
  PARTITION` (P1), `ALTER … MODIFY TTL/ORDER BY` (P1).
- `SHOW DATABASES/TABLES/CREATE TABLE/COLUMNS/PROCESSLIST(empty)/SETTINGS`,
  `DESCRIBE`, `EXISTS`, `USE`, `SET`, `KILL QUERY` (no-op), `SYSTEM …`
  commands accepted where harmless (e.g. `SYSTEM FLUSH LOGS`),
  `EXPLAIN` minimal (`EXPLAIN SYNTAX`/`AST` P2; no pipeline/performance
  analysis).
- Views: `CREATE VIEW`, **`CREATE MATERIALIZED VIEW … TO … AS SELECT`**
  (insert-triggered, very common in ClickHouse setups; P0 for the `TO`
  form, P1 for inner-table form and `POPULATE`), `LIVE VIEW`/`WINDOW VIEW`
  non-goals.
- Parameterized queries: `{name:Type}` with `param_name` values.

### Functions (P0, ~200 most used)
Aggregates: `count`, `sum`, `avg`, `min`, `max`, `any`, `anyLast`,
`argMin`, `argMax`, `uniq` (exact counts are fine only if ClickHouse's
result is exact for the same data — `uniq` is approximate in ClickHouse, so
port its HyperLogLog-ish algorithm or document the deviation), `uniqExact`,
`uniqCombined` (P1), `groupArray`, `groupUniqArray`, `groupArrayInsertAt`
(P1), `quantile`/`quantiles` (reservoir sampling: deterministic for small
inputs — verify), `quantileExact`, `median`, `stddevPop/Samp`,
`varPop/Samp`, `corr`, `topK` (P1), `sumMap` (P1), `countDistinct`; the
combinators `-If`, `-Array`, `-State`, `-Merge`, `-OrNull`, `-OrDefault`,
`-Distinct`, `-ForEach` (P1), `-Resample` (P2).
Dates: `now`, `now64`, `today`, `yesterday`, `toDate`, `toDateTime`,
`toDateTime64`, `toStartOfYear/Quarter/Month/Week/Day/Hour/Minute/
FiveMinutes/FifteenMinutes/Interval`, `toYYYYMM`, `toYYYYMMDD`,
`toYear/Month/DayOfMonth/DayOfWeek/Hour/Minute/Second`, `toUnixTimestamp`,
`fromUnixTimestamp`, `dateDiff`, `dateAdd/Sub`, `addDays/Hours…`,
`formatDateTime`, `parseDateTimeBestEffort(OrNull)`, `toTimeZone`,
`timeSlot`, `INTERVAL` arithmetic.
Strings: `length`, `lower`, `upper`, `concat`, `substring`, `position`,
`like`/`ilike`, `match`, `extract`, `replaceAll`, `replaceRegexpAll`,
`splitByChar`, `splitByString`, `trim*`, `startsWith`, `endsWith`, `format`,
`toString`, `leftPad`/`rightPad`, `base64Encode/Decode`, `lowerUTF8`,
`empty`/`notEmpty`, `URL` functions (`domain`, `path`, P1).
Arrays: `array`, `arrayJoin`, `has`, `hasAny`, `hasAll`, `indexOf`,
`length`, `arrayMap`, `arrayFilter`, `arrayExists`, `arraySort`,
`arrayReverse`, `arrayDistinct`, `arrayUniq`, `arrayConcat`, `arraySlice`,
`arrayElement`, `groupArray` round trips, `arrayStringConcat`, `range`,
`arrayEnumerate` (P1), lambda syntax `x -> …`.
Conditionals & types: `if`, `multiIf`, `CASE`, `ifNull`, `nullIf`,
`coalesce`, `isNull`, `assumeNotNull`, `toInt8…toUInt64`, `toFloat32/64`,
`toDecimal*`, `toUUID`, `CAST`, `::` cast syntax, `accurateCast` (P1),
`toTypeName`, `reinterpret*` (P2).
Math & other: `abs`, `round`, `floor`, `ceil`, `intDiv`, `modulo`, `pow`,
`sqrt`, `exp`, `log`, `greatest`, `least`, `rand`, `cityHash64`,
`sipHash64`, `xxHash64`, `MD5`/`SHA256` (P1), `generateUUIDv4`,
`JSONExtract*`/`JSONHas`/`JSONLength` (simdjson-compatible results),
`visitParam*` (P1), `tuple`, `tupleElement`, `mapKeys`/`mapValues`,
`bitAnd/Or/Xor`, `IPv4NumToString` family (P1), `version()`,
`currentDatabase()`, `hostName()`, `timezone()`, `uptime()`.

### System tables (P0: what clients and tools query)
`system.databases`, `system.tables` (incl. `engine`, `create_table_query`,
`total_rows`), `system.columns`, `system.settings`, `system.functions`,
`system.data_type_families`, `system.one`, `system.numbers`,
`system.parts` (minimal), `system.clusters` (one local cluster),
`system.users` (default), `system.build_options`, `system.time_zones`
(P1), `system.query_log` → empty (no performance analysis), `system.
processes` → current query only.

## 6. Storage

Columnar storage per table: each insert is a part (column vectors), parts
merge as described above. Row order within a part follows `ORDER BY`. In
memory for the first milestone; column files in the data dir later.
Performance is not a goal — naive row-at-a-time evaluation is acceptable
for local data sizes.

## 7. Client matrix

Scenario: create a MergeTree table and a ReplacingMergeTree table, insert
10k rows via the client's bulk insert (RowBinary/Native/JSONEachRow as each
client uses), aggregate by `toStartOfHour` with `uniqExact` and
`quantile`, `FINAL` query on the Replacing table, parameterized query, an
error (unknown table) surfaced with the right code, `system.tables`
listing.

| Client | Protocol | How to run |
|---|---|---|
| Rust `clickhouse` crate | HTTP + RowBinary | dev-dependency in `tests/clickhouse_client.rs` |
| clickhouse-connect | HTTP (Native/RowBinary) | pip |
| @clickhouse/client | HTTP (JSON formats) | npm |
| clickhouse-java / JDBC v2 | HTTP (RowBinary) | jars from Maven Central, `javac` |
| clickhouse-go v2 | native TCP | P1 (Go not installed locally; CI) |
| clickhouse-driver (Python) | native TCP | pip, P1 |
| dbt-clickhouse, Grafana plugin | P2 |

Commit test apps under `tests/clients/clickhouse/` with a runner script.

## 8. Differential tests

`tests/clickhouse_diff.rs` runs SQL scripts over HTTP against real
ClickHouse and noida in several output formats (TSVWithNamesAndTypes as the
main one, plus JSON and RowBinaryWithNamesAndTypes) and compares bodies
byte for byte, normalizing only query ids, timings in `statistics`, and
`version()`/`uptime()` values. Queries without a deterministic order must
use ORDER BY. Merge-dependent engines are compared after `OPTIMIZE …
FINAL` or with `FINAL`. Error cases compare code and name (messages too
where stable). CI: `clickhouse/clickhouse-server:24.8`. Print the number of
compared queries.

## 9. Non-goals

Distributed tables and clusters (`ON CLUSTER` accepted and ignored on one
node, as ClickHouse does with a single-shard cluster), replicated engines
(`ReplicatedMergeTree` accepted as MergeTree, P1), Keeper/ZooKeeper,
dictionaries (P2), external table engines and table functions reading
S3/HDFS/Kafka/MySQL/Postgres, projections (accepted), skip indexes
(accepted), query profiling, `system.query_log`/`trace_log` contents, the
Play UI, MySQL/Postgres wire compatibility ports of ClickHouse itself.

## 10. Milestones

1. HTTP interface, `SELECT 1`, `SELECT version()`, formats TSV/JSON/
   JSONEachRow, `system.one`/`numbers`; errors in ClickHouse's format.
2. CREATE/INSERT/SELECT on MergeTree/Memory with the P0 types; WHERE,
   GROUP BY, ORDER BY, LIMIT, core functions; diff suite for these.
3. RowBinary/Native formats; ReplacingMergeTree/SummingMergeTree + FINAL/
   OPTIMIZE; materialized views (TO form); HTTP client matrix green;
   `clickhouse` in default features.
4. Native TCP protocol; clickhouse-go and clickhouse-driver pass.
5. P1 items.
