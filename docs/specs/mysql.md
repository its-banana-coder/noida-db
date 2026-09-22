# MySQL: noida spec

- **Module:** `src/mysql/` (to create), Cargo feature `mysql` (to add),
  branch `svc/mysql`
- **Port:** 3306
- **Target:** MySQL 8.0 (8.0.3x+ behaviour), server version string
  `8.0.39-noida`
- **Reference server:** if `/usr/sbin/mysqld` or `mysqld` on PATH is
  available, start it in a temp data dir. CI uses the `mysql:8.0` image.
  Env var: `NOIDA_MYSQL_REF=host:port` (user `root`, empty password,
  database `noida_ref`).

## 1. Purpose

Apps written against MySQL (Java/Spring/Hibernate, Node, Python/Django,
Rails, PHP/Laravel, Go) point at noida and work unchanged, including their
migration tools.

The first usable target is "common dev database", not every MySQL feature:
drivers connect, migration tools create schemas, ORMs introspect them, CRUD
and transactions behave like MySQL, and unsupported features fail with the
same MySQL error class a real server would use.

## 2. Dependency on the Postgres work

noida has one SQL engine. It's being built in the Postgres service
(`svc/postgres`, `src/postgres/`). MySQL must **reuse it**, not fork it:

- Start with the protocol layer and the connection-time queries (section 5),
  which don't need the full engine.
- Once the Postgres engine is on `main`, move the dialect-neutral parts
  (storage, executor, expressions, types) into a shared module if they
  aren't already (e.g. `src/sql/`), in coordination with the Postgres owner,
  and put MySQL's rules behind a dialect switch.
- MySQL and Postgres have **separate catalogs and data**; a table created
  over MySQL is not visible over Postgres.
- MySQL syntax that maps cleanly to the shared engine should be lowered into
  dialect-neutral plan nodes. MySQL-only semantics (collation, coercion,
  `AUTO_INCREMENT`, `ON DUPLICATE KEY UPDATE`, `SHOW`, session variables)
  stay behind a MySQL dialect boundary.
- Do not block the protocol/client-bootstrap milestones on the full shared
  SQL engine. Implement a small bootstrap executor for constant selects,
  `SET`, `SHOW`, `USE` and catalog probes, then replace it with the shared
  engine as soon as the Postgres work lands.

## 3. Protocol requirements

Framing: 3-byte little-endian length + 1-byte sequence id; packets ≥ 16MB
split into 0xFFFFFF chunks. Consider `opensrv-mysql` for the protocol layer;
justify it against binary size.

Packet sequence ids start at 0 for the initial handshake, then reset to 0
for every new command-response exchange. Enforce `max_allowed_packet` as
real MySQL does: an oversized packet gets error 1153 `08S01` ("Got a packet
bigger than 'max_allowed_packet' bytes") and the connection is closed;
out-of-order sequence ids get 1156 ("Got packets out of order"). Confirm the
exact behaviour with the raw-protocol differential tests (section 8).

### 3.1 Connection phase (P0)
- Initial Handshake v10: protocol version 10, server version, connection id,
  20-byte auth scramble, capability flags, charset `utf8mb4_0900_ai_ci`
  (255), status flags, auth plugin name.
- Capabilities to advertise: `CLIENT_LONG_PASSWORD`, `CLIENT_FOUND_ROWS`,
  `CLIENT_LONG_FLAG`, `CLIENT_CONNECT_WITH_DB`, `CLIENT_PROTOCOL_41`,
  `CLIENT_TRANSACTIONS`, `CLIENT_SECURE_CONNECTION`, `CLIENT_MULTI_STATEMENTS`,
  `CLIENT_MULTI_RESULTS`, `CLIENT_PS_MULTI_RESULTS`, `CLIENT_PLUGIN_AUTH`,
  `CLIENT_CONNECT_ATTRS`, `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA`,
  `CLIENT_SESSION_TRACK`, `CLIENT_DEPRECATE_EOF`, `CLIENT_QUERY_ATTRIBUTES`.
  Do not advertise `CLIENT_SSL` (dev use); clients with `sslMode=REQUIRED`
  then fail the same way they would against a non-TLS MySQL.
- Auth plugins: `caching_sha2_password` (8.0 default) and
  `mysql_native_password`, including **AuthSwitchRequest** when the client
  starts with a different plugin. For `caching_sha2_password` without TLS,
  implement the fast-auth path and the full-auth path via
  `request public key` (RSA) or, if RSA is too heavy, document which
  clients need `allowPublicKeyRetrieval=true`/`get-server-public-key`,
  exactly as with real MySQL.
- Users: `root` with empty password by default (matches the `mysql` Docker
  image with `MYSQL_ALLOW_EMPTY_PASSWORD`); accept any user named in
  `CREATE USER` later (P2).
- Connection attributes are accepted and shown in
  `performance_schema.session_connect_attrs` (P2).
- Database selection in the handshake (`CLIENT_CONNECT_WITH_DB`) succeeds
  only for an existing database, otherwise returns 1049 / `42000`.
- `CLIENT_COMPRESS`, TLS and connection phase packet compression are not
  advertised and need not be accepted in P0.

### 3.2 Command phase
| Command | Priority |
|---|---|
| COM_QUERY (incl. multi-statements, multi-results) | P0 |
| COM_PING, COM_QUIT, COM_INIT_DB | P0 |
| COM_STMT_PREPARE / EXECUTE / CLOSE / RESET, binary result rows, parameter types, long data (COM_STMT_SEND_LONG_DATA) | P0 |
| COM_RESET_CONNECTION, COM_SET_OPTION, COM_CHANGE_USER | P1 |
| COM_FIELD_LIST (deprecated, still used by old clients), COM_STATISTICS | P1 |
| COM_STMT_FETCH (cursors), COM_BINLOG_DUMP (reply as a server with binlog off), COM_PROCESS_KILL | P2 |

Prepared statement P0 means enough for real drivers and ORMs: prepare parses
and returns parameter/column counts and metadata, execute binds all common
scalar types and returns binary rows, close/reset are accepted, and long data
works for BLOB/TEXT parameters. Server-side cursors can be rejected or fully
buffered until P2.

### 3.3 Results and errors (P0)
- Text and binary result sets, column definition packets with correct type
  codes, flags (NOT_NULL, PRI_KEY, UNIQUE_KEY, UNSIGNED, AUTO_INCREMENT,
  BINARY, BLOB…), charset, length, decimals. Drivers map types from these,
  so they must match real MySQL for the same table.
- OK packets with affected rows, last insert id, status flags
  (`SERVER_STATUS_IN_TRANS`, `SERVER_STATUS_AUTOCOMMIT`,
  `SERVER_MORE_RESULTS_EXISTS`…), warnings count, session-state tracking
  when requested.
- ERR packets with the real MySQL error code, SQLSTATE and message, for
  example: 1064 `42000` "You have an error in your SQL syntax; …", 1146
  `42S02` "Table 'db.t' doesn't exist", 1062 `23000` "Duplicate entry 'x' for
  key 't.PRIMARY'", 1054 `42S22` "Unknown column 'c' in 'field list'", 1049
  `42000` "Unknown database 'x'", 1050 `42S01` "Table 't' already exists",
  1452/1451 foreign keys, 1048 "Column 'c' cannot be null", 1366/1265 data
  truncation, 1235 `42000` "This version of MySQL doesn't yet support '…'"
  for unsupported features.
- `SHOW WARNINGS` reflects warnings produced by the previous statement.

## 4. SQL requirements (MySQL dialect)

P0 is split into bootstrap SQL (needed before the shared engine) and engine
SQL (needed before MySQL enters default features).

### P0a: bootstrap SQL and session state

- Constant queries and variables: `SELECT 1`, `SELECT VERSION()`,
  `SELECT DATABASE()`, `SELECT CONNECTION_ID()`, `SELECT USER()`,
  `SELECT CURRENT_USER()`, `SELECT @@...` for the variables in section 5,
  and simple aliases.
- `SET` forms from section 5. Store session variables even when behaviour is
  only partially implemented, and reject unknown/invalid variables with the
  real MySQL error code/message.
- `USE`, `SHOW DATABASES`, `SHOW TABLES`, `SHOW VARIABLES`, `SHOW WARNINGS`,
  `SHOW ENGINES`, `SHOW CHARACTER SET`, `SHOW COLLATION`, and empty/minimal
  `information_schema`, `mysql`, `performance_schema` and `sys` tables used
  by client bootstrap probes.
- `CREATE DATABASE`, `DROP DATABASE`, and enough catalog state for drivers
  that connect with a default schema.

### P0b: engine SQL

- DDL: `CREATE/DROP DATABASE|SCHEMA [IF [NOT] EXISTS]`, `USE`,
  `CREATE/DROP/ALTER TABLE` (ADD/DROP/MODIFY/CHANGE/RENAME COLUMN, ADD/DROP
  INDEX/KEY/CONSTRAINT, RENAME TO), `CREATE/DROP INDEX`, `TRUNCATE`,
  `RENAME TABLE`, table options (`ENGINE=InnoDB`, `DEFAULT CHARSET`,
  `COLLATE`, `AUTO_INCREMENT=n`, `COMMENT`) accepted and shown back in
  `SHOW CREATE TABLE` the way MySQL shows them.
- Column types: TINYINT…BIGINT [UNSIGNED] [ZEROFILL], DECIMAL/NUMERIC,
  FLOAT/DOUBLE, BIT, BOOL(=TINYINT(1)), CHAR/VARCHAR/BINARY/VARBINARY,
  TINYTEXT…LONGTEXT, TINYBLOB…LONGBLOB, ENUM, SET, DATE, TIME, DATETIME(fsp),
  TIMESTAMP(fsp) (with `DEFAULT CURRENT_TIMESTAMP` / `ON UPDATE
  CURRENT_TIMESTAMP`), YEAR, JSON.
- Constraints: PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT (incl. expression
  defaults), AUTO_INCREMENT (per-table counter, `LAST_INSERT_ID()`), FOREIGN
  KEY with ON DELETE/UPDATE actions, CHECK (enforced, 8.0.16+).
- DML: INSERT (multi-row, `INSERT … SET`, `INSERT IGNORE`,
  `ON DUPLICATE KEY UPDATE` with `VALUES()`/row alias), REPLACE, UPDATE and
  DELETE (incl. `ORDER BY … LIMIT`, multi-table forms), SELECT with joins
  (INNER/LEFT/RIGHT/CROSS/STRAIGHT_JOIN/NATURAL/USING), subqueries, derived
  tables, CTEs (incl. recursive), UNION [ALL], GROUP BY [WITH ROLLUP],
  HAVING, ORDER BY, `LIMIT n,m`/`LIMIT n OFFSET m`, window functions,
  `SELECT … FOR UPDATE`/`LOCK IN SHARE MODE` (accepted; single writer).
- Transactions: `START TRANSACTION`, `BEGIN`, `COMMIT`, `ROLLBACK`,
  `SAVEPOINT`, `SET autocommit`, `SET TRANSACTION ISOLATION LEVEL`
  (accepted; behaviour is serializable).
- Identifiers and literals: backticks, `"` as a string (unless
  `ANSI_QUOTES`), `0x`/`X''`/`b''` literals, `_utf8mb4'…'` introducers,
  `-- `, `#` and `/* */` comments, `/*! … */` executable comments (mysqldump
  output), user variables `@x`, `:=`.
- **MySQL semantics that differ from Postgres** (these are the point of the
  dialect switch and must be exact): case-insensitive comparisons under the
  default collation (`'a' = 'A'` is true; the default
  `utf8mb4_0900_ai_ci` is a NO PAD collation, so trailing spaces are
  significant, unlike the older PAD SPACE collations), implicit type conversion (`'1abc' + 1 = 2` with a
  warning), integer division rules (`/` returns DECIMAL, `DIV` integer),
  NULL-safe `<=>`, `||` as OR by default, zero dates and `sql_mode`
  effects (strict mode default: `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,
  NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,
  NO_ENGINE_SUBSTITUTION`), boolean results as 1/0, `GROUP BY` rules under
  ONLY_FULL_GROUP_BY.
- Functions (P0 set): `NOW()`, `CURDATE()`, `CURTIME()`, `UTC_TIMESTAMP()`,
  `DATE_FORMAT`, `STR_TO_DATE`, `DATE_ADD/SUB`, `DATEDIFF`, `TIMESTAMPDIFF`,
  `UNIX_TIMESTAMP`, `FROM_UNIXTIME`, `YEAR/MONTH/DAY…`, `CONCAT`, `CONCAT_WS`,
  `SUBSTRING`, `LEFT/RIGHT`, `LENGTH`, `CHAR_LENGTH`, `LOWER/UPPER`, `TRIM`,
  `REPLACE`, `LPAD/RPAD`, `LOCATE/INSTR`, `IFNULL`, `COALESCE`, `NULLIF`,
  `IF`, `CASE`, `CAST/CONVERT`, `ROUND/FLOOR/CEIL/ABS/MOD`, `RAND`, `UUID()`,
  `GROUP_CONCAT`, `COUNT/SUM/AVG/MIN/MAX`, `JSON_EXTRACT`/`->`/`->>`,
  `JSON_OBJECT`, `JSON_ARRAY`, `JSON_UNQUOTE`, `LAST_INSERT_ID()`,
  `ROW_COUNT()`, `FOUND_ROWS()`, `DATABASE()`, `USER()`/`CURRENT_USER()`,
  `VERSION()`, `CONNECTION_ID()`.

P0b can initially reject features that the shared engine cannot represent,
but only with the same error a real MySQL-compatible system would surface for
an unsupported construct. Do not return partial or silently different
results.

### P1
Views, `CREATE TABLE … LIKE/AS SELECT`, generated columns, `INSERT … SELECT`,
full JSON function set, `LOAD DATA LOCAL INFILE` (client-side file), prepared
statements via SQL (`PREPARE/EXECUTE`), `LOCK/UNLOCK TABLES` (accepted),
`GET_LOCK/RELEASE_LOCK` (Laravel and others use them), `FULLTEXT` indexes
with `MATCH … AGAINST` (natural language mode), spatial types accepted and
stored.

### P2
Stored procedures/functions and triggers (`CREATE PROCEDURE`, `CALL`, basic
flow control), events (accepted, not scheduled), users/grants (`CREATE
USER`, `GRANT`, `SHOW GRANTS`; enforcement optional), `XA` transactions.

## 5. Connection-time and introspection queries (P0)

Drivers and ORMs send these before any app query. Capture the exact
statements each client in the matrix sends (log them against real MySQL)
and make every one of them work:

- `SET NAMES utf8mb4 [COLLATE …]`, `SET character_set_results = NULL`,
  `SET autocommit=1`, `SET sql_mode=…`, `SET SESSION TRANSACTION …`,
  `SET time_zone=…`, `SET @@session.*`, `SET foreign_key_checks=0|1`,
  `SET unique_checks=0|1`, `SET names`, `SET character_set_client`,
  `SET character_set_connection`, `SET character_set_results`.
- Connector/J's `SELECT @@session.auto_increment_increment AS
  auto_increment_increment, @@character_set_client …` and `SHOW VARIABLES`
  / `SHOW WARNINGS`; `SELECT @@version, @@version_comment, @@tx_isolation /
  @@transaction_isolation, @@max_allowed_packet, @@lower_case_table_names,
  @@sql_mode, @@time_zone, @@system_time_zone, @@wait_timeout`…
- `SHOW DATABASES`, `SHOW TABLES [FROM db] [LIKE …]`, `SHOW FULL TABLES`,
  `SHOW [FULL] COLUMNS FROM t`, `SHOW INDEX FROM t`, `SHOW CREATE TABLE`,
  `SHOW TABLE STATUS`, `SHOW VARIABLES [LIKE …]`, `SHOW STATUS` (minimal
  values), `SHOW PROCESSLIST`, `SHOW ENGINES`, `SHOW CHARACTER SET`,
  `SHOW COLLATION`, `DESCRIBE/DESC t`.
- `information_schema`: SCHEMATA, TABLES, COLUMNS, STATISTICS,
  KEY_COLUMN_USAGE, TABLE_CONSTRAINTS, REFERENTIAL_CONSTRAINTS, ROUTINES,
  VIEWS, CHARACTER_SETS, COLLATIONS, PARAMETERS, with the columns and types
  MySQL 8.0 has (Hibernate schema validation, Flyway, Liquibase, Prisma
  introspection and Rails schema dumps read them).
- `mysql` system schema tables that tools read (`mysql.user` minimal) and
  `performance_schema` / `sys` answered as empty but existing where tools
  probe them.

`EXPLAIN` returns a minimal plausible plan row; `EXPLAIN ANALYZE` and
optimizer trace are out of scope (performance analysis).

## 6. Storage and behaviour

- Databases are namespaces; `lower_case_table_names=0` semantics on Linux
  (table names case-sensitive, column names not).
- Default database state includes `information_schema`, `mysql`,
  `performance_schema` and `sys`. User-created schemas must not collide with
  them the way real MySQL forbids or reserves them.
- AUTO_INCREMENT: never reused after delete within a run; `ALTER TABLE …
  AUTO_INCREMENT=n` honoured; `SHOW CREATE TABLE` shows the next value.
- Collation default is `utf8mb4_0900_ai_ci`; implement comparisons through
  the dialect layer so index uniqueness, `ORDER BY`, `GROUP BY`, joins and
  `WHERE` agree.
- Single writer, statements atomic; transactions roll back fully.
- In-memory is acceptable for the first milestone; persistence to the data
  dir follows the project-wide storage work.
- Time functions use an injectable clock in tests. `NOW()` (and
  `CURRENT_TIMESTAMP`) is fixed for the whole statement; `SYSDATE()` returns
  the time at the moment it executes, as in real MySQL.

## 7. Client matrix (each must run its scenario against noida)

Scenario for every client: connect, create schema via its migration or DDL,
CRUD with parameters, a transaction that rolls back, a unique-violation
error surfaced with the right code, a join + aggregate query, and schema
introspection where the tool does it.

| Client | How to run locally |
|---|---|
| `mysql` CLI 8.0 | installed |
| Connector/J 8.x + Hibernate 6 (schema validate + CRUD) + Flyway | jars from Maven Central, `javac` (Java 17 installed) |
| Spring Boot JDBC template (if feasible without Maven) | optional |
| `mysql2` (Node) + Prisma (migrate + client) | npm |
| PyMySQL + SQLAlchemy 2 + Django ORM migrations | pip |
| Go `go-sql-driver/mysql` | P1 (Go not installed locally; CI) |
| Rails ActiveRecord | P2 (Ruby not installed; CI) |
| Rust `mysql_async` or `sqlx` | dev-dependency in `tests/mysql_client.rs` |

Commit every test app under `tests/clients/mysql/` with one runner script.
The runner starts noida on a random port, optionally starts a real MySQL
reference, runs the same scenario against both where feasible, and prints a
short per-client PASS/SKIP/FAIL summary.

## 8. Differential tests

`tests/mysql_diff.rs` runs SQL scripts against real MySQL and noida and
compares: result rows (as text), column metadata (type, flags, charset,
decimals, name, org_name, table), affected rows, last insert id, warnings
and ERR code/SQLSTATE/message. Scripts cover every P0 bullet above. Local
reference: start `mysqld --initialize-insecure` in a temp datadir on a free
port as the current user. CI: `mysql:8.0` service with
`MYSQL_ALLOW_EMPTY_PASSWORD=yes`. Print the number of compared statements.

Normalize only values that legitimately differ: connection ids, timestamps
when the script did not pin the clock, temp data directories, server host and
port, and warning text containing those values. Do not normalize type codes,
flags, charsets, SQLSTATEs, affected rows, insert ids or result ordering
unless the query has no deterministic order in both servers.

Add raw protocol differential coverage for:
- handshake capability negotiation for the supported auth plugins,
- bad sequence ids, malformed packet lengths and oversized packets,
- prepared statement metadata and binary rows,
- multi-statement / multi-result sequencing,
- `COM_INIT_DB`, `COM_PING`, `COM_QUIT`, and clean EOF/OK behaviour under
  `CLIENT_DEPRECATE_EOF`.

## 9. Non-goals

Replication and binlog, Group Replication/InnoDB Cluster, storage engines
other than InnoDB behaviour (MyISAM etc. accepted as table options),
performance_schema contents, the X Protocol (port 33060), TLS.

Also out of scope for P0: query optimizer fidelity, execution plans beyond a
minimal `EXPLAIN`, storage-engine-specific locking behaviour, online DDL,
partitioning, generated invisible primary keys, histograms and cost-based
statistics.

## 10. Milestones

1. Scaffold `mysql` feature, `src/mysql/`, `spawn(addr)`, CLI routing and a
   skipped diff test that can find `NOIDA_MYSQL_REF` or local `mysqld`.
2. Handshake + both auth plugins + COM_QUERY/COM_PING/COM_INIT_DB; the
   `mysql` CLI connects and runs `SELECT 1`, `SELECT @@version`,
   `SHOW DATABASES`, `CREATE DATABASE app` and `USE app`.
3. Connection-time queries of Connector/J, mysql2 and PyMySQL work, including
   session variables, minimal schemas and `information_schema` probes.
4. Prepared statements work for scalar parameters and binary rows; Connector/J
   and mysql2 parameterized CRUD pass.
5. Engine integration: DDL/DML/SELECT P0b with MySQL semantics; Flyway,
   Hibernate schema validation, SQLAlchemy and Prisma scenarios pass.
6. Differential suite for all P0 protocol, bootstrap and engine SQL is green
   in CI; add `mysql` to default features and update `COMPATIBILITY.md`.
