# noida-db

One tiny binary for local development that speaks the wire protocols of
Postgres, MySQL, Redis, Kafka and Elasticsearch — so your existing
drivers, ORMs and CLIs point at it unchanged, without running five-plus
heavy servers (or a Docker Compose stack that idles at 2.5–4GB) just to
develop locally.

```
cargo install --path .
noida-db start                      # every built-in service, default ports
noida-db start --only redis,postgres
noida-db start --redis-port 6380
```

See `noida-db help` for the full option list.

## Why

Real Postgres/Redis/Kafka/Elasticsearch/etc. are made to run in production:
replicated, clustered, tuned, and heavy. None of that is useful when you're
just writing and testing an app on a laptop. noida-db implements the parts
of each system a developer actually touches — the commands and APIs real
clients send — and skips everything that only matters in a cluster, so it
can idle in low tens of MB instead of gigabytes, leaving room for the rest
of your dev environment (IDEs, other services, etc).

"100% compatible" means the compatibility test suite passes 100%: real
client libraries, ORMs and CLIs run against noida-db unmodified, and their
results are compared byte-for-byte against the real server. See
[COMPATIBILITY.md](COMPATIBILITY.md) for the footprint targets and rules
every service follows, and [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for
exactly what does not work yet or is intentionally out of scope.

## Tested against real applications

Beyond the compatibility test suite (real client libraries, ORMs and CLIs
compared byte-for-byte against the real server), noida-db is validated by
running actual, unmodified open-source applications against it as their
database — not a synthetic client, a real app doing real work.

| App | What it exercises | Result |
|---|---|---|
| [Gitea](https://about.gitea.com/) (Postgres + Redis) | Full production schema (~115 tables) via the xorm ORM; creating a repository, `git clone`/`git push` over HTTP, issues and comments, and a full pull-request workflow (branch push → PR → merge) via the REST API | ✅ All of the above works end to end |
| [Miniflux](https://miniflux.app/) (Postgres) | Full schema migration (134 migrations, including a `DECLARE`/`FETCH`/`CLOSE` cursor); adding a real RSS feed, fetching and parsing its entries, marking one read, and full-text search over entry titles/content (a `setweight`+`||`-combined index, queried with `websearch_to_tsquery`) | ✅ All of the above works end to end |
| [Faust](https://faust.readthedocs.io/) (Kafka) | Python streaming app pipeline (built on `aiokafka`); dynamically creating topics, concurrent consumer group joins, partition assignments via `SyncGroup`, maintaining continuous `Heartbeat` sessions through consumer rebalances, and actively streaming and decoding incoming records. | ✅ All of the above works end to end |

**RAM usage while running these workflows:** as low as 2MB idle after
boot, peaking at 15MB during the heaviest activity (Gitea's schema-check
phase), settling in the 5–15MB range at rest — well under the [footprint
targets](COMPATIBILITY.md#footprint-targets).

More applications are being added over time.

## Status

Each tested against real client libraries, differential tests against a
real server, and (for several) real unmodified applications:

| Service | State |
|---|---|
| **Postgres** | wire protocol, catalogs, DDL/DML, full-text search, range types, materialized views, tested against psycopg, SQLAlchemy, Django, asyncpg, Alembic, node-postgres, Knex, TypeORM, Sequelize, pgx, GORM, sqlx, Npgsql and JDBC, plus real applications (see above) |
| **MySQL** | handshake, literal-expression `SELECT`, and real tables: `CREATE TABLE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE`, basic `INNER`/`LEFT`/cross `JOIN`, `SHOW TABLES`/`COLUMNS`/`CREATE TABLE`, real `ERR` packets — tested against `mysql_async` and a differential test against a real MySQL 8.0 server; prepared-statement parameter binding, `GROUP BY`/aggregates and transactions are the remaining gaps — see `docs/LIMITATIONS.md` |
| **Redis** | most of the protocol implemented (217 of 242 Redis 7.2 commands) and tested against 12 real client libraries (redis-py, node-redis, ioredis, go-redis, Jedis, Lettuce, Spring Data Redis, Redisson, BullMQ, RQ, Celery, Sidekiq); real keyspace notifications (`notify-keyspace-events`) across generic/stream/HyperLogLog/hash/list/set/zset/string events — see [`src/redis/README.md`](src/redis/README.md) |
| **Kafka** | native binary protocol, topics, consumer groups, cluster configs, and real transactional isolation (`read_committed` fetches, producer fencing on stale epochs) |
| **Elasticsearch** | HTTP layer, index/document CRUD, bulk, `match`/`match_phrase`/`multi_match`/`term`/`range`/`bool`/wildcard/regexp search with real BM25 scoring, and bucket/metric aggregations — see `docs/LIMITATIONS.md` for what's not built yet (`query_string`, `search_after`, nested queries, highlighting) |

Each service is its own Cargo feature (on by default once merged) and can
be switched on or off at build time and at runtime (`--only`); a disabled
service allocates nothing.

**Not built yet, for every service:** none of it persists across a
restart today — everything lives in memory only. `--data-dir` is
accepted but not yet wired to save or load anything; see
`docs/LIMITATIONS.md` and `COMPATIBILITY.md` for the intended design.

## Documentation

- [COMPATIBILITY.md](COMPATIBILITY.md) — the compatibility promise, footprint
  targets, and the rules every service follows (scope filter, no performance
  analysis, reuse-before-you-build).
- [docs/LIMITATIONS.md](docs/LIMITATIONS.md) — what doesn't work, updated by
  every PR.
- [docs/SERVICE_GUIDE.md](docs/SERVICE_GUIDE.md) — how a service is built,
  for anyone adding or extending one.
- [docs/specs/](docs/specs/) — a detailed spec per service (API priorities,
  a client test matrix and milestones), written before implementation and
  kept as the design reference afterward.
- [THIRD_PARTY.md](THIRD_PARTY.md) — code ported from upstream projects
  (e.g. Redis's own algorithms and error texts) and its license.

## Building and testing

```
cargo build --release                       # everything on by default
cargo build --no-default-features --features redis
cargo test --all-features
scripts/check-size.sh                       # binary size budget (40MB)
```

Each service has three layers of tests: engine-level tests with exact
replies and error text, differential tests against the real server
(`NOIDA_<SERVICE>_REF=host:port`, or a local reference binary — see
`scripts/get-redis-oracle.sh`), and real-client tests under `tests/clients/`
that install their own dependencies under `target/` and run with one
command, e.g. `tests/clients/redis/run.sh`. CI runs all of it against real
reference servers.

## License

MIT — see [LICENSE](LICENSE).
