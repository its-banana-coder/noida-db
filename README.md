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

## Compatibility scorecard

There's no single "% compatible" number — no such number is actually
measured anywhere, and inventing one would be exactly the kind of claim
this project tries not to make. What *is* measured, per service: how many
real client libraries/ORMs/drivers are verified against it, how many real
unmodified applications run against it end to end, and (where a fixed,
countable target exists, like Redis's command set) direct coverage against
that target. Everything not covered here is either a known, listed gap
(`docs/LIMITATIONS.md`) or a deliberate scope exclusion (`COMPATIBILITY.md`)
— never a silent wrong answer.

| Service | Real clients/ORMs verified | Real apps, end to end | Protocol coverage | Known gaps |
|---|---|---|---|---|
| **Postgres** | 14 (psycopg, SQLAlchemy, Django, asyncpg, Alembic, node-postgres, Knex, TypeORM, Sequelize, pgx, GORM, sqlx, Npgsql, JDBC) | ✅ Gitea, ✅ Miniflux | DDL/DML, full-text search, range types, materialized views, cursors, catalogs (`pg_catalog`/`information_schema`) | PL/pgSQL, stored procedures/triggers, extensions, logical replication — see `docs/LIMITATIONS.md` |
| **Redis** | 12 (redis-py, node-redis, ioredis, go-redis, Jedis, Lettuce, Spring Data Redis, Redisson, BullMQ, RQ, Celery, Sidekiq) | — (not yet targeted; a Sidekiq-driven app is next) | 217 / 242 Redis 7.2 commands (`src/redis/README.md`) | Modules (RedisJSON, RediSearch), the 25 unimplemented commands, mostly production-only (`CLUSTER`, `DEBUG`, ...) |
| **Kafka** | 5 (kafkajs, confluent-kafka-python, kafka-go, Java kafka-clients, Spring Kafka) | ✅ Faust (streaming pipeline) | Full consumer groups, real transactional isolation (`read_committed`, producer fencing), cluster/config admin | Disk segment persistence (in progress — see below), multiple brokers |
| **MySQL** | 1 driver-level (`mysql_async`) | ✅ WordPress | Prepared statements, transactions, `ORDER BY`/`LIMIT`/`GROUP BY`/aggregates, `WHERE ... IN (...)`, real `AUTO_INCREMENT`/`last_insert_id`, cross-connection data sharing, database selection via the connection handshake, real `DEFAULT` clauses, `SQL_CALC_FOUND_ROWS`/`FOUND_ROWS()` | `HAVING`, subqueries, CTEs (ordinary and recursive), window functions, `ALTER TABLE`, multi-statement queries — see `docs/LIMITATIONS.md` |
| **Elasticsearch** | 2 (official Java and Python clients) | — (not yet targeted; a Django + django-elasticsearch-dsl app is next) | `match`/`match_phrase`/`multi_match`/`term`/`range`/`bool`/wildcard/regexp with real BM25 scoring, bucket/metric aggregations, verified against a real Elasticsearch 8.15 node | `query_string`, `search_after`, nested queries, highlighting — see `docs/LIMITATIONS.md`. Operating the ES *ecosystem* (Kibana, Grafana as a data source) is out of scope; a client library searching via the API is what's covered |

✅ = passes end to end in CI, re-run on every relevant change (see
`.github/workflows/real-apps.yml`). 🚧 = in progress, currently red in CI —
listed here instead of hidden, since a real, currently-failing signal is
more useful than silence. More apps are added over time; see "Tested
against real applications" below for what each one actually exercises.

## Tested against real applications

Beyond the compatibility test suite (real client libraries, ORMs and CLIs
compared byte-for-byte against the real server), noida-db is validated by
running actual, unmodified open-source applications against it as their
database — not a synthetic client, a real app doing real work. These run
as their own CI workflow (`real-apps.yml`), separate from the fast
per-push suite since each takes minutes and hits real networks.

| App | What it exercises | Result |
|---|---|---|
| [Gitea](https://about.gitea.com/) (Postgres + Redis) | Full production schema (~115 tables) via the xorm ORM; creating a repository, `git clone`/`git push` over HTTP, issues and comments, and a full pull-request workflow (branch push → PR → merge) via the REST API | ✅ All of the above works end to end |
| [Miniflux](https://miniflux.app/) (Postgres) | Full schema migration (134 migrations, including a `DECLARE`/`FETCH`/`CLOSE` cursor); adding a real RSS feed, fetching and parsing its entries, marking one read, and full-text search over entry titles/content (a `setweight`+`||`-combined index, queried with `websearch_to_tsquery`) | ✅ All of the above works end to end |
| [Faust](https://faust.readthedocs.io/) (Kafka) | Python streaming app pipeline (built on `aiokafka`); dynamically creating topics, concurrent consumer group joins, partition assignments via `SyncGroup`, maintaining continuous `Heartbeat` sessions through consumer rebalances, and actively streaming and decoding incoming records. | ✅ All of the above works end to end |
| [WordPress](https://wordpress.org/) (MySQL) | Real core install via WP-CLI (~12 core tables, no ORM — plain `mysqli`-backed SQL), creating a post and comment, then reading both back through the real REST API | ✅ All of the above works end to end |

**RAM usage while running these workflows:** as low as 2MB idle after
boot, peaking at 15MB during the heaviest activity (Gitea's schema-check
phase), settling in the 5–15MB range at rest — well under the [footprint
targets](COMPATIBILITY.md#footprint-targets).

More applications are being added over time: Ghost and Strapi (MySQL),
Wagtail/django-cms (Postgres), Forem (Postgres + Redis + Elasticsearch),
and a Spring Kafka application are next.

## Status

Each tested against real client libraries, differential tests against a
real server, and (for several) real unmodified applications:

| Service | State |
|---|---|
| **Postgres** | wire protocol, catalogs, DDL/DML, full-text search, range types, materialized views, tested against psycopg, SQLAlchemy, Django, asyncpg, Alembic, node-postgres, Knex, TypeORM, Sequelize, pgx, GORM, sqlx, Npgsql and JDBC, plus real applications (see above) |
| **MySQL** | handshake (including database selection from the connection string itself, not just an explicit `USE`), real tables (`CREATE TABLE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE`, basic `INNER`/`LEFT`/cross `JOIN`, `ORDER BY`/`LIMIT`/`GROUP BY`/aggregates), prepared statements with real parameter binding, `BEGIN`/`COMMIT`/`ROLLBACK`, real `AUTO_INCREMENT`/`last_insert_id`/`affected_rows`, `WHERE ... IN (...)`, `SHOW TABLES`/`COLUMNS`/`CREATE TABLE`, real `ERR` packets, text-to-number coercion in comparisons (matching MySQL's own lenient behavior, not hard-erroring), and data shared correctly across connections (not per-connection state) — tested against `mysql_async`, a differential test against a real MySQL server (including a 50-query e-commerce suite shared with Postgres, translated to MySQL's own dialect), and a real application (WordPress — see above); `HAVING`, subqueries, CTEs, window functions and `ALTER TABLE` are the remaining gaps — see `docs/LIMITATIONS.md` |
| **Redis** | most of the protocol implemented (217 of 242 Redis 7.2 commands) and tested against 12 real client libraries (redis-py, node-redis, ioredis, go-redis, Jedis, Lettuce, Spring Data Redis, Redisson, BullMQ, RQ, Celery, Sidekiq); real keyspace notifications (`notify-keyspace-events`) across generic/stream/HyperLogLog/hash/list/set/zset/string events — see [`src/redis/README.md`](src/redis/README.md) |
| **Kafka** | native binary protocol, topics, consumer groups, cluster configs, and real transactional isolation (`read_committed` fetches, producer fencing on stale epochs) |
| **Elasticsearch** | HTTP layer, index/document CRUD, bulk, `match`/`match_phrase`/`multi_match`/`term`/`range`/`bool`/wildcard/regexp search with real BM25 scoring, and bucket/metric aggregations, verified against a real Elasticsearch 8.15 node — see `docs/LIMITATIONS.md` for what's not built yet (`query_string`, `search_after`, nested queries, highlighting) |

Each service is its own Cargo feature (on by default once merged) and can
be switched on or off at build time and at runtime (`--only`); a disabled
service allocates nothing.

## Pending infrastructure

Actively being worked on, not yet complete — listed here rather than left
implicit:

- **On-disk persistence.** Nothing survives a restart today — every
  service is in-memory only, and `--data-dir` is accepted but unused. The
  shared foundation (a shutdown-hook registry that saves a snapshot on a
  clean SIGINT/SIGTERM, with atomic temp-file-then-rename writes so a save
  interrupted mid-write can't corrupt the snapshot — see `src/persistence.rs`)
  is in place; each service's own load/save is being added next, one small
  PR per service. The model stays simple on purpose: a full snapshot on
  clean shutdown, not incremental or continuous — a hard kill (`kill -9`)
  loses whatever changed since the last clean shutdown, but never corrupts
  the on-disk file.
- **Benchmarking.** The [infrastructure benchmark harness](docs/BENCHMARKING.md)
  now captures repeatable Redis wire-protocol throughput, latency, server-only
  CPU/RSS/IO, optional hardware counters, machine metadata, and SVG reports.
  It is intentionally an engine-efficiency suite, not a production-traffic
  simulation; adapters for equivalent operations in other services will follow.
- **Real-app matrix expansion** — see "Tested against real applications"
  above for what's next.

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

Real, unmodified *applications* (Gitea, Miniflux, WordPress, ...) live
under `tests/apps/` instead — the same one-command-per-app convention, but
run as their own CI workflow (`real-apps.yml`) rather than on every push,
since each takes minutes and hits real networks to download pinned,
checksum-verified releases.

## License

MIT — see [LICENSE](LICENSE).
