# Service specs

Each file here is a self-contained brief for building one noida-db service. An
agent (or person) can pick one up without other context.

| Service | Spec | Port(s) | Status |
|---|---|---|---|
| Redis | (in progress, `src/redis/`) | 6379 | building (`svc/redis-types`) |
| Postgres | (in progress) | 5432 | building (`svc/postgres`) |
| MySQL | [mysql.md](mysql.md) | 3306 | open (after Postgres engine lands) |
| Kafka | [kafka.md](kafka.md) | 9092 | open |
| Memcached | [memcached.md](memcached.md) | 11211 | open |
| MongoDB | [mongodb.md](mongodb.md) | 27017 | open |
| RabbitMQ | [rabbitmq.md](rabbitmq.md) | 5672, 15672 | open |
| Elasticsearch | [elasticsearch.md](elasticsearch.md) | 9200 | open |
| ClickHouse | [clickhouse.md](clickhouse.md) | 8123, 9000 | building (`svc/clickhouse`) |

## Read first, in this order

1. `docs/SERVICE_GUIDE.md`: module layout, the four test layers, CI, commit
   checklist. Every rule there applies.
2. `COMPATIBILITY.md`: the project's promise and footprint targets.
3. `src/redis/` and `tests/redis_*.rs`: the reference implementation. Copy
   its shapes (engine separate from server, injected clock, differential
   test harness with version gating).
4. Your service's spec.

## Principles that override everything else

- **The real server wins.** Specs describe the target, but when a spec and
  the real server disagree, match the real server and fix the spec in the
  same PR. Differential tests are the authority.
- **Usage-first.** Build what real drivers, ORMs and tools send first (the
  P0 lists). Widen to P1/P2 afterwards. A rare edge case waits until a real
  client needs it.
- **Never silently wrong.** Anything unsupported returns the error the real
  server gives for an unsupported feature, never a made-up or partial result.
- **Small and simple.** Performance is not a goal. Thread per connection,
  one lock, plain data structures. Every dependency must be justified
  against binary size (`scripts/check-size.sh`, budget 40MB for the whole
  binary) and idle RAM (tens of MB for the whole process).
- **No performance analysis.** No EXPLAIN ANALYZE, profilers, slow logs or
  query statistics. Commands that request them get a minimal valid reply.
- **Single node.** Clustering and replication features answer the way a
  standalone real server does.
- **Commit the tests.** Tests, test apps, reference-server scripts and CI
  setup are part of the deliverable. They are how the project owner trusts
  the work.

## Scope filter: local development only

noida-db is a development tool. Build what a developer on a laptop uses while
building or debugging an app. **Do not implement, not even as stubs or cheap
error replies**, anything whose purpose is running or operating production:

- **Replication, clustering, sharding, sentinel, HA, failover, leader
  election** (primary/replica, master/slave, whatever a service calls it),
  and the commands that manage or inspect such topologies.
- **Backup, restore, snapshot, migration and bulk-transfer machinery**
  (Redis DUMP/RESTORE/MIGRATE, Elasticsearch snapshots, ClickHouse
  BACKUP/RESTORE, MongoDB replica-set management, MirrorMaker...).
- **Security hardening beyond one default login**: multi-user ACL/RBAC
  management, TLS, auditing, quotas, encryption at rest. (SQL statements that
  ordinary schema migrations contain, such as `GRANT`, may be accepted and
  stored but never enforced; say so in the service's docs.)
- **Operational performance analysis and tuning** (already forbidden). Empty,
  minimal replies for inspection commands that developer GUIs probe on connect
  (for example Redis `SLOWLOG GET`) are fine.
- **Storage-engine internals and maintenance** (compaction tuning, vacuum
  statistics, rebalancing).

Excluded commands and APIs behave as *unknown* (unknown command, unsupported
API key, 404). List them in the service's non-goals and in its coverage test.

A *facade* that clients need in order to connect and work is allowed, with no
commands to manage it: MongoDB reports itself as a one-node replica set so
transactions and change streams work; Kafka advertises one broker.

Litmus test: *would a developer on a laptop use this while building or
debugging an app?* If not, skip it.

## Reuse before you build

Re-inventing costs time and adds bugs. Before writing any non-trivial
component (a protocol codec, parser, algorithm, data structure, function
library, catalog of built-ins), **search GitHub and crates.io for an existing
implementation and use it**:

1. **Use a maintained Rust crate** if it fits: check the licence, `cargo tree`
   for what it drags in, and the binary-size cost with `scripts/check-size.sh`.
2. **Otherwise port a reference implementation to Rust**: the real server's own
   source, or a good open-source project in C, Go or Java. Keep behaviour
   identical, and cite the upstream file and version in the module doc
   (`//! Ported from redis/src/sort.c, 7.2`).
3. **Write from scratch only if nothing suitable exists.** Say in the pull
   request what you searched and why it didn't fit.

The differential tests against the real server are what make porting safe:
port, then compare.

**Licences matter.** noida-db is MIT. Port or copy only code under a permissive
licence (MIT, Apache-2.0, BSD, ISC, PostgreSQL). Never copy GPL, AGPL, SSPL,
Elastic-licence or RSAL code. Record every ported source in `THIRD_PARTY.md`
with its licence, keeping the upstream copyright notice. Verify the licence
of the exact version you read; these change between releases. What we know:

| Project | Source you may port from | Avoid |
|---|---|---|
| Redis | 7.2 and earlier (BSD-3-Clause) | 7.4 and later (RSAL/SSPL, later AGPL) |
| PostgreSQL | any version (PostgreSQL licence) | |
| Kafka | Apache-2.0 | |
| Memcached | BSD-3-Clause | |
| ClickHouse | Apache-2.0 | |
| Elasticsearch | 7.10 and earlier (Apache-2.0); Lucene (Apache-2.0) | 7.11 and later (SSPL/Elastic licence) |
| RabbitMQ | check each file's header (MPL-2.0 is file-level copyleft; prefer Apache-2.0 client libraries) | |
| MySQL | | server source is GPL: use protocol documentation, MIT/Apache crates (for example `opensrv-mysql`) and client behaviour instead |
| MongoDB | drivers and `bson` (Apache-2.0) | server source is SSPL: use the public wire-protocol docs and driver behaviour |

Behaviour is not copyrightable: reading how a real server responds and writing
your own code that behaves the same is always allowed. What is restricted is
copying source.

## Working agreement

- Branch `svc/<service>` from the latest `main`. Push often; CI runs on
  every push.
- Only touch your service's module (`src/<service>/`), your tests, and the
  shared files where unavoidable: `Cargo.toml` (your feature and deps),
  `src/lib.rs` and `src/services.rs` (Postgres, Kafka and Memcached are
  already scaffolded; other services add their feature, module line and
  registry entry following the same pattern),
  `.github/workflows/ci.yml` (your reference-server container), and your
  section of `COMPATIBILITY.md`.
- Before every commit: `cargo fmt`, `cargo clippy --all-targets
  --all-features -- -D warnings`, `cargo test`, `cargo build
  --no-default-features`, `scripts/check-size.sh`.
- Add your feature to `default` in Cargo.toml once P0 works.
- Keep `docs/LIMITATIONS.md` accurate: every PR updates its service's section
  with what does not work, what is not built yet, and where it differs from the
  real server.
- Open a PR to `main` when a milestone is green in CI. The PR description
  reports: what works, how many results were compared against the real
  server (locally and in CI), binary size and idle RAM impact, known gaps.

## Definition of done for a service

1. All P0 items work and are covered by engine tests, real-client tests and
   differential tests.
2. The client matrix in the spec passes (each listed driver/ORM runs its
   scenario against noida-db), with the test apps committed and runnable from
   one script.
3. CI compares against the real server image named in the spec and passes.
4. `noida-db start` serves the service, `--only <service>` and
   `--<service>-port` work, and the binary stays within budget.
5. The service's section of `COMPATIBILITY.md` states exactly what is and
   isn't supported.
