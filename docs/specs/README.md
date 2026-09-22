# Service specs

Each file here is a self-contained brief for building one noida service. An
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
| ClickHouse | [clickhouse.md](clickhouse.md) | 8123, 9000 | open |

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
- Open a PR to `main` when a milestone is green in CI. The PR description
  reports: what works, how many results were compared against the real
  server (locally and in CI), binary size and idle RAM impact, known gaps.

## Definition of done for a service

1. All P0 items work and are covered by engine tests, real-client tests and
   differential tests.
2. The client matrix in the spec passes (each listed driver/ORM runs its
   scenario against noida), with the test apps committed and runnable from
   one script.
3. CI compares against the real server image named in the spec and passes.
4. `noida start` serves the service, `--only <service>` and
   `--<service>-port` work, and the binary stays within budget.
5. The service's section of `COMPATIBILITY.md` states exactly what is and
   isn't supported.
