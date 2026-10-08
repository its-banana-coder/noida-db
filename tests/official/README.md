# Official test suites

Each service's own upstream test suite, run unmodified against noida-db.
The pass rate is the compatibility number we publish, because anyone can
check it: same tests, same expected output, same runner against the real
server for calibration.

| Service | Suite | Runner |
|---|---|---|
| Elasticsearch | `rest-api-spec` YAML REST tests, v8.15.3 (the suite every official client runs) | `elasticsearch/run.py` |
| Postgres | `src/test/regress` (pg_regress), REL_14_STABLE | `postgres/run.py` |
| Redis | `tests/` (TCL), 7.2, external-server mode | `redis/run.sh` |

## Running

```sh
tests/official/elasticsearch/fetch.sh
python3 tests/official/elasticsearch/run.py http://127.0.0.1:9200 --json es.json

tests/official/postgres/fetch.sh
python3 tests/official/postgres/run.py 5432 --json pg.json

tests/official/redis/run.sh 6379
```

## Reading the numbers

- **Calibration first.** Each runner is run against the real server too.
  Whatever the real server doesn't pass in this environment (Postgres's C
  test library isn't built, locales, ES features needing a capabilities
  API) is the ceiling, not a noida-db gap.
- **Elasticsearch**: tests a runner may decline (capabilities API, runner
  features it doesn't implement, `awaits_fix`) are skipped for every
  server alike; the score is passed / run.
- **Postgres**: scored by statement (a statement passes when its psql output
  block is identical to the expected file's), plus whole files identical
  and matching lines. Server-side `COPY ... FROM 'file'` is sent as
  psql's `\copy` for every server.
- **Redis**: the suite's own `[ok]`/`[err]` counts; tests tagged
  `external:skip` are skipped by Redis's runner itself. Each file runs on
  its own (`--single`) so one exception only costs its file.
  `unit/moduleapi/*` is not run: it loads Redis modules built from C
  (`MODULE LOAD`), which only real Redis can do. Calibrate against a real
  Redis started with `--enable-debug-command yes`.

## Results so far

| Suite | Real server (ceiling here) | noida-db | Date |
|---|---|---|---|
| Postgres `pg_regress` (REL_14_STABLE, 216 files, ~40k statements) | PostgreSQL 14: 99.3% of statements, 179/216 files identical | 44.9% of statements, no crashes (before #109/#113) | 2026-10-06 |
| Elasticsearch YAML REST tests (v8.15.3, all 122 dirs, 1708 tests run) | Elasticsearch 8.15.3: 90.4% on a subset (full calibration pending) | 675 / 1708 (39.5%), up from 598 (35.0%) on 2026-10-07 and 165 (9.6%) before #123/#125/#130/#140/#147 | 2026-10-08 |
| Redis TCL suite (7.2.12, external mode, without `unit/moduleapi`) | Redis 7.2.12 (`--enable-debug-command yes`): ok 1990, err 1, exception 1 (`unit/replybufsize`, `unit/type/set`: need the server pid / DEBUG internals) | not run yet | 2026-10-07 |

What the real server misses here is environmental (Postgres's C test
library isn't built; ES features needing a capabilities API) and is the
ceiling for noida-db, not a gap.
