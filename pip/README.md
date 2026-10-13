# noida-db

**One tiny binary that speaks Postgres, MySQL, Redis, Kafka and
Elasticsearch**, so your existing drivers, ORMs and CLIs point at it
unchanged. No Docker Compose stack idling at 2.5–4 GB, and no five heavy
servers to boot before your dev server works.

- **~11 MB binary, ~6 MB of RAM idle** with every service running
- **Real wire protocols:** psql, redis-cli, ioredis, kafkajs, Sequelize, Django and 30+ other verified clients just connect
- **Checked against the real databases:** each one's official test suite runs against noida in CI
- **Data is saved per project** (`./.noida-db`), so it survives restarts

## Quick start

```sh
pip install noida-db
noida-db up                    # start every database in the background
noida-db ui                    # interactive panel: toggle services, copy URLs, live activity
```

`noida-db up` prints a ready-to-paste connection string for each service:

| Service | Connect with |
|---|---|
| Postgres | `postgres://postgres@127.0.0.1:5432/postgres` |
| MySQL | `mysql://root@127.0.0.1:3306` |
| Redis | `redis://127.0.0.1:6379` |
| Kafka | `127.0.0.1:9092` |
| Elasticsearch | `http://127.0.0.1:9200` |

## Commands

```sh
noida-db up                    # start in the background
noida-db status                # what's running, with connection strings
noida-db ui                    # the interactive panel
noida-db stop redis            # stop one service (data saved, memory freed)
noida-db start redis           # ...and bring it back
noida-db restart postgres
noida-db reset redis           # wipe one service's data (--all for everything)
noida-db activity -f           # watch queries and commands as they arrive
noida-db down                  # stop everything (data is saved)

noida-db start                 # or run in the foreground; Ctrl-C stops it
noida-db up --only postgres,redis --redis-port 6380
```

The `ui` panel keys are: ↑↓ to move, space to start or stop a service, `c`
to copy its connection string, `r` to reset it, `f` to filter the activity
to one service, `s` to shut down and `q` to quit (the server keeps running).

## How close is it to the real thing?

Each database's own official test suite, run against noida:

| | Suite | noida |
|---|---|---|
| Postgres | Django's database tests | ~99% passing |
| MySQL | Django's database tests | 3,984 (real MySQL 8: 4,331) |
| Redis | the official Redis test suite | 1,013 (real Redis 7.2: 1,990) |
| Elasticsearch | the official REST API tests | 1,735 of 1,786 |
| Kafka | checked against a real Kafka 3.8 broker | |

noida-db is built for local development and tests, not production: it is
single-node and keeps your data in memory, saving it to disk as it changes.
Every known gap is listed in
[LIMITATIONS.md](https://github.com/its-banana-coder/noida-db/blob/main/docs/LIMITATIONS.md).

## Platforms

Prebuilt wheels for Linux (x64, arm64), macOS (Intel, Apple Silicon) and
Windows (x64); pip installs the right one automatically.

Full docs, benchmarks and a GitHub Action for CI:
**https://github.com/its-banana-coder/noida-db**

MIT licensed.
