# What doesn't work

noida is a **local development** tool. This page lists what it does not do,
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
| Postgres | in progress | partly (see `COMPATIBILITY.md`) |
| MySQL | early scaffolding, not merged | no |
| Kafka | early scaffolding, not merged | no |
| Memcached, MongoDB, RabbitMQ, Elasticsearch, ClickHouse | specs only (`docs/specs/`) | no |

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
- Real clustering behaviour of any kind. noida is one process on one machine.

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
- Lua libraries `cmsgpack`, `struct` and `bit`. `cjson` and the `redis` table
  are available.
- Persistence. All data lives in memory and is gone when noida stops.
  `SAVE`, `BGSAVE` and `BGREWRITEAOF` succeed but write nothing. `--data-dir`
  is not used by Redis yet.
- Password protection: `requirepass` is stored but not enforced, and
  `AUTH default <anything>` succeeds.

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

Work in progress; the authoritative list is the Postgres section of
`COMPATIBILITY.md` plus the notes in the Postgres pull requests. Known
directions not covered yet: `PL/pgSQL` and stored procedures, extensions,
logical replication, `LISTEN`/`NOTIFY`, `COPY`, full window-function coverage.
Concurrency is one writer at a time.

## MySQL and Kafka

Early scaffolding only: the real `mysql` CLI cannot run queries, and standard
Kafka clients cannot produce or list topics yet. Do not point applications at
them. The specs in `docs/specs/` describe the target.

## Numbers we do not claim yet

- The footprint targets in `COMPATIBILITY.md` (idle RAM, binary size, "50x
  less memory than the real stack") are **targets**. The only measured figure
  today is the release binary size, checked in CI.
- Data lives in memory for now. The "bounded cache, data on disk" design in
  `COMPATIBILITY.md` is planned, not built.
