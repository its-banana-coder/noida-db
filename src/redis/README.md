# Redis in noida-db

A Redis-compatible server on port **6379** (RESP2 and RESP3), targeting
**Redis 7.2**. Real clients work unchanged (see "Client coverage").

```
noida-db start --only redis            # just Redis
noida-db start --redis-port 6380       # another port
```

Status (from `cargo test --test redis_coverage -- --nocapture`): of Redis
7.2's 242 commands, **217 are implemented**, **22 are out of scope by design**
and **3 are not built yet**. Subcommands are counted separately below.

## What works

| Area | Notes |
|---|---|
| Strings | all, including `LCS`, `GETEX`, `SETRANGE`, `INCRBYFLOAT` (exact x87 `long double` output on x86-64) |
| Keys | `EXPIRE` family with `NX/XX/GT/LT`, `SCAN`, `KEYS`, `RENAME`, `COPY`, `MOVE`, `SWAPDB`, `OBJECT`, `TYPE`, 16 databases |
| Hashes, lists, sets, sorted sets | all commands, with Redis's small-collection encodings and ordering |
| Blocking commands | `BLPOP` family, `BLMOVE`, `BLMPOP`, `BZPOP*`; blocked clients are served first-in-first-out |
| Streams | `XADD`, `XRANGE`, `XREAD`, consumer groups (`XGROUP`, `XREADGROUP`, `XACK`, `XCLAIM`, `XAUTOCLAIM`, `XPENDING`), `XINFO` |
| Geo, bitmaps | all, including `BITFIELD` and `GEOSEARCH` |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE`, byte-identical to Redis (sparse and dense encodings) |
| Sorting | `SORT`, `SORT_RO` with `BY`, `GET`, `LIMIT`, `STORE`, `ALPHA` |
| Pub/sub | channels, patterns, sharded channels, RESP3 push messages |
| Transactions | `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` |
| Scripting | `EVAL`, `EVALSHA`, `SCRIPT` (Lua 5.1, `redis.call`/`pcall`, `cjson`, `cmsgpack`), with Redis's error positions |
| Connection | `HELLO` (RESP3), `CLIENT` (id, name, info, list, kill, pause, reply, no-evict...), `RESET`, `AUTH`. `requirepass` is enforced (`CONFIG SET requirepass x`, or `NOIDA_REDIS_PASSWORD` at startup); there is one user, `default` |
| Debugging | `MONITOR` (a live stream of every command), `COMMAND` (info, docs, getkeys, list), `INFO`, `CONFIG GET/SET` |
| Tool probes | `SLOWLOG`, `LATENCY`, `MEMORY`, `MODULE LIST`, read-only `ACL`. These return empty or estimated data (noida-db does no performance analysis) |

## Not implemented

### By design (out of scope)

noida-db is a local development tool. These commands answer as *unknown command*.

| Commands | Why |
|---|---|
| `DUMP` `RESTORE` `RESTORE-ASKING` `MIGRATE` | RDB payloads and key migration are production tooling |
| `PSYNC` `SYNC` `REPLCONF` `REPLICAOF` `SLAVEOF` `ROLE` `WAIT` `WAITAOF` `FAILOVER` | replication |
| `SENTINEL` | sentinel |
| `CLUSTER` `ASKING` `READONLY` `READWRITE` | clustering |
| `DEBUG` `SHUTDOWN` `PFDEBUG` `PFSELFTEST` | server internals |
| `ACL SETUSER` `DELUSER` `DRYRUN` (and `LOAD`, `SAVE`) | user management: there is one user, `default` |
| `MODULE LOAD` `LOADEX` `UNLOAD` | plugins |
| `SCRIPT DEBUG` | the Lua debugger |

### Not built yet

| What | Note |
|---|---|
| `FUNCTION` (all subcommands), `FCALL`, `FCALL_RO` | Redis Functions. `EVAL`/`EVALSHA` work |
| `CLIENT TRACKING` `CACHING` `GETREDIR` `TRACKINGINFO` | client-side caching |
| Keyspace notifications | `notify-keyspace-events` can be set, but no `__keyspace@*__` / `__keyevent@*__` messages are published, so listening for expired-key events sees nothing |
| Persistence | all data lives in memory and is gone when noida-db stops. `SAVE`, `BGSAVE`, `BGREWRITEAOF` succeed but write nothing; `--data-dir` is unused by Redis |
| `maxmemory` and eviction | the setting is stored, not enforced: no eviction policies, no OOM error |
| Lua libraries `struct`, `bit` | `cjson`, `cmsgpack` and the `redis` table are available |

### Differences

- `SLOWLOG` and `LATENCY` are always empty; `MEMORY USAGE`/`STATS` are
  estimates; `INFO` counters that only matter for performance analysis are 0.
- `INCRBYFLOAT`/`HINCRBYFLOAT` reproduce x86-64 `long double` output. Redis on
  ARM prints the last digits differently.
- Reply order of unordered collections (`KEYS`, `SMEMBERS` on big sets) can
  differ; the real order is an implementation detail.
- `PFCOUNT` on a hand-corrupted HyperLogLog follows 7.2 (it reports the
  corruption); Redis 6.x overran its register array.

### Client coverage

Tested against noida-db, each over RESP2 and RESP3 (`tests/clients/redis/run.sh`,
one command, installs its own dependencies under `target/`): **redis-py** (84
checks), **node-redis** (34 checks), **ioredis** (56 checks), **go-redis** (84
checks), **Jedis** (40 checks) and **Lettuce** (28 checks) for the general
command surface; **BullMQ** (17 checks), **RQ** (14 checks) and **Celery** (5
checks) for job queues built on Lua scripts, sorted sets and streams; plus
`redis-rs` in `tests/redis_client.rs`. Together the checks cover strings,
hashes, lists, sets, sorted sets, `SCAN`, pipelines, `MULTI`/`WATCH`, Lua
scripts, pub/sub, streams and consumer groups, HyperLogLog, `SORT`, geo,
bitmaps, errors, binary data and blocking pops.

Not tried yet: Spring Data Redis, Redisson, Sidekiq (no Ruby toolchain here),
so client-specific gaps may exist there.

## How it is verified

Every command family has three layers of tests, and the first two run against
the real thing:

1. **Engine tests** (`src/redis/tests/`): expected replies and error texts are
   Redis 7.2's, byte for byte.
2. **Comparison tests** (`tests/redis_diff.rs`): the same commands run against
   a real Redis and against noida-db, and the replies must be identical. CI runs
   them against Redis 7.2; locally against any `redis-server` on your PATH,
   skipping lines that need a newer version. Examples: HyperLogLog is compared
   on 511 commands including the raw stored bytes; `MONITOR` output is
   compared line by line.
3. **Real-client tests** (`tests/redis_client.rs`, `tests/clients/redis/`):
   the `redis` crate, redis-py and ioredis over TCP, RESP2 and RESP3.

`tests/redis_coverage.rs` fails if the implemented count ever drops, and
lists out-of-scope commands explicitly.

## Where the code comes from

Behaviour, error texts and algorithms are ported from Redis 7.2's source
(BSD-3-Clause); see `THIRD_PARTY.md`. `meta.rs` (command metadata for
`COMMAND INFO/DOCS`) is generated from Redis's own `commands.def` by
`scripts/gen-redis-commands.py`.
