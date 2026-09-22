# Memcached: noida spec

- **Module:** `src/memcached/` (stub exists), Cargo feature `memcached`,
  branch `svc/memcached`
- **Port:** 11211 (TCP). UDP is a non-goal.
- **Target:** memcached 1.6.x (latest 1.6 behaviour), version string
  `1.6.29` reported by `version` (keep it configurable)
- **Reference server:** not installed locally (no Docker); if a `memcached`
  binary is on PATH, use it. CI uses the `memcached:1.6` image. Env var:
  `NOIDA_MEMCACHED_REF=host:port`.

## 1. Purpose

Apps and frameworks using memcached (PHP/Laravel, Django cache, Rails
`dalli`, Java spymemcached/xmemcached, Node memjs, Go gomemcache, Python
pymemcache/pylibmc) work unchanged. Memcached's keyspace is separate from
Redis's.

## 2. Text protocol (P0)

Line-based, `\r\n` terminated. Every command, response line and error must
be byte-identical to memcached 1.6.

| Command | Details |
|---|---|
| `set/add/replace/append/prepend <key> <flags> <exptime> <bytes> [noreply]` + data block | `STORED`, `NOT_STORED`; flags are 32-bit unsigned, echoed on get |
| `cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]` | `STORED`, `EXISTS`, `NOT_FOUND` |
| `get <key>*` / `gets <key>*` | `VALUE <key> <flags> <bytes> [<cas>]` blocks then `END` |
| `gat/gats <exptime> <key>*` | get and touch |
| `delete <key> [noreply]` | `DELETED`, `NOT_FOUND` (legacy `delete <key> 0` accepted, other time values → `CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]`) |
| `incr/decr <key> <value> [noreply]` | 64-bit unsigned; incr wraps at 2^64, decr floors at 0; non-numeric value → `CLIENT_ERROR cannot increment or decrement non-numeric value`; bad delta → `CLIENT_ERROR invalid numeric delta argument`; result length rules as memcached (value is rewritten in place) |
| `touch <key> <exptime> [noreply]` | `TOUCHED`, `NOT_FOUND` |
| `flush_all [delay] [noreply]` | `OK`; delay invalidates items at that time |
| `version`, `verbosity <n> [noreply]`, `quit` | |
| `stats [args]` | see section 5 |
| `cache_memlimit <MB> [noreply]`, `shutdown` (disabled → `ERROR: shutdown not enabled`), `misbehave`, `lru_crawler`/`slabs`/`watch` | reply as memcached does with default settings |

Rules clients depend on:
- **Expiry:** 0 = never; ≤ 2592000 (30 days) = relative seconds; larger =
  absolute unix time; negative = immediately expired. Use the injected
  clock (see Redis engine) so tests control time.
- **Keys:** ≤ 250 bytes, no spaces or control characters, otherwise
  `CLIENT_ERROR bad command line format`.
- **Item size:** default max 1MB (`-I 1m`); larger →
  `SERVER_ERROR object too large for cache`, and the data block is
  swallowed exactly as memcached does.
- **Errors:** unknown command → `ERROR`; malformed → `CLIENT_ERROR <msg>`;
  bad data chunk (wrong length / missing `\r\n`) → `CLIENT_ERROR bad data
  chunk` with memcached's recovery behaviour.
- **noreply** suppresses the reply for every storage/delete/incr/touch/flush
  command, including error replies where memcached suppresses them.
- **CAS unique:** a global 64-bit counter incremented on every
  modification, as memcached does.
- Pipelining: many commands in one packet are answered in order.

## 3. Meta protocol (P0 for modern clients, else P1)

`mg`, `ms`, `md`, `ma`, `mn`, `me` with their flags as documented in
memcached's `protocol.txt` for 1.6: return flags (`v` value, `f` client
flags, `c` cas, `t` ttl, `s` size, `k` key, `O` opaque, `q` noreply
semantics, `h` hit-before, `l` last access), `b` base64 keys, `T` ttl
update, `N` vivify on miss, `R` win/recache, stale items (`I` invalidate,
`X`/`W`/`Z` flags), `ms` modes (`MS` set, `ME` add, `MA` append, `MP`
prepend, `MR` replace, invalid mode → `CLIENT_ERROR invalid mode for ms`),
`ma` modes (incr/decr with `N` auto-create, `J` initial value, `D` delta).
Response codes `HD`, `VA`, `EN`, `NF`, `NS`, `EX`, `MN`, `ME`. Match the
exact flag echo order and error strings from memcached.

## 4. Binary protocol (P1)

Magic 0x80/0x81, 24-byte header. All opcodes: Get, Set, Add, Replace,
Delete, Increment, Decrement, Quit, Flush, GetQ, No-op, Version, GetK, GetKQ,
Append, Prepend, Stat, SetQ, AddQ, ReplaceQ, DeleteQ, IncrementQ,
DecrementQ, QuitQ, FlushQ, AppendQ, PrependQ, Touch, GAT, GATQ, GATK,
GATKQ. Quiet variants suppress success responses. Status codes (0x0001 key
not found, 0x0002 key exists, 0x0003 value too large, 0x0004 invalid
arguments, 0x0005 item not stored, 0x0006 non-numeric value, 0x0081 unknown
command…) and their message bodies as memcached sends them. SASL list/auth
opcodes reply as a server without SASL enabled.

## 5. stats (P0, minimal but well-formed)

`stats` returns memcached's field names in memcached's order (`pid`,
`uptime`, `time`, `version`, `libevent`, `pointer_size`, `rusage_*`,
`max_connections`, `curr_connections`, `total_connections`, `cmd_get`,
`cmd_set`, `get_hits`, `get_misses`, `curr_items`, `total_items`, `bytes`,
`limit_maxbytes`, `threads`, `evictions`…). Counters clients display
(items, bytes, connections, hits/misses, uptime) should be real; tuning
internals can be 0. Also `stats settings`, `stats items`, `stats slabs`
(one plausible slab class set), `stats conns`, `stats reset`. No other
performance analysis.

## 6. Storage

In-memory hash map of key → (value, flags, exptime, cas, last access).
Lazy expiry on access plus a periodic sweep. Eviction (LRU) only when the
item memory limit (`-m`, default 64MB) is reached; count it in
`evictions`. Keep it simple.

## 7. Client matrix

Scenario: set/get with flags and TTL (expiry observed), add/replace
semantics, cas conflict, incr/decr, delete, multi-get, a large value near
1MB and one above it, flush_all.

| Client | Protocol | How to run |
|---|---|---|
| Rust `memcache` crate | text (+binary if supported) | dev-dependency in `tests/memcached_client.rs` |
| pymemcache | text + meta | pip |
| memjs (Node) | binary | npm |
| spymemcached / xmemcached (Java) | text/binary | jars from Maven Central, `javac` |
| PHP memcached / Django cache / dalli | P2, CI |

Commit test apps under `tests/clients/memcached/` with a runner script.

## 8. Differential tests

`tests/memcached_diff.rs` sends raw protocol scripts (text, meta and binary
as byte strings) to real memcached and noida and compares responses byte
for byte, ignoring only values that legitimately differ (`version`, `stats`
numbers, CAS values, which are compared for relative behaviour instead).
Cover every command above including error and noreply paths. Print the
number of compared responses. CI uses the `memcached:1.6` service.

## 9. Non-goals

UDP, SASL, TLS, extstore, proxy mode, replication, `lru_crawler metadump`
contents.

## 10. Milestones

1. Text protocol storage/retrieval commands, expiry, errors; diff suite for
   them; Rust and Python clients pass.
2. Meta protocol; pymemcache meta mode passes.
3. Binary protocol; memjs and Java clients pass.
4. stats family; `memcached` in default features.
