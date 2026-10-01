# Kafka persistence: noida-db spec

- **Module to edit:** `src/kafka/engine.rs`, `src/kafka/mod.rs`,
  `src/services.rs`
- **New test file:** `tests/kafka_persistence.rs`
- **Branch:** `svc/kafka-persistence` (or similar), off current `main`
- **Status as of 2026-10-01:** `docs/LIMITATIONS.md`'s Kafka section
  already says this explicitly: *"Disk segment persistence (records live
  in-memory)."* This spec is harder than MySQL's (`docs/specs/mysql-persistence.md`)
  — read that one first, since the mechanism (§2 there) is identical; this
  file only covers what's different: Kafka's state has a real mix of
  **durable** data (topics, records, committed offsets) and **ephemeral**
  data (live consumer group membership, in-flight transaction fencing)
  that must **not** be persisted naively, or a restart produces a server
  that's technically "restored" but functionally broken for any client
  that reconnects to it.

## 1. Purpose

Same pitch as MySQL's: a developer restarting `noida-db` today loses every
Kafka topic and every record ever produced. Fix that, using the same
save-on-clean-shutdown mechanism (`src/persistence.rs`) every other
persistent service already uses — **don't** build a WAL, don't build
segment-rotation-aware incremental persistence, don't try to match real
Kafka's on-disk log-segment format. One JSON snapshot, written atomically
on clean shutdown, loaded back on startup. Same deliberate simplicity as
Postgres/Redis/Elasticsearch/MySQL.

## 2. Read `docs/specs/mysql-persistence.md` §2 first

The mechanism section there (`persistence::on_shutdown`,
`persistence::write_snapshot_atomically`, the `spawn_persistent`/
`spawn_persistent_for_test`/`Engine::snapshot`/`Engine::new_persistent`
shape copied from `src/postgres/server.rs`) applies here unchanged. This
spec only covers what's Kafka-specific: **which fields of `EngineState`
belong in the snapshot, which don't, and why** (§3), **a real serde
gotcha with this engine's map types** (§4), and **what has to happen to
in-flight transactions at save time** (§5).

## 3. What persists and what doesn't

`EngineState` (`src/kafka/engine.rs:144-167`) is the whole shared state
behind `Engine`'s `Arc<Mutex<EngineState>>`. Go field by field:

| Field | Persist? | Why |
|---|---|---|
| `topics: HashMap<String, TopicState>` | **Yes** | The actual data — topic configs, partitions, every produced record batch, high watermarks, idempotence sequence state. See §3.1 for what inside `TopicState`/`PartitionState` needs special handling. |
| `broker_id`, `host`, `port`, `cluster_id` | **Yes** | Cluster identity a reconnecting client may have cached from a prior `Metadata` response. Cheap, no reason not to. |
| `next_producer_id` | **Yes** | Keeps producer IDs monotonic across a restart — not load-bearing (no producer from before the restart can still be using an old ID; see §5), but free and matches intuition. |
| `committed_offsets: HashMap<(String, String, i32), i64>` | **Yes** | **The single most important thing to get right.** This is a consumer group's "where was I" — real Kafka persists this (the `__consumer_offsets` topic) specifically so a consumer resuming after a broker restart doesn't reprocess or skip records. Losing this on restart would be a correctness regression, not just a convenience gap. |
| `broker_configs: HashMap<String, String>` | **Yes** | Cluster-level config a `DescribeConfigs` caller might have set via `AlterConfigs`; cheap to keep. |
| `next_member_counter: u64` | **No** | Pure ID-generation counter for consumer-group member IDs. Since no old member survives a restart (§3.2), reusing low numbers again is harmless — reset to 0. |
| `groups: HashMap<String, GroupState>` | **Partially — see §3.2** | The *existence* of a group (so a client's `OffsetFetch`/`OffsetCommit` against a known `group_id` still works) is worth keeping; its *live membership* must not survive. |
| `producer_epochs: HashMap<i64, i16>`, `txn_producers: HashMap<String, (i64, i16)>`, `txn_partitions: HashMap<String, HashSet<(String, i32)>>` | **No — see §5** | Zombie-fencing and in-progress-transaction bookkeeping, meaningful only while the producer connection that owns it is still alive. Every producer connection is gone after a restart by definition. |
| `clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>` | **No (can't be)** | Test-only clock-injection hook (see `Engine::with_clock`), not real state, not `Serialize`. Already correctly excluded from `EngineState`'s own manual `Debug` impl (`src/kafka/engine.rs:169-182` — notice it's one of the three fields that `Debug` skips) for exactly this "not really state" reason. |

### 3.1 Inside `TopicState`/`PartitionState`

`TopicState` (`src/kafka/engine.rs:68-74`): `name`, `is_internal`,
`configs` are plain and persist trivially. `partitions:
HashMap<i32, PartitionState>` — `i32` keys serialize fine with
`serde_json` (see §4 for exactly where the line is), so no special
handling needed for this one map.

`PartitionState` (`src/kafka/engine.rs:29-52`):

- `id`, `leader`, `high_watermark` — persist as plain fields. Keep
  `high_watermark` as a stored field rather than recomputing it from
  `record_batches` on load (recomputing would mean decoding every
  batch's record count on every startup for no benefit — it's already
  kept consistent by every code path that appends a batch today).
- `record_batches: Vec<(i64, Vec<u8>)>` — persist as-is. This is a `Vec`,
  not a map, and both tuple members (`i64`, `Vec<u8>`) are ordinary
  serializable types — no gotcha here. This is the actual record data;
  getting this right is the entire point of the feature.
- `producer_seqs: HashMap<(i64, i16), (i32, i64)>` — **persist**, but
  needs the §4 workaround (tuple key). This is the idempotent-producer
  last-sequence-per-epoch state; real Kafka persists the equivalent
  (producer-state snapshot files) for the same reason — a long-lived
  idempotent producer that keeps the *same* `producer_id`/epoch across a
  broker restart (plausible: `InitProducerId` is typically called once
  per producer instance, not per reconnect) must not get spurious
  `OutOfOrderSequenceException`s on its next send.
- `active_txns: HashMap<i64, i64>` — **don't persist as-is**; resolved to
  empty at save time (§5).
- `aborted_txns: Vec<(i64, i64)>` — **persist as-is** (it's a `Vec`, no
  gotcha). A `read_committed` fetcher must keep skipping these offsets
  after a restart too, since this engine has no log-retention/segment
  deletion that would ever naturally age them out (see the existing
  ClickHouse/Kafka "no retention" non-goals already documented
  elsewhere) — if you drop `aborted_txns` on save, a `read_committed`
  consumer would incorrectly start seeing an old aborted transaction's
  records after any restart.

### 3.2 `GroupState`: keep the group, drop the membership

A consumer group's *committed offsets* must survive (§3, table). Its
*live membership* must not: `members`, `pending_member_ids`,
`awaiting_members`, `assignments`, `leader_id`, `rebalance_start_ms`,
`generation_id` are all tied to specific open TCP connections
(`GroupMember.last_heartbeat_ms` is checked against *this process's*
clock; `member_id`s are meaningless once the owning connection is gone)
that cease to exist the moment the process restarts. A real Kafka client
library handles a broker-perceived "I've never heard of this member"
response transparently — it's exactly what `UNKNOWN_MEMBER_ID` from
`JoinGroup`/`SyncGroup`/`Heartbeat` already means and every real consumer
already handles it by rejoining from scratch. **Don't try to preserve
membership across the restart — let the normal rejoin path handle it,
the same way it already handles a consumer's TCP connection dropping and
reconnecting today.**

Concretely: persist only `group_id` and `committed_offsets` is enough —
`committed_offsets` isn't even inside `GroupState`, it's its own top-level
`EngineState` field keyed by `(group_id, topic, partition)`, so you don't
actually need to persist `GroupState` itself at all if nothing reads a
group's existence other than via `committed_offsets`/an explicit
`JoinGroup`. **Check this against the real code before deciding**: grep
`src/kafka/engine.rs` for every place that does `self.groups.get(group_id)`
or similar and confirm nothing besides `handle_offset_commit`/
`handle_offset_fetch` needs a `GroupState` to exist ahead of a `JoinGroup`
call. If something does (e.g. `DescribeGroups` on a group with committed
offsets but no live members, which real Kafka does support and report as
state `Dead` or `Empty`), persist a `GroupState` with `state:
GroupLifecycleState::Empty` and every membership-shaped field reset to
empty/default — never a loaded `Stable`/`PreparingRebalance`/
`CompletingRebalance` state with phantom members in it.

## 4. The serde gotcha: tuple-keyed `HashMap`s don't serialize with `serde_json`

This is the one real landmine in this spec, worth its own section because
it fails at **runtime**, not at compile time, and the failure
(`serde_json::Error: key must be a string`) is not obvious from the error
message alone if you haven't hit it before.

`serde_json` can serialize a `HashMap<K, V>` as a JSON object only when
`K`'s own `Serialize` impl calls one of the serializer's "primitive" key
methods (a string, or a number — numbers get stringified automatically,
e.g. `HashMap<i64, V>` becomes `{"123": ...}` and deserializes back fine).
**A tuple key does not qualify** — serializing a tuple calls
`serialize_seq`, and `serde_json`'s map-key serializer rejects that with
a runtime error. This engine has exactly two fields with a genuine tuple
key that need to be in the snapshot:

- `PartitionState::producer_seqs: HashMap<(i64, i16), (i32, i64)>`
- `EngineState::committed_offsets: HashMap<(String, String, i32), i64>`

(`EngineState::txn_partitions: HashMap<String, HashSet<(String, i32)>>`
has a tuple *inside a `HashSet` value*, not as a map key — that's fine,
a `HashSet` serializes as a plain JSON array — but this field isn't
persisted at all per §3/§5, so it's moot either way.)

**Fix: don't derive `Serialize`/`Deserialize` directly on `EngineState`/
`PartitionState`. Write separate snapshot DTO structs** (mirroring how
Postgres's `Snapshot`/`SnapshotDb` (`src/postgres/engine.rs:120-132`)
are already a distinct on-disk shape from the live `Global`/`GlobalDb`,
not a derive on the live types themselves) that represent every
tuple-keyed map as `Vec<(K, V)>` instead:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub id: i32,
    pub leader: i32,
    pub record_batches: Vec<(i64, Vec<u8>)>,
    pub high_watermark: i64,
    pub producer_seqs: Vec<((i64, i16), (i32, i64))>, // was HashMap
    pub aborted_txns: Vec<(i64, i64)>,
    // active_txns intentionally absent -- see §5.
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicSnapshot {
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<(i32, PartitionSnapshot)>, // HashMap<i32,_> would
                                                     // actually be fine per
                                                     // the rule above, but
                                                     // using Vec here too
                                                     // keeps the DTO layer
                                                     // uniform and avoids
                                                     // relying on a subtle
                                                     // serde_json behavior
                                                     // a future reader
                                                     // might not know about.
    pub configs: HashMap<String, String>, // String keys: fine as-is.
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub topics: Vec<(String, TopicSnapshot)>,
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub cluster_id: String,
    pub next_producer_id: i64,
    pub committed_offsets: Vec<((String, String, i32), i64)>, // was HashMap
    pub broker_configs: HashMap<String, String>, // String keys: fine as-is.
}
```

`EngineState::to_snapshot(&self) -> Snapshot` and
`EngineState::from_snapshot(s: Snapshot) -> EngineState` (or free
functions, match whatever style `Engine::snapshot`/`new_persistent` ends
up calling — see §6) do the `HashMap` ⇄ `Vec<(K, V)>` conversion
explicitly with `.into_iter().collect()` both directions. Write a small
unit test that round-trips a `producer_seqs` map with at least two
entries through `to_snapshot` → `serde_json::to_vec` →
`serde_json::from_slice` → `from_snapshot` and asserts the map compares
equal, specifically to catch a future regression if someone "simplifies"
this back to a direct derive on the live types.

## 5. In-flight transactions: resolve, don't persist

§3's table excludes `active_txns`, `producer_epochs`, `txn_producers`,
`txn_partitions` from the snapshot. The reasoning: every one of these
exists to answer "is this producer connection still the legitimate owner
of this transaction/epoch," and **every producer connection is gone**
the instant the process restarts — there is no live TCP connection left
to be a zombie *or* legitimate about. Trying to round-trip fencing state
for connections that can never reconnect as themselves (a reconnecting
client always calls `InitProducerId` fresh and gets a new producer ID/
epoch) is work spent on a scenario that can't occur.

But a **still-open transaction** (one with an `active_txns` entry on some
partition) at the moment of a clean shutdown needs to be dealt with, not
just dropped — the partition's `record_batches` already physically
contain that transaction's not-yet-decided records, and a
`read_committed` fetcher needs to know whether to show them. **Resolve
every still-open transaction as aborted at save time**, inside the save
closure, before serializing:

1. For each `TopicState` → `PartitionState` with a non-empty
   `active_txns`: for each `(producer_id, first_offset)` entry, do what
   `EndTxn`'s existing abort path already does — append an abort control
   batch (`encode_control_batch(producer_id, epoch, high_watermark,
   /* committed = */ false, now_ms)`, same helper already at
   `src/kafka/engine.rs:189-229`; use the epoch from `producer_epochs` if
   still present, else `0`) to `record_batches`, bump `high_watermark`,
   move `(producer_id, first_offset)` into `aborted_txns`.
2. Clear `active_txns` on every partition.
3. *Then* build the `Snapshot` DTO from the now-fully-resolved state.

This matches the spirit of real Kafka's own crash-recovery behavior
reasonably closely (an in-doubt transaction gets resolved one way or the
other during recovery, not left open forever) without needing to
replicate the real transaction-coordinator protocol. Write this as a real
method (e.g. `EngineState::resolve_open_transactions_for_shutdown(&mut
self)`) rather than inlining it into the save closure, so the dedicated
test in §7.3 can call it directly without going through a real TCP
shutdown.

**Do not do this resolution on every ordinary save-closure invocation
speculatively "just in case"** — only the real shutdown path needs it,
since it mutates state (appends a real control batch, changes
`high_watermark`) and the plain engine-level snapshot round-trip test in
§7.2 should be free to exercise `to_snapshot`/`from_snapshot` without
tripping over transaction-abort side effects it didn't ask for.

## 6. Engine and server changes

Same shape as MySQL's §5.1/§5.2, adapted:

- `src/kafka/engine.rs`: add `Engine::snapshot(&self) -> Snapshot` (locks,
  calls `resolve_open_transactions_for_shutdown` on a cloned/locked
  `EngineState`... actually **do the resolution in place on the real
  locked state**, not a clone — the abort control batches it appends are
  real data a subsequent `Fetch` should also see even if the process
  doesn't actually exit right after (matches real behavior: the save
  closure runs synchronously inside the shutdown handler before
  `std::process::exit`, so there's no observable window where this
  matters in practice, but doing it on a clone would make the two
  diverge for no reason) — then calls `to_snapshot()`) and
  `Engine::new_persistent(snapshot: Snapshot) -> Engine` (calls
  `EngineState::from_snapshot`, wraps in `Arc::new(Mutex::new(...))`,
  same as `Engine::new` does today but starting from loaded data instead
  of `EngineState::new(host, port)`).
- `src/kafka/mod.rs`: add `spawn_persistent`/`spawn_persistent_for_test`,
  mirroring `pub fn spawn` (`src/kafka/mod.rs:16-31`) — load
  `data_dir.join("kafka.json")` if present, else build fresh via
  `Engine::new(host, port)` exactly like `spawn` already does.
- `src/services.rs`: add
  `#[cfg(feature = "kafka")] "kafka" => Some(crate::kafka::spawn_persistent(addr, data_dir)),`
  to `start_persistent` (`src/services.rs:16-29`).

## 7. Test plan

### 7.1 `tests/kafka_persistence.rs` (new)

Mirror `tests/redis_persistence.rs`'s structure (temp dir keyed by
`std::process::id()`, `spawn_persistent_for_test` returning `(addr, save)`,
call `save()` directly rather than a real signal, start a second server
against the same dir). Use the existing raw-bytes test helpers already
built for Kafka's own concurrency test
(`partition_for_key()`/`produce_length_prefixed()`/
`split_length_prefixed()` in `tests/kafka_client.rs`) rather than
reinventing a client — or use `kafkajs`/a real client under
`tests/clients/` if one already exists for Kafka; check before choosing.
Cover:

- Create a topic with 2+ partitions, produce several records across both
  partitions, restart, fetch from offset 0 on each partition and confirm
  every record (key, value, offset) matches exactly.
- Commit consumer group offsets for a `group_id`, restart, `OffsetFetch`
  the same `group_id`/topic/partition and confirm the committed offset
  survived — **this is the one assertion that matters most**; a bug here
  means every real consumer app that restarts noida-db loses its place
  and either reprocesses everything or (worse) a buggy off-by-one skips
  records.
- After restart, have a **new** consumer (a fresh `JoinGroup` against the
  same `group_id` that had committed offsets before the restart) join and
  fetch starting from that committed offset — confirms §3.2's "drop
  membership, keep offsets, let normal rejoin handle the rest" design
  actually works end-to-end through the real protocol path, not just at
  the data-structure level.
- Start a transactional produce (`InitProducerId` → `Produce` with
  `transactional_id` set → do **not** call `EndTxn`), trigger `save()`
  directly, restart, and confirm (a) a `read_committed` fetch does not
  see those records (the abort-at-save-time path from §5 worked) and (b)
  a `read_uncommitted` fetch does see them (they're still physically in
  `record_batches`, same as any aborted transaction's records today).
- An idempotent (non-transactional) producer: `InitProducerId`, produce a
  few sequenced records, restart, produce one more record **reusing the
  same `producer_id`/epoch** with the next expected sequence number, and
  confirm it's accepted (not `OutOfOrderSequenceException`) — the
  `producer_seqs` round-trip from §4 actually matters for this case.

### 7.2 Engine-level snapshot round-trip test (in
`src/kafka/engine.rs`'s own test module)

Fast, no network: build an `EngineState` with a couple of topics/
partitions/records/`producer_seqs`/`committed_offsets` directly, call
`to_snapshot()` → `serde_json::to_vec` → `serde_json::from_slice` →
`from_snapshot()`, assert every field matches. This is the right place
for the "two-entry `producer_seqs` map round-trips" regression test
mentioned at the end of §4.

### 7.3 `resolve_open_transactions_for_shutdown` unit test

Directly exercise §5's method: open a transaction (populate
`active_txns`), call it, assert `active_txns` is empty, `aborted_txns`
gained the entry, `record_batches` gained one more batch (the abort
control batch), and `high_watermark` advanced. Separately confirm calling
it when nothing is open is a no-op (doesn't append a spurious control
batch to every partition on every save).

### 7.4 Don't regress the existing suite

`cargo test --features kafka` must stay green, in particular
`tests/kafka_client.rs`'s `test_kafka_per_key_ordering_under_concurrent_production`
(the real-concurrency test from this project's Kafka exercise batch) and
`tests/kafka_diff.rs`. None use `spawn_persistent` today, but the engine
internals this spec touches (`EngineState`, `PartitionState`) are shared
by every one of them.

## 8. Docs to update in the same PR

- `docs/LIMITATIONS.md`: Kafka's "Not yet" list currently has exactly one
  line: *"Disk segment persistence (records live in-memory)."* Replace it
  with a paragraph matching the style of Postgres/Redis/Elasticsearch's
  own persistence paragraphs (point at `src/persistence.rs`, name the
  snapshot file `kafka.json`), and **explicitly document §3.2's
  membership-reset behavior** as a "Differs" bullet (real Kafka can, in
  some configurations, preserve group membership across a *graceful*
  controlled shutdown via static group membership /
  `group.instance.id`; this engine always resets to empty — say so, since
  a sufficiently observant client test could otherwise read this as a
  bug rather than a documented simplification).
- `README.md`: same "At a glance" table edit `docs/specs/mysql-persistence.md`
  §7 describes for MySQL — once both this and the MySQL spec have landed,
  the row becomes *"On-disk persistence for Postgres, Redis, Elasticsearch,
  MySQL, Kafka — survives a clean restart"* with no "next" clause left.
  Do **not** make this edit until both specs are actually done; if only
  one lands first, update the row to name that one service and leave the
  other in the "next" clause (matching the exact wording pattern already
  used today).

## 9. Explicitly out of scope

- **Real log-segment persistence, retention, compaction.** This is a
  single JSON snapshot of in-memory state, not Kafka's actual on-disk
  format. Already true of every other persisted service here (Postgres
  doesn't write real WAL/heap files either) — match that precedent.
- **Multi-broker / replication-aware snapshotting.** Single-broker only,
  same as the rest of this engine (`src/kafka/mod.rs`'s own single
  `Engine`).
- **Faithfully preserving producer-fencing/transaction-coordinator state
  across a restart.** §5 covers why this doesn't matter in practice for a
  dev tool where no producer connection survives the restart anyway —
  don't build a more faithful version of this.
- **Performance.** Same note as `docs/specs/mysql-persistence.md` §8:
  `Engine::snapshot()` clones/serializes the whole in-memory log on every
  save. Fine at dev-data scale; don't optimize it.
