# MySQL persistence: noida-db spec

- **Module to edit:** `src/mysql/engine.rs`, `src/mysql/catalog.rs`,
  `src/mysql/server.rs`, `src/mysql/mod.rs`, `src/services.rs`
- **New test file:** `tests/mysql_persistence.rs`
- **Branch:** `svc/mysql-persistence` (or similar), off current `main`
- **Status as of 2026-10-01:** MySQL itself (DDL/DML, transactions, joins,
  `GROUP BY`/`HAVING`, prepared statements, `AUTO_INCREMENT`) is done and
  merged. This is the one remaining gap for MySQL versus Postgres/Redis/
  Elasticsearch, which already persist. `docs/LIMITATIONS.md` doesn't even
  mention it today because it predates this being tracked as a real gap —
  add a line there as part of this work (see §7).

## 1. Purpose

Right now, stopping and restarting `noida-db` loses every MySQL table and
row. For a tool whose whole pitch is "point your app at this instead of a
real database," that's a real gap: a developer expects their dev data to
survive a restart the same way Postgres/Redis/Elasticsearch already do here.
This spec adds the same on-disk snapshot persistence MySQL's three sibling
services already have, using the exact same mechanism — don't invent a new
one.

## 2. The existing mechanism (read this first, copy it)

`src/persistence.rs` is the shared, already-built infrastructure every
persistent service uses:

- `persistence::on_shutdown(hook)` registers a `Fn() + Send + Sync`
  closure, run once (in registration order) when the process catches
  SIGINT/SIGTERM.
- `persistence::install_shutdown_handler()` (called once from `main.rs`)
  installs that signal handler and runs every registered hook before
  `std::process::exit(0)`.
- `persistence::write_snapshot_atomically(path, bytes)` writes to a sibling
  `.tmp` file and renames it into place, so a hard kill mid-save can never
  leave a corrupt snapshot — only ever the old file or the fully-written
  new one.

This is deliberately **save-on-clean-shutdown only**, no periodic
autosave, no WAL. A `kill -9` or crash loses whatever changed since the
last clean shutdown, but can never corrupt the snapshot file. Match this —
do not build anything fancier for MySQL.

### 2.1 The pattern to copy: `src/postgres/server.rs`

Postgres is the closest reference (multi-session, SQL engine, same shape
as MySQL's own `db: Arc<Mutex<DbState>>`). Read
`src/postgres/server.rs:67-123` and `src/postgres/engine.rs:118-231` in
full before starting. The shape:

1. `Engine` holds its real state behind `Arc<Mutex<Global>>` (Postgres) /
   `Arc<Mutex<DbState>>` (MySQL, already the case — see §3).
2. A `Snapshot` struct (`#[derive(Serialize, Deserialize)]`) is the
   on-disk shape — not necessarily identical to the in-memory struct, but
   for MySQL it can be, since `DbState` has no non-serializable fields
   once §4 below is done.
3. `Engine::snapshot(&self) -> Snapshot` locks the mutex, clones out a
   `Snapshot`.
4. `Engine::new_persistent(snapshot: Snapshot) -> Engine` is a second
   constructor (alongside `Engine::new()`) that rebuilds the live state
   from a loaded snapshot.
5. `spawn_persistent_with_for_test(addr, data_dir, cfg) -> io::Result<(SocketAddr, impl Fn() + Send + Sync + 'static)>`:
   - Builds `path = data_dir.join("<service>.json")`.
   - If `path.exists()`, reads it, `serde_json::from_slice` into the
     snapshot type (mapping a parse error to `io::ErrorKind::InvalidData`
     with a message naming the service), builds the engine via
     `new_persistent`.
   - Else, `Engine::new()`.
   - Builds a `save` closure that clones the engine's `Arc`, takes a fresh
     `snapshot()`, serializes with `serde_json::to_vec`, and calls
     `write_snapshot_atomically`. Returns `(addr, save)` **without**
     registering it — this variant exists so tests can call `save()`
     directly instead of going through the process-wide signal hook
     (unsafe to trigger from a single test once multiple services'
     persistence tests share one test binary).
   - Starts the listener thread exactly like the ordinary `spawn`.
6. `spawn_persistent(addr, data_dir) -> io::Result<SocketAddr>` is the real
   entry point: calls the `_for_test` variant, then
   `persistence::on_shutdown(save)`, returns just the address.

MySQL copies this shape with no structural changes needed — the only real
work is making `DbState` (and everything it contains) serializable (§4)
and writing the MySQL-specific `spawn_persistent*` functions (§5).

## 3. What MySQL already has going for it

Two things make this easier than it looks:

- **MySQL's shared state is already a single `Arc<Mutex<DbState>>`**
  (`src/mysql/engine.rs:14`), already cloned once per connection via
  `Engine`'s `#[derive(Clone)]` the same way Postgres shares its `Global`
  — see the comment at `src/mysql/server.rs:16-28` (a real WordPress bug
  that happened when this wasn't shared correctly; don't regress it).
  There's no separate "global vs session" split to design — `db` is the
  only field that needs to survive a restart; every other `Engine` field
  (`current_db`, `last_insert_id`, `last_column_names`, `last_found_rows`,
  `tx_snapshot`) is legitimately per-connection/per-statement and must
  **not** be in the snapshot (see §4.4).
- **`Value`'s two exotic variants are already `Serialize`/`Deserialize`.**
  `src/mysql/types.rs`'s `Value::Num(Numeric)` and `Value::Json(Box<Json>)`
  wrap `crate::sql::numeric::Numeric` and `crate::sql::json::Json`
  (`src/sql/numeric.rs`, `src/sql/json.rs`) — both already derive
  `Serialize, Deserialize`, because Postgres's own persistence already
  needs them. Deriving `Value` itself is therefore mechanical, not new
  work (§4.1).

## 4. Make the catalog serializable

All of `src/mysql/catalog.rs` and `src/mysql/types.rs::Value` currently
derive only `Clone, Debug[, PartialEq]`. None of this is a redesign —
every field is already a plain, serializable shape. Work through it
bottom-up:

### 4.1 `src/mysql/types.rs`

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Num(Numeric),
    Text(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    Ts(i64),
    Json(Box<Json>),
}
```

Add `use serde::{Deserialize, Serialize};` at the top. No variant needs a
custom (de)serializer — every one is already a plain Rust primitive or an
already-`Serialize`/`Deserialize` type. (`f64` in `Value::Float`: see §4.5
for the one real edge case this introduces.)

### 4.2 `src/mysql/catalog.rs`

Add the same derive to every type that doesn't already have it:

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnType { Int, BigInt, Varchar(usize), Text, Float, Double, Decimal(u8, u8), Date, Datetime, Boolean }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column { pub name: String, pub ty: ColumnType, pub not_null: bool, pub default: Option<Value>, pub auto_increment: bool, pub primary_key: bool }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table { pub name: String, pub columns: Vec<Column>, pub rows: Vec<Row>, pub next_auto_increment: i64 }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Schema { pub name: String, pub tables: BTreeMap<String, Arc<Table>> }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbState { pub schemas: BTreeMap<String, Schema> }
```

`Schema.tables` is `BTreeMap<String, Arc<Table>>` — serde's `Arc<T>`
support needs the `rc` cargo feature. **Check first**: `Cargo.toml` already
has `serde = { version = "1", features = ["derive", "rc"] }` (added for
Postgres), so this needs no `Cargo.toml` change. Confirm it's still there
before assuming so.

### 4.3 `DbState::default()`'s five built-in schemas

`DbState::default()` (`src/mysql/catalog.rs:56-67`) seeds
`information_schema`/`mysql`/`performance_schema`/`sys`/`test` as empty
schemas. `Engine::new_persistent` must **not** call `DbState::default()`
and then overlay the loaded snapshot — it uses the loaded `DbState`
exactly as deserialized. Decide (and write a test for, §6) what happens if
an old snapshot predates a built-in schema being added in some future
noida-db version: for now, since all five already exist on day one, this
isn't yet a real migration concern — just don't silently drop user tables
in schemas not in that fixed list if that ever happens.

### 4.4 What's explicitly *not* in the snapshot

The on-disk shape is `DbState` alone, not `Engine`. Do **not** snapshot:

- `current_db` — which schema a connection has `USE`d is a per-session
  fact; a reconnecting client always starts without a selected database,
  exactly like real MySQL after a restart.
- `last_affected_rows`, `last_insert_id`, `last_column_names`,
  `last_found_rows` — all are scoped to "the most recent statement on this
  connection," meaningless after a restart (no connections survive a
  restart at all).
- `tx_snapshot` — mid-transaction state. A snapshot can only ever be taken
  between statements server-side today (the shutdown hook fires
  independent of any open connection's transaction state); if a save
  somehow raced an open, uncommitted transaction on another thread, the
  saved `DbState` would already reflect that connection's own
  partially-applied writes (the mutex only protects per-statement
  atomicity, not transaction atomicity — this is a pre-existing property
  of the engine, not something this spec needs to fix). Out of scope here;
  just don't try to snapshot `tx_snapshot` itself, since a `ROLLBACK` after
  a restart makes no sense (the connection that opened the transaction is
  gone).

### 4.5 Known lossy edge case: non-finite floats

`serde_json` represents `f64::NAN`/`INFINITY`/`NEG_INFINITY` as JSON `null`
on serialize, which deserializes back as `0.0`, not the original value —
this is `serde_json`'s existing behavior, already true for Postgres's own
`Value::Float` today, not something new this spec introduces. MySQL itself
mostly rejects `NaN`/`Infinity` as input anyway (they're not valid numeric
literals in SQL), so this is expected to matter only as a documented
limitation, not a test blocker. Mention it in `docs/LIMITATIONS.md` under
the same persistence paragraph (§7) rather than trying to fix it — fixing
it would mean a custom encoding shared with Postgres, which is out of
scope for this spec.

## 5. Engine and server changes

### 5.1 `src/mysql/engine.rs`

Add:

```rust
impl Engine {
    /// Clones out the shared database state for an on-disk snapshot. Only
    /// `db` is persisted -- see the persistence spec, §4.4, for why every
    /// other field here is per-connection/per-statement and deliberately
    /// excluded.
    pub fn snapshot(&self) -> DbState {
        self.db.lock().unwrap().clone()
    }

    /// Builds an `Engine` whose shared database state is `db` (loaded from
    /// an on-disk snapshot), with every per-connection field at its
    /// ordinary just-connected default -- same as `Engine::new()`, just
    /// with real data instead of an empty `DbState`.
    pub fn new_persistent(db: DbState) -> Self {
        Self { db: Arc::new(Mutex::new(db)), ..Self::default() }
    }
}
```

(`DbState` already derives `Clone`, so `.clone()` in `snapshot()` is cheap
to write if not cheap to run — see §8 for why that's fine here.)

### 5.2 `src/mysql/server.rs`

Mirror `src/postgres/server.rs:67-123` exactly, substituting `mysql.json`
and `DbState`/`Engine::new_persistent` for Postgres's `Snapshot`/
`Engine::new_persistent`:

```rust
use std::path::Path;

pub fn spawn_persistent(addr: &str, data_dir: &Path) -> io::Result<SocketAddr> {
    let (addr, save) = spawn_persistent_for_test(addr, data_dir)?;
    crate::persistence::on_shutdown(save);
    Ok(addr)
}

pub fn spawn_persistent_for_test(
    addr: &str,
    data_dir: &Path,
) -> io::Result<(SocketAddr, impl Fn() + Send + Sync + 'static)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;

    let path = data_dir.join("mysql.json");
    let engine = if path.exists() {
        let bytes = std::fs::read(&path)?;
        let db: crate::mysql::catalog::DbState = serde_json::from_slice(&bytes).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("failed to parse mysql snapshot: {e}"))
        })?;
        Engine::new_persistent(db)
    } else {
        Engine::new()
    };

    let save_engine = engine.clone();
    let save = move || {
        let db = save_engine.snapshot();
        let bytes = serde_json::to_vec(&db).expect("mysql snapshot serialization failed");
        let _ = crate::persistence::write_snapshot_atomically(&path, &bytes);
    };

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            let _ = thread::spawn(move || {
                let _ = serve(stream, engine);
            });
        }
    });
    Ok((local, save))
}
```

Keep the existing plain `spawn` untouched — it's still what `--only mysql`
without a real `--data-dir` concept effectively uses today via
`services::start` (see §5.3).

### 5.3 `src/services.rs`

Add the MySQL arm to `start_persistent` (currently falls through to
`start`, meaning a running server with persistence enabled still silently
drops MySQL data on restart today — this is the actual bug this spec
fixes end-to-end):

```rust
#[cfg(feature = "mysql")]
"mysql" => Some(crate::mysql::server::spawn_persistent(addr, data_dir)),
```

Add it in the same relative position/style as the existing three arms
(`postgres`, `elasticsearch`, `redis`) at `src/services.rs:18-23`.

### 5.4 `src/mysql/mod.rs`

No export changes needed if `server::spawn_persistent` is already `pub fn`
inside `pub mod server;` (it will be, following §5.2) — `src/services.rs`
calls it as `crate::mysql::server::spawn_persistent(...)`, same path style
already used for Postgres/Redis. Elasticsearch re-exports at the `mod.rs`
level instead (`pub use server::spawn_persistent;`) because its
`services.rs` call site uses `crate::elasticsearch::spawn_persistent`
(shorter path) — pick whichever style matches what you wire into
`services.rs`; just be consistent with the path you actually write there.

## 6. Test plan

### 6.1 `tests/mysql_persistence.rs` (new)

Mirror `tests/redis_persistence.rs` exactly in structure (one test
function, temp dir under `std::env::temp_dir()` keyed by
`std::process::id()`, cleaned up with `remove_dir_all` first). Cover, in
one connected session against `addr1`:

- A table with every `ColumnType` variant actually used somewhere
  (`INT`, `BIGINT`, `VARCHAR(n)`, `TEXT`, `FLOAT`, `DOUBLE`,
  `DECIMAL(p,s)`, `DATE`, `DATETIME`, `BOOLEAN`) — one column each, so
  every arm of `Value`'s new derive is actually exercised, not just
  `Int`/`Text`.
- An `AUTO_INCREMENT` primary key column — insert a few rows, assert the
  *next* auto-increment value survives the restart (insert one more row
  after reconnecting, assert its generated ID continues the original
  sequence rather than restarting at 1). This is the one piece of
  **schema-level** mutable state (`Table::next_auto_increment`) that's
  easy to forget versus just "the rows survived."
  - **Known inherited gap, not a persistence-spec issue**: there's a
    pre-existing comment at `src/mysql/engine.rs:19-26` noting MySQL's
    `LAST_INSERT_ID()` SQL function isn't implemented yet. Don't try to
    persist session-level `last_insert_id` across a restart (§4.4 already
    excludes it, correctly) — only `next_auto_increment` is schema state.
- A `NULL` value in a nullable column and an empty string in a text
  column, to catch an accidental `Option`-collapsing bug in the derive.
- A second schema via `CREATE DATABASE other_db; USE other_db; CREATE
  TABLE ...` — confirms more than just the implicit default schema
  survives (same spirit as the Redis test's `SELECT 1` coverage).
- A `JSON` column with a real nested document (object containing an array
  containing a number) and a `DECIMAL` column with a fractional value
  (e.g. `19.99`) — these are the two variants (`Value::Json`,
  `Value::Num`) whose correctness depends on `src/sql/json.rs` and
  `src/sql/numeric.rs`'s own `Serialize`/`Deserialize` impls, not on new
  code this spec writes; worth asserting explicitly since a regression
  there would be silent otherwise.

Then: call `save1()` directly (not a real signal — see the Redis test's
own doc comment for why), assert `dir.join("mysql.json").exists()`, start
a second server against the same `dir` via `spawn_persistent_for_test`
again, reconnect, and assert every one of the above survived with the
exact same values (and types — e.g. the `DECIMAL` column must still
compare equal as a `Numeric`, not have decayed into a lossy `f64`).

### 6.2 Engine-level round-trip test (in `src/mysql/engine.rs`'s own
`#[cfg(test)] mod tests` or a new one in `catalog.rs`)

A smaller, faster-iterating test that skips the network entirely:
`Engine::new()` → run some SQL → `engine.snapshot()` → `serde_json::to_vec`
→ `serde_json::from_slice` back into `DbState` → `Engine::new_persistent`
→ run a `SELECT` and assert the rows match. Useful for debugging a serde
derive issue without spinning up a TCP server; not a replacement for 6.1.

### 6.3 Don't regress the existing suites

`cargo test --features mysql` (or `--all-features`) must stay green:
`tests/mysql_client.rs`, `tests/mysql_diff.rs`, `tests/ecommerce_mysql_diff.rs`.
None of them use `spawn_persistent` today (they use plain `spawn`), so
they shouldn't be affected — but run them anyway after the catalog-level
derive changes in §4, since those touch types every one of those suites
exercises.

## 7. Docs to update in the same PR

- `docs/LIMITATIONS.md`: MySQL's section currently says nothing about
  persistence at all (confirmed by grep — Postgres/Redis/Elasticsearch
  each have an explicit "Storage is now persistent..." paragraph with a
  pointer to `src/persistence.rs`; MySQL has none). Add the matching
  paragraph, plus one line on the §4.5 non-finite-float caveat if you
  decide it's worth calling out explicitly (Postgres's own equivalent
  paragraph doesn't mention it, so matching that precedent and leaving it
  out is also fine — use judgment, don't block the PR on this).
- `README.md`: the "At a glance" table's persistence row currently reads
  *"On-disk persistence for Postgres, Redis, Elasticsearch — survives a
  clean restart; MySQL and Kafka next"*. Once this lands, update it to
  name MySQL alongside the other three (and drop "MySQL" from the "next"
  clause — leave Kafka there until its own persistence spec, see
  `docs/specs/kafka-persistence.md`, lands separately). Same edit applies
  to the Compatibility table's MySQL row if it gets a persistence
  callout added there.

## 8. Explicitly out of scope

- **Performance.** `Engine::snapshot()` clones the entire `DbState`
  (including every `Arc<Table>` — cheap, just a refcount bump per table —
  but `BTreeMap` clones and eventually every row inside each table once
  serialized) on every save. This project's stated tradeoff is "simple
  over performant" for local-dev-scale data; don't add incremental/WAL-style
  persistence to optimize this.
- **Schema migration of the snapshot format itself.** If `DbState`'s shape
  changes in a later PR, an old `mysql.json` written by an older binary
  may fail to deserialize. This is the same property Postgres/Redis/
  Elasticsearch's snapshots already have (no versioned migration
  exists for them either) — don't build one here either; match the
  existing precedent.
- **`DROP TABLE`.** Not implemented at all today (a real, separate gap —
  see `docs/LIMITATIONS.md`'s existing MySQL "Not yet" list, added after
  `benchmarks/noidadb_bench.py` hit it). Out of scope for this spec; don't
  let it block persistence landing.
