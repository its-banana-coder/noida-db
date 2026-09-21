# Building a noida service

Every service follows the same shape. Redis (`src/redis/`) is the reference
implementation; read it before starting.

## Goal and priorities

- **Wire-compatible:** real drivers, ORMs and CLIs work unchanged.
- **Usage-first:** implement what real clients actually send first (the
  handshake, what the main drivers and ORMs call), then widen. Defer rare
  edge cases until a real client test needs them.
- **Exact where clients look:** replies, types and error codes/messages match
  the real server. Unsupported features return the real server's own
  "not supported"-style error, never a silently wrong answer.
- **Small and simple:** performance is not a goal. Plain data structures,
  a thread per connection, one lock. Keep the binary small; justify every
  dependency. Prefer protocol crates (pgwire, sqlparser, kafka-protocol,
  bson) over hand-written wire codecs.
- **No performance analysis:** don't build EXPLAIN ANALYZE, slow logs or
  stats. If clients may send such commands, accept them and return a minimal
  valid reply.
- **Single node:** no clustering or replication. Where a feature needs a
  cluster, reply as a standalone real server would.

## Layout

- `src/<service>/mod.rs` exposes `pub fn spawn(addr: &str) ->
  io::Result<SocketAddr>`: bind, serve on background threads, return the
  bound address. `src/services.rs` already routes `noida start` to it.
- Put the service behind its Cargo feature (already declared in Cargo.toml).
  Add it to `default` once it serves something useful.
- Keep protocol, engine and server separate so the engine is testable
  without sockets (see `src/redis/engine.rs`).

## Test-driven, in four layers

Write the tests first, watch them fail, then implement.

1. **Unit tests** for codecs and parsers.
2. **Engine tests** that call the engine directly, with an injected clock.
3. **Real-client tests** (`tests/<service>_client.rs`): a real driver crate
   (dev-dependency only) talks to `spawn("127.0.0.1:0")`.
4. **Differential tests** (`tests/<service>_diff.rs`): run the same script
   against the real server and noida and require identical results. Use
   `NOIDA_<SERVICE>_REF=host:port` if set (CI), else start a local server if
   one is installed, else print `SKIPPED` and pass. See `tests/redis_diff.rs`.

Gate each test file with `required-features` in Cargo.toml.

## CI

Add the real server as a service container in `.github/workflows/ci.yml`
and set `NOIDA_<SERVICE>_REF`. Add a `--nocapture` step for the diff test so
the log shows how many results were compared.

## Before you commit

`cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`,
`cargo build --no-default-features`, `scripts/check-size.sh`. Commit messages
end with the Co-Authored-By line used in this repo.
