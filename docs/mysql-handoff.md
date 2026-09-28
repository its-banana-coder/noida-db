# MySQL Agent Handoff

**Update (2026-09-28): read `docs/specs/mysql.md` section 2 first.**
Postgres is now done and merged to `main` — this branch's base (`26a5759`)
predates it, along with Kafka, ClickHouse, Elasticsearch and Memcached all
landing. Section 2 covers what's actually reusable now (`src/sql/`'s
datetime/numeric/tz/json — real and shared; there is *no* shared
binder/executor to plug into, build MySQL's own following `src/postgres/`
as a reference), two specific bug classes worth designing against from day
one (join predicate pushdown; prepared-statement re-bind must reuse
Prepare's resolved parameter types, not re-guess from decoded values), and
why WordPress specifically is the real-app validation target once the
basics work. **Rebase onto `main` before writing new code, not just before
opening the PR** — the review findings below still apply, but they should
be fixed on top of current `main`, not this stale base.

## Current branch and worktree

- Branch: `svc/mysql`
- Worktree: `/home/kawadhiya21/noida-db/.claude/worktrees/mysql`
- Base: was latest fetched `main` at commit `26a5759`; **rebase onto
  current `main` before doing anything else** (see update above).
- The Kafka agent's worktree and the Redis/Postgres worktrees were left untouched.

## Work completed in the previous pass

The earlier MySQL pass was developed in another agent worktree, but it was not committed and is not present in this isolated worktree. Its reported scope was an initial native MySQL bootstrap server with:

- MySQL protocol handshake and login flow
- `COM_PING`, `COM_INIT_DB`, `COM_QUERY`, and `COM_QUIT`
- Basic `SELECT 1`, version, database, connection ID, `SHOW DATABASES`, and `USE`
- Unknown database error `1049`
- Raw protocol tests and a placeholder differential-test target
- `cargo fmt`, build, tests, and Clippy reported passing there

Treat that implementation summary as historical context only; inspect the current branch before relying on any of it.

## Review findings to fix

1. The server advertises MySQL query attributes but does not consume the query-attributes header. The real MySQL 8.0 CLI therefore cannot run even `SELECT 1`. Either stop advertising that capability or implement parsing for it.
2. Authentication currently accepts every password. Implement the expected `caching_sha2_password` or `mysql_native_password` challenge/response behavior, including wrong-password error `1045` and auth-switch handling where needed.
3. Query handling uses exact strings after lowercasing the complete statement. Support normal syntax such as `SELECT 1 AS x`; preserve database-name case for `USE`; reject invalid `SET` variables with MySQL-compatible errors.
4. `SELECT DATABASE()` must return SQL `NULL` when no schema is selected, not an empty string.
5. `tests/mysql_diff.rs` only checks that a reference server exists. Add actual comparison coverage against the installed MySQL 8.0 server, including column metadata: type, flags, charset, and values.
6. Do not hardcode `noida_ref` as a real database. The reference database name belongs to the test harness, not the server's built-in schema list.
7. Validate packet sequence numbers and handle large multi-packet messages.
8. Add multi-statement query support according to the advertised client capability, or stop advertising that capability.

## Suggested implementation order

1. Read `docs/specs/README.md` and `docs/specs/mysql.md` fully.
2. Inspect the current service registration and feature patterns in `Cargo.toml`, `src/lib.rs`, and `src/services.rs`.
3. Build the minimal MySQL protocol module on this branch.
4. Make advertised capabilities match implemented behavior; begin by removing unsupported query attributes and multi-statements if they are not implemented.
5. Add authentication with deterministic test credentials and verify both success and error `1045` paths.
6. Replace exact-string query matching with a small parser for the specified bootstrap SQL and correct `NULL`/case behavior.
7. Add packet-framing and sequence-number tests, including payloads larger than one MySQL packet.
8. Implement the differential harness and compare values plus result-set metadata against MySQL 8.0.
9. Run formatting, build with no features, MySQL tests, the full test suite, and Clippy with warnings denied.
10. Rebase `svc/mysql` on `main` again before opening the PR (you already
    did this in step 0, per the update at the top — this catches whatever
    landed on `main` in the meantime). Shared-file conflicts are expected in `Cargo.toml`, `src/lib.rs`, `src/services.rs`, and `.github/workflows/ci.yml`.

## Constraints

- Do not modify `src/redis/` or `src/postgres/`.
- Do not modify branches `svc/redis-types` or `svc/postgres`.
- Keep shared-file edits small and isolated.
- The spec is the brief; unsupported features should produce MySQL-like errors rather than silently behaving differently.
