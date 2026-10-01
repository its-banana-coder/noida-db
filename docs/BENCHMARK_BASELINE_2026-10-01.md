# Infrastructure baseline — 2026-10-01

This is a short, warm-cache smoke baseline, not a capacity claim. Each SQL,
Redis, and Elasticsearch client used its native wire protocol/API; server CPU
and RSS exclude the benchmark client. The artifact was built with
`cargo build --release --all-features` and each point ran for half a second
with the stated client count. Linux `perf` was not enabled, so cycles and
instructions are intentionally absent. Hardware: a shared WSL2 VM (1
physical / 2 logical cores, ~6 GB RAM) — numbers are directional, not an
absolute capacity claim; re-run on a controlled host before comparing
commits.

**Supersedes the original same-day baseline below this table**, which
measured MySQL `read`/`scan` at 17-50ms — not a real engine characteristic,
but `src/mysql/server.rs` missing `TCP_NODELAY` on accepted connections
(Nagle's algorithm plus the client's own delayed-ACK timer stalling every
response). Fixed; see the README's own "Benchmarks" section and
`docs/LIMITATIONS.md` for the full writeup. This table is the first baseline
taken after that fix, and after the harness itself was fixed to reload the
table between operations (the original baseline's `read`/`scan` numbers were
also confounded by table growth left over from the `insert` phase that ran
right before them in the same sweep).

| Service | Dataset | Operation | Throughput | p50 | p99 | Server RSS | CPU time/op |
|---|---:|---|---:|---:|---:|---:|---:|
| Redis | 1K × 100 B | read | 14,710 ops/s | 65.5 µs | 151.2 µs | 9.91 MB | 29.9 µs |
| Redis | 1K × 100 B | scan | 3,092 ops/s | 296.4 µs | 632.4 µs | 10.18 MB | 213.3 µs |
| Postgres | 1K × 100 B | read | 2,354 ops/s | 412.7 µs | 720.0 µs | 9.96 MB | 348.1 µs |
| Postgres | 1K × 100 B | scan | 290 ops/s | 3391.1 µs | 4416.8 µs | 9.20 MB | 684.9 µs |
| MySQL | 1K × 100 B | read | 1,716 ops/s | 544.4 µs | 1163.0 µs | 9.76 MB | 512.2 µs |
| MySQL | 1K × 100 B | scan | 263 ops/s | 3709.4 µs | 6019.0 µs | 13.64 MB | 3484.9 µs |
| Elasticsearch | 1K × 100 B | read | 3,778 ops/s | 230.8 µs | 647.8 µs | 5.35 MB | 15.9 µs |
| Elasticsearch | 1K × 100 B | scan | 3,725 ops/s | 231.4 µs | 755.0 µs | 5.57 MB | 16.1 µs |

The raw JSONL and machine captures are kept locally under
`benchmark-results/20261001T150148Z` (Redis),
`benchmark-results/20261001T150230Z` (Postgres),
`benchmark-results/20261001T150527Z` (MySQL), and
`benchmark-results/20261001T150603Z` (Elasticsearch). They are deliberately
not committed: an individual host's output is evidence for this baseline, not
a portable project fixture. Re-run the documented command on a controlled host
before comparing commits.

Kafka is not represented in this table yet. Its producer/fetch benchmark must
be added through a native long-lived Kafka protocol client; reporting CLI setup
time as database work would invalidate the comparison.

---

## Original baseline (superseded, kept for the record)

Measured before the `TCP_NODELAY` fix and before the harness's own
operation-isolation fix — MySQL's numbers here are a test artifact, not a
real engine characteristic.

| Service | Dataset | Operation | Throughput | p50 | p99 | Server RSS | CPU time/op |
|---|---:|---|---:|---:|---:|---:|---:|
| Redis | 1K × 100 B | point read | 14,542 ops/s | 65.5 µs | 343.1 µs | 3.29 MB | 30.2 µs |
| Redis | 1K × 100 B | scan | 2,987 ops/s | 296.6 µs | 966.3 µs | 3.49 MB | 224.2 µs |
| Postgres | 1K × 100 B | point read | 1,940 ops/s | 422.3 µs | 1.30 ms | 5.46 MB | 401.9 µs |
| Postgres | 1K × 100 B | full scan | 275 ops/s | 3.35 ms | 5.24 ms | 5.57 MB | 833.3 µs |
| MySQL | 1K × 100 B | point read | 17.3 ops/s | 50.2 ms | 60.2 ms | 5.74 MB | 4.44 ms |
| MySQL | 1K × 100 B | full scan | 17.1 ops/s | 57.8 ms | 71.2 ms | 5.74 MB | 6.11 ms |
| Elasticsearch | 100 × 100 B | document GET | 3,819 ops/s | 224.8 µs | 803.1 µs | 3.01 MB | 13.1 µs |
| Elasticsearch | 100 × 100 B | match-all search | 3,611 ops/s | 228.6 µs | 895.2 µs | 3.10 MB | 19.4 µs |
