# Initial infrastructure baseline — 2026-10-01

This is a short, warm-cache smoke baseline, not a capacity claim. Each SQL,
Redis, and Elasticsearch client used its native wire protocol/API; server CPU
and RSS exclude the benchmark client. The artifact was built with
`cargo build --release --all-features` and each point ran for one second with
one client. Linux `perf` was not enabled, so cycles and instructions are
intentionally absent.

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

The raw JSONL and machine captures are kept locally under
`benchmark-results/20261001T123712Z` (Redis),
`benchmark-results/20261001T123718Z` (Postgres),
`benchmark-results/20261001T123908Z` (MySQL), and
`benchmark-results/20261001T123859Z` (Elasticsearch). They are deliberately
not committed: an individual host's output is evidence for this baseline, not
a portable project fixture. Re-run the documented command on a controlled host
before comparing commits.

Kafka is not represented in this table yet. Its producer/fetch benchmark must
be added through a native long-lived Kafka protocol client; reporting CLI setup
time as database work would invalidate the comparison.
