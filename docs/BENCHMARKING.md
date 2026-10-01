# Infrastructure benchmarks

`benchmarks/noidadb_bench.py` measures NoidaDB's database work and its
resource cost. It is deliberately not a production-traffic simulator: no
users, HTTP applications, QPS targets, or business workflows are fabricated.

The current native adapters are Redis, Postgres, MySQL, and Elasticsearch.
They measure `insert`, `read`, `update`, `delete`, and `scan` over independently
selected record counts, record sizes, and client concurrency. Operations
unsupported by an adapter are not substituted with a different operation or
silently reported as zero; they remain out of that adapter's report until a
comparable implementation exists. Kafka is next: it requires a long-lived
native Kafka-protocol client so that producer/fetch costs are not polluted by a
per-operation command-line client.

## Run

Build the exact artifact being measured, then use a unique port and output
directory. The smoke profile is intentionally small; it validates the
measurement pipeline on a laptop.

```sh
cargo build --release --all-features
python3 benchmarks/noidadb_bench.py --service redis --profile smoke --output benchmark-results
```

For a controlled machine, pin the server and client to disjoint CPUs. This
prevents the benchmark process from contaminating server CPU measurements.

```sh
python3 benchmarks/noidadb_bench.py \
  --service postgres --profile full --server-cpus 0-3 --client-cpus 4-7 --perf \
  --seconds 20 --output benchmark-results
```

`--profile full` contains the specified dimension curves through 100M records
and 1 MiB records. It can require hundreds of gigabytes and is never the
default. Narrow a run explicitly while developing:

```sh
python3 benchmarks/noidadb_bench.py --sizes 1000,10000,100000 \
  --record-sizes 100,1024 --concurrency 1,2,4,8 --operations read,scan
```

## Results and interpretation

Every run creates a timestamped directory containing:

- `environment.json`: CPU/OS/filesystem/runtime/build metadata and CPU pinning.
- `results.jsonl`: one raw result per operation/dataset/record-size/concurrency
  point, including latency percentiles, server-only CPU/RSS/virtual-memory/IO,
  and derived CPU seconds, cycles, and instructions per operation.
- `storage.json`: physical data-directory size after clean shutdown and the
  Redis snapshot flush.
- `report.html`: dependency-free SVG graphs for the selected measurements.

When `--perf` is requested, Linux `perf stat` is attached only to the server
PID and attempts cycles, instructions, cache, branch, page-fault, and context
switch counters. A `null` field means the host/kernel permissions did not make
that measurement available; it never means zero. The client is separately
pinned, and its memory is never included in server RSS.

Linux page cache is also never relabeled as database memory. It has a separate
system-wide ownership model, so the suite records server RSS and virtual memory
as database-process metrics and marks page-cache attribution unavailable rather
than manufacturing a value.

The suite starts a fresh server/data directory per invocation. Its current
Redis persistence model writes a snapshot on clean shutdown, so storage size is
also recorded after shutdown in `data/redis.json`; use that value for storage
amplification. For durability, startup/recovery, cold-cache, index, and
engine-direct comparisons, add a service adapter only with an operation whose
semantics and durability configuration match the comparison target. This keeps
the reports honest rather than comparing unrelated systems.

## Reproducibility checklist

The initial Redis adapter reports warm runs after a deterministic load. Cold
cache, index, startup/recovery, and engine-direct modes are deliberately shown
as unavailable report slots until they can be measured with a valid reset
procedure, rather than being simulated. Record the commit (also captured in
`environment.json`), leave the host otherwise idle, use a fixed governor/CPU
set when available, and retain the full result directory. Do not compare
durability modes or different record encodings without labeling them.
