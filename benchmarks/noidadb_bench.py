#!/usr/bin/env python3
"""Repeatable infrastructure benchmarks for NoidaDB's Redis service.

The harness deliberately measures a database operation, not an application
workload.  It has no third-party dependencies and writes one JSON object per
measurement plus a self-contained HTML/SVG report.  Large runs are opt-in:
the default smoke profile exercises the machinery without accidentally
allocating gigabytes on a developer laptop.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EVENTS = "cycles,instructions,cache-references,cache-misses,branches,branch-misses,page-faults,context-switches"
SMOKE = {"sizes": [1_000, 10_000], "record_sizes": [100, 1_024], "concurrency": [1, 2, 4]}
FULL = {"sizes": [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000], "record_sizes": [100, 1_024, 10_240, 102_400, 1_048_576], "concurrency": [1, 2, 4, 8, 16, 32, 64, 128]}


def read(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def command_version(command: list[str]) -> str | None:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def hardware() -> dict[str, Any]:
    cpuinfo = read("/proc/cpuinfo") or ""
    model = next((x.split(":", 1)[1].strip() for x in cpuinfo.splitlines() if x.startswith("model name")), "unknown")
    mem = next((x.split(":", 1)[1].strip() for x in (read("/proc/meminfo") or "").splitlines() if x.startswith("MemTotal")), "unknown")
    root_fs = command_version(["findmnt", "-n", "-o", "FSTYPE,SOURCE", "/"])
    physical = set()
    current_physical = current_core = None
    for line in cpuinfo.splitlines():
        if line.startswith("physical id"): current_physical = line.split(":", 1)[1].strip()
        elif line.startswith("core id"):
            current_core = line.split(":", 1)[1].strip()
        elif not line.strip() and current_physical is not None and current_core is not None:
            physical.add((current_physical, current_core)); current_physical = current_core = None
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(), "cpu_model": model,
        "physical_cores": len(physical) or None, "logical_cores": os.cpu_count(), "ram": mem,
        "storage_model": read("/sys/block/" + Path((root_fs or "").split()[-1]).name + "/device/model") or "not detected",
        "filesystem": root_fs or "not detected", "os": platform.platform(),
        "kernel": platform.release(), "python": sys.version.split()[0],
        "rustc": command_version(["rustc", "--version"]),
    }


def proc_stats(pid: int) -> dict[str, float] | None:
    """Server-only /proc metrics. Page cache intentionally is not RSS."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().split()
        status = Path(f"/proc/{pid}/status").read_text().splitlines()
        io = {k.rstrip(":"): int(v) for k, v in (line.split(":", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())}
        rss = next(int(line.split()[1]) for line in status if line.startswith("VmRSS:")) * 1024
        peak = next(int(line.split()[1]) for line in status if line.startswith("VmHWM:")) * 1024
        vmem = next(int(line.split()[1]) for line in status if line.startswith("VmSize:")) * 1024
        hz = os.sysconf("SC_CLK_TCK")
        return {"user_seconds": int(stat[13]) / hz, "system_seconds": int(stat[14]) / hz,
                "rss_bytes": rss, "peak_rss_bytes": peak, "virtual_bytes": vmem,
                "read_bytes": io.get("read_bytes", 0), "write_bytes": io.get("write_bytes", 0),
                "context_switches": sum(int(line.split()[1]) for line in status if line.startswith(("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")))}
    except (OSError, ValueError, StopIteration):
        return None


def directory_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def cpu_set(spec: str) -> set[int]:
    cpus: set[int] = set()
    for part in spec.split(","):
        start, sep, end = part.strip().partition("-")
        if not start: raise ValueError("empty CPU-set component")
        cpus.update(range(int(start), int(end) + 1) if sep else [int(start)])
    return cpus


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values.sort()
    return values[min(len(values) - 1, math.ceil(p * len(values)) - 1)]


class Redis:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")

    def close(self) -> None:
        self.file.close(); self.sock.close()

    def command(self, *parts: bytes) -> Any:
        self.sock.sendall(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts))
        return self.reply()

    def reply(self) -> Any:
        lead = self.file.read(1)
        if lead == b"+": return self.file.readline()[:-2]
        if lead == b":": return int(self.file.readline())
        if lead == b"$":
            n = int(self.file.readline())
            return None if n == -1 else self.file.read(n + 2)[:-2]
        if lead == b"*": return [self.reply() for _ in range(int(self.file.readline()))]
        raise RuntimeError("Redis protocol error: " + self.file.readline().decode(errors="replace"))


def wait_for_redis(host: str, port: int, process: subprocess.Popen[bytes] | None) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process and process.poll() is not None:
            raise RuntimeError("server exited before becoming ready")
        try:
            c = Redis(host, port); ok = c.command(b"PING") == b"PONG"; c.close()
            if ok: return
        except OSError: time.sleep(.05)
    raise RuntimeError("timed out waiting for Redis")


def launch(args: argparse.Namespace, run_dir: Path) -> subprocess.Popen[bytes] | None:
    if args.no_start: return None
    binary = Path(args.binary)
    if not binary.exists():
        raise RuntimeError(f"missing {binary}; run cargo build --release or pass --binary")
    cmd = [str(binary), "start", "--only", "redis", "--redis-port", str(args.port), "--data-dir", str(run_dir / "data")]
    if args.server_cpus and shutil.which("taskset"):
        cmd = ["taskset", "--cpu-list", args.server_cpus, *cmd]
    log = (run_dir / "server.log").open("wb")
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)


def load(host: str, port: int, records: int, record_size: int) -> None:
    value = b"x" * record_size
    c = Redis(host, port)
    c.command(b"FLUSHDB")
    for start in range(0, records, 1_000):
        for i in range(start, min(records, start + 1_000)):
            c.command(b"SET", f"k:{i:012d}".encode(), value)
    c.close()


def operation(c: Redis, name: str, n: int, records: int, value: bytes) -> None:
    key = f"k:{n % records:012d}".encode()
    if name == "read": c.command(b"GET", key)
    elif name == "update": c.command(b"SET", key, value)
    elif name == "delete": c.command(b"DEL", key); c.command(b"SET", key, value)
    elif name == "insert": c.command(b"SET", f"insert:{n:012d}".encode(), value)
    elif name == "scan": c.command(b"SCAN", b"0", b"COUNT", b"100")
    else: raise ValueError(name)


def collect_perf(pid: int) -> tuple[subprocess.Popen[str] | None, Path | None]:
    if not shutil.which("perf"): return None, None
    output = Path(f"/tmp/noidadb-perf-{pid}-{time.time_ns()}.csv")
    try:
        return subprocess.Popen(["perf", "stat", "-x,", "-e", EVENTS, "-p", str(pid), "-o", str(output), "--", "sleep", "600"], stderr=subprocess.DEVNULL, text=True), output
    except OSError: return None, None


def parse_perf(proc: subprocess.Popen[str] | None, path: Path | None) -> dict[str, int | None]:
    out = {name: None for name in EVENTS.split(",")}
    if proc: proc.send_signal(signal.SIGINT); proc.wait(timeout=5)
    if not path or not path.exists(): return out
    for line in path.read_text(errors="replace").splitlines():
        bits = [x.strip() for x in line.split(",")]
        if len(bits) >= 3 and bits[2] in out:
            try: out[bits[2]] = int(bits[0].replace(" ", "").replace(",", ""))
            except ValueError: pass
    path.unlink(missing_ok=True)
    return out


def run_case(args: argparse.Namespace, server_pid: int | None, records: int, record_size: int, concurrency: int, op: str, cold: bool) -> dict[str, Any]:
    if cold: time.sleep(.2)  # restart is the caller's responsibility; this only marks an uncached first pass.
    before = proc_stats(server_pid) if server_pid else None
    perf, perf_path = collect_perf(server_pid) if args.perf and server_pid else (None, None)
    barrier = threading.Barrier(concurrency)
    stop = time.monotonic() + args.seconds
    latency: list[float] = []; counts: list[int] = []; errors: list[str] = []
    value = b"u" * record_size

    def worker(worker_id: int) -> None:
        try:
            c = Redis(args.host, args.port); barrier.wait(); n = worker_id
            local: list[float] = []; count = 0
            while time.monotonic() < stop:
                begin = time.perf_counter_ns(); operation(c, op, n, records, value)
                local.append((time.perf_counter_ns() - begin) / 1_000); count += 1; n += concurrency
            c.close(); latency.extend(local); counts.append(count)
        except Exception as exc: errors.append(repr(exc))

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(worker, range(concurrency)))
    elapsed = time.monotonic() - started
    after = proc_stats(server_pid) if server_pid else None
    counters = parse_perf(perf, perf_path)
    cpu = {"user_seconds": None, "system_seconds": None, "cycles": counters["cycles"], "instructions": counters["instructions"],
           "ipc": None, "cache_references": counters["cache-references"], "cache_misses": counters["cache-misses"],
           "branches": counters["branches"], "branch_misses": counters["branch-misses"], "page_faults": counters["page-faults"], "context_switches": counters["context-switches"]}
    memory = {"rss_mb": None, "peak_rss_mb": None, "virtual_mb": None, "page_cache_mb": None,
              "note": "RSS is server process only; Linux page cache is intentionally not attributed to the database."}
    storage = {"database_bytes": None, "read_bytes": None, "write_bytes": None}
    if before and after:
        cpu.update(user_seconds=after["user_seconds"] - before["user_seconds"], system_seconds=after["system_seconds"] - before["system_seconds"])
        if cpu["instructions"] and cpu["cycles"]: cpu["ipc"] = cpu["instructions"] / cpu["cycles"]
        if cpu["context_switches"] is None: cpu["context_switches"] = after["context_switches"] - before["context_switches"]
        memory.update(rss_mb=after["rss_bytes"] / 1e6, peak_rss_mb=after["peak_rss_bytes"] / 1e6, virtual_mb=after["virtual_bytes"] / 1e6)
        storage.update(read_bytes=after["read_bytes"] - before["read_bytes"], write_bytes=after["write_bytes"] - before["write_bytes"])
    operations = sum(counts)
    cpu_utilization = ((cpu["user_seconds"] + cpu["system_seconds"]) / elapsed * 100
                       if elapsed and cpu["user_seconds"] is not None else None)
    return {"benchmark": op, "mode": "server", "cache_state": "cold" if cold else "warm", "dataset_records": records,
            "logical_size_bytes": records * record_size, "record_size_bytes": record_size, "concurrency": concurrency,
            "operations": operations, "duration_seconds": elapsed, "throughput_ops_sec": operations / elapsed if elapsed else 0,
            "latency_us": {"p50": percentile(latency, .50), "p95": percentile(latency, .95), "p99": percentile(latency, .99), "p999": percentile(latency, .999)},
            "cpu": cpu, "memory": memory, "storage": storage, "errors": len(errors), "error_samples": errors[:3],
            "derived": {"cycles_per_operation": cpu["cycles"] / operations if cpu["cycles"] and operations else None,
                        "instructions_per_operation": cpu["instructions"] / operations if cpu["instructions"] and operations else None,
                        "cpu_seconds_per_operation": (cpu["user_seconds"] + cpu["system_seconds"]) / operations if operations and cpu["user_seconds"] is not None else None,
                        "cpu_utilization_percent": cpu_utilization}}


def svg_chart(title: str, rows: list[dict[str, Any]], xkey: str, ykey: str) -> str:
    def value(row: dict[str, Any], key: str) -> Any:
        current: Any = row
        for part in key.split("."):
            if not isinstance(current, dict): return None
            current = current.get(part)
        return current
    points = [(value(r, xkey), value(r, ykey)) for r in rows]
    labels = {x: i for i, x in enumerate(dict.fromkeys(x for x, y in points if isinstance(x, str)))}
    points = [(float(labels[x]) if isinstance(x, str) else float(x), float(y)) for x, y in points if x is not None and y is not None]
    if not points: return f"<section><h2>{title}</h2><p>Not collected by this selected profile.</p></section>"
    width, height, pad = 720, 300, 55
    xmin, xmax = min(x for x, _ in points), max(x for x, _ in points); ymin, ymax = 0, max(y for _, y in points)
    sx = lambda x: pad + (width - 2 * pad) * ((x - xmin) / (xmax - xmin) if xmax != xmin else .5)
    sy = lambda y: height - pad - (height - 2 * pad) * (y / ymax if ymax else 0)
    path = " ".join(("M" if i == 0 else "L") + f"{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(sorted(points)))
    return f'<section><h2>{title}</h2><svg viewBox="0 0 {width} {height}" role="img"><path class="axis" d="M{pad},{pad}V{height-pad}H{width-pad}"/><path class="line" d="{path}"/>' + "".join(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="3"/>' for x,y in points) + f'<text x="{pad}" y="{height-12}">{xkey}</text><text x="{width-180}" y="{pad+12}">{ykey}</text></svg></section>'


def report(run_dir: Path, results: list[dict[str, Any]]) -> None:
    charts = [
        svg_chart("1. Throughput vs dataset size", results, "dataset_records", "throughput_ops_sec"),
        svg_chart("2. Latency vs dataset size (p99)", results, "dataset_records", "latency_us.p99"),
        svg_chart("3. CPU/op vs dataset size", results, "dataset_records", "derived.cpu_seconds_per_operation"),
        svg_chart("4. RAM vs dataset size", results, "dataset_records", "memory.rss_mb"),
        svg_chart("5. Storage vs dataset size", results, "dataset_records", "storage.database_bytes"),
        svg_chart("6. Throughput vs concurrency", results, "concurrency", "throughput_ops_sec"),
        svg_chart("7. Latency vs concurrency (p99)", results, "concurrency", "latency_us.p99"),
        svg_chart("8. CPU utilization vs concurrency", results, "concurrency", "derived.cpu_utilization_percent"),
        svg_chart("9. Throughput vs record size", results, "record_size_bytes", "throughput_ops_sec"),
        svg_chart("10. CPU/op vs record size", results, "record_size_bytes", "derived.cpu_seconds_per_operation"),
        svg_chart("11. Cold vs warm read latency", [r for r in results if r["benchmark"] == "read"], "cache_state", "latency_us.p99"),
        svg_chart("12. Indexed vs non-indexed latency", results, "dataset_records", "index_latency_not_collected"),
        svg_chart("13. Startup time vs database size", results, "dataset_records", "startup_seconds_not_collected"),
        svg_chart("14. Recovery time vs database size", results, "dataset_records", "recovery_seconds_not_collected"),
        svg_chart("15. RSS vs time", results, "elapsed_seconds_not_collected", "memory.rss_mb"),
    ]
    html = "<!doctype html><meta charset=utf-8><title>NoidaDB benchmark</title><style>body{font:16px system-ui;max-width:800px;margin:auto}svg{width:100%;border:1px solid #ddd}.axis{stroke:#777;fill:none}.line{stroke:#1769aa;fill:none;stroke-width:2}circle{fill:#1769aa}</style><h1>NoidaDB infrastructure benchmark</h1><p>Raw results: <a href=results.jsonl>results.jsonl</a>. Null counters mean the platform did not permit collection; they are not zero.</p>" + "\n".join(charts)
    (run_dir / "report.html").write_text(html)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    p.add_argument("--output", type=Path, default=Path("benchmark-results"))
    p.add_argument("--binary", default="target/release/noida-db"); p.add_argument("--host", default="127.0.0.1"); p.add_argument("--port", type=int, default=16379)
    p.add_argument("--sizes", help="comma-separated record counts"); p.add_argument("--record-sizes", help="comma-separated bytes"); p.add_argument("--concurrency", help="comma-separated clients")
    p.add_argument("--operations", default="insert,read,update,delete,scan"); p.add_argument("--seconds", type=float, default=.5); p.add_argument("--server-cpus"); p.add_argument("--client-cpus")
    p.add_argument("--perf", action="store_true", help="attempt Linux perf counters; unavailable counters remain null"); p.add_argument("--no-start", action="store_true", help="use an already-running isolated Redis endpoint")
    args = p.parse_args()
    if args.seconds <= 0: p.error("--seconds must be positive")
    preset = FULL if args.profile == "full" else SMOKE
    parse = lambda value, default: [int(x) for x in value.split(",")] if value else default
    sizes, record_sizes, concurrencies = parse(args.sizes, preset["sizes"]), parse(args.record_sizes, preset["record_sizes"]), parse(args.concurrency, preset["concurrency"])
    if args.client_cpus and hasattr(os, "sched_setaffinity"):
        try: os.sched_setaffinity(0, cpu_set(args.client_cpus))
        except ValueError as exc: p.error(f"invalid --client-cpus: {exc}")
    run_dir = args.output / datetime.now().strftime("%Y%m%dT%H%M%SZ"); run_dir.mkdir(parents=True)
    meta = hardware(); meta.update(noidadb_version=command_version([args.binary, "version"]), git_revision=command_version(["git", "rev-parse", "HEAD"]), build_flags="release profile: opt-level=z, lto=true, codegen-units=1, panic=abort, strip=true", server_cpu_set=args.server_cpus, client_cpu_set=args.client_cpus)
    (run_dir / "environment.json").write_text(json.dumps(meta, indent=2) + "\n")
    process = launch(args, run_dir)
    try:
        wait_for_redis(args.host, args.port, process)
        results: list[dict[str, Any]] = []
        for records in sizes:
            for record_size in record_sizes:
                load(args.host, args.port, records, record_size)
                for op in args.operations.split(","):
                    for concurrency in concurrencies:
                        results.append(run_case(args, process.pid if process else None, records, record_size, concurrency, op, cold=False))
                        print(json.dumps(results[-1]), flush=True)
        with (run_dir / "results.jsonl").open("w") as f:
            for result in results: f.write(json.dumps(result) + "\n")
        # Redis writes its snapshot only on a clean shutdown, so storage must
        # be measured after the server has been asked to flush it.
        if process:
            process.send_signal(signal.SIGINT)
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
            process = None
        (run_dir / "storage.json").write_text(json.dumps({"database_bytes": directory_size(run_dir / "data"), "note": "Measured after clean shutdown and snapshot flush."}, indent=2) + "\n")
        report(run_dir, results)
        print(f"results written to {run_dir}")
    finally:
        if process:
            process.send_signal(signal.SIGINT)
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
