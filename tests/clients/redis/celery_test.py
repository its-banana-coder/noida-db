"""Celery (the Python task queue) against noida-db: broker and result backend
both on Redis. Run through tests/clients/redis/run.sh, which starts the
server, sets NOIDA_REDIS_PORT and starts a worker process.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from celery_tasks import add, boom  # noqa: E402

failures = []
checks = 0


def check(name, got, want):
    global checks
    checks += 1
    if got != want:
        failures.append(f"{name}: got {got!r}, want {want!r}")


result = add.delay(2, 3)
check("result", result.get(timeout=10), 5)
check("state", result.state, "SUCCESS")

failed = boom.delay()
try:
    failed.get(timeout=10)
    check("failure raised", "no error", "ValueError")
except ValueError as e:
    check("failure message", str(e), "boom")
check("failure state", failed.state, "FAILURE")

results = [add.delay(i, i) for i in range(5)]
check("several results", [r.get(timeout=10) for r in results], [0, 2, 4, 6, 8])

print(f"celery: {checks} checks, {len(failures)} failed")
for f in failures:
    print("  FAIL", f)
sys.exit(1 if failures else 0)
