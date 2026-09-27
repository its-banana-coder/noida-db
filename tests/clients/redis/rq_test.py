"""RQ (the Python job queue) against noida-db: enqueue, run a worker, read the
results, failures, retries and scheduled jobs. Run through
tests/clients/redis/run.sh, which starts the server and sets NOIDA_REDIS_PORT.
"""

import os
import sys
import time

import redis
from rq import Queue, Retry, SimpleWorker
from rq.job import JobStatus

sys.path.insert(0, os.path.dirname(__file__))
import rq_jobs  # noqa: E402

PORT = int(os.environ["NOIDA_REDIS_PORT"])
failures = []
checks = 0


def check(name, got, want):
    global checks
    checks += 1
    if got != want:
        failures.append(f"{name}: got {got!r}, want {want!r}")


conn = redis.Redis(port=PORT)
conn.flushall()
q = Queue("default", connection=conn)

# enqueue and run
job = q.enqueue(rq_jobs.add, 2, 3)
check("queued", (job.get_status(), len(q)), (JobStatus.QUEUED, 1))
SimpleWorker([q], connection=conn).work(burst=True)
job.refresh()
check("finished", (job.get_status(), job.result), (JobStatus.FINISHED, 5))
check("queue drained", len(q), 0)
check("finished registry", job.id in q.finished_job_registry.get_job_ids(), True)

# a failing job
bad = q.enqueue(rq_jobs.boom)
SimpleWorker([q], connection=conn).work(burst=True)
bad.refresh()
check("failed", bad.get_status(), JobStatus.FAILED)
check("failed registry", bad.id in q.failed_job_registry.get_job_ids(), True)
check("exception recorded", "ValueError: boom" in (bad.latest_result().exc_string or ""), True)

# retries
retried = q.enqueue(rq_jobs.boom, retry=Retry(max=2))
for _ in range(3):
    SimpleWorker([q], connection=conn).work(burst=True)
retried.refresh()
check("retries exhausted", retried.get_status(), JobStatus.FAILED)

# several jobs come out in order
ids = [q.enqueue(rq_jobs.add, i, i).id for i in range(5)]
check("job ids in queue", q.job_ids, ids)
SimpleWorker([q], connection=conn).work(burst=True)
check("results", [q.fetch_job(i).result for i in ids], [0, 2, 4, 6, 8])

# scheduled job runs once its time arrives (the scheduler moves it to the queue)
from datetime import timedelta  # noqa: E402

sched = q.enqueue_in(timedelta(seconds=1), rq_jobs.add, 1, 1)
check("scheduled", sched.get_status(), JobStatus.SCHEDULED)
time.sleep(1.2)
w = SimpleWorker([q], connection=conn)
w.work(burst=True, with_scheduler=True)
sched.refresh()
check("scheduled job ran", (sched.get_status(), sched.result), (JobStatus.FINISHED, 2))

# dependencies
first = q.enqueue(rq_jobs.add, 1, 2)
second = q.enqueue(rq_jobs.add, 10, 20, depends_on=first)
check("deferred until dependency", second.get_status(), JobStatus.DEFERRED)
SimpleWorker([q], connection=conn).work(burst=True)
second.refresh()
check("dependency ran", (first.fetch_dependencies() if False else second.result), 30)

print(f"rq: {checks} checks, {len(failures)} failed")
for f in failures:
    print("  FAIL", f)
sys.exit(1 if failures else 0)
