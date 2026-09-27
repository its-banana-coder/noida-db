"""Task functions for celery_test.py (Celery imports them by name)."""
from celery import Celery
import os

app = Celery("noidadb_test", broker=f"redis://127.0.0.1:{os.environ['NOIDA_REDIS_PORT']}/0",
              backend=f"redis://127.0.0.1:{os.environ['NOIDA_REDIS_PORT']}/0")
app.conf.broker_connection_retry_on_startup = False


@app.task
def add(a, b):
    return a + b


@app.task
def boom():
    raise ValueError("boom")
