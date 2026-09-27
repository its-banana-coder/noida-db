"""Job functions for rq_test.py (RQ imports them by name)."""


def add(a, b):
    return a + b


def boom():
    raise ValueError("boom")
