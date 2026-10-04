"""Never-abort test harness: every case runs, failures are collected, and
report() exits non-zero if any failed."""
import os
import sys

RESULTS = []
PORT = int(os.environ.get("NOIDA_MYSQL_PORT", "3306"))


def case(name, fn, expect=None, expect_error=False, cmp=None):
    try:
        got = fn()
    except Exception as e:  # noqa: BLE001
        if expect_error:
            RESULTS.append(("PASS", name, ""))
        else:
            RESULTS.append(("FAIL", name, f"{type(e).__name__}: {str(e)[:300]}"))
        return None
    if expect_error:
        RESULTS.append(("FAIL", name, f"expected an error, got {got!r}"[:300]))
        return got
    if expect is not None and not (cmp(got, expect) if cmp else got == expect):
        RESULTS.append(("FAIL", name, f"got {got!r}, want {expect!r}"[:400]))
        return got
    RESULTS.append(("PASS", name, ""))
    return got


def report(title):
    fails = [r for r in RESULTS if r[0] == "FAIL"]
    print(f"{title}: {len(RESULTS) - len(fails)}/{len(RESULTS)} checks passed")
    for _, name, msg in fails:
        print(f"  FAIL {name}\n       {msg}")
    sys.exit(1 if fails else 0)
