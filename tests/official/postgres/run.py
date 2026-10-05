"""Runs Postgres's own regression suite (src/test/regress, REL_14_STABLE)
against a server with psql, the way pg_regress does, and scores the
output against the suite's expected files.

    tests/official/postgres/fetch.sh
    python3 tests/official/postgres/run.py PORT [test ...] [--json PATH]

As pg_regress: tests run in schedule order (sequentially) in a fresh
`regression` database, with `psql -X -a -q`, PGTZ=PST8PDT and
PGDATESTYLE='Postgres, MDY', output compared with expected/<test>.out (or
its alternatives, best match). Server-side `COPY ... FROM/TO 'file'`
becomes psql's client-side `\\copy` (applied to every server alike), since
the server may not share a filesystem with the suite.

Scores: files whose output is identical, statements whose output block is
identical, and matching lines.
"""

import difflib
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.environ.get("PG_SUITE", os.path.join(HERE, "../../../target/official/postgres/regress"))
WORK = os.environ.get("PG_WORK", os.path.join(HERE, "../../../target/official/postgres/work"))
TIMEOUT = int(os.environ.get("PG_TEST_TIMEOUT", "180"))


def schedule():
    out = []
    with open(os.path.join(SUITE, "parallel_schedule")) as fh:
        for line in fh:
            if line.startswith("test:"):
                out += line.split(":", 1)[1].split()
    return out


def subst(text):
    return (
        text.replace("@abs_srcdir@", SUITE)
        .replace("@abs_builddir@", WORK)
        .replace("@testtablespace@", os.path.join(WORK, "testtablespace"))
        .replace("@libdir@", WORK)
        .replace("@DLSUFFIX@", ".so")
    )


COPY_FILE = re.compile(r"(?im)^(\s*)copy\s+(.+?)\s+(from|to)\s+('[^']*')(.*?);\s*$")


def to_client_copy(sql):
    # `COPY t FROM '/file'` -> `\copy t FROM '/file'` (one line, no `;`).
    return COPY_FILE.sub(lambda m: f"{m.group(1)}\\copy {m.group(2)} {m.group(3)} {m.group(4)}{m.group(5)}", sql)


def source_of(test):
    sql = os.path.join(SUITE, "sql", f"{test}.sql")
    if os.path.exists(sql):
        with open(sql) as fh:
            return fh.read()
    src = os.path.join(SUITE, "input", f"{test}.source")
    if os.path.exists(src):
        with open(src) as fh:
            return subst(fh.read())
    return None


def expected_of(test):
    outs = []
    d = os.path.join(SUITE, "expected")
    for f in sorted(os.listdir(d)):
        if re.fullmatch(rf"{re.escape(test)}(_\d+)?\.out", f):
            with open(os.path.join(d, f)) as fh:
                outs.append(fh.read())
    src = os.path.join(SUITE, "output", f"{test}.source")
    if os.path.exists(src):
        with open(src) as fh:
            outs.append(subst(fh.read()))
    return outs


def psql(port, db, sql, transform=True):
    if transform:
        sql = to_client_copy(sql)
    env = dict(os.environ, PGTZ="PST8PDT", PGDATESTYLE="Postgres, MDY", PGPASSWORD="postgres",
               PGAPPNAME="pg_regress", LC_MESSAGES="C", LANG="C", LC_ALL="C")
    try:
        p = subprocess.run(
            ["psql", "-X", "-a", "-q", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", "-d", db,
             "-v", "HIDE_TABLEAM=on", "-v", "HIDE_TOAST_COMPRESSION=on"],
            input=sql.encode(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
            timeout=TIMEOUT, cwd=WORK,
        )
        return p.stdout.decode("utf-8", "replace")
    except subprocess.TimeoutExpired as e:
        # What ran before the stuck statement still counts.
        out = (e.stdout or b"").decode("utf-8", "replace")
        return out + f"\n<<noida-runner: timed out after {TIMEOUT}s>>\n"


def untransform(out):
    # psql echoes the rewritten `\copy` line; show it as the original COPY
    # so it lines up with the expected file.
    return re.sub(r"(?m)^(\s*)\\copy (.+?) (from|to) ('[^']*')(.*)$", lambda m: f"{m.group(1)}COPY {m.group(2)} {m.group(3)} {m.group(4)}{m.group(5)};", out, flags=re.I)


def statement_blocks(text):
    """Splits psql -a output into blocks, one per echoed statement (a
    statement begins at a line that isn't a result line)."""
    blocks, cur = [], []
    for line in text.splitlines():
        result_line = (
            line.startswith(" ") or line.startswith("-") or line.startswith("(") or line.startswith("ERROR")
            or line.startswith("NOTICE") or line.startswith("DETAIL") or line.startswith("HINT")
            or line.startswith("LINE ") or line.startswith("WARNING") or line.startswith("CONTEXT")
            or line.startswith("QUERY") or line.strip().startswith("^") or line == "" or line.startswith("+")
            or "|" in line and not line.rstrip().endswith(";")
        )
        if not result_line and cur and not cur[-1].rstrip().endswith(("(", ",")) and (
            cur[-1].rstrip().endswith(";") or any(c.startswith(("(", "-", "ERROR")) for c in cur[1:])
        ):
            blocks.append(cur)
            cur = []
        cur.append(line)
    if cur:
        blocks.append(cur)
    return ["\n".join(b) for b in blocks]


def score(actual, expected):
    a, e = actual.splitlines(), expected.splitlines()
    sm = difflib.SequenceMatcher(None, e, a, autojunk=False)
    lines_ok = sum(b.size for b in sm.get_matching_blocks())
    eb, ab = statement_blocks(expected), statement_blocks(actual)
    sb = difflib.SequenceMatcher(None, eb, ab, autojunk=False)
    stmts_ok = sum(b.size for b in sb.get_matching_blocks())
    return {"identical": actual == expected, "lines": len(e), "lines_ok": lines_ok,
            "stmts": len(eb), "stmts_ok": stmts_ok}


def main():
    args = [a for a in sys.argv[1:]]
    json_out = None
    if "--json" in args:
        i = args.index("--json")
        json_out = args[i + 1]
        del args[i:i + 2]
    port, only = int(args[0]), args[1:]
    os.makedirs(WORK, exist_ok=True)
    os.makedirs(os.path.join(WORK, "results"), exist_ok=True)
    os.makedirs(os.path.join(WORK, "testtablespace"), exist_ok=True)
    psql(port, "postgres", "DROP DATABASE IF EXISTS regression;\nCREATE DATABASE regression;\n", False)
    report = {}
    tests = [t for t in schedule() if not only or t in only]
    totals = {"files": 0, "identical": 0, "lines": 0, "lines_ok": 0, "stmts": 0, "stmts_ok": 0}
    for t in tests:
        sql = source_of(t)
        exps = expected_of(t)
        if sql is None or not exps:
            continue
        out = untransform(psql(port, "regression", sql))
        with open(os.path.join(WORK, "results", f"{t}.out"), "w") as fh:
            fh.write(out)
        best = max((score(out, e) for e in exps), key=lambda s: (s["identical"], s["stmts_ok"] / max(s["stmts"], 1)))
        report[t] = best
        totals["files"] += 1
        totals["identical"] += best["identical"]
        for k in ("lines", "lines_ok", "stmts", "stmts_ok"):
            totals[k] += best[k]
        print(f"{t:28} {'same' if best['identical'] else 'diff'}  statements {best['stmts_ok']:5}/{best['stmts']:<5}"
              f" lines {best['lines_ok']:6}/{best['lines']}", flush=True)
    pct = lambda a, b: 100.0 * a / max(b, 1)  # noqa: E731
    print(f"\nfiles identical {totals['identical']}/{totals['files']} ({pct(totals['identical'], totals['files']):.1f}%)"
          f"  statements {totals['stmts_ok']}/{totals['stmts']} ({pct(totals['stmts_ok'], totals['stmts']):.1f}%)"
          f"  lines {totals['lines_ok']}/{totals['lines']} ({pct(totals['lines_ok'], totals['lines']):.1f}%)")
    if json_out:
        with open(json_out, "w") as fh:
            json.dump({"totals": totals, "tests": report}, fh, indent=1)


if __name__ == "__main__":
    main()
