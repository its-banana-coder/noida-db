"""Failure-path differential test: the same failing transactions against
noida-db and a real server, comparing every step's outcome -- rows, error
codes, and transaction state -- not just the happy path. An integration
test that passes on noida-db must not pass for a reason the real server
wouldn't give.

    python failure_diff.py mysql    NOIDA_PORT REF_PORT
    python failure_diff.py postgres NOIDA_PORT REF_PORT

Divergences listed in KNOWN are documented gaps (docs/LIMITATIONS.md):
they're printed every run but don't fail it, and a known gap that starts
matching is reported so the list can shrink.
"""
import sys

KIND, NOIDA_PORT, REF_PORT = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

if KIND == "mysql":
    import pymysql

    def connect(port):
        return pymysql.connect(host="127.0.0.1", port=port, user="root", password="", autocommit=True)

    def err_code(e):
        return e.args[0] if isinstance(e, pymysql.MySQLError) else type(e).__name__

    def tx_state(c):
        return "in_tx" if c.server_status & 1 else "idle"

    DB_SETUP = ["DROP DATABASE IF EXISTS fdiff", "CREATE DATABASE fdiff", "USE fdiff"]
else:
    import psycopg

    def connect(port):
        return psycopg.connect(f"host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres",
                               autocommit=True)

    def err_code(e):
        return getattr(e, "sqlstate", None) or type(e).__name__

    def tx_state(c):
        return c.info.transaction_status.name

    DB_SETUP = ["DROP SCHEMA IF EXISTS fdiff CASCADE", "CREATE SCHEMA fdiff", "SET search_path TO fdiff"]


def run(port, scenario):
    """Runs one scenario on one server, returning its trace."""
    conns = {}

    def conn(name):
        if name not in conns:
            conns[name] = connect(port)
            cur = conns[name].cursor()
            for s in DB_SETUP[2:]:  # select the scenario database/schema
                cur.execute(s)
        return conns[name]

    setup = connect(port)
    for s in DB_SETUP:
        setup.cursor().execute(s)
    trace = []
    for step in scenario:
        who, action = step[0], step[1]
        c = conn(who)
        try:
            if action == "state":
                trace.append((who, "state", tx_state(c)))
                continue
            if action == "autocommit":
                # pymysql's autocommit is a method; psycopg's is a property.
                if KIND == "mysql":
                    c.autocommit(step[2])
                else:
                    c.autocommit = step[2]
                trace.append((who, "autocommit", step[2]))
                continue
            if action in ("commit", "rollback"):
                getattr(c, action)()
                trace.append((who, action, "ok"))
                continue
            cur = c.cursor()
            cur.execute(action)
            rows = [tuple(r) for r in cur.fetchall()] if cur.description else None
            trace.append((who, action, ("rows", rows) if rows is not None else ("ok", cur.rowcount)))
        except Exception as e:  # noqa: BLE001
            trace.append((who, action, ("error", err_code(e))))
    for c in conns.values():
        c.close()
    setup.close()
    return trace


A, B = "a", "b"  # two connections

MYSQL = {
    "dup key inside a transaction keeps it usable": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(20) UNIQUE)"),
        (A, "INSERT INTO u VALUES (1, 'a')"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (2, 'b')"),
        (A, "INSERT INTO u VALUES (3, 'a')"),
        (A, "state"),
        (A, "INSERT INTO u VALUES (4, 'c')"),
        (A, "SELECT id FROM u ORDER BY id"),
        (A, "rollback"),
        (A, "state"),
        (B, "SELECT id FROM u ORDER BY id"),
    ],
    "commit after a failed statement keeps the rest": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (2)"),
        (A, "commit"),
        (B, "SELECT id FROM u ORDER BY id"),
    ],
    "multi-row insert with a dup is all-or-nothing": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (2), (3), (1)"),
        (A, "SELECT id FROM u ORDER BY id"),
    ],
    "update into a dup changes nothing": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY, k VARCHAR(5) UNIQUE)"),
        (A, "INSERT INTO u VALUES (1, 'a'), (2, 'b')"),
        (A, "UPDATE u SET k = 'a' WHERE id = 2"),
        (A, "UPDATE u SET k = 'z'"),
        (A, "SELECT id, k FROM u ORDER BY id"),
    ],
    "strict-mode value errors": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, n VARCHAR(5) NOT NULL, i INT, d DATE, e ENUM('x','y'), r INT NOT NULL)"),
        (A, "INSERT INTO t (id, n, r) VALUES (1, NULL, 0)"),
        (A, "INSERT INTO t (id, n) VALUES (1, 'a')"),
        (A, "INSERT INTO t (id, n, r) VALUES (1, 'toolong', 0)"),
        (A, "INSERT INTO t (id, n, i, r) VALUES (1, 'a', 'abc', 0)"),
        (A, "INSERT INTO t (id, n, e, r) VALUES (1, 'a', 'z', 0)"),
        (A, "INSERT INTO t (id, n, r) VALUES (1, 'a', 0)"),
        (A, "UPDATE t SET n = NULL"),
        (A, "SELECT id, n, i, d, e, r FROM t"),
    ],
    "out-of-range integer": [
        (A, "CREATE TABLE t (i INT, s SMALLINT, ti TINYINT)"),
        (A, "INSERT INTO t (i) VALUES (99999999999)"),
        (A, "INSERT INTO t (s) VALUES (40000)"),
        (A, "INSERT INTO t (ti) VALUES (300)"),
        (A, "SELECT COUNT(*) FROM t"),
    ],
    # WordPress-style sessions turn strict mode off: MySQL then adjusts
    # values instead of rejecting them.
    "non-strict sql_mode adjusts values": [
        (A, "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'"),
        (A, "SELECT @@sql_mode"),
        (A, "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, i INT, ti TINYINT UNSIGNED,"
            " v VARCHAR(3), d DATE, e ENUM('a','b'), n INT NOT NULL, x DECIMAL(4,1))"),
        (A, "INSERT INTO t (i, n) VALUES (99999999999, 1)"),
        (A, "INSERT INTO t (ti, n) VALUES (-5, 1)"),
        (A, "INSERT INTO t (v, n) VALUES ('abcdef', 1)"),
        (A, "INSERT INTO t (d, n) VALUES ('2024-13-45', 1)"),
        (A, "INSERT INTO t (e, n) VALUES ('zzz', 1)"),
        (A, "INSERT INTO t (i) VALUES (7)"),
        (A, "INSERT INTO t (x, n) VALUES (12345.67, 1)"),
        (A, "INSERT INTO t (i, n) VALUES (1/0, 1)"),
        (A, "SELECT id, i, ti, v, d, e, n, x FROM t ORDER BY id"),
        (A, "SET SESSION sql_mode = DEFAULT"),
        (A, "SELECT @@sql_mode"),
        (A, "INSERT INTO t (ti, n) VALUES (-5, 1)"),
    ],
    "invalid dates": [
        (A, "CREATE TABLE t (d DATE, ts DATETIME)"),
        (A, "INSERT INTO t (d) VALUES ('2024-13-45')"),
        (A, "INSERT INTO t (ts) VALUES ('not a date')"),
        (A, "INSERT INTO t (d) VALUES ('2024-02-29')"),
        (A, "SELECT d FROM t"),
    ],
    "unknown names and syntax errors": [
        (A, "CREATE TABLE t (id INT)"),
        (A, "SELECT nosuch FROM t"),
        (A, "SELECT * FROM nosuch"),
        (A, "SELEC 1"),
        (A, "CREATE TABLE t (id INT)"),
        (A, "DROP TABLE nosuch"),
        (A, "INSERT INTO t (nosuch) VALUES (1)"),
    ],
    "savepoint recovers part of a transaction": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "SAVEPOINT s1"),
        (A, "INSERT INTO u VALUES (2)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "ROLLBACK TO SAVEPOINT s1"),
        (A, "INSERT INTO u VALUES (3)"),
        (A, "commit"),
        (B, "SELECT id FROM u ORDER BY id"),
        (A, "ROLLBACK TO SAVEPOINT s1"),
    ],
    "division by zero": [
        (A, "SELECT 1/0, 5 % 0, MOD(5, 0)"),
        (A, "CREATE TABLE t (x INT)"),
        (A, "INSERT INTO t VALUES (1/0)"),
        (A, "SELECT x FROM t"),
    ],
    "commit and rollback with no transaction": [
        (A, "rollback"),
        (A, "commit"),
        (A, "state"),
    ],
    "auto_increment after a failed insert": [
        (A, "CREATE TABLE u (id INT AUTO_INCREMENT PRIMARY KEY, k VARCHAR(5) UNIQUE)"),
        (A, "INSERT INTO u (k) VALUES ('a')"),
        (A, "INSERT INTO u (k) VALUES ('a')"),
        (A, "INSERT INTO u (k) VALUES ('b')"),
        (A, "SELECT id, k FROM u ORDER BY id"),
    ],
    "foreign key enforcement": [
        (A, "CREATE TABLE p (id INT PRIMARY KEY)"),
        (A, "CREATE TABLE c (pid INT, FOREIGN KEY (pid) REFERENCES p (id))"),
        (A, "INSERT INTO c VALUES (1)"),
        (A, "SELECT COUNT(*) FROM c"),
    ],
    "uncommitted writes are invisible to other connections": [
        (A, "CREATE TABLE u (id INT PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (B, "SELECT COUNT(*) FROM u"),
        (A, "rollback"),
    ],
    "ONLY_FULL_GROUP_BY": [
        (A, "CREATE TABLE t (id INT, g INT)"),
        (A, "INSERT INTO t VALUES (1, 1), (2, 1)"),
        (A, "SELECT id, COUNT(*) FROM t GROUP BY g"),
    ],
}

POSTGRES = {
    "error aborts the transaction until rollback": [
        (A, "CREATE TABLE u (id int PRIMARY KEY, email text UNIQUE)"),
        (A, "INSERT INTO u VALUES (1, 'a')"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (2, 'b')"),
        (A, "INSERT INTO u VALUES (3, 'a')"),
        (A, "state"),
        (A, "SELECT 1"),
        (A, "state"),
        (A, "rollback"),
        (A, "state"),
        (B, "SELECT id FROM u ORDER BY id"),
    ],
    "savepoint recovers an aborted transaction": [
        (A, "CREATE TABLE u (id int PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "SAVEPOINT s1"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "state"),
        (A, "ROLLBACK TO SAVEPOINT s1"),
        (A, "state"),
        (A, "INSERT INTO u VALUES (2)"),
        (A, "commit"),
        (B, "SELECT id FROM u ORDER BY id"),
    ],
    "commit of an aborted transaction rolls back": [
        (A, "CREATE TABLE u (id int PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "commit"),
        (B, "SELECT count(*) FROM u"),
    ],
    "multi-row insert with a dup is all-or-nothing": [
        (A, "CREATE TABLE u (id int PRIMARY KEY)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (2), (3), (1)"),
        (A, "SELECT id FROM u ORDER BY id"),
    ],
    "constraint and value errors": [
        (A, "CREATE TABLE p (id int PRIMARY KEY)"),
        (A, "CREATE TABLE t (id int PRIMARY KEY, n varchar(5) NOT NULL, age int CHECK (age >= 0), pid int REFERENCES p (id))"),
        (A, "INSERT INTO t (id, n) VALUES (1, NULL)"),
        (A, "INSERT INTO t (id, n, age) VALUES (1, 'a', -1)"),
        (A, "INSERT INTO t (id, n, pid) VALUES (1, 'a', 99)"),
        (A, "INSERT INTO t (id, n) VALUES (1, 'toolong')"),
        (A, "INSERT INTO t (id, n) VALUES ('abc', 'a')"),
        (A, "INSERT INTO t (id, n) VALUES (99999999999, 'a')"),
        (A, "SELECT 1 / 0"),
        (A, "SELECT count(*) FROM t"),
    ],
    "unknown names and syntax errors": [
        (A, "CREATE TABLE t (id int)"),
        (A, "SELECT nosuch FROM t"),
        (A, "SELECT * FROM nosuch"),
        (A, "SELEC 1"),
        (A, "CREATE TABLE t (id int)"),
        (A, "SELECT nosuch_function(1)"),
        (A, "INSERT INTO t (nosuch) VALUES (1)"),
    ],
    "sequences are not rolled back": [
        (A, "CREATE TABLE u (id serial PRIMARY KEY, k text)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u (k) VALUES ('a') RETURNING id"),
        (A, "rollback"),
        (A, "INSERT INTO u (k) VALUES ('b') RETURNING id"),
        (A, "commit"),
    ],
    "uncommitted writes are invisible to other connections": [
        (A, "CREATE TABLE u (id int PRIMARY KEY)"),
        (A, "autocommit", False),
        (A, "INSERT INTO u VALUES (1)"),
        (B, "SELECT count(*) FROM u"),
        (A, "commit"),
        (B, "SELECT count(*) FROM u"),
    ],
    "autocommit error does not affect the next statement": [
        (A, "CREATE TABLE u (id int PRIMARY KEY)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "INSERT INTO u VALUES (1)"),
        (A, "state"),
        (A, "INSERT INTO u VALUES (2)"),
        (A, "SELECT id FROM u ORDER BY id"),
    ],
}

# Documented gaps (docs/LIMITATIONS.md): reported, not failed.
KNOWN = {
    "mysql": {
        "foreign key enforcement": "FOREIGN KEY accepted but not enforced",
        "uncommitted writes are invisible to other connections": "no isolation between MySQL connections",
        "ONLY_FULL_GROUP_BY": "ONLY_FULL_GROUP_BY is not enforced",
    },
    "postgres": {},
}

scenarios = MYSQL if KIND == "mysql" else POSTGRES
failed, known, fixed = [], [], []
for name, steps in scenarios.items():
    got, want = run(NOIDA_PORT, steps), run(REF_PORT, steps)
    same = got == want
    if name in KNOWN[KIND]:
        (fixed if same else known).append(name)
        continue
    if same:
        print(f"  match  {name}")
        continue
    failed.append(name)
    print(f"  DIFF   {name}")
    for g, w in zip(got, want):
        mark = "  " if g == w else "!!"
        print(f"      {mark} {w[0]}: {str(w[1])[:70]}")
        if g != w:
            print(f"           real server: {w[2]}\n           noida-db:    {g[2]}")
for name in known:
    print(f"  known  {name} ({KNOWN[KIND][name]})")
for name in fixed:
    print(f"  NOW MATCHES (remove from KNOWN): {name}")
print(f"{KIND} failure-path differential: {len(scenarios) - len(failed) - len(known) - len(fixed)} match, "
      f"{len(failed)} differ, {len(known)} known gaps")
sys.exit(1 if failed else 0)
