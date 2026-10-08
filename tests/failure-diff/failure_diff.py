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
import threading
import time

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
    pending = {}  # conn name -> (thread, result holder) for ("async", sql)
    for step in scenario:
        who, action = step[0], step[1]
        c = conn(who)
        if isinstance(action, tuple) and action[0] == "reconnect":
            # Ends the session (what lives only for it goes) and opens a new one.
            c.close()
            del conns[who]
            time.sleep(0.2)
            trace.append((who, "reconnect", "ok"))
            continue
        if isinstance(action, tuple) and action[0] == "sleep":
            time.sleep(action[1])
            continue
        if isinstance(action, tuple) and action[0] == "async":
            # Runs on its own thread, so a statement that blocks on a lock
            # doesn't stop the scenario; ("join",) collects its result.
            holder = {}

            def go(c=c, sql=action[1], holder=holder):
                try:
                    cur = c.cursor()
                    cur.execute(sql)
                    rows = [tuple(r) for r in cur.fetchall()] if cur.description else None
                    holder["r"] = ("rows", rows) if rows is not None else ("ok", cur.rowcount)
                except Exception as e:  # noqa: BLE001
                    holder["r"] = ("error", err_code(e))

            t = threading.Thread(target=go)
            t.start()
            pending[who] = (t, holder, action[1])
            continue
        if isinstance(action, tuple) and action[0] == "multi" and KIND == "mysql":
            # A connection with CLIENT_MULTI_STATEMENTS: every result set.
            mc = pymysql.connect(host="127.0.0.1", port=port, user="root", password="",
                                 autocommit=True, database="fdiff",
                                 client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
            sets = []
            try:
                cur = mc.cursor()
                cur.execute(action[1])
                while True:
                    sets.append([tuple(r) for r in cur.fetchall()] if cur.description else ("ok", cur.rowcount))
                    if not cur.nextset():
                        break
                trace.append((who, action[1], ("sets", sets)))
            except Exception as e:  # noqa: BLE001
                trace.append((who, action[1], ("sets", sets, "error", err_code(e))))
            mc.close()
            continue
        if isinstance(action, tuple) and action[0] == "join":
            t, holder, sql = pending.pop(who)
            t.join(timeout=30)
            trace.append((who, sql, holder.get("r", ("still blocked",))))
            continue
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
}

# Subqueries, UNION/INTERSECT/EXCEPT, derived tables, CTEs and INSERT ...
# SELECT: happy paths and their errors, compared row for row.
QSETUP = [
    (A, "CREATE TABLE c (id INT PRIMARY KEY, name VARCHAR(20), city VARCHAR(20))"),
    (A, "CREATE TABLE o (id INT PRIMARY KEY, cid INT, amount INT, status VARCHAR(10))"),
    (A, "INSERT INTO c VALUES (1,'ann','pune'),(2,'bob','noida'),(3,'cat','noida'),(4,'dan',NULL)"),
    (A, "INSERT INTO o VALUES (10,1,50,'paid'),(11,1,70,'open'),(12,2,20,'paid'),(13,9,99,'paid'),(14,NULL,5,'open')"),
]
MYSQL_QUERIES = {
    "derived tables": QSETUP + [
        (A, "SELECT d.cid, d.total FROM (SELECT cid, SUM(amount) AS total FROM o GROUP BY cid) AS d WHERE d.total > 30 ORDER BY d.cid"),
        (A, "SELECT x.a, x.b FROM (SELECT id, name FROM c) AS x (a, b) ORDER BY x.a DESC LIMIT 2"),
        (A, "SELECT c.name, t.n FROM c JOIN (SELECT cid, COUNT(*) AS n FROM o GROUP BY cid) t ON t.cid = c.id ORDER BY c.name"),
        (A, "SELECT * FROM (SELECT id, city FROM c WHERE city = 'noida') q ORDER BY id"),
        (A, "SELECT COUNT(*) FROM (SELECT DISTINCT city FROM c) z"),
        (A, "SELECT * FROM (SELECT id FROM c)"),
        (A, "SELECT nosuch FROM (SELECT id FROM c) d"),
    ],
    "scalar and correlated subqueries": QSETUP + [
        (A, "SELECT name, (SELECT COUNT(*) FROM o WHERE o.cid = c.id) AS n FROM c ORDER BY id"),
        (A, "SELECT id FROM o WHERE amount > (SELECT AVG(amount) FROM o) ORDER BY id"),
        (A, "SELECT (SELECT MAX(amount) FROM o)"),
        (A, "SELECT (SELECT amount FROM o WHERE id = 999)"),
        (A, "SELECT (SELECT amount FROM o)"),
        (A, "SELECT (SELECT id, amount FROM o WHERE id = 10)"),
        (A, "SELECT name FROM c WHERE (SELECT SUM(amount) FROM o WHERE o.cid = c.id) >= 20 ORDER BY name"),
    ],
    "IN and EXISTS subqueries": QSETUP + [
        (A, "SELECT name FROM c WHERE id IN (SELECT cid FROM o WHERE status = 'paid') ORDER BY name"),
        (A, "SELECT name FROM c WHERE id NOT IN (SELECT cid FROM o) ORDER BY name"),
        (A, "SELECT name FROM c WHERE id NOT IN (SELECT cid FROM o WHERE cid IS NOT NULL) ORDER BY name"),
        (A, "SELECT name FROM c WHERE EXISTS (SELECT 1 FROM o WHERE o.cid = c.id) ORDER BY name"),
        (A, "SELECT name FROM c WHERE NOT EXISTS (SELECT 1 FROM o WHERE o.cid = c.id AND o.status = 'open') ORDER BY name"),
        (A, "SELECT 5 IN (SELECT amount FROM o), 6 IN (SELECT amount FROM o), NULL IN (SELECT amount FROM o WHERE id = 0)"),
        (A, "SELECT id IN (SELECT cid FROM o) AS has FROM c ORDER BY id"),
        (A, "SELECT id FROM c WHERE id IN (SELECT cid, amount FROM o)"),
    ],
    "UNION, INTERSECT and EXCEPT": QSETUP + [
        (A, "SELECT city FROM c UNION SELECT status FROM o ORDER BY city"),
        (A, "SELECT cid FROM o UNION ALL SELECT id FROM c ORDER BY 1"),
        (A, "SELECT id, name FROM c WHERE id < 3 UNION SELECT id, status FROM o WHERE id > 12 ORDER BY id DESC LIMIT 3"),
        (A, "(SELECT id FROM c ORDER BY id DESC LIMIT 1) UNION (SELECT id FROM o ORDER BY id LIMIT 1) ORDER BY id"),
        (A, "SELECT id FROM c UNION SELECT id, name FROM c"),
        (A, "SELECT id FROM c INTERSECT SELECT cid FROM o ORDER BY id"),
        (A, "SELECT id FROM c EXCEPT SELECT cid FROM o ORDER BY id"),
        (A, "SELECT COUNT(*) FROM (SELECT cid FROM o UNION SELECT id FROM c) u"),
    ],
    "common table expressions": QSETUP + [
        (A, "WITH t AS (SELECT cid, SUM(amount) AS s FROM o GROUP BY cid) SELECT c.name, t.s FROM c JOIN t ON t.cid = c.id ORDER BY t.s DESC"),
        (A, "WITH a AS (SELECT id FROM c WHERE city = 'noida'), b AS (SELECT id FROM a WHERE id > 2) SELECT * FROM b"),
        (A, "WITH t (x) AS (SELECT amount FROM o) SELECT MIN(x), MAX(x) FROM t"),
        (A, "WITH t AS (SELECT 1 AS v) SELECT * FROM t AS l JOIN t AS r ON l.v = r.v"),
        (A, "WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10) SELECT SUM(i), COUNT(*) FROM n"),
        (A, "CREATE TABLE emp (id INT PRIMARY KEY, boss INT, name VARCHAR(10))"),
        (A, "INSERT INTO emp VALUES (1,NULL,'ceo'),(2,1,'cto'),(3,2,'dev'),(4,2,'ops'),(5,3,'intern')"),
        (A, "WITH RECURSIVE chain AS (SELECT id, name, 0 AS depth FROM emp WHERE id = 1 UNION ALL SELECT e.id, e.name, chain.depth + 1 FROM emp e JOIN chain ON e.boss = chain.id) SELECT name, depth FROM chain ORDER BY depth, name"),
        (A, "WITH RECURSIVE r (i) AS (SELECT 1 UNION SELECT 1 FROM r) SELECT * FROM r"),
        (A, "WITH RECURSIVE r (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r) SELECT COUNT(*) FROM r"),
    ],
    "INSERT ... SELECT and DML with subqueries": QSETUP + [
        (A, "CREATE TABLE arch (id INT PRIMARY KEY, amount INT)"),
        (A, "INSERT INTO arch SELECT id, amount FROM o WHERE status = 'paid'"),
        (A, "INSERT INTO arch (id, amount) SELECT id + 100, amount * 2 FROM o WHERE amount > 60"),
        (A, "INSERT INTO arch SELECT id, amount FROM o"),
        (A, "INSERT INTO arch SELECT id FROM o"),
        (A, "SELECT * FROM arch ORDER BY id"),
        (A, "UPDATE o SET status = 'vip' WHERE cid IN (SELECT id FROM c WHERE city = 'pune')"),
        (A, "DELETE FROM o WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.id = o.cid)"),
        (A, "SELECT id, status FROM o ORDER BY id"),
    ],
    "subqueries in other clauses": QSETUP + [
        (A, "SELECT cid, SUM(amount) s FROM o GROUP BY cid HAVING SUM(amount) > (SELECT AVG(amount) FROM o) ORDER BY cid"),
        (A, "SELECT name FROM c ORDER BY (SELECT COUNT(*) FROM o WHERE o.cid = c.id) DESC, name"),
        (A, "SELECT * FROM (SELECT c.id, c.name, o.amount FROM c JOIN o ON o.cid = c.id) j ORDER BY amount"),
        (A, "ALTER TABLE c ADD COLUMN spent INT NOT NULL DEFAULT 0"),
        (A, "UPDATE c SET spent = (SELECT COALESCE(SUM(amount), 0) FROM o WHERE o.cid = c.id)"),
        (A, "SELECT id, spent FROM c ORDER BY id"),
        (A, "SELECT id FROM c WHERE id = (SELECT MAX(cid) FROM o WHERE cid < (SELECT COUNT(*) FROM c))"),
        (A, "WITH t AS (SELECT 1 AS x) SELECT (WITH t AS (SELECT 2 AS x) SELECT x FROM t) AS inner_x, x FROM t"),
        (A, "SELECT c.name FROM c WHERE c.id IN (SELECT o.cid FROM o WHERE o.amount > (SELECT MIN(o2.amount) FROM o o2 WHERE o2.cid = c.id)) ORDER BY 1"),
        (A, "SELECT COUNT(*) FROM c WHERE city IN (SELECT city FROM c WHERE id > 1)"),
    ],
    "affected rows count changed rows": QSETUP + [
        (A, "UPDATE c SET city = 'noida' WHERE id <= 3"),
        (A, "UPDATE c SET city = city"),
        (A, "CREATE TABLE kv (k INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO kv VALUES (1, 1)"),
        (A, "INSERT INTO kv VALUES (1, 1) ON DUPLICATE KEY UPDATE v = 1"),
        (A, "INSERT INTO kv VALUES (1, 1) ON DUPLICATE KEY UPDATE v = 2"),
        (A, "SELECT * FROM kv"),
    ],
    "ALL, ANY and SOME": QSETUP + [
        (A, "SELECT id FROM o WHERE amount > ALL (SELECT amount FROM o WHERE status = 'open') ORDER BY id"),
        (A, "SELECT id FROM o WHERE amount < ANY (SELECT amount FROM o WHERE status = 'paid') ORDER BY id"),
        (A, "SELECT id FROM o WHERE amount = SOME (SELECT amount FROM o WHERE cid = 1) ORDER BY id"),
        (A, "SELECT 1 > ALL (SELECT amount FROM o WHERE id = 0), 1 > ANY (SELECT amount FROM o WHERE id = 0)"),
        (A, "SELECT id FROM c WHERE id <> ALL (SELECT cid FROM o) ORDER BY id"),
        (A, "SELECT 100 > ALL (SELECT cid FROM o), 0 = ANY (SELECT cid FROM o)"),
    ],
    "window functions": QSETUP + [
        (A, "SELECT id, cid, amount, ROW_NUMBER() OVER (PARTITION BY cid ORDER BY amount DESC) AS rn FROM o ORDER BY id"),
        (A, "SELECT id, status, RANK() OVER w, DENSE_RANK() OVER w, PERCENT_RANK() OVER w, CUME_DIST() OVER w FROM o WINDOW w AS (ORDER BY status) ORDER BY id"),
        (A, "SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running, SUM(amount) OVER () AS total, AVG(amount) OVER (PARTITION BY status) AS avg_st FROM o ORDER BY id"),
        (A, "SELECT id, LAG(amount) OVER (ORDER BY id), LEAD(amount, 2, -1) OVER (ORDER BY id), FIRST_VALUE(amount) OVER (ORDER BY id), LAST_VALUE(amount) OVER (ORDER BY id) FROM o ORDER BY id"),
        (A, "SELECT id, LAST_VALUE(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), NTH_VALUE(amount, 2) OVER (ORDER BY id) FROM o ORDER BY id"),
        (A, "SELECT id, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), COUNT(*) OVER (ORDER BY id ROWS 2 PRECEDING), MAX(amount) OVER (ORDER BY status RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM o ORDER BY id"),
        (A, "SELECT id, NTILE(2) OVER (ORDER BY id), NTILE(3) OVER (ORDER BY id) FROM o ORDER BY id"),
        (A, "SELECT status, SUM(amount) AS s, RANK() OVER (ORDER BY SUM(amount) DESC) AS r, SUM(SUM(amount)) OVER () AS grand FROM o GROUP BY status ORDER BY r"),
        (A, "SELECT name, city, COUNT(*) OVER (PARTITION BY city) FROM c ORDER BY name"),
        (A, "SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM o ORDER BY rn LIMIT 2"),
        (A, "SELECT * FROM (SELECT id, cid, ROW_NUMBER() OVER (PARTITION BY cid ORDER BY amount DESC) AS rn FROM o) t WHERE rn = 1 ORDER BY id"),
        (A, "WITH m AS (SELECT status, SUM(amount) AS rev FROM o GROUP BY status) SELECT status, ROUND(rev / NULLIF(SUM(rev) OVER (), 0) * 100, 4) AS pct FROM m ORDER BY rev DESC"),
        (A, "SELECT id FROM o WHERE ROW_NUMBER() OVER () > 1"),
        (A, "SELECT id, RANK() OVER nosuch FROM o"),
        (A, "SELECT cid, COUNT(*) FROM o GROUP BY cid HAVING ROW_NUMBER() OVER () > 0"),
    ],
    "foreign keys: restrict, cascade, set null": [
        (A, "CREATE TABLE p (id INT PRIMARY KEY, code VARCHAR(5) UNIQUE)"),
        (A, "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p (id))"),
        (A, "CREATE TABLE cc (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_cc FOREIGN KEY (pid) REFERENCES p (id) ON DELETE CASCADE ON UPDATE CASCADE)"),
        (A, "CREATE TABLE cn (id INT PRIMARY KEY, pcode VARCHAR(5), FOREIGN KEY (pcode) REFERENCES p (code) ON DELETE SET NULL)"),
        (A, "INSERT INTO p VALUES (1,'a'),(2,'b'),(3,'c')"),
        (A, "INSERT INTO c VALUES (1, 9)"),
        (A, "INSERT INTO c VALUES (1, 1), (2, NULL)"),
        (A, "INSERT INTO cc VALUES (1, 2), (2, 2), (3, 3)"),
        (A, "INSERT INTO cn VALUES (1, 'B'), (2, 'c')"),
        (A, "DELETE FROM p WHERE id = 1"),
        (A, "UPDATE p SET id = 10 WHERE id = 1"),
        (A, "UPDATE c SET pid = 7 WHERE id = 1"),
        (A, "UPDATE p SET id = 20 WHERE id = 2"),
        (A, "SELECT * FROM cc ORDER BY id"),
        (A, "DELETE FROM p WHERE id = 3"),
        (A, "SELECT * FROM cc ORDER BY id"),
        (A, "SELECT * FROM cn ORDER BY id"),
        (A, "SELECT id FROM p ORDER BY id"),
        (A, "REPLACE INTO p VALUES (20, 'b')"),
        (A, "SELECT * FROM cc ORDER BY id"),
        (A, "TRUNCATE TABLE p"),
        (A, "DROP TABLE p"),
        (A, "DROP TABLE c, cc, cn, p"),
    ],
    "foreign keys: checks off, DDL and introspection": [
        (A, "SET FOREIGN_KEY_CHECKS = 0"),
        (A, "SELECT @@foreign_key_checks"),
        (A, "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p (id))"),
        (A, "INSERT INTO c VALUES (1, 5)"),
        (A, "CREATE TABLE p (id INT PRIMARY KEY)"),
        (A, "SET FOREIGN_KEY_CHECKS = 1"),
        (A, "INSERT INTO c VALUES (2, 5)"),
        (A, "UPDATE c SET id = 3 WHERE id = 1"),
        (A, "INSERT INTO p VALUES (5)"),
        (A, "INSERT INTO c VALUES (2, 5)"),
        (A, "SHOW CREATE TABLE c"),
        (A, "CREATE TABLE d (id INT PRIMARY KEY, pid INT)"),
        (A, "INSERT INTO d VALUES (1, 99)"),
        (A, "ALTER TABLE d ADD CONSTRAINT fk_d FOREIGN KEY (pid) REFERENCES p (id)"),
        (A, "UPDATE d SET pid = 5"),
        (A, "ALTER TABLE d ADD CONSTRAINT fk_d FOREIGN KEY (pid) REFERENCES p (id) ON DELETE CASCADE"),
        (A, "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY 1"),
        (A, "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE, TABLE_NAME, REFERENCED_TABLE_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() ORDER BY 1"),
        (A, "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'd' ORDER BY 1"),
        (A, "DELETE FROM p"),
        (A, "ALTER TABLE c DROP FOREIGN KEY c_ibfk_1"),
        (A, "ALTER TABLE c DROP FOREIGN KEY nosuch"),
        (A, "DELETE FROM p"),
        (A, "SELECT COUNT(*) FROM d"),
        (A, "CREATE TABLE e (pid INT, FOREIGN KEY (pid) REFERENCES nosuch (id))"),
        (A, "CREATE TABLE e (pid INT, FOREIGN KEY (pid) REFERENCES p (nosuch))"),
        (A, "CREATE TABLE e (pid INT NOT NULL, FOREIGN KEY (pid) REFERENCES p (id) ON DELETE SET NULL)"),
    ],
    "foreign keys: self reference and transactions": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, parent INT, FOREIGN KEY (parent) REFERENCES t (id) ON DELETE CASCADE)"),
        (A, "INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, NULL)"),
        (A, "INSERT INTO t VALUES (5, 42)"),
        (A, "INSERT INTO t VALUES (5, 5)"),
        (A, "CREATE TABLE note (id INT PRIMARY KEY, tid INT, FOREIGN KEY (tid) REFERENCES t (id) ON DELETE CASCADE)"),
        (A, "INSERT INTO note VALUES (1, 3), (2, 4)"),
        (A, "autocommit", False),
        (A, "DELETE FROM t WHERE id = 1"),
        (A, "SELECT id FROM t ORDER BY id"),
        (A, "SELECT id FROM note ORDER BY id"),
        (A, "rollback"),
        (A, "SELECT id FROM t ORDER BY id"),
        (A, "SELECT id FROM note ORDER BY id"),
        (A, "DELETE FROM t WHERE id = 1"),
        (A, "commit"),
        (B, "SELECT id FROM t ORDER BY id"),
        (B, "SELECT id FROM note ORDER BY id"),
    ],
    "foreign keys: column DDL": [
        (A, "CREATE TABLE p (id INT PRIMARY KEY, x INT)"),
        (A, "CREATE TABLE c (id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p (id))"),
        (A, "ALTER TABLE c DROP COLUMN pid"),
        (A, "ALTER TABLE p DROP COLUMN id"),
        (A, "ALTER TABLE c RENAME COLUMN pid TO parent"),
        (A, "ALTER TABLE p RENAME COLUMN id TO pk"),
        (A, "SHOW CREATE TABLE c"),
        (A, "INSERT INTO c VALUES (1, 1)"),
        (A, "ALTER TABLE p RENAME TO parent_t"),
        (A, "SHOW CREATE TABLE c"),
        (A, "ALTER TABLE c DROP FOREIGN KEY fk"),
        (A, "ALTER TABLE c DROP COLUMN parent"),
    ],
    "ONLY_FULL_GROUP_BY": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, u INT NOT NULL UNIQUE, nu INT UNIQUE)"),
        (A, "CREATE TABLE s (id INT PRIMARY KEY, tid INT, v INT)"),
        (A, "INSERT INTO t VALUES (1, 1, 10, 1, 1), (2, 1, 20, 2, NULL), (3, 2, 30, 3, 3)"),
        (A, "INSERT INTO s VALUES (1, 1, 5), (2, 1, 6)"),
        (A, "SELECT a, b FROM t GROUP BY a"),
        (A, "SELECT a, COUNT(*) FROM t"),
        (A, "SELECT id, b FROM t GROUP BY id"),
        (A, "SELECT u, b FROM t GROUP BY u"),
        (A, "SELECT nu, b FROM t GROUP BY nu"),
        (A, "SELECT a, b FROM t WHERE b = 1 GROUP BY a"),
        (A, "SELECT a, COUNT(*) FROM t WHERE a = 1"),
        (A, "SELECT a FROM t GROUP BY a ORDER BY b"),
        (A, "SELECT a+1, COUNT(*) FROM t GROUP BY a+1"),
        (A, "SELECT a+1 FROM t GROUP BY a"),
        (A, "SELECT ANY_VALUE(b), a FROM t GROUP BY a"),
        (A, "SELECT t.id, s.v FROM t JOIN s ON s.id = t.id GROUP BY t.id"),
        (A, "SELECT t.id, s.v FROM t JOIN s ON s.tid = t.id GROUP BY t.id"),
        (A, "SELECT s.id, t.b FROM t JOIN s ON s.tid = t.id GROUP BY s.id"),
        (A, "SELECT a AS x, COUNT(*) FROM t GROUP BY x"),
        (A, "SELECT a, b FROM t GROUP BY a, b ORDER BY COUNT(*)"),
        (A, "SELECT DISTINCT a FROM t ORDER BY b"),
        (A, "SELECT DISTINCT a, b FROM t ORDER BY b DESC, a"),
        (A, "SELECT a, b FROM t WHERE a = b GROUP BY a"),
        (A, "SELECT COUNT(*) FROM t ORDER BY b"),
        (A, "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY MAX(b)"),
        (A, "SELECT t.*, COUNT(*) FROM t GROUP BY t.id"),
        (A, "SELECT * FROM t GROUP BY a"),
        (A, "SELECT x.a, x.b FROM t AS x GROUP BY x.a"),
        (A, "SELECT a, RANK() OVER (ORDER BY b) FROM t GROUP BY a"),
        (A, "SELECT a, SUM(b), RANK() OVER (ORDER BY SUM(b)) FROM t GROUP BY a ORDER BY a"),
        (A, "SELECT a, b AS bb FROM t HAVING bb > 15 ORDER BY a, bb"),
        (A, "SELECT a FROM t HAVING COUNT(*) > 1"),
        (A, "SET SESSION sql_mode = ''"),
        (A, "SELECT a, b FROM t GROUP BY a ORDER BY a"),
        (A, "SELECT a, COUNT(*) FROM t"),
    ],
    "isolation: reads see committed data only": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO t VALUES (1, 10), (2, 20)"),
        (A, "autocommit", False),
        (A, "INSERT INTO t VALUES (3, 30)"),
        (A, "UPDATE t SET v = 11 WHERE id = 1"),
        (A, "DELETE FROM t WHERE id = 2"),
        (B, "SELECT id, v FROM t ORDER BY id"),
        (A, "SELECT id, v FROM t ORDER BY id"),
        (B, "SELECT COUNT(*), SUM(v) FROM t"),
        (A, "commit"),
        (B, "SELECT id, v FROM t ORDER BY id"),
    ],
    "isolation: a write waits for the row's transaction": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO t VALUES (1, 10), (2, 20)"),
        (A, "autocommit", False),
        (A, "UPDATE t SET v = v + 1 WHERE id = 1"),
        (B, "UPDATE t SET v = v + 100 WHERE id = 2"),
        (B, ("async", "UPDATE t SET v = v * 2 WHERE id = 1")),
        (A, ("sleep", 0.5)),
        (A, "commit"),
        (B, ("join",)),
        (A, "SELECT id, v FROM t ORDER BY id"),
    ],
    "isolation: lock wait timeout": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO t VALUES (1, 10)"),
        (B, "SET SESSION innodb_lock_wait_timeout = 1"),
        (A, "autocommit", False),
        (A, "UPDATE t SET v = 11 WHERE id = 1"),
        (B, "UPDATE t SET v = 12 WHERE id = 1"),
        (B, "DELETE FROM t WHERE id = 1"),
        (A, "rollback"),
        (B, "UPDATE t SET v = 12 WHERE id = 1"),
        (A, "SELECT v FROM t"),
    ],
    "isolation: an uncommitted duplicate key waits": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "autocommit", False),
        (A, "INSERT INTO t VALUES (5, 1)"),
        (B, ("async", "INSERT INTO t VALUES (5, 2)")),
        (A, ("sleep", 0.5)),
        (A, "rollback"),
        (B, ("join",)),
        (A, "INSERT INTO t VALUES (6, 1)"),
        (B, ("async", "INSERT INTO t VALUES (6, 2)")),
        (A, ("sleep", 0.5)),
        (A, "commit"),
        (B, ("join",)),
        (A, "SELECT id, v FROM t ORDER BY id"),
    ],
    "isolation: SKIP LOCKED and NOWAIT job queue": [
        (A, "CREATE TABLE jobs (id INT PRIMARY KEY, done INT NOT NULL DEFAULT 0)"),
        (A, "INSERT INTO jobs (id) VALUES (1), (2), (3)"),
        (A, "autocommit", False),
        (B, "autocommit", False),
        (A, "SELECT id FROM jobs WHERE done = 0 ORDER BY id LIMIT 1 FOR UPDATE"),
        (B, "SELECT id FROM jobs WHERE done = 0 ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"),
        (B, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT"),
        (B, "SELECT id FROM jobs WHERE id = 1 FOR SHARE NOWAIT"),
        (B, "SELECT id FROM jobs WHERE done = 0 ORDER BY id FOR UPDATE SKIP LOCKED"),
        (A, "UPDATE jobs SET done = 1 WHERE id = 1"),
        (A, "commit"),
        (B, "UPDATE jobs SET done = 1 WHERE id = 2"),
        (B, "commit"),
        (A, "SELECT id, done FROM jobs ORDER BY id"),
    ],
    "isolation: deadlock": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO t VALUES (1, 0), (2, 0)"),
        (A, "autocommit", False),
        (B, "autocommit", False),
        (A, "UPDATE t SET v = 1 WHERE id = 1"),
        (B, "UPDATE t SET v = 2 WHERE id = 2"),
        (B, ("async", "UPDATE t SET v = 2 WHERE id = 1")),
        (A, ("sleep", 0.5)),
        (A, "UPDATE t SET v = 1 WHERE id = 2"),
        (B, ("join",)),
        (A, "commit"),
        (B, "commit"),
        (A, "SELECT id, v FROM t ORDER BY id"),
    ],
    "REGEXP functions": [
        (A, "SELECT 'abc' REGEXP '^a', 'abc' RLIKE 'x', 'abc' NOT REGEXP 'b', REGEXP_LIKE('Abc','abc'), REGEXP_LIKE('Abc','abc','c')"),
        (A, "SELECT REGEXP_REPLACE('a1b22','[0-9]+','#'), REGEXP_SUBSTR('a1b22','[0-9]+',1,2), REGEXP_INSTR('a1b22','[0-9]+'), REGEXP_REPLACE('aaa','a','b',2,1)"),
        (A, "SELECT NULL REGEXP 'a', 'x.y' REGEXP '\\\\.', 'ABC' REGEXP '[[:lower:]]'"),
        (A, "CREATE TABLE r (s VARCHAR(20))"),
        (A, "INSERT INTO r VALUES ('apple'), ('Banana'), ('cherry'), (NULL)"),
        (A, "SELECT s FROM r WHERE s REGEXP '^[ab]' ORDER BY s"),
        (A, "SELECT 'a' REGEXP '('"),
    ],
    "JSON modification functions": [
        (A, """SELECT JSON_SET('{"a":1}','$.b',2,'$.a',3), JSON_INSERT('{"a":1}','$.a',9,'$.c','x'), JSON_REPLACE('{"a":1}','$.a',5,'$.z',1)"""),
        (A, """SELECT JSON_REMOVE('[1,2,3]','$[1]'), JSON_ARRAY_APPEND('{"a":[1]}','$.a',2), JSON_ARRAY_INSERT('[1,3]','$[1]',2), JSON_ARRAY_APPEND('{"a":1}','$.a',2)"""),
        (A, """SELECT JSON_EXTRACT('{"a":[{"b":1},{"b":2}]}','$.a[*].b'), JSON_EXTRACT('{"a":{"b":{"c":5}}}','$**.c'), JSON_EXTRACT('[1,2,3]','$[*]')"""),
        (A, """SELECT JSON_MERGE_PATCH('{"a":1,"b":2}','{"b":null,"c":3}'), JSON_MERGE_PRESERVE('[1]','[2]'), JSON_MERGE_PRESERVE('{"a":1}','{"a":2}')"""),
        (A, """SELECT JSON_SET('{"a":{"b":1}}','$.a.c.d',1), JSON_SET('[1]','$[5]',9), JSON_REMOVE('{"a":1}','$'), JSON_SET(NULL,'$.a',1)"""),
        (A, "CREATE TABLE j (id INT PRIMARY KEY, doc JSON)"),
        (A, """INSERT INTO j VALUES (1, '{"tags":["a"],"n":1}')"""),
        (A, """UPDATE j SET doc = JSON_SET(doc, '$.n', JSON_EXTRACT(doc,'$.n') + 1, '$.tags', JSON_ARRAY_APPEND(doc->'$.tags','$','b'))"""),
        (A, "SELECT doc, doc->>'$.tags[1]', JSON_LENGTH(doc->'$.tags') FROM j"),
    ],
    "multi-table UPDATE and DELETE": [
        (A, "CREATE TABLE c (id INT PRIMARY KEY, tier VARCHAR(5))"),
        (A, "CREATE TABLE o (id INT PRIMARY KEY, cid INT, amount INT, flag INT DEFAULT 0)"),
        (A, "INSERT INTO c VALUES (1,'gold'),(2,'basic'),(3,'gold')"),
        (A, "INSERT INTO o (id, cid, amount) VALUES (10,1,50),(11,1,70),(12,2,20),(13,9,5)"),
        (A, "UPDATE o JOIN c ON c.id = o.cid SET o.flag = 1 WHERE c.tier = 'gold'"),
        (A, "UPDATE o, c SET o.amount = o.amount * 2, c.tier = 'vip' WHERE c.id = o.cid AND o.amount > 60"),
        (A, "SELECT * FROM o ORDER BY id"),
        (A, "SELECT * FROM c ORDER BY id"),
        (A, "UPDATE o LEFT JOIN c ON c.id = o.cid SET o.flag = 2 WHERE c.id IS NULL"),
        (A, "DELETE o FROM o JOIN c ON c.id = o.cid WHERE c.tier = 'basic'"),
        (A, "DELETE o, c FROM o JOIN c ON c.id = o.cid WHERE o.amount > 100"),
        (A, "DELETE FROM o USING o LEFT JOIN c ON c.id = o.cid WHERE c.id IS NULL"),
        (A, "SELECT * FROM o ORDER BY id"),
        (A, "SELECT * FROM c ORDER BY id"),
        (A, "UPDATE o JOIN c ON c.id = o.cid SET nosuch = 1"),
    ],
    "multiple statements in one query": [
        (A, "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)"),
        (A, "SELECT 1; SELECT 2"),
        (A, ("multi", "SELECT 1; SELECT 2, 3")),
        (A, ("multi", "INSERT INTO t (v) VALUES (5); SELECT LAST_INSERT_ID(); UPDATE t SET v = 6")),
        (A, ("multi", "INSERT INTO t (v) VALUES (7); INSERT INTO t (id, v) VALUES (1, 0); INSERT INTO t (v) VALUES (8)")),
        (A, ("multi", "SELECT v FROM t ORDER BY id;")),
        (A, ("multi", "SET @x = 1; SELECT @x + 1")),
    ],
    "user variables": [
        (A, "SELECT @nosuch"),
        (A, "SET @a = 5"),
        (A, "SET @b = @a * 2, @c = CONCAT('x', @a)"),
        (A, "SELECT @a, @b, @c, @A"),
        (A, "SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS = 0"),
        (A, "SELECT @OLD_FOREIGN_KEY_CHECKS, @@foreign_key_checks"),
        (A, "SET FOREIGN_KEY_CHECKS = @OLD_FOREIGN_KEY_CHECKS"),
        (A, "SELECT @@foreign_key_checks"),
        (A, "CREATE TABLE t (id INT PRIMARY KEY, v INT)"),
        (A, "INSERT INTO t VALUES (1, 10), (2, 20)"),
        (A, "SET @m = (SELECT MAX(v) FROM t)"),
        (A, "SELECT id FROM t WHERE v = @m"),
        (A, "UPDATE t SET v = @m + 1 WHERE id = 1"),
        (A, "SELECT v FROM t ORDER BY id"),
    ],
    "rails migrations: indexes, renames, bare keywords": [
        # CURRENT_USER is left out: the account's host differs by environment.
        (A, "SELECT DATABASE(), CURRENT_USER = CURRENT_USER(), current_date = curdate(), curtime() = current_time"),
        (A, "SELECT nosuchcol"),
        (A, "CREATE TABLE ra (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, a int, b varchar(10), KEY index_ra_on_a (a), INDEX (b), KEY (b))"),
        (A, "CREATE INDEX index_ra_on_a_b ON ra (a, b)"),
        (A, "CREATE INDEX index_ra_on_a_b ON ra (b)"),
        (A, "CREATE UNIQUE INDEX uq_ab ON ra (a, b)"),
        (A, "ALTER TABLE ra ADD INDEX idx_x (b, a), ADD KEY (a)"),
        (A, "SELECT index_name, non_unique, column_name, seq_in_index FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND table_name = 'ra' ORDER BY index_name, seq_in_index"),
        (A, "INSERT INTO ra (a, b) VALUES (1, 'x'), (2, 'y')"),
        (A, "SHOW CREATE TABLE ra"),
        (A, "RENAME TABLE ra TO rb"),
        (A, "ALTER TABLE rb RENAME INDEX index_ra_on_a TO index_rb_on_a, RENAME KEY index_ra_on_a_b TO index_rb_on_a_b"),
        (A, "ALTER TABLE rb RENAME INDEX nope TO x"),
        (A, "ALTER TABLE rb RENAME INDEX index_rb_on_a TO idx_x"),
        (A, "DROP INDEX index_rb_on_a ON rb"),
        (A, "ALTER TABLE rb DROP INDEX b"),
        (A, "ALTER TABLE rb DROP INDEX nope"),
        (A, "DROP INDEX nope ON rb"),
        (A, "ALTER TABLE rb DROP COLUMN b"),
        (A, "INSERT INTO rb (a) VALUES (3)"),
        (A, "SELECT id, a FROM rb ORDER BY id"),
        (A, "SHOW CREATE TABLE rb"),
        (A, "RENAME TABLE rb TO rc, rc TO rd"),
        (A, "CREATE TABLE d (id int AUTO_INCREMENT PRIMARY KEY, a int, b int, UNIQUE KEY uab (a, b))"),
        (A, "INSERT INTO d (a, b) VALUES (1, 1), (1, 2)"),
        (A, "ALTER TABLE d DROP COLUMN b"),
    ],
    "joins without ON, recursion limits": [
        (A, "SELECT count(*) FROM (SELECT 1 a UNION ALL SELECT 2) x JOIN (SELECT 1 b UNION ALL SELECT 2 UNION ALL SELECT 3) y"),
        (A, "SELECT x.a, y.b FROM (SELECT 1 a UNION ALL SELECT 2) x JOIN (SELECT 5 b) y WHERE x.a = 2"),
        (A, "SELECT count(*) FROM (SELECT 1 a) x STRAIGHT_JOIN (SELECT 2 b) y"),
        (A, "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000) SELECT count(*) FROM t"),
        (A, "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT count(*) FROM t"),
    ],
    "views": [
        (A, "CREATE TABLE vt (id int PRIMARY KEY, a int, b varchar(10) NOT NULL, p decimal(8,2))"),
        (A, "INSERT INTO vt VALUES (1, 10, 'x', 1.5), (2, 20, 'y', 2.25), (3, 30, 'z', NULL)"),
        (A, "CREATE VIEW v1 AS SELECT id, a * 2 AS a2 FROM vt WHERE a > 10"),
        (A, "SELECT * FROM v1 ORDER BY id"),
        (A, "CREATE VIEW v2 (k, total) AS SELECT b, sum(a) FROM vt GROUP BY b"),
        (A, "SELECT * FROM v2 ORDER BY k"),
        (A, "SELECT v1.id, vt.b FROM v1 JOIN vt ON vt.id = v1.id ORDER BY v1.id"),
        (A, "CREATE VIEW v3 AS SELECT * FROM v1 WHERE a2 > 50"),
        (A, "SELECT * FROM v3"),
        (A, "CREATE VIEW v4 AS SELECT count(*) c, avg(a) av, max(b) mb, min(p) mp, sum(p) sp, avg(p) ap FROM vt"),
        (A, "SELECT * FROM v4"),
        (A, "DESCRIBE v1"),
        (A, "DESCRIBE v2"),
        (A, "SHOW COLUMNS FROM v3"),
        (A, "DESCRIBE v4"),
        (A, "CREATE VIEW v1 AS SELECT 1"),
        (A, "CREATE VIEW vbad AS SELECT nope FROM vt"),
        (A, "CREATE VIEW vbad2 (x) AS SELECT id, a FROM vt"),
        (A, "CREATE VIEW vdup AS SELECT id, id FROM vt"),
        (A, "CREATE VIEW vt AS SELECT 1"),
        (A, "CREATE OR REPLACE VIEW vt AS SELECT 1"),
        (A, "CREATE TABLE v1 (x int)"),
        (A, "SELECT table_name, table_type, table_comment FROM information_schema.tables "
            "WHERE table_schema = DATABASE() ORDER BY 1"),
        (A, "SELECT table_name, check_option, is_updatable, security_type FROM information_schema.views "
            "WHERE table_schema = DATABASE() ORDER BY 1"),
        (A, "SHOW FULL TABLES"),
        (A, "CREATE OR REPLACE VIEW v1 AS SELECT id FROM vt"),
        (A, "SELECT * FROM v3"),
        (A, "DROP VIEW v3, v2"),
        (A, "DROP VIEW nope"),
        (A, "DROP VIEW IF EXISTS nope, v1"),
        (A, "DROP VIEW vt"),
        (A, "SHOW TABLES"),
    ],
    "select into user variables": [
        (A, "CREATE TABLE si (a int, b varchar(5))"),
        (A, "INSERT INTO si VALUES (1, 'x'), (2, 'y')"),
        (A, "SELECT 1, 'a' INTO @x, @y"),
        (A, "SELECT @x, @y"),
        (A, "SELECT count(*) INTO @c FROM si"),
        (A, "SELECT @c"),
        (A, "SELECT 1, 2 INTO @only_one"),
        (A, "SELECT a INTO @m FROM si"),
        (A, "SELECT @m"),
        (A, "SET @z = 5"),
        (A, "SELECT a INTO @z FROM si WHERE a > 99"),
        (A, "SELECT @z"),
        (A, "SELECT a, b FROM si WHERE a = 2 INTO @ta, @tb"),
        (A, "SELECT @ta, @tb"),
    ],
    "join using and natural join": [
        (A, "CREATE TABLE ja (id int, x int, n varchar(5))"),
        (A, "CREATE TABLE jb (id int, y int, n varchar(5))"),
        (A, "CREATE TABLE jc (id int, z int)"),
        (A, "INSERT INTO ja VALUES (1, 10, 'p'), (2, 20, 'q'), (3, 30, 'r')"),
        (A, "INSERT INTO jb VALUES (1, 100, 'p'), (2, 200, 'z'), (4, 400, 'r')"),
        (A, "INSERT INTO jc VALUES (1, 7), (4, 9)"),
        (A, "SELECT * FROM ja JOIN jb USING (id) ORDER BY id"),
        (A, "SELECT * FROM ja LEFT JOIN jb USING (id) ORDER BY id"),
        (A, "SELECT * FROM ja RIGHT JOIN jb USING (id) ORDER BY id"),
        (A, "SELECT * FROM ja JOIN jb USING (id, n)"),
        (A, "SELECT * FROM ja NATURAL JOIN jb"),
        (A, "SELECT * FROM ja NATURAL LEFT JOIN jb ORDER BY id"),
        (A, "SELECT id, ja.id, jb.id, x, y FROM ja JOIN jb USING (id) ORDER BY ja.id"),
        (A, "SELECT id, jb.id FROM ja LEFT JOIN jb USING (id) ORDER BY id"),
        (A, "SELECT * FROM ja JOIN jb USING (id) JOIN jc USING (id)"),
        (A, "SELECT count(*) FROM ja JOIN jb USING (nope)"),
        (A, "SELECT id FROM ja JOIN jb USING (id) WHERE id IN (SELECT id FROM jc)"),
        (A, "SELECT ja.id FROM ja JOIN jb ON ja.id = jb.id ORDER BY id"),
        (A, "SELECT ja.id, jb.id FROM ja JOIN jb ON ja.id = jb.id ORDER BY id"),
    ],
    "ALTER narrowing errors": [
        (A, "CREATE TABLE n1 (a varchar(10), b varchar(10), d text, i int)"),
        (A, "INSERT INTO n1 VALUES ('ab','ab','ab',1),('abcdef','ab','abcdef',1),('abcdefg','abcdef','x',300)"),
        (A, "ALTER TABLE n1 MODIFY a varchar(3)"),
        (A, "ALTER TABLE n1 MODIFY d varchar(3)"),
        (A, "ALTER TABLE n1 MODIFY i tinyint"),
        (A, "ALTER TABLE n1 CHANGE b b2 varchar(4)"),
        (A, "ALTER TABLE n1 MODIFY a varchar(20)"),
        (A, "SELECT * FROM n1 ORDER BY a"),
    ],
    "hash and encoding functions": [
        (A, "SELECT MD5('a'), MD5(NULL), MD5(1), SHA1('a'), SHA(''), SHA2('a', 256), SHA2('a', 0), SHA2('a', 224), SHA2('a', 384), SHA2('a', 512), SHA2('a', 7), SHA2('a', NULL)"),
        (A, "SELECT CRC32('a'), CRC32(''), CRC32(12), CRC32(NULL), TO_BASE64('abc'), TO_BASE64(REPEAT('x', 60)), FROM_BASE64('YWJj'), UNHEX('4142'), UNHEX('zz'), UNHEX('141'), HEX(UNHEX('4142'))"),
        (A, "SELECT MD5('a', 'b')"),
        (A, "SELECT SHA2('a')"),
    ],
    "time columns (Django TimeField / DurationField-free)": [
        (A, "CREATE TABLE tm (id int, a time(6), b time)"),
        (A, "INSERT INTO tm VALUES (1, '12:34:56.123', '-838:59:59'), (2, '1 02:03', 123456), (3, NULL, '25:00:00')"),
        (A, "INSERT INTO tm VALUES (4, '01:02:03.5', '9:5'), (7, NULL, '-00:00:01.6')"),
        (A, "SELECT id, a, b FROM tm ORDER BY id"),
        (A, "SELECT id FROM tm WHERE b > '12:00:00' ORDER BY id"),
        (A, "INSERT INTO tm VALUES (5, 'abc', NULL)"),
        (A, "INSERT INTO tm VALUES (6, '10:61:00', NULL)"),
        (A, "SHOW CREATE TABLE tm"),
    ],
    "table and column comments": [
        (A, "CREATE TABLE cm (a int COMMENT 'it''s a', b int)"),
        (A, "ALTER TABLE cm COMMENT = 'tbl c'"),
        (A, "SELECT table_comment AS c FROM information_schema.tables WHERE table_name = 'cm' AND table_schema = database()"),
        (A, "SELECT column_name AS n, column_comment AS c FROM information_schema.columns WHERE table_name = 'cm' AND table_schema = database() ORDER BY ordinal_position"),
        (A, "SHOW CREATE TABLE cm"),
        (A, "ALTER TABLE `cm` COMMENT ''"),
        (A, "SHOW CREATE TABLE cm"),
        (A, "CREATE TABLE cm2 (a int) COMMENT='x y'"),
        (A, "SHOW CREATE TABLE cm2"),
    ],
    "comma joins (Django constraint introspection)": [
        (A, "CREATE TABLE cp (id int PRIMARY KEY, n varchar(5))"),
        (A, "CREATE TABLE cc (id int PRIMARY KEY, pid int, UNIQUE KEY uq (pid), CONSTRAINT fkp FOREIGN KEY (pid) REFERENCES cp (id))"),
        (A, "INSERT INTO cp VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
        (A, "INSERT INTO cc VALUES (10, 1), (20, 2)"),
        (A, "SELECT cp.n, cc.id FROM cp, cc WHERE cc.pid = cp.id ORDER BY cc.id"),
        (A, "SELECT count(*) FROM cp, cc"),
        (A, "SELECT a.id, b.id FROM cp a, cp b WHERE a.id < b.id ORDER BY 1, 2"),
        (A, "SELECT cp.id, x.id FROM cp, cc LEFT JOIN cp x ON x.id = cc.pid WHERE cc.pid = cp.id ORDER BY 1"),
        (A, "SELECT * FROM cp, cc, cp z WHERE z.id = cc.pid AND cp.id = 3 ORDER BY cc.id"),
        (A, "SELECT cp.id, cc.id FROM cp, cc WHERE cp.n IN ('a', 'c') AND cc.id > 10 AND (cp.id = 1 OR cc.pid = 2) ORDER BY 1, 2"),
        (A, "SELECT cp.id, cc.id FROM cp, cc WHERE n = 'b' AND cc.id BETWEEN 5 AND 15 ORDER BY 1, 2"),
        (A, "SELECT a.id, b.id FROM cp a, cp b WHERE a.id = 1 AND b.id <> 1 AND lower(b.n) LIKE '%c' ORDER BY 1, 2"),
        (A, "SELECT cp.id FROM cp, cc WHERE cp.id = (SELECT max(id) FROM cp) AND cc.id = 10"),
        (A, "SELECT kc.constraint_name AS c, kc.column_name AS col, kc.referenced_table_name AS rt, kc.referenced_column_name AS rc, c.constraint_type AS t FROM information_schema.key_column_usage AS kc, information_schema.table_constraints AS c WHERE kc.table_schema = DATABASE() AND (kc.referenced_table_schema = DATABASE() OR kc.referenced_table_schema IS NULL) AND c.table_schema = kc.table_schema AND c.constraint_name = kc.constraint_name AND c.constraint_type != 'CHECK' AND kc.table_name = 'cc' ORDER BY kc.ordinal_position, c.constraint_type"),
    ],
    "DEFAULT as an insert value and update assignment": [
        (A, "CREATE TABLE dd (id int AUTO_INCREMENT PRIMARY KEY, a int DEFAULT 5, b varchar(5) DEFAULT 'x', c int NOT NULL)"),
        (A, "INSERT INTO dd (id, a, b, c) VALUES (DEFAULT, DEFAULT, 'q', 1)"),
        (A, "INSERT INTO dd VALUES (DEFAULT, 7, DEFAULT, 2), (10, default, default, 3)"),
        (A, "INSERT INTO dd (c) VALUES ()"),
        (A, "INSERT INTO dd VALUES ()"),
        (A, "INSERT INTO dd (c, a) VALUES (DEFAULT, 1)"),
        (A, "SELECT * FROM dd ORDER BY id"),
        (A, "UPDATE dd SET a = DEFAULT, b = 'zz' WHERE id = 2"),
        (A, "SELECT * FROM dd ORDER BY id"),
    ],
    "EXPLAIN access paths": [
        (A, "CREATE TABLE tg (id int primary key, name varchar(20), k int, email varchar(50) NOT NULL, big bigint, KEY kk (k), UNIQUE KEY ue (email))"),
        (A, "CREATE TABLE tp (id int primary key auto_increment, tg_id int, KEY fk (tg_id))"),
        (A, "INSERT INTO tg VALUES (1,'a',1,'a@x',1),(2,'b',2,'b@x',2),(3,'c',2,'c@x',3)"),
        (A, "INSERT INTO tp (tg_id) VALUES (1),(1),(2)"),
        (A, "EXPLAIN SELECT 1"),
        (A, "EXPLAIN SELECT * FROM tg"),
        (A, "EXPLAIN SELECT * FROM tg WHERE k = 2"),
        (A, "EXPLAIN SELECT * FROM tg WHERE email = 'a@x'"),
        (A, "EXPLAIN SELECT * FROM tg WHERE email = 'zz'"),
        (A, "EXPLAIN SELECT * FROM tg WHERE big = 2 ORDER BY name"),
        (A, "EXPLAIN SELECT * FROM tg LEFT JOIN tp ON tp.id = tg.k"),
        (A, "EXPLAIN DELETE FROM tg WHERE k = 1"),
        (A, "EXPLAIN INSERT INTO tp (tg_id) VALUES (3)"),
        (A, "EXPLAIN FORMAT=TRADITIONAL SELECT DISTINCT name FROM tg LIMIT 2"),
        (A, "DESCRIBE SELECT * FROM tg WHERE id = 1"),
        (A, "EXPLAIN ANALYZE DELETE FROM tg"),
        (A, "EXPLAIN SELECT * FROM nope"),
        (A, "SELECT count(*) FROM tg"),
    ],
    "CHECK constraints": [
        (A, "CREATE TABLE ck1 (id int PRIMARY KEY, a int unsigned CHECK (a >= 0), b varchar(10), c int, CONSTRAINT b_ok CHECK (b IN ('x','y') OR b IS NULL), CHECK (a < c AND lower(b) <> 'Q'), CONSTRAINT cdate CHECK (c BETWEEN 1 AND 100))"),
        (A, "SHOW CREATE TABLE ck1"),
        (A, "SELECT constraint_name AS n, check_clause AS c FROM information_schema.check_constraints WHERE constraint_schema = database() ORDER BY 1"),
        (A, "SELECT constraint_name AS n, constraint_type AS t, enforced AS e FROM information_schema.table_constraints WHERE table_schema = database() AND table_name = 'ck1' ORDER BY 1"),
        (A, "INSERT INTO ck1 VALUES (1, 5, 'x', 200)"),
        (A, "INSERT INTO ck1 VALUES (1, 5, 'z', 10)"),
        (A, "INSERT INTO ck1 VALUES (1, 5, NULL, NULL)"),
        (A, "UPDATE ck1 SET c = 0"),
        (A, "INSERT INTO ck1 VALUES (2, 5, NULL, 4)"),
        (A, "INSERT IGNORE INTO ck1 VALUES (3, 5, 'q', 4), (4, 1, 'y', 9)"),
        (A, "SELECT * FROM ck1 ORDER BY id"),
        (A, "ALTER TABLE ck1 ADD CONSTRAINT big CHECK (c > 50)"),
        (A, "ALTER TABLE ck1 ADD CONSTRAINT b_ok CHECK (c > 0)"),
        (A, "ALTER TABLE ck1 DROP CHECK cdate"),
        (A, "ALTER TABLE ck1 DROP CHECK nope"),
        (A, "ALTER TABLE ck1 RENAME COLUMN c TO cc"),
        (A, "ALTER TABLE ck1 DROP CONSTRAINT ck1_chk_2"),
        (A, "SHOW CREATE TABLE ck1"),
        (A, "CREATE TABLE ck2 (id int, a int CHECK (a > 0), b int, c int, CONSTRAINT x CHECK (b > c), CHECK (b IS NOT NULL AND -a < 3 AND a + 1 <> 2 * b / 3 % 2), CHECK (b LIKE 'a%' OR c NOT BETWEEN 1 AND 2))"),
        (A, "SHOW CREATE TABLE ck2"),
        (A, "ALTER TABLE ck2 DROP COLUMN a"),
        (A, "ALTER TABLE ck2 DROP CHECK ck2_chk_3"),
        (A, "ALTER TABLE ck2 DROP CHECK ck2_chk_2"),
        (A, "ALTER TABLE ck2 DROP COLUMN a"),
        (A, "ALTER TABLE ck2 ADD COLUMN z int CHECK (z > 1), ADD CONSTRAINT w CHECK (id > 0)"),
        (A, "INSERT INTO ck2 VALUES (0, 1, 2, 3)"),
        (A, "INSERT INTO ck2 (id) VALUES (NULL)"),
        (A, "SHOW CREATE TABLE ck2"),
        (A, "DESCRIBE ck1"),
    ],
    "bit operators, math, week and statistics functions": [
        (A, "SELECT 1 & 3 AS a, 5 | 2 AS b, 6 ^ 3 AS c, ~0 AS d, 1 << 3 AS e, 8 >> 1 AS f, -1 & 255 AS g, 1.6 | 0 AS h, '7' & 3 AS i, NULL & 1 AS j, 1 << 64 AS k, ~5 AS l"),
        (A, "SELECT 1 XOR 0 AS a, 1 XOR 1 AS b, NULL XOR 1 AS c, 0 XOR 0 AS d"),
        (A, "SELECT acos(0.5) AS a, acos(2) AS b, asin(0.5) AS c, atan(1) AS d, atan(1, 2) AS e, atan2(1, 2) AS f, sin(1) AS g, cos(1) AS h, tan(1) AS i, cot(1) AS j, degrees(1) AS k, radians(180) AS l"),
        (A, "SELECT exp(1) AS a, ln(2) AS b, ln(0) AS c, log(8) AS d, log(2, 8) AS e, log(1, 8) AS f, log2(8) AS g, log10(100) AS h, log(-1) AS i, cot(0) AS j"),
        (A, "SELECT rand(1) AS a, rand(0) AS b, rand(42) AS c, rand(NULL) AS d, rand() < 1 AS e"),
        (A, "SELECT ord('ab') AS a, ord('\u00e9') AS b, ord('') AS c, ord(NULL) AS d, ord(65) AS e"),
        (A, "SELECT week('2024-01-01') AS a, week('2024-01-01', 3) AS b, week('2021-01-03', 0) AS c, week('2021-01-03', 1) AS d, week('2021-01-03', 2) AS e, week('2021-01-03', 3) AS f, week('2021-01-03', 4) AS g, week('2021-01-03', 5) AS h, week('2021-01-03', 6) AS i, week('2021-01-03', 7) AS j, week('2020-12-31', 3) AS k, week('2019-12-30', 3) AS l"),
        (A, "SELECT yearweek('2021-01-03') AS a, yearweek('2021-01-03', 3) AS b, yearweek('2019-12-30', 3) AS c, yearweek('2024-12-30', 1) AS d, weekofyear('2021-01-03') AS e, week(NULL) AS f"),
        (A, "SELECT makedate(2024, 60) AS a, makedate(24, 1) AS b, makedate(2024, 0) AS c, makedate(2023, 400) AS d, makedate(NULL, 1) AS e"),
        (A, "SELECT time_to_sec('01:00:00') AS a, time_to_sec('-01:00:01') AS b, time_to_sec('10:00:00.5') AS c, time_to_sec(NULL) AS d, sec_to_time(3661) AS e, sec_to_time(-5) AS f"),
        (A, "CREATE TABLE ag (g int, x double, i int)"),
        (A, "INSERT INTO ag VALUES (1, 1, 5), (1, 2, 6), (1, 4, NULL), (2, 10, 7), (3, NULL, NULL)"),
        (A, "SELECT g, std(x), stddev(x), stddev_pop(x), stddev_samp(x), variance(x), var_pop(x), var_samp(x), bit_and(i), bit_or(i), bit_xor(i) FROM ag GROUP BY g ORDER BY g"),
        (A, "SELECT stddev_pop(i) AS a, var_samp(i) AS b, bit_and(i) AS c FROM ag WHERE g = 9"),
    ],
    "parenthesised set operation as an expression": [
        (A, "CREATE TABLE rn (id int, o int)"),
        (A, "INSERT INTO rn VALUES (1,8),(2,1),(3,5)"),
        (A, "SELECT id FROM rn WHERE id IN ((SELECT 1) UNION (SELECT 3)) ORDER BY 1"),
        (A, "SELECT o, ((SELECT 8 AS n) UNION (SELECT 9)) AS x FROM rn ORDER BY 1"),
        (A, "SELECT o, ((SELECT 8 AS n) UNION (SELECT 8)) AS x FROM rn ORDER BY 1"),
        (A, "SELECT o, ((SELECT n FROM (SELECT 8 AS n) a WHERE a.n = rn.o) UNION (SELECT 1 FROM (SELECT 1) b WHERE rn.o = 1)) AS x FROM rn ORDER BY 1"),
        (A, "SELECT '((SELECT 1) UNION (SELECT 2))' AS s"),
    ],
    "IP address functions": [
        (A, "SELECT INET_ATON('10.0.5.9'), INET_NTOA(167773449), HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089')), INET6_NTOA(INET6_ATON('::ffff:1.2.3.4')), INET6_NTOA(UNHEX('0A000509'))"),
        (A, "SELECT IS_IPV4('10.0.5.9'), IS_IPV4('10.0.5.256'), IS_IPV6('::1'), IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9')), IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))"),
        (A, "SELECT INET_ATON('127.1'), INET_ATON('bogus'), INET6_ATON('bogus'), INET_NTOA(NULL), INET6_NTOA(INET6_ATON('::10.0.5.9')), INET6_NTOA(INET6_ATON('::1')), INET_ATON('1.2.3.256'), INET_NTOA(4294967296), INET_ATON('255.255.255.255')"),
    ],
}
MYSQL.update(MYSQL_QUERIES)

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
    },
    "postgres": {},
}

sys.path.insert(0, __import__("os").path.dirname(__file__))
from pg_edges import PG_EDGES  # noqa: E402

POSTGRES.update(PG_EDGES)
scenarios = MYSQL if KIND == "mysql" else POSTGRES
failed, known, fixed = [], [], []
for name, steps in scenarios.items():
    got, want = run(NOIDA_PORT, steps), run(REF_PORT, steps)
    # repr: NaN never equals itself, but the same NaN prints the same.
    same = repr(got) == repr(want)
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
