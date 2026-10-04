"""pymysql against noida-db's MySQL: keys and upserts, NULL/strict-mode
semantics, DML ORDER BY/LIMIT, the scalar function library, JSON/ENUM
columns, introspection, exact DECIMAL math, and transactions (explicit and
autocommit=0, per-connection rollback). Expected values are what real
MySQL 8.0 returns."""
import datetime
import decimal
import time

import pymysql
from harness import PORT, case, report

D = decimal.Decimal


def conn(db="test", autocommit=True):
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="",
                           database=db, autocommit=autocommit, charset="utf8mb4")


c = conn()


def q(sql, args=None, cn=None):
    cur = (cn or c).cursor()
    cur.execute(sql, args)
    return cur.fetchall()


def ex(sql, args=None):
    return c.cursor().execute(sql, args)


def err_code(sql):
    try:
        ex(sql)
    except pymysql.MySQLError as e:
        return e.args[0]
    return None


def one(sql):
    return q(sql)[0][0]


ex("CREATE DATABASE b2")
ex("USE b2")

# ---------------- keys & upserts ----------------
ex("CREATE TABLE kv (k VARCHAR(50) PRIMARY KEY, v INT NOT NULL DEFAULT 0, note VARCHAR(20) NULL)")
case("insert", lambda: ex("INSERT INTO kv (k, v) VALUES ('a', 1)"), 1)
case("dup PK rejected 1062", lambda: err_code("INSERT INTO kv (k, v) VALUES ('a', 2)"), 1062)
case("dup PK is case-insensitive", lambda: err_code("INSERT INTO kv (k, v) VALUES ('A', 2)"), 1062)
case("multi-row insert is atomic", lambda: (err_code("INSERT INTO kv (k, v) VALUES ('b',1),('c',1),('a',9)"),
                                            one("SELECT COUNT(*) FROM kv")), (1062, 1))
case("INSERT IGNORE skips dup", lambda: (ex("INSERT IGNORE INTO kv (k, v) VALUES ('a',5),('d',4)"),
                                         one("SELECT v FROM kv WHERE k='a'")), (1, 1))
case("REPLACE replaces", lambda: (ex("REPLACE INTO kv VALUES ('a', 7, NULL)"),
                                  one("SELECT v FROM kv WHERE k='a'"), one("SELECT COUNT(*) FROM kv")), (2, 7, 2))
case("ON DUPLICATE KEY UPDATE + VALUES()", lambda: (
    ex("INSERT INTO kv (k, v) VALUES ('a', 3) ON DUPLICATE KEY UPDATE v = v + VALUES(v)"),
    one("SELECT v FROM kv WHERE k='a'")), (2, 10))
case("ON DUPLICATE KEY UPDATE row alias", lambda: (
    ex("INSERT INTO kv (k, v) VALUES ('a', 1) AS new ON DUPLICATE KEY UPDATE v = new.v + kv.v"),
    one("SELECT v FROM kv WHERE k='a'")), (2, 11))
case("ON DUPLICATE KEY UPDATE no-op = 0 rows", lambda: ex(
    "INSERT INTO kv (k, v) VALUES ('a', 11) ON DUPLICATE KEY UPDATE v = v"), 0)
case("ON DUPLICATE KEY UPDATE new row = 1", lambda: ex(
    "INSERT INTO kv (k, v) VALUES ('z', 5) ON DUPLICATE KEY UPDATE v = v + 1"), 1)
case("INSERT ... SET", lambda: (ex("INSERT INTO kv SET k = 'set', v = 42"),
                                one("SELECT v FROM kv WHERE k = 'set'")), (1, 42))

ex("CREATE TABLE m (id INT AUTO_INCREMENT PRIMARY KEY, a INT, b INT, UNIQUE KEY ab (a, b))")
ex("INSERT INTO m (a, b) VALUES (1, 1), (1, 2)")


def dup_msg():
    try:
        ex("INSERT INTO m (a, b) VALUES (1, 1)")
    except pymysql.MySQLError as e:
        return e.args
    return None


case("composite UNIQUE message", dup_msg, (1062, "Duplicate entry '1-1' for key 'm.ab'"))
case("NULLs never collide in UNIQUE", lambda: ex("INSERT INTO m (a, b) VALUES (NULL, 1), (NULL, 1)"), 2)
case("UPDATE into a dup is rejected, row unchanged", lambda: (
    err_code("UPDATE m SET b = 1 WHERE a = 1 AND b = 2"),
    q("SELECT a, b FROM m WHERE a = 1 ORDER BY b")), (1062, ((1, 1), (1, 2))))
case("explicit id advances AUTO_INCREMENT", lambda: (
    ex("INSERT INTO m (id, a, b) VALUES (100, 5, 5)"), ex("INSERT INTO m (a, b) VALUES (6, 6)"),
    one("SELECT MAX(id) FROM m")), (1, 1, 101))
case("LAST_INSERT_ID()", lambda: one("SELECT LAST_INSERT_ID()"), 101)
case("LAST_INSERT_ID() persists across statements", lambda: (q("SELECT 1"), one("SELECT LAST_INSERT_ID()"))[1], 101)
case("TRUNCATE resets AUTO_INCREMENT", lambda: (ex("TRUNCATE TABLE m"), ex("INSERT INTO m (a, b) VALUES (1, 1)"),
                                                 one("SELECT id FROM m")), (0, 1, 1))

# ---------------- NULL / defaults / strictness ----------------
ex("CREATE TABLE n (id INT PRIMARY KEY, x INT DEFAULT 5, y VARCHAR(5) NOT NULL DEFAULT 'q')")
case("explicit NULL beats DEFAULT", lambda: (ex("INSERT INTO n (id, x) VALUES (1, NULL)"),
                                             q("SELECT x, y FROM n WHERE id = 1")), (1, ((None, "q"),)))
case("omitted column gets DEFAULT", lambda: (ex("INSERT INTO n (id) VALUES (2)"),
                                             one("SELECT x FROM n WHERE id = 2")), (1, 5))
case("NULL into NOT NULL is 1048", lambda: err_code("INSERT INTO n (id, y) VALUES (3, NULL)"), 1048)
case("UPDATE NOT NULL to NULL is 1048", lambda: err_code("UPDATE n SET y = NULL"), 1048)
case("VARCHAR overflow is 1406", lambda: err_code("INSERT INTO n (id, y) VALUES (4, 'toolong')"), 1406)
case("column names are case-insensitive", lambda: q("SELECT ID, X FROM n WHERE Id = 2"), ((2, 5),))
case("unknown column in SELECT is 1054", lambda: err_code("SELECT nosuch FROM n"), 1054)
case("unknown column in WHERE is 1054", lambda: err_code("SELECT id FROM n WHERE nosuch = 1"), 1054)
case("unknown column in UPDATE SET is 1054", lambda: err_code("UPDATE n SET nosuch = 1"), 1054)
case("unknown column in INSERT is 1054", lambda: err_code("INSERT INTO n (id, nosuch) VALUES (9, 1)"), 1054)

# ---------------- UPDATE / DELETE ORDER BY LIMIT ----------------
ex("CREATE TABLE ql (id INT PRIMARY KEY, p INT)")
ex("INSERT INTO ql VALUES (1,1),(2,0),(3,1),(4,0),(5,1)")
case("DELETE ... ORDER BY DESC LIMIT 1", lambda: (ex("DELETE FROM ql WHERE p = 1 ORDER BY id DESC LIMIT 1"),
                                                  [r[0] for r in q("SELECT id FROM ql ORDER BY id")]), (1, [1, 2, 3, 4]))
case("UPDATE ... ORDER BY LIMIT 2", lambda: (ex("UPDATE ql SET p = 9 ORDER BY id LIMIT 2"),
                                             q("SELECT id FROM ql WHERE p = 9 ORDER BY id")), (2, ((1,), (2,))))

# ---------------- scalar functions ----------------
F = [
    ("IF", "SELECT IF(1 > 0, 'y', 'n')", "y"),
    ("IF null cond", "SELECT IF(NULL, 'y', 'n')", "n"),
    ("IFNULL", "SELECT IFNULL(NULL, 3)", 3),
    ("NULLIF", "SELECT NULLIF(1, 1)", None),
    ("GREATEST", "SELECT GREATEST(3, 9, 4)", 9),
    ("ROUND half up", "SELECT ROUND(2.5)", D("3")),
    ("ROUND negative half", "SELECT ROUND(-2.5)", D("-3")),
    ("ROUND 2dp", "SELECT ROUND(1.2345, 2)", D("1.23")),
    ("FLOOR", "SELECT FLOOR(1.7)", 1),
    ("CEIL", "SELECT CEIL(1.2)", 2),
    ("ABS", "SELECT ABS(-5)", 5),
    ("DIV", "SELECT 7 DIV 2", 3),
    ("MOD", "SELECT MOD(7, 3)", 1),
    ("CONCAT_WS skips NULL", "SELECT CONCAT_WS('-', 'a', NULL, 'b')", "a-b"),
    ("TRIM", "SELECT TRIM('  x  ')", "x"),
    ("TRIM LEADING", "SELECT TRIM(LEADING 'x' FROM 'xxaxx')", "axx"),
    ("REPLACE", "SELECT REPLACE('hello', 'l', 'L')", "heLLo"),
    ("LEFT", "SELECT LEFT('hello', 2)", "he"),
    ("RIGHT", "SELECT RIGHT('hello', 2)", "lo"),
    ("LPAD", "SELECT LPAD('5', 3, '0')", "005"),
    ("LOCATE ci", "SELECT LOCATE('L', 'hello')", 3),
    ("INSTR", "SELECT INSTR('hello', 'lo')", 4),
    ("CHAR_LENGTH", "SELECT CHAR_LENGTH('héllo')", 5),
    ("LENGTH bytes", "SELECT LENGTH('héllo')", 6),
    ("CAST string prefix AS SIGNED", "SELECT CAST('42abc' AS SIGNED)", 42),
    ("CAST 3.7 AS SIGNED", "SELECT CAST(3.7 AS SIGNED)", 4),
    ("CAST AS CHAR", "SELECT CAST(12 AS CHAR)", "12"),
    ("CAST AS DECIMAL", "SELECT CAST('1.239' AS DECIMAL(5,2))", D("1.24")),
    ("DATE_FORMAT", "SELECT DATE_FORMAT('2024-03-05 14:07:09', '%Y/%m/%d %H:%i:%s')", "2024/03/05 14:07:09"),
    ("DATE_FORMAT names", "SELECT DATE_FORMAT('2024-03-05', '%W %M %D %Y')", "Tuesday March 5th 2024"),
    ("DATEDIFF", "SELECT DATEDIFF('2024-03-01', '2024-02-01')", 29),
    ("TIMESTAMPDIFF MONTH", "SELECT TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-02-29')", 0),
    ("TIMESTAMPDIFF DAY", "SELECT TIMESTAMPDIFF(DAY, '2024-01-01', '2024-03-01')", 60),
    ("UNIX_TIMESTAMP", "SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')", 86400),
    ("FROM_UNIXTIME", "SELECT FROM_UNIXTIME(0)", datetime.datetime(1970, 1, 1)),
    ("YEAR", "SELECT YEAR('2024-03-05')", 2024),
    ("DAYOFWEEK", "SELECT DAYOFWEEK('2024-03-05')", 3),
    ("WEEKDAY", "SELECT WEEKDAY('2024-03-05')", 1),
    ("EXTRACT", "SELECT EXTRACT(MONTH FROM '2024-03-05')", 3),
    ("LAST_DAY leap", "SELECT LAST_DAY('2024-02-10')", datetime.date(2024, 2, 29)),
    ("JSON_OBJECT", "SELECT JSON_OBJECT('k', 1, 'b', 'x')", '{"b": "x", "k": 1}'),
    ("JSON_ARRAY", "SELECT JSON_ARRAY(1, 'a', NULL)", '[1, "a", null]'),
    ("JSON_UNQUOTE", "SELECT JSON_UNQUOTE('\"hi\"')", "hi"),
    ("JSON_VALID", "SELECT JSON_VALID('{bad')", 0),
    ("JSON_CONTAINS", "SELECT JSON_CONTAINS('[1, 2, 3]', '2')", 1),
    ("CONVERT_TZ", "SELECT CONVERT_TZ('2001-01-01 01:00:00', 'UTC', '+05:30')", datetime.datetime(2001, 1, 1, 6, 30)),
]
for name, sql, want in F:
    case(name, (lambda s: lambda: one(s))(sql), want)

case("DATE_ADD month clamps", lambda: str(one("SELECT DATE_ADD('2024-01-31', INTERVAL 1 MONTH)")), "2024-02-29")
case("date + INTERVAL", lambda: str(one("SELECT '2024-01-01' + INTERVAL 1 DAY")), "2024-01-02")
case("DATE_SUB on DATE()", lambda: str(one("SELECT DATE_SUB(DATE('2024-03-01'), INTERVAL 1 DAY)")), "2024-02-29")
case("DATE_ADD hours", lambda: str(one("SELECT DATE_ADD('2024-01-01 23:00:00', INTERVAL 2 HOUR)")), "2024-01-02 01:00:00")
case("NOW() - INTERVAL in WHERE", lambda: q("SELECT 1 FROM DUAL WHERE NOW() > NOW() - INTERVAL 1 DAY"), ((1,),))


def labels(sql):
    cur = c.cursor()
    cur.execute(sql)
    return [d[0] for d in cur.description]


case("expression column labels", lambda: labels("SELECT COUNT(*), 1 + 1, 'x' FROM n"), ["COUNT(*)", "1 + 1", "x"])

ex("CREATE TABLE g (cat VARCHAR(5), v VARCHAR(5))")
ex("INSERT INTO g VALUES ('a','x'),('a','y'),('b','z'),('a','x')")
case("GROUP_CONCAT ORDER BY SEPARATOR", lambda: q(
    "SELECT cat, GROUP_CONCAT(v ORDER BY v DESC SEPARATOR '|') FROM g GROUP BY cat ORDER BY cat"),
    (("a", "y|x|x"), ("b", "z")))
case("GROUP_CONCAT DISTINCT", lambda: one(
    "SELECT GROUP_CONCAT(DISTINCT v ORDER BY v) FROM g WHERE cat = 'a'"), "x,y")

# ---------------- joins with aliases ----------------
ex("CREATE TABLE products (product_id INT PRIMARY KEY, name VARCHAR(20))")
ex("CREATE TABLE order_items (item_id INT PRIMARY KEY, product_id INT, qty INT)")
ex("INSERT INTO products VALUES (1, 'lamp'), (2, 'desk')")
ex("INSERT INTO order_items VALUES (10, 1, 3), (11, 1, 4), (12, 2, 1)")
case("aliased join is not a cartesian product", lambda: q(
    "SELECT p.name, SUM(oi.qty) AS units FROM products p JOIN order_items oi ON oi.product_id = p.product_id "
    "GROUP BY p.name ORDER BY units DESC"), (("lamp", D("7")), ("desk", D("1"))))
case("ambiguous bare column is 1052", lambda: err_code(
    "SELECT product_id FROM products p JOIN order_items oi ON oi.product_id = p.product_id"), 1052)

# ---------------- JSON / ENUM columns ----------------
ex("CREATE TABLE j (id INT PRIMARY KEY, doc JSON)")
ex("""INSERT INTO j VALUES (1, '{"s": "hi", "a": {"b": [10, 20]}}')""")
case("JSON column round trip (MySQL key order)", lambda: one("SELECT doc FROM j"), '{"a": {"b": [10, 20]}, "s": "hi"}')
case("-> path", lambda: one("SELECT doc->'$.a.b[1]' FROM j"), "20")
case("->> path", lambda: one("SELECT doc->>'$.s' FROM j"), "hi")
case("JSON_EXTRACT string keeps quotes", lambda: one("SELECT JSON_EXTRACT(doc, '$.s') FROM j"), '"hi"')
case("WHERE on ->>", lambda: q("SELECT id FROM j WHERE doc->>'$.s' = 'hi'"), ((1,),))
case("invalid JSON rejected 3140", lambda: err_code("INSERT INTO j VALUES (2, '{bad')"), 3140)

ex("CREATE TABLE e (id INT PRIMARY KEY, s ENUM('draft','published') NOT NULL DEFAULT 'draft')")
case("ENUM default", lambda: (ex("INSERT INTO e (id) VALUES (1)"), one("SELECT s FROM e WHERE id=1")), (1, "draft"))
case("ENUM is case-insensitive, stored canonical", lambda: (ex("INSERT INTO e VALUES (2, 'PUBLISHED')"),
                                                             one("SELECT s FROM e WHERE id=2")), (1, "published"))
case("ENUM rejects unknown 1265", lambda: err_code("INSERT INTO e VALUES (3, 'bogus')"), 1265)

# ---------------- DDL / introspection ----------------
case("CREATE TABLE IF NOT EXISTS on existing", lambda: (ex("CREATE TABLE IF NOT EXISTS kv (x INT)"),
                                                         len(q("DESCRIBE kv"))), (0, 3))
case("DROP missing is 1051", lambda: err_code("DROP TABLE nosuch"), 1051)
case("DROP IF EXISTS missing ok", lambda: ex("DROP TABLE IF EXISTS nosuch"), 0)
case("DESCRIBE", lambda: q("DESCRIBE kv"), (("k", "varchar(50)", "NO", "PRI", None, ""),
                                           ("v", "int", "NO", "", "0", ""),
                                           ("note", "varchar(20)", "YES", "", None, "")))
case("ALTER TABLE ADD/MODIFY/RENAME", lambda: (
    ex("ALTER TABLE n ADD COLUMN z INT NOT NULL DEFAULT 7 AFTER id"),
    ex("ALTER TABLE n MODIFY y VARCHAR(50) NOT NULL"),
    ex("ALTER TABLE n RENAME COLUMN y TO label"),
    [r[0] for r in q("DESCRIBE n")])[-1], ["id", "z", "x", "label"])
case("SHOW DATABASES lists created db", lambda: "b2" in [r[0] for r in q("SHOW DATABASES")], True)
case("SHOW FULL TABLES", lambda: ("kv", "BASE TABLE") in q("SHOW FULL TABLES"), True)
case("SHOW TABLES LIKE", lambda: q("SHOW TABLES LIKE 'k%'"), (("kv",),))
case("SHOW INDEX", lambda: sorted({r[2] for r in q("SHOW INDEX FROM m")}), ["PRIMARY", "ab"])
case("SHOW CREATE TABLE has UNIQUE KEY", lambda: "UNIQUE KEY `ab` (`a`,`b`)" in q("SHOW CREATE TABLE m")[0][1], True)
case("information_schema.tables", lambda: [r[0] for r in q(
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'b2' AND TABLE_NAME IN ('kv', 'm', 'n') "
    "ORDER BY TABLE_NAME")], ["kv", "m", "n"])
case("information_schema.columns", lambda: q(
    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.columns "
    "WHERE table_schema = 'b2' AND table_name = 'kv' ORDER BY ordinal_position"),
    (("k", "varchar", "NO", "PRI"), ("v", "int", "NO", ""), ("note", "varchar", "YES", "")))

# ---------------- money math stays exact ----------------
ex("CREATE TABLE acct (id INT PRIMARY KEY, bal DECIMAL(10,2) NOT NULL)")
ex("INSERT INTO acct VALUES (1, 100.00), (2, 0.10), (3, 0.20)")
case("DECIMAL * literal rounds to column scale", lambda: (ex("UPDATE acct SET bal = bal * 1.105 WHERE id = 1"),
                                                         one("SELECT bal FROM acct WHERE id = 1")), (1, D("110.50")))
case("0.1 + 0.2 exact", lambda: one("SELECT SUM(bal) FROM acct WHERE id > 1"), D("0.30"))
case("AVG keeps 4 more decimals", lambda: one("SELECT AVG(bal) FROM acct WHERE id > 1"), D("0.150000"))

# ---------------- transactions ----------------
ex("CREATE TABLE tx (id INT PRIMARY KEY, v VARCHAR(10))")
ex("INSERT INTO tx VALUES (1, 'orig')")
a = conn("b2", autocommit=False)
case("server reports autocommit off", lambda: a.get_autocommit(), False)


def basic_rollback():
    q("INSERT INTO tx VALUES (2, 'a')", cn=a)
    q("UPDATE tx SET v = 'changed' WHERE id = 1", cn=a)
    a.rollback()
    return q("SELECT id, v FROM tx ORDER BY id")


case("implicit tx rolled back (insert+update)", basic_rollback, ((1, "orig"),))
case("commit keeps", lambda: (q("INSERT INTO tx VALUES (3, 'kept')", cn=a), a.commit(),
                              one("SELECT COUNT(*) FROM tx WHERE id = 3"))[-1], 1)


def concurrent_same_table():
    q("INSERT INTO tx VALUES (5, 'a5')", cn=a)
    q("UPDATE tx SET v = 'a-upd' WHERE id = 3", cn=a)
    ex("INSERT INTO tx VALUES (6, 'other')")  # committed by another connection
    a.rollback()
    return q("SELECT id, v FROM tx ORDER BY id")


case("rollback keeps other connections' rows", concurrent_same_table, ((1, "orig"), (3, "kept"), (6, "other")))
case("delete rolled back", lambda: (q("DELETE FROM tx WHERE id IN (1, 6)", cn=a), a.rollback(),
                                    q("SELECT id FROM tx ORDER BY id"))[-1], ((1,), (3,), (6,)))
case("DDL implicitly commits", lambda: (q("INSERT INTO tx VALUES (7, 'ddl')", cn=a), q("CREATE TABLE ddl_t (x INT)", cn=a),
                                        a.rollback(), one("SELECT COUNT(*) FROM tx WHERE id = 7"))[-1], 1)


def savepoints():
    q("INSERT INTO tx VALUES (8, 'keep')", cn=a)
    q("SAVEPOINT s1", cn=a)
    q("INSERT INTO tx VALUES (9, 'drop')", cn=a)
    q("ROLLBACK TO SAVEPOINT s1", cn=a)
    a.commit()
    return q("SELECT id FROM tx WHERE id IN (8, 9)")


case("savepoint partial rollback", savepoints, ((8,),))


def disconnect_rolls_back():
    b = conn("b2", autocommit=False)
    q("INSERT INTO tx VALUES (10, 'gone')", cn=b)
    b.close()
    time.sleep(0.2)
    return one("SELECT COUNT(*) FROM tx WHERE id = 10")


case("disconnect mid-tx rolls back", disconnect_rolls_back, 0)


def failed_statement_keeps_tx():
    q("INSERT INTO tx VALUES (11, 'ok')", cn=a)
    try:
        q("INSERT INTO tx VALUES (11, 'dup')", cn=a)
    except pymysql.MySQLError:
        pass
    a.commit()
    return q("SELECT v FROM tx WHERE id = 11")


case("failed statement doesn't abort the tx", failed_statement_keeps_tx, (("ok",),))

report("pymysql")
