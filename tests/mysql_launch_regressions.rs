//! Regressions for MySQL bugs found by brute-force client testing before
//! the first public release. Every expected value here is what real MySQL
//! 8.0 returns; most of these used to be *silently* wrong (a duplicate row,
//! the wrong row deleted, a NULL where an error belonged), not errors.
#![cfg(feature = "mysql")]

use mysql_async::prelude::*;
use mysql_async::{Conn, Pool, Row};

async fn connect() -> (Pool, Conn) {
    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let pool = Pool::new(format!("mysql://root@127.0.0.1:{}/test", addr.port()).as_str());
    let conn = pool.get_conn().await.unwrap();
    (pool, conn)
}

/// First column of the first row, as text (`None` for SQL NULL).
async fn one(conn: &mut Conn, sql: &str) -> Option<String> {
    let row: Row = conn.query_first(sql).await.unwrap().expect("a row");
    row.get_opt::<Option<String>, _>(0).unwrap().unwrap()
}

async fn rows(conn: &mut Conn, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows: Vec<Row> = conn.query(sql).await.unwrap();
    rows.into_iter()
        .map(|r| {
            (0..r.len()).map(|i| r.get_opt::<Option<String>, _>(i).unwrap().unwrap()).collect()
        })
        .collect()
}

async fn err_code(conn: &mut Conn, sql: &str) -> u16 {
    match conn.query_drop(sql).await {
        Err(mysql_async::Error::Server(e)) => e.code,
        other => panic!("expected a server error from {sql:?}, got {other:?}"),
    }
}

async fn affected(conn: &mut Conn, sql: &str) -> u64 {
    conn.query_drop(sql).await.unwrap();
    conn.affected_rows()
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

#[tokio::test]
async fn keys_are_enforced_and_upserts_work() {
    let (pool, mut c) = connect().await;
    c.query_drop("CREATE TABLE kv (k VARCHAR(50) PRIMARY KEY, v INT NOT NULL DEFAULT 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO kv VALUES ('a', 1)").await.unwrap();

    assert_eq!(err_code(&mut c, "INSERT INTO kv VALUES ('a', 2)").await, 1062);
    // `_ci` collation: 'A' is the same key as 'a'.
    assert_eq!(err_code(&mut c, "INSERT INTO kv VALUES ('A', 2)").await, 1062);
    // A failing multi-row INSERT leaves the table untouched.
    assert_eq!(err_code(&mut c, "INSERT INTO kv VALUES ('b', 1), ('a', 9)").await, 1062);
    assert_eq!(one(&mut c, "SELECT COUNT(*) FROM kv").await, s("1"));

    assert_eq!(affected(&mut c, "INSERT IGNORE INTO kv VALUES ('a', 5), ('d', 4)").await, 1);
    assert_eq!(one(&mut c, "SELECT v FROM kv WHERE k = 'a'").await, s("1"));

    assert_eq!(affected(&mut c, "REPLACE INTO kv VALUES ('a', 7)").await, 2);
    assert_eq!(one(&mut c, "SELECT v FROM kv WHERE k = 'a'").await, s("7"));

    let upsert = "INSERT INTO kv VALUES ('a', 3) ON DUPLICATE KEY UPDATE v = v + VALUES(v)";
    assert_eq!(affected(&mut c, upsert).await, 2);
    assert_eq!(one(&mut c, "SELECT v FROM kv WHERE k = 'a'").await, s("10"));
    let aliased = "INSERT INTO kv VALUES ('a', 1) AS new ON DUPLICATE KEY UPDATE v = new.v + kv.v";
    assert_eq!(affected(&mut c, aliased).await, 2);
    assert_eq!(one(&mut c, "SELECT v FROM kv WHERE k = 'a'").await, s("11"));
    assert_eq!(
        affected(&mut c, "INSERT INTO kv VALUES ('z', 5) ON DUPLICATE KEY UPDATE v = 0").await,
        1
    );
    assert_eq!(one(&mut c, "SELECT COUNT(*) FROM kv").await, s("3"));

    assert_eq!(affected(&mut c, "INSERT INTO kv SET k = 'set', v = 42").await, 1);
    assert_eq!(one(&mut c, "SELECT v FROM kv WHERE k = 'set'").await, s("42"));

    c.query_drop(
        "CREATE TABLE m (id INT AUTO_INCREMENT PRIMARY KEY, a INT, b INT, UNIQUE KEY ab (a, b))",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO m (a, b) VALUES (1, 1), (1, 2)").await.unwrap();
    match c.query_drop("INSERT INTO m (a, b) VALUES (1, 1)").await {
        Err(mysql_async::Error::Server(e)) => {
            assert_eq!(e.message, "Duplicate entry '1-1' for key 'm.ab'")
        }
        other => panic!("{other:?}"),
    }
    // NULLs never collide in a UNIQUE key.
    c.query_drop("INSERT INTO m (a, b) VALUES (NULL, 1), (NULL, 1)").await.unwrap();
    assert_eq!(err_code(&mut c, "UPDATE m SET b = 1 WHERE a = 1 AND b = 2").await, 1062);

    // An explicit id moves AUTO_INCREMENT past it.
    c.query_drop("INSERT INTO m (id, a, b) VALUES (100, 5, 5)").await.unwrap();
    c.query_drop("INSERT INTO m (a, b) VALUES (6, 6)").await.unwrap();
    assert_eq!(c.last_insert_id(), Some(101));
    assert_eq!(one(&mut c, "SELECT LAST_INSERT_ID()").await, s("101"));
    c.query_drop("SELECT 1").await.unwrap();
    assert_eq!(one(&mut c, "SELECT LAST_INSERT_ID()").await, s("101"));

    c.query_drop("TRUNCATE TABLE m").await.unwrap();
    c.query_drop("INSERT INTO m (a, b) VALUES (1, 1)").await.unwrap();
    assert_eq!(one(&mut c, "SELECT id FROM m").await, s("1"));

    drop(c);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn nulls_defaults_and_strict_mode() {
    let (pool, mut c) = connect().await;
    c.query_drop(
        "CREATE TABLE n (id INT PRIMARY KEY, x INT DEFAULT 5, y VARCHAR(5) NOT NULL DEFAULT 'q')",
    )
    .await
    .unwrap();
    // An explicit NULL stays NULL; DEFAULT is only for omitted columns.
    c.query_drop("INSERT INTO n (id, x) VALUES (1, NULL)").await.unwrap();
    assert_eq!(rows(&mut c, "SELECT x, y FROM n WHERE id = 1").await, vec![vec![None, s("q")]]);
    c.query_drop("INSERT INTO n (id) VALUES (2)").await.unwrap();
    assert_eq!(one(&mut c, "SELECT x FROM n WHERE id = 2").await, s("5"));

    assert_eq!(err_code(&mut c, "INSERT INTO n (id, y) VALUES (3, NULL)").await, 1048);
    assert_eq!(err_code(&mut c, "UPDATE n SET y = NULL").await, 1048);
    assert_eq!(err_code(&mut c, "INSERT INTO n (id, y) VALUES (4, 'toolong')").await, 1406);

    // Column names are case-insensitive; unknown ones are an error, never NULL.
    assert_eq!(rows(&mut c, "SELECT ID, X FROM n WHERE Id = 2").await, vec![vec![s("2"), s("5")]]);
    assert_eq!(err_code(&mut c, "SELECT nosuch FROM n").await, 1054);
    assert_eq!(err_code(&mut c, "SELECT id FROM n WHERE nosuch = 1").await, 1054);

    assert_eq!(one(&mut c, "SELECT NULL + 1").await, None);
    assert_eq!(one(&mut c, "SELECT 10 / 4").await, s("2.5000"));

    drop(c);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn select_shaping_and_dml_order_limit() {
    let (pool, mut c) = connect().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, cat VARCHAR(5), amt DECIMAL(10,2))")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO t VALUES (1,'a',10.25),(2,'a',20.50),(3,'b',5.00),(4,'b',0.10),(5,'c',0.20)",
    )
    .await
    .unwrap();

    // LIMIT applies after aggregation, not before it.
    assert_eq!(one(&mut c, "SELECT COUNT(*) FROM t LIMIT 1").await, s("5"));
    assert_eq!(one(&mut c, "SELECT SUM(amt) FROM t").await, s("36.05"));
    assert_eq!(one(&mut c, "SELECT AVG(amt) FROM t WHERE cat = 'a'").await, s("15.375000"));
    assert_eq!(one(&mut c, "SELECT COUNT(DISTINCT cat) FROM t").await, s("3"));
    assert_eq!(rows(&mut c, "SELECT DISTINCT cat FROM t ORDER BY cat").await.len(), 3);
    assert_eq!(
        rows(
            &mut c,
            "SELECT cat, SUM(amt) AS total FROM t GROUP BY cat HAVING total > 5 ORDER BY total DESC"
        )
        .await,
        vec![vec![s("a"), s("30.75")], vec![s("b"), s("5.10")]]
    );
    assert_eq!(
        rows(&mut c, "SELECT cat, COUNT(*) FROM t GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1 OFFSET 1")
            .await,
        vec![vec![s("b"), s("2")]]
    );
    assert_eq!(
        one(&mut c, "SELECT GROUP_CONCAT(id ORDER BY id DESC SEPARATOR '|') FROM t").await,
        s("5|4|3|2|1")
    );

    // A zero-row result still describes its columns.
    let r: Vec<Row> = c.query("SELECT id, cat FROM t WHERE id > 100").await.unwrap();
    assert!(r.is_empty());

    assert_eq!(affected(&mut c, "DELETE FROM t WHERE cat = 'a' ORDER BY id DESC LIMIT 1").await, 1);
    assert_eq!(one(&mut c, "SELECT GROUP_CONCAT(id ORDER BY id) FROM t").await, s("1,3,4,5"));
    assert_eq!(affected(&mut c, "UPDATE t SET cat = 'z' ORDER BY id LIMIT 2").await, 2);
    assert_eq!(
        one(&mut c, "SELECT GROUP_CONCAT(id ORDER BY id) FROM t WHERE cat = 'z'").await,
        s("1,3")
    );

    drop(c);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn function_library() {
    let (pool, mut c) = connect().await;
    let cases = [
        ("SELECT IF(1 > 0, 'y', 'n')", "y"),
        ("SELECT ROUND(2.5)", "3"),
        ("SELECT ROUND(1.2345, 2)", "1.23"),
        ("SELECT FLOOR(1.7)", "1"),
        ("SELECT 7 DIV 2", "3"),
        ("SELECT CONCAT_WS('-', 'a', NULL, 'b')", "a-b"),
        ("SELECT TRIM(LEADING 'x' FROM 'xxaxx')", "axx"),
        ("SELECT LPAD('5', 3, '0')", "005"),
        ("SELECT LOCATE('L', 'hello')", "3"),
        ("SELECT CAST('42abc' AS SIGNED)", "42"),
        ("SELECT CAST('1.239' AS DECIMAL(5,2))", "1.24"),
        (
            "SELECT DATE_FORMAT('2024-03-05 14:07:09', '%W %M %D %Y %H:%i')",
            "Tuesday March 5th 2024 14:07",
        ),
        ("SELECT DATE_ADD('2024-01-31', INTERVAL 1 MONTH)", "2024-02-29"),
        ("SELECT '2024-01-01 23:00:00' + INTERVAL 2 HOUR", "2024-01-02 01:00:00"),
        ("SELECT TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-02-29')", "0"),
        ("SELECT DATEDIFF('2024-03-01', '2024-02-01')", "29"),
        ("SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')", "86400"),
        ("SELECT JSON_OBJECT('k', 1, 'b', 'x')", r#"{"b": "x", "k": 1}"#),
    ];
    for (sql, want) in cases {
        assert_eq!(one(&mut c, sql).await, s(want), "{sql}");
    }
    drop(c);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn json_enum_and_introspection() {
    let (pool, mut c) = connect().await;
    c.query_drop("CREATE TABLE j (id INT PRIMARY KEY, doc JSON)").await.unwrap();
    c.query_drop(r#"INSERT INTO j VALUES (1, '{"s": "hi", "a": {"b": [10, 20]}}')"#).await.unwrap();
    assert_eq!(one(&mut c, "SELECT doc FROM j").await, s(r#"{"a": {"b": [10, 20]}, "s": "hi"}"#));
    assert_eq!(one(&mut c, "SELECT doc->'$.a.b[1]' FROM j").await, s("20"));
    assert_eq!(one(&mut c, "SELECT id FROM j WHERE doc->>'$.s' = 'hi'").await, s("1"));
    assert_eq!(err_code(&mut c, "INSERT INTO j VALUES (2, '{bad')").await, 3140);

    c.query_drop(
        "CREATE TABLE e (id INT PRIMARY KEY, s ENUM('draft','published') NOT NULL DEFAULT 'draft')",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO e (id) VALUES (1)").await.unwrap();
    c.query_drop("INSERT INTO e VALUES (2, 'PUBLISHED')").await.unwrap();
    assert_eq!(
        one(&mut c, "SELECT GROUP_CONCAT(s ORDER BY id) FROM e").await,
        s("draft,published")
    );
    assert_eq!(err_code(&mut c, "INSERT INTO e VALUES (3, 'bogus')").await, 1265);

    // Migration tools re-run CREATE TABLE IF NOT EXISTS.
    c.query_drop("CREATE TABLE IF NOT EXISTS e (x INT)").await.unwrap();
    assert_eq!(err_code(&mut c, "DROP TABLE nosuch").await, 1051);
    c.query_drop("DROP TABLE IF EXISTS nosuch").await.unwrap();

    assert_eq!(
        rows(&mut c, "DESCRIBE e").await,
        vec![
            vec![s("id"), s("int"), s("NO"), s("PRI"), None, s("")],
            vec![s("s"), s("enum('draft','published')"), s("NO"), s(""), s("draft"), s("")],
        ]
    );
    assert_eq!(rows(&mut c, "SHOW TABLES LIKE 'j%'").await, vec![vec![s("j")]]);
    assert!(rows(&mut c, "SHOW FULL TABLES").await.contains(&vec![s("e"), s("BASE TABLE")]));
    assert_eq!(
        rows(
            &mut c,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'test' \
             AND table_type = 'BASE TABLE' ORDER BY table_name"
        )
        .await,
        vec![vec![s("e")], vec![s("j")]]
    );
    assert_eq!(
        rows(
            &mut c,
            "SELECT column_name, data_type, is_nullable, column_key FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'j' ORDER BY ordinal_position"
        )
        .await,
        vec![vec![s("id"), s("int"), s("NO"), s("PRI")], vec![s("doc"), s("json"), s("YES"), s("")]]
    );
    assert_eq!(rows(&mut c, "SHOW INDEX FROM e").await[0][2], s("PRIMARY"));
    c.query_drop("CREATE DATABASE other").await.unwrap();
    assert!(rows(&mut c, "SHOW DATABASES").await.contains(&vec![s("other")]));

    drop(c);
    pool.disconnect().await.unwrap();
}

/// Found by the differential suite against real MySQL: table aliases were
/// ignored, so `JOIN order_items oi ON oi.product_id = p.product_id`
/// compared `p.product_id` with itself and every join was a cartesian
/// product.
#[tokio::test]
async fn table_aliases_in_joins_and_dml() {
    let (pool, mut c) = connect().await;
    c.query_drop("CREATE TABLE products (product_id INT PRIMARY KEY, name VARCHAR(20))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE order_items (item_id INT PRIMARY KEY, product_id INT, qty INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO products VALUES (1, 'lamp'), (2, 'desk')").await.unwrap();
    c.query_drop("INSERT INTO order_items VALUES (10, 1, 3), (11, 1, 4), (12, 2, 1)")
        .await
        .unwrap();

    assert_eq!(
        rows(
            &mut c,
            "SELECT p.name, SUM(oi.qty) AS units FROM products p \
             JOIN order_items oi ON oi.product_id = p.product_id \
             GROUP BY p.name ORDER BY units DESC"
        )
        .await,
        vec![vec![s("lamp"), s("7")], vec![s("desk"), s("1")]]
    );
    // A bare column that both sides have is ambiguous, as in MySQL.
    assert_eq!(
        err_code(
            &mut c,
            "SELECT product_id FROM products p JOIN order_items oi ON oi.product_id = p.product_id"
        )
        .await,
        1052
    );
    // Once aliased, the table's own name no longer qualifies its columns.
    assert_eq!(
        err_code(
            &mut c,
            "SELECT products.name FROM products p JOIN order_items oi ON oi.product_id = p.product_id"
        )
        .await,
        1054
    );

    assert_eq!(
        affected(&mut c, "UPDATE order_items oi SET oi.qty = oi.qty + 1 WHERE oi.item_id = 12")
            .await,
        1
    );
    assert_eq!(one(&mut c, "SELECT qty FROM order_items WHERE item_id = 12").await, s("2"));
    assert_eq!(affected(&mut c, "DELETE FROM order_items oi WHERE oi.product_id = 2").await, 1);

    drop(c);
    pool.disconnect().await.unwrap();
}

/// pymysql/mysqlclient/SQLAlchemy/Django never send BEGIN: they turn
/// `autocommit` off and rely on MySQL opening a transaction implicitly.
/// That used to be ignored (every statement committed, ROLLBACK did
/// nothing); and a rollback used to restore the *whole* shared database,
/// wiping other connections' commits.
#[tokio::test]
async fn implicit_transactions_and_per_connection_rollback() {
    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let pool = Pool::new(format!("mysql://root@127.0.0.1:{}/test", addr.port()).as_str());
    let mut a = pool.get_conn().await.unwrap();
    let mut b = pool.get_conn().await.unwrap();
    a.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))").await.unwrap();
    a.query_drop("INSERT INTO t VALUES (1, 'orig')").await.unwrap();

    a.query_drop("SET autocommit = 0").await.unwrap();
    a.query_drop("INSERT INTO t VALUES (2, 'mine')").await.unwrap();
    a.query_drop("UPDATE t SET v = 'changed' WHERE id = 1").await.unwrap();
    // Another connection commits to the same table meanwhile.
    b.query_drop("INSERT INTO t VALUES (3, 'theirs')").await.unwrap();
    a.query_drop("ROLLBACK").await.unwrap();
    assert_eq!(
        rows(&mut b, "SELECT id, v FROM t ORDER BY id").await,
        vec![vec![s("1"), s("orig")], vec![s("3"), s("theirs")]]
    );

    a.query_drop("INSERT INTO t VALUES (4, 'kept')").await.unwrap();
    a.query_drop("COMMIT").await.unwrap();
    // DDL implicitly commits.
    a.query_drop("INSERT INTO t VALUES (5, 'ddl')").await.unwrap();
    a.query_drop("CREATE TABLE u (x INT)").await.unwrap();
    a.query_drop("ROLLBACK").await.unwrap();
    assert_eq!(one(&mut b, "SELECT COUNT(*) FROM t WHERE id IN (4, 5)").await, s("2"));

    // A connection that disconnects mid-transaction is rolled back.
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop("SET autocommit = 0").await.unwrap();
    c.query_drop("INSERT INTO t VALUES (6, 'gone')").await.unwrap();
    c.disconnect().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(one(&mut b, "SELECT COUNT(*) FROM t WHERE id = 6").await, s("0"));

    drop(a);
    drop(b);
    pool.disconnect().await.unwrap();
}

/// Prepared statements (the binary protocol: mysql2's execute(), Go's
/// database/sql, JDBC server-side prepares). Found via testing before a
/// public release: parameters were numbered in binding order rather than
/// textual order (`SELECT ? ... WHERE id = ?` swapped them), a `?` in
/// HAVING wasn't counted, `LIMIT ?` was rejected, and every result column
/// came back as a string.
#[tokio::test]
async fn prepared_statement_parameters_and_binary_types() {
    use mysql_async::Value as V;
    let (pool, mut c) = connect().await;
    c.query_drop(
        "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(10), qty INT, at DATETIME, price DECIMAL(6,2))",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO p VALUES (1, 'apple', 3, '2024-03-05 14:07:09', 9.99), \
         (2, 'pear', NULL, NULL, 0.10), (3, 'fig', 7, NULL, 1.00)",
    )
    .await
    .unwrap();

    let r: Vec<(String, String)> =
        c.exec("SELECT ? AS tag, name FROM p WHERE id = ?", ("x", 2)).await.unwrap();
    assert_eq!(r, vec![("x".to_string(), "pear".to_string())]);

    let r: Vec<String> = c
        .exec("SELECT name FROM p GROUP BY name HAVING COUNT(*) >= ? ORDER BY name", (1,))
        .await
        .unwrap();
    assert_eq!(r, vec!["apple", "fig", "pear"]);

    let r: Vec<String> =
        c.exec("SELECT name FROM p ORDER BY id LIMIT ? OFFSET ?", (1, 1)).await.unwrap();
    assert_eq!(r, vec!["pear"]);

    // Typed binary values, not strings.
    let row: mysql_async::Row =
        c.exec_first("SELECT id, qty, at, price FROM p WHERE id = ?", (1,)).await.unwrap().unwrap();
    assert_eq!(row.as_ref(0), Some(&V::Int(1)));
    assert_eq!(row.as_ref(1), Some(&V::Int(3)));
    assert_eq!(row.as_ref(2), Some(&V::Date(2024, 3, 5, 14, 7, 9, 0)));
    assert_eq!(row.as_ref(3), Some(&V::Bytes(b"9.99".to_vec())));

    // A DATETIME *parameter* in its binary layout (JDBC, Go, mysql2).
    c.exec_drop(
        "INSERT INTO p (id, name, at) VALUES (?, ?, ?)",
        (9, "date", V::Date(2025, 1, 2, 3, 4, 5, 600)),
    )
    .await
    .unwrap();
    assert_eq!(one(&mut c, "SELECT at FROM p WHERE id = 9").await, s("2025-01-02 03:04:05.000600"));

    drop(c);
    pool.disconnect().await.unwrap();
}

/// Migration tools (Django, Rails, Laravel, Alembic) all ALTER tables;
/// every ALTER used to be rejected. Also savepoints (Django's nested
/// `atomic()`) and the functions Django probes.
#[tokio::test]
async fn alter_table_savepoints_and_django_probes() {
    let (pool, mut c) = connect().await;
    c.query_drop("CREATE TABLE a (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(10))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO a (name) VALUES ('x'), ('y')").await.unwrap();
    c.query_drop("ALTER TABLE a ADD COLUMN qty INT NOT NULL DEFAULT 5 AFTER id").await.unwrap();
    assert_eq!(
        rows(&mut c, "SELECT * FROM a WHERE id = 1").await,
        vec![vec![s("1"), s("5"), s("x")]]
    );
    c.query_drop("ALTER TABLE a MODIFY name VARCHAR(50) NOT NULL").await.unwrap();
    c.query_drop("ALTER TABLE a CHANGE qty quantity BIGINT NOT NULL").await.unwrap();
    c.query_drop("ALTER TABLE a RENAME COLUMN name TO title").await.unwrap();
    c.query_drop("ALTER TABLE a ADD UNIQUE KEY uq_title (title)").await.unwrap();
    assert_eq!(err_code(&mut c, "INSERT INTO a (quantity, title) VALUES (1, 'x')").await, 1062);
    // A UNIQUE key over existing duplicates is refused and changes nothing.
    c.query_drop("INSERT INTO a (quantity, title) VALUES (5, 'z')").await.unwrap();
    assert_eq!(err_code(&mut c, "ALTER TABLE a ADD UNIQUE (quantity)").await, 1062);
    c.query_drop("ALTER TABLE a DROP INDEX uq_title").await.unwrap();
    c.query_drop("INSERT INTO a (quantity, title) VALUES (1, 'x')").await.unwrap();
    c.query_drop("ALTER TABLE a ALTER COLUMN quantity SET DEFAULT 9").await.unwrap();
    c.query_drop("ALTER TABLE a ADD CONSTRAINT fk FOREIGN KEY (quantity) REFERENCES b (id)")
        .await
        .unwrap();
    c.query_drop("ALTER TABLE a DROP COLUMN quantity").await.unwrap();
    c.query_drop("ALTER TABLE a RENAME TO items").await.unwrap();
    assert_eq!(
        rows(
            &mut c,
            "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name = 'items'"
        )
        .await,
        vec![vec![s("id")], vec![s("title")]]
    );
    c.query_drop("CREATE UNIQUE INDEX one_title ON items (title, id)").await.unwrap();

    c.query_drop("SET autocommit = 0").await.unwrap();
    c.query_drop("INSERT INTO items (title) VALUES ('keep')").await.unwrap();
    c.query_drop("SAVEPOINT s1").await.unwrap();
    c.query_drop("INSERT INTO items (title) VALUES ('drop')").await.unwrap();
    c.query_drop("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    c.query_drop("RELEASE SAVEPOINT s1").await.unwrap();
    c.query_drop("COMMIT").await.unwrap();
    assert_eq!(
        one(
            &mut c,
            "SELECT GROUP_CONCAT(title ORDER BY title) FROM items WHERE title IN ('keep', 'drop')"
        )
        .await,
        s("keep")
    );
    assert_eq!(
        one(&mut c, "SELECT CONVERT_TZ('2001-01-01 01:00:00', 'UTC', 'UTC') IS NOT NULL").await,
        s("1")
    );
    assert_eq!(one(&mut c, "SELECT JSON_CONTAINS('[1, 2, 3]', '2')").await, s("1"));

    drop(c);
    pool.disconnect().await.unwrap();
}
