#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_client() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();

    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();

    let res: Vec<i32> = conn.query("SELECT 1").await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], 1);

    conn.query_drop("CREATE TABLE test_table (id INT, value VARCHAR(255))").await.unwrap();
    conn.query_drop("INSERT INTO test_table (id, value) VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();

    let res: Vec<(i32, String)> = conn.query("SELECT id, value FROM test_table").await.unwrap();
    assert_eq!(res.len(), 2);
    // order is not guaranteed, but they are appended.
    // just check that the sum of id is 3
    assert_eq!(res[0].0 + res[1].0, 3);

    conn.query_drop("UPDATE test_table SET value = 'updated' WHERE id = 1").await.unwrap();
    let res: Vec<(i32, String)> =
        conn.query("SELECT id, value FROM test_table WHERE id = 1").await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], (1, "updated".to_string()));

    conn.query_drop("DELETE FROM test_table WHERE id = 2").await.unwrap();
    let res: Vec<(i32, String)> = conn.query("SELECT id, value FROM test_table").await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], (1, "updated".to_string()));

    drop(conn);
    pool.disconnect().await.unwrap();
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_prepared_statement_params() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE ps_test (id INT, value VARCHAR(255))").await.unwrap();
    conn.exec_batch(
        "INSERT INTO ps_test (id, value) VALUES (?, ?)",
        vec![(1, "one"), (2, "two"), (3, "three")],
    )
    .await
    .unwrap();

    // A real bound parameter must actually select the matching row, not
    // whatever (if anything) was literally in the PREPARE text.
    let value: Option<String> =
        conn.exec_first("SELECT value FROM ps_test WHERE id = ?", (2,)).await.unwrap();
    assert_eq!(value, Some("two".to_string()));

    // Re-executing the same prepared statement with a different bound
    // value must return the new row, proving EXECUTE isn't just replaying
    // whatever the first EXECUTE saw.
    let value: Option<String> =
        conn.exec_first("SELECT value FROM ps_test WHERE id = ?", (3,)).await.unwrap();
    assert_eq!(value, Some("three".to_string()));

    // A prepared UPDATE with a bound parameter.
    conn.exec_drop("UPDATE ps_test SET value = ? WHERE id = ?", ("updated", 1)).await.unwrap();
    let value: Option<String> =
        conn.exec_first("SELECT value FROM ps_test WHERE id = ?", (1,)).await.unwrap();
    assert_eq!(value, Some("updated".to_string()));

    drop(conn);
    pool.disconnect().await.unwrap();
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_group_by_aggregates() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE agg_test (id INT, category VARCHAR(50), amount INT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO agg_test (id, category, amount) VALUES \
         (1, 'a', 10), (2, 'a', 30), (3, 'b', 5)",
    )
    .await
    .unwrap();

    let (count, sum): (i64, i64) =
        conn.query_first("SELECT COUNT(*), SUM(amount) FROM agg_test").await.unwrap().unwrap();
    assert_eq!(count, 3);
    assert_eq!(sum, 45);

    let mut rows: Vec<(String, i64, i64)> = conn
        .query("SELECT category, COUNT(*), SUM(amount) FROM agg_test GROUP BY category")
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("a".to_string(), 2, 40), ("b".to_string(), 1, 5)]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_transactions() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE tx_test (id INT, value VARCHAR(255))").await.unwrap();
    conn.query_drop("INSERT INTO tx_test (id, value) VALUES (1, 'first')").await.unwrap();

    // Rollback undoes everything done since BEGIN.
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("INSERT INTO tx_test (id, value) VALUES (2, 'second')").await.unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();

    let rows: Vec<i32> = conn.query("SELECT id FROM tx_test").await.unwrap();
    assert_eq!(rows, vec![1]);

    // Commit keeps everything done since BEGIN.
    conn.query_drop("START TRANSACTION").await.unwrap();
    conn.query_drop("INSERT INTO tx_test (id, value) VALUES (2, 'second')").await.unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    let mut rows: Vec<i32> = conn.query("SELECT id FROM tx_test").await.unwrap();
    rows.sort();
    assert_eq!(rows, vec![1, 2]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_affected_rows() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE ar_test (id INT, value VARCHAR(255))").await.unwrap();

    conn.query_drop("INSERT INTO ar_test (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    assert_eq!(conn.affected_rows(), 3);

    conn.query_drop("UPDATE ar_test SET value = 'z' WHERE id <= 2").await.unwrap();
    assert_eq!(conn.affected_rows(), 2);

    conn.query_drop("DELETE FROM ar_test WHERE id = 3").await.unwrap();
    assert_eq!(conn.affected_rows(), 1);

    drop(conn);
    pool.disconnect().await.unwrap();
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_last_insert_id() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop(
        "CREATE TABLE lii_test (id INT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(255))",
    )
    .await
    .unwrap();

    conn.query_drop("INSERT INTO lii_test (value) VALUES ('a')").await.unwrap();
    assert_eq!(conn.last_insert_id(), Some(1));

    conn.query_drop("INSERT INTO lii_test (value) VALUES ('b')").await.unwrap();
    assert_eq!(conn.last_insert_id(), Some(2));

    // A multi-row INSERT reports the id generated for the *first* row, not
    // the last -- matching real MySQL's `LAST_INSERT_ID()`/OK-packet
    // semantics.
    conn.query_drop("INSERT INTO lii_test (value) VALUES ('c'), ('d'), ('e')").await.unwrap();
    assert_eq!(conn.last_insert_id(), Some(3));

    // A statement that doesn't generate an id (a SELECT here) reports 0/None
    // in its own OK-packet field -- matching real MySQL, where
    // `mysqli_insert_id()` only reflects the immediately preceding
    // statement, not a persisted session value (that's what the separate
    // `LAST_INSERT_ID()` SQL function is for).
    let _res: Vec<(i32, String)> = conn.query("SELECT id, value FROM lii_test").await.unwrap();
    assert_eq!(conn.last_insert_id(), None);

    // Explicit non-NULL values into an AUTO_INCREMENT column don't
    // generate a new id either.
    conn.query_drop("INSERT INTO lii_test (id, value) VALUES (100, 'explicit')").await.unwrap();
    assert_eq!(conn.last_insert_id(), None);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// WordPress's core schema always defines its primary key (and any unique
/// keys) as a table-level clause, never as a column option -- e.g.
/// `wp_options` ends its `CREATE TABLE` with
/// `PRIMARY KEY  (option_id), UNIQUE KEY option_name (option_name)`. This
/// used to be silently discarded entirely (not even a parse error), which
/// meant `CREATE TABLE` "succeeded" but lost the primary-key metadata.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_table_level_primary_key() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop(
        "CREATE TABLE wp_options_like2 (\
            option_id BIGINT UNSIGNED AUTO_INCREMENT, \
            option_name VARCHAR(191) NOT NULL DEFAULT '', \
            PRIMARY KEY (option_id), \
            UNIQUE KEY option_name (option_name)\
        )",
    )
    .await
    .unwrap();

    // AUTO_INCREMENT still works when the column is only marked as a
    // primary key via the table-level clause, not a column option.
    conn.query_drop("INSERT INTO wp_options_like2 (option_name) VALUES ('siteurl')").await.unwrap();
    assert_eq!(conn.last_insert_id(), Some(1));

    let res: Vec<(i64, String)> =
        conn.query("SELECT option_id, option_name FROM wp_options_like2").await.unwrap();
    assert_eq!(res, vec![(1, "siteurl".to_string())]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `BIGINT UNSIGNED`, `TINYINT`, `MEDIUMTEXT` and `LONGTEXT` are exactly
/// the column types WordPress's core schema (`wp_options`, `wp_posts`,
/// `wp_users`, etc.) uses that this engine didn't accept before --
/// `CREATE TABLE` failed outright on all of them.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_wordpress_style_column_types() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop(
        "CREATE TABLE wp_options_like (\
            option_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
            autoload TINYINT NOT NULL, \
            option_value MEDIUMTEXT NOT NULL, \
            option_notes LONGTEXT\
        )",
    )
    .await
    .unwrap();

    conn.query_drop(
        "INSERT INTO wp_options_like (autoload, option_value, option_notes) \
         VALUES (1, 'a mediumtext value', 'a longtext value')",
    )
    .await
    .unwrap();

    let res: Vec<(i64, i32, String, String)> = conn
        .query("SELECT option_id, autoload, option_value, option_notes FROM wp_options_like")
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], (1, 1, "a mediumtext value".to_string(), "a longtext value".to_string()));

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// Data written on one connection must be visible from a completely
/// separate connection -- the exact pattern a real app hits constantly
/// (a fresh connection per request/process, a connection pool
/// reconnecting, ...). This is precisely what WordPress's `wp core
/// install` (one PHP process/connection) followed by `wp db tables` (a
/// separate one) does, and it used to fail with "the site you have
/// requested is not installed" because each connection got its own
/// fresh, empty, unshared database.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_data_visible_across_connections() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());

    {
        let pool = Pool::new(url.as_str());
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("USE test").await.unwrap();
        conn.query_drop("CREATE TABLE cross_conn_test (id INT, value VARCHAR(255))").await.unwrap();
        conn.query_drop("INSERT INTO cross_conn_test (id, value) VALUES (1, 'from conn 1')")
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.unwrap();
    }

    // A brand new pool/connection to the same still-running server.
    {
        let pool = Pool::new(url.as_str());
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop("USE test").await.unwrap();
        let res: Vec<(i32, String)> =
            conn.query("SELECT id, value FROM cross_conn_test").await.unwrap();
        assert_eq!(res, vec![(1, "from conn 1".to_string())]);
        drop(conn);
        pool.disconnect().await.unwrap();
    }
}

/// `WHERE col IN (...)` -- used constantly by real apps (WordPress's own
/// option-priming query, `SELECT option_name, option_value FROM wp_options
/// WHERE option_name IN (...)`, runs on every single page load) but was
/// entirely unhandled before, hitting the generic "expr" unsupported
/// error. Also covers `NOT IN`, a NULL in the list/column, and a
/// parenthesized (nested) boolean expression, another gap this shares a
/// root cause with (`AstExpr::Nested` was unhandled too).
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_in_list() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE in_test (id INT, name VARCHAR(255))").await.unwrap();
    conn.query_drop(
        "INSERT INTO in_test (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, NULL)",
    )
    .await
    .unwrap();

    let mut res: Vec<i32> =
        conn.query("SELECT id FROM in_test WHERE name IN ('a', 'c')").await.unwrap();
    res.sort();
    assert_eq!(res, vec![1, 3]);

    let mut res: Vec<i32> =
        conn.query("SELECT id FROM in_test WHERE name NOT IN ('a', 'c')").await.unwrap();
    res.sort();
    // id=4's name is NULL, so `NULL NOT IN (...)` is NULL (excluded), not true.
    assert_eq!(res, vec![2]);

    // A parenthesized boolean expression (`AstExpr::Nested`), the other
    // gap found alongside IN.
    let mut res: Vec<i32> =
        conn.query("SELECT id FROM in_test WHERE (id = 1 OR id = 2)").await.unwrap();
    res.sort();
    assert_eq!(res, vec![1, 2]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `ORDER BY`, `LIMIT`/`OFFSET` (both the standard and MySQL's own
/// `LIMIT offset, limit` syntax), qualified column references
/// (`table.col`, `AstExpr::CompoundIdentifier`), and `ORDER BY` on a
/// column that isn't in the `SELECT` list at all -- all four were
/// entirely unhandled before. This is exactly the shape of WordPress's
/// own frontend post-listing query (`SELECT ... wp_posts.ID FROM wp_posts
/// WHERE ... ORDER BY wp_posts.post_date DESC LIMIT 0, 1`), which is what
/// surfaced the gap.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_order_by_limit() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE ob_test (id INT, created INT)").await.unwrap();
    conn.query_drop(
        "INSERT INTO ob_test (id, created) VALUES (1, 30), (2, 10), (3, 20), (4, NULL)",
    )
    .await
    .unwrap();

    // Plain ORDER BY ASC (default), NULL sorts first.
    let res: Vec<i32> = conn.query("SELECT id FROM ob_test ORDER BY created").await.unwrap();
    assert_eq!(res, vec![4, 2, 3, 1]);

    // DESC, qualified column reference, and a column not in the SELECT
    // list -- exactly WordPress's own query shape.
    let res: Vec<i32> =
        conn.query("SELECT ob_test.id FROM ob_test ORDER BY ob_test.created DESC").await.unwrap();
    assert_eq!(res, vec![1, 3, 2, 4]);

    // Standard LIMIT/OFFSET.
    let res: Vec<i32> =
        conn.query("SELECT id FROM ob_test ORDER BY created DESC LIMIT 2 OFFSET 1").await.unwrap();
    assert_eq!(res, vec![3, 2]);

    // MySQL's own `LIMIT offset, limit` syntax (order reversed from the
    // standard form above) -- what WordPress's own query actually uses.
    let res: Vec<i32> =
        conn.query("SELECT id FROM ob_test ORDER BY created DESC LIMIT 1, 2").await.unwrap();
    assert_eq!(res, vec![3, 2]);

    // A bare LIMIT with no ORDER BY still works.
    let res: Vec<i32> = conn.query("SELECT id FROM ob_test LIMIT 2").await.unwrap();
    assert_eq!(res.len(), 2);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// Real MySQL reports the actual selected column's name in the wire
/// column-definition packets, not a placeholder -- this is what lets a
/// real client fetch a row by column name, e.g. PHP's `mysqli`/`$wpdb`
/// (what WordPress uses everywhere via `stdClass` rows) or PDO's
/// associative fetch mode. Covers COM_QUERY (text protocol) and a
/// prepared statement's COM_STMT_EXECUTE (binary protocol), both an
/// unaliased plain column and an explicit alias.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_real_column_names() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop(
        "CREATE TABLE col_name_test (option_name VARCHAR(255), option_value VARCHAR(255))",
    )
    .await
    .unwrap();
    conn.query_drop(
        "INSERT INTO col_name_test (option_name, option_value) VALUES ('siteurl', 'http://x')",
    )
    .await
    .unwrap();

    // COM_QUERY: unaliased plain columns.
    let row: mysql_async::Row = conn
        .query_first("SELECT option_name, option_value FROM col_name_test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<String, _>("option_name"), Some("siteurl".to_string()));
    assert_eq!(row.get::<String, _>("option_value"), Some("http://x".to_string()));

    // COM_QUERY: an explicit alias.
    let row: mysql_async::Row =
        conn.query_first("SELECT option_value AS val FROM col_name_test").await.unwrap().unwrap();
    assert_eq!(row.get::<String, _>("val"), Some("http://x".to_string()));

    // COM_STMT_EXECUTE (prepared statement, binary protocol row format).
    let stmt = conn
        .prep("SELECT option_name, option_value FROM col_name_test WHERE option_name = ?")
        .await
        .unwrap();
    let row: mysql_async::Row = conn.exec_first(&stmt, ("siteurl",)).await.unwrap().unwrap();
    assert_eq!(row.get::<String, _>("option_name"), Some("siteurl".to_string()));
    assert_eq!(row.get::<String, _>("option_value"), Some("http://x".to_string()));

    drop(conn);
    pool.disconnect().await.unwrap();
}
