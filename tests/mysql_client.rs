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

/// `SELECT *` and `SELECT table.*` were entirely unhandled before (hit
/// the generic "unsupported select item" error) -- arguably the single
/// most common `SELECT` shape of all. Covers both forms, real column
/// names for the expanded columns, and mixing a wildcard with an
/// explicit column in the same projection.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_select_star() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE star_test (id INT, name VARCHAR(255))").await.unwrap();
    conn.query_drop("INSERT INTO star_test (id, name) VALUES (1, 'a')").await.unwrap();

    // Bare `SELECT *`.
    let row: mysql_async::Row = conn.query_first("SELECT * FROM star_test").await.unwrap().unwrap();
    assert_eq!(row.get::<i32, _>("id"), Some(1));
    assert_eq!(row.get::<String, _>("name"), Some("a".to_string()));

    // Qualified `table.*` -- what WordPress's own queries use.
    let row: mysql_async::Row =
        conn.query_first("SELECT star_test.* FROM star_test").await.unwrap().unwrap();
    assert_eq!(row.get::<i32, _>("id"), Some(1));
    assert_eq!(row.get::<String, _>("name"), Some("a".to_string()));

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// A column with an explicit `DEFAULT` (e.g. WordPress's own
/// `comment_count bigint(20) NOT NULL default '0'`) used to be silently
/// discarded entirely at `CREATE TABLE` time, so omitting that column
/// from an `INSERT`'s column list incorrectly hit MySQL's "doesn't have
/// a default value" error even though the schema had one.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_create_table_default_clause() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop(
        "CREATE TABLE default_test (id INT, comment_count BIGINT NOT NULL DEFAULT '0')",
    )
    .await
    .unwrap();

    // Omits comment_count entirely -- must fall back to the real default,
    // not error.
    conn.query_drop("INSERT INTO default_test (id) VALUES (1)").await.unwrap();
    let row: (i32, i64) =
        conn.query_first("SELECT id, comment_count FROM default_test").await.unwrap().unwrap();
    assert_eq!(row, (1, 0));

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `TINYTEXT` (alongside the already-supported `TEXT`/`MEDIUMTEXT`/
/// `LONGTEXT`) -- WordPress's own `wp_comments.comment_author` column
/// uses it, and its `CREATE TABLE` silently failed without this (the
/// comments table just never got created, without erroring the whole
/// install).
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_tinytext_column_type() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE tinytext_test (author TINYTEXT NOT NULL)").await.unwrap();
    conn.query_drop("INSERT INTO tinytext_test (author) VALUES ('a commenter')").await.unwrap();
    let v: String = conn.query_first("SELECT author FROM tinytext_test").await.unwrap().unwrap();
    assert_eq!(v, "a commenter");

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `SQL_CALC_FOUND_ROWS` + a later `SELECT FOUND_ROWS()` -- exactly what
/// WordPress's `WP_Query` uses for pagination (`set_found_posts()`).
/// `FOUND_ROWS()` reports the row count from BEFORE `LIMIT` truncation,
/// and persists across intervening statements (a plain `SELECT` in
/// between doesn't reset it), matching real MySQL.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_found_rows() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE found_rows_test (id INT)").await.unwrap();
    conn.query_drop("INSERT INTO found_rows_test (id) VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let ids: Vec<i32> = conn
        .query("SELECT SQL_CALC_FOUND_ROWS id FROM found_rows_test ORDER BY id LIMIT 2")
        .await
        .unwrap();
    assert_eq!(ids, vec![1, 2]);

    let found: i64 = conn.query_first("SELECT FOUND_ROWS()").await.unwrap().unwrap();
    assert_eq!(found, 5);

    // Persists across an intervening statement.
    let _: Vec<i32> = conn.query("SELECT id FROM found_rows_test").await.unwrap();
    let found: i64 = conn.query_first("SELECT FOUND_ROWS()").await.unwrap().unwrap();
    assert_eq!(found, 5);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// A database named directly in the connection string (`mysql://user@host/db`,
/// what every real client -- `mysqli_real_connect()`, PyMySQL, node-mysql2,
/// this very test's own `Pool::new()` -- actually does) must be selected
/// from the handshake response itself, not require a later explicit `USE`.
/// Without this, only a client that happened to also issue `USE` (as this
/// project's own earlier tests all did, as a workaround) ever saw a
/// database selected at all.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_database_selected_via_connection_string() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    // No `USE test` here -- the connection string's own `/test` must be
    // enough for this to succeed.
    conn.query_drop("CREATE TABLE handshake_db_test (id INT)").await.unwrap();
    conn.query_drop("INSERT INTO handshake_db_test (id) VALUES (1)").await.unwrap();
    let id: i32 = conn.query_first("SELECT id FROM handshake_db_test").await.unwrap().unwrap();
    assert_eq!(id, 1);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// Comparing a string-typed column against a numeric literal (`WHERE
/// varchar_col = 4`) must coerce the string to a number like real MySQL
/// does, not hard-error -- found via a real WordPress REST API request
/// (`WP_Query`'s revision-count query joins `wp_posts.post_parent` against
/// an integer literal) that noida-db failed with "Truncated incorrect
/// DOUBLE value" where real MySQL just returns the matching rows.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_text_column_compared_to_int_literal() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("USE test").await.unwrap();
    conn.query_drop("CREATE TABLE text_cmp_test (parent VARCHAR(20))").await.unwrap();
    conn.query_drop("INSERT INTO text_cmp_test (parent) VALUES ('4'), ('0'), ('abc')")
        .await
        .unwrap();

    let matches: Vec<String> =
        conn.query("SELECT parent FROM text_cmp_test WHERE parent = 4").await.unwrap();
    assert_eq!(matches, vec!["4".to_string()]);

    // A non-numeric string coerces to 0, matching 0 but not 4 -- it must
    // not error the whole query either.
    let zero_matches: Vec<String> =
        conn.query("SELECT parent FROM text_cmp_test WHERE parent = 0").await.unwrap();
    let mut zero_matches = zero_matches;
    zero_matches.sort();
    assert_eq!(zero_matches, vec!["0".to_string(), "abc".to_string()]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `HAVING` filtering groups by an aggregate expression (the common,
/// overwhelming case -- `HAVING COUNT(*) > N`, not a SELECT-list alias).
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_having() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("CREATE TABLE having_test (id INT, category VARCHAR(20), amount INT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO having_test VALUES (1, 'a', 10), (2, 'a', 30), (3, 'b', 5), (4, 'c', 1), (5, 'c', 2)",
    )
    .await
    .unwrap();

    // HAVING COUNT(*) > 1 -- categories 'a' and 'c' have 2 rows each, 'b' has 1.
    let mut rows: Vec<(String, i64)> = conn
        .query("SELECT category, COUNT(*) FROM having_test GROUP BY category HAVING COUNT(*) > 1")
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("a".to_string(), 2), ("c".to_string(), 2)]);

    // HAVING on a SUM, combined with WHERE filtering rows before grouping.
    let mut rows: Vec<(String, i64)> = conn
        .query(
            "SELECT category, SUM(amount) FROM having_test WHERE amount > 0 GROUP BY category HAVING SUM(amount) >= 10",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("a".to_string(), 40)]);

    // No GROUP BY at all: HAVING still applies to the single implicit group.
    let no_match: Vec<i64> =
        conn.query("SELECT COUNT(*) FROM having_test HAVING COUNT(*) > 100").await.unwrap();
    assert!(no_match.is_empty());
    let one_match: Vec<i64> =
        conn.query("SELECT COUNT(*) FROM having_test HAVING COUNT(*) = 5").await.unwrap();
    assert_eq!(one_match, vec![5]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// Found via extensive real-client testing before a public release: the
/// MySQL expression binder only handled literals, the 5 aggregate
/// functions, basic comparisons/AND/OR, +-*/, and IN-list -- LIKE, IS
/// NULL, BETWEEN, CASE WHEN, unary NOT, modulo, and every scalar function
/// (including CONCAT) all failed with a generic "unsupported" error. This
/// covers the fix for each one.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_expression_coverage() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("CREATE TABLE expr_test (id INT, name VARCHAR(50), age INT)").await.unwrap();
    conn.query_drop(
        "INSERT INTO expr_test VALUES (1, 'alice', 30), (2, NULL, 25), (3, 'carol', NULL)",
    )
    .await
    .unwrap();

    // LIKE, case-insensitive, matching MySQL's default collation.
    let names: Vec<String> =
        conn.query("SELECT name FROM expr_test WHERE name LIKE 'A%'").await.unwrap();
    assert_eq!(names, vec!["alice".to_string()]);

    // IS NULL / IS NOT NULL.
    let ids: Vec<i64> = conn.query("SELECT id FROM expr_test WHERE name IS NULL").await.unwrap();
    assert_eq!(ids, vec![2]);
    let ids: Vec<i64> = conn.query("SELECT id FROM expr_test WHERE age IS NOT NULL").await.unwrap();
    assert_eq!(ids, vec![1, 2]);

    // BETWEEN.
    let ids: Vec<i64> =
        conn.query("SELECT id FROM expr_test WHERE age BETWEEN 20 AND 28").await.unwrap();
    assert_eq!(ids, vec![2]);

    // CASE WHEN (searched) and simple CASE (operand form).
    let labels: Vec<String> = conn
        .query(
            "SELECT CASE WHEN age > 28 THEN 'old' WHEN age IS NULL THEN 'unknown' ELSE 'young' END FROM expr_test ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(labels, vec!["old".to_string(), "young".to_string(), "unknown".to_string()]);
    let labels: Vec<String> = conn
        .query("SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM expr_test ORDER BY id")
        .await
        .unwrap();
    assert_eq!(labels, vec!["one".to_string(), "two".to_string(), "other".to_string()]);

    // Unary NOT.
    let ids: Vec<i64> = conn.query("SELECT id FROM expr_test WHERE NOT id = 1").await.unwrap();
    assert_eq!(ids, vec![2, 3]);

    // Modulo.
    let rems: Vec<i64> = conn.query("SELECT id % 2 FROM expr_test ORDER BY id").await.unwrap();
    assert_eq!(rems, vec![1, 0, 1]);

    // Scalar functions.
    let concatenated: Vec<String> =
        conn.query("SELECT CONCAT(name, '!') FROM expr_test WHERE id = 1").await.unwrap();
    assert_eq!(concatenated, vec!["alice!".to_string()]);
    let upper: Vec<String> =
        conn.query("SELECT UPPER(name) FROM expr_test WHERE id = 1").await.unwrap();
    assert_eq!(upper, vec!["ALICE".to_string()]);
    let lower: Vec<String> =
        conn.query("SELECT LOWER(name) FROM expr_test WHERE id = 3").await.unwrap();
    assert_eq!(lower, vec!["carol".to_string()]);
    let len: Vec<i64> =
        conn.query("SELECT LENGTH(name) FROM expr_test WHERE id = 1").await.unwrap();
    assert_eq!(len, vec![5]);
    let sub: Vec<String> =
        conn.query("SELECT SUBSTRING(name, 1, 3) FROM expr_test WHERE id = 1").await.unwrap();
    assert_eq!(sub, vec!["ali".to_string()]);
    let coalesced: Vec<String> =
        conn.query("SELECT COALESCE(name, 'default') FROM expr_test WHERE id = 2").await.unwrap();
    assert_eq!(coalesced, vec!["default".to_string()]);
    let ifnulled: Vec<i64> =
        conn.query("SELECT IFNULL(age, -1) FROM expr_test WHERE id = 3").await.unwrap();
    assert_eq!(ifnulled, vec![-1]);

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// `CREATE DATABASE` and standalone `CREATE INDEX` both fell through to
/// the same generic "unsupported statement" error before this fix --
/// found via fresh-install testing, significant because almost any real
/// migration tool issues both before touching anything else.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_create_database_and_index() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("CREATE DATABASE newdb").await.unwrap();
    conn.query_drop("CREATE DATABASE IF NOT EXISTS newdb").await.unwrap(); // no error on repeat
    conn.query_drop("CREATE TABLE newdb.t (id INT)").await.unwrap();

    conn.query_drop("CREATE TABLE idx_test (id INT, name VARCHAR(50))").await.unwrap();
    conn.query_drop("CREATE INDEX idx_name ON idx_test (name)").await.unwrap();

    // A real duplicate-database error when IF NOT EXISTS isn't given.
    let err = conn.query_drop("CREATE DATABASE newdb").await;
    assert!(err.is_err());

    // A real error for an index on a nonexistent column.
    let err = conn.query_drop("CREATE INDEX bad ON idx_test (nope)").await;
    assert!(err.is_err());

    drop(conn);
    pool.disconnect().await.unwrap();
}

/// Found via extensive real-client testing before a public release: every
/// JOIN's ON condition was evaluated with no table context at all, so
/// every column reference inside it resolved to NULL unconditionally --
/// the condition was therefore always NULL, never true, and every single
/// JOIN (of any kind) silently returned the wrong result (INNER: always
/// empty; LEFT: always every left row with the right side all-NULL) with
/// no error. This is likely the single most severe bug found this
/// session -- JOIN is about as fundamental to SQL as it gets.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_joins_resolve_columns_correctly() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();
    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    conn.query_drop("CREATE TABLE join_customers (id INT, name VARCHAR(50))").await.unwrap();
    conn.query_drop("CREATE TABLE join_orders (id INT, customer_id INT, amount INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO join_customers VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO join_orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 75)")
        .await
        .unwrap();

    // Plain, bare JOIN (no INNER keyword) -- a separate AST node from
    // `INNER JOIN`, and was entirely unhandled on its own before this fix.
    let mut rows: Vec<(String, i64)> = conn
        .query(
            "SELECT join_customers.name, join_orders.amount FROM join_customers JOIN join_orders ON join_customers.id = join_orders.customer_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![("alice".to_string(), 50), ("alice".to_string(), 100), ("bob".to_string(), 75),]
    );

    // INNER JOIN, explicit keyword.
    let mut rows: Vec<(String, i64)> = conn
        .query(
            "SELECT join_customers.name, join_orders.amount FROM join_customers INNER JOIN join_orders ON join_customers.id = join_orders.customer_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows.len(), 3);

    // LEFT JOIN: carol has no orders, must still appear with NULL amount.
    let mut rows: Vec<(String, Option<i64>)> = conn
        .query(
            "SELECT join_customers.name, join_orders.amount FROM join_customers LEFT JOIN join_orders ON join_customers.id = join_orders.customer_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("alice".to_string(), Some(50)),
            ("alice".to_string(), Some(100)),
            ("bob".to_string(), Some(75)),
            ("carol".to_string(), None),
        ]
    );

    // The critical regression: both tables have their own `id` column.
    // Before this fix, a qualified reference to either side's `id`
    // silently resolved to whichever table's `id` happened to come first
    // in the join, regardless of which one was actually named.
    let mut rows: Vec<(i64, i64, i64, String)> = conn
        .query(
            "SELECT join_orders.id, join_orders.customer_id, join_customers.id, join_customers.name FROM join_customers JOIN join_orders ON join_customers.id = join_orders.customer_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, 1, 1, "alice".to_string()),
            (2, 1, 1, "alice".to_string()),
            (3, 2, 2, "bob".to_string()),
        ]
    );

    // RIGHT JOIN -- every order preserved, matched to its real customer
    // (this specifically exercises the side-swap implementation, which
    // would silently produce garbage without the qualifier fix above).
    let mut rows: Vec<(String, i64)> = conn
        .query(
            "SELECT join_customers.name, join_orders.amount FROM join_customers RIGHT JOIN join_orders ON join_customers.id = join_orders.customer_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![("alice".to_string(), 50), ("alice".to_string(), 100), ("bob".to_string(), 75),]
    );

    // GROUP BY/aggregate over a joined table -- the same table-context
    // resolution feeds Plan::Aggregate too, not just Project/Filter.
    let mut rows: Vec<(String, i64)> = conn
        .query(
            "SELECT join_customers.name, SUM(join_orders.amount) FROM join_customers JOIN join_orders ON join_customers.id = join_orders.customer_id GROUP BY join_customers.name",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("alice".to_string(), 150), ("bob".to_string(), 75)]);

    // Three-way join, every table sharing an `id` column -- the deepest
    // case the qualifier fix needs to keep working (incremental merge of
    // an already-qualified context with a third table).
    conn.query_drop("CREATE TABLE join_items (id INT, order_id INT, sku VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO join_items VALUES (1, 1, 'A1'), (2, 1, 'A2'), (3, 3, 'B1')")
        .await
        .unwrap();
    let mut rows: Vec<(String, i64, String)> = conn
        .query(
            "SELECT join_customers.name, join_items.id, join_items.sku FROM join_customers \
             JOIN join_orders ON join_customers.id = join_orders.customer_id \
             JOIN join_items ON join_orders.id = join_items.order_id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("alice".to_string(), 1, "A1".to_string()),
            ("alice".to_string(), 2, "A2".to_string()),
            ("bob".to_string(), 3, "B1".to_string()),
        ]
    );

    drop(conn);
    pool.disconnect().await.unwrap();
}
