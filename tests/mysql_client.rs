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
