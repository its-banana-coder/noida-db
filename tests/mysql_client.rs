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
