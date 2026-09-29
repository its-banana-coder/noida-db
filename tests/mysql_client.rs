#[cfg(feature = "mysql")]
#[tokio::test]
async fn test_mysql_client() {
    use mysql_async::Pool;
    use mysql_async::prelude::*;

    let addr = noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap();

    let url = format!("mysql://root@127.0.0.1:{}/test", addr.port());
    let pool = Pool::new(url.as_str());
    let mut conn = pool.get_conn().await.unwrap();

    let res: Vec<i32> = conn.query("SELECT 1").await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0], 1);

    drop(conn);
    pool.disconnect().await.unwrap();
}
