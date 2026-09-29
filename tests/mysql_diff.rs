//! Differential tests for the MySQL service: compares behavior against a
//! real MySQL server referenced by `NOIDA_MYSQL_REF=host:port` (falling
//! back to a locally running server on the default port 3306), the same
//! convention every other service's diff test uses.
#![cfg(feature = "mysql")]

use mysql_async::Pool;
use mysql_async::prelude::*;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

async fn get_reference_pool() -> Option<Pool> {
    let addr = if let Ok(addr) = std::env::var("NOIDA_MYSQL_REF") {
        addr
    } else if TcpStream::connect_timeout(
        &"127.0.0.1:3306".parse::<SocketAddr>().unwrap(),
        Duration::from_millis(50),
    )
    .is_ok()
    {
        "127.0.0.1:3306".to_string()
    } else {
        return None;
    };
    Some(Pool::new(format!("mysql://root@{addr}/mysql").as_str()))
}

fn start_noida_mysql() -> SocketAddr {
    noida::services::start("mysql", "127.0.0.1:0").unwrap().unwrap()
}

/// Runs `query` against both servers and asserts the returned rows match,
/// as `(text, i64)` pairs read back as strings for an easy comparison
/// (real MySQL and noida-db send integers as MySQL text-protocol
/// length-encoded strings on the wire either way).
async fn assert_same_result(noida_pool: &Pool, ref_pool: &Pool, query: &str) {
    let mut noida_conn = noida_pool.get_conn().await.unwrap();
    let mut ref_conn = ref_pool.get_conn().await.unwrap();

    let noida_rows: Vec<Option<String>> = noida_conn.query(query).await.unwrap();
    let ref_rows: Vec<Option<String>> = ref_conn.query(query).await.unwrap();

    assert_eq!(noida_rows, ref_rows, "mismatch for query: {query}");
}

#[tokio::test]
async fn test_mysql_diff() {
    let Some(ref_pool) = get_reference_pool().await else {
        println!(
            "SKIPPED: no reference MySQL server (set NOIDA_MYSQL_REF or run one on 127.0.0.1:3306)"
        );
        return;
    };

    let noida_addr = start_noida_mysql();
    let noida_pool = Pool::new(format!("mysql://root@{noida_addr}/test").as_str());

    let mut compared = 0;
    for query in [
        "SELECT 1",
        "SELECT 1 + 1",
        "SELECT 1 - 5",
        "SELECT 3 * 4",
        "SELECT 10 / 4",
        "SELECT 10 / 0",
        "SELECT 'hello'",
        "SELECT NULL",
        "SELECT 1 = 1",
        "SELECT 1 = 2",
    ] {
        assert_same_result(&noida_pool, &ref_pool, query).await;
        compared += 1;
    }

    println!("compared {compared} queries against the real server");

    noida_pool.disconnect().await.unwrap();
    ref_pool.disconnect().await.unwrap();
}
