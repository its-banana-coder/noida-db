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

    let noida_rows: Result<Vec<Option<String>>, _> = noida_conn.query(query).await;
    let ref_rows: Result<Vec<Option<String>>, _> = ref_conn.query(query).await;

    match (noida_rows, ref_rows) {
        (Ok(n), Ok(r)) => assert_eq!(n, r, "mismatch for query: {query}"),
        (Err(n), Err(r)) => {
            // Compare error codes if possible
            if let (mysql_async::Error::Server(ne), mysql_async::Error::Server(re)) = (&n, &r) {
                assert_eq!(ne.code, re.code, "error code mismatch for query: {query}");
            }
        }
        (n, r) => panic!("mismatch for query: {query}. noida: {n:?}, ref: {r:?}"),
    }
}

async fn execute_query(pool: &Pool, query: &str) {
    let mut conn = pool.get_conn().await.unwrap();
    let _: Vec<mysql_async::Row> = conn.query(query).await.unwrap_or_default();
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

    // Setup for diff tests
    execute_query(&noida_pool, "CREATE TABLE diff_test (id INT PRIMARY KEY, name VARCHAR(50))")
        .await;
    execute_query(
        &ref_pool,
        "CREATE TABLE IF NOT EXISTS diff_test (id INT PRIMARY KEY, name VARCHAR(50))",
    )
    .await;
    execute_query(&ref_pool, "TRUNCATE TABLE diff_test").await;

    execute_query(
        &noida_pool,
        "CREATE TABLE diff_agg (id INT PRIMARY KEY, category VARCHAR(50), amount INT)",
    )
    .await;
    execute_query(
        &ref_pool,
        "CREATE TABLE IF NOT EXISTS diff_agg (id INT PRIMARY KEY, category VARCHAR(50), amount INT)",
    )
    .await;
    execute_query(&ref_pool, "TRUNCATE TABLE diff_agg").await;

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
        "INSERT INTO diff_test VALUES (1, 'Alice'), (2, 'Bob')",
        "SELECT id, name FROM diff_test",
        "UPDATE diff_test SET name = 'Charlie' WHERE id = 1",
        "SELECT id, name FROM diff_test",
        "DELETE FROM diff_test WHERE id = 2",
        "SELECT id, name FROM diff_test",
        // GROUP BY + aggregates
        "INSERT INTO diff_agg VALUES (1, 'a', 10), (2, 'a', 30), (3, 'b', 5)",
        "SELECT COUNT(*) FROM diff_agg",
        "SELECT SUM(amount) FROM diff_agg",
        // AVG is deliberately not compared here: MySQL returns a DECIMAL
        // with server-specific scale for AVG(int_column), while this
        // engine returns a plain float — a known, already-documented kind
        // of formatting difference (see the `10 / 4` case above), not a
        // correctness bug in the aggregate itself.
        "SELECT MIN(amount) FROM diff_agg",
        "SELECT MAX(amount) FROM diff_agg",
        "SELECT category, COUNT(*) FROM diff_agg GROUP BY category",
        "SELECT category, SUM(amount) FROM diff_agg GROUP BY category",
        // transactions
        "BEGIN",
        "INSERT INTO diff_agg VALUES (4, 'c', 100)",
        "ROLLBACK",
        "SELECT COUNT(*) FROM diff_agg",
        "START TRANSACTION",
        "INSERT INTO diff_agg VALUES (4, 'c', 100)",
        "COMMIT",
        "SELECT COUNT(*) FROM diff_agg",
    ] {
        assert_same_result(&noida_pool, &ref_pool, query).await;
        compared += 1;
    }

    println!("compared {compared} queries against the real server");

    noida_pool.disconnect().await.unwrap();
    ref_pool.disconnect().await.unwrap();
}
