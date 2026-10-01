//! Real on-disk persistence for MySQL: save on a clean "shutdown" (simulated
//! directly here via the save closure `spawn_persistent_for_test` returns,
//! not a real SIGTERM -- unreliable to send/observe in a test), load back
//! into a fresh server, and confirm column types and schema-level state
//! round-trips correctly.

#[cfg(feature = "mysql")]
mod tests {
    use mysql_async::Pool;
    use mysql_async::prelude::*;
    use noida::mysql::server::spawn_persistent_for_test;
    use std::net::SocketAddr;

    async fn make_pool(addr: SocketAddr) -> Pool {
        let url = format!("mysql://root@{}/test", addr);
        Pool::new(url.as_str())
    }

    #[tokio::test]
    async fn persists_tables_and_schema_across_a_restart() {
        let dir = std::env::temp_dir().join(format!("noida-mysql-persist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let (addr1, save1) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        {
            let pool = make_pool(addr1).await;
            let mut con = pool.get_conn().await.unwrap();

            // ---- Set up tables in the default 'test' schema ----
            con.query_drop("USE test").await.unwrap();
            con.query_drop(
                "CREATE TABLE persist_test (
                    id       INT AUTO_INCREMENT PRIMARY KEY,
                    name     VARCHAR(255),
                    score    INT,
                    amount   DECIMAL(10, 2),
                    null_col VARCHAR(100)
                )",
            )
            .await
            .unwrap();

            // Row 1: regular values including DECIMAL.
            con.query_drop(
                "INSERT INTO persist_test (name, score, amount, null_col)
                 VALUES ('hello', 42, 19.99, NULL)",
            )
            .await
            .unwrap();

            // Row 2: empty string in name column.
            con.query_drop(
                "INSERT INTO persist_test (name, score, amount, null_col)
                 VALUES ('', 0, 0.00, NULL)",
            )
            .await
            .unwrap();

            // Row 3: advances AUTO_INCREMENT to 3.
            con.query_drop(
                "INSERT INTO persist_test (name, score, amount, null_col)
                 VALUES ('third', 99, 3.50, NULL)",
            )
            .await
            .unwrap();

            // ---- A table in the 'mysql' schema (always present) ----
            // Confirms more than just the default schema survives a restart.
            con.query_drop("USE mysql").await.unwrap();
            con.query_drop("CREATE TABLE schema_check (id INT PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            con.query_drop("INSERT INTO schema_check VALUES (1, 'in mysql schema')").await.unwrap();

            drop(con);
            pool.disconnect().await.unwrap();
        }

        // Simulate clean shutdown — trigger save directly (not a real signal;
        // see tests/redis_persistence.rs for why).
        save1();
        assert!(dir.join("mysql.json").exists(), "mysql.json must be written after save");

        // Start a second, completely fresh server against the same dir.
        let (addr2, _save2) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        let pool2 = make_pool(addr2).await;
        let mut con = pool2.get_conn().await.unwrap();

        // ---- Verify 'test' schema survived ----
        con.query_drop("USE test").await.unwrap();
        let rows: Vec<(i64, String, i64)> =
            con.query("SELECT id, name, score FROM persist_test ORDER BY id").await.unwrap();

        assert_eq!(rows.len(), 3, "all 3 rows must survive restart");

        let (id1, name1, score1) = &rows[0];
        assert_eq!(*id1, 1);
        assert_eq!(name1, "hello");
        assert_eq!(*score1, 42);

        // Row 2: empty string in name must survive.
        let (_, name2, _) = &rows[1];
        assert_eq!(name2, "", "empty string in varchar must survive");

        // Row 3 must have id=3.
        let (id3, _, _) = &rows[2];
        assert_eq!(*id3, 3);

        // DECIMAL 19.99 must survive as exact decimal, not lossy float.
        let dec_vals: Vec<String> =
            con.query("SELECT amount FROM persist_test ORDER BY id").await.unwrap();
        assert_eq!(dec_vals[0], "19.99", "DECIMAL 19.99 must round-trip exactly");

        // NULL column must remain NULL.
        let null_vals: Vec<Option<String>> =
            con.query("SELECT null_col FROM persist_test ORDER BY id").await.unwrap();
        assert!(null_vals[0].is_none(), "null_col must remain NULL after restart");

        // ---- AUTO_INCREMENT continuity: insert a new row after restart ----
        con.query_drop(
            "INSERT INTO persist_test (name, score, amount, null_col)
             VALUES ('after_restart', 0, 0.00, NULL)",
        )
        .await
        .unwrap();
        let new_ids: Vec<i64> =
            con.query("SELECT id FROM persist_test ORDER BY id DESC LIMIT 1").await.unwrap();
        assert_eq!(new_ids[0], 4, "next AUTO_INCREMENT must continue from 4, not restart at 1");

        // ---- 'mysql' schema survived ----
        con.query_drop("USE mysql").await.unwrap();
        let val: Vec<String> =
            con.query("SELECT val FROM schema_check WHERE id = 1").await.unwrap();
        assert_eq!(val[0], "in mysql schema", "mysql schema table must survive restart");

        drop(con);
        pool2.disconnect().await.unwrap();
    }

    #[test]
    fn engine_level_snapshot_round_trip() {
        use noida::mysql::catalog::DbState;
        use noida::mysql::engine::Engine;

        let mut engine = Engine::new();
        engine.execute("USE test").unwrap();
        engine
            .execute("CREATE TABLE foo (id INT PRIMARY KEY, name VARCHAR(100), amt DECIMAL(8,2))")
            .unwrap();
        engine.execute("INSERT INTO foo VALUES (1, 'alpha', 9.99)").unwrap();
        engine.execute("INSERT INTO foo VALUES (2, 'beta', 0.01)").unwrap();

        let snapshot = engine.snapshot();
        let bytes = serde_json::to_vec(&snapshot).expect("snapshot must serialize");
        let loaded: DbState = serde_json::from_slice(&bytes).expect("snapshot must deserialize");

        let mut engine2 = Engine::new_persistent(loaded);
        engine2.execute("USE test").unwrap();
        let rows = engine2.execute("SELECT id, name, amt FROM foo ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2, "both rows must survive a serde round-trip");
    }

    #[tokio::test]
    async fn uncommitted_transaction_is_discarded_across_restart() {
        let dir =
            std::env::temp_dir().join(format!("noida-mysql-uncommitted-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let (addr1, save1) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        {
            let pool = make_pool(addr1).await;
            let mut con = pool.get_conn().await.unwrap();

            con.query_drop("USE test").await.unwrap();
            con.query_drop("CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR(50))")
                .await
                .unwrap();
            con.query_drop("INSERT INTO tx_test VALUES (1, 'committed')").await.unwrap();

            // Start a transaction and insert uncommitted data.
            con.query_drop("START TRANSACTION").await.unwrap();
            con.query_drop("INSERT INTO tx_test VALUES (2, 'uncommitted')").await.unwrap();

            // Do NOT commit! Save while transaction is still open.
            drop(con);
            pool.disconnect().await.unwrap();
        }

        save1();
        assert!(dir.join("mysql.json").exists());

        // Restart server from the snapshot.
        let (addr2, _save2) = spawn_persistent_for_test("127.0.0.1:0", &dir).unwrap();
        let pool2 = make_pool(addr2).await;
        let mut con = pool2.get_conn().await.unwrap();

        con.query_drop("USE test").await.unwrap();
        let rows: Vec<(i64, String)> =
            con.query("SELECT id, val FROM tx_test ORDER BY id").await.unwrap();

        // ONLY row 1 must survive; row 2 was uncommitted and must be discarded.
        assert_eq!(rows.len(), 1, "uncommitted row must not survive restart");
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1, "committed");

        drop(con);
        pool2.disconnect().await.unwrap();
    }
}
