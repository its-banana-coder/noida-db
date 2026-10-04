pub mod binder;
pub mod catalog;
pub mod engine;
pub mod error;
pub mod exec;
pub mod funcs;
pub mod infoschema;
pub mod plan;
pub mod server;
pub mod sqlmode;
pub mod types;

#[cfg(test)]
mod tests {
    use crate::mysql::engine::Engine;
    use crate::mysql::types::Value;

    #[test]
    fn select_with_where_resolves_real_column_values() {
        // Regression test: Plan::Project's table-context lookup used to
        // match only `Plan::Scan` directly, so a WHERE clause (which wraps
        // the scan in Plan::Filter first) made every projected column
        // resolve to Value::Null instead of the real row data, even though
        // the WHERE clause itself correctly filtered to the right rows.
        let mut e = Engine::new();
        e.execute("USE test").unwrap();
        e.execute("CREATE TABLE t (id INT, value VARCHAR(255))").unwrap();
        e.execute("INSERT INTO t (id, value) VALUES (1, 'hello'), (2, 'world')").unwrap();

        let rows = e.execute("SELECT id, value FROM t").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("hello".to_string())],
                vec![Value::Int(2), Value::Text("world".to_string())],
            ]
        );

        let rows = e.execute("SELECT id, value FROM t WHERE id = 1").unwrap();
        assert_eq!(rows, vec![vec![Value::Int(1), Value::Text("hello".to_string())]]);
    }

    #[test]
    fn group_by_and_aggregates() {
        let mut e = Engine::new();
        e.execute("USE test").unwrap();
        e.execute("CREATE TABLE orders (id INT, customer VARCHAR(50), amount INT)").unwrap();
        e.execute(
            "INSERT INTO orders (id, customer, amount) VALUES \
             (1, 'alice', 10), (2, 'alice', 20), (3, 'bob', 5)",
        )
        .unwrap();

        // No GROUP BY: whole table is one implicit group.
        let rows = e
            .execute(
                "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(3));
        assert_eq!(rows[0][1], Value::Int(35));
        // Real MySQL: AVG of integers is a DECIMAL with
        // `div_precision_increment` (4) extra digits -- 11.6667, not a float
        // (kept to more digits inside for further arithmetic).
        let Value::Num(avg) = &rows[0][2] else { panic!("AVG should be a DECIMAL") };
        assert_eq!(avg.to_string(), "11.6667");
        assert_eq!(rows[0][3], Value::Int(5));
        assert_eq!(rows[0][4], Value::Int(20));

        // GROUP BY customer.
        let mut rows = e
            .execute("SELECT customer, COUNT(*), SUM(amount) FROM orders GROUP BY customer")
            .unwrap();
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(
            rows,
            vec![
                vec![Value::Text("alice".to_string()), Value::Int(2), Value::Int(30)],
                vec![Value::Text("bob".to_string()), Value::Int(1), Value::Int(5)],
            ]
        );

        // COUNT(*) on an empty table is 0, SUM/AVG/MIN/MAX are NULL.
        e.execute("CREATE TABLE empty_t (id INT)").unwrap();
        let rows = e.execute("SELECT COUNT(*), SUM(id), MIN(id) FROM empty_t").unwrap();
        assert_eq!(rows, vec![vec![Value::Int(0), Value::Null, Value::Null]]);
    }

    #[test]
    fn transactions_commit_and_rollback() {
        let mut e = Engine::new();
        e.execute("USE test").unwrap();
        e.execute("CREATE TABLE t (id INT, value VARCHAR(255))").unwrap();
        e.execute("INSERT INTO t (id, value) VALUES (1, 'first')").unwrap();

        // Rollback undoes everything done since BEGIN.
        e.execute("BEGIN").unwrap();
        e.execute("INSERT INTO t (id, value) VALUES (2, 'second')").unwrap();
        e.execute("UPDATE t SET value = 'changed' WHERE id = 1").unwrap();
        let rows = e.execute("SELECT id, value FROM t").unwrap();
        assert_eq!(rows.len(), 2); // visible mid-transaction
        e.execute("ROLLBACK").unwrap();

        let rows = e.execute("SELECT id, value FROM t").unwrap();
        assert_eq!(rows, vec![vec![Value::Int(1), Value::Text("first".to_string())]]);

        // Commit keeps everything done since BEGIN.
        e.execute("START TRANSACTION").unwrap();
        e.execute("INSERT INTO t (id, value) VALUES (2, 'second')").unwrap();
        e.execute("COMMIT").unwrap();

        let mut rows = e.execute("SELECT id, value FROM t").unwrap();
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("first".to_string())],
                vec![Value::Int(2), Value::Text("second".to_string())],
            ]
        );
    }

    #[test]
    fn affected_rows_reported_for_mutations() {
        let mut e = Engine::new();
        e.execute("USE test").unwrap();
        e.execute("CREATE TABLE t (id INT, value VARCHAR(255))").unwrap();

        e.execute("INSERT INTO t (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        assert_eq!(e.last_affected_rows, 3);

        e.execute("UPDATE t SET value = 'z' WHERE id <= 2").unwrap();
        assert_eq!(e.last_affected_rows, 2);

        e.execute("DELETE FROM t WHERE id = 3").unwrap();
        assert_eq!(e.last_affected_rows, 1);

        e.execute("SELECT id, value FROM t").unwrap();
        assert_eq!(e.last_affected_rows, 0);
    }

    #[test]
    fn prepared_statement_param_binding_at_engine_level() {
        // Engine::execute doesn't expose PREPARE/EXECUTE (that's a
        // server.rs wire-protocol concept), so this exercises the same
        // Binder -> Plan -> Executor path server.rs's COM_STMT_PREPARE/
        // COM_STMT_EXECUTE use, with Executor::params standing in for
        // decoded bound values. The real wire-protocol path is covered by
        // tests/mysql_client.rs.
        use crate::mysql::binder::Binder;
        use crate::mysql::exec::Executor;
        use sqlparser::dialect::MySqlDialect;
        use sqlparser::parser::Parser;

        let mut e = Engine::new();
        e.execute("USE test").unwrap();
        e.execute("CREATE TABLE t (id INT, value VARCHAR(255))").unwrap();
        e.execute("INSERT INTO t (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();

        let dialect = MySqlDialect {};
        let stmt =
            Parser::parse_sql(&dialect, "SELECT value FROM t WHERE id = ?").unwrap().remove(0);
        let mut binder = Binder::new(e.current_db.clone());
        let plan = binder.bind_statement(stmt).unwrap();
        assert_eq!(crate::mysql::plan::count_params(&plan), 1);

        let mut executor = Executor::new(e.db.clone(), e.current_db.clone());
        executor.params = vec![Value::Int(2)];
        let rows = executor.execute_plan(plan.clone()).unwrap();
        assert_eq!(rows, vec![vec![Value::Text("b".to_string())]]);

        // Same prepared plan, different bound value: EXECUTE must use the
        // parameter actually sent, not whatever was in the original text.
        let mut executor = Executor::new(e.db.clone(), e.current_db.clone());
        executor.params = vec![Value::Int(3)];
        let rows = executor.execute_plan(plan).unwrap();
        assert_eq!(rows, vec![vec![Value::Text("c".to_string())]]);
    }
}
