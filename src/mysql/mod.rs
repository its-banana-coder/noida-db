pub mod binder;
pub mod catalog;
pub mod engine;
pub mod error;
pub mod exec;
pub mod plan;
pub mod server;
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
}
