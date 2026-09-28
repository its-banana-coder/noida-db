//! In-memory table storage. One `Table` per (database, name); real
//! ClickHouse's parts/merges/background-merge semantics aren't built yet
//! (`OPTIMIZE ... FINAL` and `SELECT ... FINAL` land with
//! ReplacingMergeTree support — see docs/LIMITATIONS.md). Rows live in a
//! plain `Vec`, evaluated row-at-a-time: performance is not a goal for
//! local-dev data sizes (see docs/specs/README.md).

use std::collections::HashMap;

use super::types::{Type, Val};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Memory,
    MergeTree,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Memory => "Memory",
            Engine::MergeTree => "MergeTree",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<(String, Type)>,
    pub engine: Engine,
    pub order_by: Vec<String>,
    pub rows: Vec<Vec<Val>>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<(String, String), Table>,
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog::default()
    }

    pub fn get(&self, database: &str, table: &str) -> Option<&Table> {
        self.tables.get(&(database.to_string(), table.to_string()))
    }

    pub fn get_mut(&mut self, database: &str, table: &str) -> Option<&mut Table> {
        self.tables.get_mut(&(database.to_string(), table.to_string()))
    }

    pub fn exists(&self, database: &str, table: &str) -> bool {
        self.tables.contains_key(&(database.to_string(), table.to_string()))
    }

    pub fn create(&mut self, database: &str, table: &str, t: Table) {
        self.tables.insert((database.to_string(), table.to_string()), t);
    }

    pub fn drop(&mut self, database: &str, table: &str) -> bool {
        self.tables.remove(&(database.to_string(), table.to_string())).is_some()
    }

    /// `(database, table, engine name, row count)` for every table, sorted,
    /// for `system.tables`.
    pub fn list(&self) -> Vec<(String, String, &'static str, usize)> {
        let mut out: Vec<_> = self
            .tables
            .iter()
            .map(|((db, tbl), t)| (db.clone(), tbl.clone(), t.engine.name(), t.rows.len()))
            .collect();
        out.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Table {
        Table {
            columns: vec![("id".into(), Type::UInt32)],
            engine: Engine::Memory,
            order_by: vec![],
            rows: vec![],
        }
    }

    #[test]
    fn create_get_drop_roundtrip() {
        let mut c = Catalog::new();
        assert!(!c.exists("default", "t"));
        c.create("default", "t", table());
        assert!(c.exists("default", "t"));
        assert!(c.get("default", "t").is_some());
        assert!(c.drop("default", "t"));
        assert!(!c.exists("default", "t"));
        assert!(!c.drop("default", "t"));
    }

    #[test]
    fn list_is_sorted() {
        let mut c = Catalog::new();
        c.create("default", "b", table());
        c.create("default", "a", table());
        let names: Vec<_> = c.list().into_iter().map(|(_, name, _, _)| name).collect();
        assert_eq!(names, ["a", "b"]);
    }
}
