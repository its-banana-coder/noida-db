//! In-memory table storage. One `Table` per (database, name). Rows live in
//! a plain `Vec`, evaluated row-at-a-time: performance is not a goal for
//! local-dev data sizes (see docs/specs/README.md). There are no parts or
//! background merges — `ReplacingMergeTree`/`SummingMergeTree` semantics
//! are applied on demand by `engine::merge_final`, for `SELECT ... FINAL`
//! and `OPTIMIZE TABLE ... FINAL` (see docs/LIMITATIONS.md).

use std::collections::HashMap;

use super::sql;
use super::types::{Type, Val};

#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    Memory,
    MergeTree,
    /// `ver`/`is_deleted` name the columns `ReplacingMergeTree(ver[,
    /// is_deleted])` was created with, if any.
    ReplacingMergeTree {
        ver: Option<String>,
        is_deleted: Option<String>,
    },
    /// `sum_columns` names `SummingMergeTree(col, ...)`'s explicit columns;
    /// `None` means "every numeric column not in `ORDER BY`", same as real
    /// ClickHouse's default.
    SummingMergeTree {
        sum_columns: Option<Vec<String>>,
    },
}

impl Engine {
    pub fn name(&self) -> &'static str {
        match self {
            Engine::Memory => "Memory",
            Engine::MergeTree => "MergeTree",
            Engine::ReplacingMergeTree { .. } => "ReplacingMergeTree",
            Engine::SummingMergeTree { .. } => "SummingMergeTree",
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

/// A `CREATE MATERIALIZED VIEW name TO target AS SELECT ... FROM source`:
/// every `INSERT` into `source` re-runs `select` over just the inserted
/// block and appends the result to `target`.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    pub source: (String, String),
    pub target: (String, String),
    pub select: sql::Select,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<(String, String), Table>,
    mvs: HashMap<(String, String), MaterializedView>,
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

    pub fn mv_exists(&self, database: &str, name: &str) -> bool {
        self.mvs.contains_key(&(database.to_string(), name.to_string()))
    }

    pub fn create_mv(&mut self, database: &str, name: &str, mv: MaterializedView) {
        self.mvs.insert((database.to_string(), name.to_string()), mv);
    }

    /// The materialized views whose source is `(database, table)`, in
    /// creation order isn't tracked (`HashMap`) — fine, since each only
    /// ever writes to its own target.
    pub fn mvs_for_source(&self, database: &str, table: &str) -> Vec<MaterializedView> {
        self.mvs
            .values()
            .filter(|mv| mv.source == (database.to_string(), table.to_string()))
            .cloned()
            .collect()
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
