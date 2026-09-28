//! In-memory table storage. One `Table` per (database, name). Rows live in
//! a plain `Vec`, evaluated row-at-a-time: performance is not a goal for
//! local-dev data sizes (see docs/specs/README.md). There are no parts or
//! background merges — `ReplacingMergeTree`/`SummingMergeTree` semantics
//! are applied on demand by `engine::merge_final`, for `SELECT ... FINAL`
//! and `OPTIMIZE TABLE ... FINAL` (see docs/LIMITATIONS.md).

use std::collections::{HashMap, HashSet};

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

impl Table {
    /// The `ENGINE = ...(args)` clause `SHOW CREATE TABLE` prints, e.g.
    /// `ENGINE = ReplacingMergeTree(ver, is_deleted)`.
    fn engine_clause(&self) -> String {
        match &self.engine {
            Engine::Memory => "Memory".to_string(),
            Engine::MergeTree => "MergeTree".to_string(),
            Engine::ReplacingMergeTree { ver, is_deleted } => {
                let mut args = Vec::new();
                if let Some(v) = ver {
                    args.push(v.clone());
                }
                if let Some(d) = is_deleted {
                    args.push(d.clone());
                }
                if args.is_empty() {
                    "ReplacingMergeTree".to_string()
                } else {
                    format!("ReplacingMergeTree({})", args.join(", "))
                }
            }
            Engine::SummingMergeTree { sum_columns } => match sum_columns {
                Some(cols) if !cols.is_empty() => {
                    format!("SummingMergeTree({})", cols.join(", "))
                }
                _ => "SummingMergeTree".to_string(),
            },
        }
    }

    /// The canonical multi-line `CREATE TABLE` statement real ClickHouse's
    /// `SHOW CREATE TABLE` reproduces (backtick-quoted column names, one per
    /// line, `ENGINE = ...`, `ORDER BY` when the table declares one, and a
    /// `SETTINGS index_granularity = 8192` tail for `*MergeTree` engines —
    /// ClickHouse's own default, printed even though noida-db doesn't use
    /// granules). Not verified byte-for-byte against a real server (none
    /// reachable here) — see docs/LIMITATIONS.md.
    pub fn create_table_sql(&self, database: &str, table: &str) -> String {
        let mut out = format!("CREATE TABLE {database}.{table}\n(\n");
        for (i, (name, ty)) in self.columns.iter().enumerate() {
            if i > 0 {
                out.push_str(",\n");
            }
            out.push_str(&format!("    `{name}` {}", ty.name()));
        }
        out.push_str("\n)\n");
        out.push_str(&format!("ENGINE = {}\n", self.engine_clause()));
        if !self.order_by.is_empty() {
            let cols = self.order_by.join(", ");
            if self.order_by.len() == 1 {
                out.push_str(&format!("ORDER BY {cols}\n"));
            } else {
                out.push_str(&format!("ORDER BY ({cols})\n"));
            }
        }
        if !matches!(self.engine, Engine::Memory) {
            out.push_str("SETTINGS index_granularity = 8192\n");
        }
        out
    }
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

    /// `(database, table)` for every table, sorted — the identity list
    /// `system.columns` walks (unlike `list()`, no engine/row-count is
    /// needed here, and callers want the `&Table` too).
    pub fn all_tables(&self) -> Vec<(String, String, &Table)> {
        let mut out: Vec<_> =
            self.tables.iter().map(|((db, tbl), t)| (db.clone(), tbl.clone(), t)).collect();
        out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        out
    }

    /// Distinct database names that have at least one table, sorted —
    /// `SHOW DATABASES` unions this with the fixed built-in databases.
    pub fn databases_with_tables(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .tables
            .keys()
            .map(|(db, _)| db.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        out.sort();
        out
    }

    /// Table names in `database`, optionally filtered by a `LIKE` pattern
    /// (`%`/`_` wildcards), sorted — `SHOW TABLES`.
    pub fn tables_in(&self, database: &str, like: Option<&str>) -> Vec<String> {
        let mut out: Vec<String> = self
            .tables
            .keys()
            .filter(|(db, _)| db == database)
            .filter(|(_, name)| like.is_none_or(|pat| like_match(pat, name)))
            .map(|(_, name)| name.clone())
            .collect();
        out.sort();
        out
    }
}

/// A minimal SQL `LIKE` matcher: `%` matches any run of characters, `_`
/// matches exactly one. No escaping support (`\%`) — not needed by the
/// simple patterns `SHOW TABLES LIKE` is used with in practice.
pub fn like_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    like_match_rec(&p, &t)
}

fn like_match_rec(p: &[char], t: &[char]) -> bool {
    match p.first() {
        None => t.is_empty(),
        Some('%') => like_match_rec(&p[1..], t) || (!t.is_empty() && like_match_rec(p, &t[1..])),
        Some('_') => !t.is_empty() && like_match_rec(&p[1..], &t[1..]),
        Some(c) => !t.is_empty() && t[0] == *c && like_match_rec(&p[1..], &t[1..]),
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

    #[test]
    fn like_matches_percent_and_underscore_wildcards() {
        assert!(like_match("ev%", "events"));
        assert!(like_match("%vent%", "events"));
        assert!(like_match("ev_nts", "events"));
        assert!(!like_match("ev_nts", "evxxnts"));
        assert!(!like_match("users", "events"));
        assert!(like_match("%", "anything"));
    }

    #[test]
    fn all_tables_and_databases_with_tables_are_sorted() {
        let mut c = Catalog::new();
        c.create("default", "t", table());
        c.create("mydb", "u", table());
        let dbs = c.databases_with_tables();
        assert_eq!(dbs, ["default", "mydb"]);
        let names: Vec<_> = c.all_tables().into_iter().map(|(db, name, _)| (db, name)).collect();
        assert_eq!(
            names,
            [("default".to_string(), "t".to_string()), ("mydb".to_string(), "u".to_string())]
        );
    }

    #[test]
    fn tables_in_filters_by_database_and_like() {
        let mut c = Catalog::new();
        c.create("default", "events", table());
        c.create("default", "users", table());
        c.create("mydb", "orders", table());
        assert_eq!(c.tables_in("default", None), ["events", "users"]);
        assert_eq!(c.tables_in("default", Some("ev%")), ["events"]);
        assert_eq!(c.tables_in("mydb", None), ["orders"]);
    }

    #[test]
    fn create_table_sql_reproduces_a_canonical_statement() {
        let t = Table {
            columns: vec![("id".into(), Type::UInt32), ("name".into(), Type::String)],
            engine: Engine::MergeTree,
            order_by: vec!["id".to_string()],
            rows: vec![],
        };
        let sql = t.create_table_sql("default", "t");
        assert!(
            sql.starts_with("CREATE TABLE default.t\n(\n    `id` UInt32,\n    `name` String\n)\n")
        );
        assert!(sql.contains("ENGINE = MergeTree\n"));
        assert!(sql.contains("ORDER BY id\n"));
        assert!(sql.contains("SETTINGS index_granularity = 8192"));
    }

    #[test]
    fn create_table_sql_memory_engine_has_no_order_by_or_settings() {
        let t = Table {
            columns: vec![("id".into(), Type::UInt32)],
            engine: Engine::Memory,
            order_by: vec![],
            rows: vec![],
        };
        let sql = t.create_table_sql("default", "t");
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("SETTINGS"));
        assert!(sql.contains("ENGINE = Memory"));
    }

    #[test]
    fn create_table_sql_replacing_merge_tree_with_args_and_composite_order_by() {
        let t = Table {
            columns: vec![
                ("id".into(), Type::UInt32),
                ("ver".into(), Type::UInt32),
                ("deleted".into(), Type::Bool),
            ],
            engine: Engine::ReplacingMergeTree {
                ver: Some("ver".to_string()),
                is_deleted: Some("deleted".to_string()),
            },
            order_by: vec!["id".to_string(), "ver".to_string()],
            rows: vec![],
        };
        let sql = t.create_table_sql("default", "t");
        assert!(sql.contains("ENGINE = ReplacingMergeTree(ver, deleted)\n"));
        assert!(sql.contains("ORDER BY (id, ver)\n"));
    }
}
