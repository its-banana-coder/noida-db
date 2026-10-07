use crate::mysql::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    /// `INT` (32-bit). `TINYINT`/`SMALLINT`/`MEDIUMINT` have their own
    /// variants so each column enforces its own range.
    Int,
    BigInt,
    Varchar(usize),
    Text,
    Float,
    Double,
    Decimal(u8, u8),
    Date,
    Datetime,
    Boolean,
    /// `ENUM('a', 'b')`: one of the listed strings (stored as that string).
    Enum(Vec<String>),
    /// `JSON`: validated on write, stored parsed.
    Json,
    /// `BLOB`/`BINARY`/`VARBINARY` and friends.
    Blob,
    TinyInt,
    SmallInt,
    MediumInt,
    /// `TIME`: a signed duration in microseconds (`-838:59:59` to
    /// `838:59:59`), with its fractional-seconds precision (0-6).
    Time(u8),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default: Option<Value>,
    pub auto_increment: bool,
    pub primary_key: bool,
    /// `DEFAULT CURRENT_TIMESTAMP` (or `NOW()`/`CURRENT_DATE`): filled with
    /// the insert time, not a stored constant. `#[serde(default)]` so a
    /// snapshot written before this field existed still loads.
    #[serde(default)]
    pub default_now: bool,
    /// `ON UPDATE CURRENT_TIMESTAMP`: an `UPDATE` that doesn't assign this
    /// column itself sets it to the update time.
    #[serde(default)]
    pub on_update_now: bool,
    /// `UNSIGNED` integer: its range starts at 0.
    #[serde(default)]
    pub unsigned: bool,
    /// `COMMENT '...'` (empty when none).
    #[serde(default)]
    pub comment: String,
}

/// A `UNIQUE` key: a column-level `UNIQUE` (named after its column) or a
/// table-level `UNIQUE KEY name (a, b)`. The `PRIMARY KEY` isn't stored
/// here -- it's derived from the columns' own `primary_key` flags.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UniqueKey {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub next_auto_increment: i64,
    /// `#[serde(default)]` so a snapshot written before keys were tracked
    /// still loads (with no UNIQUE keys, as it always behaved).
    #[serde(default)]
    pub unique_keys: Vec<UniqueKey>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    /// Plain (non-unique) `KEY`/`INDEX`es: not used for lookups, kept so
    /// SHOW INDEX, SHOW CREATE TABLE and schema dumps see them.
    #[serde(default)]
    pub indexes: Vec<UniqueKey>,
    /// The table's `COMMENT` (empty when none).
    #[serde(default)]
    pub comment: String,
    /// `CHECK` constraints, enforced on every write.
    #[serde(default)]
    pub checks: Vec<Check>,
}

/// A `CHECK (expr)` constraint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Check {
    /// Its name (`<table>_chk_<n>` when the DDL gave none; empty until
    /// the table it's added to assigns one).
    pub name: String,
    /// The expression as SQL this engine parses back to evaluate it.
    pub expr: String,
    /// The expression the way MySQL normalizes it for SHOW CREATE TABLE
    /// and `information_schema.CHECK_CONSTRAINTS` (`(`a` >= 0)`).
    pub clause: String,
}

/// A `FOREIGN KEY (columns) REFERENCES ref_db.ref_table (ref_columns)`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_db: String,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    /// The index backing it (SHOW CREATE TABLE's `KEY` line).
    #[serde(default)]
    pub index: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum FkAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
}

impl FkAction {
    pub fn sql(self) -> &'static str {
        match self {
            FkAction::Restrict => "RESTRICT",
            FkAction::Cascade => "CASCADE",
            FkAction::SetNull => "SET NULL",
            FkAction::NoAction => "NO ACTION",
        }
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
            next_auto_increment: 1,
            unique_keys: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            comment: String::new(),
            checks: Vec::new(),
        }
    }

    /// A copy of everything but the rows (what resolving column names needs).
    pub fn shape(&self) -> Table {
        Table {
            name: self.name.clone(),
            columns: self.columns.clone(),
            rows: Vec::new(),
            next_auto_increment: self.next_auto_increment,
            unique_keys: self.unique_keys.clone(),
            foreign_keys: self.foreign_keys.clone(),
            indexes: self.indexes.clone(),
            comment: self.comment.clone(),
            checks: self.checks.clone(),
        }
    }

    /// Every index as `(name, column indices, unique)`: PRIMARY, the UNIQUE
    /// keys, then the plain ones, as MySQL lists them.
    pub fn all_indexes(&self) -> Vec<(String, Vec<usize>, bool)> {
        let mut out: Vec<_> = self.keys().into_iter().map(|(n, c)| (n, c, true)).collect();
        for k in &self.indexes {
            let idx: Vec<usize> = k
                .columns
                .iter()
                .filter_map(|c| {
                    self.columns.iter().position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if !idx.is_empty() {
                out.push((k.name.clone(), idx, false));
            }
        }
        out
    }

    /// Whether an index (of any kind) is named `name`.
    pub fn has_index(&self, name: &str) -> bool {
        (name.eq_ignore_ascii_case("PRIMARY") && self.columns.iter().any(|c| c.primary_key))
            || self
                .unique_keys
                .iter()
                .chain(&self.indexes)
                .any(|k| k.name.eq_ignore_ascii_case(name))
    }

    /// The name MySQL gives an unnamed index: its first column, then
    /// `_2`, `_3`, ... while that's taken.
    pub fn index_name_for(&self, first_col: &str) -> String {
        let mut name = first_col.to_string();
        let mut n = 2;
        while self.has_index(&name) {
            name = format!("{first_col}_{n}");
            n += 1;
        }
        name
    }

    /// Every key that must hold unique values, as `(name, column indices)`:
    /// the PRIMARY KEY first (all `primary_key` columns together, so a
    /// composite key is one key, not several), then each UNIQUE key.
    pub fn keys(&self) -> Vec<(String, Vec<usize>)> {
        let mut out = Vec::new();
        let pk: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();
        if !pk.is_empty() {
            out.push(("PRIMARY".to_string(), pk));
        }
        for k in &self.unique_keys {
            let idx: Vec<usize> = k
                .columns
                .iter()
                .filter_map(|c| {
                    self.columns.iter().position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if idx.len() == k.columns.len() && !idx.is_empty() {
                out.push((k.name.clone(), idx));
            }
        }
        out
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub tables: BTreeMap<String, Arc<Table>>,
    #[serde(default)]
    pub views: BTreeMap<String, View>,
}

/// `CREATE VIEW name [(columns)] AS select`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    /// The SELECT, as text (bound afresh wherever the view is used).
    pub sql: String,
    /// The `(columns)` list, if one was given.
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbState {
    pub schemas: BTreeMap<String, Schema>,
    /// Every open transaction's undo log, by connection id: for each table
    /// the transaction wrote, the table as it was before its first write
    /// and as the transaction last left it. Shared (not per-connection) so
    /// a clean-shutdown snapshot can leave out every open transaction's
    /// uncommitted writes. Never serialized.
    #[serde(skip)]
    pub open_txns: HashMap<u64, TxUndo>,
    /// Which connection each blocked connection is waiting for, to detect
    /// deadlocks. Never serialized.
    #[serde(skip)]
    pub waits: HashMap<u64, u64>,
}

/// One open transaction's undo log; see `DbState::open_txns`.
#[derive(Clone, Debug, Default)]
pub struct TxUndo {
    pub tables: BTreeMap<(String, String), (Arc<Table>, Arc<Table>)>,
    /// Rows locked by `SELECT ... FOR UPDATE` / `FOR SHARE` (rows the
    /// transaction wrote are locked implicitly; see `DbState::locked_rows`).
    pub locks: Vec<RowLock>,
}

#[derive(Clone, Debug)]
pub struct RowLock {
    pub db: String,
    pub table: String,
    pub key: String,
    pub exclusive: bool,
}

/// A row's identity for locking: its primary key, else the whole row.
pub fn row_key(t: &Table, row: &[Value]) -> String {
    let pk: Vec<&Value> =
        t.columns.iter().enumerate().filter(|(_, c)| c.primary_key).map(|(i, _)| &row[i]).collect();
    if pk.is_empty() { format!("{row:?}") } else { format!("{pk:?}") }
}

/// The rows only in `before` and the rows only in `after` (multisets).
pub fn row_diff(before: &[Row], after: &[Row]) -> (Vec<Row>, Vec<Row>) {
    let mut counts: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, r) in before.iter().enumerate() {
        counts.entry(format!("{r:?}")).or_default().push(i);
    }
    let mut added = Vec::new();
    for r in after {
        if counts.get_mut(&format!("{r:?}")).and_then(|v| v.pop()).is_none() {
            added.push(r.clone());
        }
    }
    let mut removed: Vec<usize> = counts.into_values().flatten().collect();
    removed.sort_unstable();
    (removed.into_iter().map(|i| before[i].clone()).collect(), added)
}

impl DbState {
    /// Every table's column names, by (database, name), for the binder.
    pub fn table_columns(&self) -> std::collections::HashMap<(String, String), Vec<String>> {
        self.schemas
            .iter()
            .flat_map(|(db, s)| {
                s.tables.values().map(move |t| {
                    (
                        (db.clone(), t.name.clone()),
                        t.columns.iter().map(|c| c.name.clone()).collect(),
                    )
                })
            })
            .collect()
    }

    /// Every view, by (database, name), for the binder.
    pub fn view_defs(&self) -> std::collections::HashMap<(String, String), View> {
        self.schemas
            .iter()
            .flat_map(|(db, s)| {
                s.views.values().map(move |v| ((db.clone(), v.name.clone()), v.clone()))
            })
            .collect()
    }
}

impl DbState {
    /// The row keys of `db`.`table` other connections' open transactions
    /// hold, with the holder and whether the lock is exclusive: every row
    /// a transaction inserted, changed or deleted, and every row it locked
    /// with `FOR UPDATE`/`FOR SHARE`.
    pub fn locked_rows(&self, me: u64, db: &str, table: &str) -> Vec<(u64, String, bool)> {
        let mut out = Vec::new();
        for (&conn, undo) in &self.open_txns {
            if conn == me {
                continue;
            }
            if let Some((before, ours)) = undo.tables.get(&(db.to_string(), table.to_string())) {
                let (removed, added) = row_diff(&before.rows, &ours.rows);
                for r in removed.iter().chain(&added) {
                    out.push((conn, row_key(ours, r), true));
                }
            }
            for l in &undo.locks {
                if l.db == db && l.table == table {
                    out.push((conn, l.key.clone(), l.exclusive));
                }
            }
        }
        out
    }

    /// `db`.`table` as connection `me` should read it: without other
    /// connections' uncommitted changes. None when nobody else has any.
    pub fn committed_view(&self, me: u64, db: &str, table: &str) -> Option<Table> {
        let key = (db.to_string(), table.to_string());
        let others: Vec<&(Arc<Table>, Arc<Table>)> = self
            .open_txns
            .iter()
            .filter(|(c, _)| **c != me)
            .filter_map(|(_, u)| u.tables.get(&key))
            .filter(|(before, ours)| !Arc::ptr_eq(before, ours))
            .collect();
        if others.is_empty() {
            return None;
        }
        let current = self.schemas.get(db)?.tables.get(table)?;
        let mut rows = current.rows.clone();
        for (before, ours) in others {
            let (removed, added) = row_diff(&before.rows, &ours.rows);
            for r in added {
                if let Some(i) = rows.iter().position(|x| *x == r) {
                    rows.remove(i);
                }
            }
            rows.extend(removed);
        }
        Some(Table { rows, ..(**current).clone() })
    }

    /// Every foreign key that references `db`.`table`, with the database
    /// and name of the table that declares it.
    pub fn referencing(&self, db: &str, table: &str) -> Vec<(String, String, ForeignKey)> {
        let mut out = Vec::new();
        for (sname, schema) in &self.schemas {
            for (tname, t) in &schema.tables {
                for fk in &t.foreign_keys {
                    if fk.ref_db == db && fk.ref_table.eq_ignore_ascii_case(table) {
                        out.push((sname.clone(), tname.clone(), fk.clone()));
                    }
                }
            }
        }
        out
    }

    /// `db`.`table` and every table a cascading write to it can reach.
    pub fn cascade_reach(&self, db: &str, table: &str) -> Vec<(String, String)> {
        let mut out = vec![(db.to_string(), table.to_string())];
        let mut i = 0;
        while i < out.len() {
            let (d, t) = out[i].clone();
            for (cd, ct, _) in self.referencing(&d, &t) {
                if !out.contains(&(cd.clone(), ct.clone())) {
                    out.push((cd, ct));
                }
            }
            i += 1;
        }
        out
    }

    /// Undoes one transaction's writes. A table nobody else has written
    /// since gets its pre-transaction image back exactly; one that another
    /// connection has also changed meanwhile gets only this transaction's
    /// own row changes reversed, so the other connection's committed rows
    /// survive. `AUTO_INCREMENT` counters are never rolled back (as in
    /// MySQL).
    pub fn undo(&mut self, undo: &TxUndo) {
        for ((db, name), (before, ours)) in &undo.tables {
            let Some(current) = self.schemas.get(db).and_then(|s| s.tables.get(name)).cloned()
            else {
                continue;
            };
            let mut restored = if Arc::ptr_eq(&current, ours) {
                (**before).clone()
            } else {
                let mut rows = current.rows.clone();
                let mut removed = before.rows.clone();
                for r in &ours.rows {
                    if let Some(i) = removed.iter().position(|x| x == r) {
                        removed.swap_remove(i);
                    } else if let Some(i) = rows.iter().position(|x| x == r) {
                        rows.remove(i);
                    }
                }
                rows.extend(removed);
                Table { rows, ..(*current).clone() }
            };
            restored.next_auto_increment = current.next_auto_increment;
            if let Some(schema) = self.schemas.get_mut(db) {
                schema.tables.insert(name.clone(), Arc::new(restored));
            }
        }
    }
}

impl Default for DbState {
    fn default() -> Self {
        let mut schemas = BTreeMap::new();
        // MySQL default schemas
        for name in ["information_schema", "mysql", "performance_schema", "sys", "test"] {
            schemas.insert(
                name.to_string(),
                Schema { name: name.to_string(), tables: BTreeMap::new(), views: BTreeMap::new() },
            );
        }
        Self { schemas, open_txns: HashMap::new(), waits: HashMap::new() }
    }
}
