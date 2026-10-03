use crate::mysql::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
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
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns, rows: Vec::new(), next_auto_increment: 1, unique_keys: Vec::new() }
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbState {
    pub schemas: BTreeMap<String, Schema>,
    /// Pre-transaction database state if a transaction is currently open.
    /// Excluded from on-disk serialization so a snapshot never stores
    /// this internal recovery field.
    #[serde(skip)]
    pub tx_base: Option<Box<DbState>>,
}

impl Default for DbState {
    fn default() -> Self {
        let mut schemas = BTreeMap::new();
        // MySQL default schemas
        for name in ["information_schema", "mysql", "performance_schema", "sys", "test"] {
            schemas.insert(
                name.to_string(),
                Schema { name: name.to_string(), tables: BTreeMap::new() },
            );
        }
        Self { schemas, tx_base: None }
    }
}
