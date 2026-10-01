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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default: Option<Value>,
    pub auto_increment: bool,
    pub primary_key: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub next_auto_increment: i64,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns, rows: Vec::new(), next_auto_increment: 1 }
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
