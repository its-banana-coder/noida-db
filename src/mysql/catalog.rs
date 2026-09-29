use crate::mysql::types::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default: Option<Value>,
    pub auto_increment: bool,
    pub primary_key: bool,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub name: String,
    pub tables: BTreeMap<String, Arc<Table>>,
}

#[derive(Clone, Debug)]
pub struct DbState {
    pub schemas: BTreeMap<String, Schema>,
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
        Self { schemas }
    }
}
